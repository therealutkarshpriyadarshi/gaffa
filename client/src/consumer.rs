use common::{GaffaError, Result};
use futures::{SinkExt, StreamExt};
use protocol::{ClientCodec, PartitionAssignment, Record, Request, Response, TopicMetadata};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio_util::codec::Framed;

/// A consumer client for reading messages from the broker
pub struct Consumer {
    framed: Arc<Mutex<Framed<TcpStream, ClientCodec>>>,
    subscriptions: HashMap<String, Vec<u32>>, // topic -> partitions
    offsets: HashMap<(String, u32), u64>,     // (topic, partition) -> offset
    // Consumer group fields
    group_id: Option<String>,
    member_id: Option<String>,
    assignments: Vec<PartitionAssignment>,
    auto_commit: bool,
    auto_commit_interval: Duration,
    heartbeat_task: Option<JoinHandle<()>>,
}

impl Consumer {
    /// Connect to a broker
    pub async fn connect(addr: &str) -> Result<Self> {
        let stream = TcpStream::connect(addr)
            .await
            .map_err(|e| GaffaError::Connection(e.to_string()))?;

        let framed = Framed::new(stream, ClientCodec);

        tracing::info!("Consumer connected to {}", addr);

        Ok(Self {
            framed: Arc::new(Mutex::new(framed)),
            subscriptions: HashMap::new(),
            offsets: HashMap::new(),
            group_id: None,
            member_id: None,
            assignments: Vec::new(),
            auto_commit: false,
            auto_commit_interval: Duration::from_secs(5),
            heartbeat_task: None,
        })
    }

    /// Set the consumer group ID
    pub fn with_group_id(mut self, group_id: impl Into<String>) -> Self {
        self.group_id = Some(group_id.into());
        self
    }

    /// Enable auto-commit with optional interval (default 5 seconds)
    pub fn with_auto_commit(mut self, interval: Option<Duration>) -> Self {
        self.auto_commit = true;
        if let Some(interval) = interval {
            self.auto_commit_interval = interval;
        }
        self
    }

    /// Join a consumer group and get partition assignments
    pub async fn join_group(&mut self, topics: Vec<&str>) -> Result<()> {
        let group_id = self
            .group_id
            .as_ref()
            .ok_or_else(|| GaffaError::Protocol("Group ID not set".to_string()))?
            .clone();

        let request = Request::JoinGroup {
            group_id: group_id.clone(),
            member_id: self.member_id.clone(),
            topics: topics.iter().map(|s| s.to_string()).collect(),
        };

        let mut framed = self.framed.lock().await;
        framed
            .send(request)
            .await
            .map_err(|e| GaffaError::Connection(e.to_string()))?;

        let response = framed
            .next()
            .await
            .ok_or_else(|| GaffaError::Connection("Connection closed".to_string()))?
            .map_err(|e| GaffaError::Connection(e.to_string()))?;

        drop(framed); // Release lock

        match response {
            Response::JoinGroupSuccess {
                member_id,
                assignments,
                ..
            } => {
                self.member_id = Some(member_id.clone());
                self.assignments = assignments.clone();

                // Initialize offsets for assigned partitions
                for assignment in &assignments {
                    let offset = self
                        .fetch_committed_offset(&assignment.topic, assignment.partition)
                        .await?;
                    self.offsets
                        .insert((assignment.topic.clone(), assignment.partition), offset);
                }

                tracing::info!(
                    "Joined group '{}' as member '{}' with {} assignments",
                    group_id,
                    member_id,
                    assignments.len()
                );

                // Start heartbeat task
                self.start_heartbeat_task();

                Ok(())
            }
            Response::JoinGroupError { error } => Err(GaffaError::Protocol(error)),
            _ => Err(GaffaError::Protocol("Unexpected response".to_string())),
        }
    }

    /// Leave the consumer group
    pub async fn leave_group(&mut self) -> Result<()> {
        let group_id = self
            .group_id
            .as_ref()
            .ok_or_else(|| GaffaError::Protocol("Not in a group".to_string()))?
            .clone();

        let member_id = self
            .member_id
            .as_ref()
            .ok_or_else(|| GaffaError::Protocol("Member ID not set".to_string()))?
            .clone();

        // Stop heartbeat task
        if let Some(task) = self.heartbeat_task.take() {
            task.abort();
        }

        let request = Request::LeaveGroup {
            group_id: group_id.clone(),
            member_id: member_id.clone(),
        };

        let mut framed = self.framed.lock().await;
        framed
            .send(request)
            .await
            .map_err(|e| GaffaError::Connection(e.to_string()))?;

        let response = framed
            .next()
            .await
            .ok_or_else(|| GaffaError::Connection("Connection closed".to_string()))?
            .map_err(|e| GaffaError::Connection(e.to_string()))?;

        match response {
            Response::LeaveGroupSuccess { .. } => {
                tracing::info!("Left group '{}'", group_id);
                self.member_id = None;
                self.assignments.clear();
                Ok(())
            }
            Response::LeaveGroupError { error } => Err(GaffaError::Protocol(error)),
            _ => Err(GaffaError::Protocol("Unexpected response".to_string())),
        }
    }

    /// Start the heartbeat background task
    fn start_heartbeat_task(&mut self) {
        let group_id = self.group_id.clone().unwrap();
        let member_id = self.member_id.clone().unwrap();
        let framed = self.framed.clone();

        let task = tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(10));
            loop {
                interval.tick().await;

                let request = Request::Heartbeat {
                    group_id: group_id.clone(),
                    member_id: member_id.clone(),
                };

                let mut framed = framed.lock().await;
                if let Err(e) = framed.send(request).await {
                    tracing::error!("Heartbeat send failed: {}", e);
                    break;
                }

                match framed.next().await {
                    Some(Ok(Response::HeartbeatSuccess)) => {
                        tracing::debug!("Heartbeat acknowledged");
                    }
                    Some(Ok(Response::HeartbeatError { error, needs_rejoin })) => {
                        if needs_rejoin {
                            tracing::warn!("Rebalance required: {}", error);
                            // In a production system, this would trigger a rejoin
                        } else {
                            tracing::error!("Heartbeat error: {}", error);
                        }
                    }
                    Some(Err(e)) => {
                        tracing::error!("Heartbeat response error: {}", e);
                        break;
                    }
                    None => {
                        tracing::error!("Connection closed during heartbeat");
                        break;
                    }
                    _ => {
                        tracing::error!("Unexpected heartbeat response");
                    }
                }
            }
        });

        self.heartbeat_task = Some(task);
    }

    /// Fetch committed offset for a partition from the broker
    async fn fetch_committed_offset(&mut self, topic: &str, partition: u32) -> Result<u64> {
        let group_id = self.group_id.as_ref().unwrap().clone();

        let request = Request::FetchOffset {
            group_id,
            topic: topic.to_string(),
            partition,
        };

        let mut framed = self.framed.lock().await;
        framed
            .send(request)
            .await
            .map_err(|e| GaffaError::Connection(e.to_string()))?;

        let response = framed
            .next()
            .await
            .ok_or_else(|| GaffaError::Connection("Connection closed".to_string()))?
            .map_err(|e| GaffaError::Connection(e.to_string()))?;

        match response {
            Response::FetchOffsetSuccess { offset, .. } => Ok(offset),
            Response::FetchOffsetError { error } => Err(GaffaError::Protocol(error)),
            _ => Err(GaffaError::Protocol("Unexpected response".to_string())),
        }
    }

    /// Commit current offsets to the broker (for consumer groups)
    pub async fn commit_offsets(&mut self) -> Result<()> {
        if self.group_id.is_none() {
            return Err(GaffaError::Protocol("Not in a consumer group".to_string()));
        }

        let group_id = self.group_id.as_ref().unwrap().clone();

        for ((topic, partition), offset) in &self.offsets {
            let request = Request::CommitOffset {
                group_id: group_id.clone(),
                topic: topic.clone(),
                partition: *partition,
                offset: *offset,
            };

            let mut framed = self.framed.lock().await;
            framed
                .send(request)
                .await
                .map_err(|e| GaffaError::Connection(e.to_string()))?;

            let response = framed
                .next()
                .await
                .ok_or_else(|| GaffaError::Connection("Connection closed".to_string()))?
                .map_err(|e| GaffaError::Connection(e.to_string()))?;

            drop(framed); // Release lock for next iteration

            match response {
                Response::CommitOffsetSuccess { .. } => {
                    tracing::debug!("Committed offset {} for {}:{}", offset, topic, partition);
                }
                Response::CommitOffsetError { error } => {
                    return Err(GaffaError::Protocol(error));
                }
                _ => return Err(GaffaError::Protocol("Unexpected response".to_string())),
            }
        }

        Ok(())
    }

    /// Subscribe to topics (all partitions)
    pub async fn subscribe(&mut self, topics: Vec<&str>) -> Result<()> {
        for topic in topics {
            // Get metadata for the topic
            let metadata = self.get_topic_metadata(topic).await?;

            // Subscribe to all partitions
            let partition_ids: Vec<u32> = (0..metadata.partitions.len() as u32).collect();
            self.subscriptions
                .insert(topic.to_string(), partition_ids.clone());

            // Initialize offsets to 0 for all partitions
            for partition_id in partition_ids {
                self.offsets.insert((topic.to_string(), partition_id), 0);
            }

            tracing::info!(
                "Subscribed to topic '{}' with {} partitions",
                topic,
                metadata.partitions.len()
            );
        }

        Ok(())
    }

    /// Poll for messages from all subscribed partitions
    ///
    /// Returns records from all subscribed partitions, round-robin fashion.
    /// Updates internal offset tracking automatically.
    pub async fn poll(&mut self, max_messages: u32) -> Result<Vec<Record>> {
        let mut all_records = Vec::new();

        // Clone subscriptions to avoid borrow checker issues
        let subscriptions: Vec<(String, Vec<u32>)> = self
            .subscriptions
            .iter()
            .map(|(topic, partitions)| (topic.clone(), partitions.clone()))
            .collect();

        // Iterate through all subscribed topic-partition pairs
        for (topic, partitions) in subscriptions {
            for partition in partitions {
                let current_offset = *self
                    .offsets
                    .get(&(topic.clone(), partition))
                    .unwrap_or(&0);

                // Fetch from this partition
                let records = self
                    .fetch(&topic, partition, current_offset, max_messages)
                    .await?;

                // Update offset
                if let Some(last_record) = records.last() {
                    self.offsets
                        .insert((topic.clone(), partition), last_record.offset + 1);
                }

                all_records.extend(records);
            }
        }

        Ok(all_records)
    }

    /// Fetch messages from a specific topic partition (low-level API)
    pub async fn fetch(
        &mut self,
        topic: &str,
        partition: u32,
        offset: u64,
        max_messages: u32,
    ) -> Result<Vec<Record>> {
        let request = Request::Fetch {
            topic: topic.to_string(),
            partition,
            offset,
            max_messages,
        };

        let mut framed = self.framed.lock().await;
        framed
            .send(request)
            .await
            .map_err(|e| GaffaError::Connection(e.to_string()))?;

        let response = framed
            .next()
            .await
            .ok_or_else(|| GaffaError::Connection("Connection closed".to_string()))?
            .map_err(|e| GaffaError::Connection(e.to_string()))?;

        match response {
            Response::FetchSuccess { records, .. } => {
                tracing::debug!(
                    "Fetched {} messages from {}:{}",
                    records.len(),
                    topic,
                    partition
                );
                Ok(records)
            }
            Response::FetchError { error } => Err(GaffaError::Protocol(error)),
            _ => Err(GaffaError::Protocol("Unexpected response".to_string())),
        }
    }

    /// Manually commit offset for a topic partition
    pub fn commit_offset(&mut self, topic: &str, partition: u32, offset: u64) {
        self.offsets.insert((topic.to_string(), partition), offset);
        tracing::debug!(
            "Committed offset {} for {}:{}",
            offset,
            topic,
            partition
        );
    }

    /// Get current offset for a topic partition
    pub fn get_offset(&self, topic: &str, partition: u32) -> Option<u64> {
        self.offsets.get(&(topic.to_string(), partition)).copied()
    }

    /// Seek to a specific offset for a topic partition
    pub fn seek(&mut self, topic: &str, partition: u32, offset: u64) {
        self.offsets.insert((topic.to_string(), partition), offset);
        tracing::debug!("Seeked to offset {} for {}:{}", offset, topic, partition);
    }

    /// Get metadata for a specific topic
    async fn get_topic_metadata(&mut self, topic: &str) -> Result<TopicMetadata> {
        let request = Request::GetMetadata {
            topics: vec![topic.to_string()],
        };

        let mut framed = self.framed.lock().await;
        framed
            .send(request)
            .await
            .map_err(|e| GaffaError::Connection(e.to_string()))?;

        let response = framed
            .next()
            .await
            .ok_or_else(|| GaffaError::Connection("Connection closed".to_string()))?
            .map_err(|e| GaffaError::Connection(e.to_string()))?;

        match response {
            Response::Metadata { mut topics } => {
                if topics.is_empty() {
                    return Err(GaffaError::TopicNotFound(topic.to_string()));
                }
                Ok(topics.remove(0))
            }
            Response::MetadataError { error } => Err(GaffaError::Protocol(error)),
            _ => Err(GaffaError::Protocol("Unexpected response".to_string())),
        }
    }

    /// Poll for messages from assigned partitions (consumer group mode)
    ///
    /// This is used when the consumer is part of a consumer group.
    /// It automatically commits offsets if auto-commit is enabled.
    pub async fn poll_group(&mut self, max_messages: u32) -> Result<Vec<Record>> {
        if self.assignments.is_empty() {
            return Ok(Vec::new());
        }

        let mut all_records = Vec::new();

        // Fetch from each assigned partition
        for assignment in self.assignments.clone() {
            let current_offset = *self
                .offsets
                .get(&(assignment.topic.clone(), assignment.partition))
                .unwrap_or(&0);

            let records = self
                .fetch(&assignment.topic, assignment.partition, current_offset, max_messages)
                .await?;

            // Update offset
            if let Some(last_record) = records.last() {
                self.offsets.insert(
                    (assignment.topic.clone(), assignment.partition),
                    last_record.offset + 1,
                );
            }

            all_records.extend(records);
        }

        // Auto-commit if enabled
        if self.auto_commit && !all_records.is_empty() {
            if let Err(e) = self.commit_offsets().await {
                tracing::warn!("Auto-commit failed: {}", e);
            }
        }

        Ok(all_records)
    }
}

impl Drop for Consumer {
    fn drop(&mut self) {
        // Abort heartbeat task on drop
        if let Some(task) = self.heartbeat_task.take() {
            task.abort();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Integration tests would require a running broker
    // These are placeholder unit tests

    #[test]
    fn test_consumer_creation() {
        // This is a placeholder test
        // Real tests would need a mock or running broker
        assert!(true);
    }
}
