use common::{GaffaError, Result};
use futures::{SinkExt, StreamExt};
use protocol::{ClientCodec, Record, Request, Response, TopicMetadata};
use tokio::net::TcpStream;
use tokio_util::codec::Framed;
use std::collections::HashMap;

/// A consumer client for reading messages from the broker
pub struct Consumer {
    framed: Framed<TcpStream, ClientCodec>,
    subscriptions: HashMap<String, Vec<u32>>, // topic -> partitions
    offsets: HashMap<(String, u32), u64>,     // (topic, partition) -> offset
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
            framed,
            subscriptions: HashMap::new(),
            offsets: HashMap::new(),
        })
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

        self.framed
            .send(request)
            .await
            .map_err(|e| GaffaError::Connection(e.to_string()))?;

        let response = self
            .framed
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

        self.framed
            .send(request)
            .await
            .map_err(|e| GaffaError::Connection(e.to_string()))?;

        let response = self
            .framed
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
