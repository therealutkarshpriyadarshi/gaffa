use common::{GaffaError, Result};
use futures::{SinkExt, StreamExt};
use protocol::{ClientCodec, Message, Request, Response, TopicMetadata};
use tokio::net::TcpStream;
use tokio_util::codec::Framed;
use std::collections::HashMap;
use std::sync::Arc;
use crate::partitioner::{Partitioner, RoundRobinPartitioner};

/// A producer client for sending messages to the broker
pub struct Producer {
    framed: Framed<TcpStream, ClientCodec>,
    partitioner: Arc<dyn Partitioner>,
    metadata_cache: HashMap<String, u32>, // topic -> partition count
}

impl Producer {
    /// Connect to a broker with the default round-robin partitioner
    pub async fn connect(addr: &str) -> Result<Self> {
        Self::connect_with_partitioner(addr, Arc::new(RoundRobinPartitioner::new())).await
    }

    /// Connect to a broker with a custom partitioner
    pub async fn connect_with_partitioner(
        addr: &str,
        partitioner: Arc<dyn Partitioner>,
    ) -> Result<Self> {
        let stream = TcpStream::connect(addr)
            .await
            .map_err(|e| GaffaError::Connection(e.to_string()))?;

        let framed = Framed::new(stream, ClientCodec);

        tracing::info!("Producer connected to {}", addr);

        Ok(Self {
            framed,
            partitioner,
            metadata_cache: HashMap::new(),
        })
    }

    /// Create a new topic
    pub async fn create_topic(&mut self, name: &str, partitions: u32) -> Result<()> {
        let request = Request::CreateTopic {
            name: name.to_string(),
            partitions,
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
            Response::CreateTopicSuccess { .. } => {
                tracing::debug!("Topic '{}' created successfully", name);
                Ok(())
            }
            Response::CreateTopicError { error } => Err(GaffaError::Protocol(error)),
            _ => Err(GaffaError::Protocol("Unexpected response".to_string())),
        }
    }

    /// Send messages to a topic partition (manual partition specification)
    pub async fn send(
        &mut self,
        topic: &str,
        partition: u32,
        messages: Vec<Message>,
    ) -> Result<u64> {
        let request = Request::Produce {
            topic: topic.to_string(),
            partition,
            messages,
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
            Response::ProduceSuccess { base_offset, .. } => {
                tracing::debug!(
                    "Messages sent successfully to {}:{}, base_offset={}",
                    topic,
                    partition,
                    base_offset
                );
                Ok(base_offset)
            }
            Response::ProduceError { error } => Err(GaffaError::Protocol(error)),
            _ => Err(GaffaError::Protocol("Unexpected response".to_string())),
        }
    }

    /// Send a single message with automatic partition selection
    pub async fn send_auto(&mut self, topic: &str, message: Message) -> Result<u64> {
        self.send_batch_auto(topic, vec![message]).await
    }

    /// Send multiple messages with automatic partition selection and batching
    ///
    /// Messages are grouped by partition using the configured partitioner,
    /// then sent in parallel batches to each partition.
    pub async fn send_batch_auto(&mut self, topic: &str, messages: Vec<Message>) -> Result<u64> {
        // Refresh metadata if not cached
        if !self.metadata_cache.contains_key(topic) {
            self.refresh_metadata(topic).await?;
        }

        let num_partitions = *self
            .metadata_cache
            .get(topic)
            .ok_or_else(|| GaffaError::TopicNotFound(topic.to_string()))?;

        // Group messages by partition
        let mut partition_batches: HashMap<u32, Vec<Message>> = HashMap::new();
        for message in messages {
            let partition = self
                .partitioner
                .partition(message.key.as_deref(), num_partitions);
            partition_batches
                .entry(partition)
                .or_insert_with(Vec::new)
                .push(message);
        }

        // Send to each partition
        let mut base_offset = 0u64;
        for (partition, batch) in partition_batches {
            let offset = self.send(topic, partition, batch).await?;
            if base_offset == 0 {
                base_offset = offset;
            }
        }

        Ok(base_offset)
    }

    /// Refresh partition metadata for a topic
    pub async fn refresh_metadata(&mut self, topic: &str) -> Result<()> {
        let request = Request::GetPartitions {
            topic: topic.to_string(),
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
            Response::Partitions { topic, count } => {
                self.metadata_cache.insert(topic.clone(), count);
                tracing::debug!("Cached metadata for topic '{}': {} partitions", topic, count);
                Ok(())
            }
            Response::PartitionsError { error } => Err(GaffaError::Protocol(error)),
            _ => Err(GaffaError::Protocol("Unexpected response".to_string())),
        }
    }

    /// Get all topic metadata
    pub async fn get_metadata(&mut self, topics: Vec<String>) -> Result<Vec<TopicMetadata>> {
        let request = Request::GetMetadata { topics };

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
            Response::Metadata { topics } => {
                // Update cache
                for topic in &topics {
                    self.metadata_cache
                        .insert(topic.name.clone(), topic.partitions.len() as u32);
                }
                Ok(topics)
            }
            Response::MetadataError { error } => Err(GaffaError::Protocol(error)),
            _ => Err(GaffaError::Protocol("Unexpected response".to_string())),
        }
    }

    /// List all topics
    pub async fn list_topics(&mut self) -> Result<Vec<String>> {
        let request = Request::ListTopics;

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
            Response::Topics { topics } => Ok(topics),
            Response::TopicsError { error } => Err(GaffaError::Protocol(error)),
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
    fn test_producer_creation() {
        // This is a placeholder test
        // Real tests would need a mock or running broker
        assert!(true);
    }
}
