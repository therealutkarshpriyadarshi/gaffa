/// Test utilities and helper wrappers for integration, load, and chaos tests
///
/// This module provides convenience wrappers around the Gaffa client API
/// to simplify test code and provide a higher-level interface.

use client::{Consumer as ClientConsumer, Producer as ClientProducer};
use common::Result;
use protocol::{Message, Record};
use std::time::Duration;
use tokio::time::sleep;

#[allow(dead_code)]
pub enum CompressionType {
    None,
    Gzip,
    Snappy,
    Lz4,
}

#[allow(dead_code)]
pub enum PartitionStrategy {
    RoundRobin,
    KeyHash,
    Sticky,
}

/// Simplified Producer wrapper for tests
pub struct Producer {
    inner: ClientProducer,
    default_partition: u32,
    compression: CompressionType,
}

impl Producer {
    /// Create a new producer connected to brokers
    pub async fn new(brokers: Vec<String>) -> Result<Self> {
        let addr = &brokers[0]; // Use first broker
        let inner = ClientProducer::connect(addr).await?;

        Ok(Self {
            inner,
            default_partition: 0,
            compression: CompressionType::None,
        })
    }

    /// Produce a single message to a topic
    pub async fn produce(&mut self, topic: &str, key: Option<&[u8]>, value: &[u8]) -> Result<()> {
        let mut msg = Message::new(value.to_vec());
        if let Some(k) = key {
            msg = msg.with_key(k.to_vec());
        }

        // Auto-create topic if needed (with 1 partition for simplicity)
        if self.inner.create_topic(topic, 1).await.is_err() {
            // Topic might already exist, continue
        }

        self.inner.send(topic, self.default_partition, vec![msg]).await?;
        Ok(())
    }

    /// Set compression type
    pub fn set_compression(&mut self, compression: CompressionType) {
        self.compression = compression;
    }

    /// Set partition strategy (placeholder - not fully implemented)
    pub fn set_partition_strategy(&mut self, _strategy: PartitionStrategy) {
        // In a real implementation, this would affect partition selection
        // For tests, we just use default partition
    }
}

/// Simplified Consumer wrapper for tests
pub struct Consumer {
    inner: ClientConsumer,
    topics: Vec<String>,
    group_id: String,
    current_offsets: std::collections::HashMap<(String, u32), u64>,
}

impl Consumer {
    /// Create a new consumer in a consumer group
    pub async fn new(
        brokers: Vec<String>,
        group_id: &str,
        topics: Vec<String>,
    ) -> Result<Self> {
        let addr = &brokers[0];
        let mut inner = ClientConsumer::connect(addr).await?
            .with_group_id(group_id);

        // Join the group with topics
        let topic_refs: Vec<&str> = topics.iter().map(|s| s.as_str()).collect();
        if inner.join_group(topic_refs).await.is_err() {
            // Group join might fail if topic doesn't exist yet, continue
        }

        Ok(Self {
            inner,
            topics,
            group_id: group_id.to_string(),
            current_offsets: std::collections::HashMap::new(),
        })
    }

    /// Poll for next message from subscribed topics
    pub async fn poll(&mut self, timeout: Duration) -> Result<Option<Record>> {
        let start = std::time::Instant::now();

        // Try each topic/partition combination
        for topic in &self.topics.clone() {
            // Try partition 0 (most tests use single partition)
            let key = (topic.clone(), 0);
            let offset = *self.current_offsets.get(&key).unwrap_or(&0);

            match self.inner.fetch(topic, 0, offset, 1).await {
                Ok(records) if !records.is_empty() => {
                    let record = records[0].clone();
                    self.current_offsets.insert(key, record.offset + 1);
                    return Ok(Some(record));
                }
                _ => {
                    // No records or error, try next topic
                }
            }

            if start.elapsed() >= timeout {
                break;
            }
        }

        // Small sleep to avoid busy-waiting
        sleep(Duration::from_millis(10)).await;
        Ok(None)
    }

    /// Commit current offsets
    pub async fn commit_sync(&mut self) -> Result<()> {
        for ((topic, partition), offset) in &self.current_offsets {
            if let Err(_) = self.inner.commit_offset(topic, *partition, *offset).await {
                // Commit might fail, continue
            }
        }
        Ok(())
    }
}

/// Helper to wait for broker startup
pub async fn wait_for_broker(addr: &str, timeout_secs: u64) -> bool {
    let start = std::time::Instant::now();
    while start.elapsed() < Duration::from_secs(timeout_secs) {
        if tokio::net::TcpStream::connect(addr).await.is_ok() {
            sleep(Duration::from_millis(100)).await; // Extra settling time
            return true;
        }
        sleep(Duration::from_millis(100)).await;
    }
    false
}

/// Helper to check if broker is responsive
pub async fn is_broker_alive(addr: &str) -> bool {
    tokio::net::TcpStream::connect(addr).await.is_ok()
}
