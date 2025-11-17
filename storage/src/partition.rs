use common::{GaffaError, Result};
use protocol::{Message, Record};
use std::sync::Arc;
use tokio::sync::RwLock;

/// In-memory storage for a single partition
/// In Phase 1, this stores messages in memory as a simple vector
#[derive(Debug)]
pub struct Partition {
    /// Partition number
    partition_id: u32,
    /// Topic name
    topic_name: String,
    /// In-memory message storage
    messages: Arc<RwLock<Vec<Record>>>,
}

impl Partition {
    /// Create a new partition
    pub fn new(topic_name: String, partition_id: u32) -> Self {
        Self {
            partition_id,
            topic_name,
            messages: Arc::new(RwLock::new(Vec::new())),
        }
    }

    /// Append messages to this partition
    /// Returns the base offset where messages were written
    pub async fn append(&self, messages: Vec<Message>) -> Result<u64> {
        if messages.is_empty() {
            return Err(GaffaError::InvalidMessage("No messages to append".to_string()));
        }

        let mut storage = self.messages.write().await;
        let base_offset = storage.len() as u64;

        for (i, message) in messages.into_iter().enumerate() {
            let record = Record {
                topic: self.topic_name.clone(),
                partition: self.partition_id,
                offset: base_offset + i as u64,
                message,
            };
            storage.push(record);
        }

        tracing::debug!(
            topic = %self.topic_name,
            partition = self.partition_id,
            base_offset = base_offset,
            count = storage.len() - base_offset as usize,
            "Appended messages to partition"
        );

        Ok(base_offset)
    }

    /// Fetch messages starting from the given offset
    pub async fn fetch(&self, offset: u64, max_messages: u32) -> Result<Vec<Record>> {
        let storage = self.messages.read().await;

        // Validate offset
        if offset > storage.len() as u64 {
            return Err(GaffaError::InvalidOffset(offset));
        }

        let start = offset as usize;
        let end = std::cmp::min(start + max_messages as usize, storage.len());

        let records = storage[start..end].to_vec();

        tracing::debug!(
            topic = %self.topic_name,
            partition = self.partition_id,
            offset = offset,
            count = records.len(),
            "Fetched messages from partition"
        );

        Ok(records)
    }

    /// Get the next available offset (number of messages)
    pub async fn next_offset(&self) -> u64 {
        let storage = self.messages.read().await;
        storage.len() as u64
    }

    /// Get partition ID
    pub fn partition_id(&self) -> u32 {
        self.partition_id
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_partition_append_and_fetch() {
        let partition = Partition::new("test-topic".to_string(), 0);

        // Append some messages
        let messages = vec![
            Message::new(b"message1".to_vec()),
            Message::new(b"message2".to_vec()),
            Message::new(b"message3".to_vec()),
        ];

        let base_offset = partition.append(messages).await.unwrap();
        assert_eq!(base_offset, 0);

        // Fetch all messages
        let records = partition.fetch(0, 10).await.unwrap();
        assert_eq!(records.len(), 3);
        assert_eq!(records[0].offset, 0);
        assert_eq!(records[1].offset, 1);
        assert_eq!(records[2].offset, 2);
        assert_eq!(records[0].message.value, b"message1");
    }

    #[tokio::test]
    async fn test_partition_append_multiple_batches() {
        let partition = Partition::new("test-topic".to_string(), 0);

        // First batch
        let batch1 = vec![
            Message::new(b"msg1".to_vec()),
            Message::new(b"msg2".to_vec()),
        ];
        let offset1 = partition.append(batch1).await.unwrap();
        assert_eq!(offset1, 0);

        // Second batch
        let batch2 = vec![
            Message::new(b"msg3".to_vec()),
        ];
        let offset2 = partition.append(batch2).await.unwrap();
        assert_eq!(offset2, 2);

        // Fetch from middle
        let records = partition.fetch(1, 2).await.unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].offset, 1);
        assert_eq!(records[1].offset, 2);
    }

    #[tokio::test]
    async fn test_partition_fetch_with_limit() {
        let partition = Partition::new("test-topic".to_string(), 0);

        let messages = vec![
            Message::new(b"msg1".to_vec()),
            Message::new(b"msg2".to_vec()),
            Message::new(b"msg3".to_vec()),
            Message::new(b"msg4".to_vec()),
            Message::new(b"msg5".to_vec()),
        ];

        partition.append(messages).await.unwrap();

        // Fetch with limit
        let records = partition.fetch(1, 2).await.unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].offset, 1);
        assert_eq!(records[1].offset, 2);
    }

    #[tokio::test]
    async fn test_partition_invalid_offset() {
        let partition = Partition::new("test-topic".to_string(), 0);

        let messages = vec![Message::new(b"msg1".to_vec())];
        partition.append(messages).await.unwrap();

        // Try to fetch from invalid offset
        let result = partition.fetch(10, 1).await;
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), GaffaError::InvalidOffset(_)));
    }

    #[tokio::test]
    async fn test_partition_next_offset() {
        let partition = Partition::new("test-topic".to_string(), 0);

        assert_eq!(partition.next_offset().await, 0);

        let messages = vec![
            Message::new(b"msg1".to_vec()),
            Message::new(b"msg2".to_vec()),
        ];
        partition.append(messages).await.unwrap();

        assert_eq!(partition.next_offset().await, 2);
    }

    #[tokio::test]
    async fn test_partition_empty_messages() {
        let partition = Partition::new("test-topic".to_string(), 0);

        let result = partition.append(vec![]).await;
        assert!(result.is_err());
    }
}
