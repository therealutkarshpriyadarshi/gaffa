use common::{GaffaError, Result};
use dashmap::DashMap;
use protocol::{Message, Record};
use std::sync::Arc;

use crate::partition::Partition;

/// A topic with multiple partitions
#[derive(Debug)]
pub struct Topic {
    /// Topic name
    name: String,
    /// Partitions for this topic
    partitions: Vec<Arc<Partition>>,
}

impl Topic {
    /// Create a new topic with the specified number of partitions
    pub fn new(name: String, num_partitions: u32) -> Self {
        let partitions = (0..num_partitions)
            .map(|i| Arc::new(Partition::new(name.clone(), i)))
            .collect();

        Self { name, partitions }
    }

    /// Get the name of this topic
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Get the number of partitions
    pub fn num_partitions(&self) -> u32 {
        self.partitions.len() as u32
    }

    /// Get a partition by ID
    pub fn get_partition(&self, partition_id: u32) -> Result<Arc<Partition>> {
        self.partitions
            .get(partition_id as usize)
            .cloned()
            .ok_or_else(|| GaffaError::PartitionNotFound(self.name.clone(), partition_id))
    }

    /// Append messages to a specific partition
    pub async fn append(&self, partition_id: u32, messages: Vec<Message>) -> Result<u64> {
        let partition = self.get_partition(partition_id)?;
        partition.append(messages).await
    }

    /// Fetch messages from a specific partition
    pub async fn fetch(
        &self,
        partition_id: u32,
        offset: u64,
        max_messages: u32,
    ) -> Result<Vec<Record>> {
        let partition = self.get_partition(partition_id)?;
        partition.fetch(offset, max_messages).await
    }
}

/// Manages all topics in the broker
#[derive(Debug, Clone)]
pub struct TopicManager {
    /// Map of topic name to Topic
    topics: Arc<DashMap<String, Arc<Topic>>>,
}

impl TopicManager {
    /// Create a new topic manager
    pub fn new() -> Self {
        Self {
            topics: Arc::new(DashMap::new()),
        }
    }

    /// Create a new topic
    pub fn create_topic(&self, name: String, num_partitions: u32) -> Result<Arc<Topic>> {
        if self.topics.contains_key(&name) {
            return Err(GaffaError::InvalidMessage(format!(
                "Topic '{}' already exists",
                name
            )));
        }

        let topic = Arc::new(Topic::new(name.clone(), num_partitions));
        self.topics.insert(name.clone(), topic.clone());

        tracing::info!(
            topic = %name,
            partitions = num_partitions,
            "Created topic"
        );

        Ok(topic)
    }

    /// Get a topic by name
    pub fn get_topic(&self, name: &str) -> Result<Arc<Topic>> {
        self.topics
            .get(name)
            .map(|entry| entry.value().clone())
            .ok_or_else(|| GaffaError::TopicNotFound(name.to_string()))
    }

    /// List all topic names
    pub fn list_topics(&self) -> Vec<String> {
        self.topics.iter().map(|entry| entry.key().clone()).collect()
    }

    /// Get the number of topics
    pub fn topic_count(&self) -> usize {
        self.topics.len()
    }
}

impl Default for TopicManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_topic_creation() {
        let topic = Topic::new("test-topic".to_string(), 3);
        assert_eq!(topic.name(), "test-topic");
        assert_eq!(topic.num_partitions(), 3);
    }

    #[test]
    fn test_topic_get_partition() {
        let topic = Topic::new("test-topic".to_string(), 3);

        // Valid partition
        let partition = topic.get_partition(0);
        assert!(partition.is_ok());
        assert_eq!(partition.unwrap().partition_id(), 0);

        // Invalid partition
        let partition = topic.get_partition(5);
        assert!(partition.is_err());
    }

    #[tokio::test]
    async fn test_topic_append_and_fetch() {
        let topic = Topic::new("test-topic".to_string(), 2);

        // Append to partition 0
        let messages = vec![
            Message::new(b"msg1".to_vec()),
            Message::new(b"msg2".to_vec()),
        ];
        let offset = topic.append(0, messages).await.unwrap();
        assert_eq!(offset, 0);

        // Fetch from partition 0
        let records = topic.fetch(0, 0, 10).await.unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].partition, 0);
    }

    #[test]
    fn test_topic_manager_create() {
        let manager = TopicManager::new();

        // Create topic
        let result = manager.create_topic("test-topic".to_string(), 3);
        assert!(result.is_ok());
        let topic = result.unwrap();
        assert_eq!(topic.name(), "test-topic");
        assert_eq!(topic.num_partitions(), 3);

        // Try to create duplicate
        let result = manager.create_topic("test-topic".to_string(), 3);
        assert!(result.is_err());
    }

    #[test]
    fn test_topic_manager_get() {
        let manager = TopicManager::new();

        manager.create_topic("test-topic".to_string(), 3).unwrap();

        // Get existing topic
        let topic = manager.get_topic("test-topic");
        assert!(topic.is_ok());
        assert_eq!(topic.unwrap().name(), "test-topic");

        // Get non-existent topic
        let topic = manager.get_topic("non-existent");
        assert!(topic.is_err());
    }

    #[test]
    fn test_topic_manager_list() {
        let manager = TopicManager::new();

        assert_eq!(manager.list_topics().len(), 0);

        manager.create_topic("topic1".to_string(), 1).unwrap();
        manager.create_topic("topic2".to_string(), 2).unwrap();

        let topics = manager.list_topics();
        assert_eq!(topics.len(), 2);
        assert!(topics.contains(&"topic1".to_string()));
        assert!(topics.contains(&"topic2".to_string()));
    }

    #[test]
    fn test_topic_manager_topic_count() {
        let manager = TopicManager::new();

        assert_eq!(manager.topic_count(), 0);

        manager.create_topic("topic1".to_string(), 1).unwrap();
        assert_eq!(manager.topic_count(), 1);

        manager.create_topic("topic2".to_string(), 2).unwrap();
        assert_eq!(manager.topic_count(), 2);
    }
}
