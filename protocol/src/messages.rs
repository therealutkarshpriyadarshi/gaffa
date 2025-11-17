use serde::{Deserialize, Serialize};

/// Represents a message to be stored in the queue
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Message {
    /// Optional message key for partitioning
    pub key: Option<Vec<u8>>,
    /// Message payload
    pub value: Vec<u8>,
    /// Timestamp when message was created (milliseconds since epoch)
    pub timestamp: u64,
}

impl Message {
    /// Create a new message with the given value
    pub fn new(value: Vec<u8>) -> Self {
        Self {
            key: None,
            value,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
        }
    }

    /// Set the message key
    pub fn with_key(mut self, key: Vec<u8>) -> Self {
        self.key = Some(key);
        self
    }

    /// Set the timestamp
    pub fn with_timestamp(mut self, timestamp: u64) -> Self {
        self.timestamp = timestamp;
        self
    }
}

/// A stored record with offset and partition information
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Record {
    /// Topic name
    pub topic: String,
    /// Partition number
    pub partition: u32,
    /// Offset within the partition
    pub offset: u64,
    /// The actual message
    pub message: Message,
}

/// Metadata about a single partition
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PartitionMetadata {
    /// Partition ID
    pub id: u32,
    /// Leader broker
    pub leader: u32,
    /// Replica brokers
    pub replicas: Vec<u32>,
    /// In-Sync Replicas
    pub isr: Vec<u32>,
    /// High watermark - highest committed offset
    pub high_watermark: u64,
}

impl PartitionMetadata {
    /// Create new partition metadata (single-broker mode)
    pub fn new(id: u32) -> Self {
        Self {
            id,
            leader: 0, // Single broker ID
            replicas: vec![0],
            isr: vec![0],
            high_watermark: 0,
        }
    }

    /// Create partition metadata with replication
    pub fn with_replication(id: u32, leader: u32, replicas: Vec<u32>, isr: Vec<u32>) -> Self {
        Self {
            id,
            leader,
            replicas,
            isr,
            high_watermark: 0,
        }
    }
}

/// Metadata about a topic
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TopicMetadata {
    /// Topic name
    pub name: String,
    /// List of partitions
    pub partitions: Vec<PartitionMetadata>,
}

impl TopicMetadata {
    /// Create new topic metadata
    pub fn new(name: String, partition_count: u32) -> Self {
        let partitions = (0..partition_count)
            .map(PartitionMetadata::new)
            .collect();
        Self { name, partitions }
    }
}

/// Partition assignment for a consumer group member
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PartitionAssignment {
    /// Topic name
    pub topic: String,
    /// Partition ID
    pub partition: u32,
}

impl PartitionAssignment {
    /// Create a new partition assignment
    pub fn new(topic: String, partition: u32) -> Self {
        Self { topic, partition }
    }
}

/// Request types that clients can send to the broker
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Request {
    /// Create a new topic
    CreateTopic {
        name: String,
        partitions: u32,
    },
    /// Produce messages to a topic
    Produce {
        topic: String,
        partition: u32,
        messages: Vec<Message>,
    },
    /// Fetch messages from a topic partition starting at an offset
    Fetch {
        topic: String,
        partition: u32,
        offset: u64,
        max_messages: u32,
    },
    /// Get metadata for specific topics (empty = all topics)
    GetMetadata {
        topics: Vec<String>,
    },
    /// Get partition count for a topic
    GetPartitions {
        topic: String,
    },
    /// List all topics
    ListTopics,
    /// Join a consumer group
    JoinGroup {
        group_id: String,
        member_id: Option<String>, // None for new members
        topics: Vec<String>,
    },
    /// Leave a consumer group
    LeaveGroup {
        group_id: String,
        member_id: String,
    },
    /// Send heartbeat to maintain group membership
    Heartbeat {
        group_id: String,
        member_id: String,
    },
    /// Commit offset for a consumer group
    CommitOffset {
        group_id: String,
        topic: String,
        partition: u32,
        offset: u64,
    },
    /// Fetch committed offset for a consumer group
    FetchOffset {
        group_id: String,
        topic: String,
        partition: u32,
    },
    /// Register a broker in the cluster (Phase 5)
    RegisterBroker {
        broker_id: u32,
        host: String,
        port: u16,
    },
    /// Get cluster metadata (brokers and partition assignments)
    GetClusterMetadata,
    /// Internal: Replication fetch from leader (broker-to-broker)
    ReplicationFetch {
        broker_id: u32,
        topic: String,
        partition: u32,
        offset: u64,
    },
}

/// Response types that the broker sends back to clients
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Response {
    /// Topic created successfully
    CreateTopicSuccess {
        name: String,
        partitions: u32,
    },
    /// Topic creation failed
    CreateTopicError {
        error: String,
    },
    /// Messages produced successfully
    ProduceSuccess {
        topic: String,
        partition: u32,
        base_offset: u64,
        count: u32,
    },
    /// Produce failed
    ProduceError {
        error: String,
    },
    /// Fetched records
    FetchSuccess {
        topic: String,
        partition: u32,
        records: Vec<Record>,
    },
    /// Fetch failed
    FetchError {
        error: String,
    },
    /// Metadata response
    Metadata {
        topics: Vec<TopicMetadata>,
    },
    /// Metadata error
    MetadataError {
        error: String,
    },
    /// Partition count response
    Partitions {
        topic: String,
        count: u32,
    },
    /// Partitions error
    PartitionsError {
        error: String,
    },
    /// List of all topics
    Topics {
        topics: Vec<String>,
    },
    /// Topics list error
    TopicsError {
        error: String,
    },
    /// Join group success with member ID and partition assignments
    JoinGroupSuccess {
        group_id: String,
        member_id: String,
        assignments: Vec<PartitionAssignment>,
    },
    /// Join group error
    JoinGroupError {
        error: String,
    },
    /// Leave group success
    LeaveGroupSuccess {
        group_id: String,
    },
    /// Leave group error
    LeaveGroupError {
        error: String,
    },
    /// Heartbeat acknowledged
    HeartbeatSuccess,
    /// Heartbeat error (possibly needs rebalance)
    HeartbeatError {
        error: String,
        needs_rejoin: bool,
    },
    /// Offset committed successfully
    CommitOffsetSuccess {
        group_id: String,
        topic: String,
        partition: u32,
        offset: u64,
    },
    /// Commit offset error
    CommitOffsetError {
        error: String,
    },
    /// Fetched offset for consumer group
    FetchOffsetSuccess {
        group_id: String,
        topic: String,
        partition: u32,
        offset: u64,
    },
    /// Fetch offset error
    FetchOffsetError {
        error: String,
    },
    /// Broker registration success
    RegisterBrokerSuccess {
        broker_id: u32,
    },
    /// Broker registration error
    RegisterBrokerError {
        error: String,
    },
    /// Cluster metadata response
    ClusterMetadata {
        brokers: Vec<BrokerMetadata>,
        topic_partitions: Vec<TopicPartitionMetadata>,
    },
    /// Cluster metadata error
    ClusterMetadataError {
        error: String,
    },
    /// Replication fetch success
    ReplicationFetchSuccess {
        records: Vec<Record>,
        high_watermark: u64,
        leader_epoch: u32,
    },
    /// Replication fetch error
    ReplicationFetchError {
        error: String,
    },
}

/// Broker metadata for cluster view
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct BrokerMetadata {
    pub id: u32,
    pub host: String,
    pub port: u16,
}

/// Topic partition metadata with replication info
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TopicPartitionMetadata {
    pub topic: String,
    pub partition: u32,
    pub leader: u32,
    pub replicas: Vec<u32>,
    pub isr: Vec<u32>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_message_creation() {
        let msg = Message::new(b"test value".to_vec());
        assert_eq!(msg.value, b"test value");
        assert!(msg.key.is_none());
        assert!(msg.timestamp > 0);
    }

    #[test]
    fn test_message_with_key() {
        let msg = Message::new(b"test value".to_vec())
            .with_key(b"test key".to_vec());
        assert_eq!(msg.value, b"test value");
        assert_eq!(msg.key, Some(b"test key".to_vec()));
    }

    #[test]
    fn test_message_serialization() {
        let msg = Message::new(b"test".to_vec())
            .with_key(b"key".to_vec());
        let serialized = bincode::serialize(&msg).unwrap();
        let deserialized: Message = bincode::deserialize(&serialized).unwrap();
        assert_eq!(msg, deserialized);
    }

    #[test]
    fn test_record_serialization() {
        let record = Record {
            topic: "test-topic".to_string(),
            partition: 0,
            offset: 42,
            message: Message::new(b"test".to_vec()),
        };
        let serialized = bincode::serialize(&record).unwrap();
        let deserialized: Record = bincode::deserialize(&serialized).unwrap();
        assert_eq!(record, deserialized);
    }

    #[test]
    fn test_request_serialization() {
        let req = Request::CreateTopic {
            name: "test".to_string(),
            partitions: 3,
        };
        let serialized = bincode::serialize(&req).unwrap();
        let deserialized: Request = bincode::deserialize(&serialized).unwrap();
        matches!(deserialized, Request::CreateTopic { .. });
    }

    #[test]
    fn test_partition_metadata() {
        let pm = PartitionMetadata::new(0);
        assert_eq!(pm.id, 0);
        assert_eq!(pm.leader, 0);
        assert_eq!(pm.replicas, vec![0]);
    }

    #[test]
    fn test_topic_metadata() {
        let tm = TopicMetadata::new("test-topic".to_string(), 3);
        assert_eq!(tm.name, "test-topic");
        assert_eq!(tm.partitions.len(), 3);
        assert_eq!(tm.partitions[0].id, 0);
        assert_eq!(tm.partitions[1].id, 1);
        assert_eq!(tm.partitions[2].id, 2);
    }

    #[test]
    fn test_metadata_serialization() {
        let tm = TopicMetadata::new("test".to_string(), 2);
        let serialized = bincode::serialize(&tm).unwrap();
        let deserialized: TopicMetadata = bincode::deserialize(&serialized).unwrap();
        assert_eq!(tm, deserialized);
    }

    #[test]
    fn test_get_metadata_request() {
        let req = Request::GetMetadata {
            topics: vec!["topic1".to_string(), "topic2".to_string()],
        };
        let serialized = bincode::serialize(&req).unwrap();
        let deserialized: Request = bincode::deserialize(&serialized).unwrap();
        matches!(deserialized, Request::GetMetadata { .. });
    }

    #[test]
    fn test_list_topics_request() {
        let req = Request::ListTopics;
        let serialized = bincode::serialize(&req).unwrap();
        let _deserialized: Request = bincode::deserialize(&serialized).unwrap();
    }

    #[test]
    fn test_partition_assignment() {
        let assignment = PartitionAssignment::new("test-topic".to_string(), 3);
        assert_eq!(assignment.topic, "test-topic");
        assert_eq!(assignment.partition, 3);

        let serialized = bincode::serialize(&assignment).unwrap();
        let deserialized: PartitionAssignment = bincode::deserialize(&serialized).unwrap();
        assert_eq!(assignment, deserialized);
    }

    #[test]
    fn test_join_group_request() {
        let req = Request::JoinGroup {
            group_id: "test-group".to_string(),
            member_id: Some("member-1".to_string()),
            topics: vec!["topic1".to_string(), "topic2".to_string()],
        };
        let serialized = bincode::serialize(&req).unwrap();
        let _deserialized: Request = bincode::deserialize(&serialized).unwrap();
    }

    #[test]
    fn test_commit_offset_request() {
        let req = Request::CommitOffset {
            group_id: "test-group".to_string(),
            topic: "test-topic".to_string(),
            partition: 2,
            offset: 100,
        };
        let serialized = bincode::serialize(&req).unwrap();
        let _deserialized: Request = bincode::deserialize(&serialized).unwrap();
    }

    #[test]
    fn test_heartbeat_request() {
        let req = Request::Heartbeat {
            group_id: "test-group".to_string(),
            member_id: "member-1".to_string(),
        };
        let serialized = bincode::serialize(&req).unwrap();
        let _deserialized: Request = bincode::deserialize(&serialized).unwrap();
    }

    #[test]
    fn test_join_group_response() {
        let resp = Response::JoinGroupSuccess {
            group_id: "test-group".to_string(),
            member_id: "member-1".to_string(),
            assignments: vec![
                PartitionAssignment::new("topic1".to_string(), 0),
                PartitionAssignment::new("topic1".to_string(), 1),
            ],
        };
        let serialized = bincode::serialize(&resp).unwrap();
        let _deserialized: Response = bincode::deserialize(&serialized).unwrap();
    }
}
