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
}
