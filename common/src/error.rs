use thiserror::Error;

/// Common error types for the Gaffa message queue system
#[derive(Error, Debug)]
pub enum GaffaError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Serialization error: {0}")]
    Serialization(String),

    #[error("Topic not found: {0}")]
    TopicNotFound(String),

    #[error("Partition not found: topic={0}, partition={1}")]
    PartitionNotFound(String, u32),

    #[error("Invalid offset: {0}")]
    InvalidOffset(u64),

    #[error("Protocol error: {0}")]
    Protocol(String),

    #[error("Connection error: {0}")]
    Connection(String),

    #[error("Invalid message: {0}")]
    InvalidMessage(String),
}

/// Result type alias for Gaffa operations
pub type Result<T> = std::result::Result<T, GaffaError>;
