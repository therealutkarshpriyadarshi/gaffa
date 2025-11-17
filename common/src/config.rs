use serde::{Deserialize, Serialize};

/// Configuration for the broker server
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BrokerConfig {
    /// Host to bind to
    pub host: String,
    /// Port to listen on
    pub port: u16,
    /// Data directory for persistent storage
    pub data_dir: String,
    /// Unique broker ID in the cluster (Phase 5)
    pub broker_id: u32,
    /// Replication factor for new topics (Phase 5)
    pub replication_factor: u32,
    /// Maximum lag for ISR in messages (Phase 5)
    pub max_isr_lag: u64,
}

impl Default for BrokerConfig {
    fn default() -> Self {
        Self {
            host: "127.0.0.1".to_string(),
            port: 9092,
            data_dir: "/tmp/gaffa".to_string(),
            broker_id: 0,
            replication_factor: 1,
            max_isr_lag: 1000,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = BrokerConfig::default();
        assert_eq!(config.host, "127.0.0.1");
        assert_eq!(config.port, 9092);
        assert_eq!(config.data_dir, "/tmp/gaffa");
    }
}
