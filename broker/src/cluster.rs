use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Unique identifier for a broker in the cluster
pub type BrokerId = u32;

/// Metadata about a broker in the cluster
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BrokerInfo {
    pub id: BrokerId,
    pub host: String,
    pub port: u16,
    #[serde(skip)]
    pub last_heartbeat: Option<Instant>,
}

impl BrokerInfo {
    pub fn new(id: BrokerId, host: String, port: u16) -> Self {
        Self {
            id,
            host,
            port,
            last_heartbeat: Some(Instant::now()),
        }
    }

    pub fn is_alive(&self, timeout: Duration) -> bool {
        match self.last_heartbeat {
            Some(last) => last.elapsed() < timeout,
            None => false,
        }
    }

    pub fn update_heartbeat(&mut self) {
        self.last_heartbeat = Some(Instant::now());
    }
}

/// Replica information for a partition
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReplicaInfo {
    pub broker_id: BrokerId,
    pub is_leader: bool,
    /// Last fetched offset for followers, current offset for leader
    pub offset: u64,
}

impl ReplicaInfo {
    pub fn new_leader(broker_id: BrokerId, offset: u64) -> Self {
        Self {
            broker_id,
            is_leader: true,
            offset,
        }
    }

    pub fn new_follower(broker_id: BrokerId, offset: u64) -> Self {
        Self {
            broker_id,
            is_leader: false,
            offset,
        }
    }
}

/// Partition replication state
#[derive(Debug, Clone)]
pub struct PartitionReplicaState {
    pub topic: String,
    pub partition: u32,
    pub leader: BrokerId,
    pub replicas: Vec<BrokerId>,
    /// In-Sync Replicas - replicas that are caught up with leader
    pub isr: HashSet<BrokerId>,
    /// Replica offsets
    pub replica_offsets: HashMap<BrokerId, u64>,
    /// High watermark - highest offset that has been replicated to all ISR members
    pub high_watermark: u64,
    /// Leader epoch - incremented on each leader election
    pub leader_epoch: u32,
}

impl PartitionReplicaState {
    pub fn new(
        topic: String,
        partition: u32,
        leader: BrokerId,
        replicas: Vec<BrokerId>,
    ) -> Self {
        let isr: HashSet<BrokerId> = replicas.iter().copied().collect();
        let replica_offsets: HashMap<BrokerId, u64> =
            replicas.iter().map(|&id| (id, 0)).collect();

        Self {
            topic,
            partition,
            leader,
            replicas,
            isr,
            replica_offsets,
            high_watermark: 0,
            leader_epoch: 0,
        }
    }

    /// Update follower's offset and ISR status
    pub fn update_replica_offset(&mut self, broker_id: BrokerId, offset: u64, lag_threshold: u64) {
        self.replica_offsets.insert(broker_id, offset);

        // Get leader offset
        let leader_offset = self.replica_offsets.get(&self.leader).copied().unwrap_or(0);

        // Update ISR based on lag
        if offset + lag_threshold >= leader_offset {
            self.isr.insert(broker_id);
        } else {
            self.isr.remove(&broker_id);
        }

        // Update high watermark (minimum offset across all ISR members)
        self.update_high_watermark();
    }

    /// Update high watermark to minimum offset across ISR
    fn update_high_watermark(&mut self) {
        if self.isr.is_empty() {
            return;
        }

        let min_offset = self
            .isr
            .iter()
            .filter_map(|&broker_id| self.replica_offsets.get(&broker_id))
            .min()
            .copied()
            .unwrap_or(0);

        self.high_watermark = min_offset;
    }

    /// Elect a new leader from ISR
    pub fn elect_new_leader(&mut self) -> Option<BrokerId> {
        // Try to elect from ISR first
        if let Some(&new_leader) = self.isr.iter().next() {
            self.leader = new_leader;
            self.leader_epoch += 1;
            tracing::info!(
                "Elected new leader {} for {}:{} (epoch {})",
                new_leader,
                self.topic,
                self.partition,
                self.leader_epoch
            );
            return Some(new_leader);
        }

        // If ISR is empty, try any replica
        if let Some(&new_leader) = self.replicas.first() {
            self.leader = new_leader;
            self.leader_epoch += 1;
            self.isr.insert(new_leader);
            tracing::warn!(
                "Elected new leader {} from replicas (ISR was empty) for {}:{} (epoch {})",
                new_leader,
                self.topic,
                self.partition,
                self.leader_epoch
            );
            return Some(new_leader);
        }

        None
    }

    /// Check if leader is in ISR
    pub fn is_leader_in_sync(&self) -> bool {
        self.isr.contains(&self.leader)
    }
}

/// Cluster metadata manager
#[derive(Clone)]
pub struct ClusterMetadata {
    /// Registered brokers
    brokers: Arc<DashMap<BrokerId, BrokerInfo>>,
    /// Partition replica states: (topic, partition) -> state
    partition_states: Arc<DashMap<(String, u32), PartitionReplicaState>>,
    /// This broker's ID
    broker_id: BrokerId,
    /// Broker heartbeat timeout
    broker_timeout: Duration,
    /// ISR lag threshold (messages)
    isr_lag_threshold: u64,
}

impl ClusterMetadata {
    pub fn new(broker_id: BrokerId, broker_timeout: Duration, isr_lag_threshold: u64) -> Self {
        Self {
            brokers: Arc::new(DashMap::new()),
            partition_states: Arc::new(DashMap::new()),
            broker_id,
            broker_timeout,
            isr_lag_threshold,
        }
    }

    /// Register a broker in the cluster
    pub fn register_broker(&self, broker: BrokerInfo) {
        tracing::info!(
            "Registering broker {} ({}:{})",
            broker.id,
            broker.host,
            broker.port
        );
        self.brokers.insert(broker.id, broker);
    }

    /// Update broker heartbeat
    pub fn update_broker_heartbeat(&self, broker_id: BrokerId) {
        if let Some(mut broker) = self.brokers.get_mut(&broker_id) {
            broker.update_heartbeat();
        }
    }

    /// Get alive brokers
    pub fn get_alive_brokers(&self) -> Vec<BrokerInfo> {
        self.brokers
            .iter()
            .filter(|entry| entry.value().is_alive(self.broker_timeout))
            .map(|entry| entry.value().clone())
            .collect()
    }

    /// Check if a broker is alive
    pub fn is_broker_alive(&self, broker_id: BrokerId) -> bool {
        self.brokers
            .get(&broker_id)
            .map(|b| b.is_alive(self.broker_timeout))
            .unwrap_or(false)
    }

    /// Create or update partition replica state
    pub fn set_partition_replicas(
        &self,
        topic: String,
        partition: u32,
        leader: BrokerId,
        replicas: Vec<BrokerId>,
    ) {
        let key = (topic.clone(), partition);

        if let Some(mut state) = self.partition_states.get_mut(&key) {
            // Update existing state
            state.leader = leader;
            state.replicas = replicas.clone();
            state.isr = replicas.iter().copied().collect();
        } else {
            // Create new state
            let state = PartitionReplicaState::new(topic, partition, leader, replicas);
            self.partition_states.insert(key, state);
        }
    }

    /// Get partition leader
    pub fn get_partition_leader(&self, topic: &str, partition: u32) -> Option<BrokerId> {
        let key = (topic.to_string(), partition);
        self.partition_states.get(&key).map(|s| s.leader)
    }

    /// Get partition replicas
    pub fn get_partition_replicas(&self, topic: &str, partition: u32) -> Vec<BrokerId> {
        let key = (topic.to_string(), partition);
        self.partition_states
            .get(&key)
            .map(|s| s.replicas.clone())
            .unwrap_or_default()
    }

    /// Get partition ISR
    pub fn get_partition_isr(&self, topic: &str, partition: u32) -> HashSet<BrokerId> {
        let key = (topic.to_string(), partition);
        self.partition_states
            .get(&key)
            .map(|s| s.isr.clone())
            .unwrap_or_default()
    }

    /// Get partition high watermark
    pub fn get_high_watermark(&self, topic: &str, partition: u32) -> u64 {
        let key = (topic.to_string(), partition);
        self.partition_states
            .get(&key)
            .map(|s| s.high_watermark)
            .unwrap_or(0)
    }

    /// Update replica offset
    pub fn update_replica_offset(
        &self,
        topic: &str,
        partition: u32,
        broker_id: BrokerId,
        offset: u64,
    ) {
        let key = (topic.to_string(), partition);
        if let Some(mut state) = self.partition_states.get_mut(&key) {
            state.update_replica_offset(broker_id, offset, self.isr_lag_threshold);
        }
    }

    /// Handle broker failure - elect new leaders for partitions where this broker was leader
    pub fn handle_broker_failure(&self, failed_broker_id: BrokerId) -> Vec<(String, u32, BrokerId)> {
        let mut new_leaders = Vec::new();

        for mut entry in self.partition_states.iter_mut() {
            let state = entry.value_mut();

            // Remove failed broker from ISR
            state.isr.remove(&failed_broker_id);

            // If this was the leader, elect a new one
            if state.leader == failed_broker_id {
                if let Some(new_leader) = state.elect_new_leader() {
                    new_leaders.push((state.topic.clone(), state.partition, new_leader));
                }
            }
        }

        new_leaders
    }

    /// Get this broker's ID
    pub fn broker_id(&self) -> BrokerId {
        self.broker_id
    }

    /// Check if this broker is the leader for a partition
    pub fn is_partition_leader(&self, topic: &str, partition: u32) -> bool {
        self.get_partition_leader(topic, partition)
            .map(|leader| leader == self.broker_id)
            .unwrap_or(false)
    }

    /// Get all partition states for debugging
    pub fn get_all_partition_states(&self) -> Vec<PartitionReplicaState> {
        self.partition_states
            .iter()
            .map(|entry| entry.value().clone())
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_broker_info() {
        let mut broker = BrokerInfo::new(1, "localhost".to_string(), 9092);
        assert!(broker.is_alive(Duration::from_secs(30)));

        std::thread::sleep(Duration::from_millis(100));
        assert!(broker.is_alive(Duration::from_secs(30)));
        assert!(!broker.is_alive(Duration::from_millis(50)));

        broker.update_heartbeat();
        assert!(broker.is_alive(Duration::from_secs(30)));
    }

    #[test]
    fn test_partition_replica_state() {
        let mut state = PartitionReplicaState::new(
            "test-topic".to_string(),
            0,
            1,
            vec![1, 2, 3],
        );

        assert_eq!(state.leader, 1);
        assert_eq!(state.isr.len(), 3);
        assert_eq!(state.high_watermark, 0);

        // Update leader offset
        state.update_replica_offset(1, 100, 10);
        assert_eq!(state.replica_offsets.get(&1), Some(&100));

        // Update follower offset
        state.update_replica_offset(2, 95, 10);
        assert!(state.isr.contains(&2)); // Within lag threshold

        // Follower falls behind
        state.update_replica_offset(3, 80, 10);
        assert!(!state.isr.contains(&3)); // Outside lag threshold

        // High watermark should be minimum of ISR
        assert_eq!(state.high_watermark, 95);
    }

    #[test]
    fn test_leader_election() {
        let mut state = PartitionReplicaState::new(
            "test-topic".to_string(),
            0,
            1,
            vec![1, 2, 3],
        );

        // Remove leader from ISR
        state.isr.remove(&1);

        // Elect new leader
        let new_leader = state.elect_new_leader();
        assert!(new_leader.is_some());
        assert_ne!(new_leader.unwrap(), 1);
        assert_eq!(state.leader_epoch, 1);
    }

    #[test]
    fn test_cluster_metadata() {
        let cluster = ClusterMetadata::new(1, Duration::from_secs(30), 100);

        // Register brokers
        cluster.register_broker(BrokerInfo::new(1, "localhost".to_string(), 9092));
        cluster.register_broker(BrokerInfo::new(2, "localhost".to_string(), 9093));
        cluster.register_broker(BrokerInfo::new(3, "localhost".to_string(), 9094));

        assert_eq!(cluster.get_alive_brokers().len(), 3);

        // Set up partition replicas
        cluster.set_partition_replicas(
            "test-topic".to_string(),
            0,
            1,
            vec![1, 2, 3],
        );

        assert_eq!(cluster.get_partition_leader("test-topic", 0), Some(1));
        assert_eq!(cluster.get_partition_replicas("test-topic", 0).len(), 3);
        assert!(cluster.is_partition_leader("test-topic", 0));
    }

    #[test]
    fn test_broker_failure() {
        let cluster = ClusterMetadata::new(1, Duration::from_secs(30), 100);

        cluster.register_broker(BrokerInfo::new(1, "localhost".to_string(), 9092));
        cluster.register_broker(BrokerInfo::new(2, "localhost".to_string(), 9093));

        cluster.set_partition_replicas("test-topic".to_string(), 0, 1, vec![1, 2]);

        // Simulate broker 1 failure
        let new_leaders = cluster.handle_broker_failure(1);
        assert_eq!(new_leaders.len(), 1);
        assert_eq!(new_leaders[0].2, 2); // New leader should be broker 2
    }
}
