use crate::cluster::{BrokerId, ClusterMetadata};
use protocol::{Message, Record};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use storage::TopicManager;
use tokio::sync::RwLock;
use tokio::time::interval;

/// Replication request for a follower to fetch from leader
#[derive(Debug, Clone)]
pub struct ReplicationFetchRequest {
    pub topic: String,
    pub partition: u32,
    pub offset: u64,
    pub max_records: u32,
}

/// Replication response from leader to follower
#[derive(Debug, Clone)]
pub struct ReplicationFetchResponse {
    pub records: Vec<Record>,
    pub high_watermark: u64,
    pub leader_epoch: u32,
}

/// Manages replication for all partitions on this broker
#[derive(Clone)]
pub struct ReplicationManager {
    cluster: ClusterMetadata,
    topic_manager: TopicManager,
    /// Follower fetch state: (topic, partition) -> last fetched offset
    follower_state: Arc<RwLock<HashMap<(String, u32), u64>>>,
    /// Replication factor for new topics
    replication_factor: u32,
    /// Max lag for ISR (in number of messages)
    max_isr_lag: u64,
}

impl ReplicationManager {
    pub fn new(
        cluster: ClusterMetadata,
        topic_manager: TopicManager,
        replication_factor: u32,
        max_isr_lag: u64,
    ) -> Self {
        Self {
            cluster,
            topic_manager,
            follower_state: Arc::new(RwLock::new(HashMap::new())),
            replication_factor,
            max_isr_lag,
        }
    }

    /// Assign replicas to a new partition using round-robin across alive brokers
    pub fn assign_partition_replicas(
        &self,
        topic: &str,
        partition: u32,
    ) -> (BrokerId, Vec<BrokerId>) {
        let alive_brokers = self.cluster.get_alive_brokers();
        let num_brokers = alive_brokers.len();

        if num_brokers == 0 {
            // Fallback to this broker if no cluster
            let broker_id = self.cluster.broker_id();
            return (broker_id, vec![broker_id]);
        }

        let repl_factor = std::cmp::min(self.replication_factor as usize, num_brokers);

        // Round-robin assignment: start at partition % num_brokers
        let start_idx = (partition as usize) % num_brokers;
        let mut replicas = Vec::new();

        for i in 0..repl_factor {
            let idx = (start_idx + i) % num_brokers;
            replicas.push(alive_brokers[idx].id);
        }

        // First replica is the leader
        let leader = replicas[0];

        (leader, replicas)
    }

    /// Start replication for a partition where this broker is a follower
    pub async fn start_follower_replication(
        &self,
        topic: String,
        partition: u32,
        leader_broker_id: BrokerId,
    ) {
        let self_clone = self.clone();
        let topic_clone = topic.clone();

        tokio::spawn(async move {
            self_clone
                .follower_replication_loop(topic_clone, partition, leader_broker_id)
                .await;
        });
    }

    /// Follower replication loop - continuously fetch from leader
    async fn follower_replication_loop(
        &self,
        topic: String,
        partition: u32,
        _leader_broker_id: BrokerId,
    ) {
        let mut interval = interval(Duration::from_millis(100)); // Fetch every 100ms

        loop {
            interval.tick().await;

            // Check if we're still a follower for this partition
            if self.cluster.is_partition_leader(&topic, partition) {
                tracing::debug!(
                    "No longer a follower for {}:{}, stopping replication loop",
                    topic,
                    partition
                );
                break;
            }

            // Get current offset
            let current_offset = {
                let state = self.follower_state.read().await;
                state.get(&(topic.clone(), partition)).copied().unwrap_or(0)
            };

            // In a multi-broker setup, we would fetch from the leader broker here
            // For now, in single-broker mode, this is a no-op
            // The actual fetch would happen via RPC to the leader broker

            // Simulate fetch and update offset
            // In real implementation, this would:
            // 1. Send ReplicationFetchRequest to leader broker
            // 2. Receive records and high watermark
            // 3. Append records to local log
            // 4. Update follower offset and ISR status

            // For now, just update our offset from the topic manager
            if let Ok(topic_handle) = self.topic_manager.get_topic(&topic) {
                if let Ok(partition_handle) = topic_handle.get_partition(partition) {
                    let next_offset = partition_handle.next_offset().await;
                    if next_offset > current_offset {
                        let mut state = self.follower_state.write().await;
                        state.insert((topic.clone(), partition), next_offset);

                        // Update cluster metadata with our offset
                        self.cluster.update_replica_offset(
                            &topic,
                            partition,
                            self.cluster.broker_id(),
                            next_offset,
                        );
                    }
                }
            }

            // Small delay to avoid busy loop
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    /// Handle a replication fetch request from a follower (when this broker is leader)
    pub async fn handle_replication_fetch(
        &self,
        request: ReplicationFetchRequest,
    ) -> Option<ReplicationFetchResponse> {
        // Verify we're the leader for this partition
        if !self.cluster.is_partition_leader(&request.topic, request.partition) {
            return None;
        }

        // Fetch records from topic manager
        let records = if let Ok(topic) = self.topic_manager.get_topic(&request.topic) {
            if let Ok(partition) = topic.get_partition(request.partition) {
                partition
                    .fetch(request.offset, request.max_records)
                    .await
                    .unwrap_or_default()
            } else {
                Vec::new()
            }
        } else {
            Vec::new()
        };

        // Get high watermark and leader epoch
        let high_watermark = self
            .cluster
            .get_high_watermark(&request.topic, request.partition);

        // Get leader epoch from partition state
        let leader_epoch = self
            .cluster
            .get_all_partition_states()
            .iter()
            .find(|s| s.topic == request.topic && s.partition == request.partition)
            .map(|s| s.leader_epoch)
            .unwrap_or(0);

        Some(ReplicationFetchResponse {
            records,
            high_watermark,
            leader_epoch,
        })
    }

    /// Update leader offset for a partition after appending records
    pub fn update_leader_offset(&self, topic: &str, partition: u32, offset: u64) {
        self.cluster.update_replica_offset(
            topic,
            partition,
            self.cluster.broker_id(),
            offset,
        );
    }

    /// Check for dead brokers and trigger leader election
    pub async fn check_broker_health(&self) {
        let alive_brokers = self.cluster.get_alive_brokers();
        let alive_ids: Vec<BrokerId> = alive_brokers.iter().map(|b| b.id).collect();

        // Get all partition states
        let states = self.cluster.get_all_partition_states();

        for state in states {
            // Check if leader is dead
            if !alive_ids.contains(&state.leader) {
                tracing::warn!(
                    "Leader broker {} for {}:{} is dead, triggering election",
                    state.leader,
                    state.topic,
                    state.partition
                );

                let new_leaders = self.cluster.handle_broker_failure(state.leader);

                // Log new leaders
                for (topic, partition, new_leader) in new_leaders {
                    if new_leader == self.cluster.broker_id() {
                        tracing::info!(
                            "This broker became leader for {}:{}",
                            topic,
                            partition
                        );
                    }
                }
            }
        }
    }

    /// Get replication lag for a follower
    pub fn get_replication_lag(&self, topic: &str, partition: u32, follower_id: BrokerId) -> Option<u64> {
        let leader_offset = self
            .cluster
            .get_all_partition_states()
            .iter()
            .find(|s| s.topic == topic && s.partition == partition)
            .and_then(|s| s.replica_offsets.get(&s.leader).copied())?;

        let follower_offset = self
            .cluster
            .get_all_partition_states()
            .iter()
            .find(|s| s.topic == topic && s.partition == partition)
            .and_then(|s| s.replica_offsets.get(&follower_id).copied())?;

        Some(leader_offset.saturating_sub(follower_offset))
    }

    /// Check if a write can be acknowledged (based on ISR)
    pub fn can_acknowledge_write(&self, topic: &str, partition: u32, offset: u64) -> bool {
        let high_watermark = self.cluster.get_high_watermark(topic, partition);
        offset <= high_watermark
    }

    /// Get replication stats for monitoring
    pub fn get_replication_stats(&self) -> ReplicationStats {
        let states = self.cluster.get_all_partition_states();

        let total_partitions = states.len();
        let leader_partitions = states
            .iter()
            .filter(|s| s.leader == self.cluster.broker_id())
            .count();
        let follower_partitions = states
            .iter()
            .filter(|s| {
                s.leader != self.cluster.broker_id()
                    && s.replicas.contains(&self.cluster.broker_id())
            })
            .count();

        let under_replicated = states
            .iter()
            .filter(|s| s.isr.len() < s.replicas.len())
            .count();

        ReplicationStats {
            total_partitions,
            leader_partitions,
            follower_partitions,
            under_replicated_partitions: under_replicated,
        }
    }
}

/// Replication statistics
#[derive(Debug, Clone)]
pub struct ReplicationStats {
    pub total_partitions: usize,
    pub leader_partitions: usize,
    pub follower_partitions: usize,
    pub under_replicated_partitions: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::BrokerInfo;
    use std::time::Duration;
    use tempfile::TempDir;

    #[test]
    fn test_assign_partition_replicas() {
        let cluster = ClusterMetadata::new(1, Duration::from_secs(30), 100);
        cluster.register_broker(BrokerInfo::new(1, "localhost".to_string(), 9092));
        cluster.register_broker(BrokerInfo::new(2, "localhost".to_string(), 9093));
        cluster.register_broker(BrokerInfo::new(3, "localhost".to_string(), 9094));

        let temp_dir = TempDir::new().unwrap();
        let topic_manager = TopicManager::new(temp_dir.path().to_str().unwrap()).unwrap();

        let repl_mgr = ReplicationManager::new(cluster, topic_manager, 3, 100);

        let (leader, replicas) = repl_mgr.assign_partition_replicas("test-topic", 0);
        assert_eq!(replicas.len(), 3);
        assert_eq!(leader, replicas[0]);
    }

    #[tokio::test]
    async fn test_replication_stats() {
        let cluster = ClusterMetadata::new(1, Duration::from_secs(30), 100);
        cluster.register_broker(BrokerInfo::new(1, "localhost".to_string(), 9092));

        cluster.set_partition_replicas("test-topic".to_string(), 0, 1, vec![1]);

        let temp_dir = TempDir::new().unwrap();
        let topic_manager = TopicManager::new(temp_dir.path().to_str().unwrap()).unwrap();

        let repl_mgr = ReplicationManager::new(cluster, topic_manager, 1, 100);

        let stats = repl_mgr.get_replication_stats();
        assert_eq!(stats.total_partitions, 1);
        assert_eq!(stats.leader_partitions, 1);
    }
}
