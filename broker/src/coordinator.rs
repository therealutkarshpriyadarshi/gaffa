use common::{GaffaError, Result};
use protocol::PartitionAssignment;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use uuid::Uuid;

/// Represents a consumer group member
#[derive(Debug, Clone)]
struct GroupMember {
    /// Unique member ID
    member_id: String,
    /// Topics this member is interested in
    topics: Vec<String>,
    /// Last heartbeat timestamp
    last_heartbeat: Instant,
    /// Current partition assignments
    assignments: Vec<PartitionAssignment>,
}

/// Represents a consumer group
#[derive(Debug)]
struct ConsumerGroup {
    /// Group ID
    group_id: String,
    /// Members in the group
    members: HashMap<String, GroupMember>,
    /// Generation ID (incremented on each rebalance)
    generation: u32,
}

impl ConsumerGroup {
    fn new(group_id: String) -> Self {
        Self {
            group_id,
            members: HashMap::new(),
            generation: 0,
        }
    }

    /// Add or update a member in the group
    fn add_member(&mut self, member: GroupMember) {
        self.members.insert(member.member_id.clone(), member);
    }

    /// Remove a member from the group
    fn remove_member(&mut self, member_id: &str) -> Option<GroupMember> {
        self.members.remove(member_id)
    }

    /// Update heartbeat for a member
    fn heartbeat(&mut self, member_id: &str) -> Result<()> {
        if let Some(member) = self.members.get_mut(member_id) {
            member.last_heartbeat = Instant::now();
            Ok(())
        } else {
            Err(GaffaError::ConsumerNotFound(member_id.to_string()))
        }
    }

    /// Get members that haven't sent heartbeat within timeout
    fn get_dead_members(&self, timeout: Duration) -> Vec<String> {
        let now = Instant::now();
        self.members
            .iter()
            .filter(|(_, member)| now.duration_since(member.last_heartbeat) > timeout)
            .map(|(id, _)| id.clone())
            .collect()
    }

    /// Trigger a rebalance
    fn rebalance(&mut self, topic_partitions: &HashMap<String, u32>) {
        self.generation += 1;

        // Collect all topics that members are interested in
        let mut all_topics: HashSet<String> = HashSet::new();
        for member in self.members.values() {
            for topic in &member.topics {
                all_topics.insert(topic.clone());
            }
        }

        // Build list of all partitions that need assignment
        let mut all_partitions: Vec<PartitionAssignment> = Vec::new();
        for topic in &all_topics {
            if let Some(&partition_count) = topic_partitions.get(topic) {
                for partition in 0..partition_count {
                    all_partitions.push(PartitionAssignment::new(topic.clone(), partition));
                }
            }
        }

        // Round-robin assignment
        let member_ids: Vec<String> = self.members.keys().cloned().collect();
        if member_ids.is_empty() {
            return;
        }

        // Clear existing assignments
        for member in self.members.values_mut() {
            member.assignments.clear();
        }

        // Assign partitions round-robin
        for (idx, partition) in all_partitions.into_iter().enumerate() {
            let member_idx = idx % member_ids.len();
            let member_id = &member_ids[member_idx];
            if let Some(member) = self.members.get_mut(member_id) {
                member.assignments.push(partition);
            }
        }

        tracing::info!(
            "Rebalanced group '{}' (generation={}): {} members, {} topics",
            self.group_id,
            self.generation,
            self.members.len(),
            all_topics.len()
        );
    }
}

/// Coordinates consumer groups and partition assignments
#[derive(Clone)]
pub struct GroupCoordinator {
    /// All consumer groups
    groups: Arc<RwLock<HashMap<String, ConsumerGroup>>>,
    /// Heartbeat timeout duration
    heartbeat_timeout: Duration,
    /// Topic partition counts (cached)
    topic_partitions: Arc<RwLock<HashMap<String, u32>>>,
}

impl GroupCoordinator {
    /// Create a new group coordinator
    pub fn new(heartbeat_timeout: Duration) -> Self {
        Self {
            groups: Arc::new(RwLock::new(HashMap::new())),
            heartbeat_timeout,
            topic_partitions: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Update partition count for a topic
    pub async fn update_topic_partitions(&self, topic: String, partition_count: u32) {
        let mut topic_partitions = self.topic_partitions.write().await;
        topic_partitions.insert(topic, partition_count);
    }

    /// Join a consumer group
    pub async fn join_group(
        &self,
        group_id: String,
        member_id: Option<String>,
        topics: Vec<String>,
    ) -> Result<(String, Vec<PartitionAssignment>)> {
        let mut groups = self.groups.write().await;

        // Get or create group
        let group = groups
            .entry(group_id.clone())
            .or_insert_with(|| ConsumerGroup::new(group_id.clone()));

        // Generate member ID if not provided
        let member_id = member_id.unwrap_or_else(|| {
            format!("{}-{}", group_id, Uuid::new_v4().to_string()[..8].to_string())
        });

        // Create member
        let member = GroupMember {
            member_id: member_id.clone(),
            topics: topics.clone(),
            last_heartbeat: Instant::now(),
            assignments: Vec::new(),
        };

        // Add member to group
        group.add_member(member);

        // Trigger rebalance
        let topic_partitions = self.topic_partitions.read().await;
        group.rebalance(&topic_partitions);

        // Get assignments for this member
        let assignments = group
            .members
            .get(&member_id)
            .map(|m| m.assignments.clone())
            .unwrap_or_default();

        tracing::info!(
            "Member '{}' joined group '{}' with {} assignments",
            member_id,
            group_id,
            assignments.len()
        );

        Ok((member_id, assignments))
    }

    /// Leave a consumer group
    pub async fn leave_group(&self, group_id: &str, member_id: &str) -> Result<()> {
        let mut groups = self.groups.write().await;

        if let Some(group) = groups.get_mut(group_id) {
            if group.remove_member(member_id).is_some() {
                tracing::info!("Member '{}' left group '{}'", member_id, group_id);

                // Trigger rebalance if there are still members
                if !group.members.is_empty() {
                    let topic_partitions = self.topic_partitions.read().await;
                    group.rebalance(&topic_partitions);
                } else {
                    // Remove empty group
                    groups.remove(group_id);
                    tracing::info!("Removed empty group '{}'", group_id);
                }

                Ok(())
            } else {
                Err(GaffaError::ConsumerNotFound(member_id.to_string()))
            }
        } else {
            Err(GaffaError::GroupNotFound(group_id.to_string()))
        }
    }

    /// Handle heartbeat from a consumer
    pub async fn heartbeat(&self, group_id: &str, member_id: &str) -> Result<bool> {
        let mut groups = self.groups.write().await;

        if let Some(group) = groups.get_mut(group_id) {
            group.heartbeat(member_id)?;
            Ok(false) // No rejoin needed
        } else {
            Err(GaffaError::GroupNotFound(group_id.to_string()))
        }
    }

    /// Check for dead members and trigger rebalance if needed
    pub async fn check_heartbeats(&self) {
        let mut groups = self.groups.write().await;
        let mut groups_to_rebalance = Vec::new();

        for (group_id, group) in groups.iter_mut() {
            let dead_members = group.get_dead_members(self.heartbeat_timeout);

            if !dead_members.is_empty() {
                tracing::warn!(
                    "Detected {} dead members in group '{}': {:?}",
                    dead_members.len(),
                    group_id,
                    dead_members
                );

                // Remove dead members
                for member_id in dead_members {
                    group.remove_member(&member_id);
                }

                groups_to_rebalance.push(group_id.clone());
            }
        }

        // Trigger rebalance for affected groups
        let topic_partitions = self.topic_partitions.read().await;
        for group_id in groups_to_rebalance {
            if let Some(group) = groups.get_mut(&group_id) {
                if !group.members.is_empty() {
                    group.rebalance(&topic_partitions);
                } else {
                    // Mark for removal
                    tracing::info!("Group '{}' is now empty after heartbeat check", group_id);
                }
            }
        }

        // Remove empty groups
        groups.retain(|_, group| !group.members.is_empty());
    }

    /// Get all groups (for debugging/monitoring)
    pub async fn get_groups(&self) -> Vec<String> {
        let groups = self.groups.read().await;
        groups.keys().cloned().collect()
    }

    /// Get group member count
    pub async fn get_group_size(&self, group_id: &str) -> Option<usize> {
        let groups = self.groups.read().await;
        groups.get(group_id).map(|g| g.members.len())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_join_group() {
        let coordinator = GroupCoordinator::new(Duration::from_secs(30));

        // Update topic metadata
        coordinator
            .update_topic_partitions("topic1".to_string(), 3)
            .await;

        // First member joins
        let (member1, assignments1) = coordinator
            .join_group(
                "group1".to_string(),
                None,
                vec!["topic1".to_string()],
            )
            .await
            .unwrap();

        assert!(!member1.is_empty());
        assert_eq!(assignments1.len(), 3); // Gets all 3 partitions

        // Second member joins
        let (member2, assignments2) = coordinator
            .join_group(
                "group1".to_string(),
                None,
                vec!["topic1".to_string()],
            )
            .await
            .unwrap();

        assert!(!member2.is_empty());
        assert_ne!(member1, member2);
        // After rebalance, partitions are distributed
        assert!(assignments2.len() > 0);
    }

    #[tokio::test]
    async fn test_leave_group() {
        let coordinator = GroupCoordinator::new(Duration::from_secs(30));

        coordinator
            .update_topic_partitions("topic1".to_string(), 3)
            .await;

        let (member1, _assignments) = coordinator
            .join_group(
                "group1".to_string(),
                Some("member1".to_string()),
                vec!["topic1".to_string()],
            )
            .await
            .unwrap();

        // Leave group
        coordinator
            .leave_group("group1", &member1)
            .await
            .unwrap();

        // Verify group size
        let size = coordinator.get_group_size("group1").await;
        assert_eq!(size, None); // Group should be removed when empty
    }

    #[tokio::test]
    async fn test_heartbeat() {
        let coordinator = GroupCoordinator::new(Duration::from_secs(30));

        coordinator
            .update_topic_partitions("topic1".to_string(), 3)
            .await;

        let (member1, _) = coordinator
            .join_group(
                "group1".to_string(),
                Some("member1".to_string()),
                vec!["topic1".to_string()],
            )
            .await
            .unwrap();

        // Send heartbeat
        let needs_rejoin = coordinator.heartbeat("group1", &member1).await.unwrap();
        assert_eq!(needs_rejoin, false);

        // Invalid member
        let result = coordinator.heartbeat("group1", "invalid").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_dead_member_detection() {
        let coordinator = GroupCoordinator::new(Duration::from_millis(100));

        coordinator
            .update_topic_partitions("topic1".to_string(), 3)
            .await;

        let (member1, _) = coordinator
            .join_group(
                "group1".to_string(),
                Some("member1".to_string()),
                vec!["topic1".to_string()],
            )
            .await
            .unwrap();

        // Wait for timeout
        tokio::time::sleep(Duration::from_millis(150)).await;

        // Check heartbeats should detect dead member
        coordinator.check_heartbeats().await;

        // Group should be removed
        let size = coordinator.get_group_size("group1").await;
        assert_eq!(size, None);
    }

    #[tokio::test]
    async fn test_round_robin_assignment() {
        let coordinator = GroupCoordinator::new(Duration::from_secs(30));

        coordinator
            .update_topic_partitions("topic1".to_string(), 6)
            .await;

        // Add 3 members
        let (_m1, _a1) = coordinator
            .join_group(
                "group1".to_string(),
                Some("member1".to_string()),
                vec!["topic1".to_string()],
            )
            .await
            .unwrap();

        let (_m2, _a2) = coordinator
            .join_group(
                "group1".to_string(),
                Some("member2".to_string()),
                vec!["topic1".to_string()],
            )
            .await
            .unwrap();

        let (_m3, a3) = coordinator
            .join_group(
                "group1".to_string(),
                Some("member3".to_string()),
                vec!["topic1".to_string()],
            )
            .await
            .unwrap();

        // After all joins, with 3 members and 6 partitions
        // Each should get 2 partitions (6 / 3 = 2)
        // The last join returns the assignments after final rebalance
        assert_eq!(a3.len(), 2);

        // Verify group has 3 members
        let size = coordinator.get_group_size("group1").await;
        assert_eq!(size, Some(3));
    }

    #[tokio::test]
    async fn test_multiple_topics() {
        let coordinator = GroupCoordinator::new(Duration::from_secs(30));

        coordinator
            .update_topic_partitions("topic1".to_string(), 2)
            .await;
        coordinator
            .update_topic_partitions("topic2".to_string(), 3)
            .await;

        let (_member1, assignments) = coordinator
            .join_group(
                "group1".to_string(),
                None,
                vec!["topic1".to_string(), "topic2".to_string()],
            )
            .await
            .unwrap();

        // Should get all 5 partitions (2 + 3)
        assert_eq!(assignments.len(), 5);
    }
}
