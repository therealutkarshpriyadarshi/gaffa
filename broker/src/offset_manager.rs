use common::Result;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::fs;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::RwLock;

/// Key for offset storage: (group_id, topic, partition)
type OffsetKey = (String, String, u32);

/// Manages committed offsets for consumer groups
#[derive(Clone)]
pub struct OffsetManager {
    /// In-memory offset storage: (group_id, topic, partition) -> offset
    offsets: Arc<RwLock<HashMap<OffsetKey, u64>>>,
    /// Directory for storing offset files
    data_dir: PathBuf,
}

impl OffsetManager {
    /// Create a new offset manager
    pub fn new(data_dir: impl AsRef<Path>) -> Result<Self> {
        let data_dir = data_dir.as_ref().join("offsets");
        std::fs::create_dir_all(&data_dir)?;

        Ok(Self {
            offsets: Arc::new(RwLock::new(HashMap::new())),
            data_dir,
        })
    }

    /// Load existing offsets from disk
    pub async fn load(&self) -> Result<()> {
        let mut offsets = self.offsets.write().await;

        // Read all offset files from the data directory
        let mut entries = fs::read_dir(&self.data_dir).await?;

        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();
            if path.is_file() && path.extension().map_or(false, |ext| ext == "offsets") {
                // Parse filename: group_id.offsets
                if let Some(group_id) = path
                    .file_stem()
                    .and_then(|s| s.to_str())
                    .map(|s| s.to_string())
                {
                    // Read and parse offset data
                    let mut file = fs::File::open(&path).await?;
                    let mut contents = String::new();
                    file.read_to_string(&mut contents).await?;

                    for line in contents.lines() {
                        if let Some((key, value)) = line.split_once('=') {
                            // Key format: topic:partition
                            if let Some((topic, partition_str)) = key.split_once(':') {
                                if let (Ok(partition), Ok(offset)) = (
                                    partition_str.parse::<u32>(),
                                    value.parse::<u64>(),
                                ) {
                                    offsets.insert(
                                        (group_id.clone(), topic.to_string(), partition),
                                        offset,
                                    );
                                }
                            }
                        }
                    }
                }
            }
        }

        tracing::info!("Loaded {} committed offsets", offsets.len());
        Ok(())
    }

    /// Commit an offset for a consumer group
    pub async fn commit_offset(
        &self,
        group_id: &str,
        topic: &str,
        partition: u32,
        offset: u64,
    ) -> Result<()> {
        // Update in-memory
        {
            let mut offsets = self.offsets.write().await;
            offsets.insert((group_id.to_string(), topic.to_string(), partition), offset);
        }

        // Persist to disk
        self.persist_group(group_id).await?;

        tracing::debug!(
            "Committed offset {} for group={}, topic={}, partition={}",
            offset,
            group_id,
            topic,
            partition
        );

        Ok(())
    }

    /// Fetch committed offset for a consumer group
    pub async fn fetch_offset(
        &self,
        group_id: &str,
        topic: &str,
        partition: u32,
    ) -> Result<Option<u64>> {
        let offsets = self.offsets.read().await;
        let offset = offsets.get(&(group_id.to_string(), topic.to_string(), partition));
        Ok(offset.copied())
    }

    /// Get all offsets for a consumer group
    pub async fn get_group_offsets(&self, group_id: &str) -> HashMap<(String, u32), u64> {
        let offsets = self.offsets.read().await;
        offsets
            .iter()
            .filter(|((gid, _, _), _)| gid == group_id)
            .map(|((_, topic, partition), offset)| ((topic.clone(), *partition), *offset))
            .collect()
    }

    /// Delete all offsets for a consumer group
    pub async fn delete_group(&self, group_id: &str) -> Result<()> {
        // Remove from memory
        {
            let mut offsets = self.offsets.write().await;
            offsets.retain(|(gid, _, _), _| gid != group_id);
        }

        // Delete file
        let file_path = self.data_dir.join(format!("{}.offsets", group_id));
        if file_path.exists() {
            fs::remove_file(file_path).await?;
        }

        tracing::info!("Deleted all offsets for group={}", group_id);
        Ok(())
    }

    /// Persist all offsets for a consumer group to disk
    async fn persist_group(&self, group_id: &str) -> Result<()> {
        let offsets = self.offsets.read().await;

        // Collect all offsets for this group
        let group_offsets: Vec<_> = offsets
            .iter()
            .filter(|((gid, _, _), _)| gid == group_id)
            .collect();

        // Write to file
        let file_path = self.data_dir.join(format!("{}.offsets", group_id));
        let mut file = fs::File::create(&file_path).await?;

        for ((_, topic, partition), offset) in group_offsets {
            let line = format!("{}:{}={}\n", topic, partition, offset);
            file.write_all(line.as_bytes()).await?;
        }

        file.sync_all().await?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_commit_and_fetch_offset() {
        let temp_dir = TempDir::new().unwrap();
        let manager = OffsetManager::new(temp_dir.path()).unwrap();

        // Commit an offset
        manager
            .commit_offset("group1", "topic1", 0, 100)
            .await
            .unwrap();

        // Fetch it back
        let offset = manager
            .fetch_offset("group1", "topic1", 0)
            .await
            .unwrap();
        assert_eq!(offset, Some(100));

        // Fetch non-existent offset
        let offset = manager
            .fetch_offset("group1", "topic1", 1)
            .await
            .unwrap();
        assert_eq!(offset, None);
    }

    #[tokio::test]
    async fn test_multiple_groups() {
        let temp_dir = TempDir::new().unwrap();
        let manager = OffsetManager::new(temp_dir.path()).unwrap();

        // Commit offsets for different groups
        manager
            .commit_offset("group1", "topic1", 0, 100)
            .await
            .unwrap();
        manager
            .commit_offset("group2", "topic1", 0, 200)
            .await
            .unwrap();

        // Fetch offsets
        let offset1 = manager.fetch_offset("group1", "topic1", 0).await.unwrap();
        let offset2 = manager.fetch_offset("group2", "topic1", 0).await.unwrap();

        assert_eq!(offset1, Some(100));
        assert_eq!(offset2, Some(200));
    }

    #[tokio::test]
    async fn test_persistence() {
        let temp_dir = TempDir::new().unwrap();

        // Create manager and commit offset
        {
            let manager = OffsetManager::new(temp_dir.path()).unwrap();
            manager
                .commit_offset("group1", "topic1", 0, 100)
                .await
                .unwrap();
            manager
                .commit_offset("group1", "topic2", 1, 200)
                .await
                .unwrap();
        }

        // Create new manager and load
        {
            let manager = OffsetManager::new(temp_dir.path()).unwrap();
            manager.load().await.unwrap();

            let offset1 = manager.fetch_offset("group1", "topic1", 0).await.unwrap();
            let offset2 = manager.fetch_offset("group1", "topic2", 1).await.unwrap();

            assert_eq!(offset1, Some(100));
            assert_eq!(offset2, Some(200));
        }
    }

    #[tokio::test]
    async fn test_get_group_offsets() {
        let temp_dir = TempDir::new().unwrap();
        let manager = OffsetManager::new(temp_dir.path()).unwrap();

        manager
            .commit_offset("group1", "topic1", 0, 100)
            .await
            .unwrap();
        manager
            .commit_offset("group1", "topic1", 1, 200)
            .await
            .unwrap();
        manager
            .commit_offset("group2", "topic1", 0, 300)
            .await
            .unwrap();

        let group1_offsets = manager.get_group_offsets("group1").await;
        assert_eq!(group1_offsets.len(), 2);
        assert_eq!(group1_offsets.get(&("topic1".to_string(), 0)), Some(&100));
        assert_eq!(group1_offsets.get(&("topic1".to_string(), 1)), Some(&200));
    }

    #[tokio::test]
    async fn test_delete_group() {
        let temp_dir = TempDir::new().unwrap();
        let manager = OffsetManager::new(temp_dir.path()).unwrap();

        manager
            .commit_offset("group1", "topic1", 0, 100)
            .await
            .unwrap();

        // Delete group
        manager.delete_group("group1").await.unwrap();

        // Verify offset is gone
        let offset = manager.fetch_offset("group1", "topic1", 0).await.unwrap();
        assert_eq!(offset, None);

        let group_offsets = manager.get_group_offsets("group1").await;
        assert_eq!(group_offsets.len(), 0);
    }

    #[tokio::test]
    async fn test_update_offset() {
        let temp_dir = TempDir::new().unwrap();
        let manager = OffsetManager::new(temp_dir.path()).unwrap();

        // Commit initial offset
        manager
            .commit_offset("group1", "topic1", 0, 100)
            .await
            .unwrap();

        // Update to new offset
        manager
            .commit_offset("group1", "topic1", 0, 200)
            .await
            .unwrap();

        let offset = manager.fetch_offset("group1", "topic1", 0).await.unwrap();
        assert_eq!(offset, Some(200));
    }
}
