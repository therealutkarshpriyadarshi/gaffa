use crate::segment::{LogSegment, SegmentConfig};
use common::{GaffaError, Result};
use protocol::{Message, Record};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::RwLock;

/// Persistent storage for a single partition using log segments
///
/// The partition manages multiple log segments:
/// - Active segment: Currently being written to
/// - Closed segments: Immutable, available for reads
///
/// When the active segment reaches its size limit, it's rotated:
/// a new active segment is created and the old one becomes closed
#[derive(Debug)]
pub struct Partition {
    /// Partition number
    partition_id: u32,
    /// Topic name
    topic_name: String,
    /// Data directory for this partition
    data_dir: PathBuf,
    /// All segments (both active and closed)
    segments: Arc<RwLock<Vec<Arc<LogSegment>>>>,
    /// Segment configuration
    config: SegmentConfig,
}

impl Partition {
    /// Create a new partition with persistent storage
    pub fn new(
        topic_name: String,
        partition_id: u32,
        data_dir: impl AsRef<Path>,
    ) -> Result<Self> {
        Self::with_config(topic_name, partition_id, data_dir, SegmentConfig::default())
    }

    /// Create a new partition with custom configuration
    pub fn with_config(
        topic_name: String,
        partition_id: u32,
        data_dir: impl AsRef<Path>,
        config: SegmentConfig,
    ) -> Result<Self> {
        let partition_dir = Self::partition_dir(data_dir.as_ref(), &topic_name, partition_id);
        std::fs::create_dir_all(&partition_dir)?;

        // Create initial segment at offset 0
        let segment = LogSegment::create(0, &partition_dir, config.clone())?;
        let segments = vec![Arc::new(segment)];

        Ok(Self {
            partition_id,
            topic_name,
            data_dir: partition_dir,
            segments: Arc::new(RwLock::new(segments)),
            config,
        })
    }

    /// Open an existing partition from disk
    pub fn open(
        topic_name: String,
        partition_id: u32,
        data_dir: impl AsRef<Path>,
    ) -> Result<Self> {
        Self::open_with_config(topic_name, partition_id, data_dir, SegmentConfig::default())
    }

    /// Open an existing partition with custom configuration
    pub fn open_with_config(
        topic_name: String,
        partition_id: u32,
        data_dir: impl AsRef<Path>,
        config: SegmentConfig,
    ) -> Result<Self> {
        let partition_dir = Self::partition_dir(data_dir.as_ref(), &topic_name, partition_id);

        // Scan directory for segment files
        let mut segment_base_offsets = Vec::new();
        for entry in std::fs::read_dir(&partition_dir)? {
            let entry = entry?;
            let path = entry.path();
            if let Some(ext) = path.extension() {
                if ext == "log" {
                    if let Some(filename) = path.file_stem() {
                        if let Ok(offset) = filename.to_string_lossy().parse::<u64>() {
                            segment_base_offsets.push(offset);
                        }
                    }
                }
            }
        }

        if segment_base_offsets.is_empty() {
            return Err(GaffaError::Storage(format!(
                "No segments found in partition directory: {}",
                partition_dir.display()
            )));
        }

        segment_base_offsets.sort_unstable();

        // Open all segments
        let mut segments = Vec::new();
        for base_offset in segment_base_offsets {
            let segment = LogSegment::open(base_offset, &partition_dir, config.clone())?;
            segments.push(Arc::new(segment));
        }

        Ok(Self {
            partition_id,
            topic_name,
            data_dir: partition_dir,
            segments: Arc::new(RwLock::new(segments)),
            config,
        })
    }

    /// Append messages to this partition
    /// Returns the base offset where messages were written
    pub async fn append(&self, messages: Vec<Message>) -> Result<u64> {
        if messages.is_empty() {
            return Err(GaffaError::InvalidMessage("No messages to append".to_string()));
        }

        let mut segments = self.segments.write().await;

        // Get the active segment (last one)
        let active_segment = segments.last().ok_or_else(|| {
            GaffaError::Storage("No active segment".to_string())
        })?;

        // Check if we need to rotate
        if active_segment.should_rotate().await {
            let next_offset = active_segment.next_offset().await;
            let new_segment = LogSegment::create(next_offset, &self.data_dir, self.config.clone())?;
            segments.push(Arc::new(new_segment));

            tracing::info!(
                topic = %self.topic_name,
                partition = self.partition_id,
                new_base_offset = next_offset,
                "Rotated to new segment"
            );
        }

        // Append to the active segment
        let active_segment = segments.last().unwrap();
        let base_offset = active_segment.append(messages).await?;

        tracing::debug!(
            topic = %self.topic_name,
            partition = self.partition_id,
            base_offset = base_offset,
            "Appended messages to partition"
        );

        Ok(base_offset)
    }

    /// Fetch messages starting from the given offset
    pub async fn fetch(&self, offset: u64, max_messages: u32) -> Result<Vec<Record>> {
        let segments = self.segments.read().await;

        // Find the segment containing the starting offset
        let start_segment_idx = self.find_segment_index(&segments, offset)?;

        let mut all_records = Vec::new();
        let mut remaining = max_messages;
        let mut current_offset = offset;

        // Read from segments until we have enough records
        for segment in &segments[start_segment_idx..] {
            if remaining == 0 {
                break;
            }

            let disk_records = segment.read_batch(current_offset, remaining).await?;
            if disk_records.is_empty() {
                break;
            }

            // Update current_offset for next segment
            if let Some(last) = disk_records.last() {
                current_offset = last.offset + 1;
            }

            remaining -= disk_records.len() as u32;

            // Convert and append records
            for disk_record in disk_records {
                all_records.push(Record {
                    topic: self.topic_name.clone(),
                    partition: self.partition_id,
                    offset: disk_record.offset,
                    message: disk_record.to_message(),
                });
            }
        }

        tracing::debug!(
            topic = %self.topic_name,
            partition = self.partition_id,
            offset = offset,
            count = all_records.len(),
            "Fetched messages from partition"
        );

        Ok(all_records)
    }

    /// Get the next available offset (total number of messages)
    pub async fn next_offset(&self) -> u64 {
        let segments = self.segments.read().await;
        if let Some(last_segment) = segments.last() {
            last_segment.next_offset().await
        } else {
            0
        }
    }

    /// Get partition ID
    pub fn partition_id(&self) -> u32 {
        self.partition_id
    }

    /// Flush all pending writes to disk
    pub async fn flush(&self) -> Result<()> {
        let segments = self.segments.read().await;
        for segment in segments.iter() {
            segment.flush().await?;
        }
        Ok(())
    }

    /// Find the index of the segment containing the given offset
    fn find_segment_index(&self, segments: &[Arc<LogSegment>], offset: u64) -> Result<usize> {
        // Binary search for the segment
        let mut left = 0;
        let mut right = segments.len();

        while left < right {
            let mid = (left + right) / 2;
            let base_offset = segments[mid].base_offset();

            if base_offset <= offset {
                left = mid + 1;
            } else {
                right = mid;
            }
        }

        if left > 0 {
            Ok(left - 1)
        } else {
            Err(GaffaError::InvalidOffset(offset))
        }
    }

    /// Get the partition directory path
    fn partition_dir(data_dir: &Path, topic: &str, partition_id: u32) -> PathBuf {
        data_dir.join(topic).join(format!("partition-{}", partition_id))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_partition_append_and_fetch() {
        let dir = tempfile::tempdir().unwrap();
        let partition = Partition::new("test-topic".to_string(), 0, dir.path()).unwrap();

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
    async fn test_partition_persistence() {
        let dir = tempfile::tempdir().unwrap();
        let topic = "test-topic".to_string();
        let partition_id = 0;

        // Create and write
        {
            let partition = Partition::new(topic.clone(), partition_id, dir.path()).unwrap();
            let messages = vec![
                Message::new(b"msg1".to_vec()),
                Message::new(b"msg2".to_vec()),
                Message::new(b"msg3".to_vec()),
            ];
            partition.append(messages).await.unwrap();
            partition.flush().await.unwrap();
        }

        // Reopen and verify
        {
            let partition = Partition::open(topic, partition_id, dir.path()).unwrap();
            let records = partition.fetch(0, 10).await.unwrap();
            assert_eq!(records.len(), 3);
            assert_eq!(records[0].message.value, b"msg1");
            assert_eq!(records[1].message.value, b"msg2");
            assert_eq!(records[2].message.value, b"msg3");
        }
    }

    #[tokio::test]
    async fn test_partition_segment_rotation() {
        let dir = tempfile::tempdir().unwrap();
        let config = SegmentConfig {
            max_size: 200, // Very small for testing
            index_interval: 5,
        };

        let partition = Partition::with_config(
            "test-topic".to_string(),
            0,
            dir.path(),
            config,
        )
        .unwrap();

        // Append enough data to trigger rotation
        for _ in 0..10 {
            let messages = vec![Message::new(vec![0xAB; 50])]; // 50 byte messages
            partition.append(messages).await.unwrap();
        }

        // Verify we can still read all messages
        let records = partition.fetch(0, 100).await.unwrap();
        assert_eq!(records.len(), 10);

        // Verify we have multiple segments
        let segments = partition.segments.read().await;
        assert!(segments.len() > 1, "Expected multiple segments after rotation");
    }

    #[tokio::test]
    async fn test_partition_multiple_batches() {
        let dir = tempfile::tempdir().unwrap();
        let partition = Partition::new("test-topic".to_string(), 0, dir.path()).unwrap();

        // First batch
        let batch1 = vec![
            Message::new(b"msg1".to_vec()),
            Message::new(b"msg2".to_vec()),
        ];
        let offset1 = partition.append(batch1).await.unwrap();
        assert_eq!(offset1, 0);

        // Second batch
        let batch2 = vec![Message::new(b"msg3".to_vec())];
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
        let dir = tempfile::tempdir().unwrap();
        let partition = Partition::new("test-topic".to_string(), 0, dir.path()).unwrap();

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
        let dir = tempfile::tempdir().unwrap();
        let partition = Partition::new("test-topic".to_string(), 0, dir.path()).unwrap();

        let messages = vec![Message::new(b"msg1".to_vec())];
        partition.append(messages).await.unwrap();

        // Try to fetch from beyond available offsets (should return empty)
        let records = partition.fetch(10, 1).await.unwrap();
        assert_eq!(records.len(), 0);
    }

    #[tokio::test]
    async fn test_partition_next_offset() {
        let dir = tempfile::tempdir().unwrap();
        let partition = Partition::new("test-topic".to_string(), 0, dir.path()).unwrap();

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
        let dir = tempfile::tempdir().unwrap();
        let partition = Partition::new("test-topic".to_string(), 0, dir.path()).unwrap();

        let result = partition.append(vec![]).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_partition_large_messages() {
        let dir = tempfile::tempdir().unwrap();
        let partition = Partition::new("test-topic".to_string(), 0, dir.path()).unwrap();

        // 1MB message
        let large_msg = vec![Message::new(vec![0xAB; 1024 * 1024])];
        partition.append(large_msg).await.unwrap();

        let records = partition.fetch(0, 1).await.unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].message.value.len(), 1024 * 1024);
    }
}
