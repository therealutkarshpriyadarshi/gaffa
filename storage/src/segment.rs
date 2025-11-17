use crate::index::OffsetIndex;
use crate::record::DiskRecord;
use common::{GaffaError, Result};
use memmap2::Mmap;
use protocol::Message;
use std::fs::{File, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::RwLock;

/// Default maximum segment size (1GB)
pub const DEFAULT_MAX_SEGMENT_SIZE: u64 = 1024 * 1024 * 1024;

/// Log segment configuration
#[derive(Debug, Clone)]
pub struct SegmentConfig {
    /// Maximum size in bytes before rotation
    pub max_size: u64,
    /// Interval for indexing (index every N records)
    pub index_interval: u32,
}

impl Default for SegmentConfig {
    fn default() -> Self {
        Self {
            max_size: DEFAULT_MAX_SEGMENT_SIZE,
            index_interval: 10, // Index every 10 records
        }
    }
}

/// A log segment represents a single file on disk containing records
/// Segments are immutable once closed and support fast reads via memory-mapping
#[derive(Debug)]
pub struct LogSegment {
    /// Base offset for this segment
    base_offset: u64,
    /// Path to the log file
    log_path: PathBuf,
    /// Path to the index file
    index_path: PathBuf,
    /// Log file handle
    log_file: Arc<RwLock<File>>,
    /// Memory-mapped log file for reads
    mmap: Arc<RwLock<Option<Mmap>>>,
    /// Offset index
    index: Arc<RwLock<OffsetIndex>>,
    /// Current size of the log file in bytes
    size: Arc<RwLock<u64>>,
    /// Next offset to be written
    next_offset: Arc<RwLock<u64>>,
    /// Configuration
    config: SegmentConfig,
    /// Number of records since last index entry
    records_since_index: Arc<RwLock<u32>>,
}

impl LogSegment {
    /// Create a new log segment
    pub fn create(
        base_offset: u64,
        dir: impl AsRef<Path>,
        config: SegmentConfig,
    ) -> Result<Self> {
        let log_path = Self::log_path(dir.as_ref(), base_offset);
        let index_path = Self::index_path(dir.as_ref(), base_offset);

        let log_file = OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .open(&log_path)?;

        let index = OffsetIndex::create(&index_path)?;

        Ok(Self {
            base_offset,
            log_path,
            index_path,
            log_file: Arc::new(RwLock::new(log_file)),
            mmap: Arc::new(RwLock::new(None)),
            index: Arc::new(RwLock::new(index)),
            size: Arc::new(RwLock::new(0)),
            next_offset: Arc::new(RwLock::new(base_offset)),
            config,
            records_since_index: Arc::new(RwLock::new(0)),
        })
    }

    /// Open an existing log segment
    pub fn open(
        base_offset: u64,
        dir: impl AsRef<Path>,
        config: SegmentConfig,
    ) -> Result<Self> {
        let log_path = Self::log_path(dir.as_ref(), base_offset);
        let index_path = Self::index_path(dir.as_ref(), base_offset);

        let log_file = OpenOptions::new()
            .write(true)
            .read(true)
            .open(&log_path)?;

        let metadata = log_file.metadata()?;
        let size = metadata.len();

        let index = OffsetIndex::open(&index_path)?;

        // Memory-map the file for reads
        let mmap = if size > 0 {
            Some(unsafe { Mmap::map(&log_file)? })
        } else {
            None
        };

        // Scan to find the next offset
        let next_offset = Self::scan_for_next_offset(&log_file, base_offset)?;

        Ok(Self {
            base_offset,
            log_path,
            index_path,
            log_file: Arc::new(RwLock::new(log_file)),
            mmap: Arc::new(RwLock::new(mmap)),
            index: Arc::new(RwLock::new(index)),
            size: Arc::new(RwLock::new(size)),
            next_offset: Arc::new(RwLock::new(next_offset)),
            config,
            records_since_index: Arc::new(RwLock::new(0)),
        })
    }

    /// Append messages to the segment
    /// Returns the base offset where messages were written
    pub async fn append(&self, messages: Vec<Message>) -> Result<u64> {
        if messages.is_empty() {
            return Err(GaffaError::InvalidMessage("No messages to append".to_string()));
        }

        let mut log_file = self.log_file.write().await;
        let mut index = self.index.write().await;
        let mut size = self.size.write().await;
        let mut next_offset = self.next_offset.write().await;
        let mut records_since_index = self.records_since_index.write().await;

        let base_offset = *next_offset;
        let message_count = messages.len();

        // Move to end of file
        log_file.seek(SeekFrom::End(0))?;
        let mut current_position = *size;

        for message in messages {
            let record = DiskRecord::new(*next_offset, message);
            let encoded = record.encode()?;

            // Write to log file
            log_file.write_all(&encoded)?;

            // Update index every N records
            if *records_since_index >= self.config.index_interval {
                index.append(*next_offset, current_position)?;
                *records_since_index = 0;
            }

            current_position += encoded.len() as u64;
            *next_offset += 1;
            *records_since_index += 1;
        }

        // Flush to disk
        log_file.sync_data()?;
        *size = current_position;

        // Remap for reads
        drop(log_file);
        self.remap().await?;

        tracing::debug!(
            base_offset = base_offset,
            count = message_count,
            size = current_position,
            "Appended messages to segment"
        );

        Ok(base_offset)
    }

    /// Read a record at a specific offset
    pub async fn read(&self, offset: u64) -> Result<DiskRecord> {
        if offset < self.base_offset {
            return Err(GaffaError::InvalidOffset(offset));
        }

        let next_offset = *self.next_offset.read().await;
        if offset >= next_offset {
            return Err(GaffaError::InvalidOffset(offset));
        }

        let mmap = self.mmap.read().await;
        let mmap_ref = mmap.as_ref().ok_or_else(|| {
            GaffaError::Storage("Segment not memory-mapped".to_string())
        })?;

        // Try to find the position using the index
        let index = self.index.read().await;
        let position = index.lookup(offset).unwrap_or(0);
        drop(index);

        // Scan from the indexed position to find the exact record
        let mut cursor = std::io::Cursor::new(&mmap_ref[position as usize..]);

        loop {
            let record = DiskRecord::decode(&mut cursor)?;
            if record.offset == offset {
                return Ok(record);
            }
            if record.offset > offset {
                return Err(GaffaError::InvalidOffset(offset));
            }
        }
    }

    /// Read multiple records starting from an offset
    pub async fn read_batch(&self, start_offset: u64, max_count: u32) -> Result<Vec<DiskRecord>> {
        if start_offset < self.base_offset {
            return Err(GaffaError::InvalidOffset(start_offset));
        }

        let next_offset = *self.next_offset.read().await;
        if start_offset >= next_offset {
            return Ok(Vec::new());
        }

        let mmap = self.mmap.read().await;
        let mmap_ref = mmap.as_ref().ok_or_else(|| {
            GaffaError::Storage("Segment not memory-mapped".to_string())
        })?;

        // Find starting position
        let index = self.index.read().await;
        let position = index.lookup(start_offset).unwrap_or(0);
        drop(index);

        let mut cursor = std::io::Cursor::new(&mmap_ref[position as usize..]);
        let mut records = Vec::new();
        let mut found_start = false;

        while records.len() < max_count as usize {
            match DiskRecord::decode(&mut cursor) {
                Ok(record) => {
                    if record.offset >= start_offset {
                        found_start = true;
                        records.push(record);
                    } else if !found_start {
                        // Keep scanning until we find the start offset
                        continue;
                    }
                }
                Err(_) => break, // End of segment
            }
        }

        Ok(records)
    }

    /// Get the base offset of this segment
    pub fn base_offset(&self) -> u64 {
        self.base_offset
    }

    /// Get the next offset that will be written
    pub async fn next_offset(&self) -> u64 {
        *self.next_offset.read().await
    }

    /// Get the current size of the segment in bytes
    pub async fn size(&self) -> u64 {
        *self.size.read().await
    }

    /// Check if the segment should be rotated based on size
    pub async fn should_rotate(&self) -> bool {
        self.size().await >= self.config.max_size
    }

    /// Flush pending writes to disk
    pub async fn flush(&self) -> Result<()> {
        let mut log_file = self.log_file.write().await;
        log_file.sync_all()?;

        let mut index = self.index.write().await;
        index.flush()?;

        Ok(())
    }

    /// Remap the file after writing
    async fn remap(&self) -> Result<()> {
        let log_file = self.log_file.read().await;
        let metadata = log_file.metadata()?;
        let file_size = metadata.len();

        let mut mmap = self.mmap.write().await;
        if file_size > 0 {
            *mmap = Some(unsafe { Mmap::map(&*log_file)? });
        }

        Ok(())
    }

    /// Scan the log file to find the next offset
    fn scan_for_next_offset(log_file: &File, base_offset: u64) -> Result<u64> {
        let metadata = log_file.metadata()?;
        if metadata.len() == 0 {
            return Ok(base_offset);
        }

        let mmap = unsafe { Mmap::map(log_file)? };
        let mut cursor = std::io::Cursor::new(&mmap[..]);
        let mut last_offset = base_offset;

        while let Ok(record) = DiskRecord::decode(&mut cursor) {
            last_offset = record.offset;
        }

        Ok(last_offset + 1)
    }

    /// Get the log file path for a given base offset
    fn log_path(dir: &Path, base_offset: u64) -> PathBuf {
        dir.join(format!("{:020}.log", base_offset))
    }

    /// Get the index file path for a given base offset
    fn index_path(dir: &Path, base_offset: u64) -> PathBuf {
        dir.join(format!("{:020}.index", base_offset))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_segment_create_and_append() {
        let dir = tempfile::tempdir().unwrap();
        let segment = LogSegment::create(0, dir.path(), SegmentConfig::default()).unwrap();

        let messages = vec![
            Message::new(b"msg1".to_vec()),
            Message::new(b"msg2".to_vec()),
            Message::new(b"msg3".to_vec()),
        ];

        let base_offset = segment.append(messages).await.unwrap();
        assert_eq!(base_offset, 0);
        assert_eq!(segment.next_offset().await, 3);
    }

    #[tokio::test]
    async fn test_segment_read() {
        let dir = tempfile::tempdir().unwrap();
        let segment = LogSegment::create(0, dir.path(), SegmentConfig::default()).unwrap();

        let messages = vec![
            Message::new(b"msg1".to_vec()),
            Message::new(b"msg2".to_vec()),
            Message::new(b"msg3".to_vec()),
        ];

        segment.append(messages).await.unwrap();

        let record = segment.read(1).await.unwrap();
        assert_eq!(record.offset, 1);
        assert_eq!(record.value, b"msg2");
    }

    #[tokio::test]
    async fn test_segment_read_batch() {
        let dir = tempfile::tempdir().unwrap();
        let segment = LogSegment::create(0, dir.path(), SegmentConfig::default()).unwrap();

        let messages = vec![
            Message::new(b"msg1".to_vec()),
            Message::new(b"msg2".to_vec()),
            Message::new(b"msg3".to_vec()),
            Message::new(b"msg4".to_vec()),
            Message::new(b"msg5".to_vec()),
        ];

        segment.append(messages).await.unwrap();

        let records = segment.read_batch(1, 3).await.unwrap();
        assert_eq!(records.len(), 3);
        assert_eq!(records[0].offset, 1);
        assert_eq!(records[1].offset, 2);
        assert_eq!(records[2].offset, 3);
    }

    #[tokio::test]
    async fn test_segment_persistence() {
        let dir = tempfile::tempdir().unwrap();
        let base_offset = 100;

        // Create and write
        {
            let segment = LogSegment::create(base_offset, dir.path(), SegmentConfig::default()).unwrap();
            let messages = vec![
                Message::new(b"msg1".to_vec()),
                Message::new(b"msg2".to_vec()),
            ];
            segment.append(messages).await.unwrap();
            segment.flush().await.unwrap();
        }

        // Reopen and verify
        {
            let segment = LogSegment::open(base_offset, dir.path(), SegmentConfig::default()).unwrap();
            assert_eq!(segment.base_offset(), base_offset);
            assert_eq!(segment.next_offset().await, base_offset + 2);

            let record = segment.read(base_offset).await.unwrap();
            assert_eq!(record.value, b"msg1");
        }
    }

    #[tokio::test]
    async fn test_segment_should_rotate() {
        let dir = tempfile::tempdir().unwrap();
        let config = SegmentConfig {
            max_size: 100, // Very small for testing
            index_interval: 5,
        };

        let segment = LogSegment::create(0, dir.path(), config).unwrap();
        assert!(!segment.should_rotate().await);

        // Append enough data to exceed max_size
        let large_message = vec![Message::new(vec![0xAB; 50])];
        segment.append(large_message.clone()).await.unwrap();
        segment.append(large_message).await.unwrap();

        assert!(segment.should_rotate().await);
    }

    #[tokio::test]
    async fn test_segment_indexing() {
        let dir = tempfile::tempdir().unwrap();
        let config = SegmentConfig {
            max_size: DEFAULT_MAX_SEGMENT_SIZE,
            index_interval: 2, // Index every 2 records
        };

        let segment = LogSegment::create(0, dir.path(), config).unwrap();

        // Append 10 messages
        for i in 0..10 {
            let msg = vec![Message::new(format!("msg{}", i).into_bytes())];
            segment.append(msg).await.unwrap();
        }

        segment.flush().await.unwrap();

        // Verify we can read any offset efficiently
        for i in 0..10 {
            let record = segment.read(i).await.unwrap();
            assert_eq!(record.offset, i);
        }
    }

    #[tokio::test]
    async fn test_segment_invalid_offset() {
        let dir = tempfile::tempdir().unwrap();
        let segment = LogSegment::create(100, dir.path(), SegmentConfig::default()).unwrap();

        let messages = vec![Message::new(b"msg1".to_vec())];
        segment.append(messages).await.unwrap();

        // Try to read before base offset
        let result = segment.read(50).await;
        assert!(result.is_err());

        // Try to read beyond available offsets
        let result = segment.read(200).await;
        assert!(result.is_err());
    }
}
