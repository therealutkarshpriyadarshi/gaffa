use common::Result;
use memmap2::Mmap;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::Path;

/// Size of each index entry (8 bytes offset + 8 bytes position)
const INDEX_ENTRY_SIZE: usize = 16;

/// Offset index for fast lookups in log segments
///
/// Format: [offset: u64, position: u64] pairs
/// - offset: The logical offset of the message in the partition
/// - position: The physical byte position in the segment file
///
/// The index is memory-mapped for fast read access
#[derive(Debug)]
pub struct OffsetIndex {
    file: File,
    mmap: Option<Mmap>,
    size: usize, // Number of entries
}

impl OffsetIndex {
    /// Create a new offset index
    pub fn create(path: impl AsRef<Path>) -> Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path.as_ref())?;

        Ok(Self {
            file,
            mmap: None,
            size: 0,
        })
    }

    /// Open an existing offset index
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path.as_ref())?;

        let metadata = file.metadata()?;
        let file_size = metadata.len() as usize;
        let size = file_size / INDEX_ENTRY_SIZE;

        // Memory-map the file for fast reads
        let mmap = if file_size > 0 {
            Some(unsafe { Mmap::map(&file)? })
        } else {
            None
        };

        Ok(Self { file, mmap, size })
    }

    /// Append a new entry to the index
    pub fn append(&mut self, offset: u64, position: u64) -> Result<()> {
        // Write the entry to the file
        let mut entry = [0u8; INDEX_ENTRY_SIZE];
        entry[0..8].copy_from_slice(&offset.to_be_bytes());
        entry[8..16].copy_from_slice(&position.to_be_bytes());

        self.file.write_all(&entry)?;
        self.file.sync_data()?;

        self.size += 1;

        // Remap the file to include the new entry
        self.remap()?;

        Ok(())
    }

    /// Look up the position for a given offset
    /// Returns the position in the segment file, or None if not found
    pub fn lookup(&self, target_offset: u64) -> Option<u64> {
        if self.size == 0 {
            return None;
        }

        let mmap = self.mmap.as_ref()?;

        // Binary search for the largest offset <= target_offset
        let mut left = 0;
        let mut right = self.size;
        let mut result_position = None;

        while left < right {
            let mid = (left + right) / 2;
            let entry_offset = self.read_offset_at(mmap, mid);

            if entry_offset <= target_offset {
                result_position = Some(self.read_position_at(mmap, mid));
                left = mid + 1;
            } else {
                right = mid;
            }
        }

        result_position
    }

    /// Get the number of entries in the index
    pub fn size(&self) -> usize {
        self.size
    }

    /// Flush pending writes to disk
    pub fn flush(&mut self) -> Result<()> {
        self.file.sync_all()?;
        Ok(())
    }

    /// Remap the file after appending
    fn remap(&mut self) -> Result<()> {
        let metadata = self.file.metadata()?;
        let file_size = metadata.len() as usize;

        if file_size > 0 {
            self.mmap = Some(unsafe { Mmap::map(&self.file)? });
        }

        Ok(())
    }

    /// Read the offset at a given index position
    fn read_offset_at(&self, mmap: &Mmap, index: usize) -> u64 {
        let pos = index * INDEX_ENTRY_SIZE;
        let mut bytes = [0u8; 8];
        bytes.copy_from_slice(&mmap[pos..pos + 8]);
        u64::from_be_bytes(bytes)
    }

    /// Read the position at a given index position
    fn read_position_at(&self, mmap: &Mmap, index: usize) -> u64 {
        let pos = index * INDEX_ENTRY_SIZE + 8;
        let mut bytes = [0u8; 8];
        bytes.copy_from_slice(&mmap[pos..pos + 8]);
        u64::from_be_bytes(bytes)
    }

    /// Get all entries (for debugging/testing)
    #[cfg(test)]
    pub fn entries(&self) -> Vec<(u64, u64)> {
        if let Some(mmap) = &self.mmap {
            (0..self.size)
                .map(|i| {
                    let offset = self.read_offset_at(mmap, i);
                    let position = self.read_position_at(mmap, i);
                    (offset, position)
                })
                .collect()
        } else {
            Vec::new()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_index_create_and_append() {
        let dir = tempfile::tempdir().unwrap();
        let index_path = dir.path().join("test.index");

        let mut index = OffsetIndex::create(&index_path).unwrap();
        assert_eq!(index.size(), 0);

        index.append(0, 0).unwrap();
        index.append(1, 100).unwrap();
        index.append(2, 250).unwrap();

        assert_eq!(index.size(), 3);
        assert_eq!(index.lookup(0), Some(0));
        assert_eq!(index.lookup(1), Some(100));
        assert_eq!(index.lookup(2), Some(250));
    }

    #[test]
    fn test_index_lookup_exact_match() {
        let dir = tempfile::tempdir().unwrap();
        let index_path = dir.path().join("test.index");

        let mut index = OffsetIndex::create(&index_path).unwrap();
        index.append(0, 0).unwrap();
        index.append(5, 500).unwrap();
        index.append(10, 1000).unwrap();

        assert_eq!(index.lookup(0), Some(0));
        assert_eq!(index.lookup(5), Some(500));
        assert_eq!(index.lookup(10), Some(1000));
    }

    #[test]
    fn test_index_lookup_floor() {
        let dir = tempfile::tempdir().unwrap();
        let index_path = dir.path().join("test.index");

        let mut index = OffsetIndex::create(&index_path).unwrap();
        index.append(0, 0).unwrap();
        index.append(5, 500).unwrap();
        index.append(10, 1000).unwrap();

        // Should return the floor (largest offset <= target)
        assert_eq!(index.lookup(3), Some(0));
        assert_eq!(index.lookup(7), Some(500));
        assert_eq!(index.lookup(15), Some(1000));
    }

    #[test]
    fn test_index_persistence() {
        let dir = tempfile::tempdir().unwrap();
        let index_path = dir.path().join("test.index");

        // Create and write
        {
            let mut index = OffsetIndex::create(&index_path).unwrap();
            index.append(0, 0).unwrap();
            index.append(10, 1000).unwrap();
            index.append(20, 2000).unwrap();
            index.flush().unwrap();
        }

        // Reopen and verify
        {
            let index = OffsetIndex::open(&index_path).unwrap();
            assert_eq!(index.size(), 3);
            assert_eq!(index.lookup(0), Some(0));
            assert_eq!(index.lookup(10), Some(1000));
            assert_eq!(index.lookup(20), Some(2000));
        }
    }

    #[test]
    fn test_index_empty_lookup() {
        let dir = tempfile::tempdir().unwrap();
        let index_path = dir.path().join("test.index");

        let index = OffsetIndex::create(&index_path).unwrap();
        assert_eq!(index.lookup(0), None);
        assert_eq!(index.lookup(100), None);
    }

    #[test]
    fn test_index_entries() {
        let dir = tempfile::tempdir().unwrap();
        let index_path = dir.path().join("test.index");

        let mut index = OffsetIndex::create(&index_path).unwrap();
        index.append(0, 0).unwrap();
        index.append(5, 500).unwrap();
        index.append(10, 1000).unwrap();

        let entries = index.entries();
        assert_eq!(entries, vec![(0, 0), (5, 500), (10, 1000)]);
    }

    #[test]
    fn test_index_large_dataset() {
        let dir = tempfile::tempdir().unwrap();
        let index_path = dir.path().join("test.index");

        let mut index = OffsetIndex::create(&index_path).unwrap();

        // Insert 1000 entries
        for i in 0..1000 {
            index.append(i * 10, i * 1000).unwrap();
        }

        assert_eq!(index.size(), 1000);

        // Verify lookups
        assert_eq!(index.lookup(0), Some(0));
        assert_eq!(index.lookup(500 * 10), Some(500 * 1000));
        assert_eq!(index.lookup(999 * 10), Some(999 * 1000));

        // Floor lookups
        assert_eq!(index.lookup(505), Some(50 * 1000)); // Floor of 505 is offset 500 (index 50)
        assert_eq!(index.lookup(9999), Some(999 * 1000));
    }
}
