use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicU32, Ordering};

/// Trait for determining which partition a message should be sent to
pub trait Partitioner: Send + Sync {
    /// Determine the partition for a message
    ///
    /// # Arguments
    /// * `key` - Optional message key
    /// * `num_partitions` - Total number of partitions in the topic
    ///
    /// # Returns
    /// Partition ID (0 to num_partitions-1)
    fn partition(&self, key: Option<&[u8]>, num_partitions: u32) -> u32;
}

/// Round-robin partitioner that distributes messages evenly across partitions
///
/// This partitioner cycles through all partitions in order, ensuring even distribution
/// regardless of message keys. Good for load balancing when ordering is not important.
#[derive(Debug)]
pub struct RoundRobinPartitioner {
    counter: AtomicU32,
}

impl RoundRobinPartitioner {
    /// Create a new round-robin partitioner
    pub fn new() -> Self {
        Self {
            counter: AtomicU32::new(0),
        }
    }
}

impl Default for RoundRobinPartitioner {
    fn default() -> Self {
        Self::new()
    }
}

impl Partitioner for RoundRobinPartitioner {
    fn partition(&self, _key: Option<&[u8]>, num_partitions: u32) -> u32 {
        if num_partitions == 0 {
            return 0;
        }
        let count = self.counter.fetch_add(1, Ordering::Relaxed);
        count % num_partitions
    }
}

/// Key-hash partitioner that routes messages with the same key to the same partition
///
/// This partitioner uses a hash function on the message key to determine the partition.
/// Messages with the same key always go to the same partition, preserving ordering
/// within a key. If no key is provided, falls back to partition 0.
#[derive(Debug)]
pub struct KeyHashPartitioner;

impl KeyHashPartitioner {
    /// Create a new key-hash partitioner
    pub fn new() -> Self {
        Self
    }

    /// Hash a byte slice to a u32
    fn hash_bytes(bytes: &[u8]) -> u32 {
        let mut hasher = DefaultHasher::new();
        bytes.hash(&mut hasher);
        hasher.finish() as u32
    }
}

impl Default for KeyHashPartitioner {
    fn default() -> Self {
        Self::new()
    }
}

impl Partitioner for KeyHashPartitioner {
    fn partition(&self, key: Option<&[u8]>, num_partitions: u32) -> u32 {
        if num_partitions == 0 {
            return 0;
        }

        match key {
            Some(k) if !k.is_empty() => {
                let hash = Self::hash_bytes(k);
                hash % num_partitions
            }
            _ => 0, // Default to partition 0 if no key
        }
    }
}

/// Sticky partitioner that sticks to a partition until it's full
///
/// This partitioner chooses a random partition and sticks to it for a batch of messages,
/// improving batching efficiency. Good for high-throughput scenarios where ordering
/// within a batch is less important than throughput.
#[derive(Debug)]
pub struct StickyPartitioner {
    current_partition: AtomicU32,
    message_count: AtomicU32,
    batch_size: u32,
}

impl StickyPartitioner {
    /// Create a new sticky partitioner with the given batch size
    pub fn new(batch_size: u32) -> Self {
        Self {
            current_partition: AtomicU32::new(0),
            message_count: AtomicU32::new(0),
            batch_size,
        }
    }

    /// Create with default batch size of 100 messages
    pub fn with_default_batch() -> Self {
        Self::new(100)
    }
}

impl Default for StickyPartitioner {
    fn default() -> Self {
        Self::with_default_batch()
    }
}

impl Partitioner for StickyPartitioner {
    fn partition(&self, _key: Option<&[u8]>, num_partitions: u32) -> u32 {
        if num_partitions == 0 {
            return 0;
        }

        let count = self.message_count.fetch_add(1, Ordering::Relaxed);

        // Switch partition every batch_size messages
        // Use integer division to determine which batch we're in
        if count > 0 && count % self.batch_size == 0 {
            let current = self.current_partition.load(Ordering::Relaxed);
            let next = (current + 1) % num_partitions;
            self.current_partition.store(next, Ordering::Relaxed);
        }

        self.current_partition.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn test_round_robin_distribution() {
        let partitioner = RoundRobinPartitioner::new();
        let num_partitions = 3;

        let partitions: Vec<u32> = (0..9)
            .map(|_| partitioner.partition(None, num_partitions))
            .collect();

        // Should cycle through 0, 1, 2, 0, 1, 2, 0, 1, 2
        assert_eq!(partitions, vec![0, 1, 2, 0, 1, 2, 0, 1, 2]);
    }

    #[test]
    fn test_round_robin_even_distribution() {
        let partitioner = RoundRobinPartitioner::new();
        let num_partitions = 3;
        let num_messages = 300;

        let mut counts = HashMap::new();
        for _ in 0..num_messages {
            let partition = partitioner.partition(None, num_partitions);
            *counts.entry(partition).or_insert(0) += 1;
        }

        // Each partition should get exactly 100 messages
        for partition in 0..num_partitions {
            assert_eq!(*counts.get(&partition).unwrap(), 100);
        }
    }

    #[test]
    fn test_key_hash_same_key_same_partition() {
        let partitioner = KeyHashPartitioner::new();
        let num_partitions = 5;
        let key = b"user-123";

        // Same key should always go to same partition
        let partition1 = partitioner.partition(Some(key), num_partitions);
        let partition2 = partitioner.partition(Some(key), num_partitions);
        let partition3 = partitioner.partition(Some(key), num_partitions);

        assert_eq!(partition1, partition2);
        assert_eq!(partition2, partition3);
    }

    #[test]
    fn test_key_hash_different_keys_distributed() {
        let partitioner = KeyHashPartitioner::new();
        let num_partitions = 5;

        let keys: Vec<&[u8]> = vec![
            b"key-1", b"key-2", b"key-3", b"key-4", b"key-5",
            b"key-6", b"key-7", b"key-8", b"key-9", b"key-10",
        ];

        let mut counts = HashMap::new();
        for key in keys {
            let partition = partitioner.partition(Some(key), num_partitions);
            *counts.entry(partition).or_insert(0) += 1;
        }

        // Should use more than one partition
        assert!(counts.len() > 1);
    }

    #[test]
    fn test_key_hash_no_key_uses_zero() {
        let partitioner = KeyHashPartitioner::new();
        let num_partitions = 5;

        let partition = partitioner.partition(None, num_partitions);
        assert_eq!(partition, 0);
    }

    #[test]
    fn test_key_hash_empty_key_uses_zero() {
        let partitioner = KeyHashPartitioner::new();
        let num_partitions = 5;

        let partition = partitioner.partition(Some(&[]), num_partitions);
        assert_eq!(partition, 0);
    }

    #[test]
    fn test_sticky_partitioner() {
        let partitioner = StickyPartitioner::new(3);
        let num_partitions = 3;

        // First 3 messages should go to partition 0
        assert_eq!(partitioner.partition(None, num_partitions), 0);
        assert_eq!(partitioner.partition(None, num_partitions), 0);
        assert_eq!(partitioner.partition(None, num_partitions), 0);

        // Next 3 messages should go to partition 1
        assert_eq!(partitioner.partition(None, num_partitions), 1);
        assert_eq!(partitioner.partition(None, num_partitions), 1);
        assert_eq!(partitioner.partition(None, num_partitions), 1);

        // Next 3 messages should go to partition 2
        assert_eq!(partitioner.partition(None, num_partitions), 2);
        assert_eq!(partitioner.partition(None, num_partitions), 2);
        assert_eq!(partitioner.partition(None, num_partitions), 2);

        // Cycle back to partition 0
        assert_eq!(partitioner.partition(None, num_partitions), 0);
    }

    #[test]
    fn test_partitioner_with_zero_partitions() {
        let rr = RoundRobinPartitioner::new();
        let kh = KeyHashPartitioner::new();
        let st = StickyPartitioner::new(10);

        assert_eq!(rr.partition(None, 0), 0);
        assert_eq!(kh.partition(Some(b"key"), 0), 0);
        assert_eq!(st.partition(None, 0), 0);
    }

    #[test]
    fn test_partitioner_with_single_partition() {
        let rr = RoundRobinPartitioner::new();
        let kh = KeyHashPartitioner::new();

        for _ in 0..10 {
            assert_eq!(rr.partition(None, 1), 0);
            assert_eq!(kh.partition(Some(b"any-key"), 1), 0);
        }
    }
}
