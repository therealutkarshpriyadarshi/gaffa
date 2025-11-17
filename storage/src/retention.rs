use serde::{Deserialize, Serialize};
use std::time::Duration;

/// Retention policy for log segments
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum RetentionPolicy {
    /// Delete segments older than the specified duration
    Time(Duration),
    /// Delete oldest segments when total size exceeds the limit
    Size(u64),
    /// Apply both time and size retention (delete if either condition is met)
    Both {
        max_age: Duration,
        max_size: u64,
    },
    /// Keep all data forever (no retention)
    Unlimited,
}

/// Configuration for retention policies
#[derive(Debug, Clone)]
pub struct RetentionConfig {
    /// Retention policy to apply
    pub policy: RetentionPolicy,
    /// Minimum number of segments to always keep (safety limit)
    pub min_segments: usize,
}

impl Default for RetentionConfig {
    fn default() -> Self {
        Self {
            // Default: keep data for 7 days
            policy: RetentionPolicy::Time(Duration::from_secs(7 * 24 * 60 * 60)),
            min_segments: 1, // Always keep at least one segment
        }
    }
}

impl RetentionConfig {
    /// Create a time-based retention config
    pub fn time(max_age: Duration) -> Self {
        Self {
            policy: RetentionPolicy::Time(max_age),
            min_segments: 1,
        }
    }

    /// Create a size-based retention config
    pub fn size(max_size: u64) -> Self {
        Self {
            policy: RetentionPolicy::Size(max_size),
            min_segments: 1,
        }
    }

    /// Create a combined time and size retention config
    pub fn both(max_age: Duration, max_size: u64) -> Self {
        Self {
            policy: RetentionPolicy::Both { max_age, max_size },
            min_segments: 1,
        }
    }

    /// Create unlimited retention (keep all data)
    pub fn unlimited() -> Self {
        Self {
            policy: RetentionPolicy::Unlimited,
            min_segments: 1,
        }
    }

    /// Set minimum segments to keep
    pub fn with_min_segments(mut self, min: usize) -> Self {
        self.min_segments = min;
        self
    }

    /// Check if a segment should be deleted based on the retention policy
    pub fn should_delete_segment(
        &self,
        segment_age: Duration,
        total_size: u64,
        segment_count: usize,
    ) -> bool {
        // Never delete if below minimum segment count
        if segment_count <= self.min_segments {
            return false;
        }

        match &self.policy {
            RetentionPolicy::Unlimited => false,
            RetentionPolicy::Time(max_age) => segment_age > *max_age,
            RetentionPolicy::Size(max_size) => total_size > *max_size,
            RetentionPolicy::Both { max_age, max_size } => {
                segment_age > *max_age || total_size > *max_size
            }
        }
    }
}

/// Metadata for a segment used in retention decisions
#[derive(Debug, Clone)]
pub struct SegmentMetadata {
    /// Base offset of the segment
    pub base_offset: u64,
    /// Size in bytes
    pub size: u64,
    /// Creation timestamp (seconds since epoch)
    pub created_at: u64,
}

impl SegmentMetadata {
    /// Calculate the age of this segment
    pub fn age(&self) -> Duration {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        Duration::from_secs(now.saturating_sub(self.created_at))
    }
}

/// Manager for applying retention policies to segments
pub struct RetentionManager {
    config: RetentionConfig,
}

impl RetentionManager {
    /// Create a new retention manager
    pub fn new(config: RetentionConfig) -> Self {
        Self { config }
    }

    /// Determine which segments should be deleted based on retention policy
    pub fn segments_to_delete(&self, segments: &[SegmentMetadata]) -> Vec<u64> {
        if segments.is_empty() {
            return Vec::new();
        }

        let mut to_delete = Vec::new();
        let total_size: u64 = segments.iter().map(|s| s.size).sum();
        let segment_count = segments.len();

        // Process segments from oldest to newest
        let mut cumulative_size = total_size;
        for (idx, segment) in segments.iter().enumerate() {
            let remaining_count = segment_count - idx;

            if self.config.should_delete_segment(
                segment.age(),
                cumulative_size,
                remaining_count,
            ) {
                to_delete.push(segment.base_offset);
                cumulative_size -= segment.size;
            } else {
                // Once we find a segment we should keep, keep all newer segments too
                break;
            }
        }

        to_delete
    }

    /// Get the current retention configuration
    pub fn config(&self) -> &RetentionConfig {
        &self.config
    }

    /// Update the retention configuration
    pub fn update_config(&mut self, config: RetentionConfig) {
        self.config = config;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_segment(base_offset: u64, size: u64, age_secs: u64) -> SegmentMetadata {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        SegmentMetadata {
            base_offset,
            size,
            created_at: now - age_secs,
        }
    }

    #[test]
    fn test_time_retention_policy() {
        let config = RetentionConfig::time(Duration::from_secs(3600)); // 1 hour

        let segments = vec![
            create_segment(0, 1000, 7200),    // 2 hours old - should delete
            create_segment(100, 1000, 5400),  // 1.5 hours old - should delete
            create_segment(200, 1000, 1800),  // 30 min old - keep
            create_segment(300, 1000, 900),   // 15 min old - keep
        ];

        let manager = RetentionManager::new(config);
        let to_delete = manager.segments_to_delete(&segments);

        assert_eq!(to_delete.len(), 2);
        assert!(to_delete.contains(&0));
        assert!(to_delete.contains(&100));
    }

    #[test]
    fn test_size_retention_policy() {
        let config = RetentionConfig::size(2500); // Max 2500 bytes

        let segments = vec![
            create_segment(0, 1000, 3600),    // Old, should delete
            create_segment(100, 1000, 1800),  // Should delete to meet size limit
            create_segment(200, 1000, 900),   // Keep
            create_segment(300, 500, 300),    // Keep
        ];

        let manager = RetentionManager::new(config);
        let to_delete = manager.segments_to_delete(&segments);

        assert_eq!(to_delete.len(), 1);
        assert!(to_delete.contains(&0));
    }

    #[test]
    fn test_combined_retention_policy() {
        let config = RetentionConfig::both(Duration::from_secs(3600), 5000);

        let segments = vec![
            create_segment(0, 1000, 7200),    // Too old - delete
            create_segment(100, 1000, 1800),  // Within age, keep
            create_segment(200, 1000, 900),   // Keep
        ];

        let manager = RetentionManager::new(config);
        let to_delete = manager.segments_to_delete(&segments);

        assert_eq!(to_delete.len(), 1);
        assert!(to_delete.contains(&0));
    }

    #[test]
    fn test_min_segments_protection() {
        let config = RetentionConfig::time(Duration::from_secs(60))
            .with_min_segments(2);

        let segments = vec![
            create_segment(0, 1000, 7200),    // Very old
            create_segment(100, 1000, 3600),  // Old
            create_segment(200, 1000, 1800),  // Old
            create_segment(300, 1000, 30),    // Recent
        ];

        let manager = RetentionManager::new(config);
        let to_delete = manager.segments_to_delete(&segments);

        // Should keep at least 2 segments
        assert!(segments.len() - to_delete.len() >= 2);
    }

    #[test]
    fn test_unlimited_retention() {
        let config = RetentionConfig::unlimited();

        let segments = vec![
            create_segment(0, 1000, 86400 * 365), // 1 year old
            create_segment(100, 1000, 86400 * 30), // 30 days old
            create_segment(200, 1000, 3600),      // 1 hour old
        ];

        let manager = RetentionManager::new(config);
        let to_delete = manager.segments_to_delete(&segments);

        assert_eq!(to_delete.len(), 0);
    }

    #[test]
    fn test_empty_segments() {
        let config = RetentionConfig::time(Duration::from_secs(3600));
        let manager = RetentionManager::new(config);
        let to_delete = manager.segments_to_delete(&[]);

        assert_eq!(to_delete.len(), 0);
    }

    #[test]
    fn test_update_config() {
        let mut manager = RetentionManager::new(RetentionConfig::unlimited());
        assert_eq!(manager.config().policy, RetentionPolicy::Unlimited);

        manager.update_config(RetentionConfig::time(Duration::from_secs(3600)));
        assert_eq!(manager.config().policy, RetentionPolicy::Time(Duration::from_secs(3600)));
    }

    #[test]
    fn test_segment_age_calculation() {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let segment = SegmentMetadata {
            base_offset: 0,
            size: 1000,
            created_at: now - 3600,
        };

        let age = segment.age();
        // Should be approximately 1 hour (3600 seconds)
        assert!(age.as_secs() >= 3599 && age.as_secs() <= 3601);
    }
}
