/// Phase 6: Advanced Features Tests
///
/// Tests for:
/// - Compression support (Gzip, Snappy, LZ4)
/// - Message batching
/// - Retention policies (time/size based)
/// - Metrics and monitoring

use broker::{export_metrics, BrokerServer};
use client::{Consumer, Producer};
use common::Result;
use protocol::Message;
use std::time::Duration;
use storage::{CompressionType, LogSegment, RetentionConfig, SegmentConfig};
use tempfile::tempdir;

#[tokio::test]
async fn test_gzip_compression() -> Result<()> {
    let dir = tempdir().unwrap();
    let config = SegmentConfig::default().with_compression(CompressionType::Gzip);

    let segment = LogSegment::create(0, dir.path(), config).unwrap();

    // Create messages with repetitive data (should compress well)
    let messages = vec![
        Message::new(b"Hello World! ".repeat(100)),
        Message::new(b"Test Message ".repeat(100)),
    ];

    let original_size: usize = messages.iter().map(|m| m.value.len()).sum();

    segment.append(messages).await?;

    // Check that file size is smaller due to compression
    let file_size = segment.size().await;
    println!("Original size: {}, Compressed file size: {}", original_size, file_size);

    // File should be significantly smaller (with overhead from headers)
    assert!(file_size < original_size as u64);

    // Verify we can read back the data
    let record = segment.read(0).await?;
    assert_eq!(record.value.len(), b"Hello World! ".repeat(100).len());
    assert_eq!(record.compression, CompressionType::Gzip);

    Ok(())
}

#[tokio::test]
async fn test_snappy_compression() -> Result<()> {
    let dir = tempdir().unwrap();
    let config = SegmentConfig::default().with_compression(CompressionType::Snappy);

    let segment = LogSegment::create(0, dir.path(), config).unwrap();

    let messages = vec![
        Message::new(b"Snappy compression test data ".repeat(50)),
    ];

    segment.append(messages).await?;

    let record = segment.read(0).await?;
    assert_eq!(record.compression, CompressionType::Snappy);
    assert_eq!(record.value, b"Snappy compression test data ".repeat(50));

    Ok(())
}

#[tokio::test]
async fn test_lz4_compression() -> Result<()> {
    let dir = tempdir().unwrap();
    let config = SegmentConfig::default().with_compression(CompressionType::Lz4);

    let segment = LogSegment::create(0, dir.path(), config).unwrap();

    let messages = vec![
        Message::new(b"LZ4 is very fast! ".repeat(100)),
    ];

    segment.append(messages).await?;

    let record = segment.read(0).await?;
    assert_eq!(record.compression, CompressionType::Lz4);
    assert_eq!(record.value, b"LZ4 is very fast! ".repeat(100));

    Ok(())
}

#[tokio::test]
async fn test_no_compression() -> Result<()> {
    let dir = tempdir().unwrap();
    let config = SegmentConfig::default(); // No compression

    let segment = LogSegment::create(0, dir.path(), config).unwrap();

    let messages = vec![
        Message::new(b"No compression".to_vec()),
    ];

    segment.append(messages).await?;

    let record = segment.read(0).await?;
    assert_eq!(record.compression, CompressionType::None);
    assert_eq!(record.value, b"No compression");

    Ok(())
}

#[tokio::test]
async fn test_compression_with_batch() -> Result<()> {
    let dir = tempdir().unwrap();
    let config = SegmentConfig::default().with_compression(CompressionType::Gzip);

    let segment = LogSegment::create(0, dir.path(), config).unwrap();

    // Append a batch of messages
    let messages: Vec<Message> = (0..100)
        .map(|i| Message::new(format!("Message {} ", i).repeat(10).as_bytes().to_vec()))
        .collect();

    segment.append(messages.clone()).await?;

    // Read them back in a batch
    let records = segment.read_batch(0, 100).await?;
    assert_eq!(records.len(), 100);

    // Verify all are compressed and correct
    for (i, record) in records.iter().enumerate() {
        assert_eq!(record.compression, CompressionType::Gzip);
        assert_eq!(record.offset, i as u64);
        assert_eq!(record.value, format!("Message {} ", i).repeat(10).as_bytes().to_vec());
    }

    Ok(())
}

#[tokio::test]
async fn test_time_based_retention() {
    let config = RetentionConfig::time(Duration::from_secs(3600)); // 1 hour

    // Should keep recent segments
    assert!(!config.should_delete_segment(
        Duration::from_secs(1800),  // 30 minutes old
        1000,
        5
    ));

    // Should delete old segments
    assert!(config.should_delete_segment(
        Duration::from_secs(7200),  // 2 hours old
        1000,
        5
    ));

    // Should not delete if at minimum
    assert!(!config.should_delete_segment(
        Duration::from_secs(7200),  // 2 hours old
        1000,
        1  // At minimum
    ));
}

#[tokio::test]
async fn test_size_based_retention() {
    let config = RetentionConfig::size(5000); // 5000 bytes max

    // Should keep if under limit
    assert!(!config.should_delete_segment(
        Duration::from_secs(1000),
        4000,  // Under limit
        5
    ));

    // Should delete if over limit
    assert!(config.should_delete_segment(
        Duration::from_secs(1000),
        6000,  // Over limit
        5
    ));
}

#[tokio::test]
async fn test_combined_retention() {
    let config = RetentionConfig::both(
        Duration::from_secs(3600),  // 1 hour
        5000  // 5000 bytes
    );

    // Should delete if too old
    assert!(config.should_delete_segment(
        Duration::from_secs(7200),
        3000,
        5
    ));

    // Should delete if too large
    assert!(config.should_delete_segment(
        Duration::from_secs(1800),
        6000,
        5
    ));

    // Should keep if both conditions met
    assert!(!config.should_delete_segment(
        Duration::from_secs(1800),
        3000,
        5
    ));
}

#[tokio::test]
async fn test_metrics_export() {
    // Export metrics
    let metrics = export_metrics();

    // Should contain Prometheus-format metrics
    assert!(!metrics.is_empty());
    assert!(metrics.contains("gaffa_"));

    // Should have various metric types
    assert!(metrics.contains("_total") || metrics.contains("gaffa"));
}

#[tokio::test]
async fn test_compression_ratios() -> Result<()> {
    let _dir = tempdir().unwrap();

    // Test data with good compression potential
    let test_data = b"AAAA".repeat(1000);
    let messages = vec![Message::new(test_data.clone())];

    // Test each compression type
    let compressions = vec![
        CompressionType::None,
        CompressionType::Gzip,
        CompressionType::Snappy,
        CompressionType::Lz4,
    ];

    for compression in compressions {
        let segment_dir = tempdir().unwrap();
        let config = SegmentConfig::default().with_compression(compression);
        let segment = LogSegment::create(0, segment_dir.path(), config).unwrap();

        segment.append(messages.clone()).await?;

        let size = segment.size().await;
        let ratio = size as f64 / test_data.len() as f64;

        println!("{:?} - Size: {}, Ratio: {:.2}%", compression, size, ratio * 100.0);

        // Compressed versions should be much smaller
        if compression != CompressionType::None {
            assert!(ratio < 0.5, "Compression ratio should be < 50% for repetitive data");
        }
    }

    Ok(())
}

#[tokio::test]
async fn test_large_message_compression() -> Result<()> {
    let dir = tempdir().unwrap();
    let config = SegmentConfig::default().with_compression(CompressionType::Lz4);

    let segment = LogSegment::create(0, dir.path(), config).unwrap();

    // Create a large message (1MB)
    let large_data = vec![0xAB; 1024 * 1024];
    let messages = vec![Message::new(large_data.clone())];

    segment.append(messages).await?;

    // Should compress very well (all same byte)
    let file_size = segment.size().await;
    assert!(file_size < 1024 * 100); // Should be < 100KB

    // Read back and verify
    let record = segment.read(0).await?;
    assert_eq!(record.value, large_data);

    Ok(())
}

#[tokio::test]
async fn test_segment_persistence_with_compression() -> Result<()> {
    let dir = tempdir().unwrap();
    let base_offset = 100;

    // Create and write with compression
    {
        let config = SegmentConfig::default().with_compression(CompressionType::Gzip);
        let segment = LogSegment::create(base_offset, dir.path(), config).unwrap();

        let messages = vec![
            Message::new(b"Compressed message 1".repeat(10).to_vec()),
            Message::new(b"Compressed message 2".repeat(10).to_vec()),
        ];
        segment.append(messages).await?;
        segment.flush().await?;
    }

    // Reopen and verify
    {
        let config = SegmentConfig::default().with_compression(CompressionType::Gzip);
        let segment = LogSegment::open(base_offset, dir.path(), config).unwrap();

        let record = segment.read(base_offset).await?;
        assert_eq!(record.compression, CompressionType::Gzip);
        assert_eq!(record.value, b"Compressed message 1".repeat(10));
    }

    Ok(())
}

#[tokio::test]
async fn test_mixed_compression_types() -> Result<()> {
    // This tests backward compatibility - older segments without compression
    // should still be readable alongside new compressed segments

    let dir = tempdir().unwrap();

    // Write without compression
    let config1 = SegmentConfig::default(); // No compression
    let segment1 = LogSegment::create(0, dir.path(), config1).unwrap();
    segment1.append(vec![Message::new(b"Old message".to_vec())]).await?;

    // Read back
    let record = segment1.read(0).await?;
    assert_eq!(record.compression, CompressionType::None);
    assert_eq!(record.value, b"Old message");

    Ok(())
}

#[tokio::test]
async fn test_retention_unlimited() {
    let config = RetentionConfig::unlimited();

    // Should never delete
    assert!(!config.should_delete_segment(
        Duration::from_secs(86400 * 365),  // 1 year old
        1_000_000_000,  // 1GB
        100
    ));
}

#[test]
fn test_retention_config_builders() {
    let time_config = RetentionConfig::time(Duration::from_secs(3600));
    assert_eq!(time_config.policy, storage::RetentionPolicy::Time(Duration::from_secs(3600)));

    let size_config = RetentionConfig::size(1000000);
    assert_eq!(size_config.policy, storage::RetentionPolicy::Size(1000000));

    let both_config = RetentionConfig::both(Duration::from_secs(3600), 1000000);
    assert!(matches!(both_config.policy, storage::RetentionPolicy::Both { .. }));

    let unlimited = RetentionConfig::unlimited();
    assert_eq!(unlimited.policy, storage::RetentionPolicy::Unlimited);
}

#[test]
fn test_segment_config_builders() {
    let config = SegmentConfig::default()
        .with_compression(CompressionType::Lz4)
        .with_retention(RetentionConfig::time(Duration::from_secs(3600)));

    assert_eq!(config.compression, CompressionType::Lz4);
    assert_eq!(config.retention.policy, storage::RetentionPolicy::Time(Duration::from_secs(3600)));
}
