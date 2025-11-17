use common::{GaffaError, Result};
use flate2::read::{GzDecoder, GzEncoder};
use flate2::Compression as GzCompression;
use serde::{Deserialize, Serialize};
use std::io::{Read, Write};

/// Compression algorithm for message batches
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u8)]
pub enum CompressionType {
    /// No compression
    None = 0,
    /// Gzip compression (good balance of speed and ratio)
    Gzip = 1,
    /// Snappy compression (very fast, lower ratio)
    Snappy = 2,
    /// LZ4 compression (extremely fast, good ratio)
    Lz4 = 3,
}

impl CompressionType {
    /// Parse compression type from u8
    pub fn from_u8(value: u8) -> Result<Self> {
        match value {
            0 => Ok(CompressionType::None),
            1 => Ok(CompressionType::Gzip),
            2 => Ok(CompressionType::Snappy),
            3 => Ok(CompressionType::Lz4),
            _ => Err(GaffaError::InvalidMessage(format!(
                "Unknown compression type: {}",
                value
            ))),
        }
    }

    /// Convert to u8
    pub fn to_u8(self) -> u8 {
        self as u8
    }
}

/// Compress data using the specified algorithm
pub fn compress(data: &[u8], compression: CompressionType) -> Result<Vec<u8>> {
    match compression {
        CompressionType::None => Ok(data.to_vec()),
        CompressionType::Gzip => compress_gzip(data),
        CompressionType::Snappy => compress_snappy(data),
        CompressionType::Lz4 => compress_lz4(data),
    }
}

/// Decompress data using the specified algorithm
pub fn decompress(data: &[u8], compression: CompressionType) -> Result<Vec<u8>> {
    match compression {
        CompressionType::None => Ok(data.to_vec()),
        CompressionType::Gzip => decompress_gzip(data),
        CompressionType::Snappy => decompress_snappy(data),
        CompressionType::Lz4 => decompress_lz4(data),
    }
}

/// Compress data using Gzip (level 6 - default balance)
fn compress_gzip(data: &[u8]) -> Result<Vec<u8>> {
    let mut encoder = GzEncoder::new(data, GzCompression::default());
    let mut compressed = Vec::new();
    encoder
        .read_to_end(&mut compressed)
        .map_err(|e| GaffaError::Compression(format!("Gzip compression failed: {}", e)))?;
    Ok(compressed)
}

/// Decompress Gzip data
fn decompress_gzip(data: &[u8]) -> Result<Vec<u8>> {
    let mut decoder = GzDecoder::new(data);
    let mut decompressed = Vec::new();
    decoder
        .read_to_end(&mut decompressed)
        .map_err(|e| GaffaError::Compression(format!("Gzip decompression failed: {}", e)))?;
    Ok(decompressed)
}

/// Compress data using Snappy
fn compress_snappy(data: &[u8]) -> Result<Vec<u8>> {
    let mut encoder = snap::write::FrameEncoder::new(Vec::new());
    encoder
        .write_all(data)
        .map_err(|e| GaffaError::Compression(format!("Snappy compression failed: {}", e)))?;
    encoder
        .into_inner()
        .map_err(|e| GaffaError::Compression(format!("Snappy compression failed: {}", e)))
}

/// Decompress Snappy data
fn decompress_snappy(data: &[u8]) -> Result<Vec<u8>> {
    let mut decoder = snap::read::FrameDecoder::new(data);
    let mut decompressed = Vec::new();
    decoder
        .read_to_end(&mut decompressed)
        .map_err(|e| GaffaError::Compression(format!("Snappy decompression failed: {}", e)))?;
    Ok(decompressed)
}

/// Compress data using LZ4
fn compress_lz4(data: &[u8]) -> Result<Vec<u8>> {
    Ok(lz4_flex::compress_prepend_size(data))
}

/// Decompress LZ4 data
fn decompress_lz4(data: &[u8]) -> Result<Vec<u8>> {
    lz4_flex::decompress_size_prepended(data)
        .map_err(|e| GaffaError::Compression(format!("LZ4 decompression failed: {:?}", e)))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_data() -> Vec<u8> {
        b"Hello, World! This is a test message that will be compressed. \
          It contains some repetitive data to ensure good compression ratios. \
          Repetitive data, repetitive data, repetitive data!".to_vec()
    }

    #[test]
    fn test_compression_type_from_u8() {
        assert_eq!(CompressionType::from_u8(0).unwrap(), CompressionType::None);
        assert_eq!(CompressionType::from_u8(1).unwrap(), CompressionType::Gzip);
        assert_eq!(CompressionType::from_u8(2).unwrap(), CompressionType::Snappy);
        assert_eq!(CompressionType::from_u8(3).unwrap(), CompressionType::Lz4);
        assert!(CompressionType::from_u8(99).is_err());
    }

    #[test]
    fn test_compression_type_to_u8() {
        assert_eq!(CompressionType::None.to_u8(), 0);
        assert_eq!(CompressionType::Gzip.to_u8(), 1);
        assert_eq!(CompressionType::Snappy.to_u8(), 2);
        assert_eq!(CompressionType::Lz4.to_u8(), 3);
    }

    #[test]
    fn test_no_compression() {
        let data = test_data();
        let compressed = compress(&data, CompressionType::None).unwrap();
        assert_eq!(compressed, data);

        let decompressed = decompress(&compressed, CompressionType::None).unwrap();
        assert_eq!(decompressed, data);
    }

    #[test]
    fn test_gzip_compression() {
        let data = test_data();
        let compressed = compress(&data, CompressionType::Gzip).unwrap();

        // Compression should reduce size for repetitive data
        assert!(compressed.len() < data.len());

        let decompressed = decompress(&compressed, CompressionType::Gzip).unwrap();
        assert_eq!(decompressed, data);
    }

    #[test]
    fn test_snappy_compression() {
        let data = test_data();
        let compressed = compress(&data, CompressionType::Snappy).unwrap();

        let decompressed = decompress(&compressed, CompressionType::Snappy).unwrap();
        assert_eq!(decompressed, data);
    }

    #[test]
    fn test_lz4_compression() {
        let data = test_data();
        let compressed = compress(&data, CompressionType::Lz4).unwrap();

        // Compression should reduce size
        assert!(compressed.len() < data.len());

        let decompressed = decompress(&compressed, CompressionType::Lz4).unwrap();
        assert_eq!(decompressed, data);
    }

    #[test]
    fn test_compression_empty_data() {
        let data = Vec::new();

        for compression in [
            CompressionType::None,
            CompressionType::Gzip,
            CompressionType::Snappy,
            CompressionType::Lz4,
        ] {
            let compressed = compress(&data, compression).unwrap();
            let decompressed = decompress(&compressed, compression).unwrap();
            assert_eq!(decompressed, data);
        }
    }

    #[test]
    fn test_compression_large_data() {
        // Test with 1MB of data
        let data = vec![0xAB; 1024 * 1024];

        for compression in [
            CompressionType::Gzip,
            CompressionType::Snappy,
            CompressionType::Lz4,
        ] {
            let compressed = compress(&data, compression).unwrap();

            // Should compress very well (all same byte)
            assert!(compressed.len() < data.len() / 10);

            let decompressed = decompress(&compressed, compression).unwrap();
            assert_eq!(decompressed, data);
        }
    }

    #[test]
    fn test_compression_ratios() {
        let data = test_data();

        let gzip = compress(&data, CompressionType::Gzip).unwrap();
        let snappy = compress(&data, CompressionType::Snappy).unwrap();
        let lz4 = compress(&data, CompressionType::Lz4).unwrap();

        println!("Original size: {}", data.len());
        println!("Gzip size: {} (ratio: {:.2}%)", gzip.len(), (gzip.len() as f64 / data.len() as f64) * 100.0);
        println!("Snappy size: {} (ratio: {:.2}%)", snappy.len(), (snappy.len() as f64 / data.len() as f64) * 100.0);
        println!("LZ4 size: {} (ratio: {:.2}%)", lz4.len(), (lz4.len() as f64 / data.len() as f64) * 100.0);

        // All should compress
        assert!(gzip.len() < data.len());
        assert!(snappy.len() < data.len());
        assert!(lz4.len() < data.len());
    }
}
