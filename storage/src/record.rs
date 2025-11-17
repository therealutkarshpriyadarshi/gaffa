use crate::compression::{compress, decompress, CompressionType};
use common::{GaffaError, Result};
use protocol::Message;
use std::io::Read;

/// On-disk record format with CRC32 checksums for data integrity and optional compression
///
/// Format (Phase 6 - with compression):
/// [8 bytes: offset]
/// [4 bytes: total record length (excluding offset and length fields)]
/// [4 bytes: CRC32 checksum]
/// [1 byte: compression type (0=None, 1=Gzip, 2=Snappy, 3=Lz4)]
/// [8 bytes: timestamp]
/// [4 bytes: key length (-1 if null)]
/// [N bytes: key (if present)]
/// [4 bytes: value length]
/// [N bytes: value (possibly compressed)]
#[derive(Debug, Clone, PartialEq)]
pub struct DiskRecord {
    pub offset: u64,
    pub timestamp: u64,
    pub key: Option<Vec<u8>>,
    pub value: Vec<u8>,
    pub compression: CompressionType,
}

impl DiskRecord {
    /// Create a new disk record without compression
    pub fn new(offset: u64, message: Message) -> Self {
        Self {
            offset,
            timestamp: message.timestamp,
            key: message.key,
            value: message.value,
            compression: CompressionType::None,
        }
    }

    /// Create a new disk record with compression
    pub fn new_with_compression(offset: u64, message: Message, compression: CompressionType) -> Self {
        Self {
            offset,
            timestamp: message.timestamp,
            key: message.key,
            value: message.value,
            compression,
        }
    }

    /// Calculate the size of this record when serialized
    pub fn serialized_size(&self) -> usize {
        let mut size = 8 + 4 + 4 + 1 + 8 + 4; // offset + length + crc + compression + timestamp + key_length
        if let Some(ref key) = self.key {
            size += key.len();
        }
        size += 4; // value_length
        size += self.value.len();
        size
    }

    /// Encode the record to bytes with CRC32 checksum
    pub fn encode(&self) -> Result<Vec<u8>> {
        // Compress the value if needed
        let value_data = compress(&self.value, self.compression)?;

        // Calculate the body size (everything after offset and length fields)
        let mut body_size = 4 + 1 + 8 + 4; // crc + compression + timestamp + key_length
        if let Some(ref key) = self.key {
            body_size += key.len();
        }
        body_size += 4; // value_length
        body_size += value_data.len();

        let mut buffer = Vec::with_capacity(12 + body_size);

        // Write offset
        buffer.extend_from_slice(&self.offset.to_be_bytes());

        // Write total body length
        buffer.extend_from_slice(&(body_size as u32).to_be_bytes());

        // Prepare the data that will be checksummed (everything after CRC field)
        let mut data_to_checksum = Vec::new();

        // Write compression type
        data_to_checksum.push(self.compression.to_u8());

        // Write timestamp
        data_to_checksum.extend_from_slice(&self.timestamp.to_be_bytes());

        // Write key length and key
        if let Some(ref key) = self.key {
            data_to_checksum.extend_from_slice(&(key.len() as i32).to_be_bytes());
            data_to_checksum.extend_from_slice(key);
        } else {
            data_to_checksum.extend_from_slice(&(-1i32).to_be_bytes());
        }

        // Write value length and value (compressed)
        data_to_checksum.extend_from_slice(&(value_data.len() as u32).to_be_bytes());
        data_to_checksum.extend_from_slice(&value_data);

        // Calculate CRC32
        let crc = crc32fast::hash(&data_to_checksum);
        buffer.extend_from_slice(&crc.to_be_bytes());

        // Append the checksummed data
        buffer.extend_from_slice(&data_to_checksum);

        Ok(buffer)
    }

    /// Decode a record from bytes and verify CRC32 checksum
    pub fn decode(mut reader: impl Read) -> Result<Self> {
        // Read offset
        let mut offset_buf = [0u8; 8];
        reader.read_exact(&mut offset_buf).map_err(|e| {
            GaffaError::Storage(format!("Failed to read offset: {}", e))
        })?;
        let offset = u64::from_be_bytes(offset_buf);

        // Read length
        let mut length_buf = [0u8; 4];
        reader.read_exact(&mut length_buf).map_err(|e| {
            GaffaError::Storage(format!("Failed to read length: {}", e))
        })?;
        let length = u32::from_be_bytes(length_buf) as usize;

        // Read CRC32
        let mut crc_buf = [0u8; 4];
        reader.read_exact(&mut crc_buf).map_err(|e| {
            GaffaError::Storage(format!("Failed to read CRC: {}", e))
        })?;
        let expected_crc = u32::from_be_bytes(crc_buf);

        // Read the rest of the data
        let data_length = length - 4; // Subtract CRC size
        let mut data = vec![0u8; data_length];
        reader.read_exact(&mut data).map_err(|e| {
            GaffaError::Storage(format!("Failed to read record data: {}", e))
        })?;

        // Verify CRC32
        let actual_crc = crc32fast::hash(&data);
        if actual_crc != expected_crc {
            return Err(GaffaError::Corruption(format!(
                "CRC mismatch at offset {}: expected {}, got {}",
                offset, expected_crc, actual_crc
            )));
        }

        // Parse the data
        let mut cursor = std::io::Cursor::new(&data);

        // Read compression type
        let mut compression_buf = [0u8; 1];
        cursor.read_exact(&mut compression_buf).map_err(|e| {
            GaffaError::Storage(format!("Failed to read compression type: {}", e))
        })?;
        let compression = CompressionType::from_u8(compression_buf[0])?;

        // Read timestamp
        let mut timestamp_buf = [0u8; 8];
        cursor.read_exact(&mut timestamp_buf).map_err(|e| {
            GaffaError::Storage(format!("Failed to read timestamp: {}", e))
        })?;
        let timestamp = u64::from_be_bytes(timestamp_buf);

        // Read key
        let mut key_len_buf = [0u8; 4];
        cursor.read_exact(&mut key_len_buf).map_err(|e| {
            GaffaError::Storage(format!("Failed to read key length: {}", e))
        })?;
        let key_len = i32::from_be_bytes(key_len_buf);

        let key = if key_len >= 0 {
            let mut key_data = vec![0u8; key_len as usize];
            cursor.read_exact(&mut key_data).map_err(|e| {
                GaffaError::Storage(format!("Failed to read key: {}", e))
            })?;
            Some(key_data)
        } else {
            None
        };

        // Read value (compressed)
        let mut value_len_buf = [0u8; 4];
        cursor.read_exact(&mut value_len_buf).map_err(|e| {
            GaffaError::Storage(format!("Failed to read value length: {}", e))
        })?;
        let value_len = u32::from_be_bytes(value_len_buf);

        let mut value_data = vec![0u8; value_len as usize];
        cursor.read_exact(&mut value_data).map_err(|e| {
            GaffaError::Storage(format!("Failed to read value: {}", e))
        })?;

        // Decompress value if needed
        let value = decompress(&value_data, compression)?;

        Ok(DiskRecord {
            offset,
            timestamp,
            key,
            value,
            compression,
        })
    }

    /// Convert to a Message
    pub fn to_message(&self) -> Message {
        Message {
            key: self.key.clone(),
            value: self.value.clone(),
            timestamp: self.timestamp,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_record_encode_decode_with_key() {
        let message = Message::new(b"test value".to_vec())
            .with_key(b"test key".to_vec())
            .with_timestamp(1234567890);

        let record = DiskRecord::new(42, message);
        let encoded = record.encode().unwrap();

        let decoded = DiskRecord::decode(&encoded[..]).unwrap();
        assert_eq!(record, decoded);
        assert_eq!(decoded.offset, 42);
        assert_eq!(decoded.timestamp, 1234567890);
        assert_eq!(decoded.key, Some(b"test key".to_vec()));
        assert_eq!(decoded.value, b"test value");
    }

    #[test]
    fn test_record_encode_decode_without_key() {
        let message = Message::new(b"test value".to_vec())
            .with_timestamp(9876543210);

        let record = DiskRecord::new(0, message);
        let encoded = record.encode().unwrap();

        let decoded = DiskRecord::decode(&encoded[..]).unwrap();
        assert_eq!(record, decoded);
        assert_eq!(decoded.offset, 0);
        assert_eq!(decoded.timestamp, 9876543210);
        assert_eq!(decoded.key, None);
        assert_eq!(decoded.value, b"test value");
    }

    #[test]
    fn test_record_crc_corruption() {
        let message = Message::new(b"test value".to_vec());
        let record = DiskRecord::new(0, message);
        let mut encoded = record.encode().unwrap();

        // Corrupt the data (flip a bit in the value)
        let data_offset = encoded.len() - 5;
        encoded[data_offset] ^= 0xFF;

        let result = DiskRecord::decode(&encoded[..]);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), GaffaError::Corruption(_)));
    }

    #[test]
    fn test_record_serialized_size() {
        let message = Message::new(b"test".to_vec())
            .with_key(b"key".to_vec());
        let record = DiskRecord::new(0, message);

        let encoded = record.encode().unwrap();
        assert_eq!(encoded.len(), record.serialized_size());
    }

    #[test]
    fn test_record_to_message() {
        let original_message = Message::new(b"test".to_vec())
            .with_key(b"key".to_vec())
            .with_timestamp(12345);

        let record = DiskRecord::new(10, original_message.clone());
        let converted_message = record.to_message();

        assert_eq!(converted_message.key, original_message.key);
        assert_eq!(converted_message.value, original_message.value);
        assert_eq!(converted_message.timestamp, original_message.timestamp);
    }

    #[test]
    fn test_large_record() {
        let large_value = vec![0xABu8; 1024 * 1024]; // 1MB
        let message = Message::new(large_value.clone());
        let record = DiskRecord::new(999, message);

        let encoded = record.encode().unwrap();
        let decoded = DiskRecord::decode(&encoded[..]).unwrap();

        assert_eq!(decoded.value, large_value);
        assert_eq!(decoded.offset, 999);
    }
}
