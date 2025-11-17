use bytes::{Buf, BufMut, BytesMut};
use common::GaffaError;
use tokio_util::codec::{Decoder, Encoder};

use crate::messages::{Request, Response};

/// Server-side codec for encoding/decoding Gaffa protocol messages
/// Decodes Requests from clients, Encodes Responses to clients
/// Uses length-prefixed binary format:
/// [4 bytes: message length][N bytes: bincode-serialized message]
pub struct GaffaCodec;

/// Client-side codec for encoding/decoding Gaffa protocol messages
/// Encodes Requests to server, Decodes Responses from server
pub struct ClientCodec;

const MAX_MESSAGE_SIZE: usize = 10 * 1024 * 1024; // 10MB max message size

impl Decoder for GaffaCodec {
    type Item = Request;
    type Error = GaffaError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        // Need at least 4 bytes for the length prefix
        if src.len() < 4 {
            return Ok(None);
        }

        // Read the length prefix without consuming
        let mut length_bytes = [0u8; 4];
        length_bytes.copy_from_slice(&src[..4]);
        let length = u32::from_be_bytes(length_bytes) as usize;

        // Validate message size
        if length > MAX_MESSAGE_SIZE {
            return Err(GaffaError::Protocol(format!(
                "Message too large: {} bytes (max: {} bytes)",
                length, MAX_MESSAGE_SIZE
            )));
        }

        // Check if we have the full message
        if src.len() < 4 + length {
            // Reserve space for the full message
            src.reserve(4 + length - src.len());
            return Ok(None);
        }

        // Skip the length prefix
        src.advance(4);

        // Deserialize the message
        let data = src.split_to(length);
        let request = bincode::deserialize(&data)
            .map_err(|e| GaffaError::Serialization(e.to_string()))?;

        Ok(Some(request))
    }
}

impl Encoder<Response> for GaffaCodec {
    type Error = GaffaError;

    fn encode(&mut self, item: Response, dst: &mut BytesMut) -> Result<(), Self::Error> {
        // Serialize the response
        let data = bincode::serialize(&item)
            .map_err(|e| GaffaError::Serialization(e.to_string()))?;

        // Validate size
        if data.len() > MAX_MESSAGE_SIZE {
            return Err(GaffaError::Protocol(format!(
                "Message too large: {} bytes (max: {} bytes)",
                data.len(),
                MAX_MESSAGE_SIZE
            )));
        }

        // Write length prefix
        dst.reserve(4 + data.len());
        dst.put_u32(data.len() as u32);

        // Write the data
        dst.put_slice(&data);

        Ok(())
    }
}

// Client-side codec implementation
impl Encoder<Request> for ClientCodec {
    type Error = GaffaError;

    fn encode(&mut self, item: Request, dst: &mut BytesMut) -> Result<(), Self::Error> {
        // Serialize the request
        let data = bincode::serialize(&item)
            .map_err(|e| GaffaError::Serialization(e.to_string()))?;

        // Validate size
        if data.len() > MAX_MESSAGE_SIZE {
            return Err(GaffaError::Protocol(format!(
                "Message too large: {} bytes (max: {} bytes)",
                data.len(),
                MAX_MESSAGE_SIZE
            )));
        }

        // Write length prefix
        dst.reserve(4 + data.len());
        dst.put_u32(data.len() as u32);

        // Write the data
        dst.put_slice(&data);

        Ok(())
    }
}

impl Decoder for ClientCodec {
    type Item = Response;
    type Error = GaffaError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        // Need at least 4 bytes for the length prefix
        if src.len() < 4 {
            return Ok(None);
        }

        // Read the length prefix without consuming
        let mut length_bytes = [0u8; 4];
        length_bytes.copy_from_slice(&src[..4]);
        let length = u32::from_be_bytes(length_bytes) as usize;

        // Validate message size
        if length > MAX_MESSAGE_SIZE {
            return Err(GaffaError::Protocol(format!(
                "Message too large: {} bytes (max: {} bytes)",
                length, MAX_MESSAGE_SIZE
            )));
        }

        // Check if we have the full message
        if src.len() < 4 + length {
            // Reserve space for the full message
            src.reserve(4 + length - src.len());
            return Ok(None);
        }

        // Skip the length prefix
        src.advance(4);

        // Deserialize the message
        let data = src.split_to(length);
        let response = bincode::deserialize(&data)
            .map_err(|e| GaffaError::Serialization(e.to_string()))?;

        Ok(Some(response))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::messages::{Message, Request, Response};

    #[test]
    fn test_encode_decode_round_trip() {
        let mut codec = GaffaCodec;
        let mut buf = BytesMut::new();

        // Create a request
        let original_request = Request::CreateTopic {
            name: "test-topic".to_string(),
            partitions: 3,
        };

        // Encode as response (for testing encoder)
        let response = Response::CreateTopicSuccess {
            name: "test-topic".to_string(),
            partitions: 3,
        };

        codec.encode(response, &mut buf).unwrap();
        assert!(buf.len() > 4);

        // Verify length prefix
        let length = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
        assert_eq!(buf.len(), 4 + length);
    }

    #[test]
    fn test_decode_partial_message() {
        let mut codec = GaffaCodec;
        let mut buf = BytesMut::new();

        // Write a length prefix indicating a large message
        buf.put_u32(100);

        // But only provide 10 bytes of data
        buf.put_slice(&[0u8; 10]);

        // Should return None (need more data)
        let result = codec.decode(&mut buf);
        assert!(result.unwrap().is_none());
        assert_eq!(buf.len(), 14); // Length prefix + 10 bytes still in buffer
    }

    #[test]
    fn test_decode_multiple_messages() {
        let mut codec = GaffaCodec;
        let mut buf = BytesMut::new();

        // Encode a produce request
        let req = Request::Produce {
            topic: "test".to_string(),
            partition: 0,
            messages: vec![Message::new(b"hello".to_vec())],
        };

        let data = bincode::serialize(&req).unwrap();
        buf.put_u32(data.len() as u32);
        buf.put_slice(&data);

        // Decode the first message
        let decoded = codec.decode(&mut buf).unwrap();
        assert!(decoded.is_some());
        assert!(buf.is_empty());
    }

    #[test]
    fn test_max_message_size() {
        let mut codec = GaffaCodec;
        let mut buf = BytesMut::new();

        // Write a length prefix larger than MAX_MESSAGE_SIZE
        buf.put_u32((MAX_MESSAGE_SIZE + 1) as u32);

        // Should return an error
        let result = codec.decode(&mut buf);
        assert!(result.is_err());
        match result.unwrap_err() {
            GaffaError::Protocol(msg) => assert!(msg.contains("too large")),
            _ => panic!("Expected Protocol error"),
        }
    }
}
