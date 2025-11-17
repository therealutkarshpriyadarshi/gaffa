use common::{GaffaError, Result};
use futures::{SinkExt, StreamExt};
use protocol::{ClientCodec, Message, Request, Response};
use tokio::net::TcpStream;
use tokio_util::codec::Framed;

/// A producer client for sending messages to the broker
pub struct Producer {
    framed: Framed<TcpStream, ClientCodec>,
}

impl Producer {
    /// Connect to a broker
    pub async fn connect(addr: &str) -> Result<Self> {
        let stream = TcpStream::connect(addr)
            .await
            .map_err(|e| GaffaError::Connection(e.to_string()))?;

        let framed = Framed::new(stream, ClientCodec);

        tracing::info!("Producer connected to {}", addr);

        Ok(Self { framed })
    }

    /// Create a new topic
    pub async fn create_topic(&mut self, name: &str, partitions: u32) -> Result<()> {
        let request = Request::CreateTopic {
            name: name.to_string(),
            partitions,
        };

        self.framed
            .send(request)
            .await
            .map_err(|e| GaffaError::Connection(e.to_string()))?;

        let response = self
            .framed
            .next()
            .await
            .ok_or_else(|| GaffaError::Connection("Connection closed".to_string()))?
            .map_err(|e| GaffaError::Connection(e.to_string()))?;

        match response {
            Response::CreateTopicSuccess { .. } => {
                tracing::debug!("Topic '{}' created successfully", name);
                Ok(())
            }
            Response::CreateTopicError { error } => Err(GaffaError::Protocol(error)),
            _ => Err(GaffaError::Protocol("Unexpected response".to_string())),
        }
    }

    /// Send messages to a topic partition
    pub async fn send(
        &mut self,
        topic: &str,
        partition: u32,
        messages: Vec<Message>,
    ) -> Result<u64> {
        let request = Request::Produce {
            topic: topic.to_string(),
            partition,
            messages,
        };

        self.framed
            .send(request)
            .await
            .map_err(|e| GaffaError::Connection(e.to_string()))?;

        let response = self
            .framed
            .next()
            .await
            .ok_or_else(|| GaffaError::Connection("Connection closed".to_string()))?
            .map_err(|e| GaffaError::Connection(e.to_string()))?;

        match response {
            Response::ProduceSuccess { base_offset, .. } => {
                tracing::debug!(
                    "Messages sent successfully to {}:{}, base_offset={}",
                    topic,
                    partition,
                    base_offset
                );
                Ok(base_offset)
            }
            Response::ProduceError { error } => Err(GaffaError::Protocol(error)),
            _ => Err(GaffaError::Protocol("Unexpected response".to_string())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Integration tests would require a running broker
    // These are placeholder unit tests

    #[test]
    fn test_producer_creation() {
        // This is a placeholder test
        // Real tests would need a mock or running broker
        assert!(true);
    }
}
