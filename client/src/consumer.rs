use common::{GaffaError, Result};
use futures::{SinkExt, StreamExt};
use protocol::{ClientCodec, Record, Request, Response};
use tokio::net::TcpStream;
use tokio_util::codec::Framed;

/// A consumer client for reading messages from the broker
pub struct Consumer {
    framed: Framed<TcpStream, ClientCodec>,
}

impl Consumer {
    /// Connect to a broker
    pub async fn connect(addr: &str) -> Result<Self> {
        let stream = TcpStream::connect(addr)
            .await
            .map_err(|e| GaffaError::Connection(e.to_string()))?;

        let framed = Framed::new(stream, ClientCodec);

        tracing::info!("Consumer connected to {}", addr);

        Ok(Self { framed })
    }

    /// Fetch messages from a topic partition
    pub async fn fetch(
        &mut self,
        topic: &str,
        partition: u32,
        offset: u64,
        max_messages: u32,
    ) -> Result<Vec<Record>> {
        let request = Request::Fetch {
            topic: topic.to_string(),
            partition,
            offset,
            max_messages,
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
            Response::FetchSuccess { records, .. } => {
                tracing::debug!(
                    "Fetched {} messages from {}:{}",
                    records.len(),
                    topic,
                    partition
                );
                Ok(records)
            }
            Response::FetchError { error } => Err(GaffaError::Protocol(error)),
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
    fn test_consumer_creation() {
        // This is a placeholder test
        // Real tests would need a mock or running broker
        assert!(true);
    }
}
