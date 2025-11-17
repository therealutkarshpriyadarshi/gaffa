use common::config::BrokerConfig;
use common::{GaffaError, Result};
use protocol::{GaffaCodec, Request, Response};
use storage::TopicManager;
use tokio::net::{TcpListener, TcpStream};
use tokio_util::codec::{Framed, FramedRead, FramedWrite};
use futures::{SinkExt, StreamExt};

/// The main broker server that handles client connections
pub struct BrokerServer {
    config: BrokerConfig,
    topic_manager: TopicManager,
}

impl BrokerServer {
    /// Create a new broker server
    pub fn new(config: BrokerConfig) -> Self {
        Self {
            config,
            topic_manager: TopicManager::new(),
        }
    }

    /// Run the broker server
    pub async fn run(self) -> anyhow::Result<()> {
        let addr = format!("{}:{}", self.config.host, self.config.port);
        let listener = TcpListener::bind(&addr).await?;

        tracing::info!("Broker listening on {}", addr);

        loop {
            match listener.accept().await {
                Ok((stream, addr)) => {
                    tracing::debug!("New connection from {}", addr);
                    let topic_manager = self.topic_manager.clone();

                    tokio::spawn(async move {
                        if let Err(e) = handle_connection(stream, topic_manager).await {
                            tracing::error!("Error handling connection from {}: {}", addr, e);
                        }
                    });
                }
                Err(e) => {
                    tracing::error!("Error accepting connection: {}", e);
                }
            }
        }
    }
}

/// Handle a single client connection
async fn handle_connection(stream: TcpStream, topic_manager: TopicManager) -> Result<()> {
    let mut framed = Framed::new(stream, GaffaCodec);

    while let Some(request_result) = framed.next().await {
        let request = request_result?;
        tracing::debug!("Received request: {:?}", request);

        let response = process_request(request, &topic_manager).await;
        tracing::debug!("Sending response: {:?}", response);

        framed.send(response).await?;
    }

    Ok(())
}

/// Process a request and return a response
async fn process_request(request: Request, topic_manager: &TopicManager) -> Response {
    match request {
        Request::CreateTopic { name, partitions } => {
            match topic_manager.create_topic(name.clone(), partitions) {
                Ok(_) => Response::CreateTopicSuccess { name, partitions },
                Err(e) => Response::CreateTopicError {
                    error: e.to_string(),
                },
            }
        }

        Request::Produce {
            topic,
            partition,
            messages,
        } => {
            // Get the topic
            let topic_obj = match topic_manager.get_topic(&topic) {
                Ok(t) => t,
                Err(e) => {
                    return Response::ProduceError {
                        error: e.to_string(),
                    }
                }
            };

            // Append messages to the partition
            match topic_obj.append(partition, messages.clone()).await {
                Ok(base_offset) => Response::ProduceSuccess {
                    topic,
                    partition,
                    base_offset,
                    count: messages.len() as u32,
                },
                Err(e) => Response::ProduceError {
                    error: e.to_string(),
                },
            }
        }

        Request::Fetch {
            topic,
            partition,
            offset,
            max_messages,
        } => {
            // Get the topic
            let topic_obj = match topic_manager.get_topic(&topic) {
                Ok(t) => t,
                Err(e) => {
                    return Response::FetchError {
                        error: e.to_string(),
                    }
                }
            };

            // Fetch messages from the partition
            match topic_obj.fetch(partition, offset, max_messages).await {
                Ok(records) => Response::FetchSuccess {
                    topic,
                    partition,
                    records,
                },
                Err(e) => Response::FetchError {
                    error: e.to_string(),
                },
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use protocol::Message;

    #[tokio::test]
    async fn test_process_create_topic() {
        let topic_manager = TopicManager::new();

        let request = Request::CreateTopic {
            name: "test-topic".to_string(),
            partitions: 3,
        };

        let response = process_request(request, &topic_manager).await;

        match response {
            Response::CreateTopicSuccess { name, partitions } => {
                assert_eq!(name, "test-topic");
                assert_eq!(partitions, 3);
            }
            _ => panic!("Expected CreateTopicSuccess"),
        }
    }

    #[tokio::test]
    async fn test_process_create_duplicate_topic() {
        let topic_manager = TopicManager::new();
        topic_manager.create_topic("test-topic".to_string(), 3).unwrap();

        let request = Request::CreateTopic {
            name: "test-topic".to_string(),
            partitions: 3,
        };

        let response = process_request(request, &topic_manager).await;

        match response {
            Response::CreateTopicError { error } => {
                assert!(error.contains("already exists"));
            }
            _ => panic!("Expected CreateTopicError"),
        }
    }

    #[tokio::test]
    async fn test_process_produce() {
        let topic_manager = TopicManager::new();
        topic_manager.create_topic("test-topic".to_string(), 3).unwrap();

        let messages = vec![
            Message::new(b"msg1".to_vec()),
            Message::new(b"msg2".to_vec()),
        ];

        let request = Request::Produce {
            topic: "test-topic".to_string(),
            partition: 0,
            messages: messages.clone(),
        };

        let response = process_request(request, &topic_manager).await;

        match response {
            Response::ProduceSuccess {
                topic,
                partition,
                base_offset,
                count,
            } => {
                assert_eq!(topic, "test-topic");
                assert_eq!(partition, 0);
                assert_eq!(base_offset, 0);
                assert_eq!(count, 2);
            }
            _ => panic!("Expected ProduceSuccess"),
        }
    }

    #[tokio::test]
    async fn test_process_produce_nonexistent_topic() {
        let topic_manager = TopicManager::new();

        let request = Request::Produce {
            topic: "nonexistent".to_string(),
            partition: 0,
            messages: vec![Message::new(b"msg".to_vec())],
        };

        let response = process_request(request, &topic_manager).await;

        match response {
            Response::ProduceError { error } => {
                assert!(error.contains("not found"));
            }
            _ => panic!("Expected ProduceError"),
        }
    }

    #[tokio::test]
    async fn test_process_fetch() {
        let topic_manager = TopicManager::new();
        topic_manager.create_topic("test-topic".to_string(), 1).unwrap();

        // Produce some messages first
        let messages = vec![
            Message::new(b"msg1".to_vec()),
            Message::new(b"msg2".to_vec()),
        ];
        let produce_req = Request::Produce {
            topic: "test-topic".to_string(),
            partition: 0,
            messages,
        };
        process_request(produce_req, &topic_manager).await;

        // Now fetch
        let fetch_req = Request::Fetch {
            topic: "test-topic".to_string(),
            partition: 0,
            offset: 0,
            max_messages: 10,
        };

        let response = process_request(fetch_req, &topic_manager).await;

        match response {
            Response::FetchSuccess {
                topic,
                partition,
                records,
            } => {
                assert_eq!(topic, "test-topic");
                assert_eq!(partition, 0);
                assert_eq!(records.len(), 2);
                assert_eq!(records[0].message.value, b"msg1");
                assert_eq!(records[1].message.value, b"msg2");
            }
            _ => panic!("Expected FetchSuccess"),
        }
    }

    #[tokio::test]
    async fn test_process_fetch_nonexistent_topic() {
        let topic_manager = TopicManager::new();

        let request = Request::Fetch {
            topic: "nonexistent".to_string(),
            partition: 0,
            offset: 0,
            max_messages: 10,
        };

        let response = process_request(request, &topic_manager).await;

        match response {
            Response::FetchError { error } => {
                assert!(error.contains("not found"));
            }
            _ => panic!("Expected FetchError"),
        }
    }
}
