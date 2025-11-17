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
    pub fn new(config: BrokerConfig) -> Result<Self> {
        // Try to open existing topics, or create a new topic manager
        let topic_manager = match TopicManager::open(&config.data_dir) {
            Ok(tm) => {
                tracing::info!("Loaded existing topics from {}", config.data_dir);
                tm
            }
            Err(_) => {
                tracing::info!("No existing topics found, creating new topic manager");
                TopicManager::new(&config.data_dir)?
            }
        };

        Ok(Self {
            config,
            topic_manager,
        })
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

        Request::GetMetadata { topics } => {
            use protocol::TopicMetadata;

            // If topics list is empty, return metadata for all topics
            let topic_names = if topics.is_empty() {
                topic_manager.list_topics()
            } else {
                topics
            };

            let mut metadata_list = Vec::new();
            for topic_name in topic_names {
                match topic_manager.get_topic(&topic_name) {
                    Ok(topic) => {
                        let metadata = TopicMetadata::new(
                            topic_name.clone(),
                            topic.num_partitions(),
                        );
                        metadata_list.push(metadata);
                    }
                    Err(_) => {
                        // Skip non-existent topics
                        continue;
                    }
                }
            }

            Response::Metadata {
                topics: metadata_list,
            }
        }

        Request::GetPartitions { topic } => {
            match topic_manager.get_topic(&topic) {
                Ok(topic_obj) => Response::Partitions {
                    topic,
                    count: topic_obj.num_partitions(),
                },
                Err(e) => Response::PartitionsError {
                    error: e.to_string(),
                },
            }
        }

        Request::ListTopics => {
            let topics = topic_manager.list_topics();
            Response::Topics { topics }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use protocol::Message;

    #[tokio::test]
    async fn test_process_create_topic() {
        let dir = tempfile::tempdir().unwrap();
        let topic_manager = TopicManager::new(dir.path()).unwrap();

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
        let dir = tempfile::tempdir().unwrap();
        let topic_manager = TopicManager::new(dir.path()).unwrap();
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
        let dir = tempfile::tempdir().unwrap();
        let topic_manager = TopicManager::new(dir.path()).unwrap();
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
        let dir = tempfile::tempdir().unwrap();
        let topic_manager = TopicManager::new(dir.path()).unwrap();

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
        let dir = tempfile::tempdir().unwrap();
        let topic_manager = TopicManager::new(dir.path()).unwrap();
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
        let dir = tempfile::tempdir().unwrap();
        let topic_manager = TopicManager::new(dir.path()).unwrap();

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

    #[tokio::test]
    async fn test_process_list_topics() {
        let dir = tempfile::tempdir().unwrap();
        let topic_manager = TopicManager::new(dir.path()).unwrap();

        // Create some topics
        topic_manager.create_topic("topic1".to_string(), 2).unwrap();
        topic_manager.create_topic("topic2".to_string(), 3).unwrap();

        let request = Request::ListTopics;
        let response = process_request(request, &topic_manager).await;

        match response {
            Response::Topics { topics } => {
                assert_eq!(topics.len(), 2);
                assert!(topics.contains(&"topic1".to_string()));
                assert!(topics.contains(&"topic2".to_string()));
            }
            _ => panic!("Expected Topics"),
        }
    }

    #[tokio::test]
    async fn test_process_get_partitions() {
        let dir = tempfile::tempdir().unwrap();
        let topic_manager = TopicManager::new(dir.path()).unwrap();
        topic_manager.create_topic("test-topic".to_string(), 5).unwrap();

        let request = Request::GetPartitions {
            topic: "test-topic".to_string(),
        };
        let response = process_request(request, &topic_manager).await;

        match response {
            Response::Partitions { topic, count } => {
                assert_eq!(topic, "test-topic");
                assert_eq!(count, 5);
            }
            _ => panic!("Expected Partitions"),
        }
    }

    #[tokio::test]
    async fn test_process_get_partitions_nonexistent() {
        let dir = tempfile::tempdir().unwrap();
        let topic_manager = TopicManager::new(dir.path()).unwrap();

        let request = Request::GetPartitions {
            topic: "nonexistent".to_string(),
        };
        let response = process_request(request, &topic_manager).await;

        match response {
            Response::PartitionsError { error } => {
                assert!(error.contains("not found"));
            }
            _ => panic!("Expected PartitionsError"),
        }
    }

    #[tokio::test]
    async fn test_process_get_metadata_all() {
        let dir = tempfile::tempdir().unwrap();
        let topic_manager = TopicManager::new(dir.path()).unwrap();

        topic_manager.create_topic("topic1".to_string(), 2).unwrap();
        topic_manager.create_topic("topic2".to_string(), 3).unwrap();

        let request = Request::GetMetadata {
            topics: vec![], // Empty = all topics
        };
        let response = process_request(request, &topic_manager).await;

        match response {
            Response::Metadata { topics } => {
                assert_eq!(topics.len(), 2);

                let topic1 = topics.iter().find(|t| t.name == "topic1").unwrap();
                assert_eq!(topic1.partitions.len(), 2);

                let topic2 = topics.iter().find(|t| t.name == "topic2").unwrap();
                assert_eq!(topic2.partitions.len(), 3);
            }
            _ => panic!("Expected Metadata"),
        }
    }

    #[tokio::test]
    async fn test_process_get_metadata_specific() {
        let dir = tempfile::tempdir().unwrap();
        let topic_manager = TopicManager::new(dir.path()).unwrap();

        topic_manager.create_topic("topic1".to_string(), 2).unwrap();
        topic_manager.create_topic("topic2".to_string(), 3).unwrap();

        let request = Request::GetMetadata {
            topics: vec!["topic1".to_string()],
        };
        let response = process_request(request, &topic_manager).await;

        match response {
            Response::Metadata { topics } => {
                assert_eq!(topics.len(), 1);
                assert_eq!(topics[0].name, "topic1");
                assert_eq!(topics[0].partitions.len(), 2);
            }
            _ => panic!("Expected Metadata"),
        }
    }
}
