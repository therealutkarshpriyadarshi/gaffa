use crate::cluster::{BrokerInfo, ClusterMetadata};
use crate::coordinator::GroupCoordinator;
use crate::offset_manager::OffsetManager;
use crate::replication::ReplicationManager;
use common::config::BrokerConfig;
use common::Result;
use protocol::{GaffaCodec, Request, Response};
use storage::TopicManager;
use std::time::Duration;
use tokio::net::{TcpListener, TcpStream};
use tokio_util::codec::Framed;
use futures::{SinkExt, StreamExt};

/// The main broker server that handles client connections
pub struct BrokerServer {
    config: BrokerConfig,
    topic_manager: TopicManager,
    coordinator: GroupCoordinator,
    offset_manager: OffsetManager,
    cluster: ClusterMetadata,
    replication_manager: ReplicationManager,
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

        // Create group coordinator with 30 second heartbeat timeout
        let coordinator = GroupCoordinator::new(Duration::from_secs(30));

        // Create offset manager
        let offset_manager = OffsetManager::new(&config.data_dir)?;

        // Create cluster metadata
        let cluster = ClusterMetadata::new(
            config.broker_id,
            Duration::from_secs(30), // Broker heartbeat timeout
            config.max_isr_lag,
        );

        // Register this broker
        cluster.register_broker(BrokerInfo::new(
            config.broker_id,
            config.host.clone(),
            config.port,
        ));

        // Create replication manager
        let replication_manager = ReplicationManager::new(
            cluster.clone(),
            topic_manager.clone(),
            config.replication_factor,
            config.max_isr_lag,
        );

        tracing::info!(
            "Initialized broker {} with replication factor {}",
            config.broker_id,
            config.replication_factor
        );

        Ok(Self {
            config,
            topic_manager,
            coordinator,
            offset_manager,
            cluster,
            replication_manager,
        })
    }

    /// Run the broker server
    pub async fn run(self) -> anyhow::Result<()> {
        // Load committed offsets
        if let Err(e) = self.offset_manager.load().await {
            tracing::warn!("Failed to load offsets: {}", e);
        }

        let addr = format!("{}:{}", self.config.host, self.config.port);
        let listener = TcpListener::bind(&addr).await?;

        tracing::info!("Broker {} listening on {}", self.config.broker_id, addr);

        // Start heartbeat checker task for consumer groups
        let coordinator = self.coordinator.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(10));
            loop {
                interval.tick().await;
                coordinator.check_heartbeats().await;
            }
        });

        // Start broker health checker task
        let replication_manager = self.replication_manager.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(10));
            loop {
                interval.tick().await;
                replication_manager.check_broker_health().await;
            }
        });

        loop {
            match listener.accept().await {
                Ok((stream, addr)) => {
                    tracing::debug!("New connection from {}", addr);
                    let topic_manager = self.topic_manager.clone();
                    let coordinator = self.coordinator.clone();
                    let offset_manager = self.offset_manager.clone();
                    let cluster = self.cluster.clone();
                    let replication_manager = self.replication_manager.clone();

                    tokio::spawn(async move {
                        if let Err(e) = handle_connection(
                            stream,
                            topic_manager,
                            coordinator,
                            offset_manager,
                            cluster,
                            replication_manager,
                        )
                        .await
                        {
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
async fn handle_connection(
    stream: TcpStream,
    topic_manager: TopicManager,
    coordinator: GroupCoordinator,
    offset_manager: OffsetManager,
    cluster: ClusterMetadata,
    replication_manager: ReplicationManager,
) -> Result<()> {
    let mut framed = Framed::new(stream, GaffaCodec);

    while let Some(request_result) = framed.next().await {
        let request = request_result?;
        tracing::debug!("Received request: {:?}", request);

        let response = process_request(
            request,
            &topic_manager,
            &coordinator,
            &offset_manager,
            &cluster,
            &replication_manager,
        )
        .await;
        tracing::debug!("Sending response: {:?}", response);

        framed.send(response).await?;
    }

    Ok(())
}

/// Process a request and return a response
async fn process_request(
    request: Request,
    topic_manager: &TopicManager,
    coordinator: &GroupCoordinator,
    offset_manager: &OffsetManager,
    cluster: &ClusterMetadata,
    replication_manager: &ReplicationManager,
) -> Response {
    match request {
        Request::CreateTopic { name, partitions } => {
            match topic_manager.create_topic(name.clone(), partitions) {
                Ok(_) => {
                    // Assign replicas for each partition
                    for partition_id in 0..partitions {
                        let (leader, replicas) = replication_manager
                            .assign_partition_replicas(&name, partition_id);
                        cluster.set_partition_replicas(
                            name.clone(),
                            partition_id,
                            leader,
                            replicas,
                        );
                    }
                    tracing::info!(
                        "Created topic '{}' with {} partitions and assigned replicas",
                        name,
                        partitions
                    );
                    Response::CreateTopicSuccess { name, partitions }
                }
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
                Ok(base_offset) => {
                    let count = messages.len() as u32;
                    let final_offset = base_offset + count as u64;

                    // Update leader offset for replication
                    replication_manager.update_leader_offset(&topic, partition, final_offset);

                    Response::ProduceSuccess {
                        topic,
                        partition,
                        base_offset,
                        count,
                    }
                }
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

        Request::JoinGroup {
            group_id,
            member_id,
            topics,
        } => {
            // Update coordinator with topic partition counts
            for topic in &topics {
                if let Ok(topic_obj) = topic_manager.get_topic(topic) {
                    coordinator
                        .update_topic_partitions(topic.clone(), topic_obj.num_partitions())
                        .await;
                }
            }

            match coordinator.join_group(group_id.clone(), member_id, topics).await {
                Ok((member_id, assignments)) => Response::JoinGroupSuccess {
                    group_id,
                    member_id,
                    assignments,
                },
                Err(e) => Response::JoinGroupError {
                    error: e.to_string(),
                },
            }
        }

        Request::LeaveGroup { group_id, member_id } => {
            match coordinator.leave_group(&group_id, &member_id).await {
                Ok(_) => Response::LeaveGroupSuccess { group_id },
                Err(e) => Response::LeaveGroupError {
                    error: e.to_string(),
                },
            }
        }

        Request::Heartbeat { group_id, member_id } => {
            match coordinator.heartbeat(&group_id, &member_id).await {
                Ok(needs_rejoin) => {
                    if needs_rejoin {
                        Response::HeartbeatError {
                            error: "Rebalance in progress".to_string(),
                            needs_rejoin: true,
                        }
                    } else {
                        Response::HeartbeatSuccess
                    }
                }
                Err(e) => Response::HeartbeatError {
                    error: e.to_string(),
                    needs_rejoin: false,
                },
            }
        }

        Request::CommitOffset {
            group_id,
            topic,
            partition,
            offset,
        } => {
            match offset_manager
                .commit_offset(&group_id, &topic, partition, offset)
                .await
            {
                Ok(_) => Response::CommitOffsetSuccess {
                    group_id,
                    topic,
                    partition,
                    offset,
                },
                Err(e) => Response::CommitOffsetError {
                    error: e.to_string(),
                },
            }
        }

        Request::FetchOffset {
            group_id,
            topic,
            partition,
        } => {
            match offset_manager
                .fetch_offset(&group_id, &topic, partition)
                .await
            {
                Ok(Some(offset)) => Response::FetchOffsetSuccess {
                    group_id,
                    topic,
                    partition,
                    offset,
                },
                Ok(None) => Response::FetchOffsetSuccess {
                    group_id,
                    topic,
                    partition,
                    offset: 0, // Default to 0 if no offset committed
                },
                Err(e) => Response::FetchOffsetError {
                    error: e.to_string(),
                },
            }
        }

        Request::RegisterBroker {
            broker_id,
            host,
            port,
        } => {
            let broker_info = BrokerInfo::new(broker_id, host, port);
            cluster.register_broker(broker_info);
            tracing::info!("Registered broker {}", broker_id);
            Response::RegisterBrokerSuccess { broker_id }
        }

        Request::GetClusterMetadata => {
            use protocol::{BrokerMetadata, TopicPartitionMetadata};

            let brokers: Vec<BrokerMetadata> = cluster
                .get_alive_brokers()
                .iter()
                .map(|b| BrokerMetadata {
                    id: b.id,
                    host: b.host.clone(),
                    port: b.port,
                })
                .collect();

            let topic_partitions: Vec<TopicPartitionMetadata> = cluster
                .get_all_partition_states()
                .iter()
                .map(|state| TopicPartitionMetadata {
                    topic: state.topic.clone(),
                    partition: state.partition,
                    leader: state.leader,
                    replicas: state.replicas.clone(),
                    isr: state.isr.iter().copied().collect(),
                })
                .collect();

            Response::ClusterMetadata {
                brokers,
                topic_partitions,
            }
        }

        Request::ReplicationFetch {
            broker_id: _,
            topic,
            partition,
            offset,
        } => {
            use crate::replication::ReplicationFetchRequest;

            let request = ReplicationFetchRequest {
                topic: topic.clone(),
                partition,
                offset,
                max_records: 1000,
            };

            match replication_manager.handle_replication_fetch(request).await {
                Some(response) => Response::ReplicationFetchSuccess {
                    records: response.records,
                    high_watermark: response.high_watermark,
                    leader_epoch: response.leader_epoch,
                },
                None => Response::ReplicationFetchError {
                    error: "Not the leader for this partition".to_string(),
                },
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use protocol::Message;

    // Helper function to create test dependencies
    fn setup_test_env(dir: &tempfile::TempDir) -> (TopicManager, GroupCoordinator, OffsetManager, ClusterMetadata, ReplicationManager) {
        let topic_manager = TopicManager::new(dir.path()).unwrap();
        let coordinator = GroupCoordinator::new(Duration::from_secs(30));
        let offset_manager = OffsetManager::new(dir.path()).unwrap();
        let cluster = ClusterMetadata::new(0, Duration::from_secs(30), 1000);
        cluster.register_broker(BrokerInfo::new(0, "localhost".to_string(), 9092));
        let replication_manager = ReplicationManager::new(cluster.clone(), topic_manager.clone(), 1, 1000);
        (topic_manager, coordinator, offset_manager, cluster, replication_manager)
    }

    #[tokio::test]
    async fn test_process_create_topic() {
        let dir = tempfile::tempdir().unwrap();
        let (topic_manager, coordinator, offset_manager, cluster, replication_manager) = setup_test_env(&dir);

        let request = Request::CreateTopic {
            name: "test-topic".to_string(),
            partitions: 3,
        };

        let response = process_request(request, &topic_manager, &coordinator, &offset_manager, &cluster, &replication_manager).await;

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
        let (topic_manager, coordinator, offset_manager, cluster, replication_manager) = setup_test_env(&dir);
        topic_manager.create_topic("test-topic".to_string(), 3).unwrap();

        let request = Request::CreateTopic {
            name: "test-topic".to_string(),
            partitions: 3,
        };

        let response = process_request(request, &topic_manager, &coordinator, &offset_manager, &cluster, &replication_manager).await;

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
        let (topic_manager, coordinator, offset_manager, cluster, replication_manager) = setup_test_env(&dir);
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

        let response = process_request(request, &topic_manager, &coordinator, &offset_manager, &cluster, &replication_manager).await;

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
        let (topic_manager, coordinator, offset_manager, cluster, replication_manager) = setup_test_env(&dir);

        let request = Request::Produce {
            topic: "nonexistent".to_string(),
            partition: 0,
            messages: vec![Message::new(b"msg".to_vec())],
        };

        let response = process_request(request, &topic_manager, &coordinator, &offset_manager, &cluster, &replication_manager).await;

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
        let (topic_manager, coordinator, offset_manager, cluster, replication_manager) = setup_test_env(&dir);
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
        process_request(produce_req, &topic_manager, &coordinator, &offset_manager, &cluster, &replication_manager).await;

        // Now fetch
        let fetch_req = Request::Fetch {
            topic: "test-topic".to_string(),
            partition: 0,
            offset: 0,
            max_messages: 10,
        };

        let response = process_request(fetch_req, &topic_manager, &coordinator, &offset_manager, &cluster, &replication_manager).await;

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
        let (topic_manager, coordinator, offset_manager, cluster, replication_manager) = setup_test_env(&dir);

        let request = Request::Fetch {
            topic: "nonexistent".to_string(),
            partition: 0,
            offset: 0,
            max_messages: 10,
        };

        let response = process_request(request, &topic_manager, &coordinator, &offset_manager, &cluster, &replication_manager).await;

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
        let (topic_manager, coordinator, offset_manager, cluster, replication_manager) = setup_test_env(&dir);

        // Create some topics
        topic_manager.create_topic("topic1".to_string(), 2).unwrap();
        topic_manager.create_topic("topic2".to_string(), 3).unwrap();

        let request = Request::ListTopics;
        let response = process_request(request, &topic_manager, &coordinator, &offset_manager, &cluster, &replication_manager).await;

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
        let (topic_manager, coordinator, offset_manager, cluster, replication_manager) = setup_test_env(&dir);
        topic_manager.create_topic("test-topic".to_string(), 5).unwrap();

        let request = Request::GetPartitions {
            topic: "test-topic".to_string(),
        };
        let response = process_request(request, &topic_manager, &coordinator, &offset_manager, &cluster, &replication_manager).await;

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
        let (topic_manager, coordinator, offset_manager, cluster, replication_manager) = setup_test_env(&dir);

        let request = Request::GetPartitions {
            topic: "nonexistent".to_string(),
        };
        let response = process_request(request, &topic_manager, &coordinator, &offset_manager, &cluster, &replication_manager).await;

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
        let (topic_manager, coordinator, offset_manager, cluster, replication_manager) = setup_test_env(&dir);

        topic_manager.create_topic("topic1".to_string(), 2).unwrap();
        topic_manager.create_topic("topic2".to_string(), 3).unwrap();

        let request = Request::GetMetadata {
            topics: vec![], // Empty = all topics
        };
        let response = process_request(request, &topic_manager, &coordinator, &offset_manager, &cluster, &replication_manager).await;

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
        let (topic_manager, coordinator, offset_manager, cluster, replication_manager) = setup_test_env(&dir);

        topic_manager.create_topic("topic1".to_string(), 2).unwrap();
        topic_manager.create_topic("topic2".to_string(), 3).unwrap();

        let request = Request::GetMetadata {
            topics: vec!["topic1".to_string()],
        };
        let response = process_request(request, &topic_manager, &coordinator, &offset_manager, &cluster, &replication_manager).await;

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
