use broker::{BrokerInfo, BrokerServer, ClusterMetadata, ReplicationManager};
use client::Producer;
use common::config::BrokerConfig;
use protocol::Message;
use std::time::Duration;
use tempfile::TempDir;
use tokio::time::sleep;

async fn start_test_broker(temp_dir: &TempDir, broker_id: u32, port: u16) -> tokio::task::JoinHandle<()> {
    let config = BrokerConfig {
        host: "127.0.0.1".to_string(),
        port,
        data_dir: temp_dir.path().to_string_lossy().to_string(),
        broker_id,
        replication_factor: 3,
        max_isr_lag: 100,
    };

    let server = BrokerServer::new(config).unwrap();

    tokio::spawn(async move {
        server.run().await.unwrap();
    })
}

#[tokio::test]
async fn test_broker_registration() {
    let temp_dir = TempDir::new().unwrap();
    let broker_handle = start_test_broker(&temp_dir, 0, 19100).await;

    // Give broker time to start
    sleep(Duration::from_millis(100)).await;

    // Connect producer
    let producer = Producer::connect("127.0.0.1:19100").await.unwrap();

    // The broker should be automatically registered
    // In a real multi-broker setup, brokers would register with each other

    broker_handle.abort();
}

#[tokio::test]
async fn test_partition_replica_assignment() {
    let temp_dir = TempDir::new().unwrap();
    let cluster = ClusterMetadata::new(0, Duration::from_secs(30), 100);

    // Register 3 brokers
    cluster.register_broker(BrokerInfo::new(0, "localhost".to_string(), 9092));
    cluster.register_broker(BrokerInfo::new(1, "localhost".to_string(), 9093));
    cluster.register_broker(BrokerInfo::new(2, "localhost".to_string(), 9094));

    // Set partition replicas
    cluster.set_partition_replicas(
        "test-topic".to_string(),
        0,
        0, // leader
        vec![0, 1, 2], // replicas
    );

    // Verify partition leader
    assert_eq!(cluster.get_partition_leader("test-topic", 0), Some(0));

    // Verify partition replicas
    let replicas = cluster.get_partition_replicas("test-topic", 0);
    assert_eq!(replicas.len(), 3);
    assert!(replicas.contains(&0));
    assert!(replicas.contains(&1));
    assert!(replicas.contains(&2));

    // Verify ISR
    let isr = cluster.get_partition_isr("test-topic", 0);
    assert_eq!(isr.len(), 3); // All replicas should be in ISR initially
}

#[tokio::test]
async fn test_isr_tracking() {
    let cluster = ClusterMetadata::new(0, Duration::from_secs(30), 10); // Low lag threshold for testing

    // Register brokers
    cluster.register_broker(BrokerInfo::new(0, "localhost".to_string(), 9092));
    cluster.register_broker(BrokerInfo::new(1, "localhost".to_string(), 9093));
    cluster.register_broker(BrokerInfo::new(2, "localhost".to_string(), 9094));

    // Set up partition with 3 replicas
    cluster.set_partition_replicas(
        "test-topic".to_string(),
        0,
        0,
        vec![0, 1, 2],
    );

    // Update leader offset
    cluster.update_replica_offset("test-topic", 0, 0, 100);

    // Update follower 1 offset (within threshold)
    cluster.update_replica_offset("test-topic", 0, 1, 95);

    // Update follower 2 offset (outside threshold)
    cluster.update_replica_offset("test-topic", 0, 2, 80);

    // Check ISR
    let isr = cluster.get_partition_isr("test-topic", 0);
    assert!(isr.contains(&0)); // Leader always in ISR
    assert!(isr.contains(&1)); // Within lag threshold
    assert!(!isr.contains(&2)); // Outside lag threshold

    // Verify high watermark (min offset across ISR)
    let hwm = cluster.get_high_watermark("test-topic", 0);
    assert_eq!(hwm, 95); // Minimum of offsets in ISR (0:100, 1:95)
}

#[tokio::test]
async fn test_leader_election() {
    let cluster = ClusterMetadata::new(1, Duration::from_secs(30), 100);

    // Register brokers
    cluster.register_broker(BrokerInfo::new(0, "localhost".to_string(), 9092));
    cluster.register_broker(BrokerInfo::new(1, "localhost".to_string(), 9093));
    cluster.register_broker(BrokerInfo::new(2, "localhost".to_string(), 9094));

    // Set up partition with broker 0 as leader
    cluster.set_partition_replicas(
        "test-topic".to_string(),
        0,
        0,
        vec![0, 1, 2],
    );

    // Verify initial leader
    assert_eq!(cluster.get_partition_leader("test-topic", 0), Some(0));

    // Simulate broker 0 failure
    let new_leaders = cluster.handle_broker_failure(0);

    // Should have elected a new leader
    assert_eq!(new_leaders.len(), 1);
    assert_eq!(new_leaders[0].0, "test-topic");
    assert_eq!(new_leaders[0].1, 0); // partition 0
    assert_ne!(new_leaders[0].2, 0); // new leader should not be broker 0

    // Verify new leader
    let new_leader = cluster.get_partition_leader("test-topic", 0).unwrap();
    assert!(new_leader == 1 || new_leader == 2);
}

#[tokio::test]
async fn test_high_watermark_calculation() {
    let cluster = ClusterMetadata::new(0, Duration::from_secs(30), 100);

    cluster.register_broker(BrokerInfo::new(0, "localhost".to_string(), 9092));
    cluster.register_broker(BrokerInfo::new(1, "localhost".to_string(), 9093));
    cluster.register_broker(BrokerInfo::new(2, "localhost".to_string(), 9094));

    cluster.set_partition_replicas(
        "test-topic".to_string(),
        0,
        0,
        vec![0, 1, 2],
    );

    // All replicas at offset 0
    let hwm = cluster.get_high_watermark("test-topic", 0);
    assert_eq!(hwm, 0);

    // Leader writes to offset 50
    cluster.update_replica_offset("test-topic", 0, 0, 50);
    let hwm = cluster.get_high_watermark("test-topic", 0);
    assert_eq!(hwm, 0); // Still 0 because followers haven't caught up

    // Follower 1 catches up to 45
    cluster.update_replica_offset("test-topic", 0, 1, 45);
    let hwm = cluster.get_high_watermark("test-topic", 0);
    assert_eq!(hwm, 0); // Still 0 because follower 2 is at 0

    // Follower 2 catches up to 40
    cluster.update_replica_offset("test-topic", 0, 2, 40);
    let hwm = cluster.get_high_watermark("test-topic", 0);
    assert_eq!(hwm, 40); // Minimum across all ISR members
}

#[tokio::test]
async fn test_replication_stats() {
    let temp_dir = TempDir::new().unwrap();
    let cluster = ClusterMetadata::new(0, Duration::from_secs(30), 100);
    cluster.register_broker(BrokerInfo::new(0, "localhost".to_string(), 9092));

    let topic_manager = storage::TopicManager::new(temp_dir.path()).unwrap();

    let repl_mgr = ReplicationManager::new(cluster.clone(), topic_manager, 1, 100);

    // Set up some partitions
    cluster.set_partition_replicas("topic1".to_string(), 0, 0, vec![0]);
    cluster.set_partition_replicas("topic1".to_string(), 1, 0, vec![0]);
    cluster.set_partition_replicas("topic2".to_string(), 0, 0, vec![0]);

    let stats = repl_mgr.get_replication_stats();

    assert_eq!(stats.total_partitions, 3);
    assert_eq!(stats.leader_partitions, 3); // This broker is leader for all
    assert_eq!(stats.follower_partitions, 0);
    assert_eq!(stats.under_replicated_partitions, 0);
}

#[tokio::test]
async fn test_broker_health_check() {
    let cluster = ClusterMetadata::new(0, Duration::from_millis(50), 100); // Short timeout for testing

    cluster.register_broker(BrokerInfo::new(0, "localhost".to_string(), 9092));
    cluster.register_broker(BrokerInfo::new(1, "localhost".to_string(), 9093));

    // Initially all brokers should be alive
    let alive = cluster.get_alive_brokers();
    assert_eq!(alive.len(), 2);

    // Wait for timeout
    sleep(Duration::from_millis(100)).await;

    // Brokers should now be dead (no heartbeat updates)
    let alive = cluster.get_alive_brokers();
    assert_eq!(alive.len(), 0);

    // Update heartbeat for broker 0
    cluster.update_broker_heartbeat(0);

    // Broker 0 should be alive again
    let alive = cluster.get_alive_brokers();
    assert_eq!(alive.len(), 1);
    assert_eq!(alive[0].id, 0);
}

#[tokio::test]
async fn test_partition_creation_with_replication() {
    let temp_dir = TempDir::new().unwrap();
    let broker_handle = start_test_broker(&temp_dir, 0, 19101).await;

    sleep(Duration::from_millis(100)).await;

    // Create topic
    let mut producer = Producer::connect("127.0.0.1:19101").await.unwrap();
    producer.create_topic("replicated-topic", 3).await.unwrap();

    // Produce some messages
    for i in 0..10 {
        let msg = Message::new(format!("message-{}", i).into_bytes());
        producer.send("replicated-topic", i % 3, vec![msg]).await.unwrap();
    }

    // The topic should have replicas assigned
    // In a single-broker setup, each partition will have replication_factor=1

    broker_handle.abort();
}
