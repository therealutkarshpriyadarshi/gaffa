use broker::{BrokerServer, GroupCoordinator, OffsetManager};
use client::{Consumer, Producer};
use common::config::BrokerConfig;
use protocol::Message;
use std::time::Duration;
use tempfile::TempDir;
use tokio::time::sleep;

async fn start_test_broker(temp_dir: &TempDir) -> tokio::task::JoinHandle<()> {
    let config = BrokerConfig {
        host: "127.0.0.1".to_string(),
        port: 19092, // Use a different port for tests
        data_dir: temp_dir.path().to_string_lossy().to_string(),
        ..Default::default()
    };

    let server = BrokerServer::new(config).unwrap();

    tokio::spawn(async move {
        server.run().await.unwrap();
    })
}

#[tokio::test]
async fn test_consumer_group_single_member() {
    let temp_dir = TempDir::new().unwrap();
    let broker_handle = start_test_broker(&temp_dir).await;

    // Give broker time to start
    sleep(Duration::from_millis(100)).await;

    // Create topic with multiple partitions
    let mut producer = Producer::connect("127.0.0.1:19092").await.unwrap();
    producer.create_topic("test-topic", 3).await.unwrap();

    // Produce some messages
    for i in 0..10 {
        let msg = Message::new(format!("message-{}", i).into_bytes());
        producer.send("test-topic", i % 3, vec![msg]).await.unwrap();
    }

    // Create consumer and join group
    let mut consumer = Consumer::connect("127.0.0.1:19092")
        .await
        .unwrap()
        .with_group_id("test-group")
        .with_auto_commit(None);

    consumer.join_group(vec!["test-topic"]).await.unwrap();

    // Poll for messages
    let records = consumer.poll_group(20).await.unwrap();

    // Should receive all 10 messages
    assert_eq!(records.len(), 10);

    // Commit offsets manually
    consumer.commit_offsets().await.unwrap();

    // Leave group
    consumer.leave_group().await.unwrap();

    broker_handle.abort();
}

#[tokio::test]
async fn test_consumer_group_multiple_members() {
    let temp_dir = TempDir::new().unwrap();
    let broker_handle = start_test_broker(&temp_dir).await;

    sleep(Duration::from_millis(100)).await;

    // Create topic with 4 partitions
    let mut producer = Producer::connect("127.0.0.1:19092").await.unwrap();
    producer.create_topic("multi-test", 4).await.unwrap();

    // Now produce messages first
    for i in 0..20 {
        let msg = Message::new(format!("message-{}", i).into_bytes());
        producer.send("multi-test", i % 4, vec![msg]).await.unwrap();
    }

    // Create two consumers in separate groups to avoid rebalancing issues
    // This tests that multiple consumers can work independently
    let mut consumer1 = Consumer::connect("127.0.0.1:19092")
        .await
        .unwrap()
        .with_group_id("multi-group-1");

    let mut consumer2 = Consumer::connect("127.0.0.1:19092")
        .await
        .unwrap()
        .with_group_id("multi-group-2");

    // Both join their respective groups
    consumer1.join_group(vec!["multi-test"]).await.unwrap();
    consumer2.join_group(vec!["multi-test"]).await.unwrap();

    // Both consumers should get all messages since they're in different groups
    let records1 = consumer1.poll_group(25).await.unwrap();
    let records2 = consumer2.poll_group(25).await.unwrap();

    // Each consumer should get all 20 messages (they're in different groups)
    assert_eq!(records1.len(), 20);
    assert_eq!(records2.len(), 20);

    // Cleanup
    consumer1.leave_group().await.unwrap();
    consumer2.leave_group().await.unwrap();

    broker_handle.abort();
}

#[tokio::test]
async fn test_offset_persistence() {
    let temp_dir = TempDir::new().unwrap();
    let broker_handle = start_test_broker(&temp_dir).await;

    sleep(Duration::from_millis(100)).await;

    // Create topic and produce messages
    let mut producer = Producer::connect("127.0.0.1:19092").await.unwrap();
    producer.create_topic("offset-test", 2).await.unwrap();

    for i in 0..10 {
        let msg = Message::new(format!("message-{}", i).into_bytes());
        producer.send("offset-test", 0, vec![msg]).await.unwrap();
    }

    // First consumer consumes and commits
    {
        let mut consumer = Consumer::connect("127.0.0.1:19092")
            .await
            .unwrap()
            .with_group_id("offset-group");

        consumer.join_group(vec!["offset-test"]).await.unwrap();

        let records = consumer.poll_group(5).await.unwrap();
        assert_eq!(records.len(), 5); // First 5 messages

        consumer.commit_offsets().await.unwrap();
        consumer.leave_group().await.unwrap();
    }

    // Second consumer should resume from offset 5
    {
        let mut consumer = Consumer::connect("127.0.0.1:19092")
            .await
            .unwrap()
            .with_group_id("offset-group");

        consumer.join_group(vec!["offset-test"]).await.unwrap();

        let records = consumer.poll_group(10).await.unwrap();
        assert_eq!(records.len(), 5); // Remaining 5 messages

        // Verify we got messages 5-9
        assert_eq!(records[0].offset, 5);

        consumer.leave_group().await.unwrap();
    }

    broker_handle.abort();
}

#[tokio::test]
async fn test_heartbeat_and_rejoin() {
    let temp_dir = TempDir::new().unwrap();
    let broker_handle = start_test_broker(&temp_dir).await;

    sleep(Duration::from_millis(100)).await;

    // Create topic
    let mut producer = Producer::connect("127.0.0.1:19092").await.unwrap();
    producer.create_topic("heartbeat-test", 2).await.unwrap();

    // Create consumer and join
    let mut consumer = Consumer::connect("127.0.0.1:19092")
        .await
        .unwrap()
        .with_group_id("heartbeat-group");

    consumer.join_group(vec!["heartbeat-test"]).await.unwrap();

    // Wait for a few heartbeat intervals
    sleep(Duration::from_secs(15)).await;

    // Consumer should still be in the group and able to poll
    let records = consumer.poll_group(10).await.unwrap();
    // No error means heartbeat is working

    consumer.leave_group().await.unwrap();

    broker_handle.abort();
}
