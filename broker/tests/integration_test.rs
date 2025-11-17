/// Integration tests for the Gaffa broker
/// These tests spin up a broker server and test end-to-end functionality

use client::{Consumer, Producer};
use common::config::BrokerConfig;
use protocol::Message;
use std::time::Duration;
use tokio::time::sleep;

/// Helper function to start a broker on a random port with a temporary data directory
async fn start_test_broker(port: u16) -> tokio::task::JoinHandle<()> {
    // Use a unique data directory for each test run
    let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
    let data_dir = temp_dir.path().to_str().unwrap().to_string();

    let config = BrokerConfig {
        host: "127.0.0.1".to_string(),
        port,
        data_dir,
    };

    tokio::spawn(async move {
        let _temp_dir_guard = temp_dir; // Keep temp dir alive
        let server = broker::server::BrokerServer::new(config).expect("Failed to create broker");
        let _ = server.run().await;
    })
}

#[tokio::test]
async fn test_end_to_end_produce_consume() {
    // Start broker on port 19092
    let broker_port = 19092;
    let _broker_handle = start_test_broker(broker_port).await;

    // Give broker time to start
    sleep(Duration::from_millis(100)).await;

    let broker_addr = format!("127.0.0.1:{}", broker_port);

    // Connect producer
    let mut producer = Producer::connect(&broker_addr).await.unwrap();

    // Create a topic
    producer.create_topic("test-topic", 3).await.unwrap();

    // Produce messages
    let messages = vec![
        Message::new(b"Hello".to_vec()).with_key(b"key1".to_vec()),
        Message::new(b"World".to_vec()).with_key(b"key2".to_vec()),
        Message::new(b"Gaffa".to_vec()).with_key(b"key3".to_vec()),
    ];

    let base_offset = producer.send("test-topic", 0, messages).await.unwrap();
    assert_eq!(base_offset, 0);

    // Connect consumer
    let mut consumer = Consumer::connect(&broker_addr).await.unwrap();

    // Consume messages
    let records = consumer.fetch("test-topic", 0, 0, 10).await.unwrap();

    assert_eq!(records.len(), 3);
    assert_eq!(records[0].message.value, b"Hello");
    assert_eq!(records[1].message.value, b"World");
    assert_eq!(records[2].message.value, b"Gaffa");
    assert_eq!(records[0].offset, 0);
    assert_eq!(records[1].offset, 1);
    assert_eq!(records[2].offset, 2);
}

#[tokio::test]
async fn test_multiple_partitions() {
    let broker_port = 19093;
    let _broker_handle = start_test_broker(broker_port).await;
    sleep(Duration::from_millis(100)).await;

    let broker_addr = format!("127.0.0.1:{}", broker_port);
    let mut producer = Producer::connect(&broker_addr).await.unwrap();

    // Create topic with 3 partitions
    producer.create_topic("multi-part-topic", 3).await.unwrap();

    // Send messages to different partitions
    for partition in 0..3 {
        let msg = Message::new(format!("Partition {}", partition).into_bytes());
        producer
            .send("multi-part-topic", partition, vec![msg])
            .await
            .unwrap();
    }

    // Verify each partition has its message
    let mut consumer = Consumer::connect(&broker_addr).await.unwrap();

    for partition in 0..3 {
        let records = consumer
            .fetch("multi-part-topic", partition, 0, 10)
            .await
            .unwrap();

        assert_eq!(records.len(), 1);
        assert_eq!(
            records[0].message.value,
            format!("Partition {}", partition).as_bytes()
        );
    }
}

#[tokio::test]
async fn test_fetch_with_offset() {
    let broker_port = 19094;
    let _broker_handle = start_test_broker(broker_port).await;
    sleep(Duration::from_millis(100)).await;

    let broker_addr = format!("127.0.0.1:{}", broker_port);
    let mut producer = Producer::connect(&broker_addr).await.unwrap();

    producer.create_topic("offset-topic", 1).await.unwrap();

    // Produce 10 messages
    let messages: Vec<_> = (0..10)
        .map(|i| Message::new(format!("Message {}", i).into_bytes()))
        .collect();

    producer.send("offset-topic", 0, messages).await.unwrap();

    // Consume starting from offset 5
    let mut consumer = Consumer::connect(&broker_addr).await.unwrap();
    let records = consumer.fetch("offset-topic", 0, 5, 10).await.unwrap();

    assert_eq!(records.len(), 5);
    assert_eq!(records[0].offset, 5);
    assert_eq!(records[0].message.value, b"Message 5");
    assert_eq!(records[4].offset, 9);
    assert_eq!(records[4].message.value, b"Message 9");
}

#[tokio::test]
async fn test_fetch_with_limit() {
    let broker_port = 19095;
    let _broker_handle = start_test_broker(broker_port).await;
    sleep(Duration::from_millis(100)).await;

    let broker_addr = format!("127.0.0.1:{}", broker_port);
    let mut producer = Producer::connect(&broker_addr).await.unwrap();

    producer.create_topic("limit-topic", 1).await.unwrap();

    // Produce 10 messages
    let messages: Vec<_> = (0..10)
        .map(|i| Message::new(format!("Message {}", i).into_bytes()))
        .collect();

    producer.send("limit-topic", 0, messages).await.unwrap();

    // Consume with limit of 3
    let mut consumer = Consumer::connect(&broker_addr).await.unwrap();
    let records = consumer.fetch("limit-topic", 0, 0, 3).await.unwrap();

    assert_eq!(records.len(), 3);
    assert_eq!(records[0].offset, 0);
    assert_eq!(records[2].offset, 2);
}
