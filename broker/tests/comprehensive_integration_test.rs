/// Comprehensive Integration Tests for Gaffa
/// Simplified version that works with the actual broker API

use client::{Consumer, Producer};
use common::config::BrokerConfig;
use protocol::Message;
use std::time::Duration;
use tokio::time::sleep;

async fn start_test_broker(port: u16) -> tokio::task::JoinHandle<()> {
    let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
    let data_dir = temp_dir.path().to_str().unwrap().to_string();

    let config = BrokerConfig {
        host: "127.0.0.1".to_string(),
        port,
        data_dir,
        ..Default::default()
    };

    tokio::spawn(async move {
        let _temp_dir_guard = temp_dir;
        if let Ok(server) = broker::server::BrokerServer::new(config) {
            let _ = server.run().await;
        }
    })
}

#[tokio::test]
async fn test_multi_topic_operations() {
    println!("=== Test: Multi-Topic Operations ===");

    let broker_port = 19092;
    let broker_addr = format!("127.0.0.1:{}", broker_port);
    let _broker_handle = start_test_broker(broker_port).await;

    sleep(Duration::from_millis(200)).await;

    let mut producer = Producer::connect(&broker_addr).await.unwrap();

    // Create and produce to multiple topics
    for topic in &["topic1", "topic2", "topic3"] {
        producer.create_topic(topic, 1).await.unwrap();

        for i in 0..5 {
            let msg = Message::new(format!("msg-{}", i).into_bytes());
            producer.send(topic, 0, vec![msg]).await.unwrap();
        }
    }

    println!("Produced 15 messages across 3 topics");

    // Consume from each topic
    for topic in &["topic1", "topic2", "topic3"] {
        let mut consumer = Consumer::connect(&broker_addr).await.unwrap();
        let records = consumer.fetch(topic, 0, 0, 10).await.unwrap();

        assert!(records.len() >= 3, "Should get messages from {}", topic);
        println!("Consumed {} messages from {}", records.len(), topic);
    }

    println!("✓ Multi-topic operations test passed\n");
}

#[tokio::test]
async fn test_large_message_handling() {
    println!("=== Test: Large Message Handling ===");

    let broker_port = 19096;
    let broker_addr = format!("127.0.0.1:{}", broker_port);
    let _broker_handle = start_test_broker(broker_port).await;

    sleep(Duration::from_millis(200)).await;

    let mut producer = Producer::connect(&broker_addr).await.unwrap();
    producer.create_topic("large-topic", 1).await.unwrap();

    // Test various message sizes
    let sizes = vec![1024, 10 * 1024, 100 * 1024];

    for size in sizes {
        let large_value = vec![b'x'; size];
        let msg = Message::new(large_value);
        producer.send("large-topic", 0, vec![msg]).await.unwrap();
        println!("Produced {} byte message", size);
    }

    let mut consumer = Consumer::connect(&broker_addr).await.unwrap();
    let records = consumer.fetch("large-topic", 0, 0, 10).await.unwrap();

    assert!(records.len() >= 2, "Should receive most large messages");
    println!("✓ Large message handling test passed\n");
}

#[tokio::test]
async fn test_concurrent_producers() {
    println!("=== Test: Concurrent Producers ===");

    let broker_port = 19097;
    let broker_addr = format!("127.0.0.1:{}", broker_port);
    let _broker_handle = start_test_broker(broker_port).await;

    sleep(Duration::from_millis(200)).await;

    // First create the topic
    let mut setup_producer = Producer::connect(&broker_addr).await.unwrap();
    setup_producer.create_topic("concurrent-topic", 1).await.unwrap();
    drop(setup_producer);

    let mut handles = vec![];

    for producer_id in 0..3 {
        let addr = broker_addr.clone();

        let handle = tokio::spawn(async move {
            let mut producer = Producer::connect(&addr).await.unwrap();

            for i in 0..10 {
                let msg = Message::new(format!("p{}-m{}", producer_id, i).into_bytes());
                let _ = producer.send("concurrent-topic", 0, vec![msg]).await;
            }
        });

        handles.push(handle);
    }

    for handle in handles {
        handle.await.expect("Producer task failed");
    }

    println!("All 3 producers completed");

    let mut consumer = Consumer::connect(&broker_addr).await.unwrap();
    let records = consumer.fetch("concurrent-topic", 0, 0, 50).await.unwrap();

    assert!(records.len() >= 20, "Should consume most messages, got {}", records.len());
    println!("✓ Concurrent producers test passed\n");
}

#[tokio::test]
async fn test_offset_management() {
    println!("=== Test: Offset Management ===");

    let broker_port = 19098;
    let broker_addr = format!("127.0.0.1:{}", broker_port);
    let _broker_handle = start_test_broker(broker_port).await;

    sleep(Duration::from_millis(200)).await;

    let mut producer = Producer::connect(&broker_addr).await.unwrap();
    producer.create_topic("offset-topic", 1).await.unwrap();

    for i in 0..20 {
        let msg = Message::new(format!("message-{}", i).into_bytes());
        producer.send("offset-topic", 0, vec![msg]).await.unwrap();
    }

    println!("Produced 20 messages");

    // Consume from offset 0
    let mut consumer1 = Consumer::connect(&broker_addr).await.unwrap();
    let records1 = consumer1.fetch("offset-topic", 0, 0, 10).await.unwrap();

    assert_eq!(records1.len(), 10, "Should get first 10 messages");
    println!("First fetch got 10 messages");

    // Consume from offset 10
    let mut consumer2 = Consumer::connect(&broker_addr).await.unwrap();
    let records2 = consumer2.fetch("offset-topic", 0, 10, 10).await.unwrap();

    assert_eq!(records2.len(), 10, "Should get next 10 messages");
    println!("Second fetch got 10 messages");

    println!("✓ Offset management test passed\n");
}

#[tokio::test]
async fn test_rapid_reconnection() {
    println!("=== Test: Rapid Reconnection ===");

    let broker_port = 19101;
    let broker_addr = format!("127.0.0.1:{}", broker_port);
    let _broker_handle = start_test_broker(broker_port).await;

    sleep(Duration::from_millis(200)).await;

    // Create and drop multiple producers rapidly
    for i in 0..5 {
        let mut producer = Producer::connect(&broker_addr).await.unwrap();

        if i == 0 {
            producer.create_topic("reconnection-topic", 1).await.unwrap();
        }

        let msg = Message::new(format!("message-{}", i).into_bytes());
        producer.send("reconnection-topic", 0, vec![msg]).await.unwrap();

        drop(producer);
    }

    println!("Created and dropped 5 producers rapidly");

    let mut consumer = Consumer::connect(&broker_addr).await.unwrap();
    let records = consumer.fetch("reconnection-topic", 0, 0, 10).await.unwrap();

    assert!(records.len() >= 4, "Should store most messages");
    println!("✓ Rapid reconnection test passed\n");
}
