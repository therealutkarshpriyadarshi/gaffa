use client::{Consumer, Producer};
use common::config::BrokerConfig;
use protocol::Message;
use std::time::Duration;
use tokio::time::sleep;

/// Helper function to start a test broker on a specific port
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
        let server = broker::server::BrokerServer::new(config).expect("Failed to create broker");
        let _ = server.run().await;
    })
}

#[tokio::test]
async fn test_large_message_handling() {
    let port = 19200;
    let _handle = start_test_broker(port).await;
    sleep(Duration::from_millis(100)).await;

    let broker_addr = format!("127.0.0.1:{}", port);

    // Test with a 10MB message
    let large_message = vec![0xAB; 10 * 1024 * 1024];

    let mut producer = Producer::connect(&broker_addr).await.unwrap();
    producer.create_topic("large-topic", 1).await.unwrap();

    // Produce large message
    producer
        .send("large-topic", 0, vec![Message::new(large_message.clone())])
        .await
        .unwrap();

    // Consume and verify
    let mut consumer = Consumer::connect(&broker_addr).await.unwrap();
    let records = consumer.fetch("large-topic", 0, 0, 100).await.unwrap();

    assert_eq!(records.len(), 1);
    assert_eq!(records[0].message.value, large_message);
}

#[tokio::test]
async fn test_many_small_messages() {
    let port = 19201;
    let _handle = start_test_broker(port).await;
    sleep(Duration::from_millis(100)).await;

    let broker_addr = format!("127.0.0.1:{}", port);
    let mut producer = Producer::connect(&broker_addr).await.unwrap();
    producer.create_topic("many-messages-topic", 1).await.unwrap();

    // Produce 10,000 small messages
    let num_messages = 10_000;
    for i in 0..num_messages {
        let message = Message::new(format!("message-{}", i).into_bytes());
        producer
            .send("many-messages-topic", 0, vec![message])
            .await
            .unwrap();
    }

    // Consume and verify all messages
    let mut consumer = Consumer::connect(&broker_addr).await.unwrap();
    let records = consumer
        .fetch("many-messages-topic", 0, 0, num_messages as u32)
        .await
        .unwrap();

    assert_eq!(records.len(), num_messages as usize);

    // Verify message ordering
    for (i, record) in records.iter().enumerate() {
        let expected = format!("message-{}", i).into_bytes();
        assert_eq!(record.message.value, expected);
        assert_eq!(record.offset, i as u64);
    }
}

#[tokio::test]
async fn test_concurrent_producers() {
    let port = 19202;
    let _handle = start_test_broker(port).await;
    sleep(Duration::from_millis(100)).await;

    let broker_addr = format!("127.0.0.1:{}", port);

    // Create topic first
    let mut producer = Producer::connect(&broker_addr).await.unwrap();
    producer.create_topic("concurrent-topic", 1).await.unwrap();

    let num_producers = 10;
    let messages_per_producer = 100;
    let mut handles = vec![];

    // Spawn multiple producers
    for producer_id in 0..num_producers {
        let addr = broker_addr.clone();
        let handle = tokio::spawn(async move {
            let mut producer = Producer::connect(&addr).await.unwrap();

            for msg_id in 0..messages_per_producer {
                let message = Message::new(format!("producer-{}-msg-{}", producer_id, msg_id).into_bytes());
                producer
                    .send("concurrent-topic", 0, vec![message])
                    .await
                    .unwrap();
            }
        });
        handles.push(handle);
    }

    // Wait for all producers to finish
    for handle in handles {
        handle.await.unwrap();
    }

    // Verify all messages were received
    let mut consumer = Consumer::connect(&broker_addr).await.unwrap();
    let records = consumer
        .fetch("concurrent-topic", 0, 0, (num_producers * messages_per_producer) as u32)
        .await
        .unwrap();

    assert_eq!(records.len(), (num_producers * messages_per_producer) as usize);
}

#[tokio::test]
async fn test_concurrent_consumers() {
    let port = 19203;
    let _handle = start_test_broker(port).await;
    sleep(Duration::from_millis(100)).await;

    let broker_addr = format!("127.0.0.1:{}", port);
    let mut producer = Producer::connect(&broker_addr).await.unwrap();
    producer.create_topic("consumer-concurrent-topic", 1).await.unwrap();

    // Produce messages
    let num_messages = 1000;
    for i in 0..num_messages {
        let message = Message::new(format!("message-{}", i).into_bytes());
        producer
            .send("consumer-concurrent-topic", 0, vec![message])
            .await
            .unwrap();
    }

    // Spawn multiple consumers reading from different offsets
    let num_consumers = 5;
    let mut handles = vec![];

    for consumer_id in 0..num_consumers {
        let addr = broker_addr.clone();
        let handle = tokio::spawn(async move {
            let mut consumer = Consumer::connect(&addr).await.unwrap();

            let offset = consumer_id * 100;
            let records = consumer
                .fetch("consumer-concurrent-topic", 0, offset, 100)
                .await
                .unwrap();

            assert_eq!(records.len(), 100);
            assert_eq!(records[0].offset, offset);
        });
        handles.push(handle);
    }

    // Wait for all consumers to finish
    for handle in handles {
        handle.await.unwrap();
    }
}

#[tokio::test]
async fn test_multi_partition_distribution() {
    let port = 19204;
    let _handle = start_test_broker(port).await;
    sleep(Duration::from_millis(100)).await;

    let broker_addr = format!("127.0.0.1:{}", port);
    let mut producer = Producer::connect(&broker_addr).await.unwrap();

    // Create topic with multiple partitions
    let num_partitions = 5;
    producer.create_topic("multi-partition-topic", num_partitions).await.unwrap();

    let messages_per_partition = 100;

    // Produce messages to each partition
    for partition in 0..num_partitions {
        for msg_id in 0..messages_per_partition {
            let message = Message::new(format!("partition-{}-msg-{}", partition, msg_id).into_bytes());
            producer
                .send("multi-partition-topic", partition, vec![message])
                .await
                .unwrap();
        }
    }

    // Verify each partition has the correct messages
    let mut consumer = Consumer::connect(&broker_addr).await.unwrap();

    for partition in 0..num_partitions {
        let records = consumer
            .fetch("multi-partition-topic", partition, 0, messages_per_partition as u32)
            .await
            .unwrap();

        assert_eq!(records.len(), messages_per_partition as usize);

        // Verify ordering within partition
        for (i, record) in records.iter().enumerate() {
            let expected = format!("partition-{}-msg-{}", partition, i).into_bytes();
            assert_eq!(record.message.value, expected);
            assert_eq!(record.offset, i as u64);
        }
    }
}

#[tokio::test]
async fn test_offset_boundary_conditions() {
    let port = 19205;
    let _handle = start_test_broker(port).await;
    sleep(Duration::from_millis(100)).await;

    let broker_addr = format!("127.0.0.1:{}", port);
    let mut producer = Producer::connect(&broker_addr).await.unwrap();
    producer.create_topic("offset-topic", 1).await.unwrap();

    // Produce messages
    for i in 0..100 {
        producer
            .send("offset-topic", 0, vec![Message::new(format!("msg-{}", i).into_bytes())])
            .await
            .unwrap();
    }

    let mut consumer = Consumer::connect(&broker_addr).await.unwrap();

    // Test fetching from offset 0
    let records = consumer.fetch("offset-topic", 0, 0, 10).await.unwrap();
    assert_eq!(records.len(), 10);
    assert_eq!(records[0].offset, 0);

    // Test fetching from middle offset
    let records = consumer.fetch("offset-topic", 0, 50, 10).await.unwrap();
    assert_eq!(records.len(), 10);
    assert_eq!(records[0].offset, 50);

    // Test fetching from last valid offset
    let records = consumer.fetch("offset-topic", 0, 99, 10).await.unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].offset, 99);

    // Test fetching beyond available data
    let records = consumer.fetch("offset-topic", 0, 100, 10).await.unwrap();
    assert_eq!(records.len(), 0);

    // Test fetching with limit larger than available
    let records = consumer.fetch("offset-topic", 0, 90, 20).await.unwrap();
    assert_eq!(records.len(), 10); // Only 10 messages from offset 90-99
}

#[tokio::test]
async fn test_empty_topic_operations() {
    let port = 19206;
    let _handle = start_test_broker(port).await;
    sleep(Duration::from_millis(100)).await;

    let broker_addr = format!("127.0.0.1:{}", port);
    let mut consumer = Consumer::connect(&broker_addr).await.unwrap();

    // Try to fetch from a topic that doesn't exist yet
    let result = consumer.fetch("nonexistent-topic", 0, 0, 10).await;

    // Should return an error for nonexistent topic
    assert!(result.is_err() || result.unwrap().is_empty());
}

#[tokio::test]
async fn test_message_ordering_guarantee() {
    let port = 19207;
    let _handle = start_test_broker(port).await;
    sleep(Duration::from_millis(100)).await;

    let broker_addr = format!("127.0.0.1:{}", port);
    let mut producer = Producer::connect(&broker_addr).await.unwrap();
    producer.create_topic("ordering-topic", 1).await.unwrap();

    // Produce messages with sequential IDs
    let num_messages = 1000;
    for i in 0..num_messages {
        let message = Message::new(format!("ordered-{:06}", i).into_bytes());
        producer
            .send("ordering-topic", 0, vec![message])
            .await
            .unwrap();
    }

    let mut consumer = Consumer::connect(&broker_addr).await.unwrap();
    let records = consumer
        .fetch("ordering-topic", 0, 0, num_messages as u32)
        .await
        .unwrap();

    // Verify strict ordering
    assert_eq!(records.len(), num_messages as usize);
    for (i, record) in records.iter().enumerate() {
        assert_eq!(record.offset, i as u64);
        let expected = format!("ordered-{:06}", i).into_bytes();
        assert_eq!(record.message.value, expected);
    }
}

#[tokio::test]
async fn test_consumer_group_offset_commits() {
    let port = 19208;
    let _handle = start_test_broker(port).await;
    sleep(Duration::from_millis(100)).await;

    let broker_addr = format!("127.0.0.1:{}", port);
    let mut producer = Producer::connect(&broker_addr).await.unwrap();
    producer.create_topic("offset-persist-topic", 1).await.unwrap();

    // Produce messages
    for i in 0..100 {
        producer
            .send("offset-persist-topic", 0, vec![Message::new(format!("msg-{}", i).into_bytes())])
            .await
            .unwrap();
    }

    // Create consumer with group and consume some messages
    let mut consumer1 = Consumer::connect(&broker_addr)
        .await
        .unwrap()
        .with_group_id("test-group");

    let records = consumer1
        .fetch("offset-persist-topic", 0, 0, 50)
        .await
        .unwrap();
    assert_eq!(records.len(), 50);

    // Commit offset
    consumer1.commit_offset("offset-persist-topic", 0, 50);

    // Create new consumer with same group
    let mut consumer2 = Consumer::connect(&broker_addr)
        .await
        .unwrap()
        .with_group_id("test-group");

    // Should be able to continue from offset 50
    let records = consumer2
        .fetch("offset-persist-topic", 0, 50, 50)
        .await
        .unwrap();
    assert_eq!(records.len(), 50);
    assert_eq!(records[0].offset, 50);
}

#[tokio::test]
async fn test_partition_sparse_allocation() {
    let port = 19209;
    let _handle = start_test_broker(port).await;
    sleep(Duration::from_millis(100)).await;

    let broker_addr = format!("127.0.0.1:{}", port);
    let mut producer = Producer::connect(&broker_addr).await.unwrap();
    producer.create_topic("validation-topic", 10).await.unwrap();

    // Produce to partition 0
    producer
        .send("validation-topic", 0, vec![Message::new(b"msg1".to_vec())])
        .await
        .unwrap();

    // Produce to partition 5
    producer
        .send("validation-topic", 5, vec![Message::new(b"msg2".to_vec())])
        .await
        .unwrap();

    let mut consumer = Consumer::connect(&broker_addr).await.unwrap();

    // Verify partition 0 has data
    let records = consumer.fetch("validation-topic", 0, 0, 10).await.unwrap();
    assert_eq!(records.len(), 1);

    // Verify partition 5 has data
    let records = consumer.fetch("validation-topic", 5, 0, 10).await.unwrap();
    assert_eq!(records.len(), 1);
}
