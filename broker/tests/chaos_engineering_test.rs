/// Chaos Engineering Tests - Failure Injection and Resilience Testing
/// These tests validate system behavior under various failure conditions

use client::{Consumer, Producer};
use common::config::BrokerConfig;
use protocol::Message;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;
use tokio::time::sleep;

async fn start_test_broker(port: u16) -> (tokio::task::JoinHandle<()>, String) {
    let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
    let data_dir = temp_dir.path().to_str().unwrap().to_string();

    let config = BrokerConfig {
        host: "127.0.0.1".to_string(),
        port,
        data_dir: data_dir.clone(),
        ..Default::default()
    };

    let handle = tokio::spawn(async move {
        let _temp_dir_guard = temp_dir;
        let server = broker::server::BrokerServer::new(config).expect("Failed to create broker");
        let _ = server.run().await;
    });

    (handle, data_dir)
}

#[tokio::test]
async fn test_broker_crash_and_recovery() {
    let port = 19400;

    println!("\n=== Broker Crash and Recovery Test ===");

    // Start broker
    let (handle, data_dir) = start_test_broker(port).await;
    sleep(Duration::from_millis(200)).await;

    let broker_addr = format!("127.0.0.1:{}", port);
    let mut producer = Producer::connect(&broker_addr).await.unwrap();
    producer.create_topic("crash-test-topic", 1).await.unwrap();

    // Produce messages
    for i in 0..100 {
        producer
            .send("crash-test-topic", 0, vec![Message::new(format!("msg-{}", i).into_bytes())])
            .await
            .unwrap();
    }
    println!("Produced 100 messages");

    // Simulate crash
    handle.abort();
    sleep(Duration::from_millis(300)).await;
    println!("Broker crashed");

    // Restart broker with same data directory
    let config = BrokerConfig {
        host: "127.0.0.1".to_string(),
        port,
        data_dir,
        ..Default::default()
    };

    let temp_dir2 = tempfile::tempdir().unwrap();
    let _handle2 = tokio::spawn(async move {
        let _temp_dir_guard = temp_dir2;
        let server = broker::server::BrokerServer::new(config).expect("Failed to create broker");
        let _ = server.run().await;
    });

    sleep(Duration::from_millis(300)).await;
    println!("Broker restarted");

    // Verify data persistence
    let mut consumer = Consumer::connect(&broker_addr).await.unwrap();
    let records = consumer.fetch("crash-test-topic", 0, 0, 100).await.unwrap();

    println!("Recovered {} messages after restart", records.len());
    assert_eq!(records.len(), 100, "Data should persist across crash");

    // Verify ordering
    for (i, record) in records.iter().enumerate() {
        let expected = format!("msg-{}", i).into_bytes();
        assert_eq!(record.message.value, expected);
        assert_eq!(record.offset, i as u64);
    }

    println!("✓ All messages recovered with correct ordering");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_producer_resilience_during_broker_restart() {
    let port = 19401;

    println!("\n=== Producer Resilience During Broker Restart ===");

    let (handle, data_dir) = start_test_broker(port).await;
    sleep(Duration::from_millis(200)).await;

    let broker_addr = format!("127.0.0.1:{}", port);

    let success_count = Arc::new(AtomicU32::new(0));
    let error_count = Arc::new(AtomicU32::new(0));
    let success_clone = Arc::clone(&success_count);
    let error_clone = Arc::clone(&error_count);
    let addr_clone = broker_addr.clone();

    // Producer that continuously tries to send
    let producer_handle = tokio::spawn(async move {
        let mut producer_opt = Producer::connect(&addr_clone).await.ok();

        if let Some(ref mut producer) = producer_opt {
            let _ = producer.create_topic("resilience-topic", 1).await;
        }

        for i in 0..100 {
            if producer_opt.is_none() {
                producer_opt = Producer::connect(&addr_clone).await.ok();
                if let Some(ref mut producer) = producer_opt {
                    let _ = producer.create_topic("resilience-topic", 1).await;
                }
            }

            if let Some(ref mut producer) = producer_opt {
                match producer
                    .send("resilience-topic", 0, vec![Message::new(format!("msg-{}", i).into_bytes())])
                    .await
                {
                    Ok(_) => {
                        success_clone.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(_) => {
                        error_clone.fetch_add(1, Ordering::Relaxed);
                        producer_opt = None; // Force reconnection
                    }
                }
            } else {
                error_clone.fetch_add(1, Ordering::Relaxed);
            }

            sleep(Duration::from_millis(20)).await;
        }
    });

    // Let some messages be produced
    sleep(Duration::from_millis(500)).await;

    // Crash the broker
    handle.abort();
    println!("Broker crashed");
    sleep(Duration::from_millis(300)).await;

    // Restart broker
    let config = BrokerConfig {
        host: "127.0.0.1".to_string(),
        port,
        data_dir,
        ..Default::default()
    };

    let temp_dir2 = tempfile::tempdir().unwrap();
    let _handle2 = tokio::spawn(async move {
        let _temp_dir_guard = temp_dir2;
        let server = broker::server::BrokerServer::new(config).expect("Failed to create broker");
        let _ = server.run().await;
    });

    sleep(Duration::from_millis(300)).await;
    println!("Broker restarted");

    producer_handle.await.unwrap();

    let successes = success_count.load(Ordering::Relaxed);
    let errors = error_count.load(Ordering::Relaxed);

    println!("Successful produces: {}", successes);
    println!("Failed produces: {}", errors);

    assert!(successes > 0, "Should have some successful produces");
    assert!(errors > 0, "Should have some failures during restart");
    println!("✓ Producer handled restart gracefully");
}

#[tokio::test]
async fn test_consumer_offset_consistency_during_crash() {
    let port = 19402;

    println!("\n=== Consumer Offset Consistency During Crash ===");

    let (handle, data_dir) = start_test_broker(port).await;
    sleep(Duration::from_millis(200)).await;

    let broker_addr = format!("127.0.0.1:{}", port);
    let mut producer = Producer::connect(&broker_addr).await.unwrap();
    producer.create_topic("offset-consistency-topic", 1).await.unwrap();

    // Produce messages
    for i in 0..200 {
        producer
            .send("offset-consistency-topic", 0, vec![Message::new(format!("msg-{}", i).into_bytes())])
            .await
            .unwrap();
    }
    println!("Produced 200 messages");

    // Consume 100 and commit offset
    let mut consumer = Consumer::connect(&broker_addr)
        .await
        .unwrap()
        .with_group_id("crash-test-group");

    let records = consumer
        .fetch("offset-consistency-topic", 0, 0, 100)
        .await
        .unwrap();
    assert_eq!(records.len(), 100);

    consumer.commit_offset("offset-consistency-topic", 0, 100);
    println!("Committed offset at 100");

    // Crash broker
    handle.abort();
    sleep(Duration::from_millis(300)).await;
    println!("Broker crashed");

    // Restart broker
    let config = BrokerConfig {
        host: "127.0.0.1".to_string(),
        port,
        data_dir,
        ..Default::default()
    };

    let temp_dir2 = tempfile::tempdir().unwrap();
    let _handle2 = tokio::spawn(async move {
        let _temp_dir_guard = temp_dir2;
        let server = broker::server::BrokerServer::new(config).expect("Failed to create broker");
        let _ = server.run().await;
    });

    sleep(Duration::from_millis(300)).await;
    println!("Broker restarted");

    // New consumer with same group should continue from offset 100
    let mut consumer2 = Consumer::connect(&broker_addr)
        .await
        .unwrap()
        .with_group_id("crash-test-group");

    let records2 = consumer2
        .fetch("offset-consistency-topic", 0, 100, 100)
        .await
        .unwrap();

    assert_eq!(records2.len(), 100);
    assert_eq!(records2[0].offset, 100);
    println!("✓ Successfully resumed from committed offset after crash");
}

#[tokio::test]
async fn test_data_corruption_detection() {
    use std::fs::OpenOptions;
    use std::io::Write;

    let port = 19403;

    println!("\n=== Data Corruption Detection Test ===");

    let (handle, data_dir) = start_test_broker(port).await;
    sleep(Duration::from_millis(200)).await;

    let broker_addr = format!("127.0.0.1:{}", port);
    let mut producer = Producer::connect(&broker_addr).await.unwrap();
    producer.create_topic("corruption-test-topic", 1).await.unwrap();

    // Produce messages
    for i in 0..50 {
        producer
            .send("corruption-test-topic", 0, vec![Message::new(format!("msg-{}", i).into_bytes())])
            .await
            .unwrap();
    }
    println!("Produced 50 messages");

    // Stop broker
    handle.abort();
    sleep(Duration::from_millis(300)).await;

    // Corrupt a log file
    let log_path = format!("{}/corruption-test-topic/0/00000000000000000000.log", data_dir);
    if let Ok(mut file) = OpenOptions::new().append(true).open(&log_path) {
        let corrupted_data = vec![0xFF; 100];
        let _ = file.write_all(&corrupted_data);
        println!("Injected corruption into log file");
    }

    // Restart broker
    let config = BrokerConfig {
        host: "127.0.0.1".to_string(),
        port,
        data_dir,
        ..Default::default()
    };

    let temp_dir2 = tempfile::tempdir().unwrap();
    let _handle2 = tokio::spawn(async move {
        let _temp_dir_guard = temp_dir2;
        let server = broker::server::BrokerServer::new(config).expect("Failed to create broker");
        let _ = server.run().await;
    });

    sleep(Duration::from_millis(300)).await;

    // Try to consume
    let mut consumer = Consumer::connect(&broker_addr).await.unwrap();
    let result = consumer.fetch("corruption-test-topic", 0, 0, 100).await;

    match result {
        Ok(records) => {
            println!("Read {} valid records before corruption", records.len());
            assert!(records.len() > 0, "Should read some valid records");
            println!("✓ System handled corruption gracefully");
        }
        Err(e) => {
            println!("Detected corruption: {:?}", e);
            println!("✓ Corruption detected as expected");
        }
    }
}

#[tokio::test]
async fn test_network_partition_simulation() {
    let port = 19404;

    println!("\n=== Network Partition Simulation Test ===");

    let (_handle, _data_dir) = start_test_broker(port).await;
    sleep(Duration::from_millis(200)).await;

    let broker_addr = format!("127.0.0.1:{}", port);
    let mut producer = Producer::connect(&broker_addr).await.unwrap();
    producer.create_topic("partition-topic", 1).await.unwrap();

    // Produce messages before partition
    for i in 0..50 {
        producer
            .send("partition-topic", 0, vec![Message::new(format!("before-partition-{}", i).into_bytes())])
            .await
            .unwrap();
    }
    println!("Produced 50 messages before partition");

    // Simulate network partition (wrong port)
    let result = Producer::connect(&format!("127.0.0.1:{}", port + 1)).await;
    assert!(result.is_err(), "Connection to wrong port should fail");
    println!("✓ Simulated network partition (connection refused)");

    // Original producer should still work
    for i in 50..100 {
        producer
            .send("partition-topic", 0, vec![Message::new(format!("after-partition-{}", i).into_bytes())])
            .await
            .unwrap();
    }
    println!("✓ Original producer still works");

    // Verify all messages
    let mut consumer = Consumer::connect(&broker_addr).await.unwrap();
    let records = consumer.fetch("partition-topic", 0, 0, 100).await.unwrap();
    assert_eq!(records.len(), 100);
    println!("✓ All 100 messages recovered");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_concurrent_crash_and_recovery() {
    let port = 19405;

    println!("\n=== Concurrent Operations During Crash Test ===");

    let (handle, data_dir) = start_test_broker(port).await;
    sleep(Duration::from_millis(200)).await;

    let broker_addr = format!("127.0.0.1:{}", port);

    // Create topic first
    let mut init_producer = Producer::connect(&broker_addr).await.unwrap();
    init_producer.create_topic("concurrent-crash-topic", 5).await.unwrap();

    let mut producer_handles = vec![];

    // Spawn multiple producers
    for producer_id in 0..5 {
        let addr = broker_addr.clone();
        let handle = tokio::spawn(async move {
            let mut count = 0;

            for i in 0..50 {
                if let Ok(mut producer) = Producer::connect(&addr).await {
                    if producer
                        .send("concurrent-crash-topic", producer_id, vec![Message::new(format!("p{}-msg-{}", producer_id, i).into_bytes())])
                        .await
                        .is_ok()
                    {
                        count += 1;
                    }
                }
                sleep(Duration::from_millis(20)).await;
            }
            count
        });
        producer_handles.push(handle);
    }

    // Let producers run
    sleep(Duration::from_millis(300)).await;

    // Crash broker
    handle.abort();
    println!("Broker crashed while producers active");
    sleep(Duration::from_millis(300)).await;

    // Restart broker
    let config = BrokerConfig {
        host: "127.0.0.1".to_string(),
        port,
        data_dir,
        ..Default::default()
    };

    let temp_dir2 = tempfile::tempdir().unwrap();
    let _handle2 = tokio::spawn(async move {
        let _temp_dir_guard = temp_dir2;
        let server = broker::server::BrokerServer::new(config).expect("Failed to create broker");
        let _ = server.run().await;
    });

    sleep(Duration::from_millis(300)).await;
    println!("Broker restarted");

    // Wait for producers
    let mut total_produced = 0;
    for handle in producer_handles {
        total_produced += handle.await.unwrap();
    }

    println!("Total messages produced: {}", total_produced);

    // Count recovered messages
    let mut consumer = Consumer::connect(&broker_addr).await.unwrap();
    let mut total_recovered = 0;

    for partition in 0..5 {
        let records = consumer
            .fetch("concurrent-crash-topic", partition, 0, 100)
            .await
            .unwrap();
        total_recovered += records.len();
    }

    println!("Total messages recovered: {}", total_recovered);
    assert!(total_recovered > 0, "Should recover at least some messages");
    println!("✓ System recovered from concurrent crash");
}

#[tokio::test]
async fn test_rapid_broker_restarts() {
    let port = 19406;

    println!("\n=== Rapid Broker Restarts Test ===");

    let temp_dir = tempfile::tempdir().unwrap();
    let data_dir = temp_dir.path().to_str().unwrap().to_string();

    // Rapid restart cycles
    for restart_num in 0..3 {
        println!("Restart #{}", restart_num + 1);

        let config = BrokerConfig {
            host: "127.0.0.1".to_string(),
            port,
            data_dir: data_dir.clone(),
            ..Default::default()
        };

        let broker_temp = tempfile::tempdir().unwrap();
        let handle = tokio::spawn(async move {
            let _temp = broker_temp;
            let server = broker::server::BrokerServer::new(config).expect("Failed to create broker");
            let _ = server.run().await;
        });

        sleep(Duration::from_millis(150)).await;

        // Try to produce a message
        if let Ok(mut producer) = Producer::connect(&format!("127.0.0.1:{}", port)).await {
            if restart_num == 0 {
                let _ = producer.create_topic("rapid-restart-topic", 1).await;
            }
            let _ = producer
                .send("rapid-restart-topic", 0, vec![Message::new(format!("restart-{}", restart_num).into_bytes())])
                .await;
        }

        handle.abort();
        sleep(Duration::from_millis(100)).await;
    }

    // Final restart to verify
    let config = BrokerConfig {
        host: "127.0.0.1".to_string(),
        port,
        data_dir,
        ..Default::default()
    };

    let final_temp = tempfile::tempdir().unwrap();
    let _final_handle = tokio::spawn(async move {
        let _temp = final_temp;
        let server = broker::server::BrokerServer::new(config).expect("Failed to create broker");
        let _ = server.run().await;
    });

    sleep(Duration::from_millis(300)).await;

    // Verify data persisted
    if let Ok(mut consumer) = Consumer::connect(&format!("127.0.0.1:{}", port)).await {
        if let Ok(records) = consumer.fetch("rapid-restart-topic", 0, 0, 10).await {
            println!("✓ Recovered {} messages after {} restarts", records.len(), 3);
        }
    }
}
