/// Chaos Engineering Tests for Gaffa
/// Simplified version demonstrating failure injection and resilience testing
/// Run with: cargo test --test chaos_test -- --nocapture --test-threads=1

use client::{Consumer, Producer};
use common::config::BrokerConfig;
use protocol::Message;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
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

async fn is_broker_alive(addr: &str) -> bool {
    tokio::net::TcpStream::connect(addr).await.is_ok()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_broker_crash_and_recovery() {
    println!("\n=== Chaos Test: Broker Crash and Recovery ===\n");

    let broker_port = 19300;
    let broker_addr = format!("127.0.0.1:{}", broker_port);

    // Create temp dir that persists across restarts
    let temp_dir = tempfile::tempdir().unwrap();
    let data_dir = temp_dir.path().to_str().unwrap().to_string();

    // Phase 1: Start broker and produce messages
    println!("Phase 1: Starting broker and producing messages...");

    let broker_handle = {
        let config = BrokerConfig {
            host: "127.0.0.1".to_string(),
            port: broker_port,
            data_dir: data_dir.clone(),
            ..Default::default()
        };

        tokio::spawn(async move {
            if let Ok(server) = broker::server::BrokerServer::new(config) {
                let _ = server.run().await;
            }
        })
    };

    sleep(Duration::from_millis(200)).await;

    let mut producer = Producer::connect(&broker_addr).await.unwrap();
    producer.create_topic("crash-topic", 1).await.unwrap();

    for i in 0..30 {
        let msg = Message::new(format!("before-crash-{}", i).into_bytes());
        let _ = producer.send("crash-topic", 0, vec![msg]).await;
    }

    println!("Produced 30 messages");

    // Phase 2: Crash the broker
    println!("\nPhase 2: Crashing broker...");
    broker_handle.abort();
    sleep(Duration::from_secs(2)).await;

    assert!(!is_broker_alive(&broker_addr).await, "Broker should be down");
    println!("Broker crashed");

    // Phase 3: Restart broker with same data directory
    println!("\nPhase 3: Restarting broker...");

    let _broker_handle = {
        let config = BrokerConfig {
            host: "127.0.0.1".to_string(),
            port: broker_port,
            data_dir: data_dir.clone(),
            ..Default::default()
        };

        tokio::spawn(async move {
            if let Ok(server) = broker::server::BrokerServer::new(config) {
                let _ = server.run().await;
            }
        })
    };

    sleep(Duration::from_millis(200)).await;
    assert!(is_broker_alive(&broker_addr).await, "Broker should be up");
    println!("Broker restarted");

    // Phase 4: Verify old messages are available
    println!("\nPhase 4: Verifying message persistence...");

    let mut consumer = Consumer::connect(&broker_addr).await.unwrap();
    let records = consumer.fetch("crash-topic", 0, 0, 50).await.unwrap();

    println!("Recovered {} messages after crash", records.len());
    assert!(records.len() >= 25, "Should recover most messages");

    println!("\n✓ Broker crash and recovery test passed\n");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_producer_intermittent_failures() {
    println!("\n=== Chaos Test: Producer Intermittent Failures ===\n");

    let broker_port = 19302;
    let broker_addr = format!("127.0.0.1:{}", broker_port);
    let _broker_handle = start_test_broker(broker_port).await;

    sleep(Duration::from_millis(200)).await;

    let mut success = 0;

    println!("Simulating producer with periodic reconnections...");

    // First create the topic
    let mut initial_producer = Producer::connect(&broker_addr).await.unwrap();
    initial_producer.create_topic("failure-topic", 1).await.unwrap();
    drop(initial_producer);

    // Simulate reconnections every few messages
    for batch in 0..10 {
        let mut producer = Producer::connect(&broker_addr).await.unwrap();

        for i in 0..5 {
            let msg = Message::new(format!("batch-{}-msg-{}", batch, i).into_bytes());
            if producer.send("failure-topic", 0, vec![msg]).await.is_ok() {
                success += 1;
            }
        }

        drop(producer); // Disconnect

        if batch % 3 == 0 {
            println!("  Reconnected at batch {}", batch);
        }
    }

    println!("\nSuccessfully produced {} messages", success);

    // Verify messages are consumable
    let mut consumer = Consumer::connect(&broker_addr).await.unwrap();
    let records = consumer.fetch("failure-topic", 0, 0, 100).await.unwrap();

    println!("Consumed {} messages", records.len());
    assert!(records.len() >= 40, "Should consume most messages");

    println!("\n✓ Producer intermittent failures test passed\n");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_concurrent_operations_during_crash() {
    println!("\n=== Chaos Test: Concurrent Operations During Crash ===\n");

    let broker_port = 19305;
    let broker_addr = format!("127.0.0.1:{}", broker_port);

    let temp_dir = tempfile::tempdir().unwrap();
    let data_dir = temp_dir.path().to_str().unwrap().to_string();

    let broker_handle = {
        let config = BrokerConfig {
            host: "127.0.0.1".to_string(),
            port: broker_port,
            data_dir: data_dir.clone(),
            ..Default::default()
        };

        tokio::spawn(async move {
            if let Ok(server) = broker::server::BrokerServer::new(config) {
                let _ = server.run().await;
            }
        })
    };

    sleep(Duration::from_millis(200)).await;

    // Create topic first
    let mut setup_producer = Producer::connect(&broker_addr).await.unwrap();
    setup_producer.create_topic("concurrent-crash-topic", 1).await.unwrap();
    drop(setup_producer);

    let produced = Arc::new(AtomicU64::new(0));
    let stop = Arc::new(AtomicBool::new(false));

    // Start continuous producer
    let producer_handle = {
        let addr = broker_addr.clone();
        let count = Arc::clone(&produced);
        let stop_flag = Arc::clone(&stop);

        tokio::spawn(async move {
            while !stop_flag.load(Ordering::Relaxed) {
                if let Ok(mut producer) = Producer::connect(&addr).await {
                    while !stop_flag.load(Ordering::Relaxed) {
                        let msg = Message::new(b"data".to_vec());
                        if producer.send("concurrent-crash-topic", 0, vec![msg]).await.is_ok() {
                            count.fetch_add(1, Ordering::Relaxed);
                        }
                        sleep(Duration::from_millis(10)).await;
                    }
                } else {
                    sleep(Duration::from_millis(500)).await;
                }
            }
        })
    };

    // Let it run
    println!("Producer running...");
    sleep(Duration::from_secs(1)).await;

    let before_crash = produced.load(Ordering::Relaxed);
    println!("Produced before crash: {}", before_crash);

    // Crash broker
    println!("Crashing broker...");
    broker_handle.abort();
    sleep(Duration::from_millis(500)).await;

    // Restart broker
    println!("Restarting broker...");
    let _broker_handle = {
        let config = BrokerConfig {
            host: "127.0.0.1".to_string(),
            port: broker_port,
            data_dir: data_dir.clone(),
            ..Default::default()
        };

        tokio::spawn(async move {
            if let Ok(server) = broker::server::BrokerServer::new(config) {
                let _ = server.run().await;
            }
        })
    };

    sleep(Duration::from_millis(200)).await;

    // Producer should recover
    sleep(Duration::from_secs(1)).await;

    let after_recovery = produced.load(Ordering::Relaxed);
    println!("Produced after recovery: {}", after_recovery);

    stop.store(true, Ordering::Relaxed);
    producer_handle.await.expect("Producer failed");

    assert!(after_recovery > before_crash, "Producer should have recovered");

    println!("\n✓ Concurrent operations during crash test passed\n");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_message_integrity_under_stress() {
    println!("\n=== Chaos Test: Message Integrity Under Stress ===\n");

    let broker_port = 19306;
    let broker_addr = format!("127.0.0.1:{}", broker_port);
    let _broker_handle = start_test_broker(broker_port).await;

    sleep(Duration::from_millis(200)).await;

    // Produce messages with verifiable content
    println!("Producing messages with verifiable content...");
    let mut producer = Producer::connect(&broker_addr).await.unwrap();
    producer.create_topic("integrity-topic", 1).await.unwrap();

    for i in 0..50 {
        let payload = format!("VERIFY:{:06}:END", i);
        let msg = Message::new(payload.into_bytes());
        let _ = producer.send("integrity-topic", 0, vec![msg]).await;
    }

    println!("Produced 50 messages");

    // Consume and verify integrity
    let mut consumer = Consumer::connect(&broker_addr).await.unwrap();
    let records = consumer.fetch("integrity-topic", 0, 0, 100).await.unwrap();

    let mut valid = 0;
    let mut corrupted = 0;

    for record in records {
        let value = String::from_utf8_lossy(&record.message.value);

        if value.starts_with("VERIFY:") && value.ends_with(":END") {
            valid += 1;
        } else {
            corrupted += 1;
            eprintln!("Corrupted: {}", value);
        }
    }

    println!("\nValid: {}, Corrupted: {}", valid, corrupted);
    assert_eq!(corrupted, 0, "No messages should be corrupted");
    assert!(valid >= 45, "Should receive most messages intact");

    println!("\n✓ Message integrity test passed\n");
}
