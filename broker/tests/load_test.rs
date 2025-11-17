/// Load Testing Framework for Gaffa
/// Simplified version demonstrating load testing concepts
/// Run with: cargo test --test load_test --release -- --nocapture --test-threads=1

use client::Producer;
use common::config::BrokerConfig;
use protocol::Message;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_high_throughput_producers() {
    println!("\n=== Load Test: High Throughput Producers ===\n");

    let broker_port = 19200;
    let broker_addr = format!("127.0.0.1:{}", broker_port);
    let _broker_handle = start_test_broker(broker_port).await;

    sleep(Duration::from_millis(200)).await;

    // Create topic first
    let mut setup_producer = Producer::connect(&broker_addr).await.unwrap();
    setup_producer.create_topic("throughput-topic", 1).await.unwrap();
    drop(setup_producer);

    let start = Instant::now();
    let produced = Arc::new(AtomicU64::new(0));
    let mut handles = vec![];

    println!("Starting 3 producers, 100 messages each...");

    for pid in 0..3 {
        let addr = broker_addr.clone();
        let count = Arc::clone(&produced);

        let handle = tokio::spawn(async move {
            if let Ok(mut producer) = Producer::connect(&addr).await {
                for i in 0..100 {
                    let msg = Message::new(format!("msg-{}-{}", pid, i).into_bytes());
                    if producer.send("throughput-topic", 0, vec![msg]).await.is_ok() {
                        count.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
        });

        handles.push(handle);
    }

    for handle in handles {
        handle.await.expect("Producer failed");
    }

    let duration = start.elapsed();
    let total = produced.load(Ordering::Relaxed);
    let throughput = total as f64 / duration.as_secs_f64();

    println!("\n═══════════════════════════════════════");
    println!("  THROUGHPUT TEST RESULTS");
    println!("═══════════════════════════════════════");
    println!("Duration:    {:.2}s", duration.as_secs_f64());
    println!("Produced:    {} messages", total);
    println!("Throughput:  {:.2} msg/s", throughput);
    println!("═══════════════════════════════════════\n");

    assert!(total >= 250, "Should produce most messages");
    println!("✓ High throughput test passed\n");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_message_size_scaling() {
    println!("\n=== Load Test: Message Size Scaling ===\n");

    let broker_port = 19204;
    let broker_addr = format!("127.0.0.1:{}", broker_port);
    let _broker_handle = start_test_broker(broker_port).await;

    sleep(Duration::from_millis(200)).await;

    let sizes = vec![
        ("1KB", 1024),
        ("10KB", 10 * 1024),
        ("100KB", 100 * 1024),
    ];

    for (name, size) in sizes {
        let mut producer = Producer::connect(&broker_addr).await.unwrap();
        let topic = format!("size-{}", name);

        producer.create_topic(&topic, 1).await.unwrap();

        let payload = vec![b'x'; size];
        let start = Instant::now();

        for _ in 0..10 {
            let msg = Message::new(payload.clone());
            let _ = producer.send(&topic, 0, vec![msg]).await;
        }

        let duration = start.elapsed();
        println!("{:6} - {:.2}ms for 10 messages", name, duration.as_millis());
    }

    println!("\n✓ Message size scaling test passed\n");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_sustained_load() {
    println!("\n=== Load Test: Sustained Load (5s) ===\n");

    let broker_port = 19206;
    let broker_addr = format!("127.0.0.1:{}", broker_port);
    let _broker_handle = start_test_broker(broker_port).await;

    sleep(Duration::from_millis(200)).await;

    let mut setup_producer = Producer::connect(&broker_addr).await.unwrap();
    setup_producer.create_topic("sustained-topic", 1).await.unwrap();
    drop(setup_producer);

    let produced = Arc::new(AtomicU64::new(0));
    let addr = broker_addr.clone();
    let count = Arc::clone(&produced);

    let handle = tokio::spawn(async move {
        if let Ok(mut producer) = Producer::connect(&addr).await {
            let start = Instant::now();
            while start.elapsed() < Duration::from_secs(5) {
                let msg = Message::new(b"data".to_vec());
                if producer.send("sustained-topic", 0, vec![msg]).await.is_ok() {
                    count.fetch_add(1, Ordering::Relaxed);
                }
                sleep(Duration::from_millis(2)).await;
            }
        }
    });

    println!("Running sustained load for 5 seconds...");
    handle.await.expect("Producer failed");

    let total = produced.load(Ordering::Relaxed);

    println!("\n═══════════════════════════════════════");
    println!("  SUSTAINED LOAD RESULTS");
    println!("═══════════════════════════════════════");
    println!("Duration:   5s");
    println!("Produced:   {} messages", total);
    println!("Rate:       {:.2} msg/s", total as f64 / 5.0);
    println!("═══════════════════════════════════════\n");

    assert!(total > 500, "Should maintain steady throughput");
    println!("✓ Sustained load test passed\n");
}
