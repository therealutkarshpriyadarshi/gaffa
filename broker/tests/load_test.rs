/// Load testing with realistic workloads
/// These tests measure performance, throughput, and latency under various load conditions

use client::{Consumer, Producer};
use common::config::BrokerConfig;
use protocol::Message;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
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
        let server = broker::server::BrokerServer::new(config).expect("Failed to create broker");
        let _ = server.run().await;
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_high_throughput_producer() {
    let port = 19300;
    let _handle = start_test_broker(port).await;
    sleep(Duration::from_millis(200)).await;

    let broker_addr = format!("127.0.0.1:{}", port);
    let mut producer = Producer::connect(&broker_addr).await.unwrap();
    producer.create_topic("throughput-topic", 4).await.unwrap();

    let num_messages = 5_000; // Reduced for faster test execution
    let message_size = 1024; // 1KB messages
    let message = vec![0xAB; message_size];

    println!("\n=== High Throughput Producer Test ===");
    println!("Producing {} messages of {} bytes each...", num_messages, message_size);

    let start = Instant::now();

    for i in 0..num_messages {
        producer
            .send("throughput-topic", (i % 4) as u32, vec![Message::new(message.clone())])
            .await
            .unwrap();
    }

    let elapsed = start.elapsed();
    let throughput = num_messages as f64 / elapsed.as_secs_f64();
    let data_rate = (num_messages * message_size) as f64 / elapsed.as_secs_f64() / 1024.0 / 1024.0;

    println!("Time elapsed: {:.2}s", elapsed.as_secs_f64());
    println!("Throughput: {:.2} messages/second", throughput);
    println!("Data rate: {:.2} MB/s", data_rate);

    assert!(throughput > 100.0, "Throughput too low: {:.2} msg/s", throughput);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_concurrent_producer_load() {
    let port = 19301;
    let _handle = start_test_broker(port).await;
    sleep(Duration::from_millis(200)).await;

    let broker_addr = format!("127.0.0.1:{}", port);
    let mut producer = Producer::connect(&broker_addr).await.unwrap();
    producer.create_topic("concurrent-load-topic", 8).await.unwrap();

    let num_producers = 10;
    let messages_per_producer = 100;
    let message_size = 512;
    let total_messages = Arc::new(AtomicU64::new(0));

    println!("\n=== Concurrent Producer Load Test ===");
    println!("{} producers × {} messages = {} total messages",
             num_producers, messages_per_producer, num_producers * messages_per_producer);

    let start = Instant::now();
    let mut handles = vec![];

    for producer_id in 0..num_producers {
        let addr = broker_addr.clone();
        let total = Arc::clone(&total_messages);

        let handle = tokio::spawn(async move {
            let mut producer = Producer::connect(&addr).await.unwrap();
            let message = vec![0xCD; message_size];

            for _ in 0..messages_per_producer {
                producer
                    .send("concurrent-load-topic", (producer_id % 8) as u32, vec![Message::new(message.clone())])
                    .await
                    .unwrap();
                total.fetch_add(1, Ordering::Relaxed);
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.await.unwrap();
    }

    let elapsed = start.elapsed();
    let total = total_messages.load(Ordering::Relaxed);
    let throughput = total as f64 / elapsed.as_secs_f64();

    println!("Time elapsed: {:.2}s", elapsed.as_secs_f64());
    println!("Total messages: {}", total);
    println!("Throughput: {:.2} messages/second", throughput);

    assert_eq!(total, (num_producers * messages_per_producer) as u64);
    assert!(throughput > 100.0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_consumer_throughput() {
    let port = 19302;
    let _handle = start_test_broker(port).await;
    sleep(Duration::from_millis(200)).await;

    let broker_addr = format!("127.0.0.1:{}", port);
    let mut producer = Producer::connect(&broker_addr).await.unwrap();
    producer.create_topic("consumer-throughput-topic", 1).await.unwrap();

    let num_messages = 2_000;
    let message_size = 1024;
    let message = vec![0xEF; message_size];

    println!("\n=== Consumer Throughput Test ===");
    println!("Producing {} messages...", num_messages);

    let produce_start = Instant::now();
    for _ in 0..num_messages {
        producer
            .send("consumer-throughput-topic", 0, vec![Message::new(message.clone())])
            .await
            .unwrap();
    }
    println!("Produced in {:.2}s", produce_start.elapsed().as_secs_f64());

    let mut consumer = Consumer::connect(&broker_addr).await.unwrap();

    let consume_start = Instant::now();
    let mut offset = 0;
    let batch_size = 500;
    let mut total_consumed = 0;

    while total_consumed < num_messages {
        let records = consumer
            .fetch("consumer-throughput-topic", 0, offset, batch_size)
            .await
            .unwrap();

        if records.is_empty() {
            break;
        }

        total_consumed += records.len();
        offset += records.len() as u64;
    }

    let consume_elapsed = consume_start.elapsed();
    let throughput = total_consumed as f64 / consume_elapsed.as_secs_f64();

    println!("Consumed {} messages", total_consumed);
    println!("Consume time: {:.2}s", consume_elapsed.as_secs_f64());
    println!("Throughput: {:.2} messages/second", throughput);

    assert_eq!(total_consumed, num_messages);
    assert!(throughput > 100.0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_latency_measurement() {
    let port = 19303;
    let _handle = start_test_broker(port).await;
    sleep(Duration::from_millis(200)).await;

    let broker_addr = format!("127.0.0.1:{}", port);
    let mut producer = Producer::connect(&broker_addr).await.unwrap();
    producer.create_topic("latency-topic", 1).await.unwrap();

    let mut consumer = Consumer::connect(&broker_addr).await.unwrap();

    let num_samples = 100;
    let mut latencies = Vec::with_capacity(num_samples);

    println!("\n=== Latency Measurement Test ===");
    println!("Measuring round-trip latency for {} operations...", num_samples);

    for i in 0..num_samples {
        let message = format!("latency-test-{}", i).into_bytes();
        let start = Instant::now();

        producer
            .send("latency-topic", 0, vec![Message::new(message.clone())])
            .await
            .unwrap();

        let records = consumer
            .fetch("latency-topic", 0, i as u64, 1)
            .await
            .unwrap();

        let latency = start.elapsed();
        latencies.push(latency);

        assert_eq!(records.len(), 1);
        assert_eq!(records[0].message.value, message);
    }

    latencies.sort();
    let min = latencies[0];
    let max = latencies[num_samples - 1];
    let median = latencies[num_samples / 2];
    let p95 = latencies[(num_samples as f64 * 0.95) as usize];
    let p99 = latencies[(num_samples as f64 * 0.99) as usize];
    let avg: Duration = latencies.iter().sum::<Duration>() / num_samples as u32;

    println!("Latency statistics:");
    println!("  Min:    {:6.2} µs", min.as_micros());
    println!("  Median: {:6.2} µs", median.as_micros());
    println!("  Avg:    {:6.2} µs", avg.as_micros());
    println!("  P95:    {:6.2} µs", p95.as_micros());
    println!("  P99:    {:6.2} µs", p99.as_micros());
    println!("  Max:    {:6.2} µs", max.as_micros());

    assert!(p99.as_millis() < 100, "P99 latency too high");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_mixed_workload_realistic() {
    let port = 19304;
    let _handle = start_test_broker(port).await;
    sleep(Duration::from_millis(200)).await;

    let broker_addr = format!("127.0.0.1:{}", port);
    let mut producer = Producer::connect(&broker_addr).await.unwrap();
    producer.create_topic("mixed-workload-topic", 4).await.unwrap();

    let duration_secs = 5;
    let num_producers = 3;
    let num_consumers = 2;
    let total_produced = Arc::new(AtomicU64::new(0));
    let total_consumed = Arc::new(AtomicU64::new(0));

    println!("\n=== Mixed Workload Realistic Test ===");
    println!("Running {} producers and {} consumers for {}s", num_producers, num_consumers, duration_secs);

    let start = Instant::now();

    // Spawn producers
    let mut producer_handles = vec![];
    for producer_id in 0..num_producers {
        let addr = broker_addr.clone();
        let total = Arc::clone(&total_produced);

        let handle = tokio::spawn(async move {
            let mut producer = Producer::connect(&addr).await.unwrap();
            let message_sizes = vec![100, 500, 1024, 2048];
            let mut msg_count = 0;
            let deadline = Instant::now() + Duration::from_secs(duration_secs);

            while Instant::now() < deadline {
                let size = message_sizes[msg_count % message_sizes.len()];
                let message = vec![0x42; size];

                if let Ok(_) = producer
                    .send("mixed-workload-topic", (producer_id % 4) as u32, vec![Message::new(message)])
                    .await
                {
                    total.fetch_add(1, Ordering::Relaxed);
                    msg_count += 1;
                }
                sleep(Duration::from_millis(2)).await;
            }
        });
        producer_handles.push(handle);
    }

    // Spawn consumers
    let mut consumer_handles = vec![];
    for consumer_id in 0..num_consumers {
        let addr = broker_addr.clone();
        let total = Arc::clone(&total_consumed);

        let handle = tokio::spawn(async move {
            let mut consumer = Consumer::connect(&addr).await.unwrap();
            let partition = (consumer_id % 4) as u32;
            let mut offset = 0;
            let deadline = Instant::now() + Duration::from_secs(duration_secs);

            while Instant::now() < deadline {
                if let Ok(records) = consumer.fetch("mixed-workload-topic", partition, offset, 50).await {
                    if !records.is_empty() {
                        total.fetch_add(records.len() as u64, Ordering::Relaxed);
                        offset += records.len() as u64;
                    }
                }
                sleep(Duration::from_millis(10)).await;
            }
        });
        consumer_handles.push(handle);
    }

    for handle in producer_handles {
        handle.await.unwrap();
    }

    for handle in consumer_handles {
        handle.await.unwrap();
    }

    let elapsed = start.elapsed();
    let produced = total_produced.load(Ordering::Relaxed);
    let consumed = total_consumed.load(Ordering::Relaxed);

    println!("Total produced: {}", produced);
    println!("Total consumed: {}", consumed);
    println!("Producer rate: {:.2} msg/s", produced as f64 / elapsed.as_secs_f64());
    println!("Consumer rate: {:.2} msg/s", consumed as f64 / elapsed.as_secs_f64());
    println!("Consumer lag: {} messages", produced as i64 - consumed as i64);

    assert!(produced > 0);
    assert!(consumed > 0);
}
