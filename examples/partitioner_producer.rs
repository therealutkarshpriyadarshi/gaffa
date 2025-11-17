use client::{Producer, KeyHashPartitioner};
use protocol::Message;
use std::sync::Arc;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize logging
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "partitioner_producer=info,client=info".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    println!("🚀 Gaffa Partitioner Producer Example (Phase 3)");
    println!("================================================\n");

    // Connect to broker with key-hash partitioner
    println!("📡 Connecting to broker with KeyHashPartitioner...");
    let mut producer = Producer::connect_with_partitioner(
        "localhost:9092",
        Arc::new(KeyHashPartitioner::new()),
    )
    .await?;
    println!("✅ Connected!\n");

    // Create a topic
    println!("📝 Creating topic 'user-events' with 5 partitions...");
    producer.create_topic("user-events", 5).await?;
    println!("✅ Topic created!\n");

    // Send messages with automatic partitioning (key-based)
    println!("📤 Sending messages with key-hash partitioning...");
    println!("   Messages with same key go to same partition for ordering\n");

    let users = vec!["user-123", "user-456", "user-789", "user-123", "user-456"];

    for (i, user) in users.iter().enumerate() {
        let message = Message::new(format!("Login event {} for {}", i + 1, user).into_bytes())
            .with_key(user.as_bytes().to_vec());

        // Auto-partition based on key
        let offset = producer.send_auto("user-events", message).await?;

        println!(
            "  ✓ Sent message {} for {} (auto-partitioned, offset={})",
            i + 1,
            user,
            offset
        );

        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    }

    println!("\n📊 Key-based partitioning ensures:");
    println!("   - All messages with key 'user-123' go to the same partition");
    println!("   - Ordering is preserved within each user's message stream");
    println!("   - Load is distributed across partitions by hashing keys\n");

    // Demonstrate batch auto-partitioning
    println!("📤 Sending batch with automatic partitioning...");
    let batch: Vec<Message> = (1..=10)
        .map(|i| {
            Message::new(format!("Batch message {}", i).into_bytes())
                .with_key(format!("batch-key-{}", i % 3).into_bytes())
        })
        .collect();

    producer.send_batch_auto("user-events", batch).await?;
    println!("  ✓ Sent 10 messages in batch, auto-partitioned by key\n");

    // Get metadata
    println!("📊 Fetching topic metadata...");
    let metadata = producer
        .get_metadata(vec!["user-events".to_string()])
        .await?;

    for topic in metadata {
        println!("  Topic: {}", topic.name);
        println!("  Partitions: {}", topic.partitions.len());
        for partition in &topic.partitions {
            println!("    - Partition {}: Leader={}", partition.id, partition.leader);
        }
    }

    println!("\n✨ Done! Phase 3 auto-partitioning demonstrated.");

    Ok(())
}
