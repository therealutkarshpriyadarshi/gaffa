/// Producer example that generates data for consumer group testing
///
/// This produces messages to multiple topics with different keys
/// to demonstrate partition assignment and ordering guarantees.

use client::Producer;
use protocol::Message;
use std::time::Duration;
use tokio::time::sleep;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    println!("🚀 Producer for Consumer Groups");
    println!("================================\n");

    let mut producer = Producer::connect("localhost:9092").await?;
    println!("✓ Connected to broker\n");

    // Create topics with multiple partitions
    println!("📋 Creating topics...");
    producer.create_topic("events", 4).await?;
    producer.create_topic("logs", 2).await?;
    println!("✓ Created topic 'events' with 4 partitions");
    println!("✓ Created topic 'logs' with 2 partitions\n");

    println!("📤 Producing messages...\n");

    // Produce messages in a loop
    for i in 0..50 {
        // Events topic - use key-based partitioning for ordering
        let user_id = format!("user-{}", i % 5); // 5 different users
        let event_msg = Message::new(format!("Event {} for {}", i, user_id).into_bytes())
            .with_key(user_id.as_bytes().to_vec());

        producer.send("events", 0, vec![event_msg]).await?;

        // Logs topic
        let log_msg = Message::new(format!("Log message {}", i).into_bytes());
        producer.send("logs", 0, vec![log_msg]).await?;

        if (i + 1) % 10 == 0 {
            println!("  ✓ Produced {} messages to each topic", i + 1);
        }

        sleep(Duration::from_millis(100)).await;
    }

    println!("\n✅ Finished producing 50 messages to each topic");
    println!("💡 Now run multiple consumer_group instances to see load balancing!");

    Ok(())
}
