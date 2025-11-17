use client::Producer;
use protocol::Message;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize logging
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "simple_producer=info,client=info".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    println!("🚀 Gaffa Simple Producer Example");
    println!("================================\n");

    // Connect to broker
    println!("📡 Connecting to broker at localhost:9092...");
    let mut producer = Producer::connect("localhost:9092").await?;
    println!("✅ Connected!\n");

    // Create a topic
    println!("📝 Creating topic 'events' with 3 partitions...");
    producer.create_topic("events", 3).await?;
    println!("✅ Topic created!\n");

    // Send messages
    println!("📤 Sending messages...");
    for i in 1..=10 {
        let message = Message::new(format!("Message {}", i).into_bytes())
            .with_key(format!("key-{}", i).into_bytes());

        let partition = i % 3; // Round-robin across partitions
        let base_offset = producer.send("events", partition, vec![message]).await?;

        println!(
            "  ✓ Sent message {} to partition {} at offset {}",
            i, partition, base_offset
        );

        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    }

    println!("\n✨ Done! All messages sent successfully.");

    Ok(())
}
