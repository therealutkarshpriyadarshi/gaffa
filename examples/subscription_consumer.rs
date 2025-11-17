use client::Consumer;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize logging
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "subscription_consumer=info,client=info".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    println!("🚀 Gaffa Subscription Consumer Example (Phase 3)");
    println!("=================================================\n");

    // Connect to broker
    println!("📡 Connecting to broker at localhost:9092...");
    let mut consumer = Consumer::connect("localhost:9092").await?;
    println!("✅ Connected!\n");

    // Subscribe to topics (all partitions)
    println!("📝 Subscribing to topics: user-events, events...");
    consumer.subscribe(vec!["user-events", "events"]).await?;
    println!("✅ Subscribed to all partitions of both topics!\n");

    println!("📥 Polling for messages from all subscribed partitions...\n");

    let mut message_count = 0;
    let mut iterations = 0;
    let max_iterations = 20; // Limit for demo purposes

    loop {
        // Poll from all subscribed partitions
        let records = consumer.poll(10).await?;

        if records.is_empty() {
            println!("⏸  No new messages. Waiting...");
            tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

            iterations += 1;
            if iterations >= max_iterations {
                println!("\n✨ Demo complete. Consumed {} messages total.", message_count);
                break;
            }
            continue;
        }

        // Display messages from all partitions
        for record in &records {
            message_count += 1;

            let key = record
                .message
                .key
                .as_ref()
                .map(|k| String::from_utf8_lossy(k).to_string())
                .unwrap_or_else(|| "null".to_string());

            let value = String::from_utf8_lossy(&record.message.value);

            println!(
                "  📨 [{}:{}@{}] key={}, value={}",
                record.topic, record.partition, record.offset, key, value
            );

            // Show current offset for this partition
            let current_offset = consumer
                .get_offset(&record.topic, record.partition)
                .unwrap_or(0);
            println!(
                "     → Offset for {}:{} is now {}",
                record.topic, record.partition, current_offset
            );
        }

        println!("  → Fetched {} messages in this poll\n", records.len());

        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

        if message_count >= 30 {
            println!("✨ Consumed {} messages. Demo complete.", message_count);
            break;
        }
    }

    println!("\n📊 Phase 3 Features Demonstrated:");
    println!("   ✓ Topic subscription (not manual partition selection)");
    println!("   ✓ Multi-partition polling in a single call");
    println!("   ✓ Automatic offset tracking per partition");
    println!("   ✓ Round-robin consumption from all partitions\n");

    Ok(())
}
