/// Example demonstrating consumer groups with automatic offset management
///
/// This example shows:
/// - Creating a consumer group
/// - Auto-partition assignment
/// - Heartbeat mechanism
/// - Auto-commit of offsets
/// - Multiple consumers in the same group sharing partitions

use client::Consumer;
use std::time::Duration;
use tokio::time::sleep;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize logging
    tracing_subscriber::fmt::init();

    println!("🎯 Consumer Group Example");
    println!("========================\n");

    // Connect to broker with consumer group configuration
    let mut consumer = Consumer::connect("localhost:9092")
        .await?
        .with_group_id("example-group")
        .with_auto_commit(Some(Duration::from_secs(5)));

    println!("✓ Connected to broker");
    println!("✓ Consumer group: example-group");
    println!("✓ Auto-commit enabled (5 second interval)\n");

    // Join the group and subscribe to topics
    println!("📝 Joining consumer group and subscribing to topics...");
    consumer.join_group(vec!["events", "logs"]).await?;
    println!("✓ Joined group successfully!\n");

    println!("📥 Polling for messages from assigned partitions...");
    println!("   (Press Ctrl+C to stop)\n");

    // Poll for messages
    let mut message_count = 0;
    loop {
        match consumer.poll_group(10).await {
            Ok(records) => {
                for record in &records {
                    message_count += 1;
                    let value_str = String::from_utf8_lossy(&record.message.value);
                    let key_str = record
                        .message
                        .key
                        .as_ref()
                        .map(|k| String::from_utf8_lossy(k).to_string())
                        .unwrap_or_else(|| "null".to_string());

                    println!(
                        "  📨 [{}:{}@{}] key={}, value={}",
                        record.topic, record.partition, record.offset, key_str, value_str
                    );
                }

                if !records.is_empty() {
                    println!(
                        "   ✓ Received {} messages (total: {})\n",
                        records.len(),
                        message_count
                    );
                }
            }
            Err(e) => {
                eprintln!("❌ Error polling: {}", e);
                break;
            }
        }

        // Sleep before next poll
        sleep(Duration::from_secs(1)).await;
    }

    // Leave group gracefully
    println!("\n👋 Leaving consumer group...");
    consumer.leave_group().await?;
    println!("✓ Left group successfully");

    Ok(())
}
