use client::Consumer;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize logging
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "simple_consumer=info,client=info".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    println!("🚀 Gaffa Simple Consumer Example");
    println!("================================\n");

    // Connect to broker
    println!("📡 Connecting to broker at localhost:9092...");
    let mut consumer = Consumer::connect("localhost:9092").await?;
    println!("✅ Connected!\n");

    // Consume from partition 0
    println!("📥 Consuming from topic 'events', partition 0...\n");

    let mut offset = 0;
    loop {
        let records = consumer.fetch("events", 0, offset, 10).await?;

        if records.is_empty() {
            println!("⏸  No more messages. Waiting...");
            tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
            continue;
        }

        for record in &records {
            let key = record
                .message
                .key
                .as_ref()
                .map(|k| String::from_utf8_lossy(k).to_string())
                .unwrap_or_else(|| "null".to_string());

            let value = String::from_utf8_lossy(&record.message.value);

            println!(
                "  📨 [offset: {}] key: {}, value: {}",
                record.offset, key, value
            );

            offset = record.offset + 1;
        }

        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
    }
}
