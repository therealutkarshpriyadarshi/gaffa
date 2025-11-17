use broker::BrokerServer;
use common::config::BrokerConfig;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize tracing
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "broker=debug,storage=debug,protocol=debug".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    // Load configuration
    let config = BrokerConfig::default();

    tracing::info!(
        host = %config.host,
        port = config.port,
        data_dir = %config.data_dir,
        "Starting Gaffa broker"
    );

    // Create and run server
    let server = BrokerServer::new(config)?;
    server.run().await?;

    Ok(())
}
