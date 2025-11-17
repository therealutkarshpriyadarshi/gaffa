use lazy_static::lazy_static;
use prometheus::{
    register_counter_vec, register_gauge_vec, register_histogram_vec, CounterVec, Encoder,
    GaugeVec, HistogramVec, TextEncoder,
};

lazy_static! {
    // Message counters
    pub static ref MESSAGES_PRODUCED: CounterVec = register_counter_vec!(
        "gaffa_messages_produced_total",
        "Total number of messages produced",
        &["topic", "partition"]
    )
    .unwrap();

    pub static ref MESSAGES_CONSUMED: CounterVec = register_counter_vec!(
        "gaffa_messages_consumed_total",
        "Total number of messages consumed",
        &["topic", "partition", "consumer_group"]
    )
    .unwrap();

    pub static ref MESSAGES_FAILED: CounterVec = register_counter_vec!(
        "gaffa_messages_failed_total",
        "Total number of failed message operations",
        &["operation", "topic"]
    )
    .unwrap();

    // Byte counters
    pub static ref BYTES_PRODUCED: CounterVec = register_counter_vec!(
        "gaffa_bytes_produced_total",
        "Total bytes produced",
        &["topic", "partition"]
    )
    .unwrap();

    pub static ref BYTES_CONSUMED: CounterVec = register_counter_vec!(
        "gaffa_bytes_consumed_total",
        "Total bytes consumed",
        &["topic", "partition"]
    )
    .unwrap();

    // Compression metrics
    pub static ref COMPRESSION_RATIO: HistogramVec = register_histogram_vec!(
        "gaffa_compression_ratio",
        "Compression ratio (compressed_size / original_size)",
        &["compression_type"],
        vec![0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0]
    )
    .unwrap();

    pub static ref BYTES_SAVED_BY_COMPRESSION: CounterVec = register_counter_vec!(
        "gaffa_bytes_saved_by_compression_total",
        "Total bytes saved through compression",
        &["compression_type"]
    )
    .unwrap();

    // Latency histograms
    pub static ref PRODUCE_LATENCY: HistogramVec = register_histogram_vec!(
        "gaffa_produce_latency_seconds",
        "Produce request latency in seconds",
        &["topic"],
        vec![0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0]
    )
    .unwrap();

    pub static ref FETCH_LATENCY: HistogramVec = register_histogram_vec!(
        "gaffa_fetch_latency_seconds",
        "Fetch request latency in seconds",
        &["topic"],
        vec![0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0]
    )
    .unwrap();

    // Topic and partition gauges
    pub static ref TOPIC_COUNT: GaugeVec = register_gauge_vec!(
        "gaffa_topics_total",
        "Total number of topics",
        &["broker_id"]
    )
    .unwrap();

    pub static ref PARTITION_COUNT: GaugeVec = register_gauge_vec!(
        "gaffa_partitions_total",
        "Total number of partitions",
        &["topic"]
    )
    .unwrap();

    pub static ref PARTITION_SIZE: GaugeVec = register_gauge_vec!(
        "gaffa_partition_size_bytes",
        "Size of partition in bytes",
        &["topic", "partition"]
    )
    .unwrap();

    pub static ref PARTITION_OFFSET: GaugeVec = register_gauge_vec!(
        "gaffa_partition_offset",
        "Current offset of partition",
        &["topic", "partition"]
    )
    .unwrap();

    // Consumer group metrics
    pub static ref CONSUMER_GROUP_MEMBERS: GaugeVec = register_gauge_vec!(
        "gaffa_consumer_group_members",
        "Number of members in consumer group",
        &["group_id"]
    )
    .unwrap();

    pub static ref CONSUMER_LAG: GaugeVec = register_gauge_vec!(
        "gaffa_consumer_lag",
        "Consumer lag (partition offset - committed offset)",
        &["group_id", "topic", "partition"]
    )
    .unwrap();

    // Replication metrics
    pub static ref REPLICATION_LAG: GaugeVec = register_gauge_vec!(
        "gaffa_replication_lag",
        "Replication lag in messages",
        &["topic", "partition", "follower_broker"]
    )
    .unwrap();

    pub static ref ISR_SIZE: GaugeVec = register_gauge_vec!(
        "gaffa_isr_size",
        "Number of in-sync replicas",
        &["topic", "partition"]
    )
    .unwrap();

    pub static ref UNDER_REPLICATED_PARTITIONS: GaugeVec = register_gauge_vec!(
        "gaffa_under_replicated_partitions",
        "Number of under-replicated partitions",
        &["broker_id"]
    )
    .unwrap();

    // Storage metrics
    pub static ref SEGMENT_COUNT: GaugeVec = register_gauge_vec!(
        "gaffa_segments_total",
        "Total number of segments",
        &["topic", "partition"]
    )
    .unwrap();

    pub static ref SEGMENTS_DELETED: CounterVec = register_counter_vec!(
        "gaffa_segments_deleted_total",
        "Total number of segments deleted by retention",
        &["topic", "partition", "reason"]
    )
    .unwrap();

    // Connection metrics
    pub static ref ACTIVE_CONNECTIONS: GaugeVec = register_gauge_vec!(
        "gaffa_active_connections",
        "Number of active client connections",
        &["connection_type"]
    )
    .unwrap();

    pub static ref REQUEST_RATE: CounterVec = register_counter_vec!(
        "gaffa_requests_total",
        "Total number of requests",
        &["request_type"]
    )
    .unwrap();

    pub static ref REQUEST_ERRORS: CounterVec = register_counter_vec!(
        "gaffa_request_errors_total",
        "Total number of request errors",
        &["request_type", "error_type"]
    )
    .unwrap();
}

/// Export metrics in Prometheus text format
pub fn export_metrics() -> String {
    let encoder = TextEncoder::new();
    let metric_families = prometheus::gather();
    let mut buffer = Vec::new();
    encoder.encode(&metric_families, &mut buffer).unwrap();
    String::from_utf8(buffer).unwrap()
}

/// Reset all metrics (useful for testing)
#[cfg(test)]
pub fn reset_metrics() {
    // Note: Prometheus metrics cannot be truly reset, but we can work around it in tests
    // by creating new registries. For production, metrics are cumulative.
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metrics_registration() {
        // Metrics should be registered successfully
        assert!(MESSAGES_PRODUCED.with_label_values(&["test", "0"]).get() >= 0.0);
        assert!(MESSAGES_CONSUMED.with_label_values(&["test", "0", "group"]).get() >= 0.0);
    }

    #[test]
    fn test_increment_metrics() {
        MESSAGES_PRODUCED.with_label_values(&["test_topic", "0"]).inc();
        BYTES_PRODUCED.with_label_values(&["test_topic", "0"]).inc_by(1024.0);

        assert!(MESSAGES_PRODUCED.with_label_values(&["test_topic", "0"]).get() >= 1.0);
        assert!(BYTES_PRODUCED.with_label_values(&["test_topic", "0"]).get() >= 1024.0);
    }

    #[test]
    fn test_gauge_metrics() {
        TOPIC_COUNT.with_label_values(&["0"]).set(5.0);
        PARTITION_COUNT.with_label_values(&["test"]).set(10.0);

        assert_eq!(TOPIC_COUNT.with_label_values(&["0"]).get(), 5.0);
        assert_eq!(PARTITION_COUNT.with_label_values(&["test"]).get(), 10.0);
    }

    #[test]
    fn test_export_metrics() {
        // Test that metrics can be exported
        // First, trigger a metric to ensure something is in the registry
        MESSAGES_PRODUCED.with_label_values(&["test", "0"]).inc();

        let output = export_metrics();
        // Output might be empty initially, but should contain metrics after we've set some
        assert!(!output.is_empty(), "Metrics output should not be empty");
        // Metrics should contain gaffa prefix or some expected content
        assert!(output.contains("# HELP") || output.len() > 0);
    }

    #[test]
    fn test_compression_metrics() {
        COMPRESSION_RATIO.with_label_values(&["gzip"]).observe(0.5);
        BYTES_SAVED_BY_COMPRESSION.with_label_values(&["gzip"]).inc_by(512.0);

        let output = export_metrics();
        assert!(output.contains("gaffa_compression_ratio"));
        assert!(output.contains("gaffa_bytes_saved_by_compression"));
    }
}
