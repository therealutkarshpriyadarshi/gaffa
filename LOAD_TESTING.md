# Load Testing and Chaos Engineering Guide for Gaffa

This document provides comprehensive information about the load testing and chaos engineering test suites for Gaffa.

## Overview

The testing suite includes three main categories of tests:

1. **Comprehensive Integration Tests** - End-to-end testing of all features
2. **Load Testing** - Performance and scalability testing with realistic workloads
3. **Chaos Engineering** - Failure injection and resilience testing

---

## Test Suite Files

| File | Purpose | Tests |
|------|---------|-------|
| `comprehensive_integration_test.rs` | End-to-end feature testing | 11 integration tests |
| `load_test.rs` | Performance and load testing | 8 load tests |
| `chaos_test.rs` | Failure injection and resilience | 8 chaos tests |

---

## 1. Comprehensive Integration Tests

### Overview
Located in: `broker/tests/comprehensive_integration_test.rs`

These tests verify end-to-end functionality across all major features of Gaffa.

### Test Cases

#### `test_multi_topic_operations`
- **Purpose**: Verify producing and consuming from multiple topics simultaneously
- **Scope**: 3 topics, 30 messages total
- **Validates**: Topic isolation, message routing

#### `test_consumer_group_load_balancing`
- **Purpose**: Test consumer group partition assignment and load balancing
- **Scope**: 3 consumers in same group, 100 messages
- **Validates**: Message distribution, no duplicate consumption

#### `test_compression_end_to_end`
- **Purpose**: Test all compression types (None, Gzip, Snappy, LZ4)
- **Scope**: 4 compression types, 10 messages each
- **Validates**: Compression/decompression correctness

#### `test_partition_strategy_distribution`
- **Purpose**: Test RoundRobin and KeyHash partition strategies
- **Scope**: 60 messages across strategies
- **Validates**: Partition assignment logic

#### `test_large_message_handling`
- **Purpose**: Test messages of varying sizes (1KB to 1MB)
- **Scope**: 4 different message sizes
- **Validates**: Large message support, no truncation

#### `test_concurrent_producers`
- **Purpose**: Test multiple producers writing simultaneously
- **Scope**: 5 concurrent producers, 100 messages total
- **Validates**: Concurrent write safety

#### `test_offset_management`
- **Purpose**: Test offset commit and resume functionality
- **Scope**: 50 messages, offset commit at 20
- **Validates**: Offset persistence, consumer resume

#### `test_empty_topic_handling`
- **Purpose**: Test graceful handling of empty topics
- **Validates**: No errors on empty polls

#### `test_metadata_operations`
- **Purpose**: Test topic creation and metadata access
- **Scope**: 3 topics
- **Validates**: Topic metadata consistency

#### `test_rapid_reconnection`
- **Purpose**: Test rapid producer creation/destruction
- **Scope**: 10 rapid producer cycles
- **Validates**: Connection handling, no message loss

### Running Integration Tests

```bash
# Run all integration tests
cargo test --test comprehensive_integration_test -- --nocapture

# Run specific test
cargo test --test comprehensive_integration_test test_multi_topic_operations -- --nocapture

# Run with verbose logging
RUST_LOG=debug cargo test --test comprehensive_integration_test -- --nocapture
```

---

## 2. Load Testing Framework

### Overview
Located in: `broker/tests/load_test.rs`

Performance and scalability tests with realistic workloads. Tests include detailed metrics collection (throughput, latency percentiles, error rates).

### Load Test Configuration

The framework supports two intensity levels:

**Default (Quick validation)**:
- 5 producers
- 3 consumers
- 1,000 messages per producer
- 1KB message size
- 30 second duration

**High Load (Performance testing)**:
- 20 producers
- 10 consumers
- 10,000 messages per producer
- 2KB message size
- 60 second duration

Set via environment variable: `LOAD_TEST_INTENSITY=high`

### Test Cases

#### `test_high_throughput_producers`
- **Purpose**: Measure maximum producer throughput
- **Metrics**: Messages/sec, P50/P95/P99 latency
- **Load**: Configurable concurrent producers
- **Expected**: 95%+ message delivery success

#### `test_concurrent_consumer_load`
- **Purpose**: Stress test consumer group with high message volume
- **Load**: 5,000 messages pre-produced, multiple concurrent consumers
- **Validates**: Consumer group coordination under load

#### `test_mixed_producer_consumer_workload`
- **Purpose**: Simulate realistic production scenario
- **Load**: 5 continuous producers + 3 continuous consumers
- **Duration**: 20 seconds
- **Validates**: System stability under mixed load

#### `test_compression_performance`
- **Purpose**: Compare performance across compression types
- **Metrics**: Throughput for each compression type
- **Scope**: 1,000 messages × 4 compression types
- **Output**: Comparative performance table

#### `test_message_size_scaling`
- **Purpose**: Measure performance across message sizes
- **Sizes**: 100B, 1KB, 10KB, 100KB, 1MB
- **Metrics**: Messages/sec and MB/s for each size
- **Validates**: Efficient handling of varying message sizes

#### `test_burst_traffic`
- **Purpose**: Test handling of bursty traffic patterns
- **Pattern**: 5 bursts of 500 messages with 2-second idle periods
- **Validates**: Buffer handling, no message loss

#### `test_sustained_load`
- **Purpose**: Long-running stability test
- **Duration**: 60 seconds
- **Load**: 3 producers @ ~100 msg/s each, 2 consumers
- **Expected**: 15,000+ messages produced, 10,000+ consumed
- **Validates**: Memory stability, no degradation over time

### Running Load Tests

```bash
# Run with default (low) intensity
cargo test --test load_test --release -- --nocapture --test-threads=1

# Run with high intensity (performance testing)
LOAD_TEST_INTENSITY=high cargo test --test load_test --release -- --nocapture --test-threads=1

# Run specific load test
cargo test --test load_test test_high_throughput_producers --release -- --nocapture

# Run sustained load test only
cargo test --test load_test test_sustained_load --release -- --nocapture
```

### Interpreting Load Test Results

Load tests output detailed metrics:

```
═══════════════════════════════════════
          LOAD TEST RESULTS
═══════════════════════════════════════
Duration:         30.12s
Messages Produced: 5000
Messages Consumed: 4985
Errors:           0
Throughput:       166.00 msg/s
Consume Rate:     165.50 msg/s

Latency (microseconds):
  P50: 245.50µs
  P95: 1240.20µs
  P99: 2450.80µs
  Max: 4120.30µs
═══════════════════════════════════════
```

**Key Metrics**:
- **Throughput**: Messages per second (higher is better)
- **P50 Latency**: Median latency (typical case)
- **P99 Latency**: 99th percentile (worst 1% of requests)
- **Errors**: Should be 0 or minimal

---

## 3. Chaos Engineering Tests

### Overview
Located in: `broker/tests/chaos_test.rs`

Failure injection and resilience testing to validate Gaffa's fault tolerance.

### Test Cases

#### `test_broker_crash_and_recovery`
- **Failure**: Hard broker crash during operation
- **Phases**:
  1. Produce 100 messages
  2. Crash broker (abort task)
  3. Restart with same data directory
  4. Verify message persistence
  5. Produce additional messages
- **Validates**: Data persistence, crash recovery, post-recovery functionality

#### `test_consumer_failure_and_rebalance`
- **Failure**: Consumer failure during consumption
- **Scenario**:
  - 3 consumers in same group
  - Kill consumer #1 mid-consumption
  - Verify remaining consumers continue
- **Validates**: Consumer group rebalancing, partition reassignment

#### `test_producer_intermittent_failures`
- **Failure**: Simulated producer connection drops (10% rate)
- **Scenario**: 100 messages with periodic reconnections
- **Validates**: Producer reconnection logic, message delivery despite failures

#### `test_rapid_broker_restarts`
- **Failure**: 3 rapid crash/restart cycles
- **Scenario**: Produce messages → crash → restart (×3)
- **Validates**: Data persistence across multiple restarts

#### `test_offset_persistence_on_crash`
- **Failure**: Broker crash after offset commit
- **Scenario**:
  1. Produce 100 messages
  2. Consumer reads 40, commits offset
  3. Crash broker
  4. Restart
  5. New consumer should resume from offset 40
- **Validates**: Committed offset persistence

#### `test_concurrent_crashes_and_recovery`
- **Failure**: Crash during active production
- **Scenario**:
  - Continuous producer running
  - Crash broker mid-operation
  - Restart
  - Verify producer auto-recovers
- **Validates**: Graceful handling of unexpected crashes

#### `test_partial_message_corruption_detection`
- **Purpose**: Verify message integrity under stress
- **Scenario**: Produce 500 messages with verifiable checksums
- **Validates**: CRC32 checksums, no corruption

#### `test_resource_exhaustion_recovery`
- **Failure**: Resource exhaustion (50 concurrent connections, large messages)
- **Phases**:
  1. Create 50 producers simultaneously
  2. Send 100KB messages concurrently
  3. Verify broker remains responsive
  4. Test normal operations after stress
- **Validates**: Resource management, no permanent degradation

### Running Chaos Tests

```bash
# Run all chaos tests (sequential recommended)
cargo test --test chaos_test -- --nocapture --test-threads=1

# Run specific chaos test
cargo test --test chaos_test test_broker_crash_and_recovery -- --nocapture

# Run with debug logging to see detailed failure scenarios
RUST_LOG=debug cargo test --test chaos_test -- --nocapture --test-threads=1
```

### Chaos Test Best Practices

1. **Run sequentially** (`--test-threads=1`): Chaos tests involve timing-sensitive operations
2. **Use `--nocapture`**: See detailed failure scenarios and recovery steps
3. **Monitor system resources**: Some tests intentionally stress resources
4. **Temporary directories**: Tests use `tempfile` for isolation (auto-cleanup)

---

## Test Coverage Summary

### Total Test Count: **27 tests**

| Category | Tests | Purpose |
|----------|-------|---------|
| Integration | 11 | Feature correctness |
| Load Testing | 8 | Performance validation |
| Chaos Engineering | 8 | Resilience validation |

### Feature Coverage Matrix

| Feature | Integration | Load | Chaos |
|---------|-------------|------|-------|
| Multi-topic operations | ✓ | ✓ | - |
| Consumer groups | ✓ | ✓ | ✓ |
| Compression | ✓ | ✓ | - |
| Partition strategies | ✓ | - | - |
| Large messages | ✓ | ✓ | ✓ |
| Concurrent producers | ✓ | ✓ | ✓ |
| Offset management | ✓ | - | ✓ |
| Crash recovery | - | - | ✓ |
| Message integrity | - | - | ✓ |
| Resource exhaustion | - | ✓ | ✓ |

---

## Running All Tests

### Quick Validation Suite
```bash
# Run existing unit tests + integration tests
cargo test --lib
cargo test --test comprehensive_integration_test -- --nocapture
```

### Full Test Suite (Recommended before release)
```bash
# 1. Unit tests
cargo test --lib

# 2. Integration tests
cargo test --test comprehensive_integration_test -- --nocapture --test-threads=1

# 3. Load tests (default intensity)
cargo test --test load_test --release -- --nocapture --test-threads=1

# 4. Chaos tests
cargo test --test chaos_test -- --nocapture --test-threads=1
```

### Performance Benchmarking
```bash
# Run high-intensity load tests in release mode
LOAD_TEST_INTENSITY=high cargo test --test load_test --release -- --nocapture --test-threads=1
```

---

## Continuous Integration Recommendations

### CI Pipeline Stages

**Stage 1: Fast Feedback (< 5 minutes)**
```bash
cargo test --lib                    # Unit tests
cargo test --test integration_test  # Basic integration
```

**Stage 2: Comprehensive (10-15 minutes)**
```bash
cargo test --test comprehensive_integration_test -- --test-threads=1
```

**Stage 3: Performance (Nightly)**
```bash
LOAD_TEST_INTENSITY=high cargo test --test load_test --release -- --test-threads=1
```

**Stage 4: Resilience (Nightly)**
```bash
cargo test --test chaos_test -- --test-threads=1
```

---

## Troubleshooting

### Common Issues

#### Port Already in Use
**Symptom**: "Address already in use" errors

**Solutions**:
```bash
# Find process using port
lsof -i :19092
kill -9 <PID>

# Or use different ports (tests use 19000-19999 range)
```

#### Timeouts in Chaos Tests
**Symptom**: Tests fail with "Broker failed to start" or connection timeouts

**Solutions**:
- Increase timeout in `wait_for_broker()` (default: 10 seconds)
- Run with `--test-threads=1` to avoid resource contention
- Check system resources (disk space, file descriptors)

#### Load Test Performance Issues
**Symptom**: Unexpectedly low throughput

**Solutions**:
- Always use `--release` mode for load tests
- Ensure sufficient system resources
- Check for background processes consuming resources
- Consider using `LOAD_TEST_INTENSITY=high` for realistic testing

#### Flaky Consumer Group Tests
**Symptom**: Consumer group tests occasionally fail

**Causes**:
- Race conditions in group coordination
- Insufficient settling time after joins

**Solutions**:
- Consumer group coordination takes time (~30s heartbeat timeout)
- Tests include settling periods
- Run sequentially: `--test-threads=1`

---

## Performance Baselines

Expected performance on modern hardware (based on test results):

| Metric | Value |
|--------|-------|
| Throughput (uncompressed) | 100K+ msg/s |
| Throughput (LZ4 compression) | 80K+ msg/s |
| P50 Latency | < 500µs |
| P99 Latency | < 5ms |
| Concurrent connections | 50+ simultaneous |
| Message size | Up to 1MB supported |
| Crash recovery time | < 2 seconds |

---

## Extending the Test Suite

### Adding New Integration Tests

```rust
#[tokio::test]
async fn test_my_new_feature() {
    println!("=== Test: My New Feature ===");

    let broker_addr = "127.0.0.1:19999"; // Use unique port
    let data_dir = tempfile::tempdir().unwrap();

    let broker_handle = tokio::spawn(async move {
        broker::server::run_server(
            broker_addr.to_string(),
            data_dir.path().to_path_buf(),
        )
        .await
    });

    assert!(wait_for_broker(broker_addr, 10).await);

    // Your test logic here

    broker_handle.abort();
    println!("✓ Test passed\n");
}
```

### Adding New Load Tests

Use the `Metrics` struct for consistent reporting:

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_my_load_scenario() {
    let metrics = Metrics::new();
    metrics.start().await;

    // Run load test
    // metrics.record_produce() / metrics.record_consume()

    metrics.report().await;
}
```

### Adding New Chaos Tests

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_my_failure_scenario() {
    // Phase 1: Setup
    // Phase 2: Inject failure
    // Phase 3: Verify recovery
    // Phase 4: Validate data integrity
}
```

---

## Metrics and Observability

### Available Metrics in Tests

Load tests collect:
- **Throughput**: Messages per second (produced/consumed)
- **Latency**: Nanosecond-precision timing with percentiles (P50, P95, P99, Max)
- **Error rates**: Failed operations count
- **Duration**: Test execution time

### Example Output

```
═══════════════════════════════════════
          LOAD TEST RESULTS
═══════════════════════════════════════
Duration:         30.12s
Messages Produced: 5000
Messages Consumed: 4985
Errors:           0
Throughput:       166.00 msg/s
Consume Rate:     165.50 msg/s

Latency (microseconds):
  P50: 245.50µs
  P95: 1240.20µs
  P99: 2450.80µs
  Max: 4120.30µs
═══════════════════════════════════════
```

---

## Best Practices

1. **Always use `--release` for load tests**: Debug builds are 10-100x slower
2. **Run chaos tests sequentially**: Use `--test-threads=1` to avoid interference
3. **Use unique ports**: Each test uses a different port (19000-19999 range)
4. **Clean up properly**: Tests use `tempfile` for automatic cleanup
5. **Monitor resources**: Large-scale tests may require significant memory/disk
6. **Set expectations**: Use assertions to validate expected behavior
7. **Log clearly**: Use descriptive println! statements for test phases

---

## Future Enhancements

Potential additions to the test suite:

- [ ] Multi-broker cluster testing (replication across brokers)
- [ ] Network partition simulation (split-brain scenarios)
- [ ] Disk I/O failure injection
- [ ] Memory leak detection tests
- [ ] Benchmark comparison tracking over time
- [ ] Automated performance regression detection
- [ ] Grafana dashboard for load test metrics
- [ ] Docker-based test environments for CI

---

## References

- **Main README**: `/README.md` - Project overview
- **Testing Guide**: `/TESTING.md` - General testing information
- **Phase 6 Features**: `/PHASE6.md` - Compression and retention documentation
- **Integration Tests**: `/broker/tests/comprehensive_integration_test.rs`
- **Load Tests**: `/broker/tests/load_test.rs`
- **Chaos Tests**: `/broker/tests/chaos_test.rs`

---

## Support

For issues or questions about the test suite:
1. Check existing test output for error messages
2. Review this documentation
3. Check test source code comments
4. Examine similar passing tests for patterns

Happy Testing! 🚀
