# Gaffa - Kafka-like Message Queue in Rust

A high-performance, distributed message queue system built from scratch in Rust, inspired by Apache Kafka.

## Why Rust?

**Rust is the ideal choice for this project:**
- **Zero-cost abstractions**: Performance comparable to C/C++, faster than Go
- **No garbage collector**: Predictable latency, crucial for message queues
- **Memory safety**: Ownership system prevents data races and memory bugs
- **Fearless concurrency**: Type system catches concurrency bugs at compile time
- **Excellent async/await**: Tokio runtime is production-ready
- **Real-world validation**: Used in production systems like Redpanda, Vector

## Project Structure

```
gaffa/
├── Cargo.toml                 # Workspace configuration
├── broker/                    # Main broker server
│   ├── Cargo.toml
│   └── src/
│       ├── main.rs
│       ├── server.rs         # TCP server
│       ├── topic.rs          # Topic management
│       ├── partition.rs      # Partition logic
│       ├── coordinator.rs    # Consumer group coordination
│       ├── replication.rs    # Replication manager
│       └── cluster.rs        # Cluster metadata
├── storage/                   # Storage engine
│   ├── Cargo.toml
│   └── src/
│       ├── lib.rs
│       ├── segment.rs        # Log segments
│       ├── index.rs          # Offset index
│       ├── record.rs         # Message format
│       └── compression.rs    # Compression support
├── protocol/                  # Wire protocol
│   ├── Cargo.toml
│   └── src/
│       ├── lib.rs
│       ├── messages.rs       # Protocol messages
│       └── codec.rs          # Serialization
├── client/                    # Producer & Consumer
│   ├── Cargo.toml
│   └── src/
│       ├── lib.rs
│       ├── producer.rs
│       └── consumer.rs
├── common/                    # Shared utilities
│   ├── Cargo.toml
│   └── src/
│       ├── lib.rs
│       ├── error.rs
│       └── config.rs
└── examples/                  # Usage examples
    ├── simple_producer.rs
    └── simple_consumer.rs
```

## Core Components

### 1. Message Broker (Server)
The central server that receives, stores, and serves messages:
- TCP server accepting producer/consumer connections
- Topic and partition management
- Message routing and storage
- Offset tracking per consumer group

### 2. Storage Layer
Append-only log storage (Kafka's key innovation):
- **Log segments**: Files on disk storing messages sequentially
- **Index files**: For fast offset lookups
- **Message format**: Binary format with headers (offset, timestamp, key, value)
- **Segment rotation**: Create new segments when size/time limits hit
- **Retention policies**: Time-based or size-based cleanup

### 3. Producer Client
Client library for sending messages:
- Connection pooling
- Partitioning strategy (round-robin, key-based, custom)
- Batching for efficiency
- Retry logic

### 4. Consumer Client
Client library for reading messages:
- Subscribe to topics/partitions
- Offset management (auto-commit, manual commit)
- Consumer group coordination
- Rebalancing when consumers join/leave

### 5. Wire Protocol
Binary protocol over TCP:
- Request/response format
- Operations: Produce, Fetch, Commit Offset, Join Group, etc.
- Efficient serialization using bincode

## Implementation Roadmap

### Phase 1: Foundation & Basic Broker (Week 1-2)

**Goal**: Single-node broker with in-memory storage

**Tasks**:
1. Setup Rust workspace with all crates
2. Define wire protocol messages (Produce, Fetch, CreateTopic)
3. Implement protocol codec (length-prefixed binary format)
4. Build basic TCP server with tokio
5. Create in-memory partition storage
6. Handle basic produce/fetch requests

**Deliverables**:
- ✅ TCP server accepting connections
- ✅ Basic request/response handling
- ✅ In-memory message storage
- ✅ Producer can send messages
- ✅ Consumer can read messages by offset

**Key Technologies**:
- `tokio` - Async runtime
- `tokio-util` - Framed codec for TCP
- `serde` + `bincode` - Serialization
- `bytes` - Efficient byte buffers

---

### Phase 2: Persistent Storage Engine (Week 3-4) ✅ COMPLETED

**Goal**: Durable, append-only log storage on disk

**Tasks**:
1. ✅ Design on-disk record format with CRC32 checksums
2. ✅ Implement log segment writer (append-only files)
3. ✅ Build offset index for fast lookups
4. ✅ Implement memory-mapped file reads
5. ✅ Add segment rotation logic
6. ✅ Integrate storage layer with broker

**Deliverables**:
- ✅ Messages persisted to disk in log files
- ✅ Offset index for fast lookups
- ✅ Segment rotation when size limit reached
- ✅ CRC32 checksums for data integrity
- ✅ Memory-mapped files for fast reads
- ✅ Broker survives restarts with data intact

**On-Disk Format**:
```
Record Format:
[8 bytes: offset]
[4 bytes: message length]
[4 bytes: CRC32]
[8 bytes: timestamp]
[4 bytes: key length (-1 if null)]
[N bytes: key]
[4 bytes: value length]
[N bytes: value]
[4 bytes: header count]
[headers...]

Index Format:
[offset: u64, position: u64] pairs
```

**Key Technologies**:
- `memmap2` - Memory-mapped file I/O
- `crc32fast` - CRC checksums

---

### Phase 3: Partitions & Topics (Week 5) ✅ COMPLETED

**Goal**: Multiple partitions per topic, parallel processing, intelligent partitioning

**Tasks**:
1. ✅ Implement Topic with multiple partitions
2. ✅ Update partition to use persistent log
3. ✅ Add partitioning strategies (round-robin, key-hash, sticky)
4. ✅ Enable parallel writes to different partitions
5. ✅ Update protocol for partition-aware requests
6. ✅ Add metadata discovery API (GetMetadata, GetPartitions, ListTopics)
7. ✅ Implement auto-partitioning producer with caching
8. ✅ Implement topic subscription consumer with offset tracking

**Deliverables**:
- ✅ Topics with configurable partition count
- ✅ Each partition has independent log
- ✅ Producer partitioning strategies (RoundRobin, KeyHash, Sticky)
- ✅ Parallel writes to different partitions
- ✅ Consumer can read from specific partition
- ✅ Metadata API for partition discovery
- ✅ Auto-partitioning producer
- ✅ Topic subscription consumer
- ✅ Automatic offset tracking

**Partitioning Strategies**:
- **RoundRobin**: Distribute evenly across partitions (default)
- **KeyHash**: Same key always goes to same partition (ordering guarantee)
- **Sticky**: Stick to partition for batch efficiency
- **Custom**: Implement Partitioner trait

**See [PHASE3.md](PHASE3.md) for detailed implementation documentation**

---

### Phase 4: Consumer Groups & Offset Management (Week 6-7)

**Goal**: Multiple consumers share partition workload

**Tasks**:
1. Implement offset manager with persistent storage
2. Build consumer group coordinator
3. Add partition assignment (round-robin strategy)
4. Implement heartbeat mechanism
5. Handle consumer join/leave (rebalancing)
6. Build consumer client with auto-commit

**Deliverables**:
- ✅ Consumer groups with automatic rebalancing
- ✅ Partition assignment (round-robin strategy)
- ✅ Offset commit/fetch with persistent storage
- ✅ Heartbeat mechanism
- ✅ Consumer leaves group on disconnect
- ✅ Automatic failure detection and rebalancing
- ✅ Auto-commit support in consumer client

**Consumer Group Protocol**:
1. Consumer sends JoinGroup request
2. Coordinator assigns partitions
3. Consumer fetches committed offsets
4. Consumer polls assigned partitions
5. Consumer sends periodic heartbeats
6. On failure, coordinator triggers rebalance

---

### Phase 5: Replication & High Availability (Week 8-10)

**Goal**: Multiple broker cluster with fault tolerance

**Tasks**:
1. Design cluster metadata structure
2. Implement replication manager
3. Add leader/follower replication per partition
4. Track In-Sync Replicas (ISR)
5. Implement leader election (simplified)
6. Handle broker failures and failover

**Deliverables**:
- ✅ Multiple brokers in cluster
- ✅ Leader/follower replication per partition
- ✅ ISR (In-Sync Replicas) tracking
- ✅ Leader election on failure
- ✅ Data durability with replication factor

**Replication Architecture**:
- Each partition has a leader and N-1 followers
- Leader handles all reads/writes
- Followers continuously fetch from leader
- High watermark = min offset across all ISR replicas
- Only committed messages (below HWM) visible to consumers

---

### Phase 6: Advanced Features (Week 11-12)

**Goal**: Production-ready features

**Tasks**:
1. Add compression support (Gzip, Snappy, LZ4)
2. Implement message batching
3. Add retention policies (time/size based)
4. Build metrics and monitoring (Prometheus)
5. Implement transactional writes (optional)
6. Performance tuning and optimization

**Deliverables**:
- ✅ Message compression
- ✅ Configurable retention policies
- ✅ Metrics for monitoring
- ✅ Production-ready performance

---

## Key Concepts

### Topics & Partitions
- **Topic**: Logical stream of messages (e.g., "user-events")
- **Partition**: Physical log file, enables parallelism
- Messages in a partition are totally ordered

### Offsets
- Sequential ID for each message in a partition (0, 1, 2, ...)
- Consumers track their position by offset
- Enables replay and fault tolerance

### Consumer Groups
- Multiple consumers share partition load
- Each partition assigned to exactly one consumer in group
- Enables horizontal scaling

### Log Segments
- Messages stored in immutable, append-only files
- Segments rotated when size/time limit reached
- Old segments deleted based on retention policy

### Replication
- Each partition replicated across N brokers
- One leader, N-1 followers
- Provides fault tolerance and durability

## Dependencies

```toml
[workspace.dependencies]
tokio = { version = "1.35", features = ["full"] }
bytes = "1.5"
serde = { version = "1.0", features = ["derive"] }
bincode = "1.3"
memmap2 = "0.9"
crc32fast = "1.3"
thiserror = "1.0"
anyhow = "1.0"
tracing = "0.1"
tracing-subscriber = "0.3"
dashmap = "5.5"
```

## Performance Targets

- **Throughput**: 100K+ messages/sec (single partition)
- **Latency**: <10ms p99 for produce
- **Storage**: Efficient with compression
- **Concurrency**: Handle 1000+ concurrent connections

## Testing Strategy

### Unit Tests
- Record encoding/decoding
- Segment append/read operations
- Index lookups
- Partitioning strategies

### Integration Tests
- End-to-end produce/consume flow
- Consumer group rebalancing
- Broker failover
- Data persistence across restarts

### Performance Tests
- Throughput benchmarks
- Latency measurements
- Concurrent client stress tests

## Usage Examples

### Producer
```rust
use gaffa::client::Producer;
use gaffa::protocol::Message;

#[tokio::main]
async fn main() {
    let producer = Producer::connect("localhost:9092").await.unwrap();

    let message = Message::new()
        .key(b"user-123")
        .value(b"User logged in");

    producer.send("events", message).await.unwrap();
}
```

### Consumer
```rust
use gaffa::client::Consumer;

#[tokio::main]
async fn main() {
    let mut consumer = Consumer::connect("localhost:9092")
        .group_id("my-app")
        .await
        .unwrap();

    consumer.subscribe(vec!["events"]).await.unwrap();

    loop {
        let records = consumer.poll(Duration::from_secs(1)).await.unwrap();
        for record in records {
            println!("Received: {:?}", record.value);
        }
    }
}
```

## Getting Started

### Build
```bash
cargo build --release
```

### Run Broker
```bash
cargo run --bin broker -- --port 9092 --data-dir /tmp/gaffa
```

### Run Examples
```bash
# Producer
cargo run --example simple_producer

# Consumer
cargo run --example simple_consumer
```

## Contributing

This is a learning project to understand distributed systems and Rust. Contributions and suggestions are welcome!

## Resources

- [Apache Kafka Documentation](https://kafka.apache.org/documentation/)
- [Designing Data-Intensive Applications](https://dataintensive.net/) - Chapter 11
- [The Log: What every software engineer should know](https://engineering.linkedin.com/distributed-systems/log-what-every-software-engineer-should-know-about-real-time-datas-unifying)
- [Tokio Documentation](https://tokio.rs/)
- [Rust Async Book](https://rust-lang.github.io/async-book/)

## License

MIT License - Feel free to use this for learning and experimentation.

---

**Current Status**: Phase 4 completed - Consumer groups with automatic partition assignment, persistent offset management, and heartbeat-based failure detection. Horizontal scaling of consumers with automatic rebalancing. All 92 tests passing. See [PHASE4.md](PHASE4.md) for details.
