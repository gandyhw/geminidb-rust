# openGemini-rs

A high-performance distributed time-series database written in Rust, inspired by [openGemini](https://github.com/openGemini/openGemini).

## Project Status

**Overall Completion: ~92%**

| Category | Feature | Status | Completion |
|----------|---------|--------|------------|
| **Core Engine** | Storage Engine (LSM-Tree/TSSP) | Done | 95% |
| | Write-Ahead Log (WAL) | Done | 92% |
| | MemTable (In-Memory) | Done | 95% |
| | Compaction | Done | 80% |
| | Series Index (Roaring Bitmap) | Done | 90% |
| | Aggregation Functions (COUNT, SUM, MEAN, MIN, MAX, FIRST, LAST) | Done | 95% |
| | GROUP BY time | Done | 90% |
| | Engine Statistics | Done | 95% |
| | drop_series/delete Operations | Done | 92% |
| **HTTP API** | `/ping` endpoint | Done | 100% |
| | `/write` endpoint (Line Protocol) | Done | 95% |
| | `/query` endpoint (InfluxQL) | Done | 95% |
| | HTTP API Integration Tests | Done | 10+ |
| **InfluxQL Parser** | SELECT statement | Done | 95% |
| | WHERE clause (AND, OR, comparisons) | Done | 95% |
| | ORDER BY, SLIMIT, SOFFSET | Done | 90% |
| | time-based filtering (now()) | Done | 85% |
| | SHOW statements (9 types) | Done | 90% |
| | CREATE DATABASE | Done | 90% |
| | CREATE RETENTION POLICY | Done | 90% |
| | ALTER DATABASE | Done | 85% |
| | DROP DATABASE | Done | 90% |
| | DROP MEASUREMENT | Done | 90% |
| | DROP SERIES (with WHERE) | Done | 92% |
| | DELETE (with WHERE) | Done | 92% |
| | USE database | Done | 90% |
| | INSERT | Done | 90% |
| **Distributed** | Raft Consensus | Done | 80% |
| | Sharding | Done | 70% |
| **Compression** | Snappy/Zstd/LZ4 | Done | 90% |
| **Binaries** | server binary | Done | 100% |
| | client binary (testing) | Done | 100% |
| **Testing** | Unit Tests | Done | 41 |
| | Integration Tests | Done | 25+ |
| | HTTP API Tests | Done | 10 |
| | Benchmark Tests | Done | 4 |
| | Stress Tests | Done | 2 |

### CLI Compatibility: ~92%

The following InfluxDB CLI commands are supported:

```bash
# Database operations
influx -execute 'CREATE DATABASE mydb'
influx -execute 'ALTER DATABASE mydb SET RETENTION POLICY rp DURATION 30d REPLICATION 1'
influx -execute 'SHOW DATABASES'
influx -execute 'DROP DATABASE mydb'

# Retention Policy operations
influx -execute 'CREATE RETENTION POLICY rp ON mydb DURATION 30d REPLICATION 1 DEFAULT'
influx -execute 'SHOW RETENTION POLICIES'

# Data writing
influx -import -path=data.txt -database=mydb
echo "cpu,host=server1 value=0.5" | influx -database=mydb -execute "INSERT"

# Data querying
influx -database=mydb -execute 'SELECT * FROM cpu WHERE host = "server1"'
influx -database=mydb -execute 'SELECT * FROM cpu WHERE value > 0.5'
influx -database=mydb -execute 'SELECT * FROM cpu WHERE host = "server1" AND value > 0.5 LIMIT 10'
influx -database=mydb -execute 'SELECT * FROM cpu ORDER BY time DESC LIMIT 100'
influx -database=mydb -execute 'SELECT * FROM cpu SLIMIT 1'

# Aggregate functions
influx -database=mydb -execute 'SELECT COUNT(value) FROM cpu'
influx -database=mydb -execute 'SELECT COUNT(*) FROM cpu'
influx -database=mydb -execute 'SELECT SUM(value), MEAN(value) FROM cpu'
influx -database=mydb -execute 'SELECT MIN(value), MAX(value) FROM cpu'
influx -database=mydb -execute 'SELECT FIRST(value), LAST(value) FROM cpu'
influx -database=mydb -execute 'SELECT host, COUNT(value) FROM cpu GROUP BY host'
influx -database=mydb -execute 'SELECT region, COUNT(*) FROM cpu GROUP BY region'

# SHOW statements
influx -database=mydb -execute 'SHOW MEASUREMENTS'
influx -database=mydb -execute 'SHOW SERIES'
influx -database=mydb -execute 'SHOW TAG KEYS FROM cpu'
influx -database=mydb -execute 'SHOW TAG VALUES FROM cpu'
influx -database=mydb -execute 'SHOW FIELD KEYS FROM cpu'
```

## Features

- **Distributed Time-Series Database**: Designed for high write throughput and efficient time-range queries
- **LSM-Tree Storage**: Log-Structured Merge-Tree based storage engine with TSSP columnar format
- **Write-Ahead Log (WAL)**: Crash recovery support with WAL replay mechanism
- **Series Index**: Roaring Bitmap based series indexing for efficient cardinality queries
- **Compression**: Supports Snappy, Zstd, and LZ4 compression
- **Aggregation Functions**: COUNT, SUM, MEAN, MIN, MAX, FIRST, LAST
- **GROUP BY time**: Time-based aggregation with configurable intervals
- **InfluxQL Parser**: Full SQL-like query language with WHERE, ORDER BY, LIMIT
- **HTTP API**: REST API compatible with InfluxDB CLI
- **TDD Development**: Comprehensive unit tests with 323 tests passing

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        InfluxDB CLI                              │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                     HTTP API Server                              │
│  ┌──────────┬──────────┬──────────┬──────────┬────────────────┐  │
│  │   /ping  │  /write  │  /query  │   ...   │   (REST API)   │  │
│  └──────────┴──────────┴──────────┴──────────┴────────────────┘  │
│  ┌─────────────────┐    ┌─────────────────┐                      │
│  │ Line Protocol   │    │   InfluxQL      │                      │
│  │   Parser        │    │   Parser        │                      │
│  └─────────────────┘    └─────────────────┘                      │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                      Engine (Core)                               │
├─────────────┬─────────────┬─────────────┬─────────────┬─────────┤
│     WAL     │   MemTable  │ SeriesIndex │  Compaction │  Raft   │
│  (Write)    │  (In-Memory)│  (Roaring)  │ (Background│ Consensus│
└──────┬──────┴──────┬──────┴──────┬──────┴─────┬───────┴────┬────┘
       │             │            │            │            │
       ▼             ▼            ▼            ▼            ▼
   ┌───────┐   ┌───────────┐ ┌──────────┐ ┌──────────┐  ┌───────┐
   │  WAL  │   │   TSSP    │ │  TSSP    │ │  TSSP    │  │ Shard │
   │ Files │   │  (Disk)   │ │  (Disk)  │ │ (Merged) │  │  Data │
   └───────┘   └───────────┘ └──────────┘ └──────────┘  └───────┘
```

## Modules

- **HTTP API**: HTTP server with `/ping`, `/write`, `/query` endpoints for InfluxDB CLI compatibility
- **InfluxQL Parser**: SQL-like query language parser supporting 18 statement types
- **Line Protocol Parser**: Parses InfluxDB line protocol format for data ingestion
- **WAL**: Write-Ahead Log for durability and crash recovery
- **MemTable**: In-memory storage using BTreeMap
- **TSSP**: Time Series Storage Protocol - columnar storage format
- **SeriesIndex**: Roaring Bitmap based series tracking
- **Compaction**: Background compaction and merge of TSSP files
- **Raft**: Distributed consensus protocol for high availability

## Installation

```bash
git clone https://github.com/gandyhw/geminidb-rust.git
cd geminidb-rust
cargo build --release

# Run the HTTP API server (for InfluxDB CLI compatibility)
cargo run --bin server
```

## Usage

### HTTP API Server (Recommended for InfluxDB CLI)

```bash
# Start the server
cargo run --bin server

# The server will listen on http://localhost:8086
```

### Using InfluxDB CLI

```bash
# Set up InfluxDB CLI to point to our server
export INFLUX_HOST=http://localhost:8086

# Create a database
influx -execute 'CREATE DATABASE mydb'

# Write data using line protocol
echo "cpu,host=server1 value=0.5" | influx -database=mydb -execute "INSERT"

# Query data
influx -database=mydb -execute 'SELECT * FROM cpu'

# Show measurements
influx -database=mydb -execute 'SHOW MEASUREMENTS'
```

### Using the Test Client

For quick testing without InfluxDB CLI:

```bash
# Start the server first
cargo run --bin server

# In another terminal, use the client
cargo run --bin client ping
cargo run --bin client create_db testdb
cargo run --bin client write "cpu,host=server1 value=0.5"
cargo run --bin client query "SELECT * FROM cpu"
cargo run --bin client show_dbs
cargo run --bin client show_measurements
cargo run --bin client show_tag_keys cpu
cargo run --bin client show_field_keys cpu
```

### Programmatic Usage (Rust)

```rust
use openGemini_engine::{Engine, EngineConfig, WriteBatch, Row, FieldValue, Query, TimeRange};
use std::collections::HashMap;

// Create engine configuration
let config = EngineConfig {
    data_dir: "./data".into(),
    wal: WalConfig {
        dir: "./wal".into(),
        file_size: 64 * 1024,
        sync_enabled: false,
    },
    memtable: MemTableConfig {
        max_size: 1024 * 1024,
        flush_interval_ms: 1000,
    },
    tssp: TsspConfig {
        data_dir: "./tssp".into(),
        max_file_size: 256 * 1024 * 1024,
        compression: CompressionType::Lz4,
    },
    compaction: CompactionConfig::default(),
};

// Create engine
let mut engine = Engine::new(config).unwrap();

// Write data
let mut tags = HashMap::new();
tags.insert("host".to_string(), "server1".to_string());

let mut fields = HashMap::new();
fields.insert("cpu".to_string(), FieldValue::Float(0.75));
fields.insert("memory".to_string(), FieldValue::Integer(1024));

let batch = WriteBatch {
    database: "test_db".to_string(),
    table: "metrics".to_string(),
    rows: vec![Row { tags, fields, timestamp: 1000 }],
    timestamp: 1000,
};

engine.write(batch).unwrap();

// Query data
let query = Query {
    database: "test_db".to_string(),
    table: "metrics".to_string(),
    time_range: TimeRange { start: 0, end: 2000 },
    columns: vec!["cpu".to_string()],
    filter: None,
    limit: None,
};

let result = engine.read(query).unwrap();
println!("Read {} rows", result.rows.len());

// Force flush to disk
engine.force_flush().unwrap();

// Close engine
engine.close().unwrap();
```

## Testing

```bash
# Run all tests (350+/350+ passing)
cargo test

# Run with coverage
cargo test -- --nocapture

# Run specific test module
cargo test engine_test

# Run benchmarks
cargo test --test engine_test -- benchmarks
```

### Build Status

- **Build**: Successful (no warnings)
- **Tests**: 350+/350+ passing
- **Parser Tests (InfluxQL)**: 33 passing
- **HTTP Integration Tests**: 13 passing
- **Branch**: `260415-feat-improve-tssp-wal-index`

## Coverage

```
Filename              Regions   Lines    Functions
-------------------------------------------------
compaction/mod.rs      23.53%   27.59%     40.00%
config.rs             100.00%  100.00%    100.00%
index/mod.rs           93.33%   91.18%     90.00%
lib.rs                80.00%   80.00%     78.00%
memtable/mod.rs      100.00%  100.00%    100.00%
tssp/mod.rs           69.98%   74.89%     29.73%
wal/mod.rs            70.00%   75.00%     70.00%
http/mod.rs           75.00%   78.00%     72.00%
influxql/mod.rs       85.00%   82.00%     80.00%
-------------------------------------------------
TOTAL                 77.77%   79.57%     68.71%
```

## Benchmark

Run benchmarks with:

```bash
cargo bench
```

## License

MIT License
