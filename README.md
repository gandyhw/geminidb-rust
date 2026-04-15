# openGemini-rs

A high-performance distributed time-series database written in Rust, inspired by [openGemini](https://github.com/openGemini/openGemini).

## Features

- **Distributed Time-Series Database**: Designed for high write throughput and efficient time-range queries
- **LSM-Tree Storage**: Log-Structured Merge-Tree based storage engine with TSSP columnar format
- **Write-Ahead Log (WAL)**: Crash recovery support with WAL replay mechanism
- **Series Index**: Roaring Bitmap based series indexing for efficient cardinality queries
- **Compression**: Supports Snappy, Zstd, and LZ4 compression
- **TDD Development**: Comprehensive unit tests with 77%+ coverage

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                      Engine                              │
├─────────────┬─────────────┬─────────────┬───────────────┤
│     WAL     │   MemTable  │  SeriesIndex │  Compaction  │
│  (Write)    │  (In-Memory)│  (Roaring)  │  (Background) │
└──────┬──────┴──────┬──────┴──────┬──────┴───────┬───────┘
       │             │            │              │
       ▼             ▼            ▼              ▼
   ┌───────┐   ┌───────────┐ ┌──────────┐  ┌──────────┐
   │  WAL  │   │   TSSP    │ │  TSSP    │  │  TSSP    │
   │ Files │   │  (Disk)   │ │  (Disk)  │  │  (Merged)│
   └───────┘   └───────────┘ └──────────┘  └──────────┘
```

## Modules

- **WAL**: Write-Ahead Log for durability and crash recovery
- **MemTable**: In-memory storage using BTreeMap
- **TSSP**: Time Series Storage Protocol - columnar storage format
- **SeriesIndex**: Roaring Bitmap based series tracking
- **Compaction**: Background compaction and merge of TSSP files

## Installation

```bash
git clone https://github.com/gandyhw/geminidb-rust.git
cd geminidb-rust
cargo build --release
```

## Usage

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
cargo test
```

## Coverage

```
Filename              Regions   Lines    Functions
-------------------------------------------------
compaction/mod.rs      23.53%   27.59%     40.00%
config.rs             100.00%  100.00%    100.00%
index/mod.rs           93.33%   91.18%     90.00%
lib.rs                76.16%   76.01%     75.68%
memtable/mod.rs      100.00%  100.00%    100.00%
tssp/mod.rs           69.98%   74.89%     29.73%
wal/mod.rs            69.31%   74.25%     68.42%
-------------------------------------------------
TOTAL                 74.77%   77.57%     63.71%
```

## Benchmark

Run benchmarks with:

```bash
cargo bench
```

## License

MIT License
