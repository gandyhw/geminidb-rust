use std::hint::black_box;
use criterion::{criterion_group, criterion_main, Criterion};
use openGemini_engine::config::{EngineConfig, WalConfig, MemTableConfig, TsspConfig, CompactionConfig, CompressionType};
use openGemini_engine::{Engine, FieldValue, Query, Row, TimeRange, WriteBatch};
use std::collections::HashMap;
use tempfile::TempDir;

fn create_test_engine_config(temp_dir: &TempDir) -> EngineConfig {
    EngineConfig {
        data_dir: temp_dir.path().to_path_buf(),
        wal: WalConfig {
            dir: temp_dir.path().join("wal"),
            file_size: 64 * 1024 * 1024,
            sync_enabled: false,
        },
        memtable: MemTableConfig {
            max_size: 1024 * 1024 * 1024,
            flush_interval_ms: 1000,
        },
        tssp: TsspConfig {
            data_dir: temp_dir.path().join("data"),
            max_file_size: 256 * 1024 * 1024,
            compression: CompressionType::None,
        },
        compaction: CompactionConfig::default(),
    }
}

fn create_write_batch(timestamp: i64) -> WriteBatch {
    let mut tags = HashMap::new();
    tags.insert("host".to_string(), format!("server{}", timestamp));
    tags.insert("region".to_string(), "us-east".to_string());

    let mut fields = HashMap::new();
    fields.insert("cpu".to_string(), FieldValue::Float(0.5));
    fields.insert("memory".to_string(), FieldValue::Integer(1024));
    fields.insert("disk".to_string(), FieldValue::Unsigned(50000));

    WriteBatch {
        database: "test_db".to_string(),
        table: "cpu_metrics".to_string(),
        rows: vec![Row {
            tags,
            fields,
            timestamp,
        }],
        timestamp,
    }
}

fn bench_engine_write(c: &mut Criterion) {
    c.bench_function("engine_write_100", |b| {
        b.iter(|| {
            let temp_dir = TempDir::new().unwrap();
            let config = create_test_engine_config(&temp_dir);
            let mut engine = Engine::new(config).unwrap();
            
            for i in 0..100 {
                let batch = create_write_batch(i as i64 * 1000);
                engine.write(batch).unwrap();
            }
            
            black_box(engine);
        });
    });

    c.bench_function("engine_write_1000", |b| {
        b.iter(|| {
            let temp_dir = TempDir::new().unwrap();
            let config = create_test_engine_config(&temp_dir);
            let mut engine = Engine::new(config).unwrap();
            
            for i in 0..1000 {
                let batch = create_write_batch(i as i64 * 1000);
                engine.write(batch).unwrap();
            }
            
            black_box(engine);
        });
    });
}

fn bench_engine_read(c: &mut Criterion) {
    let temp_dir = TempDir::new().unwrap();
    let config = create_test_engine_config(&temp_dir);
    let mut engine = Engine::new(config).unwrap();
    
    for i in 0..1000 {
        let batch = create_write_batch(i as i64 * 1000);
        engine.write(batch).unwrap();
    }
    
    c.bench_function("engine_read_1000", |b| {
        b.iter(|| {
            let query = Query {
                database: "test_db".to_string(),
                table: "cpu_metrics".to_string(),
                time_range: TimeRange { start: 0, end: 1000000 },
                columns: vec!["cpu".to_string(), "memory".to_string()],
                filter: None,
                limit: None,
            };
            let _ = engine.read(query);
        });
    });
}

fn bench_engine_flush(c: &mut Criterion) {
    c.bench_function("engine_flush_1000", |b| {
        b.iter(|| {
            let temp_dir = TempDir::new().unwrap();
            let config = create_test_engine_config(&temp_dir);
            let mut engine = Engine::new(config).unwrap();
            
            for i in 0..1000 {
                let batch = create_write_batch(i as i64 * 1000);
                engine.write(batch).unwrap();
            }
            
            engine.flush().unwrap();
            black_box(engine);
        });
    });
}

fn bench_engine_write_and_flush(c: &mut Criterion) {
    c.bench_function("engine_write_and_flush_1000", |b| {
        b.iter(|| {
            let temp_dir = TempDir::new().unwrap();
            let config = create_test_engine_config(&temp_dir);
            let mut engine = Engine::new(config).unwrap();
            
            for i in 0..1000 {
                let batch = create_write_batch(i as i64 * 1000);
                engine.write(batch).unwrap();
            }
            
            engine.close().unwrap();
            black_box(());
        });
    });
}

criterion_group! {
    name = benches;
    config = Criterion::default();
    targets = bench_engine_write, bench_engine_read, bench_engine_flush, bench_engine_write_and_flush
}
criterion_main!(benches);
