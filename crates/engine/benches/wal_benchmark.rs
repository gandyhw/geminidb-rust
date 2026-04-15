use std::hint::black_box;
use criterion::{criterion_group, criterion_main, Criterion};
use openGemini_engine::config::WalConfig;
use openGemini_engine::wal::Wal;
use openGemini_engine::{FieldValue, Row, WriteBatch};
use std::collections::HashMap;
use tempfile::TempDir;

fn create_test_batch(table: &str, timestamp: i64) -> WriteBatch {
    let mut tags = HashMap::new();
    tags.insert("host".to_string(), format!("server{}", timestamp));
    tags.insert("region".to_string(), "us-east".to_string());

    let mut fields = HashMap::new();
    fields.insert("cpu".to_string(), FieldValue::Float(0.5));
    fields.insert("memory".to_string(), FieldValue::Integer(1024));

    WriteBatch {
        database: "test_db".to_string(),
        table: table.to_string(),
        rows: vec![Row {
            tags,
            fields,
            timestamp,
        }],
        timestamp,
    }
}

fn bench_wal_write(c: &mut Criterion) {
    c.bench_function("wal_write_100", |b| {
        b.iter(|| {
            let temp_dir = TempDir::new().unwrap();
            let config = WalConfig {
                dir: temp_dir.path().to_path_buf(),
                file_size: 64 * 1024 * 1024,
                sync_enabled: false,
            };
            let wal = Wal::new(&config).unwrap();
            
            for i in 0..100 {
                let batch = create_test_batch("cpu", i as i64);
                let _ = wal.write(&batch);
            }
            black_box(wal);
        });
    });

    c.bench_function("wal_write_1000", |b| {
        b.iter(|| {
            let temp_dir = TempDir::new().unwrap();
            let config = WalConfig {
                dir: temp_dir.path().to_path_buf(),
                file_size: 64 * 1024 * 1024,
                sync_enabled: false,
            };
            let wal = Wal::new(&config).unwrap();
            
            for i in 0..1000 {
                let batch = create_test_batch("cpu", i as i64);
                let _ = wal.write(&batch);
            }
            black_box(wal);
        });
    });
}

fn bench_wal_read(c: &mut Criterion) {
    let temp_dir = TempDir::new().unwrap();
    let config = WalConfig {
        dir: temp_dir.path().to_path_buf(),
        file_size: 64 * 1024 * 1024,
        sync_enabled: false,
    };
    let wal = Wal::new(&config).unwrap();
    
    for i in 0..1000 {
        let batch = create_test_batch("cpu", i as i64);
        wal.write(&batch).unwrap();
    }
    
    c.bench_function("wal_read_1000", |b| {
        b.iter(|| {
            for i in 0..1000 {
                let _ = wal.read(1, i as u64 * 200);
            }
        });
    });
}

criterion_group! {
    name = benches;
    config = Criterion::default();
    targets = bench_wal_write, bench_wal_read
}
criterion_main!(benches);
