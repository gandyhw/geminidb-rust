use std::hint::black_box;
use criterion::{criterion_group, criterion_main, Criterion};
use openGemini_engine::memtable::MemTable;
use openGemini_engine::{FieldValue, Row, WriteBatch};
use std::collections::HashMap;

fn create_test_batch(table: &str, timestamps: &[i64]) -> WriteBatch {
    let rows: Vec<Row> = timestamps.iter().map(|&ts| {
        let mut tags = HashMap::new();
        tags.insert("host".to_string(), format!("server{}", ts));
        tags.insert("region".to_string(), "us-east".to_string());

        let mut fields = HashMap::new();
        fields.insert("cpu".to_string(), FieldValue::Float(0.5));
        fields.insert("memory".to_string(), FieldValue::Integer(1024));

        Row { tags, fields, timestamp: ts }
    }).collect();

    WriteBatch {
        database: "test_db".to_string(),
        table: table.to_string(),
        rows,
        timestamp: timestamps[0],
    }
}

fn bench_memtable_insert(c: &mut Criterion) {
    c.bench_function("memtable_insert_100", |b| {
        b.iter(|| {
            let mut memtable = MemTable::new(1024 * 1024 * 1024);
            for i in 0..100 {
                let batch = create_test_batch("cpu", &[i as i64 * 1000]);
                memtable.insert(batch).unwrap();
            }
            black_box(memtable);
        });
    });

    c.bench_function("memtable_insert_1000", |b| {
        b.iter(|| {
            let mut memtable = MemTable::new(1024 * 1024 * 1024);
            for i in 0..1000 {
                let batch = create_test_batch("cpu", &[i as i64 * 1000]);
                memtable.insert(batch).unwrap();
            }
            black_box(memtable);
        });
    });

    c.bench_function("memtable_insert_10000", |b| {
        b.iter(|| {
            let mut memtable = MemTable::new(1024 * 1024 * 1024);
            for i in 0..10000 {
                let batch = create_test_batch("cpu", &[i as i64 * 1000]);
                memtable.insert(batch).unwrap();
            }
            black_box(memtable);
        });
    });
}

fn bench_memtable_scan(c: &mut Criterion) {
    let mut memtable = MemTable::new(1024 * 1024 * 1024);
    for i in 0..10000 {
        let batch = create_test_batch("cpu", &[i as i64 * 1000]);
        memtable.insert(batch).unwrap();
    }
    
    c.bench_function("memtable_scan_10000_range", |b| {
        b.iter(|| {
            let _ = memtable.scan(b"cpu", 2500000, 7500000);
        });
    });
}

fn bench_memtable_flush(c: &mut Criterion) {
    c.bench_function("memtable_flush_1000", |b| {
        b.iter(|| {
            let mut memtable = MemTable::new(1024 * 1024 * 1024);
            for i in 0..1000 {
                let batch = create_test_batch("cpu", &[i as i64 * 1000]);
                memtable.insert(batch).unwrap();
            }
            memtable.flush().unwrap();
            black_box(memtable);
        });
    });
}

criterion_group! {
    name = benches;
    config = Criterion::default();
    targets = bench_memtable_insert, bench_memtable_scan, bench_memtable_flush
}
criterion_main!(benches);
