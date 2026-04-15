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
    WriteBatch { database: "test_db".to_string(), table: table.to_string(), rows, timestamp: timestamps[0] }
}

#[cfg(test)]
mod benchmarks {
    use super::*;
    use std::time::Instant;

    #[test]
    pub fn bench_memtable_insert_1000() {
        let start = Instant::now();
        let mut memtable = MemTable::new(1024 * 1024 * 1024);
        for i in 0..1000 {
            let batch = create_test_batch("cpu", &[i as i64 * 1000]);
            memtable.insert(batch).unwrap();
        }
        let elapsed = start.elapsed();
        println!("memtable_insert_1000: {:?}", elapsed);
        assert!(elapsed.as_millis() < 1000);
    }

    #[test]
    pub fn bench_memtable_insert_10000() {
        let start = Instant::now();
        let mut memtable = MemTable::new(1024 * 1024 * 1024);
        for i in 0..10000 {
            let batch = create_test_batch("cpu", &[i as i64 * 1000]);
            memtable.insert(batch).unwrap();
        }
        let elapsed = start.elapsed();
        println!("memtable_insert_10000: {:?}", elapsed);
        assert!(elapsed.as_millis() < 5000);
    }

    #[test]
    pub fn bench_memtable_scan() {
        let mut memtable = MemTable::new(1024 * 1024 * 1024);
        for i in 0..10000 {
            let batch = create_test_batch("cpu", &[i as i64 * 1000]);
            memtable.insert(batch).unwrap();
        }
        let start = Instant::now();
        let _ = memtable.scan(b"cpu", 2500000, 7500000);
        let elapsed = start.elapsed();
        println!("memtable_scan_10000: {:?}", elapsed);
    }

    #[test]
    pub fn bench_memtable_flush() {
        let start = Instant::now();
        let mut memtable = MemTable::new(1024 * 1024 * 1024);
        for i in 0..1000 {
            let batch = create_test_batch("cpu", &[i as i64 * 1000]);
            memtable.insert(batch).unwrap();
        }
        memtable.flush().unwrap();
        let elapsed = start.elapsed();
        println!("memtable_flush_1000: {:?}", elapsed);
    }
}
