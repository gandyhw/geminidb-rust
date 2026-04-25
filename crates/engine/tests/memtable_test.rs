use openGemini_engine::config::MemTableConfig;
use openGemini_engine::memtable::MemTable;
use openGemini_engine::{FieldValue, Row, WriteBatch};
use std::collections::HashMap;

fn create_test_row(timestamp: i64) -> Row {
    let mut tags = HashMap::new();
    tags.insert("host".to_string(), format!("server{}", timestamp));
    tags.insert("region".to_string(), "us-east".to_string());

    let mut fields = HashMap::new();
    fields.insert("cpu".to_string(), FieldValue::Float(0.5));
    fields.insert("memory".to_string(), FieldValue::Integer(1024));
    fields.insert("count".to_string(), FieldValue::Unsigned(100));
    fields.insert("error".to_string(), FieldValue::Boolean(false));
    fields.insert("name".to_string(), FieldValue::String(b"test".to_vec()));

    Row {
        tags,
        fields,
        timestamp,
    }
}

fn create_test_batch(table: &str, timestamps: &[i64]) -> WriteBatch {
    let rows: Vec<Row> = timestamps.iter().map(|&ts| create_test_row(ts)).collect();

    WriteBatch {
        database: "test_db".to_string(),
        table: table.to_string(),
        rows,
        timestamp: timestamps[0],
    }
}

#[test]
fn test_memtable_new() {
    let memtable = MemTable::new(1024 * 1024);
    assert_eq!(memtable.size(), 0);
    assert_eq!(memtable.row_count(), 0);
    assert!(!memtable.should_flush());
}

#[test]
fn test_memtable_insert_single_row() {
    let mut memtable = MemTable::new(1024 * 1024);
    let batch = create_test_batch("cpu", &[1000]);

    let result = memtable.insert(batch);
    assert!(result.is_ok());
    assert_eq!(memtable.row_count(), 1);
    assert!(memtable.size() > 0);
}

#[test]
fn test_memtable_insert_multiple_rows() {
    let mut memtable = MemTable::new(1024 * 1024);
    let batch = create_test_batch("cpu", &[1000, 2000, 3000]);

    let result = memtable.insert(batch);
    assert!(result.is_ok());
    assert_eq!(memtable.row_count(), 3);
}

#[test]
fn test_memtable_scan() {
    let mut memtable = MemTable::new(1024 * 1024);
    let batch = create_test_batch("cpu", &[1000, 2000, 3000, 4000, 5000]);
    memtable.insert(batch).unwrap();

    let results = memtable.scan(b"cpu", 2000, 4000).unwrap();
    assert_eq!(results.len(), 2);
}

#[test]
fn test_memtable_scan_all() {
    let mut memtable = MemTable::new(1024 * 1024);
    let batch = create_test_batch("cpu", &[1000, 2000, 3000]);
    memtable.insert(batch).unwrap();

    let results = memtable.scan(b"cpu", 0, i64::MAX).unwrap();
    assert_eq!(results.len(), 3);
}

#[test]
fn test_memtable_scan_empty_range() {
    let mut memtable = MemTable::new(1024 * 1024);
    let batch = create_test_batch("cpu", &[1000, 2000, 3000]);
    memtable.insert(batch).unwrap();

    let results = memtable.scan(b"cpu", 5000, 6000).unwrap();
    assert_eq!(results.len(), 0);
}

#[test]
fn test_memtable_scan_different_table() {
    let mut memtable = MemTable::new(1024 * 1024);
    let batch1 = create_test_batch("cpu", &[1000, 2000]);
    let batch2 = create_test_batch("memory", &[3000, 4000]);
    memtable.insert(batch1).unwrap();
    memtable.insert(batch2).unwrap();

    let cpu_results = memtable.scan(b"cpu", 0, i64::MAX).unwrap();
    let mem_results = memtable.scan(b"memory", 0, i64::MAX).unwrap();

    assert_eq!(cpu_results.len(), 2);
    assert_eq!(mem_results.len(), 2);
}

#[test]
fn test_memtable_should_flush() {
    let mut memtable = MemTable::new(100);
    let batch = create_test_batch("cpu", &[1000]);

    memtable.insert(batch).unwrap();
    assert!(memtable.should_flush());
}

#[test]
fn test_memtable_flush() {
    let mut memtable = MemTable::new(1024 * 1024);
    let batch = create_test_batch("cpu", &[1000, 2000, 3000]);
    memtable.insert(batch).unwrap();

    let rows = memtable.flush().unwrap();
    assert_eq!(rows.len(), 3);
    assert_eq!(memtable.row_count(), 0);
    assert_eq!(memtable.size(), 0);
}

#[test]
fn test_memtable_default_config() {
    let config = MemTableConfig::default();
    assert_eq!(config.max_size, 64 * 1024 * 1024);
    assert_eq!(config.flush_interval_ms, 1000);
}

#[test]
fn test_memtable_field_value_types() {
    let mut memtable = MemTable::new(1024 * 1024);
    let mut fields = HashMap::new();
    fields.insert("int_field".to_string(), FieldValue::Integer(-42));
    fields.insert("uint_field".to_string(), FieldValue::Unsigned(42));
    fields.insert("float_field".to_string(), FieldValue::Float(3.14));
    fields.insert("bool_field".to_string(), FieldValue::Boolean(true));
    fields.insert("string_field".to_string(), FieldValue::String(b"hello".to_vec()));

    let row = Row {
        tags: HashMap::new(),
        fields,
        timestamp: 1000,
    };

    let batch = WriteBatch {
        database: "test".to_string(),
        table: "test".to_string(),
        rows: vec![row],
        timestamp: 1000,
    };

    let result = memtable.insert(batch);
    assert!(result.is_ok());
}

#[test]
fn test_memtable_clear() {
    let mut memtable = MemTable::new(1024 * 1024);
    let batch = create_test_batch("cpu", &[1000, 2000, 3000]);
    memtable.insert(batch).unwrap();

    assert_eq!(memtable.row_count(), 3);
    assert!(memtable.size() > 0);

    memtable.clear().unwrap();

    assert_eq!(memtable.row_count(), 0);
    assert_eq!(memtable.size(), 0);
}

#[test]
fn test_memtable_scan_boundary_timestamps() {
    let mut memtable = MemTable::new(1024 * 1024);
    let batch = create_test_batch("cpu", &[1000, 2000, 3000]);
    memtable.insert(batch).unwrap();

    let results = memtable.scan(b"cpu", 1000, 1001).unwrap();
    assert_eq!(results.len(), 1);

    let results = memtable.scan(b"cpu", 3000, 3001).unwrap();
    assert_eq!(results.len(), 1);

    let results = memtable.scan(b"cpu", 999, 1001).unwrap();
    assert_eq!(results.len(), 1);
}

#[test]
fn test_memtable_exact_flush_threshold() {
    let mut memtable = MemTable::new(200);
    let batch1 = create_test_batch("cpu", &[1000]);
    memtable.insert(batch1).unwrap();

    assert!(memtable.should_flush());

    let batch2 = create_test_batch("memory", &[2000]);
    memtable.insert(batch2).unwrap();

    assert!(memtable.should_flush());
}

#[test]
fn test_memtable_multiple_batches_same_table() {
    let mut memtable = MemTable::new(1024 * 1024);

    let batch1 = create_test_batch("cpu", &[1000, 2000]);
    memtable.insert(batch1).unwrap();

    let batch2 = create_test_batch("cpu", &[3000, 4000]);
    memtable.insert(batch2).unwrap();

    let results = memtable.scan(b"cpu", 0, i64::MAX).unwrap();
    assert_eq!(results.len(), 4);
}

#[test]
fn test_memtable_scan_after_clear() {
    let mut memtable = MemTable::new(1024 * 1024);
    let batch = create_test_batch("cpu", &[1000, 2000]);
    memtable.insert(batch).unwrap();

    memtable.clear().unwrap();

    let results = memtable.scan(b"cpu", 0, i64::MAX).unwrap();
    assert_eq!(results.len(), 0);
}

#[test]
fn test_memtable_row_key_ordering() {
    let mut memtable = MemTable::new(1024 * 1024);

    let mut tags1 = std::collections::HashMap::new();
    tags1.insert("host".to_string(), "server1".to_string());
    let row1 = Row {
        tags: tags1,
        fields: std::collections::HashMap::new(),
        timestamp: 1000,
    };
    let batch1 = WriteBatch {
        database: "test_db".to_string(),
        table: "cpu".to_string(),
        rows: vec![row1],
        timestamp: 1000,
    };
    memtable.insert(batch1).unwrap();

    let mut tags2 = std::collections::HashMap::new();
    tags2.insert("host".to_string(), "server2".to_string());
    let row2 = Row {
        tags: tags2,
        fields: std::collections::HashMap::new(),
        timestamp: 1000,
    };
    let batch2 = WriteBatch {
        database: "test_db".to_string(),
        table: "cpu".to_string(),
        rows: vec![row2],
        timestamp: 1000,
    };
    memtable.insert(batch2).unwrap();

    let results = memtable.scan(b"cpu", 0, i64::MAX).unwrap();
    assert_eq!(results.len(), 2);
}
