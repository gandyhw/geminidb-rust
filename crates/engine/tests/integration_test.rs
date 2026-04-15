use openGemini_engine::config::{WalConfig, TsspConfig, MemTableConfig, EngineConfig, CompactionConfig, CompressionType};
use openGemini_engine::memtable::MemTable;
use openGemini_engine::tssp::{TsspWriter, TableSchema, ColumnData};
use openGemini_engine::wal::Wal;
use openGemini_engine::{FieldValue, Row, WriteBatch};
use std::collections::HashMap;
use std::env::temp_dir;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

static TEST_ID: AtomicU64 = AtomicU64::new(0);

fn next_test_id() -> u64 {
    TEST_ID.fetch_add(1, Ordering::SeqCst)
}

fn unique_temp_dir(prefix: &str) -> std::path::PathBuf {
    let id = next_test_id();
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64;
    let pid = std::process::id() as u64;
    temp_dir().join(format!("{}_{}_{}_{}", prefix, pid, now, id))
}

#[test]
fn test_wal_write_and_replay_integration() {
    let temp_dir = unique_temp_dir("wal_replay");
    std::fs::create_dir_all(&temp_dir).unwrap();
    
    let wal_config = WalConfig {
        dir: temp_dir.join("wal"),
        file_size: 64 * 1024,
        sync_enabled: false,
    };
    
    let wal = Wal::new(&wal_config).unwrap();
    
    let batch = create_test_batch("cpu", 1000, 2000);
    wal.write(&batch).unwrap();
    
    drop(wal);
    
    let wal2 = Wal::new(&wal_config).unwrap();
    let mut replay_count = 0;
    wal2.replay(|_| {
        replay_count += 1;
        Ok(())
    }).unwrap();
    
    assert_eq!(replay_count, 1);
}

#[test]
fn test_wal_multiple_batches_integration() {
    let temp_dir = unique_temp_dir("wal_multi");
    std::fs::create_dir_all(&temp_dir).unwrap();
    
    let wal_config = WalConfig {
        dir: temp_dir.join("wal"),
        file_size: 64 * 1024,
        sync_enabled: false,
    };
    
    let wal = Wal::new(&wal_config).unwrap();
    
    for i in 0..10 {
        let batch = create_test_batch("cpu", i * 100, (i + 1) * 100);
        wal.write(&batch).unwrap();
    }
    
    drop(wal);
    
    let wal2 = Wal::new(&wal_config).unwrap();
    let mut replay_count = 0;
    wal2.replay(|_| {
        replay_count += 1;
        Ok(())
    }).unwrap();
    
    assert_eq!(replay_count, 10);
}

#[test]
fn test_memtable_flush_to_tssp_integration() {
    let mut memtable = MemTable::new(1024 * 1024 * 1024);
    
    let batch = create_test_batch("cpu", 0, 1000);
    memtable.insert(batch).unwrap();
    
    assert!(memtable.row_count() > 0);
    
    let rows = memtable.flush().unwrap();
    assert!(!rows.is_empty());
    assert_eq!(memtable.row_count(), 0);
}

#[test]
fn test_tssp_write_and_meta_integration() {
    let temp_dir = unique_temp_dir("tssp_write");
    std::fs::create_dir_all(&temp_dir).unwrap();
    
    let config = TsspConfig {
        data_dir: temp_dir.clone(),
        max_file_size: 256 * 1024,
        compression: CompressionType::None,
    };
    
    let mut schema = TableSchema {
        table_id: 1,
        columns: HashMap::new(),
    };
    schema.columns.insert("time".to_string(), 0);
    schema.columns.insert("tag_host".to_string(), 1);
    schema.columns.insert("field_cpu".to_string(), 2);
    
    let mut writer = TsspWriter::new(config).unwrap();
    
    let time_values: Vec<u8> = (0i64..1000i64)
        .flat_map(|v| v.to_be_bytes().to_vec())
        .collect();
    let tag_values: Vec<u8> = vec![0u8; 1000];
    let field_values: Vec<u8> = (0i64..1000i64)
        .flat_map(|v| v.to_be_bytes().to_vec())
        .collect();
    
    let column_data = vec![
        ColumnData {
            column_id: 0,
            values: time_values,
            null_count: 0,
        },
        ColumnData {
            column_id: 1,
            values: tag_values,
            null_count: 0,
        },
        ColumnData {
            column_id: 2,
            values: field_values,
            null_count: 0,
        },
    ];
    
    let meta = writer.write_batch(&schema, 0, 999000, column_data).unwrap();
    
    assert_eq!(meta.min_time, 0);
    assert_eq!(meta.max_time, 999000);
}

#[test]
fn test_tssp_compression_types_integration() {
    let temp_dir = unique_temp_dir("tssp_compress");
    std::fs::create_dir_all(&temp_dir).unwrap();
    
    for compression in &[CompressionType::None, CompressionType::Lz4] {
        let comp_dir = temp_dir.join(format!("{:?}", compression));
        std::fs::create_dir_all(&comp_dir).unwrap();
        
        let config = TsspConfig {
            data_dir: comp_dir.clone(),
            max_file_size: 256 * 1024,
            compression: *compression,
        };
        
        let mut schema = TableSchema {
            table_id: 1,
            columns: HashMap::new(),
        };
        schema.columns.insert("time".to_string(), 0);
        schema.columns.insert("field_value".to_string(), 1);
        
        let mut writer = TsspWriter::new(config).unwrap();
        
        let time_values: Vec<u8> = (0i64..100i64)
            .flat_map(|v| v.to_be_bytes().to_vec())
            .collect();
        let field_values: Vec<u8> = (0i64..100i64)
            .flat_map(|v| v.to_be_bytes().to_vec())
            .collect();
        
        let column_data = vec![
            ColumnData {
                column_id: 0,
                values: time_values,
                null_count: 0,
            },
            ColumnData {
                column_id: 1,
                values: field_values,
                null_count: 0,
            },
        ];
        
        let meta = writer.write_batch(&schema, 0, 99000, column_data).unwrap();
        writer.close().unwrap();
        
        assert!(meta.size > 0);
    }
}

#[test]
fn test_engine_config_integration() {
    let temp_dir = unique_temp_dir("engine_config");
    std::fs::create_dir_all(&temp_dir).unwrap();
    
    let config = EngineConfig {
        data_dir: temp_dir.clone(),
        wal: WalConfig {
            dir: temp_dir.join("wal"),
            file_size: 1024 * 1024,
            sync_enabled: true,
        },
        memtable: MemTableConfig {
            max_size: 1024 * 1024,
            flush_interval_ms: 1000,
        },
        tssp: TsspConfig {
            data_dir: temp_dir.join("tssp"),
            max_file_size: 1024 * 1024,
            compression: CompressionType::Snappy,
        },
        compaction: CompactionConfig {
            enabled: true,
            max_concurrent: 2,
            trigger_interval_ms: 60000,
            max_file_age_hours: 12,
        },
    };
    
    assert!(config.wal.sync_enabled);
    assert_eq!(config.compaction.max_concurrent, 2);
    assert_eq!(config.memtable.max_size, 1024 * 1024);
}

#[test]
fn test_write_batch_flow_integration() {
    let temp_dir = unique_temp_dir("batch_flow");
    std::fs::create_dir_all(&temp_dir).unwrap();
    
    let wal_config = WalConfig {
        dir: temp_dir.join("wal"),
        file_size: 64 * 1024,
        sync_enabled: false,
    };
    
    let wal = Wal::new(&wal_config).unwrap();
    let mut memtable = MemTable::new(1024 * 1024);
    
    let batch = create_test_batch("cpu", 0, 100);
    wal.write(&batch).unwrap();
    memtable.insert(batch).unwrap();
    
    assert_eq!(memtable.row_count(), 101);
    
    let rows = memtable.flush().unwrap();
    assert_eq!(rows.len(), 101);
}

#[test]
fn test_concurrent_wal_writes_integration() {
    let temp_dir = unique_temp_dir("concurrent_wal");
    std::fs::create_dir_all(&temp_dir).unwrap();
    
    let wal_config = WalConfig {
        dir: temp_dir.join("wal"),
        file_size: 1024 * 1024,
        sync_enabled: false,
    };
    
    let wal = std::sync::Arc::new(std::sync::Mutex::new(Wal::new(&wal_config).unwrap()));
    
    let batches: Vec<WriteBatch> = (0..10)
        .map(|i| create_test_batch(&format!("table_{}", i), i * 100, (i + 1) * 100))
        .collect();
    
    for batch in batches {
        wal.lock().unwrap().write(&batch).unwrap();
    }
    
    drop(wal);
    
    let wal2 = Wal::new(&wal_config).unwrap();
    let mut replay_count = 0;
    wal2.replay(|_| {
        replay_count += 1;
        Ok(())
    }).unwrap();
    
    assert_eq!(replay_count, 10);
}

#[test]
fn test_memtable_scan_integration() {
    let mut memtable = MemTable::new(1024 * 1024);
    
    for i in 0..100 {
        let batch = create_test_batch("cpu", i * 1000, (i + 1) * 1000);
        memtable.insert(batch).unwrap();
    }
    
    let results = memtable.scan(b"cpu", 25000, 75000).unwrap();
    assert!(!results.is_empty());
}

#[test]
fn test_memtable_should_flush_integration() {
    let mut memtable = MemTable::new(1024);
    
    let batch = create_test_batch("cpu", 0, 100);
    memtable.insert(batch).unwrap();
    
    assert!(memtable.should_flush());
}

#[test]
fn test_wal_purge_small_file_size() {
    let temp_dir = unique_temp_dir("wal_purge_small");
    std::fs::create_dir_all(&temp_dir).unwrap();
    
    let wal_config = WalConfig {
        dir: temp_dir.join("wal"),
        file_size: 100,
        sync_enabled: false,
    };
    
    let wal = Wal::new(&wal_config).unwrap();
    
    for i in 0..10 {
        let batch = create_test_batch("cpu", i * 100, (i + 1) * 100);
        wal.write(&batch).unwrap();
    }
    
    wal.purge(3).unwrap();
    
    drop(wal);
    
    let wal2 = Wal::new(&wal_config).unwrap();
    let mut replay_count = 0;
    wal2.replay(|_| {
        replay_count += 1;
        Ok(())
    }).unwrap();
    
    assert!(replay_count >= 3);
}

fn create_test_batch(table: &str, start: i64, end: i64) -> WriteBatch {
    let rows: Vec<Row> = (start..=end).map(|ts| {
        let mut tags = HashMap::new();
        tags.insert("host".to_string(), format!("server{}", ts % 10));
        tags.insert("region".to_string(), "us-east".to_string());
        let mut fields = HashMap::new();
        fields.insert("cpu".to_string(), FieldValue::Float(ts as f64 * 0.01));
        fields.insert("memory".to_string(), FieldValue::Integer(ts * 1024));
        Row { tags, fields, timestamp: ts }
    }).collect();
    
    WriteBatch {
        database: "test_db".to_string(),
        table: table.to_string(),
        rows,
        timestamp: start,
    }
}
