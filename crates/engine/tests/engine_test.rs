use openGemini_engine::config::{EngineConfig, WalConfig, MemTableConfig, TsspConfig, CompactionConfig, CompressionType};
use openGemini_engine::{Engine, WriteBatch, Row, FieldValue, Query, TimeRange};
use std::collections::HashMap;
use tempfile::TempDir;

fn create_test_engine_config(temp_dir: &TempDir) -> EngineConfig {
    EngineConfig {
        data_dir: temp_dir.path().to_path_buf(),
        wal: WalConfig {
            dir: temp_dir.path().join("wal"),
            file_size: 64 * 1024,
            sync_enabled: false,
        },
        memtable: MemTableConfig {
            max_size: 1024 * 1024,
            flush_interval_ms: 1000,
        },
        tssp: TsspConfig {
            data_dir: temp_dir.path().join("data"),
            max_file_size: 256 * 1024,
            compression: CompressionType::None,
        },
        compaction: CompactionConfig::default(),
    }
}

fn create_test_write_batch(timestamp: i64) -> WriteBatch {
    let mut tags = HashMap::new();
    tags.insert("host".to_string(), "server1".to_string());

    let mut fields = HashMap::new();
    fields.insert("cpu".to_string(), FieldValue::Float(0.5));

    WriteBatch {
        database: "test_db".to_string(),
        table: "cpu".to_string(),
        rows: vec![Row {
            tags,
            fields,
            timestamp,
        }],
        timestamp,
    }
}

#[test]
fn test_engine_new() {
    let temp_dir = TempDir::new().unwrap();
    let config = create_test_engine_config(&temp_dir);

    let engine = Engine::new(config);
    assert!(engine.is_ok());
}

#[test]
fn test_engine_write() {
    let temp_dir = TempDir::new().unwrap();
    let config = create_test_engine_config(&temp_dir);

    let mut engine = Engine::new(config).unwrap();
    let batch = create_test_write_batch(1000);

    let result = engine.write(batch);
    assert!(result.is_ok());
}

#[test]
fn test_engine_write_multiple_batches() {
    let temp_dir = TempDir::new().unwrap();
    let config = create_test_engine_config(&temp_dir);

    let mut engine = Engine::new(config).unwrap();

    for i in 0..5 {
        let batch = create_test_write_batch(1000 + i as i64 * 100);
        assert!(engine.write(batch).is_ok());
    }
}

#[test]
fn test_engine_close() {
    let temp_dir = TempDir::new().unwrap();
    let config = create_test_engine_config(&temp_dir);

    let mut engine = Engine::new(config).unwrap();
    let batch = create_test_write_batch(1000);
    engine.write(batch).unwrap();

    let result = engine.close();
    assert!(result.is_ok());
}

#[test]
fn test_engine_flush() {
    let temp_dir = TempDir::new().unwrap();
    let config = create_test_engine_config(&temp_dir);

    let mut engine = Engine::new(config).unwrap();
    let batch = create_test_write_batch(1000);
    engine.write(batch).unwrap();

    let result = engine.flush();
    assert!(result.is_ok());
}

#[test]
fn test_engine_read() {
    let temp_dir = TempDir::new().unwrap();
    let config = create_test_engine_config(&temp_dir);

    let mut engine = Engine::new(config).unwrap();
    let batch = create_test_write_batch(1000);
    engine.write(batch).unwrap();

    let query = Query {
        database: "test_db".to_string(),
        table: "cpu".to_string(),
        time_range: TimeRange { start: 0, end: 2000 },
        columns: vec!["cpu".to_string()],
        filter: None,
        limit: None,
    };

    let result = engine.read(query);
    assert!(result.is_ok());
    let query_result = result.unwrap();
    assert_eq!(query_result.rows.len(), 1);
}

#[test]
fn test_engine_write_and_flush_cycle() {
    let temp_dir = TempDir::new().unwrap();
    let config = create_test_engine_config(&temp_dir);

    let mut engine = Engine::new(config).unwrap();

    for i in 0..10 {
        let batch = create_test_write_batch(i * 1000);
        engine.write(batch).unwrap();

        if i % 3 == 0 {
            engine.flush().unwrap();
        }
    }

    engine.close().unwrap();
}

#[test]
fn test_engine_flush_and_read_from_tssp() {
    let temp_dir = TempDir::new().unwrap();
    let config = create_test_engine_config(&temp_dir);

    let mut engine = Engine::new(config).unwrap();

    for i in 0..5 {
        let batch = create_test_write_batch(1000 + i as i64 * 100);
        engine.write(batch).unwrap();
    }

    let flush_result = engine.force_flush();
    assert!(flush_result.is_ok());

    let query = Query {
        database: "test_db".to_string(),
        table: "cpu".to_string(),
        time_range: TimeRange { start: 0, end: 2000 },
        columns: vec!["cpu".to_string()],
        filter: None,
        limit: None,
    };

    let result = engine.read(query);
    assert!(result.is_ok());
    let query_result = result.unwrap();
    assert_eq!(query_result.rows.len(), 5);
}

#[test]
fn test_engine_compaction() {
    let temp_dir = TempDir::new().unwrap();
    let config = create_test_engine_config(&temp_dir);

    let mut engine = Engine::new(config).unwrap();

    for i in 0..10 {
        let batch = create_test_write_batch(1000 + i as i64 * 100);
        engine.write(batch).unwrap();
    }

    engine.force_flush().unwrap();
    
    for i in 10..20 {
        let batch = create_test_write_batch(3000 + i as i64 * 100);
        engine.write(batch).unwrap();
    }

    engine.force_flush().unwrap();
    
    for i in 20..30 {
        let batch = create_test_write_batch(5000 + i as i64 * 100);
        engine.write(batch).unwrap();
    }

    engine.force_flush().unwrap();

    let file_count_before = engine.get_tssp_file_count();
    assert_eq!(file_count_before, 3);

    engine.compact().unwrap();

    let file_count_after = engine.get_tssp_file_count();
    assert!(file_count_after < file_count_before);

    let query = Query {
        database: "test_db".to_string(),
        table: "cpu".to_string(),
        time_range: TimeRange { start: 0, end: 8000 },
        columns: vec!["cpu".to_string()],
        filter: None,
        limit: None,
    };

    let result = engine.read(query);
    assert!(result.is_ok());
    let query_result = result.unwrap();
    assert_eq!(query_result.rows.len(), 30);
}

#[test]
fn test_engine_read_with_filter() {
    let temp_dir = TempDir::new().unwrap();
    let config = create_test_engine_config(&temp_dir);

    let mut engine = Engine::new(config).unwrap();

    for i in 0..10 {
        let mut tags = std::collections::HashMap::new();
        tags.insert("host".to_string(), if i < 5 { "server1".to_string() } else { "server2".to_string() });

        let mut fields = std::collections::HashMap::new();
        fields.insert("cpu".to_string(), FieldValue::Float(i as f64 * 10.0));

        let batch = WriteBatch {
            database: "test_db".to_string(),
            table: "cpu".to_string(),
            rows: vec![Row {
                tags,
                fields,
                timestamp: 1000 + i as i64 * 100,
            }],
            timestamp: 1000 + i as i64 * 100,
        };
        engine.write(batch).unwrap();
    }

    engine.force_flush().unwrap();

    use openGemini_engine::FilterExpr;

    let query = Query {
        database: "test_db".to_string(),
        table: "cpu".to_string(),
        time_range: TimeRange { start: 0, end: 2000 },
        columns: vec!["cpu".to_string()],
        filter: Some(FilterExpr::Gt("cpu".to_string(), FieldValue::Float(25.0))),
        limit: None,
    };

    let result = engine.read(query);
    assert!(result.is_ok());
    let query_result = result.unwrap();
    assert_eq!(query_result.rows.len(), 7);
}

#[test]
fn test_engine_read_with_limit() {
    let temp_dir = TempDir::new().unwrap();
    let config = create_test_engine_config(&temp_dir);

    let mut engine = Engine::new(config).unwrap();

    for i in 0..10 {
        let batch = create_test_write_batch(1000 + i as i64 * 100);
        engine.write(batch).unwrap();
    }

    engine.force_flush().unwrap();

    let query = Query {
        database: "test_db".to_string(),
        table: "cpu".to_string(),
        time_range: TimeRange { start: 0, end: 5000 },
        columns: vec!["cpu".to_string()],
        filter: None,
        limit: Some(3),
    };

    let result = engine.read(query);
    assert!(result.is_ok());
    let query_result = result.unwrap();
    assert_eq!(query_result.rows.len(), 3);
}
