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

#[test]
fn test_engine_stats() {
    let temp_dir = TempDir::new().unwrap();
    let config = create_test_engine_config(&temp_dir);

    let mut engine = Engine::new(config).unwrap();

    for i in 0..5 {
        let batch = create_test_write_batch(1000 + i as i64 * 100);
        engine.write(batch).unwrap();
    }

    let stats = engine.get_stats();
    assert_eq!(stats.series_count, 1);
    assert_eq!(stats.memtable_row_count, 5);
}

#[test]
fn test_engine_query_result_aggregations() {
    let temp_dir = TempDir::new().unwrap();
    let config = create_test_engine_config(&temp_dir);

    let mut engine = Engine::new(config).unwrap();

    let mut batch = WriteBatch {
        database: "test_db".to_string(),
        table: "cpu".to_string(),
        rows: vec![],
        timestamp: 0,
    };

    for i in 0..5 {
        let mut fields = std::collections::HashMap::new();
        fields.insert("value".to_string(), FieldValue::Float(i as f64 * 10.0));

        batch.rows.push(Row {
            tags: std::collections::HashMap::new(),
            fields,
            timestamp: 1000 + i as i64 * 100,
        });
    }

    engine.write(batch).unwrap();

    let query = Query {
        database: "test_db".to_string(),
        table: "cpu".to_string(),
        time_range: TimeRange { start: 0, end: 5000 },
        columns: vec![],
        filter: None,
        limit: None,
    };

    let result = engine.read(query).unwrap();
    assert_eq!(result.rows.len(), 5);

    let count = result.count("value");
    assert_eq!(count, 5);

    let sum = result.sum("value");
    assert!(sum.is_some());

    let mean = result.mean("value");
    assert!(mean.is_some());

    let min = result.min("value");
    assert!(min.is_some());

    let max = result.max("value");
    assert!(max.is_some());
}

#[test]
fn test_engine_group_by_time() {
    let temp_dir = TempDir::new().unwrap();
    let config = create_test_engine_config(&temp_dir);

    let mut engine = Engine::new(config).unwrap();

    let mut batch = WriteBatch {
        database: "test_db".to_string(),
        table: "cpu".to_string(),
        rows: vec![],
        timestamp: 0,
    };

    for i in 0..10 {
        let mut fields = std::collections::HashMap::new();
        fields.insert("value".to_string(), FieldValue::Float(i as f64));

        batch.rows.push(Row {
            tags: std::collections::HashMap::new(),
            fields,
            timestamp: 1000 + i as i64 * 1000000000,
        });
    }

    engine.write(batch).unwrap();

    let query = Query {
        database: "test_db".to_string(),
        table: "cpu".to_string(),
        time_range: TimeRange { start: 0, end: i64::MAX },
        columns: vec![],
        filter: None,
        limit: None,
    };

    let result = engine.read(query).unwrap();
    let grouped = engine.group_by_time(result.rows, 3_000_000_000);

    assert!(grouped.len() < 10);
}

#[test]
fn test_engine_multiple_series() {
    let temp_dir = TempDir::new().unwrap();
    let config = create_test_engine_config(&temp_dir);

    let mut engine = Engine::new(config).unwrap();

    for series_id in 0..5 {
        let mut tags = std::collections::HashMap::new();
        tags.insert("host".to_string(), format!("server{}", series_id));

        let mut fields = std::collections::HashMap::new();
        fields.insert("cpu".to_string(), FieldValue::Float(50.0 + series_id as f64));

        let batch = WriteBatch {
            database: "test_db".to_string(),
            table: "cpu".to_string(),
            rows: vec![Row {
                tags,
                fields,
                timestamp: 1000 + series_id as i64 * 100,
            }],
            timestamp: 1000 + series_id as i64 * 100,
        };

        engine.write(batch).unwrap();
    }

    let series_count = engine.get_series_count();
    assert_eq!(series_count, 5);
}

#[test]
fn test_engine_delete() {
    let temp_dir = TempDir::new().unwrap();
    let config = create_test_engine_config(&temp_dir);

    let mut engine = Engine::new(config).unwrap();

    let batch = create_test_write_batch(1000);
    engine.write(batch).unwrap();

    let result = engine.delete("cpu", None);
    assert!(result.is_ok());
}

#[test]
fn test_engine_drop_measurement() {
    let temp_dir = TempDir::new().unwrap();
    let config = create_test_engine_config(&temp_dir);

    let mut engine = Engine::new(config).unwrap();

    let batch = create_test_write_batch(1000);
    engine.write(batch).unwrap();

    let measurements_before = engine.measurements();
    assert!(!measurements_before.is_empty());

    let result = engine.drop_measurement("cpu");
    assert!(result.is_ok());

    let measurements_after = engine.measurements();
    assert!(measurements_after.is_empty());
}

#[test]
fn test_engine_create_database() {
    let temp_dir = TempDir::new().unwrap();
    let config = create_test_engine_config(&temp_dir);

    let engine = Engine::new(config).unwrap();

    let result = engine.create_database("testdb");
    assert!(result.is_ok());

    let schema = engine.schema().read().unwrap();
    assert!(schema.databases.contains_key("testdb"));
}

#[test]
fn test_engine_create_retention_policy() {
    let temp_dir = TempDir::new().unwrap();
    let config = create_test_engine_config(&temp_dir);

    let engine = Engine::new(config).unwrap();

    engine.create_database("testdb").unwrap();
    let result = engine.create_retention_policy("testdb", "rp1", 86400, 1);
    assert!(result.is_ok());

    let policies = engine.get_retention_policies("testdb");
    assert_eq!(policies.len(), 1);
    assert_eq!(policies[0].0, "rp1");
}

#[test]
fn test_engine_drop_database() {
    let temp_dir = TempDir::new().unwrap();
    let config = create_test_engine_config(&temp_dir);

    let engine = Engine::new(config).unwrap();

    engine.create_database("testdb").unwrap();
    let result = engine.drop_database("testdb");
    assert!(result.is_ok());

    let schema = engine.schema().read().unwrap();
    assert!(!schema.databases.contains_key("testdb"));
}

#[test]
fn test_engine_tag_keys_and_field_keys() {
    let temp_dir = TempDir::new().unwrap();
    let config = create_test_engine_config(&temp_dir);

    let mut engine = Engine::new(config).unwrap();

    let batch = create_test_write_batch(1000);
    engine.write(batch).unwrap();

    let tag_keys = engine.get_tag_keys("cpu");
    assert!(tag_keys.contains(&"host".to_string()));

    let field_keys = engine.get_field_keys("cpu");
    assert!(field_keys.contains(&"cpu".to_string()));
}

#[test]
fn test_engine_wal_replay() {
    let temp_dir = TempDir::new().unwrap();
    let config = create_test_engine_config(&temp_dir);

    {
        let mut engine = Engine::new(config.clone()).unwrap();
        for i in 0..5 {
            let batch = create_test_write_batch(1000 + i as i64 * 100);
            engine.write(batch).unwrap();
        }
        engine.close().unwrap();
    }

    let engine2 = Engine::new(config).unwrap();
    let stats = engine2.get_stats();
    assert_eq!(stats.memtable_row_count, 5);
}

#[test]
fn test_engine_read_time_range() {
    let temp_dir = TempDir::new().unwrap();
    let config = create_test_engine_config(&temp_dir);

    let mut engine = Engine::new(config).unwrap();

    for i in 0..10 {
        let batch = create_test_write_batch(1000 + i as i64 * 1000);
        engine.write(batch).unwrap();
    }

    let query = Query {
        database: "test_db".to_string(),
        table: "cpu".to_string(),
        time_range: TimeRange { start: 3000, end: 7000 },
        columns: vec![],
        filter: None,
        limit: None,
    };

    let result = engine.read(query).unwrap();
    assert_eq!(result.rows.len(), 4);
}

#[cfg(test)]
mod benchmarks {
    use super::*;
    use std::time::Instant;

    #[test]
    fn bench_engine_write_throughput() {
        let temp_dir = TempDir::new().unwrap();
        let config = create_test_engine_config(&temp_dir);

        let mut engine = Engine::new(config).unwrap();

        let start = Instant::now();
        let num_batches = 1000;

        for i in 0..num_batches {
            let batch = create_test_write_batch(1000 + i as i64);
            engine.write(batch).unwrap();
        }

        let elapsed = start.elapsed();
        let throughput = num_batches as f64 / elapsed.as_secs_f64();

        println!("Write throughput: {:.2} batches/sec", throughput);
        assert!(throughput > 0.0);
    }

    #[test]
    fn bench_engine_read_throughput() {
        let temp_dir = TempDir::new().unwrap();
        let config = create_test_engine_config(&temp_dir);

        let mut engine = Engine::new(config).unwrap();

        for i in 0..1000 {
            let batch = create_test_write_batch(1000 + i as i64);
            engine.write(batch).unwrap();
        }

        engine.force_flush().unwrap();

        let query = Query {
            database: "test_db".to_string(),
            table: "cpu".to_string(),
            time_range: TimeRange { start: 0, end: i64::MAX },
            columns: vec![],
            filter: None,
            limit: None,
        };

        let start = Instant::now();
        let num_queries = 100;

        for _ in 0..num_queries {
            let _ = engine.read(query.clone());
        }

        let elapsed = start.elapsed();
        let throughput = num_queries as f64 / elapsed.as_secs_f64();

        println!("Read throughput: {:.2} queries/sec", throughput);
        assert!(throughput > 0.0);
    }

    #[test]
    fn bench_query_result_aggregations() {
        let temp_dir = TempDir::new().unwrap();
        let config = create_test_engine_config(&temp_dir);

        let mut engine = Engine::new(config).unwrap();

        let mut batch = WriteBatch {
            database: "test_db".to_string(),
            table: "cpu".to_string(),
            rows: vec![],
            timestamp: 0,
        };

        for i in 0..1000 {
            let mut fields = std::collections::HashMap::new();
            fields.insert("value".to_string(), FieldValue::Float(i as f64));

            batch.rows.push(Row {
                tags: std::collections::HashMap::new(),
                fields,
                timestamp: 1000 + i as i64 * 1000,
            });
        }

        engine.write(batch).unwrap();

        let query = Query {
            database: "test_db".to_string(),
            table: "cpu".to_string(),
            time_range: TimeRange { start: 0, end: i64::MAX },
            columns: vec![],
            filter: None,
            limit: None,
        };

        let result = engine.read(query).unwrap();

        let start = Instant::now();
        for _ in 0..100 {
            let _ = result.sum("value");
            let _ = result.mean("value");
            let _ = result.min("value");
            let _ = result.max("value");
        }
        let elapsed = start.elapsed();

        println!("Aggregation (1000 rows x 100 iterations): {:?}", elapsed);
        assert!(elapsed.as_millis() < 1000);
    }
}
