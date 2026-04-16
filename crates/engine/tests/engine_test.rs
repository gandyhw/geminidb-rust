use openGemini_engine::config::{EngineConfig, WalConfig, MemTableConfig, TsspConfig, CompactionConfig, CompressionType};
use openGemini_engine::{Engine, WriteBatch, Row, FieldValue, Query, TimeRange};
use std::collections::HashMap;
use std::time::Instant;
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

#[test]
fn test_engine_drop_series_by_id() {
    let temp_dir = TempDir::new().unwrap();
    let config = create_test_engine_config(&temp_dir);

    let mut engine = Engine::new(config).unwrap();

    for series_id in 0..3 {
        let mut tags = std::collections::HashMap::new();
        tags.insert("host".to_string(), format!("server{}", series_id));

        let mut fields = std::collections::HashMap::new();
        fields.insert("cpu".to_string(), FieldValue::Float(50.0 + series_id as f64));

        let batch = WriteBatch {
            database: "test_db".to_string(),
            table: "cpu".to_string(),
            rows: vec![Row {
                tags: tags.clone(),
                fields,
                timestamp: 1000 + series_id as i64 * 100,
            }],
            timestamp: 1000 + series_id as i64 * 100,
        };

        engine.write(batch).unwrap();
    }

    let series_count_before = engine.get_series_count();
    assert_eq!(series_count_before, 3);

    let mut query_tags = std::collections::HashMap::new();
    query_tags.insert("host".to_string(), "server1".to_string());
    let series_id = engine.get_series_id("cpu", &query_tags).unwrap();
    engine.drop_series(Some(series_id)).unwrap();

    let series_count_after = engine.get_series_count();
    assert!(series_count_after < series_count_before);
}

#[test]
fn test_engine_drop_all_series() {
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

    engine.drop_series(None).unwrap();

    let stats = engine.get_stats();
    assert_eq!(stats.series_count, 0);
}

#[test]
fn test_engine_delete_with_tags() {
    let temp_dir = TempDir::new().unwrap();
    let config = create_test_engine_config(&temp_dir);

    let mut engine = Engine::new(config).unwrap();

    let mut tags1 = std::collections::HashMap::new();
    tags1.insert("host".to_string(), "server1".to_string());
    let batch1 = WriteBatch {
        database: "test_db".to_string(),
        table: "cpu".to_string(),
        rows: vec![Row {
            tags: tags1.clone(),
            fields: std::collections::HashMap::new(),
            timestamp: 1000,
        }],
        timestamp: 1000,
    };
    engine.write(batch1).unwrap();

    let mut tags2 = std::collections::HashMap::new();
    tags2.insert("host".to_string(), "server2".to_string());
    let batch2 = WriteBatch {
        database: "test_db".to_string(),
        table: "cpu".to_string(),
        rows: vec![Row {
            tags: tags2.clone(),
            fields: std::collections::HashMap::new(),
            timestamp: 2000,
        }],
        timestamp: 2000,
    };
    engine.write(batch2).unwrap();

    engine.delete("cpu", Some(&tags1)).unwrap();

    let stats = engine.get_stats();
    assert!(stats.memtable_row_count < 2);
}

#[test]
fn test_engine_get_series_id() {
    let temp_dir = TempDir::new().unwrap();
    let config = create_test_engine_config(&temp_dir);

    let mut engine = Engine::new(config).unwrap();

    let mut tags = std::collections::HashMap::new();
    tags.insert("host".to_string(), "server1".to_string());

    let batch = WriteBatch {
        database: "test_db".to_string(),
        table: "cpu".to_string(),
        rows: vec![Row {
            tags,
            fields: std::collections::HashMap::new(),
            timestamp: 1000,
        }],
        timestamp: 1000,
    };
    engine.write(batch).unwrap();

    let mut query_tags = std::collections::HashMap::new();
    query_tags.insert("host".to_string(), "server1".to_string());

    let series_id = engine.get_series_id("cpu", &query_tags);
    assert!(series_id.is_some());
}

#[test]
fn test_engine_is_series_deleted() {
    let temp_dir = TempDir::new().unwrap();
    let config = create_test_engine_config(&temp_dir);

    let mut engine = Engine::new(config).unwrap();

    let batch = create_test_write_batch(1000);
    engine.write(batch).unwrap();

    let mut query_tags = std::collections::HashMap::new();
    query_tags.insert("host".to_string(), "server1".to_string());
    let series_id = engine.get_series_id("cpu", &query_tags).unwrap();
    
    assert!(!engine.is_series_deleted(series_id));
    
    engine.drop_series(Some(series_id)).unwrap();
    
    assert!(engine.is_series_deleted(series_id));
}

#[test]
fn test_engine_batch_write_performance() {
    let temp_dir = TempDir::new().unwrap();
    let config = create_test_engine_config(&temp_dir);

    let mut engine = Engine::new(config).unwrap();

    let start = Instant::now();
    let num_batches = 100;
    let batch_size = 100;

    for batch_idx in 0..num_batches {
        let mut rows = Vec::new();
        for row_idx in 0..batch_size {
            let mut tags = std::collections::HashMap::new();
            tags.insert("host".to_string(), format!("server{}", row_idx % 10));

            let mut fields = std::collections::HashMap::new();
            fields.insert("cpu".to_string(), FieldValue::Float((batch_idx * batch_size + row_idx) as f64));

            rows.push(Row {
                tags,
                fields,
                timestamp: (batch_idx * batch_size + row_idx) as i64,
            });
        }

        let batch = WriteBatch {
            database: "test_db".to_string(),
            table: "cpu".to_string(),
            rows,
            timestamp: (batch_idx * batch_size) as i64,
        };

        engine.write(batch).unwrap();
    }

    let elapsed = start.elapsed();
    let total_rows = num_batches * batch_size;
    let throughput = total_rows as f64 / elapsed.as_secs_f64();

    println!("Batch write throughput: {:.2} rows/sec ({:?} for {} rows)", throughput, elapsed, total_rows);
    assert!(throughput > 0.0);
}

#[test]
fn test_engine_multiple_measurements() {
    let temp_dir = TempDir::new().unwrap();
    let config = create_test_engine_config(&temp_dir);

    let mut engine = Engine::new(config).unwrap();

    for measurement in &["cpu", "memory", "disk"] {
        let batch = WriteBatch {
            database: "test_db".to_string(),
            table: measurement.to_string(),
            rows: vec![Row {
                tags: std::collections::HashMap::new(),
                fields: std::collections::HashMap::new(),
                timestamp: 1000,
            }],
            timestamp: 1000,
        };
        engine.write(batch).unwrap();
    }

    let measurements = engine.measurements();
    assert_eq!(measurements.len(), 3);
    assert!(measurements.contains(&"cpu".to_string()));
    assert!(measurements.contains(&"memory".to_string()));
    assert!(measurements.contains(&"disk".to_string()));
}

#[test]
fn test_engine_query_result_count() {
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
            timestamp: i * 1000,
        });
    }

    engine.write(batch).unwrap();

    let query = Query {
        database: "test_db".to_string(),
        table: "cpu".to_string(),
        time_range: TimeRange { start: 0, end: 10000 },
        columns: vec![],
        filter: None,
        limit: None,
    };

    let result = engine.read(query).unwrap();
    let count = result.count("value");
    assert_eq!(count, 10);

    let count_missing = result.count("missing_field");
    assert_eq!(count_missing, 0);
}

#[test]
fn test_engine_query_result_first_last() {
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
        fields.insert("value".to_string(), FieldValue::Integer(i));

        batch.rows.push(Row {
            tags: std::collections::HashMap::new(),
            fields,
            timestamp: i * 1000,
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
    
    let first = result.first("value");
    assert!(first.is_some());
    if let Some(FieldValue::Integer(v)) = first {
        assert_eq!(v, 0);
    }

    let last = result.last("value");
    assert!(last.is_some());
    if let Some(FieldValue::Integer(v)) = last {
        assert_eq!(v, 4);
    }
}

#[test]
fn test_engine_aggregation_with_integer() {
    let temp_dir = TempDir::new().unwrap();
    let config = create_test_engine_config(&temp_dir);

    let mut engine = Engine::new(config).unwrap();

    let mut batch = WriteBatch {
        database: "test_db".to_string(),
        table: "cpu".to_string(),
        rows: vec![],
        timestamp: 0,
    };

    for i in 1..=10 {
        let mut fields = std::collections::HashMap::new();
        fields.insert("count".to_string(), FieldValue::Integer(i));

        batch.rows.push(Row {
            tags: std::collections::HashMap::new(),
            fields,
            timestamp: i * 1000,
        });
    }

    engine.write(batch).unwrap();

    let query = Query {
        database: "test_db".to_string(),
        table: "cpu".to_string(),
        time_range: TimeRange { start: 0, end: 11000 },
        columns: vec![],
        filter: None,
        limit: None,
    };

    let result = engine.read(query).unwrap();

    let sum = result.sum("count");
    assert!(sum.is_some());
    if let Some(FieldValue::Integer(v)) = sum {
        assert_eq!(v, 55);
    }

    let min = result.min("count");
    if let Some(FieldValue::Integer(v)) = min {
        assert_eq!(v, 1);
    }

    let max = result.max("count");
    if let Some(FieldValue::Integer(v)) = max {
        assert_eq!(v, 10);
    }

    let mean = result.mean("count");
    assert!(mean.is_some());
    if let Some(v) = mean {
        assert_eq!(v, 5.5);
    }
}

#[test]
fn test_engine_aggregation_with_mixed_types() {
    let temp_dir = TempDir::new().unwrap();
    let config = create_test_engine_config(&temp_dir);

    let mut engine = Engine::new(config).unwrap();

    let mut batch = WriteBatch {
        database: "test_db".to_string(),
        table: "cpu".to_string(),
        rows: vec![],
        timestamp: 0,
    };

    batch.rows.push(Row {
        tags: std::collections::HashMap::new(),
        fields: {
            let mut f = std::collections::HashMap::new();
            f.insert("value".to_string(), FieldValue::Integer(10));
            f
        },
        timestamp: 1000,
    });

    batch.rows.push(Row {
        tags: std::collections::HashMap::new(),
        fields: {
            let mut f = std::collections::HashMap::new();
            f.insert("value".to_string(), FieldValue::Float(20.5));
            f
        },
        timestamp: 2000,
    });

    engine.write(batch).unwrap();

    let query = Query {
        database: "test_db".to_string(),
        table: "cpu".to_string(),
        time_range: TimeRange { start: 0, end: 3000 },
        columns: vec![],
        filter: None,
        limit: None,
    };

    let result = engine.read(query).unwrap();
    let sum = result.sum("value");
    assert!(sum.is_some());
}

#[test]
fn test_engine_time_range_exclusive() {
    let temp_dir = TempDir::new().unwrap();
    let config = create_test_engine_config(&temp_dir);

    let mut engine = Engine::new(config).unwrap();

    let batch = WriteBatch {
        database: "test_db".to_string(),
        table: "cpu".to_string(),
        rows: vec![
            Row {
                tags: std::collections::HashMap::new(),
                fields: std::collections::HashMap::new(),
                timestamp: 1000,
            },
            Row {
                tags: std::collections::HashMap::new(),
                fields: std::collections::HashMap::new(),
                timestamp: 2000,
            },
            Row {
                tags: std::collections::HashMap::new(),
                fields: std::collections::HashMap::new(),
                timestamp: 3000,
            },
        ],
        timestamp: 1000,
    };

    engine.write(batch).unwrap();

    let query = Query {
        database: "test_db".to_string(),
        table: "cpu".to_string(),
        time_range: TimeRange { start: 1500, end: 2500 },
        columns: vec![],
        filter: None,
        limit: None,
    };

    let result = engine.read(query).unwrap();
    assert_eq!(result.rows.len(), 1);
}

#[test]
fn test_engine_empty_result() {
    let temp_dir = TempDir::new().unwrap();
    let config = create_test_engine_config(&temp_dir);

    let engine = Engine::new(config).unwrap();

    let query = Query {
        database: "test_db".to_string(),
        table: "cpu".to_string(),
        time_range: TimeRange { start: 0, end: 1000 },
        columns: vec![],
        filter: None,
        limit: None,
    };

    let result = engine.read(query).unwrap();
    assert_eq!(result.rows.len(), 0);
}

#[cfg(test)]
mod stress_tests {
    use super::*;
    use std::time::Instant;

    #[test]
    fn stress_large_batch_write() {
        let temp_dir = TempDir::new().unwrap();
        let config = create_test_engine_config(&temp_dir);

        let mut engine = Engine::new(config).unwrap();

        let mut rows = Vec::new();
        for i in 0..10000 {
            rows.push(Row {
                tags: {
                    let mut t = std::collections::HashMap::new();
                    t.insert("host".to_string(), format!("server{}", i % 100));
                    t
                },
                fields: {
                    let mut f = std::collections::HashMap::new();
                    f.insert("cpu".to_string(), FieldValue::Float(i as f64));
                    f
                },
                timestamp: i,
            });
        }

        let batch = WriteBatch {
            database: "test_db".to_string(),
            table: "cpu".to_string(),
            rows,
            timestamp: 0,
        };

        let start = Instant::now();
        engine.write(batch).unwrap();
        let elapsed = start.elapsed();

        println!("Large batch write (10000 rows): {:?}", elapsed);
        assert!(elapsed.as_secs() < 10);
    }

    #[test]
    fn stress_many_series() {
        let temp_dir = TempDir::new().unwrap();
        let config = create_test_engine_config(&temp_dir);

        let mut engine = Engine::new(config).unwrap();

        let num_series: usize = 1000;
        for i in 0..num_series {
            let mut tags = std::collections::HashMap::new();
            tags.insert("host".to_string(), format!("server{}", i));
            tags.insert("region".to_string(), format!("region{}", i % 10));

            let batch = WriteBatch {
                database: "test_db".to_string(),
                table: "metrics".to_string(),
                rows: vec![Row {
                    tags,
                    fields: {
                        let mut f = std::collections::HashMap::new();
                        f.insert("value".to_string(), FieldValue::Float(i as f64));
                        f
                    },
                    timestamp: i as i64,
                }],
                timestamp: i as i64,
            };

            engine.write(batch).unwrap();
        }

        let series_count = engine.get_series_count();
        assert_eq!(series_count, num_series);

        let tag_keys = engine.get_tag_keys("metrics");
        assert!(tag_keys.len() >= 2);
    }
}
