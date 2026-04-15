use openGemini_engine::config::WalConfig;
use openGemini_engine::wal::Wal;
use openGemini_engine::{FieldValue, Row, WriteBatch};
use std::collections::HashMap;

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
        rows: vec![Row { tags, fields, timestamp }],
        timestamp,
    }
}

#[cfg(test)]
mod benchmarks {
    use super::*;
    use std::time::Instant;
    use tempfile::TempDir;

    #[test]
    pub fn bench_wal_write_100() {
        let temp_dir = TempDir::new().unwrap();
        let config = WalConfig {
            dir: temp_dir.path().to_path_buf(),
            file_size: 64 * 1024 * 1024,
            sync_enabled: false,
        };
        let wal = Wal::new(&config).unwrap();

        let start = Instant::now();
        for i in 0..100 {
            let batch = create_test_batch("cpu", i);
            wal.write(&batch).unwrap();
        }
        let elapsed = start.elapsed();
        println!("wal_write_100: {:?}", elapsed);
    }

    #[test]
    pub fn bench_wal_write_1000() {
        let temp_dir = TempDir::new().unwrap();
        let config = WalConfig {
            dir: temp_dir.path().to_path_buf(),
            file_size: 64 * 1024 * 1024,
            sync_enabled: false,
        };
        let wal = Wal::new(&config).unwrap();

        let start = Instant::now();
        for i in 0..1000 {
            let batch = create_test_batch("cpu", i as i64);
            wal.write(&batch).unwrap();
        }
        let elapsed = start.elapsed();
        println!("wal_write_1000: {:?}", elapsed);
    }

    #[test]
    pub fn bench_wal_read_1000() {
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

        let start = Instant::now();
        for i in 0..1000 {
            let _ = wal.read(1, i as u64 * 200);
        }
        let elapsed = start.elapsed();
        println!("wal_read_1000: {:?}", elapsed);
    }
}
