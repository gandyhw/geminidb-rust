use openGemini_engine::config::WalConfig;
use openGemini_engine::wal::Wal;
use openGemini_engine::{FieldValue, Row, WriteBatch};
use std::collections::HashMap;
use tempfile::TempDir;

fn create_test_batch() -> WriteBatch {
    let mut tags = HashMap::new();
    tags.insert("host".to_string(), "server1".to_string());
    tags.insert("region".to_string(), "us-east".to_string());

    let mut fields = HashMap::new();
    fields.insert("cpu".to_string(), FieldValue::Float(0.5));
    fields.insert("memory".to_string(), FieldValue::Integer(1024));

    WriteBatch {
        database: "test_db".to_string(),
        table: "cpu_metrics".to_string(),
        rows: vec![Row {
            tags,
            fields,
            timestamp: 1000000,
        }],
        timestamp: 1000000,
    }
}

#[test]
fn test_wal_create() {
    let temp_dir = TempDir::new().unwrap();
    let config = WalConfig {
        dir: temp_dir.path().to_path_buf(),
        file_size: 1024 * 1024,
        sync_enabled: false,
    };

    let wal = Wal::new(&config);
    assert!(wal.is_ok());
}

#[test]
fn test_wal_write_single_entry() {
    let temp_dir = TempDir::new().unwrap();
    let config = WalConfig {
        dir: temp_dir.path().to_path_buf(),
        file_size: 1024 * 1024,
        sync_enabled: false,
    };

    let wal = Wal::new(&config).unwrap();
    let batch = create_test_batch();

    let entry = wal.write(&batch);
    assert!(entry.is_ok());
    assert_eq!(entry.unwrap().offset, 0);
}

#[test]
fn test_wal_write_multiple_entries() {
    let temp_dir = TempDir::new().unwrap();
    let config = WalConfig {
        dir: temp_dir.path().to_path_buf(),
        file_size: 1024 * 1024,
        sync_enabled: false,
    };

    let wal = Wal::new(&config).unwrap();
    let batch = create_test_batch();

    let entry1 = wal.write(&batch);
    let entry2 = wal.write(&batch);

    assert!(entry1.is_ok());
    assert!(entry2.is_ok());
}

#[test]
fn test_wal_read_entry() {
    let temp_dir = TempDir::new().unwrap();
    let config = WalConfig {
        dir: temp_dir.path().to_path_buf(),
        file_size: 1024 * 1024,
        sync_enabled: false,
    };

    let wal = Wal::new(&config).unwrap();
    let batch = create_test_batch();

    let entry = wal.write(&batch).unwrap();
    let read_batch = wal.read(entry.file_id, entry.offset);

    assert!(read_batch.is_ok());
    let recovered = read_batch.unwrap();
    assert_eq!(recovered.database, "test_db");
    assert_eq!(recovered.table, "cpu_metrics");
    assert_eq!(recovered.rows.len(), 1);
}

#[test]
fn test_wal_close() {
    let temp_dir = TempDir::new().unwrap();
    let config = WalConfig {
        dir: temp_dir.path().to_path_buf(),
        file_size: 1024 * 1024,
        sync_enabled: false,
    };

    let wal = Wal::new(&config).unwrap();
    let batch = create_test_batch();
    wal.write(&batch).unwrap();

    let result = wal.close();
    assert!(result.is_ok());
}

#[test]
fn test_wal_purge() {
    let temp_dir = TempDir::new().unwrap();
    let config = WalConfig {
        dir: temp_dir.path().to_path_buf(),
        file_size: 1024 * 1024,
        sync_enabled: false,
    };

    let wal = Wal::new(&config).unwrap();
    let batch = create_test_batch();
    wal.write(&batch).unwrap();

    let result = wal.purge(1);
    assert!(result.is_ok());
}

#[test]
fn test_wal_replay() {
    let temp_dir = TempDir::new().unwrap();
    let config = WalConfig {
        dir: temp_dir.path().to_path_buf(),
        file_size: 1024 * 1024,
        sync_enabled: false,
    };

    let wal = Wal::new(&config).unwrap();
    
    for i in 0..5 {
        let batch = create_test_batch_with_id(i);
        wal.write(&batch).unwrap();
    }

    let mut replayed_batches = Vec::new();
    
    wal.replay(|batch| {
        replayed_batches.push(batch);
        Ok(())
    }).unwrap();
    
    assert_eq!(replayed_batches.len(), 5);
}

fn create_test_batch_with_id(id: i64) -> WriteBatch {
    let mut tags = HashMap::new();
    tags.insert("host".to_string(), format!("server{}", id));

    let mut fields = HashMap::new();
    fields.insert("cpu".to_string(), FieldValue::Float(id as f64));

    WriteBatch {
        database: "test_db".to_string(),
        table: "cpu_metrics".to_string(),
        rows: vec![Row {
            tags,
            fields,
            timestamp: id * 1000,
        }],
        timestamp: id * 1000,
    }
}

#[test]
fn test_wal_entry() {
    let entry = openGemini_engine::wal::WalEntry {
        file_id: 1,
        offset: 100,
        size: 256,
    };

    assert_eq!(entry.file_id, 1);
    assert_eq!(entry.offset, 100);
    assert_eq!(entry.size, 256);
}

#[test]
fn test_wal_write_with_sync() {
    let temp_dir = TempDir::new().unwrap();
    let config = WalConfig {
        dir: temp_dir.path().to_path_buf(),
        file_size: 1024 * 1024,
        sync_enabled: true,
    };

    let wal = Wal::new(&config).unwrap();
    let batch = create_test_batch();

    let entry = wal.write(&batch);
    assert!(entry.is_ok());
}

#[test]
fn test_wal_file_rotation() {
    let temp_dir = TempDir::new().unwrap();
    let config = WalConfig {
        dir: temp_dir.path().to_path_buf(),
        file_size: 100,
        sync_enabled: false,
    };

    let wal = Wal::new(&config).unwrap();

    for i in 0..10 {
        let batch = create_test_batch_with_id(i);
        let entry = wal.write(&batch);
        assert!(entry.is_ok());
    }

    let result = wal.close();
    assert!(result.is_ok());
}

#[test]
fn test_wal_read_nonexistent() {
    let temp_dir = TempDir::new().unwrap();
    let config = WalConfig {
        dir: temp_dir.path().to_path_buf(),
        file_size: 1024 * 1024,
        sync_enabled: false,
    };

    let wal = Wal::new(&config).unwrap();

    let result = wal.read(999, 0);
    assert!(result.is_err());
}

#[test]
fn test_wal_replay_empty() {
    let temp_dir = TempDir::new().unwrap();
    let config = WalConfig {
        dir: temp_dir.path().to_path_buf(),
        file_size: 1024 * 1024,
        sync_enabled: false,
    };

    let wal = Wal::new(&config).unwrap();

    let mut replayed_batches = Vec::new();

    wal.replay(|batch| {
        replayed_batches.push(batch);
        Ok(())
    }).unwrap();

    assert_eq!(replayed_batches.len(), 0);
}

#[test]
fn test_wal_purge_with_files() {
    let temp_dir = TempDir::new().unwrap();
    let config = WalConfig {
        dir: temp_dir.path().to_path_buf(),
        file_size: 1024,
        sync_enabled: false,
    };

    let wal = Wal::new(&config).unwrap();

    for i in 0..5 {
        let batch = create_test_batch_with_id(i);
        wal.write(&batch).unwrap();
    }

    let result = wal.purge(0);
    assert!(result.is_ok());
}

#[test]
fn test_wal_close_idempotent() {
    let temp_dir = TempDir::new().unwrap();
    let config = WalConfig {
        dir: temp_dir.path().to_path_buf(),
        file_size: 1024 * 1024,
        sync_enabled: false,
    };

    let wal = Wal::new(&config).unwrap();
    let batch = create_test_batch();
    wal.write(&batch).unwrap();

    let result1 = wal.close();
    let result2 = wal.close();

    assert!(result1.is_ok());
    assert!(result2.is_ok());
}
