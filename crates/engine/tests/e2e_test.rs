use openGemini_engine::config::{WalConfig, TsspConfig, MemTableConfig, EngineConfig, CompactionConfig, CompressionType};
use openGemini_engine::memtable::MemTable;
use openGemini_engine::schema::{Schema, RetentionPolicy};
use openGemini_engine::shard::{ShardManager, ShardMapper};
use openGemini_engine::tiered_storage::{StorageTier, TieredStorageManager, TierConfig};
use openGemini_engine::downsample::{DownsampleInterval, DownsampleEngine};
use openGemini_engine::metaclient::{MetaClient, MetaClientStub, NodeInfo};
use openGemini_engine::{FieldValue, Row, WriteBatch};
use std::collections::HashMap;
use std::env::temp_dir;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};

static TEST_ID: AtomicU64 = AtomicU64::new(0);

fn next_test_id() -> u64 {
    TEST_ID.fetch_add(1, Ordering::SeqCst)
}

fn unique_temp_dir(prefix: &str) -> std::path::PathBuf {
    let id = next_test_id();
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64;
    let pid = std::process::id() as u64;
    temp_dir().join(format!("{}_{}_{}_{}", prefix, pid, now, id))
}

#[test]
fn test_end_to_end_write_and_query() {
    let temp_dir = unique_temp_dir("e2e_write_query");
    std::fs::create_dir_all(&temp_dir).unwrap();
    
    let mut schema = Schema::new();
    schema.create_database("testdb".to_string()).unwrap();
    schema.create_retention_policy("testdb", RetentionPolicy::new("rp1".to_string(), 86400)).unwrap();
    
    let config = EngineConfig {
        data_dir: temp_dir.clone(),
        wal: WalConfig {
            dir: temp_dir.join("wal"),
            file_size: 64 * 1024,
            sync_enabled: false,
        },
        memtable: MemTableConfig {
            max_size: 1024 * 1024,
            flush_interval_ms: 1000,
        },
        tssp: TsspConfig {
            data_dir: temp_dir.join("tssp"),
            max_file_size: 256 * 1024,
            compression: CompressionType::None,
        },
        compaction: CompactionConfig::default(),
    };
    
    let mut memtable = MemTable::new(config.memtable.max_size);
    
    let batch = create_test_batch("cpu", 0, 1000);
    memtable.insert(batch).unwrap();
    
    assert_eq!(memtable.row_count(), 1001);
    
    let rows = memtable.flush().unwrap();
    assert_eq!(rows.len(), 1001);
    assert_eq!(memtable.row_count(), 0);
}

#[test]
fn test_schema_with_retention_policy() {
    let mut schema = Schema::new();
    schema.create_database("testdb".to_string()).unwrap();
    
    let rp = RetentionPolicy::new("rp1".to_string(), 86400 * 7);
    schema.create_retention_policy("testdb", rp).unwrap();
    
    let db = schema.get_database("testdb").unwrap();
    let rp = db.get_default_rp().unwrap();
    assert_eq!(rp.name, "rp1");
    assert_eq!(rp.duration_seconds, 86400 * 7);
}

#[test]
fn test_shard_mapper_with_custom_duration() {
    let mapper = ShardMapper::new(3600, 3);
    
    assert_eq!(mapper.map_shard(0), 0);
    assert_eq!(mapper.map_shard(3600000), 1000);
    
    assert_eq!(mapper.get_replica_count(), 3);
}

#[test]
fn test_shard_manager_with_multiple_dbs() {
    let temp_dir = unique_temp_dir("shard_multi_db");
    std::fs::create_dir_all(&temp_dir).unwrap();
    
    let manager = ShardManager::new(temp_dir.clone());
    
    manager.create_shard(1, "db1", "rp1").unwrap();
    manager.create_shard(2, "db2", "rp1").unwrap();
    manager.create_shard(3, "db1", "rp2").unwrap();
    
    let db1_shards = manager.get_shards_by_db("db1");
    assert_eq!(db1_shards.len(), 2);
    
    let db2_shards = manager.get_shards_by_db("db2");
    assert_eq!(db2_shards.len(), 1);
}

#[test]
fn test_tiered_storage_with_custom_config() {
    let temp_dir = unique_temp_dir("tier_custom");
    std::fs::create_dir_all(&temp_dir).unwrap();
    
    let config = TierConfig::new(7200, 86400)
        .with_cold_enabled(true)
        .with_cold_path(temp_dir.join("cold"));
    
    let manager = TieredStorageManager::new(temp_dir.clone(), config);
    
    let now = chrono::Utc::now().timestamp();
    
    assert_eq!(manager.determine_tier(now), StorageTier::Hot);
    assert_eq!(manager.determine_tier(now - 10000), StorageTier::Warm);
    assert_eq!(manager.determine_tier(now - 200000), StorageTier::Cold);
    
    assert!(manager.is_cold_enabled());
}

#[test]
fn test_downsample_with_aggregators() {
    let mut engine = DownsampleEngine::new();
    
    let rule = openGemini_engine::downsample::DownsampleRule::new(
        "cpu_1m_to_5m".to_string(),
        DownsampleInterval::Min1,
        DownsampleInterval::Min5,
    )
    .with_aggregators(vec![
        openGemini_engine::downsample::AggregatorType::Mean,
        openGemini_engine::downsample::AggregatorType::Max,
    ]);
    
    engine.add_rule(rule);
    
    let values = vec![10i64, 20, 30, 40, 50];
    let result = engine.aggregate("cpu_1m_to_5m", &values).unwrap();
    
    assert_eq!(result[&openGemini_engine::downsample::AggregatorType::Mean], 30);
    assert_eq!(result[&openGemini_engine::downsample::AggregatorType::Max], 50);
}

#[test]
fn test_metaclient_peer_management() {
    let addr1: SocketAddr = "127.0.0.1:8080".parse().unwrap();
    let addr2: SocketAddr = "127.0.0.1:8081".parse().unwrap();
    
    let peers = vec![
        NodeInfo::new(1, addr1).with_leader(true),
        NodeInfo::new(2, addr2),
    ];
    
    let mut stub = MetaClientStub::new(1, peers);
    
    assert!(stub.is_leader());
    assert!(stub.leader().is_some());
    assert_eq!(stub.peers().len(), 2);
}

#[test]
fn test_metaclient_shard_ownership() {
    let peers = vec![NodeInfo::new(1, "127.0.0.1:8080".parse().unwrap())];
    let mut stub = MetaClientStub::new(1, peers);
    
    let mapping = openGemini_engine::metaclient::ShardMapping::new(
        1, 100, "testdb", "rp1", 0, 3600
    );
    stub.update_shard_readers(&mapping).unwrap();
    
    let owner = stub.get_shard_owner(1).unwrap();
    assert!(owner.is_some());
    assert_eq!(owner.unwrap().node_id, 100);
}

#[test]
fn test_full_query_flow_with_schema() {
    let mut schema = Schema::new();
    schema.create_database("testdb".to_string()).unwrap();
    schema.create_retention_policy("testdb", RetentionPolicy::new("rp1".to_string(), 86400)).unwrap();
    
    let executor = openGemini_engine::query::QueryExecutor::new(schema);
    
    let request = openGemini_engine::query::QueryRequest::new(
        "testdb".to_string(),
        "cpu".to_string(),
        openGemini_engine::TimeRange { start: 0, end: 1000 },
    )
    .with_limit(100);
    
    let rows = vec![
        Row {
            tags: HashMap::new(),
            fields: {
                let mut h = HashMap::new();
                h.insert("value".to_string(), FieldValue::Integer(100));
                h
            },
            timestamp: 500,
        },
    ];
    
    let results = executor.execute_select(&request, rows).unwrap();
    assert_eq!(results.len(), 1);
}

#[test]
fn test_compaction_config_custom() {
    let config = CompactionConfig {
        enabled: true,
        max_concurrent: 8,
        trigger_interval_ms: 60000,
        max_file_age_hours: 48,
    };
    
    assert!(config.enabled);
    assert_eq!(config.max_concurrent, 8);
    assert_eq!(config.max_file_age_hours, 48);
}

#[test]
fn test_wal_config_custom() {
    let temp_dir = unique_temp_dir("wal_config_test");
    std::fs::create_dir_all(&temp_dir).unwrap();
    
    let config = WalConfig {
        dir: temp_dir.join("wal"),
        file_size: 128 * 1024 * 1024,
        sync_enabled: true,
    };
    
    assert_eq!(config.file_size, 128 * 1024 * 1024);
    assert!(config.sync_enabled);
}

#[test]
fn test_memtable_size_tracking() {
    let mut memtable = MemTable::new(1024);
    
    let batch = create_test_batch("cpu", 0, 100);
    memtable.insert(batch).unwrap();
    
    assert!(memtable.size() > 0);
    assert!(memtable.should_flush());
}

fn create_test_batch(table: &str, start: i64, end: i64) -> WriteBatch {
    let rows: Vec<Row> = (start..=end).map(|ts| {
        let mut tags = HashMap::new();
        tags.insert("host".to_string(), format!("server{}", ts % 10));
        let mut fields = HashMap::new();
        fields.insert("cpu".to_string(), FieldValue::Float(ts as f64 * 0.01));
        Row { tags, fields, timestamp: ts }
    }).collect();
    
    WriteBatch {
        database: "test_db".to_string(),
        table: table.to_string(),
        rows,
        timestamp: start,
    }
}
