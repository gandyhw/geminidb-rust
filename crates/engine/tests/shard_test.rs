use openGemini_engine::shard::{ShardInfo, ShardManager, ShardMapper, ShardStatus};
use std::path::PathBuf;
use tempfile::tempdir;

fn create_temp_dir() -> PathBuf {
    tempdir().unwrap().into_path()
}

#[test]
fn test_shard_info_new() {
    let path = PathBuf::from("/tmp/test_shard");
    let info = ShardInfo::new(1, "testdb", "rp1", path.clone());

    assert_eq!(info.id, 1);
    assert_eq!(info.database, "testdb");
    assert_eq!(info.retention_policy, "rp1");
    assert_eq!(info.path, path);
    assert_eq!(info.status, ShardStatus::Active);
    assert_eq!(info.replica_count, 1);
    assert_eq!(info.shard_duration_seconds, 3600 * 24 * 7);
    assert_eq!(info.tier, 0);
    assert_eq!(info.size_bytes, 0);
    assert_eq!(info.row_count, 0);
}

#[test]
fn test_shard_info_with_replica_count() {
    let info = ShardInfo::new(1, "testdb", "rp1", PathBuf::from("/tmp/test"));
    let info = info.with_replica_count(3);

    assert_eq!(info.replica_count, 3);
}

#[test]
fn test_shard_info_with_shard_duration() {
    let info = ShardInfo::new(1, "testdb", "rp1", PathBuf::from("/tmp/test"));
    let info = info.with_shard_duration(86400);

    assert_eq!(info.shard_duration_seconds, 86400);
}

#[test]
fn test_shard_info_with_status() {
    let info = ShardInfo::new(1, "testdb", "rp1", PathBuf::from("/tmp/test"));
    let info = info.with_status(ShardStatus::ReadOnly);

    assert_eq!(info.status, ShardStatus::ReadOnly);
}

#[test]
fn test_shard_info_with_tier() {
    let info = ShardInfo::new(1, "testdb", "rp1", PathBuf::from("/tmp/test"));
    let info = info.with_tier(2);

    assert_eq!(info.tier, 2);
}

#[test]
fn test_shard_info_with_size() {
    let info = ShardInfo::new(1, "testdb", "rp1", PathBuf::from("/tmp/test"));
    let info = info.with_size(1024 * 1024);

    assert_eq!(info.size_bytes, 1024 * 1024);
}

#[test]
fn test_shard_info_with_row_count() {
    let info = ShardInfo::new(1, "testdb", "rp1", PathBuf::from("/tmp/test"));
    let info = info.with_row_count(1000);

    assert_eq!(info.row_count, 1000);
}

#[test]
fn test_shard_info_is_active() {
    let info = ShardInfo::new(1, "testdb", "rp1", PathBuf::from("/tmp/test"));
    assert!(info.is_active());

    let readonly = info.with_status(ShardStatus::ReadOnly);
    assert!(!readonly.is_active());
}

#[test]
fn test_shard_info_is_readonly() {
    let info = ShardInfo::new(1, "testdb", "rp1", PathBuf::from("/tmp/test"));
    assert!(!info.is_readonly());

    let readonly = info.with_status(ShardStatus::ReadOnly);
    assert!(readonly.is_readonly());
}

#[test]
fn test_shard_info_is_migrating() {
    let info = ShardInfo::new(1, "testdb", "rp1", PathBuf::from("/tmp/test"));
    assert!(!info.is_migrating());

    let migrating = info.with_status(ShardStatus::Migrating);
    assert!(migrating.is_migrating());
}

#[test]
fn test_shard_info_time_range() {
    let info = ShardInfo::new(1, "testdb", "rp1", PathBuf::from("/tmp/test"));
    let (start, end) = info.time_range();

    let expected_start = 1 * 3600 * 24 * 7 as i64;
    let expected_end = expected_start + 3600 * 24 * 7 as i64;
    assert_eq!(start, expected_start);
    assert_eq!(end, expected_end);
}

#[test]
fn test_shard_manager_new() {
    let data_dir = create_temp_dir();
    let manager = ShardManager::new(data_dir.clone());

    assert_eq!(manager.get_all_shards().len(), 0);
    assert_eq!(manager.shard_count(), 0);
}

#[test]
fn test_shard_manager_create_shard() {
    let data_dir = create_temp_dir();
    let manager = ShardManager::new(data_dir.clone());

    let shard = manager.create_shard(1, "testdb", "rp1").unwrap();

    assert_eq!(shard.id, 1);
    assert_eq!(shard.database, "testdb");
    assert_eq!(shard.retention_policy, "rp1");
    assert_eq!(manager.shard_count(), 1);
}

#[test]
fn test_shard_manager_get_shard() {
    let data_dir = create_temp_dir();
    let manager = ShardManager::new(data_dir.clone());

    manager.create_shard(1, "testdb", "rp1").unwrap();
    manager.create_shard(2, "testdb", "rp1").unwrap();

    let shard1 = manager.get_shard(1);
    assert!(shard1.is_some());
    assert_eq!(shard1.unwrap().id, 1);

    let nonexistent = manager.get_shard(999);
    assert!(nonexistent.is_none());
}

#[test]
fn test_shard_manager_get_all_shards() {
    let data_dir = create_temp_dir();
    let manager = ShardManager::new(data_dir.clone());

    manager.create_shard(1, "testdb", "rp1").unwrap();
    manager.create_shard(2, "testdb", "rp1").unwrap();
    manager.create_shard(3, "testdb2", "rp1").unwrap();

    let all_shards = manager.get_all_shards();
    assert_eq!(all_shards.len(), 3);
    assert!(all_shards.contains(&1));
    assert!(all_shards.contains(&2));
    assert!(all_shards.contains(&3));
}

#[test]
fn test_shard_manager_remove_shard() {
    let data_dir = create_temp_dir();
    let manager = ShardManager::new(data_dir.clone());

    manager.create_shard(1, "testdb", "rp1").unwrap();
    manager.create_shard(2, "testdb", "rp1").unwrap();

    assert_eq!(manager.shard_count(), 2);

    let removed = manager.remove_shard(1);
    assert!(removed);
    assert_eq!(manager.shard_count(), 1);
    assert!(manager.get_shard(1).is_none());
    assert!(manager.get_shard(2).is_some());

    let not_removed = manager.remove_shard(999);
    assert!(!not_removed);
}

#[test]
fn test_shard_manager_get_shards_by_db() {
    let data_dir = create_temp_dir();
    let manager = ShardManager::new(data_dir.clone());

    manager.create_shard(1, "testdb", "rp1").unwrap();
    manager.create_shard(2, "testdb", "rp1").unwrap();
    manager.create_shard(3, "otherdb", "rp1").unwrap();

    let testdb_shards = manager.get_shards_by_db("testdb");
    assert_eq!(testdb_shards.len(), 2);

    let otherdb_shards = manager.get_shards_by_db("otherdb");
    assert_eq!(otherdb_shards.len(), 1);

    let nonexistent = manager.get_shards_by_db("nonexistent");
    assert_eq!(nonexistent.len(), 0);
}

#[test]
fn test_shard_manager_get_shards_by_rp() {
    let data_dir = create_temp_dir();
    let manager = ShardManager::new(data_dir.clone());

    manager.create_shard(1, "testdb", "rp1").unwrap();
    manager.create_shard(2, "testdb", "rp2").unwrap();
    manager.create_shard(3, "testdb", "rp1").unwrap();

    let rp1_shards = manager.get_shards_by_rp("testdb", "rp1");
    assert_eq!(rp1_shards.len(), 2);

    let rp2_shards = manager.get_shards_by_rp("testdb", "rp2");
    assert_eq!(rp2_shards.len(), 1);

    let nonexistent = manager.get_shards_by_rp("testdb", "nonexistent");
    assert_eq!(nonexistent.len(), 0);
}

#[test]
fn test_shard_manager_update_shard_status() {
    let data_dir = create_temp_dir();
    let manager = ShardManager::new(data_dir.clone());

    manager.create_shard(1, "testdb", "rp1").unwrap();

    let updated = manager.update_shard_status(1, ShardStatus::ReadOnly);
    assert!(updated);

    let shard = manager.get_shard(1).unwrap();
    assert_eq!(shard.status, ShardStatus::ReadOnly);

    let not_updated = manager.update_shard_status(999, ShardStatus::ReadOnly);
    assert!(!not_updated);
}

#[test]
fn test_shard_status_variants() {
    assert_eq!(ShardStatus::Active, ShardStatus::Active);
    assert_eq!(ShardStatus::Inactive, ShardStatus::Inactive);
    assert_eq!(ShardStatus::ReadOnly, ShardStatus::ReadOnly);
    assert_eq!(ShardStatus::Migrating, ShardStatus::Migrating);
    assert_eq!(ShardStatus::Spliting, ShardStatus::Spliting);
}

#[test]
fn test_shard_mapper_new() {
    let mapper = ShardMapper::new(3600, 2);
    assert_eq!(mapper.get_virtual_nodes(), 150);
    assert_eq!(mapper.get_replica_count(), 2);
    assert_eq!(mapper.get_shard_duration(), 3600);
}

#[test]
fn test_shard_mapper_map_shard() {
    let mapper = ShardMapper::new(10, 1);
    let nodes = vec!["node1".to_string(), "node2".to_string()];

    let node = mapper.map_shard_to_node(1, &nodes);
    assert!(node.is_some());

    let nonexistent = mapper.map_shard_to_node(1, &[]);
    assert!(nonexistent.is_none());
}

#[test]
fn test_shard_mapper_get_shard_path() {
    let mapper = ShardMapper::new(10, 1);
    let data_dir = PathBuf::from("/tmp/data");

    let path = mapper.get_shard_path(&data_dir, "testdb", "rp1", 1);
    assert_eq!(path, PathBuf::from("/tmp/data/testdb/rp1/1"));
}

#[test]
fn test_shard_mapper_consistent_hash() {
    let mapper = ShardMapper::new(100, 1);

    let node1 = mapper.map_shard_to_node(1, &["n1".to_string(), "n2".to_string()]);
    let node2 = mapper.map_shard_to_node(1, &["n1".to_string(), "n2".to_string()]);

    assert_eq!(node1, node2);
}

#[test]
fn test_shard_mapper_different_shards_different_nodes() {
    let mapper = ShardMapper::new(10, 1);
    let nodes = vec![
        "node1".to_string(),
        "node2".to_string(),
        "node3".to_string(),
    ];

    let mut results = Vec::new();
    for i in 0..100 {
        if let Some(node) = mapper.map_shard_to_node(i, &nodes) {
            results.push(node);
        }
    }

    assert!(!results.is_empty());
}
