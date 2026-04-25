use crate::error::{Error, Result};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShardStatus {
    Active,
    Inactive,
    ReadOnly,
    Migrating,
    Spliting,
}

#[derive(Debug, Clone)]
pub struct ShardInfo {
    pub id: u64,
    pub database: String,
    pub retention_policy: String,
    pub path: PathBuf,
    pub status: ShardStatus,
    pub replica_count: u32,
    pub shard_duration_seconds: u64,
    pub tier: u8,
    pub size_bytes: u64,
    pub row_count: u64,
    pub created_at: i64,
    pub updated_at: i64,
}

impl ShardInfo {
    pub fn new(
        id: u64,
        database: &str,
        retention_policy: &str,
        path: PathBuf,
    ) -> Self {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0);
        Self {
            id,
            database: database.to_string(),
            retention_policy: retention_policy.to_string(),
            path,
            status: ShardStatus::Active,
            replica_count: 1,
            shard_duration_seconds: 3600 * 24 * 7,
            tier: 0,
            size_bytes: 0,
            row_count: 0,
            created_at: now,
            updated_at: now,
        }
    }

    pub fn with_replica_count(mut self, replica_count: u32) -> Self {
        self.replica_count = replica_count;
        self
    }

    pub fn with_shard_duration(mut self, duration: u64) -> Self {
        self.shard_duration_seconds = duration;
        self
    }

    pub fn with_status(mut self, status: ShardStatus) -> Self {
        self.status = status;
        self
    }

    pub fn with_tier(mut self, tier: u8) -> Self {
        self.tier = tier;
        self
    }

    pub fn with_size(mut self, size: u64) -> Self {
        self.size_bytes = size;
        self
    }

    pub fn with_row_count(mut self, count: u64) -> Self {
        self.row_count = count;
        self
    }

    pub fn update_metadata(&mut self) {
        self.updated_at = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0);
    }

    pub fn is_active(&self) -> bool {
        self.status == ShardStatus::Active
    }

    pub fn is_readonly(&self) -> bool {
        self.status == ShardStatus::ReadOnly
    }

    pub fn is_migrating(&self) -> bool {
        self.status == ShardStatus::Migrating
    }

    pub fn time_range(&self) -> (i64, i64) {
        let start = self.id as i64 * self.shard_duration_seconds as i64;
        let end = start + self.shard_duration_seconds as i64;
        (start, end)
    }
}

pub struct ShardManager {
    shards: RwLock<HashMap<u64, Arc<ShardInfo>>>,
    data_dir: PathBuf,
}

impl ShardManager {
    pub fn new(data_dir: PathBuf) -> Self {
        Self {
            shards: RwLock::new(HashMap::new()),
            data_dir,
        }
    }

    pub fn create_shard(
        &self,
        shard_id: u64,
        database: &str,
        retention_policy: &str,
    ) -> Result<Arc<ShardInfo>> {
        let shard_path = self.data_dir
            .join(database)
            .join(retention_policy)
            .join(shard_id.to_string());

        std::fs::create_dir_all(&shard_path).map_err(|e| Error::Io(e))?;

        let info = ShardInfo::new(shard_id, database, retention_policy, shard_path);
        let arc = Arc::new(info);

        let mut shards = self.shards.write().map_err(|_| Error::Io(std::io::Error::new(
            std::io::ErrorKind::WouldBlock,
            "failed to acquire write lock",
        )))?;
        shards.insert(shard_id, arc.clone());

        Ok(arc)
    }

    pub fn get_shard(&self, shard_id: u64) -> Option<Arc<ShardInfo>> {
        let shards = self.shards.read().ok()?;
        shards.get(&shard_id).cloned()
    }

    pub fn get_all_shards(&self) -> Vec<u64> {
        self.shards.read().map(|s| s.keys().cloned().collect()).unwrap_or_default()
    }

    pub fn remove_shard(&self, shard_id: u64) -> bool {
        let mut shards = match self.shards.write() {
            Ok(s) => s,
            Err(_) => return false,
        };
        shards.remove(&shard_id).is_some()
    }

    pub fn shard_count(&self) -> usize {
        self.shards.read().map(|s| s.len()).unwrap_or(0)
    }

    pub fn get_shards_by_db(&self, database: &str) -> Vec<Arc<ShardInfo>> {
        self.shards.read()
            .ok()
            .map(|s| s.values().filter(|shard| shard.database == database).cloned().collect())
            .unwrap_or_default()
    }

    pub fn get_shards_by_rp(&self, database: &str, rp: &str) -> Vec<Arc<ShardInfo>> {
        self.shards.read()
            .ok()
            .map(|s| s.values()
                .filter(|shard| shard.database == database && shard.retention_policy == rp)
                .cloned().collect())
            .unwrap_or_default()
    }

    pub fn update_shard_status(&self, shard_id: u64, status: ShardStatus) -> bool {
        let mut shards = match self.shards.write() {
            Ok(s) => s,
            Err(_) => return false,
        };

        if let Some(shard) = shards.get(&shard_id) {
            let mut info: ShardInfo = ShardInfo {
                id: shard.id,
                database: shard.database.clone(),
                retention_policy: shard.retention_policy.clone(),
                path: shard.path.clone(),
                status,
                replica_count: shard.replica_count,
                shard_duration_seconds: shard.shard_duration_seconds,
                tier: shard.tier,
                size_bytes: shard.size_bytes,
                row_count: shard.row_count,
                created_at: shard.created_at,
                updated_at: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_secs() as i64)
                    .unwrap_or(0),
            };
            shards.insert(shard_id, Arc::new(info));
            return true;
        }
        false
    }

    pub fn update_shard_metadata(&self, shard_id: u64, size_bytes: u64, row_count: u64) -> bool {
        let mut shards = match self.shards.write() {
            Ok(s) => s,
            Err(_) => return false,
        };

        if let Some(shard) = shards.get(&shard_id) {
            let info: ShardInfo = ShardInfo {
                id: shard.id,
                database: shard.database.clone(),
                retention_policy: shard.retention_policy.clone(),
                path: shard.path.clone(),
                status: shard.status,
                replica_count: shard.replica_count,
                shard_duration_seconds: shard.shard_duration_seconds,
                tier: shard.tier,
                size_bytes,
                row_count,
                created_at: shard.created_at,
                updated_at: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_secs() as i64)
                    .unwrap_or(0),
            };
            shards.insert(shard_id, Arc::new(info));
            return true;
        }
        false
    }

    pub fn split_shard(&self, shard_id: u64, _split_timestamp: i64) -> Result<Option<(Arc<ShardInfo>, Arc<ShardInfo>)>> {
        let shard = match self.get_shard(shard_id) {
            Some(s) => s,
            None => return Ok(None),
        };

        if shard.is_migrating() || shard.is_readonly() {
            return Err(Error::InvalidArgument(format!("Shard {} is not in a splittable state", shard_id)));
        }

        let _ = self.update_shard_status(shard_id, ShardStatus::Spliting);

        let new_shard_id = shard_id + 1000000;
        let new_info = ShardInfo::new(
            new_shard_id,
            &shard.database,
            &shard.retention_policy,
            self.data_dir
                .join(&shard.database)
                .join(&shard.retention_policy)
                .join(new_shard_id.to_string()),
        )
        .with_size(shard.size_bytes / 2)
        .with_row_count(shard.row_count / 2);

        let updated_original = ShardInfo {
            id: shard.id,
            database: shard.database.clone(),
            retention_policy: shard.retention_policy.clone(),
            path: shard.path.clone(),
            status: ShardStatus::Active,
            replica_count: shard.replica_count,
            shard_duration_seconds: shard.shard_duration_seconds,
            tier: shard.tier,
            size_bytes: shard.size_bytes / 2,
            row_count: shard.row_count / 2,
            created_at: shard.created_at,
            updated_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_secs() as i64)
                .unwrap_or(0),
        };

        {
            let mut shards = match self.shards.write() {
                Ok(s) => s,
                Err(_) => return Err(Error::InvalidArgument("lock error".to_string())),
            };
            shards.insert(shard_id, Arc::new(updated_original.clone()));
            shards.insert(new_shard_id, Arc::new(new_info.clone()));
        }

        Ok(Some((Arc::new(updated_original), Arc::new(new_info))))
    }

    pub fn migrate_shard(&self, shard_id: u64, _target_node: &str) -> Result<bool> {
        let shard = match self.get_shard(shard_id) {
            Some(s) => s,
            None => return Err(Error::NotFound(format!("Shard {} not found", shard_id))),
        };

        if !shard.is_active() {
            return Err(Error::InvalidArgument(format!("Shard {} is not active", shard_id)));
        }

        let _ = self.update_shard_status(shard_id, ShardStatus::Migrating);

        Ok(true)
    }

    pub fn get_shard_stats(&self) -> ShardStats {
        let shards = self.shards.read().unwrap_or_else(|e| e.into_inner());
        let total_shards = shards.len();
        let total_size: u64 = shards.values().map(|s| s.size_bytes).sum();
        let total_rows: u64 = shards.values().map(|s| s.row_count).sum();

        let mut status_counts = HashMap::new();
        for shard in shards.values() {
            *status_counts.entry(format!("{:?}", shard.status)).or_insert(0) += 1;
        }

        ShardStats {
            total_shards,
            total_size_bytes: total_size,
            total_rows,
            status_counts,
        }
    }

    pub fn find_shard_by_time(&self, database: &str, rp: &str, timestamp: i64) -> Option<Arc<ShardInfo>> {
        let shards = self.get_shards_by_rp(database, rp);
        shards.into_iter().find(|shard| {
            let (start, end) = shard.time_range();
            timestamp >= start && timestamp < end
        })
    }
}

#[derive(Debug, Clone)]
pub struct ShardStats {
    pub total_shards: usize,
    pub total_size_bytes: u64,
    pub total_rows: u64,
    pub status_counts: HashMap<String, usize>,
}

impl ShardStats {
    pub fn average_shard_size(&self) -> u64 {
        if self.total_shards == 0 {
            return 0;
        }
        self.total_size_bytes / self.total_shards as u64
    }

    pub fn shard_count_by_status(&self, status: &str) -> usize {
        self.status_counts.get(status).copied().unwrap_or(0)
    }
}

pub struct ShardMapper {
    shard_duration_seconds: u64,
    replica_count: u32,
    virtual_nodes: u32,
}

impl ShardMapper {
    pub fn new(shard_duration_seconds: u64, replica_count: u32) -> Self {
        Self {
            shard_duration_seconds,
            replica_count,
            virtual_nodes: 150,
        }
    }

    pub fn with_virtual_nodes(mut self, vnodes: u32) -> Self {
        self.virtual_nodes = vnodes;
        self
    }

    pub fn map_shard(&self, timestamp: i64) -> u64 {
        (timestamp / self.shard_duration_seconds as i64).unsigned_abs() as u64
    }

    pub fn get_shard_duration(&self) -> u64 {
        self.shard_duration_seconds
    }

    pub fn get_replica_count(&self) -> u32 {
        self.replica_count
    }

    pub fn get_virtual_nodes(&self) -> u32 {
        self.virtual_nodes
    }

    pub fn contains(&self, _shard_id: u64, _timestamp: i64) -> bool {
        true
    }

    pub fn get_shard_path(&self, data_dir: &PathBuf, database: &str, rp: &str, shard_id: u64) -> PathBuf {
        data_dir.join(database).join(rp).join(shard_id.to_string())
    }

    pub fn map_shard_to_node(&self, shard_id: u64, nodes: &[String]) -> Option<String> {
        if nodes.is_empty() {
            return None;
        }
        let vnode = (shard_id * self.virtual_nodes as u64) % (nodes.len() as u64 * self.virtual_nodes as u64);
        let node_idx = (vnode / self.virtual_nodes as u64) as usize;
        nodes.get(node_idx).cloned()
    }

    pub fn calculate_shard_key(&self, database: &str, rp: &str, timestamp: i64) -> String {
        format!("{}/{}/{}", database, rp, self.map_shard(timestamp))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env::temp_dir;

    #[test]
    fn test_shard_status() {
        assert_eq!(ShardStatus::Active, ShardStatus::Active);
        assert_eq!(ShardStatus::Inactive, ShardStatus::Inactive);
        assert_eq!(ShardStatus::ReadOnly, ShardStatus::ReadOnly);
        assert_eq!(ShardStatus::Migrating, ShardStatus::Migrating);
        assert_eq!(ShardStatus::Spliting, ShardStatus::Spliting);
    }

    #[test]
    fn test_shard_info_new() {
        let info = ShardInfo::new(1, "testdb", "rp1", PathBuf::from("/data"));
        assert_eq!(info.id, 1);
        assert_eq!(info.database, "testdb");
        assert_eq!(info.retention_policy, "rp1");
        assert_eq!(info.status, ShardStatus::Active);
        assert_eq!(info.replica_count, 1);
        assert_eq!(info.shard_duration_seconds, 3600 * 24 * 7);
        assert_eq!(info.tier, 0);
        assert_eq!(info.size_bytes, 0);
        assert_eq!(info.row_count, 0);
    }

    #[test]
    fn test_shard_info_with_options() {
        let info = ShardInfo::new(1, "testdb", "rp1", PathBuf::from("/data"))
            .with_replica_count(3)
            .with_shard_duration(3600)
            .with_status(ShardStatus::ReadOnly)
            .with_tier(1)
            .with_size(1024)
            .with_row_count(100);

        assert_eq!(info.replica_count, 3);
        assert_eq!(info.shard_duration_seconds, 3600);
        assert_eq!(info.status, ShardStatus::ReadOnly);
        assert_eq!(info.tier, 1);
        assert_eq!(info.size_bytes, 1024);
        assert_eq!(info.row_count, 100);
    }

    #[test]
    fn test_shard_info_is_active() {
        let info = ShardInfo::new(1, "testdb", "rp1", PathBuf::from("/data"));
        assert!(info.is_active());
        assert!(!info.is_readonly());
        assert!(!info.is_migrating());
    }

    #[test]
    fn test_shard_info_time_range() {
        let info = ShardInfo::new(1, "testdb", "rp1", PathBuf::from("/data"))
            .with_shard_duration(3600);
        let (start, end) = info.time_range();
        assert_eq!(start, 3600);
        assert_eq!(end, 7200);
    }

    #[test]
    fn test_shard_mapper_new() {
        let mapper = ShardMapper::new(3600, 1);
        assert_eq!(mapper.get_shard_duration(), 3600);
        assert_eq!(mapper.get_replica_count(), 1);
        assert_eq!(mapper.get_virtual_nodes(), 150);
    }

    #[test]
    fn test_shard_mapper_with_virtual_nodes() {
        let mapper = ShardMapper::new(3600, 1).with_virtual_nodes(300);
        assert_eq!(mapper.get_virtual_nodes(), 300);
    }

    #[test]
    fn test_shard_mapper_map_shard() {
        let mapper = ShardMapper::new(3600, 1);

        assert_eq!(mapper.map_shard(0), 0);
        assert_eq!(mapper.map_shard(3600), 1);
        assert_eq!(mapper.map_shard(7200), 2);
        assert_eq!(mapper.map_shard(3600000), 1000);
    }

    #[test]
    fn test_shard_mapper_contains() {
        let mapper = ShardMapper::new(3600, 1);
        assert!(mapper.contains(0, 0));
        assert!(mapper.contains(1, 3600));
    }

    #[test]
    fn test_shard_mapper_get_shard_path() {
        let mapper = ShardMapper::new(3600, 1);
        let data_dir = PathBuf::from("/data");
        let path = mapper.get_shard_path(&data_dir, "testdb", "rp1", 1);

        assert_eq!(path, PathBuf::from("/data/testdb/rp1/1"));
    }

    #[test]
    fn test_shard_mapper_map_shard_to_node() {
        let mapper = ShardMapper::new(3600, 1);
        let nodes = vec!["node1".to_string(), "node2".to_string(), "node3".to_string()];

        let result = mapper.map_shard_to_node(0, &nodes);
        assert!(result.is_some());
    }

    #[test]
    fn test_shard_mapper_calculate_shard_key() {
        let mapper = ShardMapper::new(3600, 1);
        let key = mapper.calculate_shard_key("testdb", "rp1", 7200);
        assert_eq!(key, "testdb/rp1/2");
    }

    #[test]
    fn test_shard_manager_new() {
        let temp_dir = temp_dir();
        let manager = ShardManager::new(temp_dir);
        assert_eq!(manager.shard_count(), 0);
    }

    #[test]
    fn test_shard_manager_create_shard() {
        let temp_dir = temp_dir();
        let manager = ShardManager::new(temp_dir.clone());

        let _shard = manager.create_shard(1, "testdb", "rp1").unwrap();
        assert_eq!(manager.shard_count(), 1);

        let retrieved = manager.get_shard(1);
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().id, 1);
    }

    #[test]
    fn test_shard_manager_get_nonexistent_shard() {
        let temp_dir = temp_dir();
        let manager = ShardManager::new(temp_dir);

        let shard = manager.get_shard(999);
        assert!(shard.is_none());
    }

    #[test]
    fn test_shard_manager_remove_shard() {
        let temp_dir = temp_dir();
        let manager = ShardManager::new(temp_dir);

        manager.create_shard(1, "testdb", "rp1").unwrap();
        assert_eq!(manager.shard_count(), 1);

        let removed = manager.remove_shard(1);
        assert!(removed);
        assert_eq!(manager.shard_count(), 0);
    }

    #[test]
    fn test_shard_manager_get_all_shards() {
        let temp_dir = temp_dir();
        let manager = ShardManager::new(temp_dir);

        manager.create_shard(1, "testdb", "rp1").unwrap();
        manager.create_shard(2, "testdb", "rp1").unwrap();
        manager.create_shard(3, "testdb", "rp1").unwrap();

        let all = manager.get_all_shards();
        assert_eq!(all.len(), 3);
        assert!(all.contains(&1));
        assert!(all.contains(&2));
        assert!(all.contains(&3));
    }

    #[test]
    fn test_shard_manager_get_shards_by_db() {
        let temp_dir = temp_dir();
        let manager = ShardManager::new(temp_dir);

        manager.create_shard(1, "testdb1", "rp1").unwrap();
        manager.create_shard(2, "testdb1", "rp2").unwrap();
        manager.create_shard(3, "testdb2", "rp1").unwrap();

        let shards = manager.get_shards_by_db("testdb1");
        assert_eq!(shards.len(), 2);
    }

    #[test]
    fn test_shard_manager_get_shards_by_rp() {
        let temp_dir = temp_dir();
        let manager = ShardManager::new(temp_dir);

        manager.create_shard(1, "testdb", "rp1").unwrap();
        manager.create_shard(2, "testdb", "rp2").unwrap();
        manager.create_shard(3, "testdb", "rp1").unwrap();

        let shards = manager.get_shards_by_rp("testdb", "rp1");
        assert_eq!(shards.len(), 2);
    }

    #[test]
    fn test_shard_manager_update_status() {
        let temp_dir = temp_dir();
        let manager = ShardManager::new(temp_dir);

        manager.create_shard(1, "testdb", "rp1").unwrap();

        let updated = manager.update_shard_status(1, ShardStatus::ReadOnly);
        assert!(updated);

        let shard = manager.get_shard(1).unwrap();
        assert_eq!(shard.status, ShardStatus::ReadOnly);
    }

    #[test]
    fn test_shard_manager_update_metadata() {
        let temp_dir = temp_dir();
        let manager = ShardManager::new(temp_dir);

        manager.create_shard(1, "testdb", "rp1").unwrap();

        let updated = manager.update_shard_metadata(1, 1024, 100);
        assert!(updated);

        let shard = manager.get_shard(1).unwrap();
        assert_eq!(shard.size_bytes, 1024);
        assert_eq!(shard.row_count, 100);
    }

    #[test]
    fn test_shard_manager_get_shard_stats() {
        let temp_dir = temp_dir();
        let manager = ShardManager::new(temp_dir);

        manager.create_shard(1, "testdb", "rp1").unwrap();
        manager.create_shard(2, "testdb", "rp1").unwrap();
        manager.update_shard_metadata(1, 1024, 100);
        manager.update_shard_metadata(2, 2048, 200);

        let stats = manager.get_shard_stats();
        assert_eq!(stats.total_shards, 2);
        assert_eq!(stats.total_size_bytes, 3072);
        assert_eq!(stats.total_rows, 300);
    }

    #[test]
    fn test_shard_stats_average_shard_size() {
        let temp_dir = temp_dir();
        let manager = ShardManager::new(temp_dir);

        manager.create_shard(1, "testdb", "rp1").unwrap();
        manager.create_shard(2, "testdb", "rp1").unwrap();
        manager.update_shard_metadata(1, 1024, 100);
        manager.update_shard_metadata(2, 2048, 200);

        let stats = manager.get_shard_stats();
        assert_eq!(stats.average_shard_size(), 1536);
    }

    #[test]
    fn test_shard_manager_find_shard_by_time() {
        let temp_dir = temp_dir();
        let manager = ShardManager::new(temp_dir);

        manager.create_shard(1, "testdb", "rp1").unwrap();
        manager.create_shard(2, "testdb", "rp1").unwrap();

        let shard = manager.find_shard_by_time("testdb", "rp1", 700000);
        assert!(shard.is_some());
        assert_eq!(shard.unwrap().id, 1);
    }

    #[test]
    fn test_shard_manager_split_shard() {
        let temp_dir = temp_dir();
        let manager = ShardManager::new(temp_dir);

        manager.create_shard(1, "testdb", "rp1").unwrap();
        manager.update_shard_metadata(1, 1024, 100);

        let result = manager.split_shard(1, 7200);
        assert!(result.is_ok());
        let split_result = result.unwrap();
        assert!(split_result.is_some());

        let (original, new_shard) = split_result.unwrap();
        assert_eq!(original.id, 1);
        assert!(new_shard.id > 1000000);
    }

    #[test]
    fn test_shard_info_active_status() {
        let temp_dir = temp_dir();
        let manager = ShardManager::new(temp_dir);

        manager.create_shard(1, "testdb", "rp1").unwrap();
        let shard = manager.get_shard(1).unwrap();

        assert!(shard.is_active());
        assert!(!shard.is_readonly());
        assert!(!shard.is_migrating());
    }

    #[test]
    fn test_shard_info_readonly_status() {
        let info = ShardInfo::new(1, "testdb", "rp1", PathBuf::from("/tmp/test"))
            .with_status(ShardStatus::ReadOnly);

        assert!(!info.is_active());
        assert!(info.is_readonly());
    }

    #[test]
    fn test_shard_info_migrating_status() {
        let info = ShardInfo::new(1, "testdb", "rp1", PathBuf::from("/tmp/test"))
            .with_status(ShardStatus::Migrating);

        assert!(info.is_migrating());
    }

    #[test]
    fn test_shard_info_update_metadata() {
        let mut info = ShardInfo::new(1, "testdb", "rp1", PathBuf::from("/tmp/test"));

        let old_updated_at = info.updated_at;
        info.update_metadata();

        assert!(info.updated_at >= old_updated_at);
    }

    #[test]
    fn test_shard_info_with_replica_count() {
        let info = ShardInfo::new(1, "testdb", "rp1", PathBuf::from("/tmp/test"))
            .with_replica_count(3);

        assert_eq!(info.replica_count, 3);
    }

    #[test]
    fn test_shard_info_with_tier() {
        let info = ShardInfo::new(1, "testdb", "rp1", PathBuf::from("/tmp/test"))
            .with_tier(2);

        assert_eq!(info.tier, 2);
    }

    #[test]
    fn test_shard_info_with_size_and_row_count() {
        let info = ShardInfo::new(1, "testdb", "rp1", PathBuf::from("/tmp/test"))
            .with_size(1024 * 1024)
            .with_row_count(10000);

        assert_eq!(info.size_bytes, 1024 * 1024);
        assert_eq!(info.row_count, 10000);
    }

    #[test]
    fn test_shard_info_shard_duration() {
        let info = ShardInfo::new(1, "testdb", "rp1", PathBuf::from("/tmp/test"))
            .with_shard_duration(3600 * 24);

        assert_eq!(info.shard_duration_seconds, 3600 * 24);
    }
}
