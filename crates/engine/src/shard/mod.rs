use crate::config::{EngineConfig, WalConfig, MemTableConfig, TsspConfig};
use crate::error::{Error, Result};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShardStatus {
    Active,
    Inactive,
    ReadOnly,
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
}

impl ShardInfo {
    pub fn new(
        id: u64,
        database: &str,
        retention_policy: &str,
        path: PathBuf,
    ) -> Self {
        Self {
            id,
            database: database.to_string(),
            retention_policy: retention_policy.to_string(),
            path,
            status: ShardStatus::Active,
            replica_count: 1,
            shard_duration_seconds: 3600 * 24 * 7,
            tier: 0,
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
        
        if let Some(shard) = shards.get_mut(&shard_id) {
            let new_info = ShardInfo {
                id: shard.id,
                database: shard.database.clone(),
                retention_policy: shard.retention_policy.clone(),
                path: shard.path.clone(),
                status,
                replica_count: shard.replica_count,
                shard_duration_seconds: shard.shard_duration_seconds,
                tier: shard.tier,
            };
            shards.insert(shard_id, Arc::new(new_info));
            return true;
        }
        false
    }
}

pub struct ShardMapper {
    shard_duration_seconds: u64,
    replica_count: u32,
}

impl ShardMapper {
    pub fn new(shard_duration_seconds: u64, replica_count: u32) -> Self {
        Self {
            shard_duration_seconds,
            replica_count,
        }
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

    pub fn contains(&self, _shard_id: u64, _timestamp: i64) -> bool {
        true
    }

    pub fn get_shard_path(&self, data_dir: &PathBuf, database: &str, rp: &str, shard_id: u64) -> PathBuf {
        data_dir.join(database).join(rp).join(shard_id.to_string())
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
    }

    #[test]
    fn test_shard_info_with_options() {
        let info = ShardInfo::new(1, "testdb", "rp1", PathBuf::from("/data"))
            .with_replica_count(3)
            .with_shard_duration(3600)
            .with_status(ShardStatus::ReadOnly)
            .with_tier(1);
        
        assert_eq!(info.replica_count, 3);
        assert_eq!(info.shard_duration_seconds, 3600);
        assert_eq!(info.status, ShardStatus::ReadOnly);
        assert_eq!(info.tier, 1);
    }

    #[test]
    fn test_shard_mapper_new() {
        let mapper = ShardMapper::new(3600, 1);
        assert_eq!(mapper.get_shard_duration(), 3600);
        assert_eq!(mapper.get_replica_count(), 1);
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
    fn test_shard_manager_new() {
        let temp_dir = temp_dir();
        let manager = ShardManager::new(temp_dir);
        assert_eq!(manager.shard_count(), 0);
    }

    #[test]
    fn test_shard_manager_create_shard() {
        let temp_dir = temp_dir();
        let manager = ShardManager::new(temp_dir.clone());
        
        let shard = manager.create_shard(1, "testdb", "rp1").unwrap();
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
}
