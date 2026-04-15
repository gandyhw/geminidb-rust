use crate::error::Result;
use std::path::PathBuf;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StorageTier {
    Hot = 0,
    Warm = 1,
    Cold = 2,
}

impl StorageTier {
    pub fn from_u8(v: u8) -> Self {
        match v {
            0 => StorageTier::Hot,
            1 => StorageTier::Warm,
            2 => StorageTier::Cold,
            _ => StorageTier::Hot,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            StorageTier::Hot => "hot",
            StorageTier::Warm => "warm",
            StorageTier::Cold => "cold",
        }
    }

    pub fn next(&self) -> Option<StorageTier> {
        match self {
            StorageTier::Hot => Some(StorageTier::Warm),
            StorageTier::Warm => Some(StorageTier::Cold),
            StorageTier::Cold => None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct TierConfig {
    pub hot_duration_seconds: u64,
    pub warm_duration_seconds: u64,
    pub cold_enabled: bool,
    pub cold_path: Option<PathBuf>,
}

impl Default for TierConfig {
    fn default() -> Self {
        Self {
            hot_duration_seconds: 3600,
            warm_duration_seconds: 86400 * 7,
            cold_enabled: false,
            cold_path: None,
        }
    }
}

impl TierConfig {
    pub fn new(hot_duration_seconds: u64, warm_duration_seconds: u64) -> Self {
        Self {
            hot_duration_seconds,
            warm_duration_seconds,
            cold_enabled: false,
            cold_path: None,
        }
    }

    pub fn with_cold_enabled(mut self, enabled: bool) -> Self {
        self.cold_enabled = enabled;
        self
    }

    pub fn with_cold_path(mut self, path: PathBuf) -> Self {
        self.cold_path = Some(path);
        self
    }

    pub fn get_tier_for_timestamp(&self, timestamp: i64, now: i64) -> StorageTier {
        let age_seconds = (now - timestamp) as u64;

        if age_seconds < self.hot_duration_seconds {
            return StorageTier::Hot;
        }

        if age_seconds < self.hot_duration_seconds + self.warm_duration_seconds {
            return StorageTier::Warm;
        }

        StorageTier::Cold
    }

    pub fn is_tier_expired(&self, timestamp: i64, now: i64, tier: StorageTier) -> bool {
        let age_seconds = (now - timestamp) as u64;

        match tier {
            StorageTier::Hot => false,
            StorageTier::Warm => {
                age_seconds >= self.hot_duration_seconds + self.warm_duration_seconds
            }
            StorageTier::Cold => true,
        }
    }
}

pub struct TieredStorageManager {
    config: TierConfig,
    data_dir: PathBuf,
}

impl TieredStorageManager {
    pub fn new(data_dir: PathBuf, config: TierConfig) -> Self {
        Self { data_dir, config }
    }

    pub fn get_tier_path(&self, tier: StorageTier, db: &str, rp: &str, shard_id: u64) -> PathBuf {
        match tier {
            StorageTier::Hot | StorageTier::Warm => {
                self.data_dir.join(db).join(rp).join(shard_id.to_string())
            }
            StorageTier::Cold => {
                if let Some(ref cold_path) = self.config.cold_path {
                    cold_path.join(db).join(rp).join(shard_id.to_string())
                } else {
                    self.data_dir.join("cold").join(db).join(rp).join(shard_id.to_string())
                }
            }
        }
    }

    pub fn determine_tier(&self, timestamp: i64) -> StorageTier {
        let now = chrono::Utc::now().timestamp();
        self.config.get_tier_for_timestamp(timestamp, now)
    }

    pub fn should_migrate_to_next_tier(&self, timestamp: i64, current_tier: StorageTier) -> bool {
        let now = chrono::Utc::now().timestamp();
        
        if current_tier.next().is_some() {
            let age_seconds = (now - timestamp) as u64;
            
            match current_tier {
                StorageTier::Hot => age_seconds >= self.config.hot_duration_seconds,
                StorageTier::Warm => age_seconds >= self.config.hot_duration_seconds + self.config.warm_duration_seconds,
                StorageTier::Cold => false,
            }
        } else {
            false
        }
    }

    pub fn get_config(&self) -> &TierConfig {
        &self.config
    }

    pub fn is_cold_enabled(&self) -> bool {
        self.config.cold_enabled
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_storage_tier_from_u8() {
        assert_eq!(StorageTier::from_u8(0), StorageTier::Hot);
        assert_eq!(StorageTier::from_u8(1), StorageTier::Warm);
        assert_eq!(StorageTier::from_u8(2), StorageTier::Cold);
        assert_eq!(StorageTier::from_u8(255), StorageTier::Hot);
    }

    #[test]
    fn test_storage_tier_as_str() {
        assert_eq!(StorageTier::Hot.as_str(), "hot");
        assert_eq!(StorageTier::Warm.as_str(), "warm");
        assert_eq!(StorageTier::Cold.as_str(), "cold");
    }

    #[test]
    fn test_storage_tier_next() {
        assert_eq!(StorageTier::Hot.next(), Some(StorageTier::Warm));
        assert_eq!(StorageTier::Warm.next(), Some(StorageTier::Cold));
        assert_eq!(StorageTier::Cold.next(), None);
    }

    #[test]
    fn test_tier_config_default() {
        let config = TierConfig::default();
        assert_eq!(config.hot_duration_seconds, 3600);
        assert_eq!(config.warm_duration_seconds, 86400 * 7);
        assert!(!config.cold_enabled);
        assert!(config.cold_path.is_none());
    }

    #[test]
    fn test_tier_config_new() {
        let config = TierConfig::new(7200, 86400 * 14);
        assert_eq!(config.hot_duration_seconds, 7200);
        assert_eq!(config.warm_duration_seconds, 86400 * 14);
    }

    #[test]
    fn test_tier_config_with_options() {
        let config = TierConfig::new(3600, 86400)
            .with_cold_enabled(true)
            .with_cold_path(PathBuf::from("/cold/storage"));
        
        assert!(config.cold_enabled);
        assert_eq!(config.cold_path, Some(PathBuf::from("/cold/storage")));
    }

    #[test]
    fn test_tier_config_get_tier_for_timestamp_hot() {
        let config = TierConfig::new(3600, 86400);
        let now = 100000;
        let timestamp = now - 1000;
        
        assert_eq!(config.get_tier_for_timestamp(timestamp, now), StorageTier::Hot);
    }

    #[test]
    fn test_tier_config_get_tier_for_timestamp_warm() {
        let config = TierConfig::new(3600, 86400);
        let now = 100000;
        let timestamp = now - 7200;
        
        assert_eq!(config.get_tier_for_timestamp(timestamp, now), StorageTier::Warm);
    }

    #[test]
    fn test_tier_config_get_tier_for_timestamp_cold() {
        let config = TierConfig::new(3600, 86400);
        let now = 100000;
        let timestamp = now - 100000;
        
        assert_eq!(config.get_tier_for_timestamp(timestamp, now), StorageTier::Cold);
    }

    #[test]
    fn test_tier_config_is_tier_expired() {
        let config = TierConfig::new(3600, 86400);
        let now = 100000;
        
        assert!(!config.is_tier_expired(now - 1000, now, StorageTier::Hot));
        assert!(!config.is_tier_expired(now - 3600, now, StorageTier::Hot));
        assert!(!config.is_tier_expired(now - 7200, now, StorageTier::Warm));
        assert!(config.is_tier_expired(now - 100000, now, StorageTier::Warm));
        assert!(config.is_tier_expired(now - 1000, now, StorageTier::Cold));
    }

    #[test]
    fn test_tiered_storage_manager_get_tier_path() {
        let data_dir = PathBuf::from("/data");
        let config = TierConfig::default();
        let manager = TieredStorageManager::new(data_dir.clone(), config);
        
        let hot_path = manager.get_tier_path(StorageTier::Hot, "db1", "rp1", 1);
        assert_eq!(hot_path, PathBuf::from("/data/db1/rp1/1"));
        
        let warm_path = manager.get_tier_path(StorageTier::Warm, "db1", "rp1", 1);
        assert_eq!(warm_path, PathBuf::from("/data/db1/rp1/1"));
    }

    #[test]
    fn test_tiered_storage_manager_get_tier_path_cold() {
        let data_dir = PathBuf::from("/data");
        let config = TierConfig::new(3600, 86400).with_cold_path(PathBuf::from("/cold"));
        let manager = TieredStorageManager::new(data_dir, config);
        
        let cold_path = manager.get_tier_path(StorageTier::Cold, "db1", "rp1", 1);
        assert_eq!(cold_path, PathBuf::from("/cold/db1/rp1/1"));
    }

    #[test]
    fn test_tiered_storage_manager_determine_tier() {
        let data_dir = PathBuf::from("/data");
        let config = TierConfig::new(3600, 86400);
        let manager = TieredStorageManager::new(data_dir, config);
        
        let now = chrono::Utc::now().timestamp();
        assert_eq!(manager.determine_tier(now), StorageTier::Hot);
        assert_eq!(manager.determine_tier(now - 7200), StorageTier::Warm);
        assert_eq!(manager.determine_tier(now - 100000), StorageTier::Cold);
    }

    #[test]
    fn test_tiered_storage_manager_should_migrate() {
        let data_dir = PathBuf::from("/data");
        let config = TierConfig::new(3600, 86400);
        let manager = TieredStorageManager::new(data_dir, config);
        
        let now = chrono::Utc::now().timestamp();
        
        assert!(!manager.should_migrate_to_next_tier(now - 1000, StorageTier::Hot));
        assert!(manager.should_migrate_to_next_tier(now - 7200, StorageTier::Hot));
        assert!(!manager.should_migrate_to_next_tier(now - 7200, StorageTier::Warm));
        assert!(manager.should_migrate_to_next_tier(now - 100000, StorageTier::Warm));
        assert!(!manager.should_migrate_to_next_tier(now, StorageTier::Cold));
    }
}
