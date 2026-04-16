use crate::{Engine, Result};
use std::path::{Path, PathBuf};
use std::fs::{self, File};
use std::io::Read;
use serde::{Deserialize, Serialize};
use chrono::{DateTime, Utc};

#[derive(Debug, Clone)]
pub struct BackupConfig {
    pub enabled: bool,
    pub backup_dir: PathBuf,
    pub max_full_backups: usize,
    pub max_incremental_backups: usize,
}

impl Default for BackupConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            backup_dir: PathBuf::from("backups"),
            max_full_backups: 3,
            max_incremental_backups: 10,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackupMeta {
    pub id: String,
    pub backup_type: BackupType,
    pub database: String,
    pub created_at: DateTime<Utc>,
    pub files: Vec<BackupFileInfo>,
    pub total_size: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum BackupType {
    Full,
    Incremental,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackupFileInfo {
    pub path: String,
    pub size: u64,
    pub checksum: String,
    pub shard_id: u64,
    pub measurement: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackupManifest {
    pub version: String,
    pub created_at: DateTime<Utc>,
    pub full_backup_id: Option<String>,
    pub incremental_backups: Vec<String>,
    pub databases: Vec<DatabaseBackup>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatabaseBackup {
    pub name: String,
    pub retention_policy: String,
    pub shard_id: u64,
    pub files: Vec<BackupFileInfo>,
}

pub struct BackupManager {
    config: BackupConfig,
    engine: std::sync::Arc<std::sync::RwLock<Option<Engine>>>,
}

impl BackupManager {
    pub fn new(config: BackupConfig, engine: std::sync::Arc<std::sync::RwLock<Option<Engine>>>) -> Self {
        Self { config, engine }
    }
    
    pub fn create_full_backup(&self, database: &str, backup_id: Option<String>) -> Result<BackupMeta> {
        let id = backup_id.unwrap_or_else(generate_backup_id);
        let backup_path = self.config.backup_dir.join(&id);
        
        fs::create_dir_all(&backup_path)?;
        
        let binding = self.engine.read().unwrap();
        let _engine = match binding.as_ref() {
            Some(e) => e,
            None => return Err(crate::Error::InvalidArgument("engine not initialized".to_string())),
        };
        
        let files = Vec::new();
        let total_size = 0u64;
        
        let manifest = BackupManifest {
            version: "1.0".to_string(),
            created_at: Utc::now(),
            full_backup_id: Some(id.clone()),
            incremental_backups: Vec::new(),
            databases: vec![DatabaseBackup {
                name: database.to_string(),
                retention_policy: "default".to_string(),
                shard_id: 0,
                files: files.clone(),
            }],
        };
        
        let manifest_path = backup_path.join("manifest.json");
        let manifest_json = serde_json::to_string_pretty(&manifest)
            .map_err(|e| crate::Error::InvalidArgument(e.to_string()))?;
        fs::write(&manifest_path, manifest_json)?;
        
        self.cleanup_old_backups()?;
        
        Ok(BackupMeta {
            id,
            backup_type: BackupType::Full,
            database: database.to_string(),
            created_at: Utc::now(),
            files,
            total_size,
        })
    }
    
    pub fn create_incremental_backup(&self, database: &str, since_backup_id: &str) -> Result<BackupMeta> {
        let id = generate_backup_id();
        let backup_path = self.config.backup_dir.join(&id);
        
        fs::create_dir_all(&backup_path)?;
        
        let since_manifest = self.load_manifest(since_backup_id)?;
        
        let files = Vec::new();
        let total_size = 0u64;
        
        let manifest = BackupManifest {
            version: "1.0".to_string(),
            created_at: Utc::now(),
            full_backup_id: since_manifest.full_backup_id.clone(),
            incremental_backups: vec![id.clone()],
            databases: vec![],
        };
        
        let manifest_path = backup_path.join("manifest.json");
        let manifest_json = serde_json::to_string_pretty(&manifest)
            .map_err(|e| crate::Error::InvalidArgument(e.to_string()))?;
        fs::write(&manifest_path, manifest_json)?;
        
        Ok(BackupMeta {
            id,
            backup_type: BackupType::Incremental,
            database: database.to_string(),
            created_at: Utc::now(),
            files,
            total_size,
        })
    }
    
    pub fn restore(&self, backup_id: &str, target_path: Option<&Path>) -> Result<()> {
        let backup_path = self.config.backup_dir.join(backup_id);
        if !backup_path.exists() {
            return Err(crate::Error::NotFound(format!("backup {} not found", backup_id)));
        }
        
        let manifest = self.load_manifest(backup_id)?;
        let target = target_path.unwrap_or(&self.config.backup_dir);
        
        for db in &manifest.databases {
            for file in &db.files {
                let source = Path::new(&file.path);
                let dest = target.join("data")
                    .join(&db.name)
                    .join(&db.retention_policy)
                    .join(format!("shard_{}", db.shard_id))
                    .join(source.file_name().unwrap());
                
                if source.exists() {
                    fs::create_dir_all(dest.parent().unwrap())?;
                    fs::copy(source, &dest)?;
                }
            }
        }
        
        Ok(())
    }
    
    pub fn list_backups(&self) -> Result<Vec<BackupMeta>> {
        let mut backups = Vec::new();
        
        if !self.config.backup_dir.exists() {
            return Ok(backups);
        }
        
        if let Ok(entries) = fs::read_dir(&self.config.backup_dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                
                if path.is_dir() {
                    if let Ok(manifest) = self.load_manifest(path.file_name().unwrap().to_str().unwrap()) {
                        let backup_type = if manifest.incremental_backups.is_empty() {
                            BackupType::Full
                        } else {
                            BackupType::Incremental
                        };
                        
                        let total_size: u64 = manifest.databases.iter()
                            .flat_map(|db| &db.files)
                            .map(|f| f.size)
                            .sum();
                        
                        backups.push(BackupMeta {
                            id: path.file_name().unwrap().to_str().unwrap().to_string(),
                            backup_type,
                            database: manifest.databases.first()
                                .map(|d| d.name.clone())
                                .unwrap_or_default(),
                            created_at: manifest.created_at,
                            files: manifest.databases.iter()
                                .flat_map(|d| &d.files)
                                .cloned()
                                .collect(),
                            total_size,
                        });
                    }
                }
            }
        }
        
        backups.sort_by(|a, b| b.created_at.cmp(&a.created_at));
        Ok(backups)
    }
    
    fn load_manifest(&self, backup_id: &str) -> Result<BackupManifest> {
        let manifest_path = self.config.backup_dir.join(backup_id).join("manifest.json");
        let content = fs::read_to_string(&manifest_path)?;
        serde_json::from_str(&content)
            .map_err(|e| crate::Error::InvalidArgument(e.to_string()))
    }
    
    fn cleanup_old_backups(&self) -> Result<()> {
        let backups = self.list_backups()?;
        
        let full_backups: Vec<_> = backups.iter()
            .filter(|b| b.backup_type == BackupType::Full)
            .collect();
        
        if full_backups.len() > self.config.max_full_backups {
            for backup in &full_backups[self.config.max_full_backups..] {
                let path = self.config.backup_dir.join(&backup.id);
                let _ = fs::remove_dir_all(&path);
            }
        }
        
        let incremental_backups: Vec<_> = backups.iter()
            .filter(|b| b.backup_type == BackupType::Incremental)
            .collect();
        
        if incremental_backups.len() > self.config.max_incremental_backups {
            for backup in &incremental_backups[self.config.max_incremental_backups..] {
                let path = self.config.backup_dir.join(&backup.id);
                let _ = fs::remove_dir_all(&path);
            }
        }
        
        Ok(())
    }
}

fn generate_backup_id() -> String {
    let now = Utc::now();
    format!("backup_{}_{}", now.format("%Y%m%d_%H%M%S"), now.timestamp_subsec_nanos())
}

fn calculate_checksum(path: &Path) -> Result<String> {
    let mut file = File::open(path)?;
    let mut buffer = Vec::new();
    file.read_to_end(&mut buffer)?;
    
    let checksum = crc32fast::hash(&buffer);
    Ok(format!("{:08x}", checksum))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_backup_config_default() {
        let config = BackupConfig::default();
        assert!(config.enabled);
        assert_eq!(config.max_full_backups, 3);
        assert_eq!(config.max_incremental_backups, 10);
    }

    #[test]
    fn test_generate_backup_id() {
        let id1 = generate_backup_id();
        let id2 = generate_backup_id();
        assert!(id1.starts_with("backup_"));
        assert!(id2.starts_with("backup_"));
        assert_ne!(id1, id2);
    }

    #[test]
    fn test_calculate_checksum() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("test.tssp");
        
        std::fs::write(&file_path, b"test data").unwrap();
        
        let checksum = calculate_checksum(&file_path).unwrap();
        assert_eq!(checksum.len(), 8);
        assert!(checksum.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn test_backup_type_equality() {
        assert_eq!(BackupType::Full, BackupType::Full);
        assert_eq!(BackupType::Incremental, BackupType::Incremental);
        assert_ne!(BackupType::Full, BackupType::Incremental);
    }

    #[test]
    fn test_backup_file_info_serialization() {
        let info = BackupFileInfo {
            path: "/data/test.tssp".to_string(),
            size: 1024,
            checksum: "abc123".to_string(),
            shard_id: 1,
            measurement: "cpu".to_string(),
        };
        
        let json = serde_json::to_string(&info).unwrap();
        let decoded: BackupFileInfo = serde_json::from_str(&json).unwrap();
        
        assert_eq!(decoded.path, info.path);
        assert_eq!(decoded.size, info.size);
        assert_eq!(decoded.checksum, info.checksum);
    }
}
