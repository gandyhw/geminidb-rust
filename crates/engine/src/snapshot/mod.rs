use crate::error::{Error, Result};
use crate::FileMeta;
use serde::{Serialize, Deserialize};
use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::RwLock;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotId {
    pub id: u64,
    pub timestamp: i64,
    #[serde(skip)]
    pub path: PathBuf,
}

impl SnapshotId {
    pub fn new(id: u64, timestamp: i64, path: PathBuf) -> Self {
        Self { id, timestamp, path }
    }

    pub fn age_seconds(&self, now: i64) -> i64 {
        now - self.timestamp
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotManifest {
    pub snapshot_id: u64,
    pub timestamp: i64,
    pub file_metas: Vec<FileMeta>,
    pub series_count: usize,
    pub total_rows: u64,
    pub checksum: u32,
}

impl SnapshotManifest {
    pub fn new(snapshot_id: u64, timestamp: i64, file_metas: Vec<FileMeta>, series_count: usize, total_rows: u64) -> Self {
        let checksum = Self::compute_checksum(&file_metas);
        Self {
            snapshot_id,
            timestamp,
            file_metas,
            series_count,
            total_rows,
            checksum,
        }
    }

    fn compute_checksum(file_metas: &[FileMeta]) -> u32 {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        let mut hasher = DefaultHasher::new();
        for meta in file_metas {
            meta.hash(&mut hasher);
        }
        hasher.finish() as u32
    }
}

#[derive(Debug, Clone)]
pub struct SnapshotFile {
    pub path: PathBuf,
    pub manifest: SnapshotManifest,
}

pub struct SnapshotService {
    snapshot_dir: PathBuf,
    snapshots: RwLock<HashMap<u64, SnapshotFile>>,
    max_snapshots: usize,
    retention_seconds: i64,
    next_id: RwLock<u64>,
}

impl SnapshotService {
    pub fn new(snapshot_dir: PathBuf, max_snapshots: usize, retention_seconds: i64) -> Result<Self> {
        fs::create_dir_all(&snapshot_dir).map_err(|e| Error::Snapshot(e.to_string()))?;
        
        let service = Self {
            snapshot_dir,
            snapshots: RwLock::new(HashMap::new()),
            max_snapshots,
            retention_seconds,
            next_id: RwLock::new(1),
        };
        
        service.load_existing_snapshots()?;
        Ok(service)
    }

    fn load_existing_snapshots(&self) -> Result<()> {
        if !self.snapshot_dir.exists() {
            return Ok(());
        }

        for entry in fs::read_dir(&self.snapshot_dir)? {
            let entry = entry.map_err(|e| Error::Snapshot(e.to_string()))?;
            let path = entry.path();
            
            if path.extension().and_then(|s| s.to_str()) == Some("manifest") {
                if let Ok(manifest) = self.read_manifest(&path) {
                    let snapshot_id = manifest.snapshot_id;
                    let snapshots_dir = self.snapshot_dir.join(format!("snapshot_{}", snapshot_id));
                    let snapshot_file = SnapshotFile {
                        path: snapshots_dir,
                        manifest,
                    };
                    self.snapshots.write().unwrap().insert(snapshot_id, snapshot_file);
                    
                    let mut next_id = self.next_id.write().unwrap();
                    if snapshot_id >= *next_id {
                        *next_id = snapshot_id + 1;
                    }
                }
            }
        }
        Ok(())
    }

    fn read_manifest(&self, path: &Path) -> Result<SnapshotManifest> {
        let file = File::open(path).map_err(|e| Error::Snapshot(e.to_string()))?;
        let reader = BufReader::new(file);
        let manifest: SnapshotManifest = serde_json::from_reader(reader)
            .map_err(|e| Error::Snapshot(e.to_string()))?;
        Ok(manifest)
    }

    fn write_manifest(&self, manifest: &SnapshotManifest) -> Result<()> {
        let path = self.snapshot_dir.join(format!("{}.manifest", manifest.snapshot_id));
        let file = File::create(&path).map_err(|e| Error::Snapshot(e.to_string()))?;
        let mut writer = BufWriter::new(file);
        serde_json::to_writer(&mut writer, manifest).map_err(|e| Error::Snapshot(e.to_string()))?;
        writer.flush().map_err(|e| Error::Snapshot(e.to_string()))?;
        Ok(())
    }

    pub fn create_snapshot(&self, file_metas: Vec<FileMeta>, series_count: usize, total_rows: u64) -> Result<SnapshotId> {
        let snapshot_id = {
            let mut next_id = self.next_id.write().unwrap();
            let id = *next_id;
            *next_id += 1;
            id
        };

        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos() as i64;

        let snapshot_dir = self.snapshot_dir.join(format!("snapshot_{}", snapshot_id));
        fs::create_dir_all(&snapshot_dir).map_err(|e| Error::Snapshot(e.to_string()))?;

        let manifest = SnapshotManifest::new(snapshot_id, timestamp, file_metas, series_count, total_rows);
        self.write_manifest(&manifest)?;

        let snapshot_file = SnapshotFile {
            path: snapshot_dir.clone(),
            manifest,
        };
        
        self.snapshots.write().unwrap().insert(snapshot_id, snapshot_file);
        self.cleanup_old_snapshots()?;

        Ok(SnapshotId::new(snapshot_id, timestamp, snapshot_dir))
    }

    pub fn get_snapshot(&self, snapshot_id: u64) -> Option<SnapshotFile> {
        self.snapshots.read().unwrap().get(&snapshot_id).cloned()
    }

    pub fn list_snapshots(&self) -> Vec<SnapshotId> {
        self.snapshots.read().unwrap()
            .values()
            .map(|sf| SnapshotId::new(sf.manifest.snapshot_id, sf.manifest.timestamp, sf.path.clone()))
            .collect()
    }

    pub fn delete_snapshot(&self, snapshot_id: u64) -> Result<bool> {
        let removed = self.snapshots.write().unwrap().remove(&snapshot_id);
        
        if removed.is_some() {
            let manifest_path = self.snapshot_dir.join(format!("{}.manifest", snapshot_id));
            let snapshot_path = self.snapshot_dir.join(format!("snapshot_{}", snapshot_id));
            
            let _ = fs::remove_file(manifest_path);
            let _ = fs::remove_dir_all(snapshot_path);
            
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn cleanup_old_snapshots(&self) -> Result<()> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;

        let to_delete: Vec<u64> = {
            let snapshots = self.snapshots.read().unwrap();
            let count = snapshots.len();
            if count <= self.max_snapshots {
                return Ok(());
            }
            snapshots.values()
                .filter(|sf| sf.manifest.timestamp < now - self.retention_seconds)
                .map(|sf| sf.manifest.snapshot_id)
                .collect()
        };

        for snapshot_id in to_delete {
            let _ = self.delete_snapshot(snapshot_id);
        }

        let excess: Vec<u64> = {
            let snapshots = self.snapshots.read().unwrap();
            let count = snapshots.len();
            if count <= self.max_snapshots {
                return Ok(());
            }
            let excess_count = count - self.max_snapshots;
            let mut ids: Vec<u64> = snapshots.keys().cloned().collect();
            ids.sort_by_key(|id| {
                snapshots.get(id)
                    .map(|sf| sf.manifest.timestamp)
                    .unwrap_or(0)
            });
            ids.into_iter().take(excess_count).collect()
        };

        for snapshot_id in excess {
            let _ = self.delete_snapshot(snapshot_id);
        }

        Ok(())
    }

    pub fn get_latest_snapshot(&self) -> Option<SnapshotId> {
        self.snapshots.read().unwrap()
            .values()
            .max_by_key(|sf| sf.manifest.timestamp)
            .map(|sf| SnapshotId::new(sf.manifest.snapshot_id, sf.manifest.timestamp, sf.path.clone()))
    }

    pub fn verify_snapshot(&self, snapshot_id: u64) -> Result<bool> {
        let binding = self.snapshots.read().unwrap();
        let snapshot = binding
            .get(&snapshot_id)
            .ok_or_else(|| Error::Snapshot(format!("snapshot {} not found", snapshot_id)))?;

        let computed_checksum = SnapshotManifest::compute_checksum(&snapshot.manifest.file_metas);
        
        Ok(computed_checksum == snapshot.manifest.checksum)
    }

    pub fn snapshot_count(&self) -> usize {
        self.snapshots.read().unwrap().len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env::temp_dir;

    fn unique_temp_dir(prefix: &str) -> std::path::PathBuf {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64;
        let pid = std::process::id() as u64;
        let path = temp_dir().join(format!("{}_{}_{}_{}", prefix, pid, now, 0));
        let _ = std::fs::remove_dir_all(&path);
        path
    }

    fn create_test_file_metas() -> Vec<FileMeta> {
        vec![
            FileMeta {
                file_id: 1,
                min_time: 1000,
                max_time: 2000,
                size: 1024 * 1024,
                bloom_filter_data: None,
            },
            FileMeta {
                file_id: 2,
                min_time: 2000,
                max_time: 3000,
                size: 2048 * 1024,
                bloom_filter_data: None,
            },
        ]
    }

    #[test]
    fn test_snapshot_creation() {
        let temp_dir = unique_temp_dir("snapshot_creation");
        std::fs::create_dir_all(&temp_dir).unwrap();
        
        let service = SnapshotService::new(temp_dir.clone(), 10, 86400).unwrap();
        let file_metas = create_test_file_metas();
        
        let snapshot_id = service.create_snapshot(file_metas.clone(), 300, 5000).unwrap();
        
        assert_eq!(snapshot_id.id, 1);
        assert!(snapshot_id.path.exists());
        
        let retrieved = service.get_snapshot(snapshot_id.id).unwrap();
        assert_eq!(retrieved.manifest.snapshot_id, 1);
        assert_eq!(retrieved.manifest.series_count, 300);
        assert_eq!(retrieved.manifest.total_rows, 5000);
        
        std::fs::remove_dir_all(temp_dir).unwrap();
    }

    #[test]
    fn test_snapshot_list() {
        let temp_dir = unique_temp_dir("snapshot_list");
        std::fs::create_dir_all(&temp_dir).unwrap();
        
        let service = SnapshotService::new(temp_dir.clone(), 10, 86400).unwrap();
        
        let id1 = service.create_snapshot(create_test_file_metas(), 100, 1000).unwrap();
        let id2 = service.create_snapshot(create_test_file_metas(), 200, 2000).unwrap();
        let id3 = service.create_snapshot(create_test_file_metas(), 300, 3000).unwrap();
        
        let snapshots = service.list_snapshots();
        assert_eq!(snapshots.len(), 3);
        
        let ids: Vec<u64> = snapshots.iter().map(|s| s.id).collect();
        assert!(ids.contains(&id1.id));
        assert!(ids.contains(&id2.id));
        assert!(ids.contains(&id3.id));
        
        std::fs::remove_dir_all(temp_dir).unwrap();
    }

    #[test]
    fn test_snapshot_deletion() {
        let temp_dir = unique_temp_dir("snapshot_delete");
        std::fs::create_dir_all(&temp_dir).unwrap();
        
        let service = SnapshotService::new(temp_dir.clone(), 10, 86400).unwrap();
        
        let id1 = service.create_snapshot(create_test_file_metas(), 100, 1000).unwrap();
        let _id2 = service.create_snapshot(create_test_file_metas(), 200, 2000).unwrap();
        
        assert_eq!(service.snapshot_count(), 2);
        
        let deleted = service.delete_snapshot(id1.id).unwrap();
        assert!(deleted);
        assert_eq!(service.snapshot_count(), 1);
        assert!(service.get_snapshot(id1.id).is_none());
        
        std::fs::remove_dir_all(temp_dir).unwrap();
    }

    #[test]
    fn test_max_snapshots_retention() {
        let temp_dir = unique_temp_dir("snapshot_max");
        std::fs::create_dir_all(&temp_dir).unwrap();
        
        let service = SnapshotService::new(temp_dir.clone(), 3, 86400).unwrap();
        
        for i in 0..5 {
            service.create_snapshot(create_test_file_metas(), (i + 1) * 100, ((i + 1) * 1000) as u64).unwrap();
        }
        
        assert_eq!(service.snapshot_count(), 3);
        
        let snapshots = service.list_snapshots();
        assert_eq!(snapshots.len(), 3);
        
        std::fs::remove_dir_all(temp_dir).unwrap();
    }

    #[test]
    fn test_latest_snapshot() {
        let temp_dir = unique_temp_dir("snapshot_latest");
        std::fs::create_dir_all(&temp_dir).unwrap();
        
        let service = SnapshotService::new(temp_dir.clone(), 10, 86400).unwrap();
        
        let id1 = service.create_snapshot(create_test_file_metas(), 100, 1000).unwrap();
        let id2 = service.create_snapshot(create_test_file_metas(), 200, 2000).unwrap();
        let id3 = service.create_snapshot(create_test_file_metas(), 300, 3000).unwrap();
        
        let latest = service.get_latest_snapshot().unwrap();
        assert_eq!(latest.id, id3.id);
        
        service.delete_snapshot(id3.id).unwrap();
        
        let latest = service.get_latest_snapshot().unwrap();
        assert_eq!(latest.id, id2.id);
        assert_ne!(latest.id, id1.id);
        
        std::fs::remove_dir_all(temp_dir).unwrap();
    }

    #[test]
    fn test_snapshot_verification() {
        let temp_dir = unique_temp_dir("snapshot_verify");
        std::fs::create_dir_all(&temp_dir).unwrap();
        
        let service = SnapshotService::new(temp_dir.clone(), 10, 86400).unwrap();
        
        let id = service.create_snapshot(create_test_file_metas(), 300, 5000).unwrap();
        
        assert!(service.verify_snapshot(id.id).unwrap());
        
        std::fs::remove_dir_all(temp_dir).unwrap();
    }

    #[test]
    fn test_snapshot_id_age() {
        let temp_dir = unique_temp_dir("snapshot_age");
        std::fs::create_dir_all(&temp_dir).unwrap();
        
        let service = SnapshotService::new(temp_dir.clone(), 10, 86400).unwrap();
        
        let id = service.create_snapshot(create_test_file_metas(), 100, 1000).unwrap();
        
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos() as i64;
        
        let age = id.age_seconds(now);
        assert!(age >= 0);
        assert!(age < 1_000_000_000);
        
        std::fs::remove_dir_all(temp_dir).unwrap();
    }
}