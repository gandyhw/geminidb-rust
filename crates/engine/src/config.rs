use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Config {
    pub engine: EngineConfig,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct EngineConfig {
    pub data_dir: PathBuf,
    pub wal: WalConfig,
    pub memtable: MemTableConfig,
    pub tssp: TsspConfig,
    pub compaction: CompactionConfig,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct WalConfig {
    pub dir: PathBuf,
    pub file_size: u64,
    pub sync_enabled: bool,
}

impl Default for WalConfig {
    fn default() -> Self {
        Self {
            dir: PathBuf::from("wal"),
            file_size: 64 * 1024 * 1024,
            sync_enabled: true,
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct MemTableConfig {
    pub max_size: u64,
    pub flush_interval_ms: u64,
}

impl Default for MemTableConfig {
    fn default() -> Self {
        Self {
            max_size: 64 * 1024 * 1024,
            flush_interval_ms: 1000,
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct TsspConfig {
    pub data_dir: PathBuf,
    pub max_file_size: u64,
    pub compression: CompressionType,
}

impl Default for TsspConfig {
    fn default() -> Self {
        Self {
            data_dir: PathBuf::from("data"),
            max_file_size: 256 * 1024 * 1024,
            compression: CompressionType::Snappy,
        }
    }
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
pub enum CompressionType {
    None,
    Snappy,
    Zstd,
    Lz4,
}

impl CompressionType {
    pub fn extension(&self) -> &'static str {
        match self {
            CompressionType::None => "",
            CompressionType::Snappy => ".snappy",
            CompressionType::Zstd => ".zstd",
            CompressionType::Lz4 => ".lz4",
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct CompactionConfig {
    pub enabled: bool,
    pub max_concurrent: usize,
    pub trigger_interval_ms: u64,
    pub max_file_age_hours: u64,
}

impl Default for CompactionConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_concurrent: 4,
            trigger_interval_ms: 300_000,
            max_file_age_hours: 24,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_wal_config_default() {
        let config = WalConfig::default();
        assert_eq!(config.file_size, 64 * 1024 * 1024);
        assert!(config.sync_enabled);
    }

    #[test]
    fn test_wal_config_custom() {
        let config = WalConfig {
            dir: PathBuf::from("/tmp/wal"),
            file_size: 1024 * 1024,
            sync_enabled: false,
        };
        assert_eq!(config.file_size, 1024 * 1024);
        assert!(!config.sync_enabled);
    }

    #[test]
    fn test_mem_table_config_default() {
        let config = MemTableConfig::default();
        assert_eq!(config.max_size, 64 * 1024 * 1024);
        assert_eq!(config.flush_interval_ms, 1000);
    }

    #[test]
    fn test_mem_table_config_custom() {
        let config = MemTableConfig {
            max_size: 128 * 1024 * 1024,
            flush_interval_ms: 5000,
        };
        assert_eq!(config.max_size, 128 * 1024 * 1024);
        assert_eq!(config.flush_interval_ms, 5000);
    }

    #[test]
    fn test_tssp_config_default() {
        let config = TsspConfig::default();
        assert_eq!(config.max_file_size, 256 * 1024 * 1024);
        assert_eq!(config.compression, CompressionType::Snappy);
    }

    #[test]
    fn test_tssp_config_custom() {
        let config = TsspConfig {
            data_dir: PathBuf::from("/tmp/data"),
            max_file_size: 512 * 1024 * 1024,
            compression: CompressionType::Zstd,
        };
        assert_eq!(config.max_file_size, 512 * 1024 * 1024);
        assert_eq!(config.compression, CompressionType::Zstd);
    }

    #[test]
    fn test_compaction_config_default() {
        let config = CompactionConfig::default();
        assert!(config.enabled);
        assert_eq!(config.max_concurrent, 4);
        assert_eq!(config.trigger_interval_ms, 300_000);
        assert_eq!(config.max_file_age_hours, 24);
    }

    #[test]
    fn test_compaction_config_custom() {
        let config = CompactionConfig {
            enabled: false,
            max_concurrent: 8,
            trigger_interval_ms: 600_000,
            max_file_age_hours: 48,
        };
        assert!(!config.enabled);
        assert_eq!(config.max_concurrent, 8);
        assert_eq!(config.trigger_interval_ms, 600_000);
        assert_eq!(config.max_file_age_hours, 48);
    }

    #[test]
    fn test_compression_type_extension() {
        assert_eq!(CompressionType::None.extension(), "");
        assert_eq!(CompressionType::Snappy.extension(), ".snappy");
        assert_eq!(CompressionType::Zstd.extension(), ".zstd");
        assert_eq!(CompressionType::Lz4.extension(), ".lz4");
    }

    #[test]
    fn test_engine_config() {
        let config = EngineConfig {
            data_dir: PathBuf::from("/tmp/engine"),
            wal: WalConfig::default(),
            memtable: MemTableConfig::default(),
            tssp: TsspConfig::default(),
            compaction: CompactionConfig::default(),
        };
        assert_eq!(config.data_dir, PathBuf::from("/tmp/engine"));
    }
}
