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
