use openGemini_engine::config::{CompressionType, Config, EngineConfig, WalConfig, MemTableConfig, TsspConfig, CompactionConfig};
use std::path::PathBuf;

#[test]
fn test_default_wal_config() {
    let config = WalConfig::default();
    assert_eq!(config.dir, PathBuf::from("wal"));
    assert_eq!(config.file_size, 64 * 1024 * 1024);
    assert!(config.sync_enabled);
}

#[test]
fn test_default_memtable_config() {
    let config = MemTableConfig::default();
    assert_eq!(config.max_size, 64 * 1024 * 1024);
    assert_eq!(config.flush_interval_ms, 1000);
}

#[test]
fn test_default_tssp_config() {
    let config = TsspConfig::default();
    assert_eq!(config.data_dir, PathBuf::from("data"));
    assert_eq!(config.max_file_size, 256 * 1024 * 1024);
    assert_eq!(config.compression, CompressionType::Snappy);
}

#[test]
fn test_default_compaction_config() {
    let config = CompactionConfig::default();
    assert!(config.enabled);
    assert_eq!(config.max_concurrent, 4);
    assert_eq!(config.trigger_interval_ms, 300_000);
    assert_eq!(config.max_file_age_hours, 24);
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
    assert_eq!(config.wal.file_size, 64 * 1024 * 1024);
    assert_eq!(config.memtable.max_size, 64 * 1024 * 1024);
}

#[test]
fn test_config_creation() {
    let config = Config {
        engine: EngineConfig {
            data_dir: PathBuf::from("/tmp/test"),
            wal: WalConfig::default(),
            memtable: MemTableConfig::default(),
            tssp: TsspConfig::default(),
            compaction: CompactionConfig::default(),
        },
    };

    assert!(config.engine.data_dir.exists() || !config.engine.data_dir.as_os_str().is_empty());
}
