use openGemini_engine::config::TsspConfig;
use openGemini_engine::config::CompressionType;
use std::path::PathBuf;

#[test]
fn test_default_tssp_config() {
    let config = TsspConfig::default();
    assert_eq!(config.data_dir, PathBuf::from("data"));
    assert_eq!(config.max_file_size, 256 * 1024 * 1024);
    assert_eq!(config.compression, CompressionType::Snappy);
}

#[test]
fn test_tssp_config_custom_values() {
    let config = TsspConfig {
        data_dir: PathBuf::from("/custom/data"),
        max_file_size: 128 * 1024 * 1024,
        compression: CompressionType::Zstd,
    };

    assert_eq!(config.data_dir, PathBuf::from("/custom/data"));
    assert_eq!(config.max_file_size, 128 * 1024 * 1024);
    assert_eq!(config.compression, CompressionType::Zstd);
}

#[test]
fn test_compression_type_all() {
    let types = vec![
        (CompressionType::None, ""),
        (CompressionType::Snappy, ".snappy"),
        (CompressionType::Zstd, ".zstd"),
        (CompressionType::Lz4, ".lz4"),
    ];

    for (compression, expected_ext) in types {
        assert_eq!(compression.extension(), expected_ext);
    }
}
