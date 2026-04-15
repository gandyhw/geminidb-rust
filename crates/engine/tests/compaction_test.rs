use openGemini_engine::config::CompactionConfig;

#[test]
fn test_default_compaction_config() {
    let config = CompactionConfig::default();
    assert!(config.enabled);
    assert_eq!(config.max_concurrent, 4);
    assert_eq!(config.trigger_interval_ms, 300_000);
    assert_eq!(config.max_file_age_hours, 24);
}

#[test]
fn test_compaction_config_disabled() {
    let config = CompactionConfig {
        enabled: false,
        max_concurrent: 2,
        trigger_interval_ms: 60000,
        max_file_age_hours: 48,
    };

    assert!(!config.enabled);
    assert_eq!(config.max_concurrent, 2);
}
