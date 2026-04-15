use openGemini_engine::config::CompactionConfig;
use openGemini_engine::compaction::{CompactionCandidate, CompactionManager, CompactionResult};
use openGemini_engine::tssp::FileMeta;

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

#[test]
fn test_compaction_manager_new() {
    let config = CompactionConfig::default();
    let manager = CompactionManager::new(config.clone());
    let retrieved_config = manager.get_config();
    assert_eq!(retrieved_config.enabled, config.enabled);
    assert_eq!(retrieved_config.max_concurrent, config.max_concurrent);
}

#[test]
fn test_compaction_manager_needs_compaction_disabled() {
    let config = CompactionConfig {
        enabled: false,
        max_concurrent: 4,
        trigger_interval_ms: 300_000,
        max_file_age_hours: 24,
    };
    let manager = CompactionManager::new(config);
    
    assert!(!manager.needs_compaction(0));
    assert!(!manager.needs_compaction(1));
    assert!(!manager.needs_compaction(2));
    assert!(!manager.needs_compaction(3));
    assert!(!manager.needs_compaction(100));
}

#[test]
fn test_compaction_manager_needs_compaction_below_threshold() {
    let config = CompactionConfig::default();
    let manager = CompactionManager::new(config);
    
    assert!(!manager.needs_compaction(0));
    assert!(!manager.needs_compaction(1));
    assert!(!manager.needs_compaction(2));
}

#[test]
fn test_compaction_manager_needs_compaction_at_threshold() {
    let config = CompactionConfig::default();
    let manager = CompactionManager::new(config);
    
    assert!(manager.needs_compaction(3));
    assert!(manager.needs_compaction(10));
    assert!(manager.needs_compaction(100));
}

#[test]
fn test_compaction_manager_get_config() {
    let config = CompactionConfig::default();
    let manager = CompactionManager::new(config.clone());
    
    let retrieved_config = manager.get_config();
    assert_eq!(retrieved_config.enabled, config.enabled);
    assert_eq!(retrieved_config.max_concurrent, config.max_concurrent);
    assert_eq!(retrieved_config.trigger_interval_ms, config.trigger_interval_ms);
    assert_eq!(retrieved_config.max_file_age_hours, config.max_file_age_hours);
}

#[test]
fn test_compaction_candidate_new_empty() {
    let files: Vec<FileMeta> = Vec::new();
    let candidate = CompactionCandidate::new(files);
    
    assert!(candidate.files.is_empty());
    assert_eq!(candidate.total_size, 0);
    assert_eq!(candidate.time_range, (0, 0));
}

#[test]
fn test_compaction_candidate_new_single_file() {
    let files = vec![FileMeta {
        file_id: 1,
        min_time: 100,
        max_time: 200,
        size: 1024,
        bloom_filter_data: None,
    }];
    let candidate = CompactionCandidate::new(files.clone());
    
    assert_eq!(candidate.files.len(), 1);
    assert_eq!(candidate.total_size, 1024);
    assert_eq!(candidate.time_range, (100, 200));
}

#[test]
fn test_compaction_candidate_new_multiple_files() {
    let files = vec![
        FileMeta {
            file_id: 1,
            min_time: 100,
            max_time: 200,
            size: 1024,
            bloom_filter_data: None,
        },
        FileMeta {
            file_id: 2,
            min_time: 50,
            max_time: 300,
            size: 2048,
            bloom_filter_data: None,
        },
        FileMeta {
            file_id: 3,
            min_time: 150,
            max_time: 250,
            size: 4096,
            bloom_filter_data: None,
        },
    ];
    let candidate = CompactionCandidate::new(files);
    
    assert_eq!(candidate.files.len(), 3);
    assert_eq!(candidate.total_size, 1024 + 2048 + 4096);
    assert_eq!(candidate.time_range, (50, 300));
}

#[test]
fn test_compaction_result_new() {
    let result = CompactionResult::new();
    
    assert!(result.original_files.is_empty());
    assert!(result.compacted_files.is_empty());
    assert_eq!(result.rows_written, 0);
}
