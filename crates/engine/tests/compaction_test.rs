use openGemini_engine::config::CompactionConfig;
use openGemini_engine::compaction::{CompactionCandidate, CompactionManager, CompactionResult, CompactionPriority, CompactionOptions, CompactionLevel};
use openGemini_engine::tssp::FileMeta;

fn create_test_file_meta(file_id: u64, min_time: i64, max_time: i64, size: u64) -> FileMeta {
    FileMeta {
        file_id,
        min_time,
        max_time,
        size,
        bloom_filter_data: None,
    }
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

#[test]
fn test_compaction_candidate_with_priority() {
    let files = vec![create_test_file_meta(1, 100, 200, 1024)];
    let candidate = CompactionCandidate::new(files);

    let high_priority = candidate.with_priority(CompactionPriority::High);
    assert_eq!(high_priority.priority, CompactionPriority::High);
}

#[test]
fn test_compaction_candidate_file_count() {
    let files = vec![
        create_test_file_meta(1, 100, 200, 1024),
        create_test_file_meta(2, 200, 300, 2048),
    ];
    let candidate = CompactionCandidate::new(files);

    assert_eq!(candidate.file_count(), 2);
}

#[test]
fn test_compaction_candidate_is_empty() {
    let empty_candidate = CompactionCandidate::new(Vec::new());
    assert!(empty_candidate.is_empty());

    let files = vec![create_test_file_meta(1, 100, 200, 1024)];
    let non_empty = CompactionCandidate::new(files);
    assert!(!non_empty.is_empty());
}

#[test]
fn test_compaction_candidate_contains_file() {
    let files = vec![
        create_test_file_meta(1, 100, 200, 1024),
        create_test_file_meta(2, 200, 300, 2048),
    ];
    let candidate = CompactionCandidate::new(files);

    assert!(candidate.contains_file(1));
    assert!(candidate.contains_file(2));
    assert!(!candidate.contains_file(3));
}

#[test]
fn test_compaction_candidate_overlap_ratio_no_overlap() {
    let candidate1 = CompactionCandidate::new(vec![
        create_test_file_meta(1, 100, 200, 1024),
    ]);
    let candidate2 = CompactionCandidate::new(vec![
        create_test_file_meta(2, 300, 400, 2048),
    ]);

    assert_eq!(candidate1.overlap_ratio(&candidate2), 0.0);
}

#[test]
fn test_compaction_candidate_overlap_ratio_full_overlap() {
    let candidate1 = CompactionCandidate::new(vec![
        create_test_file_meta(1, 100, 300, 1024),
    ]);
    let candidate2 = CompactionCandidate::new(vec![
        create_test_file_meta(2, 100, 300, 2048),
    ]);

    assert_eq!(candidate1.overlap_ratio(&candidate2), 1.0);
}

#[test]
fn test_compaction_candidate_overlap_ratio_partial_overlap() {
    let candidate1 = CompactionCandidate::new(vec![
        create_test_file_meta(1, 100, 250, 1024),
    ]);
    let candidate2 = CompactionCandidate::new(vec![
        create_test_file_meta(2, 200, 350, 2048),
    ]);

    let ratio = candidate1.overlap_ratio(&candidate2);
    assert!(ratio > 0.0 && ratio < 1.0);
}

#[test]
fn test_compaction_result_builder_pattern() {
    let original = vec![create_test_file_meta(1, 100, 200, 2048)];
    let compacted = vec![create_test_file_meta(2, 100, 200, 1024)];

    let result = CompactionResult::new()
        .with_original_files(original.clone())
        .with_compacted_files(compacted.clone())
        .with_rows_written(100)
        .with_bytes_written(1024)
        .with_duration_ms(50);

    assert_eq!(result.original_files.len(), 1);
    assert_eq!(result.compacted_files.len(), 1);
    assert_eq!(result.rows_written, 100);
    assert_eq!(result.bytes_written, 1024);
    assert_eq!(result.duration_ms, 50);
}

#[test]
fn test_compaction_result_compression_ratio() {
    let original = vec![
        create_test_file_meta(1, 100, 200, 2048),
        create_test_file_meta(2, 200, 300, 2048),
    ];
    let compacted = vec![
        create_test_file_meta(3, 100, 300, 1024),
    ];

    let result = CompactionResult::new()
        .with_original_files(original)
        .with_compacted_files(compacted)
        .with_bytes_written(1024);

    assert_eq!(result.compression_ratio(), 0.25);
}

#[test]
fn test_compaction_result_compression_ratio_zero_bytes() {
    let result = CompactionResult::new().with_bytes_written(0);
    assert_eq!(result.compression_ratio(), 1.0);
}

#[test]
fn test_compaction_result_space_saved() {
    let original = vec![create_test_file_meta(1, 100, 200, 2048)];
    let compacted = vec![create_test_file_meta(2, 100, 200, 512)];

    let result = CompactionResult::new()
        .with_original_files(original)
        .with_compacted_files(compacted);

    assert_eq!(result.space_saved(), 1536);
}

#[test]
fn test_compaction_manager_get_level() {
    let config = CompactionConfig::default();
    let manager = CompactionManager::new(config);

    assert_eq!(manager.get_level(0), CompactionLevel::Level0);
    assert_eq!(manager.get_level(1), CompactionLevel::Level1);
    assert_eq!(manager.get_level(3), CompactionLevel::Level1);
    assert_eq!(manager.get_level(4), CompactionLevel::Level2);
    assert_eq!(manager.get_level(11), CompactionLevel::Level3);
}

#[test]
fn test_compaction_manager_last_compaction_time() {
    let config = CompactionConfig::default();
    let mut manager = CompactionManager::new(config);

    assert!(manager.get_last_compaction_time().is_none());

    manager.set_last_compaction_time(1234567890);
    assert_eq!(manager.get_last_compaction_time(), Some(1234567890));
}

#[test]
fn test_compaction_manager_calculate_output_size() {
    let config = CompactionConfig::default();
    let manager = CompactionManager::new(config);

    let candidates = vec![
        CompactionCandidate::new(vec![create_test_file_meta(1, 100, 200, 1024)]),
        CompactionCandidate::new(vec![create_test_file_meta(2, 200, 300, 2048)]),
    ];

    assert_eq!(manager.calculate_compaction_output_size(&candidates), 3072);
}

#[test]
fn test_compaction_manager_should_compact_file() {
    let config = CompactionConfig::default();
    let manager = CompactionManager::new(config);

    let young_file = create_test_file_meta(1, 100, 200, 1024);
    assert!(!manager.should_compact_file(&young_file, 24));

    let old_file = create_test_file_meta(2, 100, (24 * 3600 + 100) as i64, 2048);
    assert!(manager.should_compact_file(&old_file, 24));
}

#[test]
fn test_compaction_manager_get_priority() {
    let config = CompactionConfig::default();
    let manager = CompactionManager::new(config);

    assert_eq!(manager.get_compaction_priority(3, 100), CompactionPriority::Normal);
    assert_eq!(manager.get_compaction_priority(5, 100), CompactionPriority::Normal);
    assert_eq!(manager.get_compaction_priority(5, 1024 * 1024 * 1024 + 1), CompactionPriority::High);
    assert_eq!(manager.get_compaction_priority(10, 100), CompactionPriority::High);
    assert_eq!(manager.get_compaction_priority(20, 100), CompactionPriority::Critical);
}

#[test]
fn test_compaction_manager_estimate_time() {
    let config = CompactionConfig::default();
    let manager = CompactionManager::new(config);

    let duration = manager.estimate_compaction_time(50 * 1024 * 1024);
    assert_eq!(duration.as_secs(), 1);
}

#[test]
fn test_compaction_level_enum_values() {
    assert_eq!(CompactionLevel::Level0, CompactionLevel::Level0);
    assert_eq!(CompactionLevel::Level1, CompactionLevel::Level1);
    assert_eq!(CompactionLevel::Level2, CompactionLevel::Level2);
    assert_eq!(CompactionLevel::Level3, CompactionLevel::Level3);
}

#[test]
fn test_compaction_options_custom() {
    let options = CompactionOptions {
        max_file_size: 128 * 1024 * 1024,
        max_compact_files: 5,
        priority: CompactionPriority::High,
        force: true,
    };

    assert_eq!(options.max_file_size, 128 * 1024 * 1024);
    assert_eq!(options.max_compact_files, 5);
    assert_eq!(options.priority, CompactionPriority::High);
    assert!(options.force);
}

#[test]
fn test_compaction_priority_compare() {
    assert!(CompactionPriority::Low < CompactionPriority::Normal);
    assert!(CompactionPriority::Normal < CompactionPriority::High);
    assert!(CompactionPriority::High < CompactionPriority::Critical);
    assert!(CompactionPriority::Low <= CompactionPriority::Low);
    assert!(CompactionPriority::Critical >= CompactionPriority::Critical);
}
