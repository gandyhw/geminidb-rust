use crate::config::CompactionConfig;
use crate::FileMeta;
use std::cmp::Ordering;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompactionLevel {
    Level0,
    Level1,
    Level2,
    Level3,
}

impl CompactionLevel {
    pub fn from_file_count(count: usize) -> Self {
        match count {
            0 => CompactionLevel::Level0,
            1..=3 => CompactionLevel::Level1,
            4..=10 => CompactionLevel::Level2,
            _ => CompactionLevel::Level3,
        }
    }
}

#[derive(Debug, Clone)]
pub struct CompactionOptions {
    pub max_file_size: u64,
    pub max_compact_files: usize,
    pub priority: CompactionPriority,
    pub force: bool,
}

impl Default for CompactionOptions {
    fn default() -> Self {
        Self {
            max_file_size: 256 * 1024 * 1024,
            max_compact_files: 10,
            priority: CompactionPriority::Normal,
            force: false,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum CompactionPriority {
    Low = 0,
    Normal = 1,
    High = 2,
    Critical = 3,
}

pub struct CompactionManager {
    config: CompactionConfig,
    active_compactions: usize,
    last_compaction_time: Option<i64>,
}

impl CompactionManager {
    pub fn new(config: CompactionConfig) -> Self {
        Self {
            config,
            active_compactions: 0,
            last_compaction_time: None,
        }
    }

    pub fn needs_compaction(&self, file_count: usize) -> bool {
        if !self.config.enabled {
            return false;
        }
        file_count >= 3
    }

    pub fn get_config(&self) -> &CompactionConfig {
        &self.config
    }

    pub fn get_level(&self, file_count: usize) -> CompactionLevel {
        CompactionLevel::from_file_count(file_count)
    }

    pub fn is_compaction_running(&self) -> bool {
        self.active_compactions >= self.config.max_concurrent
    }

    pub fn start_compaction(&mut self) -> bool {
        if self.is_compaction_running() {
            return false;
        }
        self.active_compactions += 1;
        true
    }

    pub fn end_compaction(&mut self) {
        if self.active_compactions > 0 {
            self.active_compactions -= 1;
        }
    }

    pub fn get_active_compaction_count(&self) -> usize {
        self.active_compactions
    }

    pub fn get_last_compaction_time(&self) -> Option<i64> {
        self.last_compaction_time
    }

    pub fn set_last_compaction_time(&mut self, time: i64) {
        self.last_compaction_time = Some(time);
    }

    pub fn select_compaction_candidates(
        &self,
        files: &[FileMeta],
        options: &CompactionOptions,
    ) -> Vec<CompactionCandidate> {
        if files.is_empty() {
            return Vec::new();
        }

        let mut sorted_files = files.to_vec();
        sorted_files.sort_by(|a, b| {
            let size_cmp = b.size.cmp(&a.size);
            if size_cmp != Ordering::Equal {
                return size_cmp;
            }
            a.min_time.cmp(&b.min_time)
        });

        let mut candidates = Vec::new();
        let mut current_batch = Vec::new();

        for file in sorted_files {
            if current_batch.len() >= options.max_compact_files {
                candidates.push(CompactionCandidate::new(current_batch));
                current_batch = Vec::new();
            }

            current_batch.push(file);
        }

        if !current_batch.is_empty() {
            candidates.push(CompactionCandidate::new(current_batch));
        }

        candidates
    }

    pub fn calculate_compaction_output_size(
        &self,
        candidates: &[CompactionCandidate],
    ) -> u64 {
        candidates.iter().map(|c| c.total_size).sum()
    }

    pub fn should_compact_file(&self, file: &FileMeta, max_age_hours: u64) -> bool {
        let file_age_hours = file.max_time as u64 / 3600;
        file_age_hours >= max_age_hours
    }

    pub fn get_compaction_priority(&self, file_count: usize, total_size: u64) -> CompactionPriority {
        match file_count {
            f if f >= 20 => CompactionPriority::Critical,
            f if f >= 10 => CompactionPriority::High,
            f if f >= 5 && total_size > 1024 * 1024 * 1024 => CompactionPriority::High,
            _ => CompactionPriority::Normal,
        }
    }

    pub fn estimate_compaction_time(&self, total_size: u64) -> std::time::Duration {
        let throughput = 50 * 1024 * 1024;
        std::time::Duration::from_secs(total_size / throughput)
    }
}

pub struct CompactionCandidate {
    pub files: Vec<FileMeta>,
    pub total_size: u64,
    pub time_range: (i64, i64),
    pub level: CompactionLevel,
    pub priority: CompactionPriority,
}

impl CompactionCandidate {
    pub fn new(files: Vec<FileMeta>) -> Self {
        let total_size = files.iter().map(|f| f.size).sum();
        let min_time = files.iter().map(|f| f.min_time).min().unwrap_or(0);
        let max_time = files.iter().map(|f| f.max_time).max().unwrap_or(0);
        let level = CompactionLevel::from_file_count(files.len());

        Self {
            files,
            total_size,
            time_range: (min_time, max_time),
            level,
            priority: CompactionPriority::Normal,
        }
    }

    pub fn with_priority(mut self, priority: CompactionPriority) -> Self {
        self.priority = priority;
        self
    }

    pub fn file_count(&self) -> usize {
        self.files.len()
    }

    pub fn is_empty(&self) -> bool {
        self.files.is_empty()
    }

    pub fn contains_file(&self, file_id: u64) -> bool {
        self.files.iter().any(|f| f.file_id == file_id)
    }

    pub fn overlap_ratio(&self, other: &CompactionCandidate) -> f64 {
        let self_min = self.time_range.0;
        let self_max = self.time_range.1;
        let other_min = other.time_range.0;
        let other_max = other.time_range.1;

        let overlap_start = self_min.max(other_min);
        let overlap_end = self_max.min(other_max);

        if overlap_start >= overlap_end {
            return 0.0;
        }

        let overlap_size = overlap_end - overlap_start;
        let self_size = self_max - self_min;
        let other_size = other_max - other_min;

        let min_size = self_size.min(other_size);
        if min_size == 0 {
            return 0.0;
        }

        overlap_size as f64 / min_size as f64
    }
}

pub struct CompactionResult {
    pub original_files: Vec<FileMeta>,
    pub compacted_files: Vec<FileMeta>,
    pub rows_written: usize,
    pub bytes_written: u64,
    pub duration_ms: u64,
}

impl CompactionResult {
    pub fn new() -> Self {
        Self {
            original_files: Vec::new(),
            compacted_files: Vec::new(),
            rows_written: 0,
            bytes_written: 0,
            duration_ms: 0,
        }
    }

    pub fn with_original_files(mut self, files: Vec<FileMeta>) -> Self {
        self.original_files = files;
        self
    }

    pub fn with_compacted_files(mut self, files: Vec<FileMeta>) -> Self {
        self.compacted_files = files;
        self
    }

    pub fn with_rows_written(mut self, rows: usize) -> Self {
        self.rows_written = rows;
        self
    }

    pub fn with_bytes_written(mut self, bytes: u64) -> Self {
        self.bytes_written = bytes;
        self
    }

    pub fn with_duration_ms(mut self, ms: u64) -> Self {
        self.duration_ms = ms;
        self
    }

    pub fn compression_ratio(&self) -> f64 {
        if self.bytes_written == 0 {
            return 1.0;
        }
        let original_size: u64 = self.original_files.iter().map(|f| f.size).sum();
        if original_size == 0 {
            return 1.0;
        }
        self.bytes_written as f64 / original_size as f64
    }

    pub fn space_saved(&self) -> i64 {
        let original_size: i64 = self.original_files.iter().map(|f| f.size as i64).sum();
        let compacted_size = self.compacted_files.iter().map(|f| f.size as i64).sum::<i64>();
        original_size - compacted_size
    }
}

impl Default for CompactionResult {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
    fn test_compaction_level_from_file_count() {
        assert_eq!(CompactionLevel::from_file_count(0), CompactionLevel::Level0);
        assert_eq!(CompactionLevel::from_file_count(1), CompactionLevel::Level1);
        assert_eq!(CompactionLevel::from_file_count(3), CompactionLevel::Level1);
        assert_eq!(CompactionLevel::from_file_count(4), CompactionLevel::Level2);
        assert_eq!(CompactionLevel::from_file_count(10), CompactionLevel::Level2);
        assert_eq!(CompactionLevel::from_file_count(11), CompactionLevel::Level3);
    }

    #[test]
    fn test_compaction_options_default() {
        let options = CompactionOptions::default();
        assert_eq!(options.max_file_size, 256 * 1024 * 1024);
        assert_eq!(options.max_compact_files, 10);
        assert_eq!(options.priority, CompactionPriority::Normal);
        assert!(!options.force);
    }

    #[test]
    fn test_compaction_priority_ordering() {
        assert!(CompactionPriority::Low < CompactionPriority::Normal);
        assert!(CompactionPriority::Normal < CompactionPriority::High);
        assert!(CompactionPriority::High < CompactionPriority::Critical);
    }

    #[test]
    fn test_manager_start_end_compaction() {
        let config = CompactionConfig::default();
        let mut manager = CompactionManager::new(config);

        assert!(!manager.is_compaction_running());
        assert_eq!(manager.get_active_compaction_count(), 0);

        assert!(manager.start_compaction());
        assert!(!manager.is_compaction_running());
        assert_eq!(manager.get_active_compaction_count(), 1);

        manager.end_compaction();
        assert!(!manager.is_compaction_running());
        assert_eq!(manager.get_active_compaction_count(), 0);
    }

    #[test]
    fn test_manager_max_concurrent_limit() {
        let mut config = CompactionConfig::default();
        config.max_concurrent = 2;
        let mut manager = CompactionManager::new(config);

        assert!(manager.start_compaction());
        assert!(manager.start_compaction());
        assert!(!manager.start_compaction());
        assert_eq!(manager.get_active_compaction_count(), 2);
    }

    #[test]
    fn test_select_compaction_candidates() {
        let config = CompactionConfig::default();
        let manager = CompactionManager::new(config);

        let files = vec![
            create_test_file_meta(1, 100, 200, 1024),
            create_test_file_meta(2, 200, 300, 2048),
            create_test_file_meta(3, 300, 400, 4096),
        ];

        let options = CompactionOptions {
            max_compact_files: 2,
            ..Default::default()
        };

        let candidates = manager.select_compaction_candidates(&files, &options);
        assert!(!candidates.is_empty());
    }

    #[test]
    fn test_candidate_with_priority() {
        let files = vec![create_test_file_meta(1, 100, 200, 1024)];
        let candidate = CompactionCandidate::new(files);
        assert_eq!(candidate.priority, CompactionPriority::Normal);

        let candidate = candidate.with_priority(CompactionPriority::High);
        assert_eq!(candidate.priority, CompactionPriority::High);
    }

    #[test]
    fn test_candidate_contains_file() {
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
    fn test_candidate_overlap_ratio() {
        let candidate1 = CompactionCandidate::new(vec![
            create_test_file_meta(1, 100, 300, 1024),
        ]);
        let candidate2 = CompactionCandidate::new(vec![
            create_test_file_meta(2, 200, 400, 2048),
        ]);

        let ratio = candidate1.overlap_ratio(&candidate2);
        assert!(ratio > 0.0);
        assert!(ratio <= 1.0);
    }

    #[test]
    fn test_candidate_no_overlap() {
        let candidate1 = CompactionCandidate::new(vec![
            create_test_file_meta(1, 100, 200, 1024),
        ]);
        let candidate2 = CompactionCandidate::new(vec![
            create_test_file_meta(2, 300, 400, 2048),
        ]);

        let ratio = candidate1.overlap_ratio(&candidate2);
        assert_eq!(ratio, 0.0);
    }

    #[test]
    fn test_compaction_result_compression_ratio() {
        let mut result = CompactionResult::new();
        result.original_files = vec![create_test_file_meta(1, 100, 200, 1000)];
        result.bytes_written = 500;

        assert_eq!(result.compression_ratio(), 0.5);
    }

    #[test]
    fn test_compaction_result_space_saved() {
        let mut result = CompactionResult::new();
        result.original_files = vec![
            create_test_file_meta(1, 100, 200, 1000),
            create_test_file_meta(2, 200, 300, 2000),
        ];
        result.compacted_files = vec![
            create_test_file_meta(3, 100, 300, 1500),
        ];

        assert_eq!(result.space_saved(), 1500);
    }

    #[test]
    fn test_should_compact_file() {
        let config = CompactionConfig::default();
        let manager = CompactionManager::new(config);

        let file = create_test_file_meta(1, 100, 200, 1024);
        assert!(!manager.should_compact_file(&file, 24));

        let old_file = create_test_file_meta(1, 0, 100000, 1024);
        assert!(manager.should_compact_file(&old_file, 24));
    }

    #[test]
    fn test_get_compaction_priority() {
        let config = CompactionConfig::default();
        let manager = CompactionManager::new(config);

        assert_eq!(manager.get_compaction_priority(5, 1024), CompactionPriority::Normal);
        assert_eq!(manager.get_compaction_priority(10, 1024), CompactionPriority::High);
        assert_eq!(manager.get_compaction_priority(20, 1024), CompactionPriority::Critical);
    }

    #[test]
    fn test_compaction_config_default() {
        let config = CompactionConfig::default();
        assert_eq!(config.max_concurrent, 4);
        assert!(config.enabled);
        assert_eq!(config.trigger_interval_ms, 300_000);
        assert_eq!(config.max_file_age_hours, 24);
    }

    #[test]
    fn test_compaction_config_custom() {
        let mut config = CompactionConfig::default();
        config.max_concurrent = 8;
        config.enabled = false;

        assert_eq!(config.max_concurrent, 8);
        assert!(!config.enabled);
    }

    #[test]
    fn test_select_candidates_with_empty_list() {
        let config = CompactionConfig::default();
        let manager = CompactionManager::new(config);

        let candidates = manager.select_compaction_candidates(&[], &CompactionOptions::default());
        assert!(candidates.is_empty());
    }

    #[test]
    fn test_select_candidates_with_single_file() {
        let config = CompactionConfig::default();
        let manager = CompactionManager::new(config);

        let files = vec![
            create_test_file_meta(1, 100, 200, 1024),
        ];

        let candidates = manager.select_compaction_candidates(&files, &CompactionOptions::default());
        assert_eq!(candidates.len(), 1);
    }

    #[test]
    fn test_select_candidates_with_many_files() {
        let config = CompactionConfig::default();
        let manager = CompactionManager::new(config);

        let files = vec![
            create_test_file_meta(1, 100, 200, 1024),
            create_test_file_meta(2, 200, 300, 1024),
            create_test_file_meta(3, 300, 400, 1024),
            create_test_file_meta(4, 400, 500, 1024),
            create_test_file_meta(5, 500, 600, 1024),
        ];

        let options = CompactionOptions {
            max_compact_files: 3,
            ..Default::default()
        };

        let candidates = manager.select_compaction_candidates(&files, &options);
        assert!(!candidates.is_empty());
    }

    #[test]
    fn test_candidate_multiple_files_time_overlap() {
        let files = vec![
            create_test_file_meta(1, 100, 300, 1024),
            create_test_file_meta(2, 200, 400, 2048),
            create_test_file_meta(3, 350, 500, 4096),
        ];
        let candidate = CompactionCandidate::new(files);

        assert_eq!(candidate.file_count(), 3);
        assert!(candidate.total_size > 0);
    }

    #[test]
    fn test_candidate_time_range() {
        let files = vec![
            create_test_file_meta(1, 100, 200, 1024),
            create_test_file_meta(2, 300, 400, 2048),
        ];
        let candidate = CompactionCandidate::new(files);

        assert_eq!(candidate.time_range.0, 100);
        assert_eq!(candidate.time_range.1, 400);
    }

    #[test]
    fn test_compaction_result_zero_values() {
        let result = CompactionResult::new();
        assert_eq!(result.compression_ratio(), 1.0);
        assert_eq!(result.space_saved(), 0);
    }

    #[test]
    fn test_compaction_result_unchanged() {
        let mut result = CompactionResult::new();
        result.original_files = vec![
            create_test_file_meta(1, 100, 200, 1000),
        ];
        result.compacted_files = vec![
            create_test_file_meta(2, 100, 200, 1000),
        ];
        result.bytes_written = 1000;

        assert_eq!(result.compression_ratio(), 1.0);
        assert_eq!(result.space_saved(), 0);
    }

    #[test]
    fn test_compaction_result_multiple_files() {
        let mut result = CompactionResult::new();
        result.original_files = vec![
            create_test_file_meta(1, 100, 200, 1000),
            create_test_file_meta(2, 200, 300, 2000),
            create_test_file_meta(3, 300, 400, 3000),
        ];
        result.compacted_files = vec![
            create_test_file_meta(4, 100, 400, 1500),
        ];
        result.bytes_written = 1500;

        assert_eq!(result.space_saved(), 4500);
    }

    #[test]
    fn test_compaction_options_with_force() {
        let options = CompactionOptions {
            force: true,
            priority: CompactionPriority::High,
            max_compact_files: 5,
            max_file_size: 128 * 1024 * 1024,
        };

        assert!(options.force);
        assert_eq!(options.priority, CompactionPriority::High);
        assert_eq!(options.max_compact_files, 5);
    }
}