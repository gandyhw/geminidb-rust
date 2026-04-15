use crate::config::CompactionConfig;
use crate::FileMeta;

pub struct CompactionManager {
    config: CompactionConfig,
}

impl CompactionManager {
    pub fn new(config: CompactionConfig) -> Self {
        Self { config }
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
}

pub struct CompactionCandidate {
    pub files: Vec<FileMeta>,
    pub total_size: u64,
    pub time_range: (i64, i64),
}

impl CompactionCandidate {
    pub fn new(files: Vec<FileMeta>) -> Self {
        let total_size = files.iter().map(|f| f.size).sum();
        let min_time = files.iter().map(|f| f.min_time).min().unwrap_or(0);
        let max_time = files.iter().map(|f| f.max_time).max().unwrap_or(0);
        
        Self {
            files,
            total_size,
            time_range: (min_time, max_time),
        }
    }
}

pub struct CompactionResult {
    pub original_files: Vec<FileMeta>,
    pub compacted_files: Vec<FileMeta>,
    pub rows_written: usize,
}

impl CompactionResult {
    pub fn new() -> Self {
        Self {
            original_files: Vec::new(),
            compacted_files: Vec::new(),
            rows_written: 0,
        }
    }
}