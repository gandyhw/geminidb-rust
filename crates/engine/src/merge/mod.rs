use crate::config::CompactionConfig;
use crate::error::{Error, Result};
use crate::tssp::{FileMeta, TsspReader, TsspWriter};
use crate::TsspConfig;
use std::collections::HashMap;
use std::path::PathBuf;

#[derive(Debug, Clone)]
pub struct MergeCandidate {
    pub files: Vec<FileMeta>,
    pub output_path: PathBuf,
}

impl MergeCandidate {
    pub fn new(files: Vec<FileMeta>, output_path: PathBuf) -> Self {
        Self { files, output_path }
    }

    pub fn is_valid(&self) -> bool {
        !self.files.is_empty() && self.files.len() >= 2
    }

    pub fn total_size(&self) -> u64 {
        self.files.iter().map(|f| f.size).sum()
    }

    pub fn time_range(&self) -> (i64, i64) {
        let min = self.files.iter().map(|f| f.min_time).min().unwrap_or(0);
        let max = self.files.iter().map(|f| f.max_time).max().unwrap_or(0);
        (min, max)
    }
}

pub struct MergeResult {
    pub input_files: Vec<FileMeta>,
    pub output_file: Option<FileMeta>,
    pub rows_merged: usize,
    pub bytes_written: u64,
}

impl MergeResult {
    pub fn new() -> Self {
        Self {
            input_files: Vec::new(),
            output_file: None,
            rows_merged: 0,
            bytes_written: 0,
        }
    }

    pub fn with_output(mut self, file: FileMeta) -> Self {
        self.output_file = Some(file);
        self
    }
}

impl Default for MergeResult {
    fn default() -> Self {
        Self::new()
    }
}

pub struct MergeTool {
    config: TsspConfig,
}

impl MergeTool {
    pub fn new(config: TsspConfig) -> Self {
        Self { config }
    }

    pub fn merge(&mut self, candidates: Vec<MergeCandidate>) -> Result<Vec<MergeResult>> {
        let mut results = Vec::new();

        for candidate in candidates {
            if !candidate.is_valid() {
                continue;
            }

            let result = self.merge_single_candidate(&candidate)?;
            results.push(result);
        }

        Ok(results)
    }

    fn merge_single_candidate(&mut self, candidate: &MergeCandidate) -> Result<MergeResult> {
        let mut result = MergeResult::new();
        result.input_files = candidate.files.clone();

        let mut writer = TsspWriter::new(self.config.clone())?;

        for file in &candidate.files {
            result.rows_merged += 1;
            result.bytes_written += file.size;
        }

        let output_meta = writer.close()?;
        result.output_file = Some(output_meta);

        Ok(result)
    }

    pub fn select_merge_candidates(
        &self,
        files: &[FileMeta],
        max_files: usize,
    ) -> Vec<MergeCandidate> {
        if files.len() < 2 {
            return Vec::new();
        }

        let mut candidates = Vec::new();
        let mut sorted_files = files.to_vec();
        sorted_files.sort_by_key(|f| f.min_time);

        let mut i = 0;
        while i <= sorted_files.len() - 2 {
            let batch_size = (max_files as usize).min(sorted_files.len() - i);
            let batch: Vec<FileMeta> = sorted_files[i..i + batch_size].to_vec();
            
            candidates.push(MergeCandidate::new(batch, self.config.data_dir.clone()));
            i += batch_size;
        }

        candidates
    }
}

pub struct MergeScheduler {
    enabled: bool,
    max_concurrent: usize,
    in_progress: HashMap<u64, MergeResult>,
}

impl MergeScheduler {
    pub fn new(config: &CompactionConfig) -> Self {
        Self {
            enabled: config.enabled,
            max_concurrent: config.max_concurrent,
            in_progress: HashMap::new(),
        }
    }

    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    pub fn max_concurrent(&self) -> usize {
        self.max_concurrent
    }

    pub fn can_start_merge(&self) -> bool {
        self.in_progress.len() < self.max_concurrent
    }

    pub fn start_merge(&mut self, shard_id: u64) -> bool {
        if !self.can_start_merge() {
            return false;
        }
        self.in_progress.insert(shard_id, MergeResult::new());
        true
    }

    pub fn complete_merge(&mut self, shard_id: u64) -> Option<MergeResult> {
        self.in_progress.remove(&shard_id)
    }

    pub fn cancel_merge(&mut self, shard_id: u64) -> bool {
        self.in_progress.remove(&shard_id).is_some()
    }

    pub fn in_progress_count(&self) -> usize {
        self.in_progress.len()
    }

    pub fn get_merge_status(&self, shard_id: u64) -> Option<&MergeResult> {
        self.in_progress.get(&shard_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_file_meta(id: u64, min_time: i64, max_time: i64, size: u64) -> FileMeta {
        FileMeta {
            file_id: id,
            min_time,
            max_time,
            size,
            bloom_filter_data: None,
        }
    }

    #[test]
    fn test_merge_candidate_new() {
        let files = vec![
            create_test_file_meta(1, 100, 200, 1024),
            create_test_file_meta(2, 150, 250, 2048),
        ];
        let path = PathBuf::from("/data");
        let candidate = MergeCandidate::new(files, path);
        
        assert!(candidate.is_valid());
        assert_eq!(candidate.total_size(), 3072);
        assert_eq!(candidate.time_range(), (100, 250));
    }

    #[test]
    fn test_merge_candidate_invalid() {
        let files = vec![create_test_file_meta(1, 100, 200, 1024)];
        let path = PathBuf::from("/data");
        let candidate = MergeCandidate::new(files, path);
        
        assert!(!candidate.is_valid());
    }

    #[test]
    fn test_merge_result_new() {
        let result = MergeResult::new();
        assert!(result.input_files.is_empty());
        assert!(result.output_file.is_none());
        assert_eq!(result.rows_merged, 0);
    }

    #[test]
    fn test_merge_result_with_output() {
        let meta = create_test_file_meta(100, 100, 200, 4096);
        let result = MergeResult::new().with_output(meta.clone());
        
        assert!(result.output_file.is_some());
        assert_eq!(result.output_file.unwrap().file_id, 100);
    }

    #[test]
    fn test_merge_scheduler_new() {
        let config = CompactionConfig {
            enabled: true,
            max_concurrent: 4,
            trigger_interval_ms: 300000,
            max_file_age_hours: 24,
        };
        let scheduler = MergeScheduler::new(&config);
        
        assert!(scheduler.is_enabled());
        assert_eq!(scheduler.max_concurrent(), 4);
        assert!(scheduler.can_start_merge());
        assert_eq!(scheduler.in_progress_count(), 0);
    }

    #[test]
    fn test_merge_scheduler_disabled() {
        let config = CompactionConfig {
            enabled: false,
            max_concurrent: 4,
            trigger_interval_ms: 300000,
            max_file_age_hours: 24,
        };
        let scheduler = MergeScheduler::new(&config);
        
        assert!(!scheduler.is_enabled());
    }

    #[test]
    fn test_merge_scheduler_start_complete() {
        let config = CompactionConfig::default();
        let mut scheduler = MergeScheduler::new(&config);
        
        assert!(scheduler.start_merge(1));
        assert!(scheduler.start_merge(2));
        assert_eq!(scheduler.in_progress_count(), 2);
        
        let result = scheduler.complete_merge(1);
        assert!(result.is_some());
        assert_eq!(scheduler.in_progress_count(), 1);
    }

    #[test]
    fn test_merge_scheduler_max_concurrent() {
        let config = CompactionConfig {
            enabled: true,
            max_concurrent: 2,
            trigger_interval_ms: 300000,
            max_file_age_hours: 24,
        };
        let mut scheduler = MergeScheduler::new(&config);
        
        assert!(scheduler.start_merge(1));
        assert!(scheduler.start_merge(2));
        assert!(!scheduler.can_start_merge());
        assert!(!scheduler.start_merge(3));
    }

    #[test]
    fn test_merge_scheduler_cancel() {
        let config = CompactionConfig::default();
        let mut scheduler = MergeScheduler::new(&config);
        
        assert!(scheduler.start_merge(1));
        assert!(scheduler.cancel_merge(1));
        assert!(!scheduler.cancel_merge(999));
        assert!(scheduler.can_start_merge());
    }

    #[test]
    fn test_merge_tool_select_candidates() {
        let config = TsspConfig {
            data_dir: PathBuf::from("/data"),
            max_file_size: 256 * 1024 * 1024,
            compression: crate::config::CompressionType::Snappy,
        };
        let tool = MergeTool::new(config);
        
        let files = vec![
            create_test_file_meta(1, 100, 200, 1024),
            create_test_file_meta(2, 200, 300, 2048),
            create_test_file_meta(3, 300, 400, 3072),
        ];
        
        let candidates = tool.select_merge_candidates(&files, 2);
        
        assert!(!candidates.is_empty());
    }

    #[test]
    fn test_merge_tool_select_candidates_single_file() {
        let config = TsspConfig {
            data_dir: PathBuf::from("/data"),
            max_file_size: 256 * 1024 * 1024,
            compression: crate::config::CompressionType::Snappy,
        };
        let tool = MergeTool::new(config);
        
        let files = vec![create_test_file_meta(1, 100, 200, 1024)];
        
        let candidates = tool.select_merge_candidates(&files, 2);
        
        assert!(candidates.is_empty());
    }
}
