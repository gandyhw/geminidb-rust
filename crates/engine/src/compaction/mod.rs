use crate::config::CompactionConfig;
use crate::error::Result;

pub struct CompactionManager {
    config: CompactionConfig,
}

impl CompactionManager {
    pub fn new(config: CompactionConfig) -> Self {
        Self { config }
    }

    pub fn needs_compaction(&self) -> bool {
        self.config.enabled
    }

    pub fn compact(&self) -> Result<()> {
        Ok(())
    }
}
