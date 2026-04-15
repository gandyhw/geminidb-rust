use crate::config::TsspConfig;
use crate::error::Result;
use std::path::PathBuf;

pub struct TsspWriter {
    config: TsspConfig,
}

pub struct TsspReader {
    config: TsspConfig,
}

#[derive(Debug, Clone)]
pub struct FileMeta {
    pub file_id: u64,
    pub min_time: i64,
    pub max_time: i64,
    pub size: u64,
}

impl TsspWriter {
    pub fn new(config: TsspConfig) -> Self {
        Self { config }
    }

    pub fn write(&mut self, _data: &[u8]) -> Result<FileMeta> {
        Ok(FileMeta {
            file_id: 0,
            min_time: 0,
            max_time: 0,
            size: 0,
        })
    }

    pub fn close(self) -> Result<FileMeta> {
        Ok(FileMeta {
            file_id: 0,
            min_time: 0,
            max_time: 0,
            size: 0,
        })
    }
}

impl TsspReader {
    pub fn new(config: TsspConfig) -> Self {
        Self { config }
    }

    pub fn read(&self, _file_meta: &FileMeta) -> Result<Vec<u8>> {
        Ok(vec![])
    }
}
