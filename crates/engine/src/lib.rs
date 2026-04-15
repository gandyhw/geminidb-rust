pub mod config;
pub mod error;

pub mod wal;
pub mod memtable;
pub mod tssp;
pub mod index;
pub mod compaction;

pub use config::{Config, EngineConfig, WalConfig, MemTableConfig, TsspConfig, CompactionConfig, CompressionType};
pub use error::{Error, Result};
pub use wal::Wal;
pub use memtable::MemTable;
pub use tssp::{TsspReader, TsspWriter, FileMeta};
pub use index::SeriesIndex;
pub use compaction::CompactionManager;

use std::path::PathBuf;

pub struct Engine {
    config: EngineConfig,
    wal: Wal,
    memtable: MemTable,
    tssp_manager: TsspManager,
    compaction: CompactionManager,
}

struct TsspManager {
    data_dir: PathBuf,
}

impl Engine {
    pub fn new(config: EngineConfig) -> Result<Self> {
        let tssp_data_dir = config.tssp.data_dir.clone();
        std::fs::create_dir_all(&tssp_data_dir).map_err(|e| Error::Tssp(e.to_string()))?;
        
        let wal = Wal::new(&config.wal)?;
        let memtable = MemTable::new(config.memtable.max_size);
        let tssp_manager = TsspManager {
            data_dir: tssp_data_dir,
        };
        let compaction = CompactionManager::new(config.compaction.clone());

        Ok(Self {
            config,
            wal,
            memtable,
            tssp_manager,
            compaction,
        })
    }

    pub fn write(&mut self, batch: WriteBatch) -> Result<()> {
        self.wal.write(&batch)?;
        self.memtable.insert(batch)?;
        Ok(())
    }

    pub fn read(&self, query: Query) -> Result<QueryResult> {
        let rows = self.memtable.scan(
            query.table.as_bytes(),
            query.time_range.start,
            query.time_range.end,
        )?;

        let row_count = rows.len();
        let results: Vec<Row> = rows.into_iter().map(|(_, v)| {
            let tags: std::collections::HashMap<String, String> = serde_json::from_slice(&v.tags).unwrap_or_default();
            let fields: std::collections::HashMap<String, FieldValue> = serde_json::from_slice(&v.fields).unwrap_or_default();
            Row { tags, fields, timestamp: 0 }
        }).collect();

        Ok(QueryResult {
            rows: results,
            stats: QueryStats {
                files_read: 0,
                rows_scanned: row_count,
                bytes_read: 0,
                execution_time_ms: 0,
            },
        })
    }

    pub fn flush(&mut self) -> Result<()> {
        if self.memtable.should_flush() {
            let rows = self.memtable.flush()?;
            if !rows.is_empty() {
                let _meta = self.tssp_manager.write_rows(rows)?;
            }
        }
        Ok(())
    }

    pub fn close(&mut self) -> Result<()> {
        self.flush()?;
        self.wal.close()?;
        Ok(())
    }
}

impl TsspManager {
    fn write_rows(&self, _rows: Vec<Row>) -> Result<FileMeta> {
        Ok(FileMeta {
            file_id: 0,
            min_time: 0,
            max_time: 0,
            size: 0,
        })
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct WriteBatch {
    pub database: String,
    pub table: String,
    pub rows: Vec<Row>,
    pub timestamp: i64,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Row {
    pub tags: std::collections::HashMap<String, String>,
    pub fields: std::collections::HashMap<String, FieldValue>,
    pub timestamp: i64,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum FieldValue {
    Integer(i64),
    Float(f64),
    String(Vec<u8>),
    Boolean(bool),
    Unsigned(u64),
}

#[derive(Debug, Clone)]
pub struct Query {
    pub database: String,
    pub table: String,
    pub time_range: TimeRange,
    pub columns: Vec<String>,
    pub filter: Option<FilterExpr>,
    pub limit: Option<usize>,
}

#[derive(Debug, Clone)]
pub struct TimeRange {
    pub start: i64,
    pub end: i64,
}

#[derive(Debug, Clone)]
pub enum FilterExpr {
    And(Box<FilterExpr>, Box<FilterExpr>),
    Or(Box<FilterExpr>, Box<FilterExpr>),
    Eq(String, FieldValue),
    Ne(String, FieldValue),
    Gt(String, FieldValue),
    Lt(String, FieldValue),
}

pub struct QueryResult {
    pub rows: Vec<Row>,
    pub stats: QueryStats,
}

#[derive(Debug, Default)]
pub struct QueryStats {
    pub files_read: usize,
    pub rows_scanned: usize,
    pub bytes_read: usize,
    pub execution_time_ms: u64,
}
