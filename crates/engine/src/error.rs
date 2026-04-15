use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("WAL error: {0}")]
    Wal(String),

    #[error("MemTable error: {0}")]
    MemTable(String),

    #[error("TSSP error: {0}")]
    Tssp(String),

    #[error("Compression error: {0}")]
    Compression(String),

    #[error("Index error: {0}")]
    Index(String),

    #[error("Schema error: {0}")]
    Schema(String),

    #[error("Snapshot error: {0}")]
    Snapshot(String),

    #[error("Parse error: {0}")]
    Parse(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Invalid argument: {0}")]
    InvalidArgument(String),

    #[error("Not found: {0}")]
    NotFound(String),
}

pub type Result<T> = std::result::Result<T, Error>;
