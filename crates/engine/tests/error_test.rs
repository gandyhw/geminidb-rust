use openGemini_engine::error::{Error, Result};

#[test]
fn test_error_display() {
    let err = Error::Wal("test wal error".to_string());
    assert_eq!(format!("{}", err), "WAL error: test wal error");

    let err = Error::MemTable("test memtable error".to_string());
    assert_eq!(format!("{}", err), "MemTable error: test memtable error");

    let err = Error::Tssp("test tssp error".to_string());
    assert_eq!(format!("{}", err), "TSSP error: test tssp error");

    let err = Error::Compression("test compression error".to_string());
    assert_eq!(format!("{}", err), "Compression error: test compression error");
}

#[test]
fn test_error_io() {
    use std::io;
    let io_err = io::Error::new(io::ErrorKind::NotFound, "file not found");
    let err = Error::Io(io_err);
    assert!(format!("{}", err).contains("file not found"));
}

#[test]
fn test_error_invalid_argument() {
    let err = Error::InvalidArgument("invalid config".to_string());
    assert_eq!(format!("{}", err), "Invalid argument: invalid config");
}

#[test]
fn test_error_not_found() {
    let err = Error::NotFound("key not found".to_string());
    assert_eq!(format!("{}", err), "Not found: key not found");
}

#[test]
fn test_result_ok() {
    let result: Result<i32> = Ok(42);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), 42);
}

#[test]
fn test_result_err() {
    let result: Result<i32> = Err(Error::Wal("error".to_string()));
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("WAL error"));
}
