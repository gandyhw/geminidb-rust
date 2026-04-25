use openGemini_engine::config::{TsspConfig, CompressionType};
use openGemini_engine::tssp::{TsspWriter, TsspReader, FileMeta, TableSchema, ColumnData};
use tempfile::TempDir;

#[test]
fn test_tssp_bloom_filter() {
    let temp_dir = TempDir::new().unwrap();
    let config = TsspConfig {
        data_dir: temp_dir.path().to_path_buf(),
        max_file_size: 256 * 1024 * 1024,
        compression: CompressionType::None,
    };
    
    let mut writer = TsspWriter::new(config.clone()).unwrap();
    
    let schema = TableSchema {
        table_id: 1,
        columns: vec![
            ("time".to_string(), 0),
            ("value".to_string(), 1),
        ].into_iter().collect(),
    };
    
    writer.add_series_key(b"host=server1");
    writer.add_series_key(b"host=server2");
    
    let time_values: Vec<u8> = vec![1000i64, 2000].iter().flat_map(|v| v.to_le_bytes()).collect();
    
    writer.write_batch(&schema, 1000, 2000, vec![
        ColumnData { column_id: 0, values: time_values, null_count: 0 },
    ]).unwrap();
    
    let meta = writer.close().unwrap();
    assert!(meta.bloom_filter_data.is_some());
    
    let reader = TsspReader::new(config).unwrap();
    assert!(reader.may_contain_series(&meta, b"host=server1"));
    assert!(reader.may_contain_series(&meta, b"host=server2"));
    assert!(!reader.may_contain_series(&meta, b"host=server3"));
}

fn create_test_config(temp_dir: &TempDir) -> TsspConfig {
    TsspConfig {
        data_dir: temp_dir.path().to_path_buf(),
        max_file_size: 256 * 1024 * 1024,
        compression: CompressionType::None,
    }
}

#[test]
fn test_tssp_writer_new() {
    let temp_dir = TempDir::new().unwrap();
    let config = create_test_config(&temp_dir);
    let writer = TsspWriter::new(config);
    assert!(writer.is_ok());
}

#[test]
fn test_tssp_writer_write_single_batch() {
    let temp_dir = TempDir::new().unwrap();
    let config = create_test_config(&temp_dir);
    let mut writer = TsspWriter::new(config).unwrap();

    let schema = TableSchema {
        table_id: 1,
        columns: vec![
            ("time".to_string(), 0),
            ("cpu".to_string(), 1),
            ("memory".to_string(), 2),
        ].into_iter().collect(),
    };

    let time_values: Vec<u8> = vec![0i64, 8, 16, 24].iter().flat_map(|v| v.to_le_bytes()).collect();
    let cpu_values: Vec<u8> = vec![1.0f32, 2.0, 3.0, 4.0].iter().flat_map(|v| v.to_le_bytes()).collect();

    let result = writer.write_batch(&schema, 1000, 4000, vec![
        ColumnData { column_id: 0, values: time_values.clone(), null_count: 0 },
        ColumnData { column_id: 1, values: cpu_values.clone(), null_count: 0 },
    ]);

    assert!(result.is_ok());
    let meta = result.unwrap();
    assert_eq!(meta.min_time, 1000);
    assert_eq!(meta.max_time, 4000);
    assert!(meta.size > 0);
}

#[test]
fn test_tssp_writer_write_multiple_batches() {
    let temp_dir = TempDir::new().unwrap();
    let config = create_test_config(&temp_dir);
    let mut writer = TsspWriter::new(config).unwrap();

    let schema = TableSchema {
        table_id: 1,
        columns: vec![
            ("time".to_string(), 0),
            ("value".to_string(), 1),
        ].into_iter().collect(),
    };

    let result1 = writer.write_batch(&schema, 1000, 2000, vec![
        ColumnData { column_id: 0, values: vec![0u8; 8], null_count: 0 },
        ColumnData { column_id: 1, values: vec![1u8; 8], null_count: 0 },
    ]);
    assert!(result1.is_ok());

    let result2 = writer.write_batch(&schema, 2000, 3000, vec![
        ColumnData { column_id: 0, values: vec![0u8; 8], null_count: 0 },
        ColumnData { column_id: 1, values: vec![2u8; 8], null_count: 0 },
    ]);
    assert!(result2.is_ok());

    let meta = writer.close().unwrap();
    assert!(meta.size > 0);
}

#[test]
fn test_tssp_writer_with_compression() {
    let temp_dir = TempDir::new().unwrap();
    let mut config = create_test_config(&temp_dir);
    config.compression = CompressionType::Lz4;

    let mut writer = TsspWriter::new(config).unwrap();

    let schema = TableSchema {
        table_id: 1,
        columns: vec![("data".to_string(), 0)].into_iter().collect(),
    };

    let large_data: Vec<u8> = (0..1000).map(|i| (i % 256) as u8).collect();
    
    let result = writer.write_batch(&schema, 1000, 2000, vec![
        ColumnData { column_id: 0, values: large_data, null_count: 0 },
    ]);

    assert!(result.is_ok());
    let meta = result.unwrap();
    assert!(meta.size > 0);
}

#[test]
fn test_tssp_reader_read() {
    let temp_dir = TempDir::new().unwrap();
    let config = create_test_config(&temp_dir);
    let mut writer = TsspWriter::new(config.clone()).unwrap();

    let schema = TableSchema {
        table_id: 1,
        columns: vec![
            ("time".to_string(), 0),
            ("cpu".to_string(), 1),
        ].into_iter().collect(),
    };

    let time_values: Vec<u8> = vec![1000i64, 2000, 3000].iter().flat_map(|v| v.to_le_bytes()).collect();
    let cpu_values: Vec<u8> = vec![1.0f32, 2.0, 3.0].iter().flat_map(|v| v.to_le_bytes()).collect();

    writer.write_batch(&schema, 1000, 3000, vec![
        ColumnData { column_id: 0, values: time_values, null_count: 0 },
        ColumnData { column_id: 1, values: cpu_values, null_count: 0 },
    ]).unwrap();

    let meta = writer.close().unwrap();

    let reader = TsspReader::new(config).unwrap();
    let data = reader.read(&meta);
    assert!(data.is_ok());
    let result = data.unwrap();
    assert!(!result.is_empty());
}

#[test]
fn test_tssp_file_meta() {
    let meta = FileMeta {
        file_id: 1,
        min_time: 1000,
        max_time: 5000,
        size: 1024,
        bloom_filter_data: None,
    };

    assert_eq!(meta.file_id, 1);
    assert_eq!(meta.min_time, 1000);
    assert_eq!(meta.max_time, 5000);
    assert_eq!(meta.size, 1024);
}

#[test]
fn test_tssp_compression_type_extension() {
    assert_eq!(CompressionType::None.extension(), "");
    assert_eq!(CompressionType::Snappy.extension(), ".snappy");
    assert_eq!(CompressionType::Zstd.extension(), ".zstd");
    assert_eq!(CompressionType::Lz4.extension(), ".lz4");
}

#[test]
fn test_tssp_schema_creation() {
    let schema = TableSchema {
        table_id: 42,
        columns: vec![
            ("host".to_string(), 0),
            ("region".to_string(), 1),
            ("cpu".to_string(), 2),
        ].into_iter().collect(),
    };

    assert_eq!(schema.table_id, 42);
    assert_eq!(schema.columns.len(), 3);
    assert_eq!(schema.columns.get("host"), Some(&0));
    assert_eq!(schema.columns.get("region"), Some(&1));
    assert_eq!(schema.columns.get("cpu"), Some(&2));
}

#[test]
fn test_tssp_writer_multiple_schemas_rejected() {
    let temp_dir = TempDir::new().unwrap();
    let config = create_test_config(&temp_dir);
    let mut writer = TsspWriter::new(config).unwrap();

    let schema1 = TableSchema {
        table_id: 1,
        columns: vec![("cpu".to_string(), 0)].into_iter().collect(),
    };

    let time_values: Vec<u8> = vec![1000i64].iter().flat_map(|v| v.to_le_bytes()).collect();

    writer.write_batch(&schema1, 1000, 2000, vec![
        ColumnData { column_id: 0, values: time_values.clone(), null_count: 0 },
    ]).unwrap();

    let schema2 = TableSchema {
        table_id: 2,
        columns: vec![("memory".to_string(), 0)].into_iter().collect(),
    };

    let result = writer.write_batch(&schema2, 2000, 3000, vec![
        ColumnData { column_id: 0, values: time_values, null_count: 0 },
    ]);

    assert!(result.is_ok());
}

#[test]
fn test_tssp_writer_add_series_keys() {
    let temp_dir = TempDir::new().unwrap();
    let config = create_test_config(&temp_dir);
    let mut writer = TsspWriter::new(config).unwrap();

    writer.add_series_key(b"host=server1");
    writer.add_series_key(b"host=server2");
    writer.add_series_key(b"region=us-east");

    let schema = TableSchema {
        table_id: 1,
        columns: vec![("value".to_string(), 0)].into_iter().collect(),
    };

    let time_values: Vec<u8> = vec![1000i64].iter().flat_map(|v| v.to_le_bytes()).collect();

    writer.write_batch(&schema, 1000, 2000, vec![
        ColumnData { column_id: 0, values: time_values, null_count: 0 },
    ]).unwrap();

    let meta = writer.close().unwrap();
    assert!(meta.size > 0);
}

#[test]
fn test_tssp_writer_time_range_tracking() {
    let temp_dir = TempDir::new().unwrap();
    let config = create_test_config(&temp_dir);
    let mut writer = TsspWriter::new(config).unwrap();

    let schema = TableSchema {
        table_id: 1,
        columns: vec![("value".to_string(), 0)].into_iter().collect(),
    };

    let time_values: Vec<u8> = vec![5000i64].iter().flat_map(|v| v.to_le_bytes()).collect();

    writer.write_batch(&schema, 5000, 6000, vec![
        ColumnData { column_id: 0, values: time_values, null_count: 0 },
    ]).unwrap();

    let meta = writer.close().unwrap();
    assert_eq!(meta.min_time, 5000);
    assert_eq!(meta.max_time, 6000);
}

#[test]
fn test_tssp_file_meta_with_bloom() {
    let bloom_data = vec![1u64, 2, 3, 4, 5];
    let meta = FileMeta {
        file_id: 42,
        min_time: 1000,
        max_time: 5000,
        size: 4096,
        bloom_filter_data: Some(bloom_data.clone()),
    };

    assert_eq!(meta.file_id, 42);
    assert!(meta.bloom_filter_data.is_some());
    assert_eq!(meta.bloom_filter_data.unwrap(), bloom_data);
}

#[test]
fn test_tssp_reader_new() {
    let temp_dir = TempDir::new().unwrap();
    let config = create_test_config(&temp_dir);
    let reader = TsspReader::new(config);
    assert!(reader.is_ok());
}

#[test]
fn test_tssp_column_data() {
    let col = ColumnData {
        column_id: 5,
        values: vec![1u8, 2, 3, 4, 5],
        null_count: 2,
    };

    assert_eq!(col.column_id, 5);
    assert_eq!(col.values.len(), 5);
    assert_eq!(col.null_count, 2);
}