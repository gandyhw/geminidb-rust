use crate::config::{TsspConfig, CompressionType};
use crate::error::{Error, Result};
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

static FILE_ID_COUNTER: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, Clone)]
pub struct ColumnData {
    pub column_id: u32,
    pub values: Vec<u8>,
    pub null_count: u32,
}

#[derive(Debug, Clone)]
pub struct TableSchema {
    pub table_id: u64,
    pub columns: HashMap<String, u32>,
}

pub struct TsspWriter {
    config: TsspConfig,
    file: Option<BufWriter<File>>,
    file_path: Option<PathBuf>,
    file_id: u64,
    min_time: i64,
    max_time: i64,
    bytes_written: u64,
    schema: Option<TableSchema>,
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
    pub fn new(config: TsspConfig) -> Result<Self> {
        let file_id = FILE_ID_COUNTER.fetch_add(1, Ordering::SeqCst);
        let file_path = config.data_dir.join(format!("{:016x}.tssp", file_id));
        
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&file_path)
            .map_err(|e| Error::Tssp(format!("failed to create file: {}", e)))?;

        Ok(Self {
            config,
            file: Some(BufWriter::new(file)),
            file_path: Some(file_path),
            file_id,
            min_time: i64::MAX,
            max_time: i64::MIN,
            bytes_written: 0,
            schema: None,
        })
    }

    pub fn write_batch(&mut self, schema: &TableSchema, min_time: i64, max_time: i64, columns: Vec<ColumnData>) -> Result<FileMeta> {
        if self.file.is_none() {
            return Err(Error::Tssp("writer is closed".to_string()));
        }

        if self.schema.is_none() {
            self.schema = Some(schema.clone());
            self.write_header(schema)?;
        }

        self.min_time = self.min_time.min(min_time);
        self.max_time = self.max_time.max(max_time);

        let compression = self.config.compression;
        let writer = self.file.as_mut().unwrap();
        
        for col in &columns {
            let col_header = ColumnHeader {
                column_id: col.column_id,
                data_size: col.values.len() as u64,
                null_count: col.null_count,
            };
            
            col_header.write_to(writer)?;
            
            let compressed = compress_data(&col.values, compression)?;
            let size_bytes = (compressed.len() as u64).to_le_bytes();
            writer.write_all(&size_bytes)?;
            writer.write_all(&compressed)?;
            self.bytes_written += 8 + compressed.len() as u64;
        }

        Ok(FileMeta {
            file_id: self.file_id,
            min_time: self.min_time,
            max_time: self.max_time,
            size: self.bytes_written,
        })
    }

    fn write_header(&mut self, schema: &TableSchema) -> Result<()> {
        let writer = self.file.as_mut().unwrap();
        
        let magic: [u8; 4] = [0x54, 0x53, 0x53, 0x50];
        writer.write_all(&magic)?;
        
        let version: u32 = 1;
        writer.write_all(&version.to_le_bytes())?;
        
        writer.write_all(&schema.table_id.to_le_bytes())?;
        
        let col_count = schema.columns.len() as u32;
        writer.write_all(&col_count.to_le_bytes())?;
        
        for (name, id) in &schema.columns {
            let name_bytes = name.as_bytes();
            let name_len = name_bytes.len() as u32;
            writer.write_all(&name_len.to_le_bytes())?;
            writer.write_all(name_bytes)?;
            writer.write_all(&id.to_le_bytes())?;
        }

        self.bytes_written = 4 + 4 + 8 + 4 + schema.columns.iter()
            .map(|(name, _)| 4 + name.len() as u32 + 4)
            .sum::<u32>() as u64;
        
        Ok(())
    }

    pub fn close(mut self) -> Result<FileMeta> {
        if let Some(mut writer) = self.file.take() {
            writer.flush().map_err(|e| Error::Tssp(format!("flush error: {}", e)))?;
            let file = writer.into_inner().map_err(|e| Error::Tssp(format!("into inner error: {}", e)))?;
            file.sync_all().map_err(|e| Error::Tssp(format!("sync error: {}", e)))?;
        }

        let file_path = self.file_path.take();
        if let Some(path) = file_path {
            let size = std::fs::metadata(&path)
                .map(|m| m.len())
                .unwrap_or(self.bytes_written);
            
            return Ok(FileMeta {
                file_id: self.file_id,
                min_time: self.min_time,
                max_time: self.max_time,
                size,
            });
        }

        Ok(FileMeta {
            file_id: self.file_id,
            min_time: self.min_time,
            max_time: self.max_time,
            size: self.bytes_written,
        })
    }
}

impl TsspReader {
    pub fn new(config: TsspConfig) -> Result<Self> {
        Ok(Self { config })
    }

    pub fn read(&self, file_meta: &FileMeta) -> Result<Vec<u8>> {
        let file_path = self.config.data_dir.join(format!("{:016x}.tssp", file_meta.file_id));
        
        if !file_path.exists() {
            return Err(Error::NotFound(format!("file not found: {:?}", file_path)));
        }

        let file = File::open(&file_path).map_err(|e| Error::Tssp(e.to_string()))?;
        let mut reader = BufReader::new(file);
        
        let mut magic = [0u8; 4];
        reader.read_exact(&mut magic).map_err(|e| Error::Tssp(e.to_string()))?;
        
        if &magic != b"TSSP" {
            return Err(Error::Tssp("invalid file format".to_string()));
        }

        let mut version = [0u8; 4];
        reader.read_exact(&mut version).map_err(|e| Error::Tssp(e.to_string()))?;
        
        let mut table_id = [0u8; 8];
        reader.read_exact(&mut table_id).map_err(|e| Error::Tssp(e.to_string()))?;
        
        let mut col_count_bytes = [0u8; 4];
        reader.read_exact(&mut col_count_bytes).map_err(|e| Error::Tssp(e.to_string()))?;
        let col_count = u32::from_le_bytes(col_count_bytes) as usize;
        
        let mut columns = Vec::new();
        for _ in 0..col_count {
            let mut name_len_bytes = [0u8; 4];
            reader.read_exact(&mut name_len_bytes).map_err(|e| Error::Tssp(e.to_string()))?;
            let name_len = u32::from_le_bytes(name_len_bytes) as usize;
            
            let mut name_bytes = vec![0u8; name_len];
            reader.read_exact(&mut name_bytes).map_err(|e| Error::Tssp(e.to_string()))?;
            let name = String::from_utf8(name_bytes).map_err(|e| Error::Tssp(e.to_string()))?;
            
            let mut col_id_bytes = [0u8; 4];
            reader.read_exact(&mut col_id_bytes).map_err(|e| Error::Tssp(e.to_string()))?;
            let col_id = u32::from_le_bytes(col_id_bytes);
            
            columns.push((name, col_id));
        }

        let compression = self.config.compression;
        let mut output = Vec::new();
        output.extend_from_slice(&table_id);
        output.extend_from_slice(&(col_count as u64).to_le_bytes());
        
        for (name, col_id) in &columns {
            output.extend_from_slice(&col_id.to_le_bytes());
            let name_bytes = name.as_bytes();
            output.extend_from_slice(&(name_bytes.len() as u64).to_le_bytes());
            output.extend_from_slice(name_bytes);
        }
        
        output.extend_from_slice(&0u64.to_le_bytes());
        
        for _ in 0..columns.len() {
            let mut col_header = [0u8; 16];
            if reader.read_exact(&mut col_header).is_err() {
                break;
            }
            let col_id = u32::from_le_bytes([col_header[0], col_header[1], col_header[2], col_header[3]]);
            let _data_size = u64::from_le_bytes([col_header[4], col_header[5], col_header[6], col_header[7], col_header[8], col_header[9], col_header[10], col_header[11]]);
            let _null_count = u32::from_le_bytes([col_header[12], col_header[13], col_header[14], col_header[15]]);
            
            let mut size_bytes = [0u8; 8];
            if reader.read_exact(&mut size_bytes).is_err() {
                break;
            }
            let compressed_size = u64::from_le_bytes(size_bytes) as usize;
            
            let mut compressed = vec![0u8; compressed_size];
            reader.read_exact(&mut compressed).map_err(|e| Error::Tssp(e.to_string()))?;
            
            let decompressed = decompress_data(&compressed, compression)?;
            output.extend_from_slice(&col_id.to_le_bytes());
            output.extend_from_slice(&(decompressed.len() as u64).to_le_bytes());
            output.extend_from_slice(&decompressed);
        }

        Ok(output)
    }
}

#[derive(Debug, Clone)]
struct ColumnHeader {
    column_id: u32,
    data_size: u64,
    null_count: u32,
}

impl ColumnHeader {
    fn write_to<W: Write>(&self, writer: &mut W) -> Result<()> {
        writer.write_all(&self.column_id.to_le_bytes()).map_err(|e| Error::Tssp(e.to_string()))?;
        writer.write_all(&self.data_size.to_le_bytes()).map_err(|e| Error::Tssp(e.to_string()))?;
        writer.write_all(&self.null_count.to_le_bytes()).map_err(|e| Error::Tssp(e.to_string()))?;
        Ok(())
    }
}

fn compress_data(data: &[u8], compression: CompressionType) -> Result<Vec<u8>> {
    match compression {
        CompressionType::None => Ok(data.to_vec()),
        CompressionType::Snappy => {
            let mut result = Vec::with_capacity(data.len());
            let mut encoder = snap::raw::Encoder::new();
            encoder.compress(data, &mut result).map_err(|e| Error::Compression(e.to_string()))?;
            Ok(result)
        }
        CompressionType::Zstd => {
            let mut result = Vec::with_capacity(data.len());
            let mut encoder = zstd::Encoder::new(&mut result, 0).map_err(|e| Error::Compression(e.to_string()))?;
            encoder.write_all(data).map_err(|e| Error::Compression(e.to_string()))?;
            encoder.finish().map_err(|e| Error::Compression(e.to_string()))?;
            Ok(result)
        }
        CompressionType::Lz4 => {
            let mut result = Vec::with_capacity(data.len());
            let mut encoder = lz4_flex::frame::FrameEncoder::new(&mut result);
            encoder.write_all(data).map_err(|e| Error::Compression(e.to_string()))?;
            encoder.try_finish().map_err(|e| Error::Compression(e.to_string()))?;
            Ok(result)
        }
    }
}

fn decompress_data(data: &[u8], compression: CompressionType) -> Result<Vec<u8>> {
    match compression {
        CompressionType::None => Ok(data.to_vec()),
        CompressionType::Snappy => {
            let mut result = Vec::with_capacity(data.len() * 4);
            let mut decoder = snap::raw::Decoder::new();
            decoder.decompress(data, &mut result).map_err(|e| Error::Compression(e.to_string()))?;
            Ok(result)
        }
        CompressionType::Zstd => {
            zstd::decode_all(data).map_err(|e| Error::Compression(e.to_string()))
        }
        CompressionType::Lz4 => {
            let mut decoder = lz4_flex::frame::FrameDecoder::new(data);
            let mut result = Vec::new();
            decoder.read_to_end(&mut result).map_err(|e| Error::Compression(e.to_string()))?;
            Ok(result)
        }
    }
}