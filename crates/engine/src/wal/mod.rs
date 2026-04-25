use crate::config::WalConfig;
use crate::error::{Error, Result};
use crate::WriteBatch;
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use parking_lot::Mutex;
use std::fs::{self, File, OpenOptions};
use std::io::{BufWriter, Read, Seek, SeekFrom, Write};
use std::path::PathBuf;

#[derive(Debug, Clone)]
pub struct WalEntry {
    pub file_id: u64,
    pub offset: u64,
    pub size: u32,
}

pub struct Wal {
    dir: PathBuf,
    file_size: u64,
    sync_enabled: bool,
    current_file: Mutex<Option<WalFile>>,
    file_id: Mutex<u64>,
}

struct WalFile {
    file: BufWriter<File>,
    file_id: u64,
    offset: u64,
}

impl Wal {
    pub fn new(config: &WalConfig) -> Result<Self> {
        if !config.dir.exists() {
            fs::create_dir_all(&config.dir)?;
        }
        
        Ok(Self {
            dir: config.dir.clone(),
            file_size: config.file_size,
            sync_enabled: config.sync_enabled,
            current_file: Mutex::new(None),
            file_id: Mutex::new(0),
        })
    }

    pub fn write(&self, batch: &WriteBatch) -> Result<WalEntry> {
        let mut current = self.current_file.lock();
        if current.is_none() {
            *current = Some(self.create_file()?);
        }

        let file = current.as_mut().unwrap();
        if file.offset >= self.file_size {
            *current = Some(self.create_file()?);
            let file = current.as_mut().unwrap();
            let offset = self.write_entry_internal(file, batch)?;
            return Ok(WalEntry {
                file_id: file.file_id,
                offset,
                size: 0,
            });
        }

        let offset = self.write_entry_internal(file, batch)?;
        Ok(WalEntry {
            file_id: file.file_id,
            offset,
            size: 0,
        })
    }

    fn create_file(&self) -> Result<WalFile> {
        let mut file_id = self.file_id.lock();
        let id = *file_id;
        *file_id += 1;

        let path = self.dir.join(format!("{:016x}.wal", id));
        let file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .read(true)
            .open(&path)?;

        Ok(WalFile { file: BufWriter::new(file), file_id: id, offset: 0 })
    }

    fn write_entry_internal(&self, file: &mut WalFile, batch: &WriteBatch) -> Result<u64> {
        let offset = file.offset;
        let data = bincode::serialize(batch).map_err(|e| Error::Wal(e.to_string()))?;

        file.file.write_u32::<BigEndian>(data.len() as u32)?;
        file.file.write_all(&data)?;
        file.offset += 4 + data.len() as u64;

        file.file.flush()?;
        if self.sync_enabled {
            file.file.get_ref().sync_all()?;
        }

        Ok(offset)
    }

    pub fn read(&self, file_id: u64, offset: u64) -> Result<WriteBatch> {
        let path = self.dir.join(format!("{:016x}.wal", file_id));
        let mut file = File::open(&path)?;

        file.seek(SeekFrom::Start(offset))?;
        let len = file.read_u32::<BigEndian>()?;
        let mut data = vec![0u8; len as usize];
        file.read_exact(&mut data)?;

        let batch: WriteBatch = bincode::deserialize(&data).map_err(|e| Error::Wal(e.to_string()))?;
        Ok(batch)
    }

    pub fn mark_flushed(&self, _file_id: u64) -> Result<()> {
        Ok(())
    }

    pub fn len(&self) -> usize {
        let entries = fs::read_dir(&self.dir).map(|e| e.count()).unwrap_or(0);
        let current = self.current_file.lock();
        if current.is_some() {
            entries + 1
        } else {
            entries
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn purge(&self, before_file_id: u64) -> Result<()> {
        let entries = fs::read_dir(&self.dir)?;
        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().map(|e| e == "wal").unwrap_or(false) {
                if let Some(stem) = path.file_stem().and_then(|s| s.to_str()) {
                    if let Ok(id) = u64::from_str_radix(stem, 16) {
                        if id < before_file_id {
                            fs::remove_file(path)?;
                        }
                    }
                }
            }
        }
        Ok(())
    }

    pub fn close(&self) -> Result<()> {
        let mut current = self.current_file.lock();
        if let Some(mut file) = current.take() {
            file.file.flush()?;
            file.file.get_ref().sync_all()?;
        }
        Ok(())
    }

    pub fn replay<F>(&self, mut callback: F) -> Result<()>
    where
        F: FnMut(WriteBatch) -> Result<()>,
    {
        let entries = fs::read_dir(&self.dir)?;
        let mut wal_files: Vec<u64> = Vec::new();
        
        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().map(|e| e == "wal").unwrap_or(false) {
                if let Some(stem) = path.file_stem().and_then(|s| s.to_str()) {
                    if let Ok(id) = u64::from_str_radix(stem, 16) {
                        wal_files.push(id);
                    }
                }
            }
        }
        
        wal_files.sort();
        
        for file_id in wal_files {
            self.replay_file(file_id, &mut callback)?;
        }
        
        Ok(())
    }
    
    fn replay_file<F>(&self, file_id: u64, callback: &mut F) -> Result<()>
    where
        F: FnMut(WriteBatch) -> Result<()>,
    {
        let path = self.dir.join(format!("{:016x}.wal", file_id));
        if !path.exists() {
            return Ok(());
        }
        
        let mut file = File::open(&path)?;
        let file_size = file.metadata()?.len();
        let mut offset = 0u64;
        
        while offset < file_size {
            let len = {
                let mut len_bytes = [0u8; 4];
                file.read_exact(&mut len_bytes)?;
                offset += 4;
                u32::from_be_bytes(len_bytes) as u64
            };
            
            if len == 0 {
                break;
            }
            
            let mut data = vec![0u8; len as usize];
            file.read_exact(&mut data)?;
            offset += len;
            
            match bincode::deserialize::<WriteBatch>(&data) {
                Ok(batch) => {
                    callback(batch)?;
                }
                Err(e) => {
                    eprintln!("WAL replay: failed to deserialize batch: {}", e);
                }
            }
        }
        
        Ok(())
    }

    pub fn get_wal_files(&self) -> Result<Vec<u64>> {
        let entries = fs::read_dir(&self.dir)?;
        let mut wal_files: Vec<u64> = Vec::new();
        
        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().map(|e| e == "wal").unwrap_or(false) {
                if let Some(stem) = path.file_stem().and_then(|s| s.to_str()) {
                    if let Ok(id) = u64::from_str_radix(stem, 16) {
                        wal_files.push(id);
                    }
                }
            }
        }
        
        wal_files.sort();
        Ok(wal_files)
    }
}
