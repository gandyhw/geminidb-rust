#![allow(dead_code)]
#![allow(non_snake_case)]

pub mod config;
pub mod error;

pub mod wal;
pub mod memtable;
pub mod tssp;
pub mod index;
pub mod compaction;
pub mod bloom;
pub mod block_index;
pub mod ttl;
pub mod schema;
pub mod shard;
pub mod tiered_storage;
pub mod merge;
pub mod downsample;
pub mod query;
pub mod metaclient;
pub mod snapshot;
pub mod scheduler;
pub mod api;
pub mod raft;
pub mod line_protocol;
pub mod influxql;
pub mod http;
pub mod prometheus;
pub mod otel;
pub mod backup;
pub mod syscontrol;
pub mod arrow_flight;
pub mod cache;

pub use config::{Config, EngineConfig, WalConfig, MemTableConfig, TsspConfig, CompactionConfig, CompressionType};
pub use error::{Error, Result};
pub use wal::Wal;
pub use memtable::MemTable;
pub use http::{HttpServer, HttpConfig};
pub use tssp::{TsspReader, TsspWriter, FileMeta, TableSchema, ColumnData};
pub use index::SeriesIndex;
pub use compaction::CompactionManager;
pub use schema::{Schema, Database, Measurement, Field, Tag, FieldType, RetentionPolicy};
pub use shard::{ShardInfo, ShardManager, ShardMapper, ShardStatus};
pub use tiered_storage::{StorageTier, TierConfig, TieredStorageManager};
pub use merge::{MergeCandidate, MergeResult, MergeTool, MergeScheduler};
pub use downsample::{DownsampleInterval, DownsampleRule, DownsampleEngine, AggregatorType};
pub use query::{QueryRequest, QueryExecutor};
pub use metaclient::{MetaClient, MetaClientStub, NodeInfo, ShardMapping, DatabaseInfo, ShardGroup, ReplicaShardInfo};
pub use snapshot::{SnapshotId, SnapshotManifest, SnapshotFile, SnapshotService};
pub use scheduler::{Scheduler, ScheduledTask, TaskId, TaskHandler, TaskExecution, TaskPriority};

use std::path::PathBuf;
use std::sync::{Arc, Mutex, RwLock};

use crate::cache::{CacheConfig, QueryCache, generate_cache_key};

pub struct Engine {
    config: EngineConfig,
    wal: Wal,
    memtable: MemTable,
    tssp_manager: TsspManager,
    compaction: CompactionManager,
    series_index: SeriesIndex,
    shard_manager: ShardManager,
    shard_mapper: ShardMapper,
    tiered_storage: TieredStorageManager,
    schema: Arc<RwLock<Schema>>,
    measurements: std::collections::HashSet<String>,
    measurement_tag_keys: std::collections::HashMap<String, std::collections::HashSet<String>>,
    measurement_field_keys: std::collections::HashMap<String, std::collections::HashSet<String>>,
    series_key_to_id: std::collections::HashMap<Vec<u8>, u64>,
    deleted_series: std::collections::HashSet<u64>,
    query_cache: QueryCache,
}

struct TsspManager {
    data_dir: PathBuf,
    config: TsspConfig,
    file_metas: Mutex<Vec<FileMeta>>,
}

impl Engine {
    pub fn new(config: EngineConfig) -> Result<Self> {
        let tssp_data_dir = config.tssp.data_dir.clone();
        std::fs::create_dir_all(&tssp_data_dir).map_err(|e| Error::Tssp(e.to_string()))?;
        
        let wal = Wal::new(&config.wal)?;
        let memtable = MemTable::new(config.memtable.max_size);
        let tssp_manager = TsspManager::new(tssp_data_dir, config.tssp.clone());
        let compaction = CompactionManager::new(config.compaction.clone());
        let series_index = SeriesIndex::new();
        let shard_manager = ShardManager::new(config.data_dir.clone());
        let shard_mapper = ShardMapper::new(3600 * 24 * 7, 1);
        let tiered_storage = TieredStorageManager::new(config.data_dir.clone(), TierConfig::default());
        let schema = Arc::new(RwLock::new(Schema::new()));
        let query_cache = QueryCache::new(CacheConfig::default());

        let mut engine = Self {
            config,
            wal,
            memtable,
            tssp_manager,
            compaction,
            series_index,
            shard_manager,
            shard_mapper,
            tiered_storage,
            schema,
            measurements: std::collections::HashSet::new(),
            measurement_tag_keys: std::collections::HashMap::new(),
            measurement_field_keys: std::collections::HashMap::new(),
            series_key_to_id: std::collections::HashMap::new(),
            deleted_series: std::collections::HashSet::new(),
            query_cache,
        };
        
        engine.replay_wal()?;
        
        Ok(engine)
    }

    fn replay_wal(&mut self) -> Result<()> {
        let series_index = &mut self.series_index;
        let memtable = &mut self.memtable;
        let series_key_to_id = &mut self.series_key_to_id;
        
        self.wal.replay(|batch| {
            for row in &batch.rows {
                let series_key = Self::encode_series_key(&batch.table, &row.tags);
                let series_id = Self::compute_series_id(&series_key);
                if !series_index.contains(series_id) {
                    series_index.add(series_id);
                    series_key_to_id.insert(series_key, series_id);
                }
            }
            memtable.insert(batch.clone())?;
            Ok(())
        })
    }
    
    fn compute_series_id(series_key: &[u8]) -> u64 {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        let mut hasher = DefaultHasher::new();
        series_key.hash(&mut hasher);
        hasher.finish()
    }
    
    fn encode_series_key(measurement: &str, tags: &std::collections::HashMap<String, String>) -> Vec<u8> {
        let mut keys: Vec<_> = tags.iter().collect();
        keys.sort();
        let tag_str = keys.iter()
            .map(|(k, v)| format!("{}={}", k, v))
            .collect::<Vec<_>>()
            .join(",");
        format!("{}_{}", measurement, tag_str).into_bytes()
    }

    pub fn write(&mut self, batch: WriteBatch) -> Result<()> {
        self.measurements.insert(batch.table.clone());
        
        let tag_keys = self.measurement_tag_keys
            .entry(batch.table.clone())
            .or_default();
        
        let field_keys = self.measurement_field_keys
            .entry(batch.table.clone())
            .or_default();
        
        for row in &batch.rows {
            for key in row.tags.keys() {
                tag_keys.insert(key.clone());
            }
            
            for key in row.fields.keys() {
                field_keys.insert(key.clone());
            }
            
            let series_key = Self::encode_series_key(&batch.table, &row.tags);
            let series_id = Self::compute_series_id(&series_key);
            if !self.series_index.contains(series_id) {
                self.series_index.add(series_id);
                self.series_key_to_id.insert(series_key, series_id);
            }
        }
        self.wal.write(&batch)?;
        self.memtable.insert(batch)?;
        Ok(())
    }

    pub fn read(&self, query: Query) -> Result<QueryResult> {
        let start_time = std::time::Instant::now();
        
        let memtable_rows = self.memtable.scan(
            query.table.as_bytes(),
            query.time_range.start,
            query.time_range.end,
        )?;

        let tssp_rows = self.tssp_manager.read_files(
            query.table.as_bytes(),
            query.time_range.start,
            query.time_range.end,
        )?;

        let mut all_rows: Vec<Row> = memtable_rows.into_iter().map(|(k, v)| {
            let tags: std::collections::HashMap<String, String> = serde_json::from_slice(&v.tags).unwrap_or_default();
            let fields: std::collections::HashMap<String, FieldValue> = serde_json::from_slice(&v.fields).unwrap_or_default();
            Row { tags, fields, timestamp: k.timestamp }
        }).collect();

        all_rows.extend(tssp_rows);

        if let Some(ref filter) = query.filter {
            all_rows.retain(|row| filter.evaluate(row));
        }

        if let Some(limit) = query.limit {
            all_rows.truncate(limit);
        }

        let row_count = all_rows.len();
        let files_read = self.tssp_manager.get_file_count();
        let execution_time = start_time.elapsed().as_millis() as u64;
        
        Ok(QueryResult {
            rows: all_rows,
            stats: QueryStats {
                files_read,
                rows_scanned: row_count,
                bytes_read: 0,
                execution_time_ms: execution_time,
                series_count: self.series_index.len() as usize,
            },
        })
    }
    
    pub fn group_by_time(&self, rows: Vec<Row>, interval_ns: i64) -> Vec<Row> {
        if rows.is_empty() || interval_ns <= 0 {
            return rows;
        }
        
        let mut grouped: std::collections::HashMap<i64, Vec<&Row>> = std::collections::HashMap::new();
        
        for row in &rows {
            let bucket = (row.timestamp / interval_ns) * interval_ns;
            grouped.entry(bucket).or_default().push(row);
        }
        
        let mut result: Vec<Row> = grouped.into_iter().map(|(bucket, group_rows)| {
            let mut aggregated_fields: std::collections::HashMap<String, FieldValue> = std::collections::HashMap::new();
            
            if let Some(first_row) = group_rows.first() {
                for (key, value) in &first_row.fields {
                    let mut sum_float = 0.0;
                    let mut count = 0;
                    let mut min_f: Option<f64> = None;
                    let mut max_f: Option<f64> = None;
                    
                    for row in &group_rows {
                        if let Some(fv) = row.fields.get(key) {
                            if let Some(f) = fv.as_f64() {
                                sum_float += f;
                                count += 1;
                                min_f = Some(min_f.map_or(f, |m| m.min(f)));
                                max_f = Some(max_f.map_or(f, |m| m.max(f)));
                            }
                        }
                    }
                    
                    if count > 0 {
                        aggregated_fields.insert(key.clone(), FieldValue::Float(sum_float / count as f64));
                    } else {
                        aggregated_fields.insert(key.clone(), value.clone());
                    }
                }
            }
            
            Row {
                tags: std::collections::HashMap::new(),
                fields: aggregated_fields,
                timestamp: bucket,
            }
        }).collect();
        
        result.sort_by_key(|r| r.timestamp);
        result
    }
    
    pub fn get_stats(&self) -> EngineStats {
        EngineStats {
            series_count: self.series_index.len() as usize,
            memtable_size: self.memtable.size(),
            memtable_row_count: self.memtable.row_count(),
            tssp_file_count: self.tssp_manager.get_file_count(),
            wal_entries: self.wal.len(),
            shard_count: self.shard_manager.shard_count(),
        }
    }
    
    pub fn get_series_count(&self) -> usize {
        self.series_index.len() as usize
    }
    
    pub fn get_memtable_size(&self) -> u64 {
        self.memtable.size()
    }
    
    pub fn get_tssp_file_count(&self) -> usize {
        self.tssp_manager.get_file_count()
    }

    pub fn create_shard(&self, shard_id: u64, database: &str, rp: &str) -> Result<()> {
        self.shard_manager.create_shard(shard_id, database, rp)?;
        Ok(())
    }

    pub fn get_shard(&self, shard_id: u64) -> Option<std::sync::Arc<ShardInfo>> {
        self.shard_manager.get_shard(shard_id)
    }

    pub fn get_all_shards(&self) -> Vec<u64> {
        self.shard_manager.get_all_shards()
    }

    pub fn get_shards_by_db(&self, database: &str) -> Vec<std::sync::Arc<ShardInfo>> {
        self.shard_manager.get_shards_by_db(database)
    }

    pub fn map_shard(&self, timestamp: i64) -> u64 {
        self.shard_mapper.map_shard(timestamp)
    }

    pub fn get_shard_path(&self, database: &str, rp: &str, shard_id: u64) -> PathBuf {
        self.shard_mapper.get_shard_path(&self.config.data_dir, database, rp, shard_id)
    }

    pub fn determine_tier(&self, timestamp: i64) -> StorageTier {
        self.tiered_storage.determine_tier(timestamp)
    }

    pub fn should_migrate_tier(&self, timestamp: i64, current_tier: StorageTier) -> bool {
        self.tiered_storage.should_migrate_to_next_tier(timestamp, current_tier)
    }

    pub fn get_shard_count(&self) -> usize {
        self.shard_manager.shard_count()
    }

    pub fn get_tier_info(&self) -> (StorageTier, StorageTier, StorageTier) {
        (StorageTier::Hot, StorageTier::Warm, StorageTier::Cold)
    }

    pub fn flush(&mut self) -> Result<()> {
        if self.memtable.should_flush() {
            let rows = self.memtable.flush()?;
            if !rows.is_empty() {
                let meta = self.tssp_manager.write_rows(rows)?;
                self.tssp_manager.add_file_meta(meta);
            }
        }
        Ok(())
    }

    pub fn force_flush(&mut self) -> Result<()> {
        if self.memtable.row_count() > 0 {
            let rows = self.memtable.flush()?;
            if !rows.is_empty() {
                let meta = self.tssp_manager.write_rows(rows)?;
                self.tssp_manager.add_file_meta(meta);
            }
        }
        Ok(())
    }

    pub fn compact(&mut self) -> Result<()> {
        if self.compaction.needs_compaction(self.tssp_manager.get_file_count()) {
            let files = self.tssp_manager.get_file_metas();
            if files.len() >= 2 {
                self.tssp_manager.compact_files()?;
            }
        }
        Ok(())
    }

    pub fn close(&mut self) -> Result<()> {
        self.force_flush()?;
        self.wal.close()?;
        Ok(())
    }

    pub fn schema(&self) -> &Arc<RwLock<Schema>> {
        &self.schema
    }

    pub fn measurements(&self) -> Vec<String> {
        self.measurements.iter().cloned().collect()
    }

    pub fn get_tag_keys(&self, measurement: &str) -> Vec<String> {
        self.measurement_tag_keys
            .get(measurement)
            .map(|keys| keys.iter().cloned().collect())
            .unwrap_or_default()
    }

    pub fn get_field_keys(&self, measurement: &str) -> Vec<String> {
        self.measurement_field_keys
            .get(measurement)
            .map(|keys| keys.iter().cloned().collect())
            .unwrap_or_default()
    }

    pub fn query(&self, request: QueryRequest) -> Result<QueryResult> {
        let query = Query {
            database: request.database,
            table: request.measurement,
            time_range: request.time_range,
            columns: request.selected_fields,
            filter: request.filter,
            limit: request.limit,
        };
        let result = self.read(query)?;
        Ok(result)
    }

    pub fn create_database(&self, name: &str) -> Result<()> {
        let mut schema = self.schema.write().unwrap();
        schema.create_database(name.to_string())?;
        Ok(())
    }

    pub fn create_retention_policy(&self, db_name: &str, name: &str, duration_seconds: u64, replica_count: u32) -> Result<()> {
        let mut schema = self.schema.write().unwrap();
        let rp = RetentionPolicy::new(name.to_string(), duration_seconds)
            .with_replica_count(replica_count);
        schema.create_retention_policy(db_name, rp)?;
        Ok(())
    }

    pub fn get_retention_policies(&self, db_name: &str) -> Vec<(String, u64, u32)> {
        let schema = self.schema.read().unwrap();
        if let Some(db) = schema.databases.get(db_name) {
            db.retention_policies.iter()
                .map(|(name, rp)| (name.clone(), rp.duration_seconds, rp.replica_count))
                .collect()
        } else {
            Vec::new()
        }
    }

    pub fn drop_database(&self, name: &str) -> Result<()> {
        let mut schema = self.schema.write().unwrap();
        if !schema.databases.contains_key(name) {
            return Err(crate::Error::Schema(format!("database {} not found", name)));
        }
        schema.databases.remove(name);
        Ok(())
    }

    pub fn drop_measurement(&mut self, name: &str) -> Result<()> {
        self.measurements.remove(name);
        self.measurement_tag_keys.remove(name);
        self.measurement_field_keys.remove(name);
        Ok(())
    }

    pub fn drop_series(&mut self, series_id: Option<u64>) -> Result<()> {
        match series_id {
            Some(id) => {
                self.series_index.remove(id);
                self.deleted_series.insert(id);
            }
            None => {
                for id in self.series_key_to_id.values() {
                    self.series_index.remove(*id);
                    self.deleted_series.insert(*id);
                }
            }
        }
        self.memtable.clear()?;
        Ok(())
    }

    pub fn delete(&mut self, measurement: &str, tags: Option<&std::collections::HashMap<String, String>>) -> Result<()> {
        if let Some(tags_to_delete) = tags {
            let series_key = Self::encode_series_key(measurement, tags_to_delete);
            let series_id = Self::compute_series_id(&series_key);
            if self.series_key_to_id.contains_key(&series_key) {
                self.series_index.remove(series_id);
                self.deleted_series.insert(series_id);
            }
        } else {
            if let Some(id) = self.series_key_to_id.get(&Self::encode_series_key(measurement, &std::collections::HashMap::new())) {
                self.series_index.remove(*id);
                self.deleted_series.insert(*id);
            }
        }
        self.memtable.clear()?;
        Ok(())
    }

    pub fn is_series_deleted(&self, series_id: u64) -> bool {
        self.deleted_series.contains(&series_id)
    }

    pub fn get_series_id(&self, measurement: &str, tags: &std::collections::HashMap<String, String>) -> Option<u64> {
        let series_key = Self::encode_series_key(measurement, tags);
        self.series_key_to_id.get(&series_key).copied()
    }

    pub fn get_cache_stats(&self) -> crate::cache::CacheStats {
        self.query_cache.stats()
    }

    pub fn invalidate_cache(&self, key: Option<&str>) {
        match key {
            Some(k) => self.query_cache.invalidate(k),
            None => self.query_cache.clear(),
        }
    }

    pub fn invalidate_cache_by_prefix(&self, prefix: &str) {
        self.query_cache.invalidate_prefix(prefix);
    }

    pub fn cache_query_result(&self, database: &str, measurement: &str, query: &str, time_range: (i64, i64), result: &QueryResult) {
        let key = generate_cache_key(database, measurement, query, time_range);
        if let Ok(value) = serde_json::to_vec(result) {
            self.query_cache.insert(key, value);
        }
    }

    pub fn get_cached_query_result(&self, database: &str, measurement: &str, query: &str, time_range: (i64, i64)) -> Option<QueryResult> {
        let key = generate_cache_key(database, measurement, query, time_range);
        self.query_cache.get(&key)
    }
}

impl TsspManager {
    pub fn new(data_dir: PathBuf, config: TsspConfig) -> Self {
        Self {
            data_dir,
            config,
            file_metas: Mutex::new(Vec::new()),
        }
    }

    pub fn write_rows(&self, rows: Vec<Row>) -> Result<FileMeta> {
        if rows.is_empty() {
            return Ok(FileMeta {
                file_id: 0,
                min_time: 0,
                max_time: 0,
                size: 0,
                bloom_filter_data: None,
            });
        }

        let mut writer = TsspWriter::new(self.config.clone())?;

        let mut schema: std::collections::HashMap<String, u32> = std::collections::HashMap::new();
        schema.insert("time".to_string(), 0);
        
        let first_row = &rows[0];
        let mut col_id = 1;
        for tag_key in first_row.tags.keys() {
            schema.insert(tag_key.clone(), col_id);
            col_id += 1;
        }
        for field_key in first_row.fields.keys() {
            schema.insert(field_key.clone(), col_id);
            col_id += 1;
        }

        let table_schema = TableSchema {
            table_id: 1,
            columns: schema.clone(),
        };

        let mut min_time = i64::MAX;
        let mut max_time = i64::MIN;
        
        let mut time_col = Vec::new();
        let mut tag_json_col = Vec::new();
        let mut field_json_col = Vec::new();

        for row in &rows {
            min_time = min_time.min(row.timestamp);
            max_time = max_time.max(row.timestamp);
            
            time_col.extend_from_slice(&row.timestamp.to_le_bytes());
            
            let tags_json = serde_json::to_vec(&row.tags).unwrap_or_default();
            let fields_json = serde_json::to_vec(&row.fields).unwrap_or_default();
            
            tag_json_col.extend_from_slice(&(tags_json.len() as u64).to_le_bytes());
            tag_json_col.extend_from_slice(&tags_json);
            
            field_json_col.extend_from_slice(&(fields_json.len() as u64).to_le_bytes());
            field_json_col.extend_from_slice(&fields_json);
        }

        let columns = vec![
            ColumnData { column_id: 0, values: time_col, null_count: 0 },
            ColumnData { column_id: 1, values: tag_json_col, null_count: 0 },
            ColumnData { column_id: 2, values: field_json_col, null_count: 0 },
        ];

        let _meta = writer.write_batch(&table_schema, min_time, max_time, columns)?;
        writer.close()
    }

    pub fn add_file_meta(&self, meta: FileMeta) {
        if meta.file_id > 0 {
            let mut metas = self.file_metas.lock().unwrap();
            metas.push(meta);
        }
    }

    pub fn get_file_metas(&self) -> Vec<FileMeta> {
        let metas = self.file_metas.lock().unwrap();
        metas.clone()
    }

    pub fn get_file_count(&self) -> usize {
        let metas = self.file_metas.lock().unwrap();
        metas.len()
    }

    pub fn remove_file(&self, file_id: u64) -> Result<()> {
        let file_path = self.data_dir.join(format!("{:016x}.tssp", file_id));
        if file_path.exists() {
            std::fs::remove_file(&file_path).map_err(|e| Error::Tssp(e.to_string()))?;
        }
        let mut metas = self.file_metas.lock().unwrap();
        metas.retain(|m| m.file_id != file_id);
        Ok(())
    }

    pub fn read_files(&self, _table: &[u8], start: i64, end: i64) -> Result<Vec<Row>> {
        let metas = self.file_metas.lock().unwrap();
        let reader = TsspReader::new(self.config.clone())?;
        
        let mut all_rows = Vec::new();
        
        for meta in metas.iter() {
            if meta.max_time < start || meta.min_time > end {
                continue;
            }
            
            if let Ok(data) = reader.read(meta) {
                let rows = self.decode_data(&data)?;
                all_rows.extend(rows);
            }
        }
        
        Ok(all_rows)
    }

    pub fn read_all_rows_from_file(&self, meta: &FileMeta) -> Result<Vec<Row>> {
        let reader = TsspReader::new(self.config.clone())?;
        if let Ok(data) = reader.read(meta) {
            return self.decode_data(&data);
        }
        Ok(Vec::new())
    }

    pub fn compact_files(&self) -> Result<()> {
        let files = self.get_file_metas();
        if files.len() < 2 {
            return Ok(());
        }

        let mut all_rows = Vec::new();
        for meta in &files {
            let rows = self.read_all_rows_from_file(meta)?;
            all_rows.extend(rows);
        }

        if all_rows.is_empty() {
            return Ok(());
        }

        all_rows.sort_by_key(|r| r.timestamp);

        let compacted_meta = self.write_rows(all_rows)?;

        for meta in &files {
            self.remove_file(meta.file_id)?;
        }

        if compacted_meta.file_id > 0 {
            self.add_file_meta(compacted_meta);
        }

        Ok(())
    }

    fn decode_data(&self, data: &[u8]) -> Result<Vec<Row>> {
        let mut rows = Vec::new();
        let mut offset = 0;
        
        let _table_id = u64::from_le_bytes(data[offset..offset+8].try_into().unwrap());
        offset += 8;
        
        let col_count = u64::from_le_bytes(data[offset..offset+8].try_into().unwrap()) as usize;
        offset += 8;
        
        let mut col_id_to_name: std::collections::HashMap<u32, String> = std::collections::HashMap::new();
        
        for _ in 0..col_count {
            if offset + 12 > data.len() {
                break;
            }
            
            let col_id = u32::from_le_bytes(data[offset..offset+4].try_into().unwrap());
            offset += 4;
            
            let name_len = u64::from_le_bytes(data[offset..offset+8].try_into().unwrap()) as usize;
            offset += 8;
            
            if offset + name_len > data.len() {
                break;
            }
            
            let name = String::from_utf8(data[offset..offset+name_len].to_vec()).unwrap_or_default();
            offset += name_len;
            
            col_id_to_name.insert(col_id, name);
        }
        
        let end_marker = u64::from_le_bytes(data[offset..offset+8].try_into().unwrap());
        offset += 8;
        
        if end_marker != 0 {
            return Ok(rows);
        }
        
        let mut col_data: std::collections::HashMap<u32, Vec<u8>> = std::collections::HashMap::new();
        
        while offset < data.len() {
            if offset + 12 > data.len() {
                break;
            }
            
            let col_id = u32::from_le_bytes(data[offset..offset+4].try_into().unwrap());
            offset += 4;
            
            let data_len = u64::from_le_bytes(data[offset..offset+8].try_into().unwrap()) as usize;
            offset += 8;
            
            if offset + data_len > data.len() {
                break;
            }
            
            let col_values = data[offset..offset+data_len].to_vec();
            col_data.insert(col_id, col_values);
            offset += data_len;
        }
        
        let time_col = col_data.get(&0);
        let tag_col = col_data.get(&1);
        let field_col = col_data.get(&2);
        
        let row_count = time_col.map(|c| c.len() / 8).unwrap_or(0);
        
        for i in 0..row_count {
            let timestamp = if let Some(time_data) = time_col {
                i64::from_le_bytes(time_data[i*8..(i+1)*8].try_into().unwrap())
            } else {
                continue;
            };
            
            let mut tags = std::collections::HashMap::new();
            let mut fields = std::collections::HashMap::new();
            
            if let Some(tag_data) = tag_col {
                let mut tag_offset = 0;
                for _j in 0..i {
                    if tag_offset + 8 > tag_data.len() {
                        break;
                    }
                    let json_len = u64::from_le_bytes(tag_data[tag_offset..tag_offset+8].try_into().unwrap()) as usize;
                    tag_offset += 8 + json_len;
                }
                
                if tag_offset + 8 <= tag_data.len() {
                    let json_len = u64::from_le_bytes(tag_data[tag_offset..tag_offset+8].try_into().unwrap()) as usize;
                    tag_offset += 8;
                    
                    if tag_offset + json_len <= tag_data.len() {
                        let json_bytes = &tag_data[tag_offset..tag_offset+json_len];
                        if let Ok(parsed_tags) = serde_json::from_slice::<std::collections::HashMap<String, String>>(json_bytes) {
                            tags = parsed_tags;
                        }
                    }
                }
            }
            
            if let Some(field_data) = field_col {
                let mut field_offset = 0;
                for _j in 0..i {
                    if field_offset + 8 > field_data.len() {
                        break;
                    }
                    let json_len = u64::from_le_bytes(field_data[field_offset..field_offset+8].try_into().unwrap()) as usize;
                    field_offset += 8 + json_len;
                }
                
                if field_offset + 8 <= field_data.len() {
                    let json_len = u64::from_le_bytes(field_data[field_offset..field_offset+8].try_into().unwrap()) as usize;
                    field_offset += 8;
                    
                    if field_offset + json_len <= field_data.len() {
                        let json_bytes = &field_data[field_offset..field_offset+json_len];
                        if let Ok(parsed_fields) = serde_json::from_slice::<std::collections::HashMap<String, FieldValue>>(json_bytes) {
                            fields = parsed_fields;
                        }
                    }
                }
            }
            
            rows.push(Row { tags, fields, timestamp });
        }
        
        Ok(rows)
    }
}

#[allow(dead_code)]
fn bytes_to_string(bytes: &[u8]) -> Result<String> {
    Ok(String::from_utf8(bytes.to_vec()).unwrap_or_default())
}

#[allow(dead_code)]
fn bytes_to_i64(bytes: &[u8]) -> Result<i64> {
    let arr: [u8; 8] = bytes.try_into().map_err(|_| Error::InvalidArgument("invalid bytes".to_string()))?;
    Ok(i64::from_le_bytes(arr))
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

impl FieldValue {
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            FieldValue::Float(v) => Some(*v),
            FieldValue::Integer(v) => Some(*v as f64),
            FieldValue::Unsigned(v) => Some(*v as f64),
            FieldValue::String(s) => String::from_utf8_lossy(s).to_string().parse().ok(),
            FieldValue::Boolean(b) => Some(if *b { 1.0 } else { 0.0 }),
        }
    }
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
    Gte(String, FieldValue),
    Lt(String, FieldValue),
    Lte(String, FieldValue),
}

impl FilterExpr {
    pub fn evaluate(&self, row: &Row) -> bool {
        match self {
            FilterExpr::And(left, right) => {
                left.evaluate(row) && right.evaluate(row)
            }
            FilterExpr::Or(left, right) => {
                left.evaluate(row) || right.evaluate(row)
            }
            FilterExpr::Eq(field, value) => {
                if let Some(field_value) = row.fields.get(field) {
                    field_value == value
                } else {
                    false
                }
            }
            FilterExpr::Ne(field, value) => {
                if let Some(field_value) = row.fields.get(field) {
                    field_value != value
                } else {
                    true
                }
            }
            FilterExpr::Gt(field, value) => {
                if let Some(field_value) = row.fields.get(field) {
                    field_value > value
                } else {
                    false
                }
            }
            FilterExpr::Gte(field, value) => {
                if let Some(field_value) = row.fields.get(field) {
                    field_value >= value
                } else {
                    false
                }
            }
            FilterExpr::Lt(field, value) => {
                if let Some(field_value) = row.fields.get(field) {
                    field_value < value
                } else {
                    false
                }
            }
            FilterExpr::Lte(field, value) => {
                if let Some(field_value) = row.fields.get(field) {
                    field_value <= value
                } else {
                    false
                }
            }
        }
    }
}

impl PartialEq for FieldValue {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (FieldValue::Integer(a), FieldValue::Integer(b)) => a == b,
            (FieldValue::Float(a), FieldValue::Float(b)) => a == b,
            (FieldValue::String(a), FieldValue::String(b)) => a == b,
            (FieldValue::Boolean(a), FieldValue::Boolean(b)) => a == b,
            (FieldValue::Unsigned(a), FieldValue::Unsigned(b)) => a == b,
            _ => false,
        }
    }
}

impl PartialOrd for FieldValue {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        match (self, other) {
            (FieldValue::Integer(a), FieldValue::Integer(b)) => a.partial_cmp(b),
            (FieldValue::Float(a), FieldValue::Float(b)) => a.partial_cmp(b),
            (FieldValue::Unsigned(a), FieldValue::Unsigned(b)) => a.partial_cmp(b),
            _ => None,
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
pub struct QueryResult {
    pub rows: Vec<Row>,
    pub stats: QueryStats,
}

impl QueryResult {
    pub fn count(&self, field: &str) -> usize {
        self.rows.iter().filter(|r| r.fields.contains_key(field)).count()
    }
    
    pub fn sum(&self, field: &str) -> Option<FieldValue> {
        let mut sum_float = 0.0;
        let mut sum_int = 0i64;
        let mut has_float = false;
        
        for row in &self.rows {
            if let Some(v) = row.fields.get(field) {
                match v {
                    FieldValue::Float(f) => {
                        sum_float += f;
                        has_float = true;
                    }
                    FieldValue::Integer(i) => {
                        sum_int += i;
                    }
                    FieldValue::Unsigned(u) => {
                        sum_int += *u as i64;
                    }
                    _ => {}
                }
            }
        }
        
        if has_float {
            Some(FieldValue::Float(sum_float))
        } else if sum_int != 0 {
            Some(FieldValue::Integer(sum_int))
        } else {
            Some(FieldValue::Integer(0))
        }
    }
    
    pub fn mean(&self, field: &str) -> Option<f64> {
        let mut count = 0;
        let mut sum = 0.0;
        
        for row in &self.rows {
            if let Some(v) = row.fields.get(field) {
                if let Some(f) = v.as_f64() {
                    sum += f;
                    count += 1;
                }
            }
        }
        
        if count > 0 {
            Some(sum / count as f64)
        } else {
            None
        }
    }
    
    pub fn min(&self, field: &str) -> Option<FieldValue> {
        let mut min_val: Option<FieldValue> = None;
        
        for row in &self.rows {
            if let Some(v) = row.fields.get(field) {
                match (&min_val, v) {
                    (None, _) => min_val = Some(v.clone()),
                    (Some(FieldValue::Float(m)), FieldValue::Float(f))
                        if f.lt(m) => { min_val = Some(v.clone()); }
                    (Some(FieldValue::Integer(m)), FieldValue::Integer(i))
                        if i.lt(m) => { min_val = Some(v.clone()); }
                    (Some(FieldValue::Unsigned(m)), FieldValue::Unsigned(u))
                        if u.lt(m) => { min_val = Some(v.clone()); }
                    (Some(FieldValue::Float(m)), FieldValue::Integer(i))
                        if (*i as f64).lt(m) => { min_val = Some(v.clone()); }
                    (Some(FieldValue::Integer(m)), FieldValue::Float(f))
                        if f.lt(&(*m as f64)) => { min_val = Some(v.clone()); }
                    _ => {}
                }
            }
        }
        
        min_val
    }
    
    pub fn max(&self, field: &str) -> Option<FieldValue> {
        let mut max_val: Option<FieldValue> = None;
        
        for row in &self.rows {
            if let Some(v) = row.fields.get(field) {
                match (&max_val, v) {
                    (None, _) => max_val = Some(v.clone()),
                    (Some(FieldValue::Float(m)), FieldValue::Float(f))
                        if f.gt(m) => { max_val = Some(v.clone()); }
                    (Some(FieldValue::Integer(m)), FieldValue::Integer(i))
                        if i.gt(m) => { max_val = Some(v.clone()); }
                    (Some(FieldValue::Unsigned(m)), FieldValue::Unsigned(u))
                        if u.gt(m) => { max_val = Some(v.clone()); }
                    (Some(FieldValue::Float(m)), FieldValue::Integer(i))
                        if (*i as f64).gt(m) => { max_val = Some(v.clone()); }
                    (Some(FieldValue::Integer(m)), FieldValue::Float(f))
                        if f.gt(&(*m as f64)) => { max_val = Some(v.clone()); }
                    _ => {}
                }
            }
        }
        
        max_val
    }
    
    pub fn first(&self, field: &str) -> Option<FieldValue> {
        self.rows.first().and_then(|r| r.fields.get(field).cloned())
    }
    
    pub fn last(&self, field: &str) -> Option<FieldValue> {
        self.rows.last().and_then(|r| r.fields.get(field).cloned())
    }
}

#[derive(Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct QueryStats {
    pub files_read: usize,
    pub rows_scanned: usize,
    pub bytes_read: usize,
    pub execution_time_ms: u64,
    pub series_count: usize,
}

#[derive(Debug, Default, Clone)]
pub struct EngineStats {
    pub series_count: usize,
    pub memtable_size: u64,
    pub memtable_row_count: u64,
    pub tssp_file_count: usize,
    pub wal_entries: usize,
    pub shard_count: usize,
}
