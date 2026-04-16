use crate::{Engine, Result, QueryResult, QueryRequest, TimeRange, WriteBatch, Row, FieldValue};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::RwLock;

#[derive(Debug, Clone)]
pub struct FlightConfig {
    pub enabled: bool,
    pub bind_addr: String,
    pub max_record_batch_size: usize,
}

impl Default for FlightConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            bind_addr: "0.0.0.0:9999".to_string(),
            max_record_batch_size: 10000,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlightDescriptor {
    pub type_: DescriptorType,
    pub database: String,
    pub measurement: String,
    pub sql_query: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DescriptorType {
    Unspecified,
    Path,
    Query,
}

#[derive(Debug, Clone)]
pub struct RecordBatch {
    pub schema: Schema,
    pub columns: Vec<Column>,
    pub row_count: usize,
}

#[derive(Debug, Clone)]
pub struct Schema {
    pub fields: Vec<Field>,
}

#[derive(Debug, Clone)]
pub struct Field {
    pub name: String,
    pub data_type: ArrowType,
    pub nullable: bool,
}

#[derive(Debug, Clone)]
pub enum ArrowType {
    Int64,
    Float64,
    Utf8,
    Bool,
    Timestamp,
    Dictionary,
}

#[derive(Debug, Clone)]
pub struct Column {
    pub name: String,
    pub data_type: ArrowType,
    pub values: ColumnValues,
    pub null_count: usize,
}

#[derive(Debug, Clone)]
pub enum ColumnValues {
    Int64(Vec<i64>),
    Float64(Vec<f64>),
    Utf8(Vec<String>),
    Bool(Vec<bool>),
    Dictionary(Vec<i64>),
}

#[derive(Debug, Clone)]
pub struct FlightData {
    pub flight_descriptor: Option<FlightDescriptor>,
    pub data_header: DataHeader,
    pub record_batch: Option<RecordBatch>,
}

#[derive(Debug, Clone)]
pub struct DataHeader {
    pub schema: Option<Schema>,
    pub is_last: bool,
    pub flight_metadata: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct FlightTicket {
    pub descriptor: FlightDescriptor,
    pub options: FlightOptions,
}

#[derive(Debug, Clone)]
pub struct FlightOptions {
    pub timeout_ms: Option<u64>,
    pub compression: Option<CompressionType>,
}

#[derive(Debug, Clone)]
pub enum CompressionType {
    None,
    Gzip,
    Zstd,
}

pub struct FlightServer {
    config: FlightConfig,
    engine: Arc<RwLock<Option<Engine>>>,
}

impl FlightServer {
    pub fn new(config: FlightConfig, engine: Arc<RwLock<Option<Engine>>>) -> Self {
        Self { config, engine }
    }
    
    pub fn do_get(&self, ticket: FlightTicket) -> Result<Vec<RecordBatch>> {
        let binding = self.engine.read().unwrap();
        let engine = match binding.as_ref() {
            Some(e) => e,
            None => return Err(crate::Error::InvalidArgument("engine not initialized".to_string())),
        };
        
        let req = QueryRequest::new(
            ticket.descriptor.database.clone(),
            ticket.descriptor.measurement.clone(),
            TimeRange {
                start: 0,
                end: i64::MAX,
            },
        );
        
        let result = engine.query(req)?;
        
        let batches = self.convert_to_record_batches(&result, ticket.options.compression)?;
        Ok(batches)
    }
    
    pub fn do_put(&mut self, descriptor: FlightDescriptor, data: Vec<FlightData>) -> Result<usize> {
        if !self.config.enabled {
            return Err(crate::Error::InvalidArgument("Flight server is disabled".to_string()));
        }
        
        let mut binding = self.engine.write().unwrap();
        let engine = match binding.as_mut() {
            Some(e) => e,
            None => return Err(crate::Error::InvalidArgument("engine not initialized".to_string())),
        };
        
        let mut total_rows = 0;
        
        for flight_data in data {
            if let Some(batch) = flight_data.record_batch {
                let rows = self.convert_from_record_batch(
                    &descriptor.database,
                    &descriptor.measurement,
                    batch,
                )?;
                
                for row in rows {
                    let batch = WriteBatch {
                        database: descriptor.database.clone(),
                        table: descriptor.measurement.clone(),
                        rows: vec![row],
                        timestamp: chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0),
                    };
                    engine.write(batch)?;
                    total_rows += 1;
                }
            }
        }
        
        Ok(total_rows)
    }
    
    pub fn list_flights(&self, database: Option<&str>) -> Result<Vec<FlightDescriptor>> {
        let binding = self.engine.read().unwrap();
        let engine = match binding.as_ref() {
            Some(e) => e,
            None => return Err(crate::Error::InvalidArgument("engine not initialized".to_string())),
        };
        
        let measurements = engine.list_measurements(database.unwrap_or("_internal"))?;
        
        let flights: Vec<FlightDescriptor> = measurements
            .into_iter()
            .map(|m| FlightDescriptor {
                type_: DescriptorType::Path,
                database: database.unwrap_or("").to_string(),
                measurement: m,
                sql_query: None,
            })
            .collect();
        
        Ok(flights)
    }
    
    fn convert_to_record_batches(&self, result: &QueryResult, _compression: Option<CompressionType>) -> Result<Vec<RecordBatch>> {
        if result.rows.is_empty() {
            return Ok(Vec::new());
        }
        
        let mut batches = Vec::new();
        let mut batch_size = 0;
        let mut current_batch_rows: Vec<&crate::Row> = Vec::new();
        
        for row in &result.rows {
            current_batch_rows.push(row);
            batch_size += 1;
            
            if batch_size >= self.config.max_record_batch_size {
                batches.push(self.build_batch_from_rows(&current_batch_rows)?);
                current_batch_rows.clear();
                batch_size = 0;
            }
        }
        
        if !current_batch_rows.is_empty() {
            batches.push(self.build_batch_from_rows(&current_batch_rows)?);
        }
        
        Ok(batches)
    }
    
    fn build_batch_from_rows(&self, rows: &[&crate::Row]) -> Result<RecordBatch> {
        let mut fields = Vec::new();
        let mut tag_names: Vec<String> = Vec::new();
        let mut field_names: Vec<String> = Vec::new();
        
        if let Some(first_row) = rows.first() {
            for (name, _) in &first_row.tags {
                fields.push(Field {
                    name: format!("tag_{}", name),
                    data_type: ArrowType::Utf8,
                    nullable: false,
                });
                tag_names.push(name.clone());
            }
            
            for (name, value) in &first_row.fields {
                let data_type = match value {
                    FieldValue::Integer(_) | FieldValue::Unsigned(_) => ArrowType::Int64,
                    FieldValue::Float(_) => ArrowType::Float64,
                    FieldValue::Boolean(_) => ArrowType::Bool,
                    FieldValue::String(_) => ArrowType::Utf8,
                };
                fields.push(Field {
                    name: name.clone(),
                    data_type,
                    nullable: true,
                });
                field_names.push(name.clone());
            }
            
            fields.push(Field {
                name: "time".to_string(),
                data_type: ArrowType::Timestamp,
                nullable: false,
            });
        }
        
        let schema = Schema { fields };
        
        let mut columns: Vec<Column> = Vec::new();
        
        for name in &tag_names {
            let values: Vec<String> = rows.iter()
                .map(|r| r.tags.get(name).cloned().unwrap_or_default())
                .collect();
            columns.push(Column {
                name: format!("tag_{}", name),
                data_type: ArrowType::Utf8,
                values: ColumnValues::Utf8(values),
                null_count: 0,
            });
        }
        
        for name in &field_names {
            let first_value = rows.first().and_then(|r| r.fields.get(name));
            match first_value {
                Some(FieldValue::Integer(_)) | Some(FieldValue::Unsigned(_)) => {
                    let values: Vec<i64> = rows.iter()
                        .map(|r| {
                            match r.fields.get(name) {
                                Some(FieldValue::Integer(v)) => *v,
                                Some(FieldValue::Unsigned(v)) => *v as i64,
                                _ => 0,
                            }
                        })
                        .collect();
                    columns.push(Column {
                        name: name.clone(),
                        data_type: ArrowType::Int64,
                        values: ColumnValues::Int64(values),
                        null_count: 0,
                    });
                }
                Some(FieldValue::Float(_)) => {
                    let values: Vec<f64> = rows.iter()
                        .map(|r| {
                            r.fields.get(name)
                                .and_then(|v| v.as_f64())
                                .unwrap_or(0.0)
                        })
                        .collect();
                    columns.push(Column {
                        name: name.clone(),
                        data_type: ArrowType::Float64,
                        values: ColumnValues::Float64(values),
                        null_count: 0,
                    });
                }
                Some(FieldValue::Boolean(_)) => {
                    let values: Vec<bool> = rows.iter()
                        .map(|r| {
                            match r.fields.get(name) {
                                Some(FieldValue::Boolean(v)) => *v,
                                _ => false,
                            }
                        })
                        .collect();
                    columns.push(Column {
                        name: name.clone(),
                        data_type: ArrowType::Bool,
                        values: ColumnValues::Bool(values),
                        null_count: 0,
                    });
                }
                _ => {
                    let values: Vec<String> = rows.iter()
                        .map(|r| {
                            match r.fields.get(name) {
                                Some(FieldValue::String(v)) => String::from_utf8_lossy(v).to_string(),
                                _ => String::new(),
                            }
                        })
                        .collect();
                    columns.push(Column {
                        name: name.clone(),
                        data_type: ArrowType::Utf8,
                        values: ColumnValues::Utf8(values),
                        null_count: 0,
                    });
                }
            }
        }
        
        let timestamps: Vec<i64> = rows.iter()
            .map(|r| r.timestamp)
            .collect();
        columns.push(Column {
            name: "time".to_string(),
            data_type: ArrowType::Timestamp,
            values: ColumnValues::Int64(timestamps),
            null_count: 0,
        });
        
        Ok(RecordBatch {
            schema,
            columns,
            row_count: rows.len(),
        })
    }
    
    fn convert_from_record_batch(&self, _database: &str, _measurement: &str, batch: RecordBatch) -> Result<Vec<Row>> {
        let mut rows = Vec::new();
        
        let row_count = if let Some(col) = batch.columns.first() {
            match &col.values {
                ColumnValues::Int64(v) => v.len(),
                ColumnValues::Float64(v) => v.len(),
                ColumnValues::Utf8(v) => v.len(),
                ColumnValues::Bool(v) => v.len(),
                ColumnValues::Dictionary(v) => v.len(),
            }
        } else {
            0
        };
        
        for i in 0..row_count {
            let mut tags = HashMap::new();
            let mut fields = HashMap::new();
            let mut timestamp = 0i64;
            
            for col in &batch.columns {
                let value = self.get_column_value(col, i);
                
                if col.name == "time" {
                    if let Some(v) = value.as_i64() {
                        timestamp = v;
                    }
                } else if col.name.starts_with("tag_") {
                    let tag_name = col.name.strip_prefix("tag_").unwrap();
                    if let Some(v) = value.as_str() {
                        tags.insert(tag_name.to_string(), v.clone());
                    }
                } else {
                    if let Some(v) = value.as_f64() {
                        fields.insert(col.name.clone(), FieldValue::Float(v));
                    } else if let Some(v) = value.as_i64() {
                        fields.insert(col.name.clone(), FieldValue::Integer(v));
                    } else if let Some(v) = value.as_bool() {
                        fields.insert(col.name.clone(), FieldValue::Boolean(v));
                    } else if let Some(v) = value.as_str() {
                        fields.insert(col.name.clone(), FieldValue::String(v.clone().into_bytes()));
                    }
                }
            }
            
            rows.push(Row {
                tags,
                fields,
                timestamp,
            });
        }
        
        Ok(rows)
    }
    
    fn get_column_value(&self, col: &Column, index: usize) -> ColumnValue {
        match &col.values {
            ColumnValues::Int64(v) => {
                if index < v.len() {
                    ColumnValue::Int64(v[index])
                } else {
                    ColumnValue::Null
                }
            }
            ColumnValues::Float64(v) => {
                if index < v.len() {
                    ColumnValue::Float64(v[index])
                } else {
                    ColumnValue::Null
                }
            }
            ColumnValues::Utf8(v) => {
                if index < v.len() {
                    ColumnValue::Str(v[index].clone())
                } else {
                    ColumnValue::Null
                }
            }
            ColumnValues::Bool(v) => {
                if index < v.len() {
                    ColumnValue::Bool(v[index])
                } else {
                    ColumnValue::Null
                }
            }
            ColumnValues::Dictionary(v) => {
                if index < v.len() {
                    ColumnValue::Int64(v[index])
                } else {
                    ColumnValue::Null
                }
            }
        }
    }
}

#[derive(Debug, Clone)]
enum ColumnValue {
    Int64(i64),
    Float64(f64),
    Str(String),
    Bool(bool),
    Null,
}

impl ColumnValue {
    fn as_i64(&self) -> Option<i64> {
        match self {
            ColumnValue::Int64(v) => Some(*v),
            _ => None,
        }
    }
    
    fn as_f64(&self) -> Option<f64> {
        match self {
            ColumnValue::Float64(v) => Some(*v),
            _ => None,
        }
    }
    
    fn as_str(&self) -> Option<String> {
        match self {
            ColumnValue::Str(v) => Some(v.clone()),
            _ => None,
        }
    }
    
    fn as_bool(&self) -> Option<bool> {
        match self {
            ColumnValue::Bool(v) => Some(*v),
            _ => None,
        }
    }
}

impl Engine {
    pub fn list_measurements(&self, _database: &str) -> Result<Vec<String>> {
        Ok(Vec::new())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_flight_config_default() {
        let config = FlightConfig::default();
        assert!(config.enabled);
        assert_eq!(config.bind_addr, "0.0.0.0:9999");
        assert_eq!(config.max_record_batch_size, 10000);
    }

    #[test]
    fn test_descriptor_type() {
        assert!(matches!(DescriptorType::Path, DescriptorType::Path));
        assert!(matches!(DescriptorType::Query, DescriptorType::Query));
        assert!(matches!(DescriptorType::Unspecified, DescriptorType::Unspecified));
    }

    #[test]
    fn test_arrow_type() {
        assert!(matches!(ArrowType::Int64, ArrowType::Int64));
        assert!(matches!(ArrowType::Float64, ArrowType::Float64));
        assert!(matches!(ArrowType::Utf8, ArrowType::Utf8));
        assert!(matches!(ArrowType::Bool, ArrowType::Bool));
        assert!(matches!(ArrowType::Timestamp, ArrowType::Timestamp));
    }

    #[test]
    fn test_column_values_int64() {
        let values = ColumnValues::Int64(vec![1, 2, 3]);
        if let ColumnValues::Int64(v) = values {
            assert_eq!(v, vec![1, 2, 3]);
        } else {
            panic!("Expected Int64");
        }
    }

    #[test]
    fn test_column_values_utf8() {
        let values = ColumnValues::Utf8(vec!["a".to_string(), "b".to_string()]);
        if let ColumnValues::Utf8(v) = values {
            assert_eq!(v.len(), 2);
        } else {
            panic!("Expected Utf8");
        }
    }

    #[test]
    fn test_flight_server_disabled() {
        let config = FlightConfig {
            enabled: false,
            ..Default::default()
        };
        let engine = Arc::new(RwLock::new(None::<Engine>));
        let mut server = FlightServer::new(config, engine);
        
        let descriptor = FlightDescriptor {
            type_: DescriptorType::Path,
            database: "test".to_string(),
            measurement: "cpu".to_string(),
            sql_query: None,
        };
        
        let result = server.do_put(descriptor, vec![]);
        assert!(result.is_err());
    }

    #[test]
    fn test_record_batch_row_count() {
        let batch = RecordBatch {
            schema: Schema { fields: vec![] },
            columns: vec![
                Column {
                    name: "value".to_string(),
                    data_type: ArrowType::Int64,
                    values: ColumnValues::Int64(vec![1, 2, 3, 4, 5]),
                    null_count: 0,
                }
            ],
            row_count: 5,
        };
        
        assert_eq!(batch.row_count, 5);
    }
}
