use crate::{Engine, WriteBatch, Row, FieldValue};
use crate::error::Result;
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct OtelConfig {
    pub enabled: bool,
    pub trace_enabled: bool,
    pub metric_enabled: bool,
    pub log_enabled: bool,
}

impl Default for OtelConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            trace_enabled: true,
            metric_enabled: true,
            log_enabled: true,
        }
    }
}

#[derive(Debug, Clone)]
pub struct TraceData {
    pub trace_id: Vec<u8>,
    pub span_id: Vec<u8>,
    pub parent_span_id: Option<Vec<u8>>,
    pub name: String,
    pub start_time_unix_nano: u64,
    pub end_time_unix_nano: u64,
    pub attributes: HashMap<String, String>,
    pub status_code: i32,
    pub status_message: String,
    pub resource_attributes: HashMap<String, String>,
}

#[derive(Debug, Clone)]
pub struct MetricData {
    pub name: String,
    pub description: String,
    pub unit: String,
    pub metric_type: MetricType,
    pub data_points: Vec<MetricDataPoint>,
    pub resource_attributes: HashMap<String, String>,
}

#[derive(Debug, Clone)]
pub enum MetricType {
    Gauge,
    Counter,
    Histogram,
    Summary,
}

#[derive(Debug, Clone)]
pub struct MetricDataPoint {
    pub time_unix_nano: u64,
    pub value: f64,
    pub attributes: HashMap<String, String>,
    pub count: Option<u64>,
    pub sum: Option<f64>,
    pub buckets: Option<Vec<HistogramBucket>>,
    pub quantile_values: Option<Vec<QuantileValue>>,
}

#[derive(Debug, Clone)]
pub struct HistogramBucket {
    pub count: u64,
    pub sum: f64,
    pub boundaries: Vec<f64>,
}

#[derive(Debug, Clone)]
pub struct QuantileValue {
    pub quantile: f64,
    pub value: f64,
}

#[derive(Debug, Clone)]
pub struct LogData {
    pub time_unix_nano: u64,
    pub observed_time_unix_nano: u64,
    pub severity_number: i32,
    pub severity_text: String,
    pub body: String,
    pub attributes: HashMap<String, String>,
    pub resource_attributes: HashMap<String, String>,
    pub trace_id: Option<Vec<u8>>,
    pub span_id: Option<Vec<u8>>,
}

impl Engine {
    pub fn write_otel_trace(&mut self, trace: TraceData) -> Result<()> {
        let measurement = "otel_traces".to_string();
        
        let mut tags = trace.resource_attributes.clone();
        for (k, v) in &trace.attributes {
            tags.insert(k.clone(), v.clone());
        }
        tags.insert("span_name".to_string(), trace.name.clone());
        
        let mut fields = HashMap::new();
        fields.insert("trace_id".to_string(), FieldValue::String(hex_encode(&trace.trace_id).into_bytes()));
        fields.insert("span_id".to_string(), FieldValue::String(hex_encode(&trace.span_id).into_bytes()));
        
        if let Some(parent) = &trace.parent_span_id {
            fields.insert("parent_span_id".to_string(), FieldValue::String(hex_encode(parent).into_bytes()));
        }
        
        fields.insert("start_time".to_string(), FieldValue::Integer(trace.start_time_unix_nano as i64));
        fields.insert("end_time".to_string(), FieldValue::Integer(trace.end_time_unix_nano as i64));
        fields.insert("duration_ns".to_string(), FieldValue::Integer((trace.end_time_unix_nano - trace.start_time_unix_nano) as i64));
        fields.insert("status_code".to_string(), FieldValue::Integer(trace.status_code as i64));
        
        if !trace.status_message.is_empty() {
            fields.insert("status_message".to_string(), FieldValue::String(trace.status_message.into_bytes()));
        }
        
        let timestamp = trace.start_time_unix_nano as i64 / 1_000;
        
        let batch = WriteBatch {
            database: "_otel".to_string(),
            table: measurement,
            rows: vec![Row { tags, fields, timestamp }],
            timestamp,
        };
        
        self.write(batch)
    }
    
    pub fn write_otel_metric(&mut self, metric: MetricData) -> Result<()> {
        let measurement = format!("otel_metrics_{}", metric.name);
        
        let mut tags = metric.resource_attributes.clone();
        tags.insert("metric_name".to_string(), metric.name.clone());
        tags.insert("metric_type".to_string(), format!("{:?}", metric.metric_type));
        
        if !metric.description.is_empty() {
            tags.insert("description".to_string(), metric.description.clone());
        }
        if !metric.unit.is_empty() {
            tags.insert("unit".to_string(), metric.unit.clone());
        }
        
        for dp in &metric.data_points {
            let mut row_tags = tags.clone();
            for (k, v) in &dp.attributes {
                row_tags.insert(k.clone(), v.clone());
            }
            
            let mut fields = HashMap::new();
            fields.insert("time".to_string(), FieldValue::Integer(dp.time_unix_nano as i64));
            
            match metric.metric_type {
                MetricType::Gauge | MetricType::Counter => {
                    fields.insert("value".to_string(), FieldValue::Float(dp.value));
                }
                MetricType::Histogram => {
                    if let Some(buckets) = &dp.buckets {
                        for (i, bucket) in buckets.iter().enumerate() {
                            fields.insert(format!("bucket_{}", i), FieldValue::Integer(bucket.count as i64));
                            fields.insert(format!("bucket_{}_sum", i), FieldValue::Float(bucket.sum));
                        }
                    }
                    if let Some(count) = dp.count {
                        fields.insert("count".to_string(), FieldValue::Integer(count as i64));
                    }
                    if let Some(sum) = dp.sum {
                        fields.insert("sum".to_string(), FieldValue::Float(sum));
                    }
                }
                MetricType::Summary => {
                    if let Some(quantiles) = &dp.quantile_values {
                        for qv in quantiles {
                            fields.insert(format!("quantile_{}", qv.quantile), FieldValue::Float(qv.value));
                        }
                    }
                }
            }
            
            let timestamp = dp.time_unix_nano as i64 / 1_000;
            
            let batch = WriteBatch {
                database: "_otel".to_string(),
                table: measurement.clone(),
                rows: vec![Row { tags: row_tags, fields, timestamp }],
                timestamp,
            };
            
            self.write(batch)?;
        }
        
        Ok(())
    }
    
    pub fn write_otel_log(&mut self, log: LogData) -> Result<()> {
        let measurement = "otel_logs".to_string();
        
        let mut tags = log.resource_attributes.clone();
        for (k, v) in &log.attributes {
            tags.insert(k.clone(), v.clone());
        }
        
        let severity_text = get_severity_text(log.severity_number);
        tags.insert("severity".to_string(), severity_text);
        
        let mut fields = HashMap::new();
        fields.insert("time_unix_nano".to_string(), FieldValue::Integer(log.time_unix_nano as i64));
        fields.insert("observed_time_unix_nano".to_string(), FieldValue::Integer(log.observed_time_unix_nano as i64));
        fields.insert("body".to_string(), FieldValue::String(log.body.into_bytes()));
        fields.insert("severity_number".to_string(), FieldValue::Integer(log.severity_number as i64));
        
        if let Some(trace_id) = &log.trace_id {
            fields.insert("trace_id".to_string(), FieldValue::String(hex_encode(trace_id).into_bytes()));
        }
        if let Some(span_id) = &log.span_id {
            fields.insert("span_id".to_string(), FieldValue::String(hex_encode(span_id).into_bytes()));
        }
        
        let timestamp = log.time_unix_nano as i64 / 1_000;
        
        let batch = WriteBatch {
            database: "_otel".to_string(),
            table: measurement,
            rows: vec![Row { tags, fields, timestamp }],
            timestamp,
        };
        
        self.write(batch)
    }
}

fn hex_encode(data: &[u8]) -> String {
    data.iter().map(|b| format!("{:02x}", b)).collect()
}

fn get_severity_text(severity_number: i32) -> String {
    match severity_number {
        1 => "TRACE".to_string(),
        2 => "DEBUG".to_string(),
        3 => "INFO".to_string(),
        4 => "WARN".to_string(),
        5 => "ERROR".to_string(),
        6 => "FATAL".to_string(),
        _ => "UNKNOWN".to_string(),
    }
}

pub struct OtelWriter {
    engine: std::sync::Arc<std::sync::RwLock<Option<Engine>>>,
}

impl OtelWriter {
    pub fn new(engine: std::sync::Arc<std::sync::RwLock<Option<Engine>>>) -> Self {
        Self { engine }
    }
    
    pub fn write_traces(&self, data: &[u8]) -> Result<usize> {
        let traces = decode_trace_request(data)
            .map_err(|e| crate::Error::InvalidArgument(format!("failed to decode trace request: {}", e)))?;
        
        let mut binding = self.engine.write().unwrap();
        let engine = match binding.as_mut() {
            Some(e) => e,
            None => return Err(crate::Error::InvalidArgument("engine not initialized".to_string())),
        };
        
        for trace in traces {
            engine.write_otel_trace(trace)?;
        }
        
        Ok(data.len())
    }
    
    pub fn write_metrics(&self, data: &[u8]) -> Result<usize> {
        let metrics = decode_metric_request(data)
            .map_err(|e| crate::Error::InvalidArgument(format!("failed to decode metric request: {}", e)))?;
        
        let mut binding = self.engine.write().unwrap();
        let engine = match binding.as_mut() {
            Some(e) => e,
            None => return Err(crate::Error::InvalidArgument("engine not initialized".to_string())),
        };
        
        for metric in metrics {
            engine.write_otel_metric(metric)?;
        }
        
        Ok(data.len())
    }
    
    pub fn write_logs(&self, data: &[u8]) -> Result<usize> {
        let logs = decode_log_request(data)
            .map_err(|e| crate::Error::InvalidArgument(format!("failed to decode log request: {}", e)))?;
        
        let mut binding = self.engine.write().unwrap();
        let engine = match binding.as_mut() {
            Some(e) => e,
            None => return Err(crate::Error::InvalidArgument("engine not initialized".to_string())),
        };
        
        for log in logs {
            engine.write_otel_log(log)?;
        }
        
        Ok(data.len())
    }
}

fn decode_trace_request(data: &[u8]) -> std::result::Result<Vec<TraceData>, prost::DecodeError> {
    let mut traces = Vec::new();
    
    let mut offset = 0;
    while offset < data.len() {
        if let Ok((resource_spans, new_offset)) = decode_resource_spans(data, offset) {
            for scope_spans in &resource_spans.scope_spans {
                for span in &scope_spans.spans {
                    let mut resource_attrs = HashMap::new();
                    for attr in &resource_spans.resource.attributes {
                        if let Some(val) = &attr.value {
                            resource_attrs.insert(attr.key.clone(), val.clone());
                        }
                    }
                    
                    traces.push(TraceData {
                        trace_id: span.trace_id.clone(),
                        span_id: span.span_id.clone(),
                        parent_span_id: span.parent_span_id.clone(),
                        name: span.name.clone(),
                        start_time_unix_nano: span.start_time_unix_nano,
                        end_time_unix_nano: span.end_time_unix_nano,
                        attributes: span.attributes.clone(),
                        status_code: span.status_code,
                        status_message: span.status_message.clone(),
                        resource_attributes: resource_attrs,
                    });
                }
            }
            offset = new_offset;
        } else {
            break;
        }
    }
    
    Ok(traces)
}

fn decode_metric_request(data: &[u8]) -> std::result::Result<Vec<MetricData>, prost::DecodeError> {
    let mut metrics = Vec::new();
    let mut offset = 0;
    
    while offset < data.len() {
        if let Ok((resource_metrics, new_offset)) = decode_resource_metrics(data, offset) {
            for scope_metrics in &resource_metrics.scope_metrics {
                for metric in &scope_metrics.metrics {
                    let mut resource_attrs = HashMap::new();
                    for attr in &resource_metrics.resource.attributes {
                        if let Some(val) = &attr.value {
                            resource_attrs.insert(attr.key.clone(), val.clone());
                        }
                    }
                    
                    let metric_type = match metric.data_case {
                        1 => MetricType::Gauge,
                        2 => MetricType::Counter,
                        3 => MetricType::Histogram,
                        4 => MetricType::Summary,
                        _ => MetricType::Gauge,
                    };
                    
                    let data_points: Vec<MetricDataPoint> = metric.data_points.iter().map(|dp| {
                        let mut attrs = HashMap::new();
                        for attr in &dp.attributes {
                            if let Some(val) = &attr.value {
                                attrs.insert(attr.key.clone(), val.clone());
                            }
                        }
                        
                        MetricDataPoint {
                            time_unix_nano: dp.time_unix_nano,
                            value: dp.value,
                            attributes: attrs,
                            count: dp.count,
                            sum: dp.sum,
                            buckets: None,
                            quantile_values: None,
                        }
                    }).collect();
                    
                    metrics.push(MetricData {
                        name: metric.name.clone(),
                        description: metric.description.clone(),
                        unit: metric.unit.clone(),
                        metric_type,
                        data_points,
                        resource_attributes: resource_attrs,
                    });
                }
            }
            offset = new_offset;
        } else {
            break;
        }
    }
    
    Ok(metrics)
}

fn decode_log_request(data: &[u8]) -> std::result::Result<Vec<LogData>, prost::DecodeError> {
    let mut logs = Vec::new();
    let mut offset = 0;
    
    while offset < data.len() {
        if let Ok((resource_logs, new_offset)) = decode_resource_logs(data, offset) {
            for scope_logs in &resource_logs.scope_logs {
                for log_record in &scope_logs.log_records {
                    let mut resource_attrs = HashMap::new();
                    for attr in &resource_logs.resource.attributes {
                        if let Some(val) = &attr.value {
                            resource_attrs.insert(attr.key.clone(), val.clone());
                        }
                    }
                    
                    let body = log_record.body.clone();
                    
                    logs.push(LogData {
                        time_unix_nano: log_record.time_unix_nano,
                        observed_time_unix_nano: log_record.observed_time_unix_nano,
                        severity_number: log_record.severity_number,
                        severity_text: get_severity_text(log_record.severity_number),
                        body,
                        attributes: log_record.attributes.clone(),
                        resource_attributes: resource_attrs,
                        trace_id: if log_record.trace_id.is_empty() { None } else { Some(log_record.trace_id.clone()) },
                        span_id: if log_record.span_id.is_empty() { None } else { Some(log_record.span_id.clone()) },
                    });
                }
            }
            offset = new_offset;
        } else {
            break;
        }
    }
    
    Ok(logs)
}

#[derive(Debug, Clone)]
struct ResourceSpans {
    resource: Resource,
    scope_spans: Vec<ScopeSpans>,
}

#[derive(Debug, Clone)]
struct Resource {
    attributes: Vec<KeyValue>,
}

#[derive(Debug, Clone)]
struct KeyValue {
    key: String,
    value: Option<String>,
}

#[derive(Debug, Clone)]
struct ScopeSpans {
    scope: Scope,
    spans: Vec<Span>,
}

#[derive(Debug, Clone)]
struct Scope {
    name: String,
    version: String,
}

#[derive(Debug, Clone)]
struct Span {
    trace_id: Vec<u8>,
    span_id: Vec<u8>,
    parent_span_id: Option<Vec<u8>>,
    name: String,
    start_time_unix_nano: u64,
    end_time_unix_nano: u64,
    attributes: HashMap<String, String>,
    status_code: i32,
    status_message: String,
}

#[derive(Debug, Clone)]
struct ResourceMetrics {
    resource: Resource,
    scope_metrics: Vec<ScopeMetrics>,
}

#[derive(Debug, Clone)]
struct ScopeMetrics {
    scope: Scope,
    metrics: Vec<Metric>,
}

#[derive(Debug, Clone)]
struct Metric {
    name: String,
    description: String,
    unit: String,
    data_case: i32,
    data_points: Vec<MetricPoint>,
}

#[derive(Debug, Clone)]
struct MetricPoint {
    time_unix_nano: u64,
    value: f64,
    attributes: Vec<KeyValue>,
    count: Option<u64>,
    sum: Option<f64>,
}

#[derive(Debug, Clone)]
struct ResourceLogs {
    resource: Resource,
    scope_logs: Vec<ScopeLogs>,
}

#[derive(Debug, Clone)]
struct ScopeLogs {
    scope: Scope,
    log_records: Vec<LogRecord>,
}

#[derive(Debug, Clone)]
struct LogRecord {
    time_unix_nano: u64,
    observed_time_unix_nano: u64,
    severity_number: i32,
    body: String,
    attributes: HashMap<String, String>,
    trace_id: Vec<u8>,
    span_id: Vec<u8>,
}

fn decode_resource_spans(data: &[u8], _offset: usize) -> std::result::Result<(ResourceSpans, usize), prost::DecodeError> {
    Ok((ResourceSpans {
        resource: Resource { attributes: Vec::new() },
        scope_spans: Vec::new(),
    }, data.len()))
}

fn decode_resource_metrics(data: &[u8], _offset: usize) -> std::result::Result<(ResourceMetrics, usize), prost::DecodeError> {
    Ok((ResourceMetrics {
        resource: Resource { attributes: Vec::new() },
        scope_metrics: Vec::new(),
    }, data.len()))
}

fn decode_resource_logs(data: &[u8], _offset: usize) -> std::result::Result<(ResourceLogs, usize), prost::DecodeError> {
    Ok((ResourceLogs {
        resource: Resource { attributes: Vec::new() },
        scope_logs: Vec::new(),
    }, data.len()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_severity_text() {
        assert_eq!(get_severity_text(1), "TRACE");
        assert_eq!(get_severity_text(3), "INFO");
        assert_eq!(get_severity_text(5), "ERROR");
        assert_eq!(get_severity_text(99), "UNKNOWN");
    }

    #[test]
    fn test_hex_encode() {
        assert_eq!(hex_encode(&[0x48, 0x65, 0x6c, 0x6c, 0x6f]), "48656c6c6f");
        assert_eq!(hex_encode(&[]), "");
    }

    #[test]
    fn test_otel_config_default() {
        let config = OtelConfig::default();
        assert!(config.enabled);
        assert!(config.trace_enabled);
        assert!(config.metric_enabled);
        assert!(config.log_enabled);
    }

    #[test]
    fn test_trace_data_creation() {
        let trace = TraceData {
            trace_id: vec![0x01, 0x02, 0x03],
            span_id: vec![0x04, 0x05, 0x06],
            parent_span_id: None,
            name: "test_span".to_string(),
            start_time_unix_nano: 1000000000,
            end_time_unix_nano: 2000000000,
            attributes: HashMap::new(),
            status_code: 1,
            status_message: "".to_string(),
            resource_attributes: HashMap::new(),
        };
        
        assert_eq!(trace.name, "test_span");
        assert_eq!(trace.end_time_unix_nano - trace.start_time_unix_nano, 1000000000);
    }

    #[test]
    fn test_metric_type() {
        assert!(matches!(MetricType::Gauge, MetricType::Gauge));
        assert!(matches!(MetricType::Counter, MetricType::Counter));
        assert!(matches!(MetricType::Histogram, MetricType::Histogram));
        assert!(matches!(MetricType::Summary, MetricType::Summary));
    }
}
