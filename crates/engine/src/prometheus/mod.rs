use crate::{Engine, WriteBatch, Row, FieldValue, QueryRequest, TimeRange};
use crate::error::{Error, Result};
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct PrometheusConfig {
    pub remote_write_enabled: bool,
    pub remote_read_enabled: bool,
    pub promql_enabled: bool,
}

impl Default for PrometheusConfig {
    fn default() -> Self {
        Self {
            remote_write_enabled: true,
            remote_read_enabled: true,
            promql_enabled: true,
        }
    }
}

#[derive(Debug, Clone)]
pub struct RemoteWriteRequest {
    pub timeseries: Vec<TimeSeries>,
}

#[derive(Debug, Clone)]
pub struct TimeSeries {
    pub labels: Vec<Label>,
    pub samples: Vec<Sample>,
}

#[derive(Debug, Clone)]
pub struct Label {
    pub name: String,
    pub value: String,
}

#[derive(Debug, Clone)]
pub struct Sample {
    pub value: f64,
    pub timestamp: i64,
}

impl Engine {
    pub fn write_prometheus(&mut self, req: RemoteWriteRequest) -> Result<()> {
        for ts in req.timeseries {
            let measurement = extract_measurement_name(&ts.labels);
            let tags = extract_tags(&ts.labels);
            
            let measurement_clone = measurement.clone();
            let tags_clone = tags.clone();
            
            for sample in ts.samples {
                let mut fields = HashMap::new();
                fields.insert("value".to_string(), FieldValue::Float(sample.value));
                
                let row = Row {
                    tags: tags_clone.clone(),
                    fields,
                    timestamp: sample.timestamp * 1_000_000,
                };
                
                let batch = WriteBatch {
                    database: "_prometheus".to_string(),
                    table: measurement_clone.clone(),
                    rows: vec![row],
                    timestamp: sample.timestamp * 1_000_000,
                };
                
                self.write(batch)?;
            }
        }
        Ok(())
    }
    
    pub fn read_prometheus(&self, query: &PrometheusQuery) -> Result<PrometheusReadResponse> {
        let mut results = Vec::new();
        
        for matcher in &query.matchers {
            let measurement = &matcher.name;
            
            let time_range = TimeRange {
                start: query.start_timestamp_ms * 1_000_000,
                end: query.end_timestamp_ms * 1_000_000,
            };
            
            let req = QueryRequest::new(
                "_prometheus".to_string(),
                measurement.clone(),
                time_range,
            );
            
            let result = self.query(req)?;
            
            let mut timeseries = Vec::new();
            for row in result.rows {
                let labels = build_labels(&row.tags);
                let samples = vec![
                    Sample {
                        value: row.fields.get("value").and_then(|v| v.as_f64()).unwrap_or(0.0),
                        timestamp: row.timestamp / 1_000_000,
                    }
                ];
                timeseries.push(TimeSeries { labels, samples });
            }
            
            results.push(QueryResult { timeseries });
        }
        
        Ok(PrometheusReadResponse { results })
    }
}

#[derive(Debug, Clone)]
pub struct PrometheusQuery {
    pub start_timestamp_ms: i64,
    pub end_timestamp_ms: i64,
    pub matchers: Vec<Matcher>,
}

#[derive(Debug, Clone)]
pub struct Matcher {
    pub name: String,
    pub label_selectors: Vec<LabelSelector>,
}

#[derive(Debug, Clone)]
pub enum LabelSelector {
    Equal(String),
    NotEqual(String),
    RegexEqual(String),
    RegexNotEqual(String),
}

#[derive(Debug, Clone)]
pub struct PrometheusReadResponse {
    pub results: Vec<QueryResult>,
}

#[derive(Debug, Clone)]
pub struct QueryResult {
    pub timeseries: Vec<TimeSeries>,
}

fn extract_measurement_name(labels: &[Label]) -> String {
    for label in labels {
        if label.name == "__name__" {
            return label.value.clone();
        }
    }
    "unknown".to_string()
}

fn extract_tags(labels: &[Label]) -> HashMap<String, String> {
    labels
        .iter()
        .filter(|label| !label.name.starts_with("__") && label.name != "__name__")
        .map(|label| (label.name.clone(), label.value.clone()))
        .collect()
}

fn build_labels(tags: &HashMap<String, String>) -> Vec<Label> {
    let mut labels = vec![
        Label {
            name: "__name__".to_string(),
            value: "metric".to_string(),
        }
    ];
    
    for (k, v) in tags {
        labels.push(Label {
            name: k.clone(),
            value: v.clone(),
        });
    }
    
    labels
}

pub struct PrometheusWriter {
    engine: std::sync::Arc<std::sync::RwLock<Option<Engine>>>,
}

impl PrometheusWriter {
    pub fn new(engine: std::sync::Arc<std::sync::RwLock<Option<Engine>>>) -> Self {
        Self { engine }
    }
    
    pub fn write(&self, data: &[u8]) -> Result<usize> {
        let request = decode_write_request(data)
            .map_err(|e| Error::InvalidArgument(format!("failed to decode prometheus write request: {}", e)))?;
        
        let mut binding = self.engine.write().unwrap();
        let engine = match binding.as_mut() {
            Some(e) => e,
            None => return Err(Error::InvalidArgument("engine not initialized".to_string())),
        };
        engine.write_prometheus(request)?;
        
        Ok(data.len())
    }
}

pub struct PrometheusReader {
    engine: std::sync::Arc<std::sync::RwLock<Option<Engine>>>,
}

impl PrometheusReader {
    pub fn new(engine: std::sync::Arc<std::sync::RwLock<Option<Engine>>>) -> Self {
        Self { engine }
    }
    
    pub fn read(&self, data: &[u8]) -> Result<Vec<u8>> {
        let request = decode_read_request(data)
            .map_err(|e| Error::InvalidArgument(format!("failed to decode prometheus read request: {}", e)))?;
        
        let binding = self.engine.read().unwrap();
        let engine = match binding.as_ref() {
            Some(e) => e,
            None => return Err(Error::InvalidArgument("engine not initialized".to_string())),
        };
        let response = engine.read_prometheus(&request)?;
        
        encode_read_response(&response)
            .map_err(|e| Error::InvalidArgument(format!("failed to encode prometheus read response: {}", e)))
    }
}

fn decode_write_request(data: &[u8]) -> std::result::Result<RemoteWriteRequest, prost::DecodeError> {
    let mut timeseries = Vec::new();
    let mut offset = 0;
    
    while offset < data.len() {
        if offset + 8 > data.len() {
            break;
        }
        
        let field_number = (data[offset] >> 3) as u32;
        let wire_type = data[offset] & 0x7;
        offset += 1;
        
        if field_number == 1 && wire_type == 2 {
            let (msg_len, new_offset) = decode_varint(data, offset)?;
            offset = new_offset;
            
            if offset + msg_len as usize > data.len() {
                break;
            }
            
            let ts_data = &data[offset..offset + msg_len as usize];
            if let Ok(ts) = decode_timeseries(ts_data) {
                timeseries.push(ts);
            }
            offset += msg_len as usize;
        } else {
            break;
        }
    }
    
    Ok(RemoteWriteRequest { timeseries })
}

fn decode_timeseries(data: &[u8]) -> std::result::Result<TimeSeries, prost::DecodeError> {
    let mut labels = Vec::new();
    let mut samples = Vec::new();
    let mut offset = 0;
    
    while offset < data.len() {
        if offset >= data.len() {
            break;
        }
        
        let field_number = (data[offset] >> 3) as u32;
        let wire_type = data[offset] & 0x7;
        offset += 1;
        
        match field_number {
            1 => {
                if wire_type == 2 {
                    let (msg_len, new_offset) = decode_varint(data, offset)?;
                    offset = new_offset;
                    if offset + msg_len as usize > data.len() {
                        break;
                    }
                    let label_data = &data[offset..offset + msg_len as usize];
                    if let Ok(label) = decode_label(label_data) {
                        labels.push(label);
                    }
                    offset += msg_len as usize;
                }
            }
            2 => {
                if wire_type == 2 {
                    let (msg_len, new_offset) = decode_varint(data, offset)?;
                    offset = new_offset;
                    if offset + msg_len as usize > data.len() {
                        break;
                    }
                    let sample_data = &data[offset..offset + msg_len as usize];
                    if let Ok(sample) = decode_sample(sample_data) {
                        samples.push(sample);
                    }
                    offset += msg_len as usize;
                }
            }
            _ => {
                break;
            }
        }
    }
    
    Ok(TimeSeries { labels, samples })
}

fn decode_label(data: &[u8]) -> std::result::Result<Label, prost::DecodeError> {
    let mut name = String::new();
    let mut value = String::new();
    let mut offset = 0;
    
    while offset < data.len() {
        let field_number = (data[offset] >> 3) as u32;
        let wire_type = data[offset] & 0x7;
        offset += 1;
        
        match field_number {
            1 => {
                if wire_type == 2 {
                    let (len, new_offset) = decode_varint(data, offset)?;
                    offset = new_offset;
                    if offset + len as usize <= data.len() {
                        name = String::from_utf8_lossy(&data[offset..offset + len as usize]).to_string();
                        offset += len as usize;
                    }
                }
            }
            2 => {
                if wire_type == 2 {
                    let (len, new_offset) = decode_varint(data, offset)?;
                    offset = new_offset;
                    if offset + len as usize <= data.len() {
                        value = String::from_utf8_lossy(&data[offset..offset + len as usize]).to_string();
                        offset += len as usize;
                    }
                }
            }
            _ => break,
        }
    }
    
    Ok(Label { name, value })
}

fn decode_sample(data: &[u8]) -> std::result::Result<Sample, prost::DecodeError> {
    let mut value = 0.0_f64;
    let mut timestamp = 0_i64;
    let mut offset = 0;
    
    while offset < data.len() {
        let field_number = (data[offset] >> 3) as u32;
        let wire_type = data[offset] & 0x7;
        offset += 1;
        
        match field_number {
            1 => {
                if wire_type == 1 {
                    if offset + 8 <= data.len() {
                        value = f64::from_le_bytes(data[offset..offset + 8].try_into().unwrap());
                        offset += 8;
                    }
                }
            }
            2 => {
                if wire_type == 1 {
                    if offset + 8 <= data.len() {
                        timestamp = i64::from_le_bytes(data[offset..offset + 8].try_into().unwrap());
                        offset += 8;
                    }
                }
            }
            _ => break,
        }
    }
    
    Ok(Sample { value, timestamp })
}

fn decode_read_request(data: &[u8]) -> std::result::Result<PrometheusQuery, prost::DecodeError> {
    let mut query = PrometheusQuery {
        start_timestamp_ms: 0,
        end_timestamp_ms: 0,
        matchers: Vec::new(),
    };
    
    let mut offset = 0;
    
    while offset < data.len() {
        if offset >= data.len() {
            break;
        }
        
        let field_number = (data[offset] >> 3) as u32;
        let wire_type = data[offset] & 0x7;
        offset += 1;
        
        match field_number {
            1 => {
                if wire_type == 2 {
                    let (msg_len, new_offset) = decode_varint(data, offset)?;
                    offset = new_offset;
                    if offset + msg_len as usize <= data.len() {
                        let query_data = &data[offset..offset + msg_len as usize];
                        if let Ok((start, end, matchers)) = decode_query(query_data) {
                            query.start_timestamp_ms = start;
                            query.end_timestamp_ms = end;
                            query.matchers = matchers;
                        }
                        offset += msg_len as usize;
                    }
                }
            }
            _ => break,
        }
    }
    
    Ok(query)
}

fn decode_query(data: &[u8]) -> std::result::Result<(i64, i64, Vec<Matcher>), prost::DecodeError> {
    let mut start_timestamp_ms = 0_i64;
    let mut end_timestamp_ms = 0_i64;
    let mut matchers = Vec::new();
    let mut offset = 0;
    
    while offset < data.len() {
        let field_number = (data[offset] >> 3) as u32;
        let wire_type = data[offset] & 0x7;
        offset += 1;
        
        match field_number {
            1 => {
                if wire_type == 1 {
                    if offset + 8 <= data.len() {
                        start_timestamp_ms = i64::from_le_bytes(data[offset..offset + 8].try_into().unwrap());
                        offset += 8;
                    }
                }
            }
            2 => {
                if wire_type == 1 {
                    if offset + 8 <= data.len() {
                        end_timestamp_ms = i64::from_le_bytes(data[offset..offset + 8].try_into().unwrap());
                        offset += 8;
                    }
                }
            }
            3 => {
                if wire_type == 2 {
                    let (msg_len, new_offset) = decode_varint(data, offset)?;
                    offset = new_offset;
                    if offset + msg_len as usize <= data.len() {
                        let matcher_data = &data[offset..offset + msg_len as usize];
                        if let Ok(matcher) = decode_matcher(matcher_data) {
                            matchers.push(matcher);
                        }
                        offset += msg_len as usize;
                    }
                }
            }
            _ => break,
        }
    }
    
    Ok((start_timestamp_ms, end_timestamp_ms, matchers))
}

fn decode_matcher(data: &[u8]) -> std::result::Result<Matcher, prost::DecodeError> {
    let mut name = String::new();
    let label_selectors = Vec::new();
    let mut offset = 0;
    
    while offset < data.len() {
        let field_number = (data[offset] >> 3) as u32;
        let wire_type = data[offset] & 0x7;
        offset += 1;
        
        match field_number {
            1 => {
                if wire_type == 2 {
                    let (len, new_offset) = decode_varint(data, offset)?;
                    offset = new_offset;
                    if offset + len as usize <= data.len() {
                        name = String::from_utf8_lossy(&data[offset..offset + len as usize]).to_string();
                        offset += len as usize;
                    }
                }
            }
            _ => break,
        }
    }
    
    Ok(Matcher { name, label_selectors })
}

fn decode_varint(data: &[u8], offset: usize) -> std::result::Result<(u64, usize), prost::DecodeError> {
    let mut result = 0_u64;
    let mut shift = 0;
    let mut new_offset = offset;
    
    while new_offset < data.len() {
        let byte = data[new_offset];
        new_offset += 1;
        result |= ((byte & 0x7F) as u64) << shift;
        if byte & 0x80 == 0 {
            return Ok((result, new_offset));
        }
        shift += 7;
        if shift >= 64 {
            break;
        }
    }
    
    Err(prost::DecodeError::new("invalid varint"))
}

fn encode_read_response(response: &PrometheusReadResponse) -> std::result::Result<Vec<u8>, prost::EncodeError> {
    let mut buf = Vec::new();
    
    for result in &response.results {
        let ts_data = encode_query_result(result)?;
        buf.extend_from_slice(&[0x0a]);
        buf.extend_from_slice(&encode_varint(ts_data.len() as u64));
        buf.extend_from_slice(&ts_data);
    }
    
    Ok(buf)
}

fn encode_query_result(result: &QueryResult) -> std::result::Result<Vec<u8>, prost::EncodeError> {
    let mut buf = Vec::new();
    
    for ts in &result.timeseries {
        let ts_data = encode_timeseries(ts)?;
        buf.extend_from_slice(&[0x0a]);
        buf.extend_from_slice(&encode_varint(ts_data.len() as u64));
        buf.extend_from_slice(&ts_data);
    }
    
    Ok(buf)
}

fn encode_timeseries(ts: &TimeSeries) -> std::result::Result<Vec<u8>, prost::EncodeError> {
    let mut buf = Vec::new();
    
    for label in &ts.labels {
        let label_data = encode_label(label)?;
        buf.extend_from_slice(&[0x0a]);
        buf.extend_from_slice(&encode_varint(label_data.len() as u64));
        buf.extend_from_slice(&label_data);
    }
    
    for sample in &ts.samples {
        let sample_data = encode_sample(sample)?;
        buf.extend_from_slice(&[0x12]);
        buf.extend_from_slice(&encode_varint(sample_data.len() as u64));
        buf.extend_from_slice(&sample_data);
    }
    
    Ok(buf)
}

fn encode_label(label: &Label) -> std::result::Result<Vec<u8>, prost::EncodeError> {
    let mut buf = Vec::new();
    
    buf.extend_from_slice(&[0x0a]);
    buf.extend_from_slice(&encode_varint(label.name.len() as u64));
    buf.extend_from_slice(label.name.as_bytes());
    
    buf.extend_from_slice(&[0x12]);
    buf.extend_from_slice(&encode_varint(label.value.len() as u64));
    buf.extend_from_slice(label.value.as_bytes());
    
    Ok(buf)
}

fn encode_sample(sample: &Sample) -> std::result::Result<Vec<u8>, prost::EncodeError> {
    let mut buf = Vec::new();
    
    buf.extend_from_slice(&[0x09]);
    buf.extend_from_slice(&sample.value.to_le_bytes());
    
    buf.extend_from_slice(&[0x11]);
    buf.extend_from_slice(&sample.timestamp.to_le_bytes());
    
    Ok(buf)
}

fn encode_varint(mut value: u64) -> Vec<u8> {
    let mut buf = Vec::new();
    loop {
        if value & !0x7F == 0 {
            buf.push(value as u8);
            break;
        } else {
            buf.push((value & 0x7F) as u8 | 0x80);
            value >>= 7;
        }
    }
    buf
}

pub fn format_metrics(engine: &Engine) -> String {
    let stats = engine.get_stats();
    let mut output = String::new();
    
    output.push_str("# HELP openGemini_series_count Number of series in the database\n");
    output.push_str("# TYPE openGemini_series_count gauge\n");
    output.push_str(&format!("openGemini_series_count {}\n", stats.series_count));
    
    output.push_str("# HELP openGemini_memtable_size_bytes Size of the memtable in bytes\n");
    output.push_str("# TYPE openGemini_memtable_size_bytes gauge\n");
    output.push_str(&format!("openGemini_memtable_size_bytes {}\n", stats.memtable_size));
    
    output.push_str("# HELP openGemini_memtable_row_count Number of rows in the memtable\n");
    output.push_str("# TYPE openGemini_memtable_row_count gauge\n");
    output.push_str(&format!("openGemini_memtable_row_count {}\n", stats.memtable_row_count));
    
    output.push_str("# HELP openGemini_tssp_file_count Number of TSSP files\n");
    output.push_str("# TYPE openGemini_tssp_file_count gauge\n");
    output.push_str(&format!("openGemini_tssp_file_count {}\n", stats.tssp_file_count));
    
    output.push_str("# HELP openGemini_wal_entries Number of entries in WAL\n");
    output.push_str("# TYPE openGemini_wal_entries gauge\n");
    output.push_str(&format!("openGemini_wal_entries {}\n", stats.wal_entries));
    
    output.push_str("# HELP openGemini_shard_count Number of shards\n");
    output.push_str("# TYPE openGemini_shard_count gauge\n");
    output.push_str(&format!("openGemini_shard_count {}\n", stats.shard_count));
    
    output
}

#[derive(Debug)]
pub struct PrometheusQueryResult {
    pub metric: HashMap<String, String>,
    pub value: f64,
}

pub fn execute_promql_query(engine: &Engine, query: &str) -> PrometheusQueryResult {
    let metric = parse_promql_metric(query);
    
    let req = QueryRequest::new(
        "_prometheus".to_string(),
        metric.get("__name__").cloned().unwrap_or_else(|| "unknown".to_string()),
        TimeRange {
            start: 0,
            end: i64::MAX,
        },
    );
    
    let result = engine.query(req).unwrap_or_else(|_| crate::QueryResult {
        rows: Vec::new(),
        stats: crate::QueryStats::default(),
    });
    
    let value = if let Some(row) = result.rows.last() {
        row.fields.get("value").and_then(|v| v.as_f64()).unwrap_or(0.0)
    } else {
        0.0
    };
    
    PrometheusQueryResult { metric, value }
}

pub fn execute_promql_query_range(engine: &Engine, query: &str, start: i64, end: i64, step: i64) -> Vec<(i64, f64)> {
    let metric = parse_promql_metric(query);
    
    let req = QueryRequest::new(
        "_prometheus".to_string(),
        metric.get("__name__").cloned().unwrap_or_else(|| "unknown".to_string()),
        TimeRange {
            start: start * 1_000_000,
            end: end * 1_000_000,
        },
    );
    
    let result = engine.query(req).unwrap_or_else(|_| crate::QueryResult {
        rows: Vec::new(),
        stats: crate::QueryStats::default(),
    });
    
    let mut values = Vec::new();
    let mut current_time = start;
    
    while current_time <= end {
        let timestamp_ns = current_time * 1_000_000;
        
        let closest_value = result.rows.iter()
            .min_by_key(|r| (r.timestamp - timestamp_ns).abs())
            .and_then(|r| r.fields.get("value").and_then(|v| v.as_f64()))
            .unwrap_or(f64::NAN);
        
        values.push((current_time, closest_value));
        current_time += step;
    }
    
    values
}

fn parse_promql_metric(query: &str) -> HashMap<String, String> {
    let mut metric = HashMap::new();
    metric.insert("__name__".to_string(), "unknown".to_string());
    
    let query_lower = query.to_lowercase();
    
    if let Some(name_end) = query_lower.find("{") {
        let name = query[..name_end].trim().to_string();
        if !name.is_empty() && !name.contains("(") {
            metric.insert("__name__".to_string(), name);
        }
        
        let brace_start = query.find("{").unwrap_or(0);
        let brace_end = query.find("}").unwrap_or(query.len());
        let labels_str = &query[brace_start + 1..brace_end];
        
        for label_pair in labels_str.split(',') {
            let parts: Vec<&str> = label_pair.split('=').collect();
            if parts.len() == 2 {
                let key = parts[0].trim().to_string();
                let value = parts[1].trim_matches('"').trim_matches('\'').to_string();
                if !key.starts_with("__") {
                    metric.insert(key, value);
                }
            }
        }
    } else {
        let name = query.trim();
        if !name.is_empty() && !name.contains("(") {
            metric.insert("__name__".to_string(), name.to_string());
        }
    }
    
    metric
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_decode_write_request() {
        let req = RemoteWriteRequest {
            timeseries: vec![
                TimeSeries {
                    labels: vec![
                        Label { name: "__name__".to_string(), value: "cpu_usage".to_string() },
                        Label { name: "host".to_string(), value: "server1".to_string() },
                    ],
                    samples: vec![
                        Sample { value: 0.75, timestamp: 1234567890 },
                        Sample { value: 0.80, timestamp: 1234567891 },
                    ],
                },
            ],
        };
        
        assert_eq!(req.timeseries.len(), 1);
        assert_eq!(req.timeseries[0].labels.len(), 2);
        assert_eq!(req.timeseries[0].samples.len(), 2);
    }

    #[test]
    fn test_extract_measurement_name() {
        let labels = vec![
            Label { name: "__name__".to_string(), value: "http_requests".to_string() },
            Label { name: "method".to_string(), value: "GET".to_string() },
        ];
        
        let name = extract_measurement_name(&labels);
        assert_eq!(name, "http_requests");
    }

    #[test]
    fn test_extract_tags() {
        let labels = vec![
            Label { name: "__name__".to_string(), value: "http_requests".to_string() },
            Label { name: "method".to_string(), value: "GET".to_string() },
            Label { name: "status".to_string(), value: "200".to_string() },
        ];
        
        let tags = extract_tags(&labels);
        assert_eq!(tags.len(), 2);
        assert_eq!(tags.get("method"), Some(&"GET".to_string()));
        assert_eq!(tags.get("status"), Some(&"200".to_string()));
    }

    #[test]
    fn test_encode_varint() {
        assert_eq!(encode_varint(0), vec![0x00]);
        assert_eq!(encode_varint(127), vec![0x7f]);
        assert_eq!(encode_varint(128), vec![0x80, 0x01]);
        assert_eq!(encode_varint(300), vec![0xac, 0x02]);
    }

    #[test]
    fn test_build_labels() {
        let mut tags = HashMap::new();
        tags.insert("host".to_string(), "server1".to_string());
        tags.insert("region".to_string(), "us-east".to_string());
        
        let labels = build_labels(&tags);
        assert!(labels.iter().any(|l| l.name == "__name__" && l.value == "metric"));
        assert!(labels.iter().any(|l| l.name == "host" && l.value == "server1"));
        assert!(labels.iter().any(|l| l.name == "region" && l.value == "us-east"));
    }
}
