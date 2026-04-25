use crate::error::{Error, Result};
use crate::line_protocol::LineProtocolParser;
use crate::influxql::Parser;
use crate::prometheus;
use crate::{Engine, WriteBatch, Row, FieldValue, TimeRange, QueryRequest};
use crate::syscontrol::SystemControls;
use serde::{Deserialize, Serialize};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::io::{Read, Write as IoWrite};
use std::thread;
use std::sync::{Arc, RwLock, atomic::{AtomicUsize, Ordering}};

#[derive(Debug, Clone)]
pub struct HttpConfig {
    pub bind_addr: SocketAddr,
    pub read_timeout_secs: u64,
    pub write_timeout_secs: u64,
    pub max_connections: usize,
    pub max_concurrent_write_limit: usize,
    pub max_concurrent_query_limit: usize,
    pub max_enqueued_write_limit: usize,
    pub max_enqueued_query_limit: usize,
    pub write_request_rate_limit: f64,
    pub query_request_rate_limit: f64,
    pub cors_enabled: bool,
    pub auth_enabled: bool,
    pub compression_enabled: bool,
}

impl Default for HttpConfig {
    fn default() -> Self {
        Self {
            bind_addr: "0.0.0.0:8086".parse().unwrap(),
            read_timeout_secs: 30,
            write_timeout_secs: 30,
            max_connections: 100,
            max_concurrent_write_limit: 100,
            max_concurrent_query_limit: 200,
            max_enqueued_write_limit: 50,
            max_enqueued_query_limit: 100,
            write_request_rate_limit: 0.0,
            query_request_rate_limit: 0.0,
            cors_enabled: true,
            auth_enabled: false,
            compression_enabled: true,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct WriteParams {
    pub db: Option<String>,
    pub rp: Option<String>,
    pub precision: Option<String>,
    #[serde(default)]
    pub consistency: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct QueryParams {
    pub db: Option<String>,
    pub rp: Option<String>,
    pub query: Option<String>,
    pub chunked: Option<bool>,
    #[serde(default)]
    pub chunk_size: Option<usize>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PingResponse {
    pub status: String,
    pub version: String,
}

pub struct HttpServer {
    config: HttpConfig,
    engine: Arc<RwLock<Option<Engine>>>,
    running: Arc<RwLock<bool>>,
    version: String,
    active_writes: AtomicUsize,
    active_queries: AtomicUsize,
    syscontrol: Arc<RwLock<Option<SystemControls>>>,
}

impl HttpServer {
    pub fn new(config: HttpConfig) -> Self {
        Self {
            config,
            engine: Arc::new(RwLock::new(None)),
            running: Arc::new(RwLock::new(false)),
            version: env!("CARGO_PKG_VERSION").to_string(),
            active_writes: AtomicUsize::new(0),
            active_queries: AtomicUsize::new(0),
            syscontrol: Arc::new(RwLock::new(None)),
        }
    }

    pub fn with_engine(self, engine: Engine) -> Self {
        *self.engine.write().unwrap() = Some(engine);
        self
    }

    pub fn set_engine(&self, engine: Engine) {
        *self.engine.write().unwrap() = Some(engine);
    }

    pub fn set_syscontrol(&self, syscontrol: SystemControls) {
        *self.syscontrol.write().unwrap() = Some(syscontrol);
    }

    fn check_write_throttle(&self) -> Result<()> {
        let active = self.active_writes.load(Ordering::Relaxed);
        if active >= self.config.max_concurrent_write_limit {
            return Err(Error::InvalidArgument("write throttle: too many concurrent writes".to_string()));
        }
        if active >= self.config.max_enqueued_write_limit {
            return Err(Error::InvalidArgument("write throttle: write queue full".to_string()));
        }
        Ok(())
    }

    fn check_query_throttle(&self) -> Result<()> {
        let active = self.active_queries.load(Ordering::Relaxed);
        if active >= self.config.max_concurrent_query_limit {
            return Err(Error::InvalidArgument("query throttle: too many concurrent queries".to_string()));
        }
        if active >= self.config.max_enqueued_query_limit {
            return Err(Error::InvalidArgument("query throttle: query queue full".to_string()));
        }
        Ok(())
    }

    pub fn start(&self) -> Result<()> {
        let listener = TcpListener::bind(self.config.bind_addr)
            .map_err(|e| Error::InvalidArgument(format!("failed to bind: {}", e)))?;

        *self.running.write().unwrap() = true;

        let running = self.running.clone();
        let engine = self.engine.clone();
        let version = self.version.clone();
        let syscontrol = self.syscontrol.clone();

        thread::spawn(move || {
            for stream in listener.incoming() {
                let running = running.clone();
                let engine = engine.clone();
                let version = version.clone();
                let syscontrol = syscontrol.clone();

                if !*running.read().unwrap() {
                    break;
                }

                match stream {
                    Ok(stream) => {
                        let addr = stream.peer_addr().unwrap_or_else(|_| "0.0.0.0:0".parse().unwrap());
                        thread::spawn(move || {
                            if let Err(e) = handle_connection(stream, addr, &engine, &version, &syscontrol) {
                                eprintln!("connection error: {}", e);
                            }
                        });
                    }
                    Err(e) => {
                        eprintln!("incoming connection error: {}", e);
                    }
                }
            }
        });

        Ok(())
    }

    pub fn stop(&self) {
        *self.running.write().unwrap() = false;
    }

    pub fn is_running(&self) -> bool {
        *self.running.read().unwrap()
    }
}

fn handle_connection(
    mut stream: TcpStream,
    _addr: SocketAddr,
    engine: &Arc<RwLock<Option<Engine>>>,
    version: &str,
    syscontrol: &Arc<RwLock<Option<SystemControls>>>,
) -> Result<()> {
    let mut buffer = Vec::new();
    let mut tmp_buf = [0u8; 8192];
    
    loop {
        let n = stream.read(&mut tmp_buf)
            .map_err(|e| Error::InvalidArgument(format!("read error: {}", e)))?;
        if n == 0 {
            break;
        }
        buffer.extend_from_slice(&tmp_buf[..n]);
        
        if buffer.len() > 8192 * 4 {
            break;
        }
        
        if buffer.windows(4).any(|w| w == b"\r\n\r\n") {
            break;
        }
    }

    if buffer.is_empty() {
        return Ok(());
    }

    let header_end = buffer.windows(4).position(|w| w == b"\r\n\r\n").map(|p| p + 4).unwrap_or(buffer.len());
    
    let header_part = &buffer[..header_end];
    let body_part = if header_end < buffer.len() { &buffer[header_end..] } else { &[] };
    
    let request = String::from_utf8_lossy(header_part).to_string();
    let lines: Vec<&str> = request.lines().collect();

    if lines.is_empty() {
        return Ok(());
    }

    let first_line = lines[0].to_uppercase();

    let response = if first_line.starts_with("GET /PING") || first_line == "GET /PING" {
        let resp = PingResponse {
            status: "ok".to_string(),
            version: version.to_string(),
        };
        format_http_response(200, "OK", &serde_json::to_string(&resp).unwrap())
    } else if first_line.starts_with("POST /WRITE") || first_line.starts_with("GET /WRITE") {
        handle_write(&lines, body_part, engine)
    } else if first_line.starts_with("POST /QUERY") || first_line.starts_with("GET /QUERY") {
        handle_query(&lines, engine)
    } else if first_line.starts_with("POST /API/V1/WRITE") {
        handle_prometheus_write(body_part, engine)
    } else if first_line.starts_with("POST /API/V1/READ") || first_line.starts_with("GET /API/V1/READ") {
        handle_prometheus_read(body_part, engine)
    } else if first_line.starts_with("GET /API/V1/QUERY") {
        handle_prometheus_query(&lines, engine)
    } else if first_line.starts_with("GET /API/V1/QUERY_RANGE") {
        handle_prometheus_query_range(&lines, engine)
    } else if first_line.starts_with("GET /METRICS") {
        handle_metrics(engine)
    } else if first_line.starts_with("GET /STATUS") || first_line.starts_with("GET /STATUS ") {
        handle_status(engine)
    } else if first_line.starts_with("POST /DEBUG/CTRL") {
        handle_debug_ctrl(body_part, syscontrol)
    } else if first_line.starts_with("GET /") {
        let path = first_line.strip_prefix("GET ").unwrap_or("").split_whitespace().next().unwrap_or("");
        if path == "/" || path.is_empty() {
            format_http_response(200, "OK", "{\"status\": \"ok\"}")
        } else if path == "/ping" {
            let resp = PingResponse {
                status: "ok".to_string(),
                version: version.to_string(),
            };
            format_http_response(200, "OK", &serde_json::to_string(&resp).unwrap())
        } else {
            format_http_response(404, "Not Found", "404 Not Found")
        }
    } else {
        format_http_response(405, "Method Not Allowed", "Method Not Allowed")
    };

    stream.write_all(response.as_bytes())
        .map_err(|e| Error::InvalidArgument(format!("write error: {}", e)))?;
    stream.flush()
        .map_err(|e| Error::InvalidArgument(format!("flush error: {}", e)))?;

    Ok(())
}

fn handle_write(lines: &[&str], body: &[u8], engine: &Arc<RwLock<Option<Engine>>>) -> String {
    let mut binding = engine.write().unwrap();
    let engine_guard = match binding.as_mut() {
        Some(e) => e,
        None => return format_http_response(500, "Internal Server Error", "Engine not initialized"),
    };

    let first_line = lines.first().unwrap_or(&"");
    let mut db_name = String::new();
    let mut precision = String::from("ns");

    if let Some(query_start) = first_line.find("/write") {
        let after_write = &first_line[query_start + 6..];
        if let Some(query_start) = after_write.find('?') {
            let query_string = &after_write[query_start + 1..];
            for param in query_string.split('&') {
                let param_lower = param.to_lowercase();
                if param_lower.starts_with("db=") {
                    db_name = url_decode(&param[3..]);
                } else if param_lower.starts_with("precision=") {
                    precision = url_decode(&param[10..]);
                }
            }
        }
    }

    let parser = LineProtocolParser::new();
    let mut rows_written = 0;
    let mut parse_errors = Vec::new();

    let body_str = String::from_utf8_lossy(body).to_string();
    let lines_to_parse: Vec<&str> = if !body_str.is_empty() {
        body_str.lines().collect()
    } else {
        lines.iter().skip(1).copied().collect()
    };

    for (idx, line) in lines_to_parse.iter().enumerate() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }

        match parser.parse(line) {
            Ok(parsed) => {
                let timestamp = convert_timestamp(parsed.timestamp, &precision);
                let table_name = parsed.measurement.clone();
                let mut row = parsed.into_row();
                row.timestamp = timestamp;
                
                let batch = WriteBatch {
                    database: db_name.clone(),
                    table: table_name,
                    rows: vec![row],
                    timestamp: 0,
                };

                if engine_guard.write(batch).is_ok() {
                    rows_written += 1;
                }
            }
            Err(e) => {
                parse_errors.push(format!("line {}: {}", idx + 1, e));
            }
        }
    }

    if !parse_errors.is_empty() && rows_written == 0 {
        let error_msg = parse_errors.join("; ");
        return format_http_response(400, "Bad Request", &format!("{{\"error\":\"{}\"}}", error_msg));
    }

    if rows_written > 0 {
        format_http_response(204, "No Content", "")
    } else if parse_errors.is_empty() {
        format_http_response(204, "No Content", "")
    } else {
        format_http_response(500, "Internal Server Error", &format!("{{\"written\":{}}}", rows_written))
    }
}

fn url_decode(s: &str) -> String {
    let mut result = String::new();
    let mut chars = s.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '%' {
            let hex: String = chars.by_ref().take(2).collect();
            if hex.len() == 2 {
                if let Ok(byte) = u8::from_str_radix(&hex, 16) {
                    result.push(byte as char);
                } else {
                    result.push('%');
                    result.push_str(&hex);
                }
            } else {
                result.push('%');
                result.push_str(&hex);
            }
        } else if c == '+' {
            result.push(' ');
        } else {
            result.push(c);
        }
    }
    result
}

fn convert_timestamp(ts: i64, precision: &str) -> i64 {
    match precision {
        "u" | "us" => ts / 1000,
        "ms" => ts / 1_000_000,
        "s" => ts / 1_000_000_000,
        "m" => ts / (60 * 1_000_000_000),
        "h" => ts / (3600 * 1_000_000_000),
        _ => ts,
    }
}

fn handle_query(lines: &[&str], engine: &Arc<RwLock<Option<Engine>>>) -> String {
    let stmt = {
        let first_line = lines.first().unwrap_or(&"");
        let mut query_str = String::new();

        if let Some(path_start) = first_line.find("/query") {
            let after_query = &first_line[path_start + 6..];
            let query_part = if let Some(pos) = after_query.find('?') {
                &after_query[pos + 1..]
            } else {
                after_query
            };
            
            for param in query_part.split('&') {
                let param_lower = param.to_lowercase();
                if param_lower.starts_with("q=") {
                    query_str = url_decode(&param[2..]);
                }
            }
        }

        if query_str.is_empty() {
            return format_http_response(400, "Bad Request", "{\"error\":\"missing query parameter\"}");
        }

        let mut parser = Parser::new(&query_str);
        match parser.parse_statement() {
            Some(s) => s,
            None => {
                let err = format!("{{\"error\":\"invalid query: {}\"}}", query_str);
                return format_http_response(400, "Bad Request", &err);
            }
        }
    };

    match stmt {
        crate::influxql::Statement::Select(select) => {
            let filter = if let Some(ref cond) = select.condition {
                let mut parser = crate::influxql::Parser::new(cond);
                parser.parse_condition(cond)
            } else {
                None
            };
            
            let mut query = QueryRequest::new(
                "".to_string(),
                select.measurement.clone(),
                TimeRange {
                    start: 0,
                    end: i64::MAX,
                },
            ).with_limit(select.limit.unwrap_or(1000));
            
            if let Some(f) = filter {
                query = query.with_filter(f);
            }
            
            if !select.group_by.is_empty() {
                query = query.with_group_by(select.group_by.clone());
            }

            let guard = engine.read().unwrap();
            let engine_guard = match guard.as_ref() {
                Some(e) => e,
                None => return format_http_response(500, "Internal Server Error", "Engine not initialized"),
            };

            match engine_guard.query(query) {
                Ok(result) => {
                    let result_str = format_results(&select.measurement, &select.fields, &result);
                    format_http_response(200, "OK", &result_str)
                }
                Err(e) => format_http_response(500, "Internal Server Error", &e.to_string()),
            }
        }
        crate::influxql::Statement::CreateDatabase(name) => {
            let mut guard = engine.write().unwrap();
            let engine_guard = match guard.as_mut() {
                Some(e) => e,
                None => return format_http_response(500, "Internal Server Error", "Engine not initialized"),
            };
            if let Err(e) = engine_guard.create_database(&name) {
                return format_http_response(500, "Internal Server Error", &e.to_string());
            }
            format_http_response(200, "OK", "{\"results\":[{\"success\":true}]}")
        }
        crate::influxql::Statement::CreateRetentionPolicy(rp) => {
            let mut guard = engine.write().unwrap();
            let engine_guard = match guard.as_mut() {
                Some(e) => e,
                None => return format_http_response(500, "Internal Server Error", "Engine not initialized"),
            };
            if let Err(e) = engine_guard.create_retention_policy(&rp.database, &rp.name, rp.duration_seconds, rp.replica_count) {
                return format_http_response(500, "Internal Server Error", &e.to_string());
            }
            format_http_response(200, "OK", "{\"results\":[{\"success\":true}]}")
        }
        crate::influxql::Statement::Insert { measurement, tags, fields, timestamp } => {
            let mut guard = engine.write().unwrap();
            let engine_guard = match guard.as_mut() {
                Some(e) => e,
                None => return format_http_response(500, "Internal Server Error", "Engine not initialized"),
            };
            
            let fields_map: std::collections::HashMap<String, FieldValue> = fields
                .into_iter()
                .map(|(k, v)| (k, FieldValue::Float(v)))
                .collect();
            
            let row = Row {
                tags,
                fields: fields_map,
                timestamp: timestamp.unwrap_or(0),
            };
            
            let batch = WriteBatch {
                database: String::new(),
                table: measurement,
                rows: vec![row],
                timestamp: 0,
            };
            
            if let Err(e) = engine_guard.write(batch) {
                return format_http_response(500, "Internal Server Error", &e.to_string());
            }
            format_http_response(200, "OK", "{\"results\":[{\"success\":true}]}")
        }
        crate::influxql::Statement::DropDatabase(name) => {
            let mut guard = engine.write().unwrap();
            let engine_guard = match guard.as_mut() {
                Some(e) => e,
                None => return format_http_response(500, "Internal Server Error", "Engine not initialized"),
            };
            if let Err(e) = engine_guard.drop_database(&name) {
                return format_http_response(500, "Internal Server Error", &e.to_string());
            }
            format_http_response(200, "OK", "{\"results\":[{\"success\":true}]}")
        }
        crate::influxql::Statement::DropMeasurement(name) => {
            let mut guard = engine.write().unwrap();
            let engine_guard = match guard.as_mut() {
                Some(e) => e,
                None => return format_http_response(500, "Internal Server Error", "Engine not initialized"),
            };
            if let Err(e) = engine_guard.drop_measurement(&name) {
                return format_http_response(500, "Internal Server Error", &e.to_string());
            }
            format_http_response(200, "OK", "{\"results\":[{\"success\":true}]}")
        }
        crate::influxql::Statement::AlterDatabase(alter_db) => {
            let mut guard = engine.write().unwrap();
            let engine_guard = match guard.as_mut() {
                Some(e) => e,
                None => return format_http_response(500, "Internal Server Error", "Engine not initialized"),
            };
            if let Some(rp_name) = alter_db.rp_name {
                if let Some(duration) = alter_db.duration_seconds {
                    if let Err(e) = engine_guard.create_retention_policy(&alter_db.name, &rp_name, duration, alter_db.replica_count.unwrap_or(1)) {
                        return format_http_response(500, "Internal Server Error", &e.to_string());
                    }
                }
            }
            format_http_response(200, "OK", "{\"results\":[{\"success\":true}]}")
        }
        crate::influxql::Statement::DropSeries { condition: _ } => {
            let mut guard = engine.write().unwrap();
            let engine_guard = match guard.as_mut() {
                Some(e) => e,
                None => return format_http_response(500, "Internal Server Error", "Engine not initialized"),
            };
            if let Err(e) = engine_guard.drop_series(None) {
                return format_http_response(500, "Internal Server Error", &e.to_string());
            }
            format_http_response(200, "OK", "{\"results\":[{\"success\":true}]}")
        }
        crate::influxql::Statement::Delete { condition: _ } => {
            let mut guard = engine.write().unwrap();
            let engine_guard = match guard.as_mut() {
                Some(e) => e,
                None => return format_http_response(500, "Internal Server Error", "Engine not initialized"),
            };
            if let Err(e) = engine_guard.delete("", None) {
                return format_http_response(500, "Internal Server Error", &e.to_string());
            }
            format_http_response(200, "OK", "{\"results\":[{\"success\":true}]}")
        }
        crate::influxql::Statement::ShowSeries => {
            let guard = engine.read().unwrap();
            let engine_guard = match guard.as_ref() {
                Some(e) => e,
                None => return format_http_response(500, "Internal Server Error", "Engine not initialized"),
            };
            let series_count = engine_guard.get_series_count();
            let json = format!(r#"{{"results":[{{"series":[{{"name":"series","columns":["count"],"values":[["{series_count}"]]}}]}}]"}}"#, series_count = series_count);
            format_http_response(200, "OK", &json)
        }
        crate::influxql::Statement::ShowRetentionPolicies(db_name) => {
            let guard = engine.read().unwrap();
            let engine_guard = match guard.as_ref() {
                Some(e) => e,
                None => return format_http_response(500, "Internal Server Error", "Engine not initialized"),
            };
            let db = db_name.unwrap_or_else(|| "".to_string());
            let policies = engine_guard.get_retention_policies(&db);
            if policies.is_empty() {
                format_http_response(200, "OK", "{\"results\":[{\"series\":[{\"name\":\"retentionPolicies\",\"values\":[]}]}]}")
            } else {
                let values: Vec<String> = policies.iter()
                    .map(|(name, duration, replica)| format!(r#"["{}","{}","{}"]"#, name, duration, replica))
                    .collect();
                let values_json = values.join(",");
                format_http_response(200, "OK", &format!("{{\"results\":[{{\"series\":[{{\"name\":\"retentionPolicies\",\"columns\":[\"name\",\"duration\",\"replicaCount\"],\"values\":[{}]}}]}}]}}", values_json))
            }
        }
        crate::influxql::Statement::ShowDatabases => {
            let guard = engine.read().unwrap();
            let engine_guard = match guard.as_ref() {
                Some(e) => e,
                None => return format_http_response(500, "Internal Server Error", "Engine not initialized"),
            };
            let schema = engine_guard.schema().read().unwrap();
            let databases: Vec<Vec<String>> = schema.databases.keys()
                .map(|name| vec![name.clone()])
                .collect();
            if databases.is_empty() {
                format_http_response(200, "OK", "{\"results\":[{\"series\":[{\"name\":\"databases\",\"values\":[]}]}]}")
            } else {
                let values_json: String = databases.iter()
                    .map(|row| format!("[{}]", row.iter().map(|s| format!("\"{}\"", s)).collect::<Vec<_>>().join(",")))
                    .collect::<Vec<_>>()
                    .join(",");
                format_http_response(200, "OK", &format!("{{\"results\":[{{\"series\":[{{\"name\":\"databases\",\"values\":[{}]}}]}}]}}", values_json))
            }
        }
        crate::influxql::Statement::ShowMeasurements => {
            let guard = engine.read().unwrap();
            let engine_guard = match guard.as_ref() {
                Some(e) => e,
                None => return format_http_response(500, "Internal Server Error", "Engine not initialized"),
            };
            let measurements: Vec<Vec<String>> = engine_guard.measurements()
                .iter()
                .map(|name| vec![name.clone()])
                .collect();
            if measurements.is_empty() {
                format_http_response(200, "OK", "{\"results\":[{\"series\":[{\"name\":\"measurements\",\"values\":[]}]}]}")
            } else {
                let values_json: String = measurements.iter()
                    .map(|row| format!("[{}]", row.iter().map(|s| format!("\"{}\"", s)).collect::<Vec<_>>().join(",")))
                    .collect::<Vec<_>>()
                    .join(",");
                format_http_response(200, "OK", &format!("{{\"results\":[{{\"series\":[{{\"name\":\"measurements\",\"values\":[{}]}}]}}]}}", values_json))
            }
        }
        crate::influxql::Statement::ShowTagKeys(measurement) => {
            let guard = engine.read().unwrap();
            let engine_guard = match guard.as_ref() {
                Some(e) => e,
                None => return format_http_response(500, "Internal Server Error", "Engine not initialized"),
            };
            
            let tag_keys = if let Some(ref meas) = measurement {
                engine_guard.get_tag_keys(meas)
            } else {
                let mut all_keys = std::collections::HashSet::new();
                for meas in engine_guard.measurements() {
                    for key in engine_guard.get_tag_keys(&meas) {
                        all_keys.insert(key);
                    }
                }
                all_keys.into_iter().collect()
            };
            
            let tag_values: Vec<Vec<String>> = tag_keys.iter()
                .map(|k| vec![k.clone()])
                .collect();
            
            if tag_values.is_empty() {
                format_http_response(200, "OK", "{\"results\":[{\"series\":[{\"name\":\"tagKeys\",\"values\":[]}]}]}")
            } else {
                let values_json: String = tag_values.iter()
                    .map(|row| format!("[{}]", row.iter().map(|s| format!("\"{}\"", s)).collect::<Vec<_>>().join(",")))
                    .collect::<Vec<_>>()
                    .join(",");
                format_http_response(200, "OK", &format!("{{\"results\":[{{\"series\":[{{\"name\":\"tagKeys\",\"values\":[{}]}}]}}]}}", values_json))
            }
        }
        crate::influxql::Statement::ShowTagValues(measurement, tag_key) => {
            let guard = engine.read().unwrap();
            let engine_guard = match guard.as_ref() {
                Some(e) => e,
                None => return format_http_response(500, "Internal Server Error", "Engine not initialized"),
            };
            
            let mut all_values: Vec<String> = Vec::new();
            
            if let Some(ref meas) = measurement {
                if tag_key.is_empty() {
                    for key in engine_guard.get_tag_keys(meas) {
                        all_values.push(key);
                    }
                }
            }
            
            let values: Vec<Vec<String>> = all_values.iter()
                .map(|v| vec![v.clone()])
                .collect();
            
            if values.is_empty() {
                format_http_response(200, "OK", "{\"results\":[{\"series\":[{\"name\":\"tagValues\",\"values\":[]}]}]}")
            } else {
                let values_json: String = values.iter()
                    .map(|row| format!("[{}]", row.iter().map(|s| format!("\"{}\"", s)).collect::<Vec<_>>().join(",")))
                    .collect::<Vec<_>>()
                    .join(",");
                format_http_response(200, "OK", &format!("{{\"results\":[{{\"series\":[{{\"name\":\"tagValues\",\"values\":[{}]}}]}}]}}", values_json))
            }
        }
        crate::influxql::Statement::ShowFieldKeys(measurement) => {
            let guard = engine.read().unwrap();
            let engine_guard = match guard.as_ref() {
                Some(e) => e,
                None => return format_http_response(500, "Internal Server Error", "Engine not initialized"),
            };
            
            let field_keys = if let Some(ref meas) = measurement {
                engine_guard.get_field_keys(meas)
            } else {
                let mut all_keys = std::collections::HashSet::new();
                for meas in engine_guard.measurements() {
                    for key in engine_guard.get_field_keys(&meas) {
                        all_keys.insert(key);
                    }
                }
                all_keys.into_iter().collect()
            };
            
            let field_values: Vec<Vec<String>> = field_keys.iter()
                .map(|k| vec![k.clone()])
                .collect();
            
            if field_values.is_empty() {
                format_http_response(200, "OK", "{\"results\":[{\"series\":[{\"name\":\"fieldKeys\",\"values\":[]}]}]}")
            } else {
                let values_json: String = field_values.iter()
                    .map(|row| format!("[{}]", row.iter().map(|s| format!("\"{}\"", s)).collect::<Vec<_>>().join(",")))
                    .collect::<Vec<_>>()
                    .join(",");
                format_http_response(200, "OK", &format!("{{\"results\":[{{\"series\":[{{\"name\":\"fieldKeys\",\"values\":[{}]}}]}}]}}", values_json))
            }
        }
        crate::influxql::Statement::Use { database } => {
            format_http_response(200, "OK", &format!("{{\"results\":[{{\"success\":true,\"database\":\"{}\"}}]}}", database))
        }
        crate::influxql::Statement::Set { key, value } => {
            format_http_response(200, "OK", &format!("{{\"results\":[{{\"success\":true,\"key\":\"{}\",\"value\":\"{}\"}}]}}", key, value))
        }
        crate::influxql::Statement::Grant(_) => {
            format_http_response(200, "OK", "{\"results\":[{\"success\":true}]}")
        }
        crate::influxql::Statement::Revoke(_) => {
            format_http_response(200, "OK", "{\"results\":[{\"success\":true}]}")
        }
    }
}

fn handle_prometheus_write(body: &[u8], engine: &Arc<RwLock<Option<Engine>>>) -> String {
    use crate::prometheus::PrometheusWriter;
    let writer = PrometheusWriter::new(engine.clone());
    match writer.write(body) {
        Ok(_) => format_http_response(204, "No Content", ""),
        Err(e) => format_http_response(500, "Internal Server Error", &e.to_string()),
    }
}

fn handle_prometheus_read(body: &[u8], engine: &Arc<RwLock<Option<Engine>>>) -> String {
    use crate::prometheus::PrometheusReader;
    let reader = PrometheusReader::new(engine.clone());
    match reader.read(body) {
        Ok(response) => {
            let mut buf = Vec::new();
            buf.extend_from_slice(b"HTTP/1.1 200 OK\r\n");
            buf.extend_from_slice(b"Server: openGemini-rs/\r\n");
            buf.extend_from_slice(b"Content-Type: application/x-protobuf\r\n");
            buf.extend_from_slice(format!("Content-Length: {}\r\n", response.len()).as_bytes());
            buf.extend_from_slice(b"Connection: close\r\n");
            buf.extend_from_slice(b"\r\n");
            buf.extend_from_slice(&response);
            String::from_utf8_lossy(&buf).to_string()
        }
        Err(e) => format_http_response(500, "Internal Server Error", &e.to_string()),
    }
}

fn handle_prometheus_query(lines: &[&str], engine: &Arc<RwLock<Option<Engine>>>) -> String {
    let first_line = lines.first().unwrap_or(&"");
    let mut query_str = String::new();
    let mut time_str = String::from("0");

    if let Some(path_start) = first_line.find("/query") {
        let after_query = &first_line[path_start + 6..];
        let query_part = if let Some(pos) = after_query.find('?') {
            &after_query[pos + 1..]
        } else {
            after_query
        };

        for param in query_part.split('&') {
            let param_lower = param.to_lowercase();
            if param_lower.starts_with("q=") {
                query_str = url_decode(&param[2..]);
            } else if param_lower.starts_with("time=") {
                time_str = url_decode(&param[5..]);
            }
        }
    }

    if query_str.is_empty() {
        return format_http_response(400, "Bad Request", "{\"error\":\"missing query parameter\"}");
    }

    let binding = engine.read().unwrap();
    let engine_guard = match binding.as_ref() {
        Some(e) => e,
        None => return format_http_response(500, "Internal Server Error", "Engine not initialized"),
    };

    let result = crate::prometheus::execute_promql_query(engine_guard, &query_str);
    
    let response = format!(
        "{{\"status\":\"success\",\"data\":{{\"resultType\":\"vector\",\"result\":[{{\"metric\":{},\"value\":[{},{:?}]}}]}}}}",
        serde_json::to_string(&result.metric).unwrap(),
        time_str,
        result.value
    );
    
    format_http_response(200, "OK", &response)
}

fn handle_prometheus_query_range(lines: &[&str], engine: &Arc<RwLock<Option<Engine>>>) -> String {
    let first_line = lines.first().unwrap_or(&"");
    let mut query_str = String::new();
    let mut start_str = String::from("0");
    let mut end_str = String::from("0");
    let mut step_str = String::from("60");

    if let Some(path_start) = first_line.find("/query_range") {
        let after_query = &first_line[path_start + 11..];
        let query_part = if let Some(pos) = after_query.find('?') {
            &after_query[pos + 1..]
        } else {
            after_query
        };

        for param in query_part.split('&') {
            let param_lower = param.to_lowercase();
            if param_lower.starts_with("query=") {
                query_str = url_decode(&param[6..]);
            } else if param_lower.starts_with("start=") {
                start_str = url_decode(&param[6..]);
            } else if param_lower.starts_with("end=") {
                end_str = url_decode(&param[4..]);
            } else if param_lower.starts_with("step=") {
                step_str = url_decode(&param[5..]);
            }
        }
    }

    if query_str.is_empty() {
        return format_http_response(400, "Bad Request", "{\"error\":\"missing query parameter\"}");
    }

    let binding = engine.read().unwrap();
    let engine_guard = match binding.as_ref() {
        Some(e) => e,
        None => return format_http_response(500, "Internal Server Error", "Engine not initialized"),
    };

    let start: i64 = start_str.parse().unwrap_or(0);
    let end: i64 = end_str.parse().unwrap_or(0);
    let step: i64 = step_str.parse().unwrap_or(60);

    let values: Vec<(i64, f64)> = prometheus::execute_promql_query_range(engine_guard, &query_str, start, end, step);
    
    let mut result_values = Vec::new();
    for (ts, val) in &values {
        result_values.push(format!("[{},{:?}]", ts, val));
    }
    
    let response = format!(
        "{{\"status\":\"success\",\"data\":{{\"resultType\":\"matrix\",\"result\":[{{\"metric\":{{}},\"values\":[{}]}}]}}}}",
        result_values.join(",")
    );
    
    format_http_response(200, "OK", &response)
}

fn handle_metrics(engine: &Arc<RwLock<Option<Engine>>>) -> String {
    let binding = engine.read().unwrap();
    let engine_guard = match binding.as_ref() {
        Some(e) => e,
        None => return "HTTP/1.1 500 Internal Server Error\r\n\r\nEngine not initialized".to_string(),
    };

    let metrics = prometheus::format_metrics(engine_guard);
    
    let mut buf = Vec::new();
    buf.extend_from_slice(b"HTTP/1.1 200 OK\r\n");
    buf.extend_from_slice(b"Server: openGemini-rs\r\n");
    buf.extend_from_slice(b"Content-Type: text/plain; version=0.0.4\r\n");
    buf.extend_from_slice(format!("Content-Length: {}\r\n", metrics.len()).as_bytes());
    buf.extend_from_slice(b"Connection: close\r\n");
    buf.extend_from_slice(b"\r\n");
    buf.extend_from_slice(metrics.as_bytes());
    
    String::from_utf8_lossy(&buf).to_string()
}

#[derive(Debug, Serialize, Deserialize)]
struct StatusResponse {
    status: String,
    version: String,
    series_count: usize,
    memtable_size: u64,
    tssp_file_count: usize,
    shard_count: usize,
}

fn handle_status(engine: &Arc<RwLock<Option<Engine>>>) -> String {
    let binding = engine.read().unwrap();
    let engine_guard = match binding.as_ref() {
        Some(e) => e,
        None => return format_http_response(500, "Internal Server Error", "{{\"error\":\"Engine not initialized\"}}"),
    };

    let stats = engine_guard.get_stats();
    let resp = StatusResponse {
        status: "ok".to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        series_count: stats.series_count,
        memtable_size: stats.memtable_size,
        tssp_file_count: stats.tssp_file_count,
        shard_count: stats.shard_count,
    };

    format_http_response(200, "OK", &serde_json::to_string(&resp).unwrap())
}

fn handle_debug_ctrl(body: &[u8], syscontrol: &Arc<RwLock<Option<SystemControls>>>) -> String {
    let mut binding = match syscontrol.write() {
        Ok(b) => b,
        Err(_) => return format_http_response(500, "Internal Server Error", "{{\"error\":\"Lock poisoned\"}}"),
    };
    let syscontrol_guard = match binding.as_mut() {
        Some(sc) => sc,
        None => return format_http_response(500, "Internal Server Error", "{{\"error\":\"System controls not initialized\"}}"),
    };

    let body_str = String::from_utf8_lossy(body).to_string();
    
    #[derive(Deserialize)]
    struct CtrlRequest {
        command: String,
        #[serde(default)]
        switch_on: Option<String>,
        #[serde(default)]
        all_shards: Option<String>,
        #[serde(default)]
        duration: Option<String>,
        #[serde(default)]
        limit: Option<String>,
    }

    let req: CtrlRequest = match serde_json::from_str(&body_str) {
        Ok(r) => r,
        Err(e) => return format_http_response(400, "Bad Request", &format!("{{\"error\":\"Invalid JSON: {}\"}}", e)),
    };

    let params: Vec<(&str, &str)> = vec![
        ("switchon", req.switch_on.as_deref().unwrap_or("")),
        ("allshards", req.all_shards.as_deref().unwrap_or("")),
        ("duration", req.duration.as_deref().unwrap_or("")),
        ("limit", req.limit.as_deref().unwrap_or("")),
    ].into_iter().filter(|(_, v)| !v.is_empty()).collect();

    let response = syscontrol_guard.process_command(&req.command, &params);
    format_http_response(200, "OK", &serde_json::to_string(&response).unwrap())
}

fn format_results(measurement: &str, fields: &[crate::influxql::Field], result: &crate::QueryResult) -> String {
    let mut values = Vec::new();
    
    let has_aggregates = fields.iter().any(|f| matches!(f, crate::influxql::Field::Aggregate(_)));
    
    if has_aggregates {
        let mut row_values = Vec::new();
        row_values.push("0".to_string());
        for field in fields {
            match field {
                crate::influxql::Field::Raw(name) => {
                    if *name == "*" {
                        let first_row = result.rows.first();
                        if let Some(row) = first_row {
                            for v in row.fields.values() {
                                row_values.push(field_value_to_string(v));
                            }
                        }
                    } else {
                        let first_row = result.rows.first();
                        if let Some(row) = first_row {
                            if let Some(v) = row.fields.get(name) {
                                row_values.push(field_value_to_string(v));
                            } else {
                                row_values.push("".to_string());
                            }
                        } else {
                            row_values.push("".to_string());
                        }
                    }
                }
                crate::influxql::Field::Aggregate(func) => {
                    let aggregate_value = match func {
                        crate::influxql::AggregateFunc::Count(None) => {
                            FieldValue::Integer(result.rows.len() as i64)
                        }
                        crate::influxql::AggregateFunc::Count(Some(field_name)) => {
                            FieldValue::Integer(result.count(field_name) as i64)
                        }
                        crate::influxql::AggregateFunc::Sum(field_name) => {
                            result.sum(field_name).unwrap_or(FieldValue::Integer(0))
                        }
                        crate::influxql::AggregateFunc::Mean(field_name) => {
                            if let Some(mean) = result.mean(field_name) {
                                FieldValue::Float(mean)
                            } else {
                                FieldValue::Float(0.0)
                            }
                        }
                        crate::influxql::AggregateFunc::Min(field_name) => {
                            result.min(field_name).unwrap_or(FieldValue::Integer(0))
                        }
                        crate::influxql::AggregateFunc::Max(field_name) => {
                            result.max(field_name).unwrap_or(FieldValue::Integer(0))
                        }
                        crate::influxql::AggregateFunc::First(field_name) => {
                            result.first(field_name).unwrap_or(FieldValue::Integer(0))
                        }
                        crate::influxql::AggregateFunc::Last(field_name) => {
                            result.last(field_name).unwrap_or(FieldValue::Integer(0))
                        }
                    };
                    row_values.push(field_value_to_string(&aggregate_value));
                }
            }
        }
        values.push(format!("[{}]", row_values.iter().map(|s| format!("\"{}\"", s)).collect::<Vec<_>>().join(",")));
    } else {
        for row in &result.rows {
            let mut row_values = Vec::new();
            row_values.push(row.timestamp.to_string());
            for field in fields {
                match field {
                    crate::influxql::Field::Raw(name) => {
                        if *name == "*" {
                            for v in row.fields.values() {
                                row_values.push(field_value_to_string(v));
                            }
                        } else {
                            if let Some(v) = row.fields.get(name) {
                                row_values.push(field_value_to_string(v));
                            } else {
                                row_values.push("".to_string());
                            }
                        }
                    }
                    crate::influxql::Field::Aggregate(_) => {}
                }
            }
            values.push(format!("[{}]", row_values.iter().map(|s| format!("\"{}\"", s)).collect::<Vec<_>>().join(",")));
        }
    }

    let columns = if fields.iter().any(|f| matches!(f, crate::influxql::Field::Raw(name) if name == "*")) {
        vec!["time".to_string()]
    } else {
        let mut cols = vec!["time".to_string()];
        for f in fields {
            let name = match f {
                crate::influxql::Field::Raw(name) => name.clone(),
                crate::influxql::Field::Aggregate(func) => format!("{:?}", func),
            };
            cols.push(name);
        }
        cols
    };

    format!(
        "{{\"results\":[{{\"series\":[{{\"name\":\"{}\",\"columns\":{},\"values\":{}}}]}}]}}",
        measurement,
        serde_json::to_string(&columns).unwrap(),
        format!("[{}]", values.join(","))
    )
}

fn field_value_to_string(v: &FieldValue) -> String {
    match v {
        FieldValue::Integer(i) => i.to_string(),
        FieldValue::Float(f) => f.to_string(),
        FieldValue::String(s) => String::from_utf8_lossy(s).to_string(),
        FieldValue::Boolean(b) => b.to_string(),
        FieldValue::Unsigned(u) => u.to_string(),
    }
}

fn format_http_response(status_code: u16, status_text: &str, body: &str) -> String {
    let content_length = body.len();
    let body_if_no_content = if status_code == 204 { "" } else { body };

    format!(
        "HTTP/1.1 {} {}\r\n\
         Server: openGemini-rs/{}\r\n\
         Content-Type: application/json\r\n\
         Content-Length: {}\r\n\
         Connection: close\r\n\
         \r\n\
         {}",
        status_code,
        status_text,
        env!("CARGO_PKG_VERSION"),
        content_length,
        body_if_no_content
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_http_response() {
        let resp = format_http_response(200, "OK", "{\"test\": true}");
        assert!(resp.contains("HTTP/1.1 200 OK"));
        assert!(resp.contains("Content-Length: 14"));
    }

    #[test]
    fn test_http_config_default() {
        let config = HttpConfig::default();
        assert_eq!(config.bind_addr, "0.0.0.0:8086".parse().unwrap());
        assert_eq!(config.read_timeout_secs, 30);
    }

    #[test]
    fn test_field_value_to_string() {
        assert_eq!(field_value_to_string(&FieldValue::Integer(100)), "100");
        assert_eq!(field_value_to_string(&FieldValue::Float(50.5)), "50.5");
        assert_eq!(field_value_to_string(&FieldValue::Boolean(true)), "true");
    }

    #[test]
    fn test_http_server_creation() {
        let server = HttpServer::new(HttpConfig::default());
        assert!(!server.is_running());
    }

    #[test]
    fn test_url_decode_simple() {
        assert_eq!(url_decode("hello"), "hello");
        assert_eq!(url_decode("hello%20world"), "hello world");
        assert_eq!(url_decode("test%2Fpath"), "test/path");
    }

    #[test]
    fn test_url_decode_plus() {
        assert_eq!(url_decode("hello+world"), "hello world");
    }

    #[test]
    fn test_convert_timestamp_ns() {
        assert_eq!(convert_timestamp(1000000000, "ns"), 1000000000);
    }

    #[test]
    fn test_convert_timestamp_us() {
        assert_eq!(convert_timestamp(1000000, "us"), 1000);
    }

    #[test]
    fn test_convert_timestamp_ms() {
        assert_eq!(convert_timestamp(1000000000, "ms"), 1000);
    }

    #[test]
    fn test_convert_timestamp_s() {
        assert_eq!(convert_timestamp(1000000000, "s"), 1);
    }
}