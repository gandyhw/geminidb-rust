use crate::error::{Error, Result};
use crate::line_protocol::{LineProtocolParser, ParsedLine};
use crate::influxql::Parser;
use crate::{Engine, EngineConfig, WriteBatch, Row, FieldValue, TimeRange, QueryRequest};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::io::{Read, Write as IoWrite};
use std::time::{SystemTime, UNIX_EPOCH};
use std::thread;
use std::sync::{Arc, RwLock};

#[derive(Debug, Clone)]
pub struct HttpConfig {
    pub bind_addr: SocketAddr,
    pub read_timeout_secs: u64,
    pub write_timeout_secs: u64,
    pub max_connections: usize,
}

impl Default for HttpConfig {
    fn default() -> Self {
        Self {
            bind_addr: "127.0.0.1:8086".parse().unwrap(),
            read_timeout_secs: 30,
            write_timeout_secs: 30,
            max_connections: 100,
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
}

impl HttpServer {
    pub fn new(config: HttpConfig) -> Self {
        Self {
            config,
            engine: Arc::new(RwLock::new(None)),
            running: Arc::new(RwLock::new(false)),
            version: env!("CARGO_PKG_VERSION").to_string(),
        }
    }

    pub fn with_engine(mut self, engine: Engine) -> Self {
        *self.engine.write().unwrap() = Some(engine);
        self
    }

    pub fn set_engine(&self, engine: Engine) {
        *self.engine.write().unwrap() = Some(engine);
    }

    pub fn start(&self) -> Result<()> {
        let listener = TcpListener::bind(self.config.bind_addr)
            .map_err(|e| Error::InvalidArgument(format!("failed to bind: {}", e)))?;

        *self.running.write().unwrap() = true;

        let running = self.running.clone();
        let engine = self.engine.clone();
        let version = self.version.clone();

        thread::spawn(move || {
            for stream in listener.incoming() {
                let running = running.clone();
                let engine = engine.clone();
                let version = version.clone();

                if !*running.read().unwrap() {
                    break;
                }

                match stream {
                    Ok(stream) => {
                        let addr = stream.peer_addr().unwrap_or_else(|_| "0.0.0.0:0".parse().unwrap());
                        thread::spawn(move || {
                            if let Err(e) = handle_connection(stream, addr, &engine, &version) {
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
) -> Result<()> {
    let mut buffer = [0u8; 8192];
    let n = stream.read(&mut buffer)
        .map_err(|e| Error::InvalidArgument(format!("read error: {}", e)))?;

    if n == 0 {
        return Ok(());
    }

    let request = String::from_utf8_lossy(&buffer[..n]).to_string();
    let lines: Vec<&str> = request.lines().collect();

    if lines.is_empty() {
        return Ok(());
    }

    let first_line = lines[0].to_uppercase();

    let response = if first_line.starts_with("GET /ping") {
        let resp = PingResponse {
            status: "ok".to_string(),
            version: version.to_string(),
        };
        format_http_response(200, "OK", &serde_json::to_string(&resp).unwrap())
    } else if first_line.starts_with("POST /write") || first_line.starts_with("GET /write") {
        handle_write(&lines, engine)
    } else if first_line.starts_with("POST /query") || first_line.starts_with("GET /query") {
        handle_query(&lines, engine)
    } else if first_line.starts_with("GET /") {
        let path = first_line.strip_prefix("GET ").unwrap_or("").split_whitespace().next().unwrap_or("");
        if path == "/ping" {
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

fn handle_write(lines: &[&str], engine: &Arc<RwLock<Option<Engine>>>) -> String {
    let mut binding = engine.write().unwrap();
    let engine_guard = match binding.as_mut() {
        Some(e) => e,
        None => return format_http_response(500, "Internal Server Error", "Engine not initialized"),
    };

    let mut db_name = "".to_string();

    for line in lines {
        if line.to_lowercase().starts_with("db=") {
            db_name = line[3..].to_string();
        }
    }

    let parser = LineProtocolParser::new();
    let mut rows_written = 0;

    for line in lines {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') || line.starts_with("POST") || line.starts_with("GET") || line.starts_with("db=") || line.starts_with("precision=") || line.starts_with("Content-Type") || line.starts_with("Host") || line.starts_with("Connection") {
            continue;
        }

        match parser.parse(line) {
            Ok(parsed) => {
                let table_name = parsed.measurement.clone();
                let row = parsed.into_row();
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
                return format_http_response(400, "Bad Request", &format!("Parse error: {}", e));
            }
        }
    }

    if rows_written > 0 {
        format_http_response(204, "No Content", "")
    } else {
        format_http_response(500, "Internal Server Error", "Failed to write data")
    }
}

fn handle_query(lines: &[&str], engine: &Arc<RwLock<Option<Engine>>>) -> String {
    let stmt = {
        let mut db_name = "".to_string();
        let mut query_str = String::new();

        for line in lines {
            let line = line.trim();
            if line.to_lowercase().starts_with("db=") {
                db_name = line[3..].to_string();
            } else if line.to_lowercase().starts_with("q=") {
                query_str = line[2..].to_string();
            } else if line.starts_with("GET /query") || line.starts_with("POST /query") {
                if let Some(pos) = line.find("q=") {
                    let rest = &line[pos + 2..];
                    if let Some(end) = rest.find('&') {
                        query_str = rest[..end].to_string();
                    } else {
                        query_str = rest.trim().to_string();
                    }
                    query_str = query_str.replace("%22", "\"").replace("%20", " ");
                }
            }
        }

        if query_str.is_empty() {
            return format_http_response(400, "Bad Request", "Missing query parameter");
        }

        let mut parser = Parser::new(&query_str);
        match parser.parse_statement() {
            Some(s) => s,
            None => return format_http_response(400, "Bad Request", "Invalid query"),
        }
    };

    match stmt {
        crate::influxql::Statement::Select(select) => {
            let query = QueryRequest::new(
                "".to_string(),
                select.measurement.clone(),
                TimeRange {
                    start: 0,
                    end: i64::MAX,
                },
            ).with_limit(select.limit.unwrap_or(1000));

            let guard = engine.read().unwrap();
            let engine_guard = match guard.as_ref() {
                Some(e) => e,
                None => return format_http_response(500, "Internal Server Error", "Engine not initialized"),
            };

            match engine_guard.query(query) {
                Ok(rows) => {
                    let result_str = format_results(&select.measurement, &select.fields, &rows);
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
        crate::influxql::Statement::ShowDatabases => {
            format_http_response(200, "OK", "{\"results\":[{\"series\":[{\"name\":\"databases\",\"values\":[[\"testdb\"]]}]}]}")
        }
        crate::influxql::Statement::ShowMeasurements => {
            format_http_response(200, "OK", "{\"results\":[{\"series\":[{\"name\":\"measurements\",\"values\":[]}]}]}")
        }
        crate::influxql::Statement::ShowTagKeys => {
            format_http_response(200, "OK", "{\"results\":[{\"series\":[{\"name\":\"tagKeys\",\"values\":[]}]}]}")
        }
        _ => format_http_response(501, "Not Implemented", "Query type not implemented"),
    }
}

fn format_results(measurement: &str, fields: &[String], rows: &[Row]) -> String {
    let mut values = Vec::new();
    for row in rows {
        let mut row_values = Vec::new();
        row_values.push(row.timestamp.to_string());
        for field in fields {
            if *field == "*" {
                for (_, v) in &row.fields {
                    row_values.push(field_value_to_string(v));
                }
            } else {
                if let Some(v) = row.fields.get(field) {
                    row_values.push(field_value_to_string(v));
                } else {
                    row_values.push("".to_string());
                }
            }
        }
        values.push(format!("[{}]", row_values.iter().map(|s| format!("\"{}\"", s)).collect::<Vec<_>>().join(",")));
    }

    let columns = if fields.iter().any(|f| *f == "*") {
        vec!["time".to_string()]
    } else {
        let mut cols = vec!["time".to_string()];
        cols.extend(fields.iter().cloned());
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
        assert_eq!(config.bind_addr, "127.0.0.1:8086".parse().unwrap());
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
}