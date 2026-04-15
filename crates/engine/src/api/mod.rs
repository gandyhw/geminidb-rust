use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WriteRequest {
    pub database: String,
    pub table: String,
    pub tags: HashMap<String, String>,
    pub fields: HashMap<String, crate::FieldValue>,
    #[serde(default)]
    pub timestamp: Option<i64>,
}

impl WriteRequest {
    pub fn to_row(&self) -> crate::Row {
        crate::Row {
            tags: self.tags.clone(),
            fields: self.fields.clone(),
            timestamp: self.timestamp.unwrap_or_else(|| {
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_nanos() as i64
            }),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WriteResponse {
    pub success: bool,
    pub rows_written: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

impl WriteResponse {
    pub fn success(rows_written: usize) -> Self {
        Self {
            success: true,
            rows_written,
            error: None,
        }
    }

    pub fn error(msg: String) -> Self {
        Self {
            success: false,
            rows_written: 0,
            error: Some(msg),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryRequest {
    pub database: String,
    pub table: String,
    #[serde(default)]
    pub condition: Option<String>,
    #[serde(default)]
    pub limit: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryResponse {
    pub success: bool,
    pub rows: Vec<crate::Row>,
    pub row_count: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

impl QueryResponse {
    pub fn success(rows: Vec<crate::Row>) -> Self {
        let row_count = rows.len();
        Self {
            success: true,
            rows,
            row_count,
            error: None,
        }
    }

    pub fn error(msg: String) -> Self {
        Self {
            success: false,
            rows: Vec::new(),
            row_count: 0,
            error: Some(msg),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateDatabaseRequest {
    pub name: String,
    #[serde(default)]
    pub retention_policy: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateDatabaseResponse {
    pub success: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

impl CreateDatabaseResponse {
    pub fn success() -> Self {
        Self {
            success: true,
            error: None,
        }
    }

    pub fn error(msg: String) -> Self {
        Self {
            success: false,
            error: Some(msg),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthResponse {
    pub status: String,
    pub version: String,
    pub uptime_seconds: u64,
}

impl HealthResponse {
    pub fn healthy(version: &str, uptime: u64) -> Self {
        Self {
            status: "ok".to_string(),
            version: version.to_string(),
            uptime_seconds: uptime,
        }
    }

    pub fn unhealthy(reason: &str) -> Self {
        Self {
            status: format!("unhealthy: {}", reason),
            version: env!("CARGO_PKG_VERSION").to_string(),
            uptime_seconds: 0,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageStatsResponse {
    pub database_count: usize,
    pub table_count: usize,
    pub total_rows: u64,
    pub memtable_size_bytes: u64,
    pub wal_size_bytes: u64,
}

pub struct ApiState {
    version: String,
    start_time: SystemTime,
    requests_received: Arc<RwLock<u64>>,
    requests_success: Arc<RwLock<u64>>,
    requests_failed: Arc<RwLock<u64>>,
}

impl ApiState {
    pub fn new() -> Self {
        Self {
            version: env!("CARGO_PKG_VERSION").to_string(),
            start_time: SystemTime::now(),
            requests_received: Arc::new(RwLock::new(0)),
            requests_success: Arc::new(RwLock::new(0)),
            requests_failed: Arc::new(RwLock::new(0)),
        }
    }

    pub fn version(&self) -> &str {
        &self.version
    }

    pub fn uptime_seconds(&self) -> u64 {
        SystemTime::now()
            .duration_since(self.start_time)
            .map(|d| d.as_secs())
            .unwrap_or(0)
    }

    pub fn record_request(&self, success: bool) {
        *self.requests_received.write().unwrap() += 1;
        if success {
            *self.requests_success.write().unwrap() += 1;
        } else {
            *self.requests_failed.write().unwrap() += 1;
        }
    }

    pub fn stats(&self) -> (u64, u64, u64) {
        let received = *self.requests_received.read().unwrap();
        let success = *self.requests_success.read().unwrap();
        let failed = *self.requests_failed.read().unwrap();
        (received, success, failed)
    }
}

impl Default for ApiState {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_write_request_to_row() {
        let mut tags = HashMap::new();
        tags.insert("host".to_string(), "server1".to_string());
        
        let mut fields = HashMap::new();
        fields.insert("cpu".to_string(), crate::FieldValue::Float(0.5));
        
        let req = WriteRequest {
            database: "testdb".to_string(),
            table: "cpu".to_string(),
            tags,
            fields,
            timestamp: Some(1000),
        };
        
        let row = req.to_row();
        assert_eq!(row.timestamp, 1000);
        assert_eq!(row.tags.get("host").unwrap(), "server1");
    }

    #[test]
    fn test_write_response_success() {
        let resp = WriteResponse::success(10);
        assert!(resp.success);
        assert_eq!(resp.rows_written, 10);
        assert!(resp.error.is_none());
    }

    #[test]
    fn test_write_response_error() {
        let resp = WriteResponse::error("test error".to_string());
        assert!(!resp.success);
        assert_eq!(resp.rows_written, 0);
        assert_eq!(resp.error.unwrap(), "test error");
    }

    #[test]
    fn test_query_response_success() {
        let row = crate::Row {
            tags: HashMap::new(),
            fields: {
                let mut h = HashMap::new();
                h.insert("cpu".to_string(), crate::FieldValue::Float(0.5));
                h
            },
            timestamp: 1000,
        };
        
        let resp = QueryResponse::success(vec![row]);
        assert!(resp.success);
        assert_eq!(resp.row_count, 1);
        assert_eq!(resp.rows.len(), 1);
    }

    #[test]
    fn test_health_response_healthy() {
        let resp = HealthResponse::healthy("1.0.0", 100);
        assert_eq!(resp.status, "ok");
        assert_eq!(resp.version, "1.0.0");
        assert_eq!(resp.uptime_seconds, 100);
    }

    #[test]
    fn test_health_response_unhealthy() {
        let resp = HealthResponse::unhealthy("no engine");
        assert!(resp.status.contains("unhealthy"));
        assert_eq!(resp.uptime_seconds, 0);
    }

    #[test]
    fn test_api_state_creation() {
        let state = ApiState::new();
        assert_eq!(state.uptime_seconds(), 0);
        assert_eq!(state.version(), env!("CARGO_PKG_VERSION"));
    }

    #[test]
    fn test_api_state_request_tracking() {
        let state = ApiState::new();
        
        state.record_request(true);
        state.record_request(true);
        state.record_request(false);
        
        let (received, success, failed) = state.stats();
        assert_eq!(received, 3);
        assert_eq!(success, 2);
        assert_eq!(failed, 1);
    }

    #[test]
    fn test_create_database_response() {
        let success = CreateDatabaseResponse::success();
        assert!(success.success);
        assert!(success.error.is_none());

        let error = CreateDatabaseResponse::error("db exists".to_string());
        assert!(!error.success);
        assert_eq!(error.error.unwrap(), "db exists");
    }

    #[test]
    fn test_write_request_without_timestamp() {
        let req = WriteRequest {
            database: "testdb".to_string(),
            table: "cpu".to_string(),
            tags: HashMap::new(),
            fields: HashMap::new(),
            timestamp: None,
        };
        
        let row = req.to_row();
        assert!(row.timestamp > 0);
    }

    #[test]
    fn test_storage_stats_response() {
        let stats = StorageStatsResponse {
            database_count: 5,
            table_count: 10,
            total_rows: 1000,
            memtable_size_bytes: 1024 * 1024,
            wal_size_bytes: 2048 * 1024,
        };
        
        assert_eq!(stats.database_count, 5);
        assert_eq!(stats.table_count, 10);
        assert_eq!(stats.total_rows, 1000);
    }
}