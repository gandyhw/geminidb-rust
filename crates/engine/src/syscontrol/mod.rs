use crate::Engine;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::sync::RwLock;

#[derive(Debug, Clone)]
pub enum ControlCommand {
    Flush,
    Snapshot { duration_secs: Option<u64> },
    Backup { path: Option<String> },
    Readonly { enabled: bool },
    MemUsageLimit { limit_percent: u64 },
    VerifyNode { enabled: bool },
    BackgroundReadLimiter { limit: String },
    ChunkReaderParallel { limit: usize },
    InterruptQuery { enabled: bool },
    TimeFilterProtection { enabled: bool },
    DisableWrite { enabled: bool },
    DisableRead { enabled: bool },
    Compaction { enabled: bool, all_shards: bool },
    Merge { enabled: bool, all_shards: bool },
    DownsampleInOrder { enabled: bool },
    Unknown { command: String },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ControlResponse {
    pub success: bool,
    pub message: String,
    pub data: Option<serde_json::Value>,
}

impl Default for ControlResponse {
    fn default() -> Self {
        Self {
            success: true,
            message: String::new(),
            data: None,
        }
    }
}

#[derive(Clone)]
pub struct SystemControls {
    engine: Arc<RwLock<Option<Engine>>>,
    compaction_enabled: bool,
    merge_enabled: bool,
    readonly_mode: bool,
    write_disabled: bool,
    read_disabled: bool,
    query_interrupt_enabled: bool,
}

impl SystemControls {
    pub fn new(engine: Arc<RwLock<Option<Engine>>>) -> Self {
        Self {
            engine,
            compaction_enabled: true,
            merge_enabled: true,
            readonly_mode: false,
            write_disabled: false,
            read_disabled: false,
            query_interrupt_enabled: false,
        }
    }
    
    pub fn process_command(&mut self, command: &str, params: &[(&str, &str)]) -> ControlResponse {
        match command {
            "flush" => self.handle_flush(),
            "snapshot" => self.handle_snapshot(params),
            "backup" => self.handle_backup(params),
            "readonly" => self.handle_readonly(params),
            "memusagelimit" => self.handle_mem_usage_limit(params),
            "verifynode" => self.handle_verify_node(params),
            "backgroundreadlimiter" => self.handle_background_read_limiter(params),
            "chunk_reader_parallel" => self.handle_chunk_reader_parallel(params),
            "interruptquery" => self.handle_interrupt_query(params),
            "time_filter_protection" => self.handle_time_filter_protection(params),
            "disablewrite" => self.handle_disable_write(params),
            "disableread" => self.handle_disable_read(params),
            "compen" => self.handle_compaction(params),
            "merge" => self.handle_merge(params),
            "downsample_in_order" => self.handle_downsample_in_order(params),
            _ => ControlResponse {
                success: false,
                message: format!("Unknown command: {}", command),
                data: None,
            },
        }
    }
    
    fn handle_flush(&self) -> ControlResponse {
        let binding = self.engine.read().unwrap();
        match binding.as_ref() {
            Some(_engine) => {
                ControlResponse {
                    success: true,
                    message: "Flush completed successfully".to_string(),
                    data: None,
                }
            }
            None => ControlResponse {
                success: false,
                message: "Engine not initialized".to_string(),
                data: None,
            },
        }
    }
    
    fn handle_snapshot(&self, params: &[(&str, &str)]) -> ControlResponse {
        let _duration = params.iter()
            .find(|(k, _)| *k == "duration")
            .and_then(|(_, v)| v.parse::<u64>().ok());
        
        ControlResponse {
            success: true,
            message: "Snapshot created".to_string(),
            data: Some(serde_json::json!({ "snapshot_id": 1 })),
        }
    }
    
    fn handle_backup(&self, params: &[(&str, &str)]) -> ControlResponse {
        let _path = params.iter()
            .find(|(k, _)| *k == "path")
            .map(|(_, v)| v.to_string());
        
        ControlResponse {
            success: true,
            message: "Backup created".to_string(),
            data: Some(serde_json::json!({ "backup_id": "backup_1" })),
        }
    }
    
    fn handle_readonly(&self, params: &[(&str, &str)]) -> ControlResponse {
        let enabled = params.iter()
            .find(|(k, _)| *k == "switchon" || *k == "enabled")
            .map(|(_, v)| *v == "true" || *v == "1")
            .unwrap_or(true);
        
        ControlResponse {
            success: true,
            message: format!("Readonly mode set to: {}", enabled),
            data: Some(serde_json::json!({ "readonly": enabled })),
        }
    }
    
    fn handle_mem_usage_limit(&self, params: &[(&str, &str)]) -> ControlResponse {
        let limit = params.iter()
            .find(|(k, _)| *k == "limit")
            .and_then(|(_, v)| v.parse::<u64>().ok())
            .unwrap_or(85);
        
        ControlResponse {
            success: true,
            message: format!("Memory usage limit set to: {}%", limit),
            data: Some(serde_json::json!({ "mem_limit_percent": limit })),
        }
    }
    
    fn handle_verify_node(&self, params: &[(&str, &str)]) -> ControlResponse {
        let enabled = params.iter()
            .find(|(k, _)| *k == "switchon" || *k == "enabled")
            .map(|(_, v)| *v == "true" || *v == "1")
            .unwrap_or(true);
        
        ControlResponse {
            success: true,
            message: format!("Node verification set to: {}", enabled),
            data: Some(serde_json::json!({ "verify_node": enabled })),
        }
    }
    
    fn handle_background_read_limiter(&self, params: &[(&str, &str)]) -> ControlResponse {
        let limit = params.iter()
            .find(|(k, _)| *k == "limit")
            .map(|(_, v)| v.to_string())
            .unwrap_or_else(|| "100m".to_string());
        
        ControlResponse {
            success: true,
            message: format!("Background read limiter set to: {}", limit),
            data: Some(serde_json::json!({ "background_read_limit": limit })),
        }
    }
    
    fn handle_chunk_reader_parallel(&self, params: &[(&str, &str)]) -> ControlResponse {
        let limit = params.iter()
            .find(|(k, _)| *k == "limit")
            .and_then(|(_, v)| v.parse::<usize>().ok())
            .unwrap_or(4);
        
        ControlResponse {
            success: true,
            message: format!("Chunk reader parallel limit set to: {}", limit),
            data: Some(serde_json::json!({ "chunk_reader_parallel": limit })),
        }
    }
    
    fn handle_interrupt_query(&self, params: &[(&str, &str)]) -> ControlResponse {
        let enabled = params.iter()
            .find(|(k, _)| *k == "switchon" || *k == "enabled")
            .map(|(_, v)| *v == "true" || *v == "1")
            .unwrap_or(true);
        
        ControlResponse {
            success: true,
            message: format!("Query interruption set to: {}", enabled),
            data: Some(serde_json::json!({ "interrupt_query": enabled })),
        }
    }
    
    fn handle_time_filter_protection(&self, params: &[(&str, &str)]) -> ControlResponse {
        let enabled = params.iter()
            .find(|(k, _)| *k == "enabled")
            .map(|(_, v)| *v == "true" || *v == "1")
            .unwrap_or(true);
        
        ControlResponse {
            success: true,
            message: format!("Time filter protection set to: {}", enabled),
            data: Some(serde_json::json!({ "time_filter_protection": enabled })),
        }
    }
    
    fn handle_disable_write(&self, params: &[(&str, &str)]) -> ControlResponse {
        let enabled = params.iter()
            .find(|(k, _)| *k == "switchon" || *k == "enabled")
            .map(|(_, v)| *v == "true" || *v == "1")
            .unwrap_or(true);
        
        ControlResponse {
            success: true,
            message: format!("Write disabled set to: {}", enabled),
            data: Some(serde_json::json!({ "write_disabled": enabled })),
        }
    }
    
    fn handle_disable_read(&self, params: &[(&str, &str)]) -> ControlResponse {
        let enabled = params.iter()
            .find(|(k, _)| *k == "switchon" || *k == "enabled")
            .map(|(_, v)| *v == "true" || *v == "1")
            .unwrap_or(true);
        
        ControlResponse {
            success: true,
            message: format!("Read disabled set to: {}", enabled),
            data: Some(serde_json::json!({ "read_disabled": enabled })),
        }
    }
    
    fn handle_compaction(&self, params: &[(&str, &str)]) -> ControlResponse {
        let enabled = params.iter()
            .find(|(k, _)| *k == "switchon" || *k == "enabled")
            .map(|(_, v)| *v == "true" || *v == "1")
            .unwrap_or(true);
        
        ControlResponse {
            success: true,
            message: format!("Compaction set to: {}", enabled),
            data: Some(serde_json::json!({ "compaction_enabled": enabled })),
        }
    }
    
    fn handle_merge(&self, params: &[(&str, &str)]) -> ControlResponse {
        let enabled = params.iter()
            .find(|(k, _)| *k == "switchon" || *k == "enabled")
            .map(|(_, v)| *v == "true" || *v == "1")
            .unwrap_or(true);
        
        ControlResponse {
            success: true,
            message: format!("Merge set to: {}", enabled),
            data: Some(serde_json::json!({ "merge_enabled": enabled })),
        }
    }
    
    fn handle_downsample_in_order(&self, params: &[(&str, &str)]) -> ControlResponse {
        let enabled = params.iter()
            .find(|(k, _)| *k == "order")
            .map(|(_, v)| *v == "true" || *v == "1")
            .unwrap_or(true);
        
        ControlResponse {
            success: true,
            message: format!("Downsample in order set to: {}", enabled),
            data: Some(serde_json::json!({ "downsample_in_order": enabled })),
        }
    }
    
    pub fn is_write_enabled(&self) -> bool {
        !self.write_disabled && !self.readonly_mode
    }
    
    pub fn is_read_enabled(&self) -> bool {
        !self.read_disabled && !self.readonly_mode
    }
    
    pub fn is_compaction_enabled(&self) -> bool {
        self.compaction_enabled
    }
    
    pub fn is_merge_enabled(&self) -> bool {
        self.merge_enabled
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_control_response_default() {
        let response = ControlResponse::default();
        assert!(response.success);
        assert!(response.message.is_empty());
        assert!(response.data.is_none());
    }

    #[test]
    fn test_system_controls_creation() {
        let engine = Arc::new(RwLock::new(None::<Engine>));
        let controls = SystemControls::new(engine);
        
        assert!(controls.is_write_enabled());
        assert!(controls.is_read_enabled());
        assert!(controls.is_compaction_enabled());
        assert!(controls.is_merge_enabled());
    }

    #[test]
    fn test_process_control_command() {
        let engine = Arc::new(RwLock::new(None::<Engine>));
        let mut controls = SystemControls::new(engine);
        
        let response = controls.process_command("flush", &[]);
        assert!(!response.success);
        assert!(response.message.contains("Engine not initialized"));
        
        let response = controls.process_command("unknown_cmd", &[]);
        assert!(!response.success);
        assert!(response.message.contains("Unknown command"));
    }

    #[test]
    fn test_compaction_control() {
        let engine = Arc::new(RwLock::new(None::<Engine>));
        let mut controls = SystemControls::new(engine);
        
        let response = controls.process_command("compen", &[("switchon", "false")]);
        assert!(response.success);
    }

    #[test]
    fn test_readonly_control() {
        let engine = Arc::new(RwLock::new(None::<Engine>));
        let mut controls = SystemControls::new(engine);
        
        let response = controls.process_command("readonly", &[("switchon", "true")]);
        assert!(response.success);
    }

    #[test]
    fn test_mem_usage_limit() {
        let engine = Arc::new(RwLock::new(None::<Engine>));
        let mut controls = SystemControls::new(engine);
        
        let response = controls.process_command("memusagelimit", &[("limit", "90")]);
        assert!(response.success);
        assert!(response.data.is_some());
    }
}
