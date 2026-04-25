use std::time::{SystemTime, UNIX_EPOCH};

type RowTuple = (i64, std::collections::HashMap<String, String>, std::collections::HashMap<String, crate::FieldValue>);

#[derive(Debug, Clone)]
pub struct TtlManager {
    default_ttl_seconds: u64,
    table_ttls: std::collections::HashMap<String, u64>,
}

impl TtlManager {
    pub fn new(default_ttl_seconds: u64) -> Self {
        Self {
            default_ttl_seconds,
            table_ttls: std::collections::HashMap::new(),
        }
    }
    
    pub fn set_table_ttl(&mut self, table: &str, ttl_seconds: u64) {
        self.table_ttls.insert(table.to_string(), ttl_seconds);
    }
    
    pub fn get_ttl_seconds(&self, table: &str) -> u64 {
        self.table_ttls.get(table).copied().unwrap_or(self.default_ttl_seconds)
    }
    
    pub fn is_expired(&self, table: &str, timestamp: i64) -> bool {
        let ttl = self.get_ttl_seconds(table);
        if ttl == 0 {
            return false;
        }
        
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        
        let age_seconds = now - timestamp / 1_000_000_000;
        age_seconds > ttl as i64
    }
    
    pub fn get_expired_threshold(&self, table: &str) -> i64 {
        let ttl = self.get_ttl_seconds(table);
        if ttl == 0 {
            return 0;
        }
        
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        
        now - (ttl as i64) * 1_000_000_000
    }
    
    pub fn filter_expired_rows(
        &self,
        table: &str,
        rows: &mut Vec<RowTuple>,
        timestamps: &[i64],
    ) {
        let ttl = self.get_ttl_seconds(table);
        if ttl == 0 {
            return;
        }
        
        let threshold = self.get_expired_threshold(table);
        rows.retain(|(_, _, _)| {
            let ts = timestamps.first().copied().unwrap_or(0);
            ts > threshold
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_ttl_manager_new() {
        let ttl = TtlManager::new(3600);
        assert_eq!(ttl.default_ttl_seconds, 3600);
    }
    
    #[test]
    fn test_ttl_manager_set_table_ttl() {
        let mut ttl = TtlManager::new(3600);
        ttl.set_table_ttl("cpu", 7200);
        assert_eq!(ttl.get_ttl_seconds("cpu"), 7200);
        assert_eq!(ttl.get_ttl_seconds("memory"), 3600);
    }
    
    #[test]
    fn test_ttl_manager_zero_ttl() {
        let ttl = TtlManager::new(0);
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        
        assert!(!ttl.is_expired("test", now * 1_000_000_000));
    }
    
    #[test]
    fn test_get_expired_threshold_zero_ttl() {
        let ttl = TtlManager::new(0);
        assert_eq!(ttl.get_expired_threshold("test"), 0);
    }
}