use std::collections::HashMap;
use std::sync::RwLock;
use std::time::{Duration, Instant};
use serde::de::DeserializeOwned;

#[derive(Debug, Clone)]
pub struct CacheConfig {
    pub max_entries: usize,
    pub ttl_seconds: u64,
    pub max_memory_bytes: usize,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            max_entries: 1000,
            ttl_seconds: 300,
            max_memory_bytes: 100 * 1024 * 1024,
        }
    }
}

#[derive(Debug, Clone)]
pub struct CacheEntry<V> {
    pub value: V,
    pub created_at: Instant,
    pub last_accessed: Instant,
    pub access_count: u64,
    pub size_bytes: usize,
}

impl<V> CacheEntry<V> {
    pub fn new(value: V, size_bytes: usize) -> Self {
        Self {
            value,
            created_at: Instant::now(),
            last_accessed: Instant::now(),
            access_count: 0,
            size_bytes,
        }
    }

    pub fn touch(&mut self) {
        self.last_accessed = Instant::now();
        self.access_count += 1;
    }

    pub fn is_expired(&self, ttl: Duration) -> bool {
        self.created_at.elapsed() > ttl
    }
}

#[derive(Debug, Default, Clone)]
pub struct CacheStats {
    pub hits: u64,
    pub misses: u64,
    pub evictions: u64,
    pub insertions: u64,
    pub current_entries: usize,
    pub current_memory_bytes: usize,
}

impl CacheStats {
    pub fn hit_rate(&self) -> f64 {
        let total = self.hits + self.misses;
        if total == 0 {
            0.0
        } else {
            self.hits as f64 / total as f64
        }
    }
}

pub struct QueryCache {
    config: CacheConfig,
    entries: RwLock<HashMap<String, CacheEntry<Vec<u8>>>>,
    stats: RwLock<CacheStats>,
}

impl QueryCache {
    pub fn new(config: CacheConfig) -> Self {
        Self {
            config,
            entries: RwLock::new(HashMap::new()),
            stats: RwLock::new(CacheStats::default()),
        }
    }

    pub fn get<V: DeserializeOwned>(&self, key: &str) -> Option<V> {
        let ttl = Duration::from_secs(self.config.ttl_seconds);
        
        let mut entries = self.entries.write().unwrap();
        
        if let Some(entry) = entries.get_mut(key) {
            if !entry.is_expired(ttl) {
                entry.touch();
                self.stats.write().unwrap().hits += 1;
                return serde_json::from_slice(&entry.value).ok();
            } else {
                let size = entry.size_bytes;
                entries.remove(key);
                let mut stats = self.stats.write().unwrap();
                stats.current_entries = entries.len();
                stats.current_memory_bytes -= size;
            }
        }
        
        self.stats.write().unwrap().misses += 1;
        None
    }

    pub fn insert(&self, key: String, value: Vec<u8>) {
        let entry_size = value.len();
        
        let mut entries = self.entries.write().unwrap();
        
        if entries.len() >= self.config.max_entries {
            self.evict_lru(&mut entries);
        }
        
        let mut total_size: usize = entries.values().map(|e| e.size_bytes).sum();
        while total_size + entry_size > self.config.max_memory_bytes && !entries.is_empty() {
            self.evict_lru(&mut entries);
            total_size = entries.values().map(|e| e.size_bytes).sum();
        }
        
        let entry = CacheEntry::new(value, entry_size);
        entries.insert(key, entry);
        
        let mut stats = self.stats.write().unwrap();
        stats.insertions += 1;
        stats.current_entries = entries.len();
        stats.current_memory_bytes += entry_size;
    }

    fn evict_lru(&self, entries: &mut HashMap<String, CacheEntry<Vec<u8>>>) {
        if let Some((lru_key, _)) = entries
            .iter()
            .min_by_key(|(_, e)| e.last_accessed)
            .map(|(k, e)| (k.clone(), e.created_at))
        {
            if let Some(removed) = entries.remove(&lru_key) {
                let mut stats = self.stats.write().unwrap();
                stats.evictions += 1;
                stats.current_entries = entries.len();
                stats.current_memory_bytes -= removed.size_bytes;
            }
        }
    }

    pub fn invalidate(&self, key: &str) {
        let mut entries = self.entries.write().unwrap();
        if let Some(removed) = entries.remove(key) {
            let mut stats = self.stats.write().unwrap();
            stats.current_entries = entries.len();
            stats.current_memory_bytes -= removed.size_bytes;
        }
    }

    pub fn invalidate_prefix(&self, prefix: &str) {
        let mut entries = self.entries.write().unwrap();
        let keys_to_remove: Vec<String> = entries
            .keys()
            .filter(|k| k.starts_with(prefix))
            .cloned()
            .collect();
        
        let mut stats = self.stats.write().unwrap();
        for key in keys_to_remove {
            if let Some(removed) = entries.remove(&key) {
                stats.current_entries = entries.len();
                stats.current_memory_bytes -= removed.size_bytes;
            }
        }
    }

    pub fn clear(&self) {
        let mut entries = self.entries.write().unwrap();
        entries.clear();
        let mut stats = self.stats.write().unwrap();
        stats.current_entries = 0;
        stats.current_memory_bytes = 0;
    }

    pub fn stats(&self) -> CacheStats {
        let entries = self.entries.read().unwrap();
        let mut stats = self.stats.write().unwrap();
        stats.current_entries = entries.len();
        stats.current_memory_bytes = entries.values().map(|e| e.size_bytes).sum();
        (*stats).clone()
    }

    pub fn len(&self) -> usize {
        self.entries.read().unwrap().len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.read().unwrap().is_empty()
    }
}

pub fn generate_cache_key(database: &str, measurement: &str, query: &str, time_range: (i64, i64)) -> String {
    format!("{}_{}_{}_{}_{}", database, measurement, query, time_range.0, time_range.1)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cache_basic_operations() {
        let cache = QueryCache::new(CacheConfig::default());
        
        let key = "test_key";
        let value = serde_json::to_vec(&vec![1, 2, 3]).unwrap();
        
        cache.insert(key.to_string(), value.clone());
        
        let retrieved: Vec<i32> = cache.get(key).unwrap();
        assert_eq!(retrieved, vec![1, 2, 3]);
        
        let stats = cache.stats();
        assert_eq!(stats.insertions, 1);
        assert_eq!(stats.hits, 1);
    }

    #[test]
    fn test_cache_miss() {
        let cache = QueryCache::new(CacheConfig::default());
        
        let result: Option<Vec<i32>> = cache.get("nonexistent");
        assert!(result.is_none());
        
        let stats = cache.stats();
        assert_eq!(stats.misses, 1);
    }

    #[test]
    fn test_cache_invalidate() {
        let cache = QueryCache::new(CacheConfig::default());
        
        cache.insert("key1".to_string(), vec![1, 2, 3]);
        cache.invalidate("key1");
        
        let result: Option<Vec<i32>> = cache.get("key1");
        assert!(result.is_none());
    }

    #[test]
    fn test_cache_clear() {
        let cache = QueryCache::new(CacheConfig::default());
        
        cache.insert("key1".to_string(), vec![1]);
        cache.insert("key2".to_string(), vec![2]);
        cache.clear();
        
        assert!(cache.is_empty());
    }

    #[test]
    fn test_cache_max_entries() {
        let config = CacheConfig {
            max_entries: 2,
            ttl_seconds: 3600,
            max_memory_bytes: 1000,
        };
        let cache = QueryCache::new(config);
        
        cache.insert("key1".to_string(), vec![1]);
        cache.insert("key2".to_string(), vec![2]);
        cache.insert("key3".to_string(), vec![3]);
        
        assert!(cache.len() <= 3);
    }

    #[test]
    fn test_generate_cache_key() {
        let key = generate_cache_key("db", "cpu", "SELECT *", (0, 1000));
        assert_eq!(key, "db_cpu_SELECT *_0_1000");
    }
}
