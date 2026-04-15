use openGemini_engine::schema::{Schema, Database, Measurement, FieldType, RetentionPolicy};
use openGemini_engine::shard::{ShardManager, ShardMapper};
use std::path::PathBuf;

#[cfg(test)]
mod schema_benchmarks {
    use super::*;
    use std::time::Instant;

    #[test]
    pub fn bench_schema_create_database() {
        let mut schema = Schema::new();
        let start = Instant::now();
        for i in 0..100 {
            let _ = schema.create_database(format!("testdb_{}", i));
        }
        let elapsed = start.elapsed();
        println!("schema_create_database_100: {:?}", elapsed);
        assert!(elapsed.as_millis() < 100);
    }

    #[test]
    pub fn bench_schema_get_database() {
        let mut schema = Schema::new();
        for i in 0..100 {
            let _ = schema.create_database(format!("testdb_{}", i));
        }
        let start = Instant::now();
        for _ in 0..10000 {
            let _ = schema.get_database("testdb_50");
        }
        let elapsed = start.elapsed();
        println!("schema_get_database_10000: {:?}", elapsed);
        assert!(elapsed.as_millis() < 100);
    }

    #[test]
    pub fn bench_schema_create_rp() {
        let mut schema = Schema::new();
        schema.create_database("testdb".to_string()).unwrap();
        
        let start = Instant::now();
        for i in 0..100 {
            let rp = RetentionPolicy::new(format!("rp_{}", i), 86400);
            let _ = schema.create_retention_policy("testdb", rp);
        }
        let elapsed = start.elapsed();
        println!("schema_create_rp_100: {:?}", elapsed);
        assert!(elapsed.as_millis() < 100);
    }

    #[test]
    pub fn bench_measurement_creation() {
        let start = Instant::now();
        for _ in 0..1000 {
            let mut m = Measurement::new("cpu".to_string());
            m.add_tag("host".to_string(), "server1".to_string());
            m.add_field("usage".to_string(), FieldType::Float);
            m.add_field("idle".to_string(), FieldType::Float);
            m.add_field("count".to_string(), FieldType::Integer);
        }
        let elapsed = start.elapsed();
        println!("measurement_creation_1000: {:?}", elapsed);
        assert!(elapsed.as_millis() < 500);
    }

    #[test]
    pub fn bench_retention_policy_is_expired() {
        let rp = RetentionPolicy::new("rp1".to_string(), 86400);
        let old_timestamp = chrono::Utc::now().timestamp() - 100000;
        
        let start = Instant::now();
        for _ in 0..100000 {
            let _ = rp.is_expired(old_timestamp);
        }
        let elapsed = start.elapsed();
        println!("retention_policy_is_expired_100000: {:?}", elapsed);
        assert!(elapsed.as_millis() < 1000);
    }
}

#[cfg(test)]
mod shard_benchmarks {
    use super::*;
    use std::env::temp_dir;
    use std::time::Instant;

    #[test]
    pub fn bench_shard_manager_create_shard() {
        let temp_dir = temp_dir().join("bench_shard_create");
        let manager = ShardManager::new(temp_dir);
        
        let start = Instant::now();
        for i in 0..100 {
            let _ = manager.create_shard(i, "testdb", "rp1");
        }
        let elapsed = start.elapsed();
        println!("shard_manager_create_shard_100: {:?}", elapsed);
        assert!(elapsed.as_millis() < 1000);
    }

    #[test]
    pub fn bench_shard_manager_get_shard() {
        let temp_dir = temp_dir().join("bench_shard_get");
        let manager = ShardManager::new(temp_dir);
        
        for i in 0..100 {
            let _ = manager.create_shard(i, "testdb", "rp1");
        }
        
        let start = Instant::now();
        for _ in 0..100000 {
            let _ = manager.get_shard(50);
        }
        let elapsed = start.elapsed();
        println!("shard_manager_get_shard_100000: {:?}", elapsed);
        assert!(elapsed.as_millis() < 100);
    }

    #[test]
    pub fn bench_shard_mapper_map_shard() {
        let mapper = ShardMapper::new(3600, 1);
        
        let start = Instant::now();
        for i in 0..1000000 {
            let _ = mapper.map_shard(i);
        }
        let elapsed = start.elapsed();
        println!("shard_mapper_map_shard_1000000: {:?}", elapsed);
        assert!(elapsed.as_millis() < 100);
    }

    #[test]
    pub fn bench_shard_mapper_get_shards_by_db() {
        let temp_dir = temp_dir().join("bench_shard_by_db");
        let manager = ShardManager::new(temp_dir);
        
        for i in 0..100 {
            let db = if i % 2 == 0 { "db1" } else { "db2" };
            let _ = manager.create_shard(i, db, "rp1");
        }
        
        let start = Instant::now();
        for _ in 0..10000 {
            let _ = manager.get_shards_by_db("db1");
        }
        let elapsed = start.elapsed();
        println!("shard_manager_get_shards_by_db_10000: {:?}", elapsed);
        assert!(elapsed.as_millis() < 500);
    }

    #[test]
    pub fn bench_concurrent_shard_access() {
        let temp_dir = temp_dir().join("bench_concurrent_shard");
        let manager = ShardManager::new(temp_dir);
        
        for i in 0..10 {
            let _ = manager.create_shard(i, "testdb", "rp1");
        }
        
        let start = Instant::now();
        for _ in 0..10000 {
            for i in 0..10 {
                let shard = manager.get_shard(i);
                if let Some(s) = shard {
                    let _ = s.id;
                }
            }
        }
        let elapsed = start.elapsed();
        println!("concurrent_shard_access_100000: {:?}", elapsed);
        assert!(elapsed.as_millis() < 2000);
    }
}
