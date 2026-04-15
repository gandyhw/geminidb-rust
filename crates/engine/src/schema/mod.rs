use crate::error::{Error, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum FieldType {
    Integer,
    Float,
    String,
    Boolean,
}

impl FieldType {
    pub fn as_str(&self) -> &'static str {
        match self {
            FieldType::Integer => "integer",
            FieldType::Float => "float",
            FieldType::String => "string",
            FieldType::Boolean => "boolean",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Tag {
    pub key: String,
    pub value: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Field {
    pub name: String,
    pub ftype: FieldType,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Measurement {
    pub name: String,
    pub tags: Vec<Tag>,
    pub fields: Vec<Field>,
    pub schema_version: u64,
}

impl Measurement {
    pub fn new(name: String) -> Self {
        Self {
            name,
            tags: Vec::new(),
            fields: Vec::new(),
            schema_version: 0,
        }
    }

    pub fn add_tag(&mut self, key: String, value: String) {
        self.tags.push(Tag { key, value });
    }

    pub fn add_field(&mut self, name: String, ftype: FieldType) {
        self.fields.push(Field { name, ftype });
    }

    pub fn get_field(&self, name: &str) -> Option<&Field> {
        self.fields.iter().find(|f| f.name == name)
    }

    pub fn get_tag(&self, key: &str) -> Option<&Tag> {
        self.tags.iter().find(|t| t.key == key)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetentionPolicy {
    pub name: String,
    pub duration_seconds: u64,
    pub replica_count: u32,
    pub shard_duration_seconds: u64,
}

impl RetentionPolicy {
    pub fn new(name: String, duration_seconds: u64) -> Self {
        Self {
            name,
            duration_seconds,
            replica_count: 1,
            shard_duration_seconds: 3600 * 24 * 7,
        }
    }

    pub fn with_replica_count(mut self, replica_count: u32) -> Self {
        self.replica_count = replica_count;
        self
    }

    pub fn with_shard_duration(mut self, shard_duration_seconds: u64) -> Self {
        self.shard_duration_seconds = shard_duration_seconds;
        self
    }

    pub fn is_expired(&self, timestamp: i64) -> bool {
        let now = chrono::Utc::now().timestamp();
        (now - timestamp) as u64 > self.duration_seconds
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Database {
    pub name: String,
    pub retention_policies: HashMap<String, RetentionPolicy>,
    pub default_rp: Option<String>,
}

impl Database {
    pub fn new(name: String) -> Self {
        Self {
            name,
            retention_policies: HashMap::new(),
            default_rp: None,
        }
    }

    pub fn add_rp(&mut self, rp: RetentionPolicy) {
        if self.default_rp.is_none() {
            self.default_rp = Some(rp.name.clone());
        }
        self.retention_policies.insert(rp.name.clone(), rp);
    }

    pub fn get_rp(&self, name: &str) -> Option<&RetentionPolicy> {
        self.retention_policies.get(name)
    }

    pub fn get_default_rp(&self) -> Option<&RetentionPolicy> {
        self.default_rp.as_ref().and_then(|name| self.retention_policies.get(name))
    }
}

pub struct Schema {
    pub databases: HashMap<String, Database>,
}

impl Schema {
    pub fn new() -> Self {
        Self {
            databases: HashMap::new(),
        }
    }

    pub fn create_database(&mut self, name: String) -> Result<&mut Database> {
        if self.databases.contains_key(&name) {
            return Err(Error::Schema(format!("database {} already exists", name)));
        }
        self.databases.insert(name.clone(), Database::new(name.clone()));
        Ok(self.databases.get_mut(&name).unwrap())
    }

    pub fn get_database(&self, name: &str) -> Option<&Database> {
        self.databases.get(name)
    }

    pub fn get_database_mut(&mut self, name: &str) -> Option<&mut Database> {
        self.databases.get_mut(name)
    }

    pub fn create_measurement(&mut self, db_name: &str, rp_name: &str, measurement: Measurement) -> Result<()> {
        let db = self.databases.get_mut(db_name)
            .ok_or_else(|| Error::Schema(format!("database {} not found", db_name)))?;
        
        let rp = db.retention_policies.get_mut(rp_name)
            .ok_or_else(|| Error::Schema(format!("retention policy {} not found", rp_name)))?;
        
        drop(rp);
        Ok(())
    }

    pub fn list_databases(&self) -> Vec<&str> {
        self.databases.keys().map(|s| s.as_str()).collect()
    }

    pub fn create_retention_policy(&mut self, db_name: &str, rp: RetentionPolicy) -> Result<()> {
        let db = self.databases.get_mut(db_name)
            .ok_or_else(|| Error::Schema(format!("database {} not found", db_name)))?;
        db.add_rp(rp);
        Ok(())
    }
}

impl Default for Schema {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_field_type_as_str() {
        assert_eq!(FieldType::Integer.as_str(), "integer");
        assert_eq!(FieldType::Float.as_str(), "float");
        assert_eq!(FieldType::String.as_str(), "string");
        assert_eq!(FieldType::Boolean.as_str(), "boolean");
    }

    #[test]
    fn test_measurement_new() {
        let m = Measurement::new("cpu".to_string());
        assert_eq!(m.name, "cpu");
        assert!(m.tags.is_empty());
        assert!(m.fields.is_empty());
        assert_eq!(m.schema_version, 0);
    }

    #[test]
    fn test_measurement_add_tag_and_field() {
        let mut m = Measurement::new("cpu".to_string());
        m.add_tag("host".to_string(), "server1".to_string());
        m.add_field("usage".to_string(), FieldType::Float);
        
        assert_eq!(m.tags.len(), 1);
        assert_eq!(m.fields.len(), 1);
        assert_eq!(m.get_tag("host").unwrap().value, "server1");
        assert_eq!(m.get_field("usage").unwrap().ftype, FieldType::Float);
    }

    #[test]
    fn test_retention_policy_new() {
        let rp = RetentionPolicy::new("rp1".to_string(), 86400);
        assert_eq!(rp.name, "rp1");
        assert_eq!(rp.duration_seconds, 86400);
        assert_eq!(rp.replica_count, 1);
    }

    #[test]
    fn test_retention_policy_with_options() {
        let rp = RetentionPolicy::new("rp1".to_string(), 86400)
            .with_replica_count(3)
            .with_shard_duration(3600);
        
        assert_eq!(rp.replica_count, 3);
        assert_eq!(rp.shard_duration_seconds, 3600);
    }

    #[test]
    fn test_database_new() {
        let db = Database::new("testdb".to_string());
        assert_eq!(db.name, "testdb");
        assert!(db.retention_policies.is_empty());
        assert!(db.default_rp.is_none());
    }

    #[test]
    fn test_database_add_rp() {
        let mut db = Database::new("testdb".to_string());
        let rp = RetentionPolicy::new("rp1".to_string(), 86400);
        db.add_rp(rp);
        
        assert_eq!(db.retention_policies.len(), 1);
        assert_eq!(db.default_rp, Some("rp1".to_string()));
    }

    #[test]
    fn test_database_get_rp() {
        let mut db = Database::new("testdb".to_string());
        db.add_rp(RetentionPolicy::new("rp1".to_string(), 86400));
        
        assert!(db.get_rp("rp1").is_some());
        assert!(db.get_rp("nonexistent").is_none());
    }

    #[test]
    fn test_schema_new() {
        let schema = Schema::new();
        assert!(schema.databases.is_empty());
    }

    #[test]
    fn test_schema_create_database() {
        let mut schema = Schema::new();
        let result = schema.create_database("testdb".to_string());
        assert!(result.is_ok());
        assert_eq!(schema.list_databases(), vec!["testdb"]);
    }

    #[test]
    fn test_schema_create_duplicate_database() {
        let mut schema = Schema::new();
        schema.create_database("testdb".to_string()).unwrap();
        let result = schema.create_database("testdb".to_string());
        assert!(result.is_err());
    }

    #[test]
    fn test_schema_get_database() {
        let mut schema = Schema::new();
        schema.create_database("testdb".to_string()).unwrap();
        
        assert!(schema.get_database("testdb").is_some());
        assert!(schema.get_database("nonexistent").is_none());
    }

    #[test]
    fn test_schema_create_retention_policy() {
        let mut schema = Schema::new();
        schema.create_database("testdb".to_string()).unwrap();
        
        let rp = RetentionPolicy::new("rp1".to_string(), 86400);
        let result = schema.create_retention_policy("testdb", rp);
        assert!(result.is_ok());
        
        let db = schema.get_database("testdb").unwrap();
        assert!(db.get_rp("rp1").is_some());
    }

    #[test]
    fn test_schema_create_rp_nonexistent_db() {
        let mut schema = Schema::new();
        let rp = RetentionPolicy::new("rp1".to_string(), 86400);
        let result = schema.create_retention_policy("nonexistent", rp);
        assert!(result.is_err());
    }
}
