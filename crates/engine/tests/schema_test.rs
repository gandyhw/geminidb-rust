use openGemini_engine::schema::{Database, Field, FieldType, Measurement, RetentionPolicy, Schema, Tag};

#[test]
fn test_field_type_as_str() {
    assert_eq!(FieldType::Integer.as_str(), "integer");
    assert_eq!(FieldType::Float.as_str(), "float");
    assert_eq!(FieldType::String.as_str(), "string");
    assert_eq!(FieldType::Boolean.as_str(), "boolean");
}

#[test]
fn test_field_type_variants() {
    assert_eq!(FieldType::Integer, FieldType::Integer);
    assert_eq!(FieldType::Float, FieldType::Float);
    assert_eq!(FieldType::String, FieldType::String);
    assert_eq!(FieldType::Boolean, FieldType::Boolean);
}

#[test]
fn test_tag_new() {
    let tag = Tag {
        key: "host".to_string(),
        value: "server1".to_string(),
    };
    assert_eq!(tag.key, "host");
    assert_eq!(tag.value, "server1");
}

#[test]
fn test_field_new() {
    let field = Field {
        name: "cpu".to_string(),
        ftype: FieldType::Float,
    };
    assert_eq!(field.name, "cpu");
    assert_eq!(field.ftype, FieldType::Float);
}

#[test]
fn test_measurement_new() {
    let measurement = Measurement::new("cpu".to_string());

    assert_eq!(measurement.name, "cpu");
    assert!(measurement.tags.is_empty());
    assert!(measurement.fields.is_empty());
    assert_eq!(measurement.schema_version, 0);
}

#[test]
fn test_measurement_add_tag() {
    let mut measurement = Measurement::new("cpu".to_string());
    measurement.add_tag("host".to_string(), "server1".to_string());

    assert_eq!(measurement.tags.len(), 1);
    assert_eq!(measurement.tags[0].key, "host");
    assert_eq!(measurement.tags[0].value, "server1");
}

#[test]
fn test_measurement_add_field() {
    let mut measurement = Measurement::new("cpu".to_string());
    measurement.add_field("usage".to_string(), FieldType::Float);

    assert_eq!(measurement.fields.len(), 1);
    assert_eq!(measurement.fields[0].name, "usage");
    assert_eq!(measurement.fields[0].ftype, FieldType::Float);
}

#[test]
fn test_measurement_get_field() {
    let mut measurement = Measurement::new("cpu".to_string());
    measurement.add_field("usage".to_string(), FieldType::Float);
    measurement.add_field("idle".to_string(), FieldType::Float);

    let found = measurement.get_field("usage");
    assert!(found.is_some());
    assert_eq!(found.unwrap().name, "usage");

    let not_found = measurement.get_field("nonexistent");
    assert!(not_found.is_none());
}

#[test]
fn test_measurement_get_tag() {
    let mut measurement = Measurement::new("cpu".to_string());
    measurement.add_tag("host".to_string(), "server1".to_string());
    measurement.add_tag("region".to_string(), "us-east".to_string());

    let found = measurement.get_tag("host");
    assert!(found.is_some());
    assert_eq!(found.unwrap().key, "host");

    let not_found = measurement.get_tag("nonexistent");
    assert!(not_found.is_none());
}

#[test]
fn test_retention_policy_new() {
    let rp = RetentionPolicy::new("rp1".to_string(), 86400);

    assert_eq!(rp.name, "rp1");
    assert_eq!(rp.duration_seconds, 86400);
    assert_eq!(rp.replica_count, 1);
    assert_eq!(rp.shard_duration_seconds, 3600 * 24 * 7);
}

#[test]
fn test_retention_policy_with_replica_count() {
    let rp = RetentionPolicy::new("rp1".to_string(), 86400);
    let rp = rp.with_replica_count(3);

    assert_eq!(rp.replica_count, 3);
}

#[test]
fn test_retention_policy_with_shard_duration() {
    let rp = RetentionPolicy::new("rp1".to_string(), 86400);
    let rp = rp.with_shard_duration(3600);

    assert_eq!(rp.shard_duration_seconds, 3600);
}

#[test]
fn test_retention_policy_is_expired() {
    let rp = RetentionPolicy::new("rp1".to_string(), 86400);
    let now = chrono::Utc::now().timestamp();

    assert!(!rp.is_expired(now));

    let old_timestamp = now - 100000;
    assert!(rp.is_expired(old_timestamp));
}

#[test]
fn test_schema_new() {
    let schema = Schema::new();
    assert!(schema.get_database("testdb").is_none());
}

#[test]
fn test_schema_create_database() {
    let mut schema = Schema::new();
    let result = schema.create_database("testdb".to_string());

    assert!(result.is_ok());
    assert!(schema.get_database("testdb").is_some());
}

#[test]
fn test_schema_create_database_twice() {
    let mut schema = Schema::new();
    let _ = schema.create_database("testdb".to_string());
    let result = schema.create_database("testdb".to_string());

    assert!(result.is_err());
}

#[test]
fn test_schema_drop_database() {
    let mut schema = Schema::new();
    schema.create_database("testdb".to_string()).unwrap();
    schema.databases.remove("testdb");

    assert!(schema.get_database("testdb").is_none());
}

#[test]
fn test_schema_create_retention_policy() {
    let mut schema = Schema::new();
    schema.create_database("testdb".to_string()).unwrap();

    let rp = RetentionPolicy::new("rp1".to_string(), 86400);
    let result = schema.create_retention_policy("testdb", rp);

    assert!(result.is_ok());
}

#[test]
fn test_schema_create_retention_policy_database_not_found() {
    let mut schema = Schema::new();
    let rp = RetentionPolicy::new("rp1".to_string(), 86400);
    let result = schema.create_retention_policy("nonexistent", rp);

    assert!(result.is_err());
}

#[test]
fn test_schema_get_retention_policy() {
    let mut schema = Schema::new();
    schema.create_database("testdb".to_string()).unwrap();

    let rp = RetentionPolicy::new("rp1".to_string(), 86400);
    schema.create_retention_policy("testdb", rp).unwrap();

    let db = schema.get_database("testdb").unwrap();
    let found = db.get_rp("rp1");
    assert!(found.is_some());
    assert_eq!(found.unwrap().name, "rp1");
}

#[test]
fn test_schema_list_databases() {
    let mut schema = Schema::new();
    schema.create_database("db1".to_string()).unwrap();
    schema.create_database("db2".to_string()).unwrap();

    let dbs = schema.list_databases();
    assert_eq!(dbs.len(), 2);
}

#[test]
fn test_database_new() {
    let db = Database::new("testdb".to_string());

    assert_eq!(db.name, "testdb");
    assert!(db.retention_policies.is_empty());
    assert_eq!(db.default_rp, None);
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
    let rp = RetentionPolicy::new("rp1".to_string(), 86400);
    db.add_rp(rp);

    let found = db.get_rp("rp1");
    assert!(found.is_some());
    assert_eq!(found.unwrap().name, "rp1");

    let not_found = db.get_rp("nonexistent");
    assert!(not_found.is_none());
}

#[test]
fn test_database_get_default_rp() {
    let mut db = Database::new("testdb".to_string());
    let rp = RetentionPolicy::new("rp1".to_string(), 86400);
    db.add_rp(rp);

    let default = db.get_default_rp();
    assert!(default.is_some());
    assert_eq!(default.unwrap().name, "rp1");
}

#[test]
fn test_database_get_default_rp_none() {
    let db = Database::new("testdb".to_string());

    let default = db.get_default_rp();
    assert!(default.is_none());
}
