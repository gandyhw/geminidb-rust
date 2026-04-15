use crate::error::Result;
use crate::schema::{Schema, Measurement};
use crate::{Row, FieldValue, FilterExpr, TimeRange};

#[derive(Debug, Clone)]
pub struct QueryRequest {
    pub database: String,
    pub measurement: String,
    pub time_range: TimeRange,
    pub selected_fields: Vec<String>,
    pub filter: Option<FilterExpr>,
    pub group_by: Vec<String>,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
}

impl QueryRequest {
    pub fn new(database: String, measurement: String, time_range: TimeRange) -> Self {
        Self {
            database,
            measurement,
            time_range,
            selected_fields: Vec::new(),
            filter: None,
            group_by: Vec::new(),
            limit: None,
            offset: None,
        }
    }

    pub fn with_fields(mut self, fields: Vec<String>) -> Self {
        self.selected_fields = fields;
        self
    }

    pub fn with_filter(mut self, filter: FilterExpr) -> Self {
        self.filter = Some(filter);
        self
    }

    pub fn with_limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }

    pub fn with_group_by(mut self, group_by: Vec<String>) -> Self {
        self.group_by = group_by;
        self
    }
}

pub struct QueryExecutor {
    schema: Schema,
}

impl QueryExecutor {
    pub fn new(schema: Schema) -> Self {
        Self { schema }
    }

    pub fn with_schema(mut schema: Schema) -> Self {
        Self { schema }
    }

    pub fn validate_query(&self, request: &QueryRequest) -> Result<()> {
        if request.measurement.is_empty() {
            return Err(crate::Error::InvalidArgument("measurement cannot be empty".to_string()));
        }

        let db = self.schema.get_database(&request.database)
            .ok_or_else(|| crate::Error::Schema(format!("database {} not found", request.database)))?;

        if db.get_default_rp().is_none() && db.retention_policies.is_empty() {
            return Err(crate::Error::Schema(format!("no retention policy in database {}", request.database)));
        }

        Ok(())
    }

    pub fn get_measurement_fields(&self, db_name: &str, measurement: &str) -> Result<Vec<String>> {
        let db = self.schema.get_database(db_name)
            .ok_or_else(|| crate::Error::Schema(format!("database {} not found", db_name)))?;

        let rp = db.get_default_rp()
            .ok_or_else(|| crate::Error::Schema(format!("no default retention policy in database {}", db_name)))?;

        drop(rp);

        Ok(vec!["_time".to_string(), "tag_*".to_string(), "field_*".to_string()])
    }

    pub fn execute_select(
        &self,
        request: &QueryRequest,
        rows: Vec<Row>,
    ) -> Result<Vec<Row>> {
        self.validate_query(request)?;

        let mut results = rows;

        if let Some(ref filter) = request.filter {
            results.retain(|row| filter.evaluate(row));
        }

        if let Some(limit) = request.limit {
            results.truncate(limit);
        }

        if let Some(offset) = request.offset {
            if offset < results.len() {
                results = results[offset..].to_vec();
            } else {
                results.clear();
            }
        }

        Ok(results)
    }

    pub fn aggregate_group_by(
        &self,
        request: &QueryRequest,
        rows: Vec<Row>,
    ) -> Result<Vec<Row>> {
        if request.group_by.is_empty() {
            return self.execute_select(request, rows);
        }

        let mut groups: std::collections::HashMap<String, Vec<Row>> = std::collections::HashMap::new();

        for row in rows {
            let key = request.group_by.iter()
                .map(|tag| {
                    row.tags.get(tag).cloned().unwrap_or_default()
                })
                .collect::<Vec<_>>()
                .join(",");

            groups.entry(key).or_default().push(row);
        }

        let mut results = Vec::new();
        for (key, group_rows) in groups {
            let first_row = &group_rows[0];
            let mut aggregated_row = Row {
                tags: first_row.tags.clone(),
                fields: std::collections::HashMap::new(),
                timestamp: first_row.timestamp,
            };

            for field in &request.selected_fields {
                if field == "_time" {
                    aggregated_row.fields.insert("_time".to_string(), FieldValue::Integer(first_row.timestamp));
                    continue;
                }

                let values: Vec<i64> = group_rows.iter()
                    .filter_map(|r| {
                        if let Some(FieldValue::Integer(v)) = r.fields.get(field) {
                            Some(*v)
                        } else {
                            None
                        }
                    })
                    .collect();

                if !values.is_empty() {
                    let sum: i64 = values.iter().sum();
                    aggregated_row.fields.insert(field.clone(), FieldValue::Integer(sum));
                }
            }

            results.push(aggregated_row);
        }

        Ok(results)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_query_request_new() {
        let request = QueryRequest::new(
            "testdb".to_string(),
            "cpu".to_string(),
            TimeRange { start: 0, end: 1000 },
        );
        
        assert_eq!(request.database, "testdb");
        assert_eq!(request.measurement, "cpu");
        assert_eq!(request.time_range.start, 0);
        assert_eq!(request.time_range.end, 1000);
    }

    #[test]
    fn test_query_request_with_options() {
        let request = QueryRequest::new(
            "testdb".to_string(),
            "cpu".to_string(),
            TimeRange { start: 0, end: 1000 },
        )
        .with_fields(vec!["field1".to_string(), "field2".to_string()])
        .with_limit(100)
        .with_group_by(vec!["tag1".to_string()]);

        assert_eq!(request.selected_fields.len(), 2);
        assert_eq!(request.limit, Some(100));
        assert_eq!(request.group_by.len(), 1);
    }

    #[test]
    fn test_query_executor_new() {
        let schema = Schema::new();
        let executor = QueryExecutor::new(schema);
        assert!(executor.schema.databases.is_empty());
    }

    #[test]
    fn test_query_executor_validate_query_empty_measurement() {
        let schema = Schema::new();
        let executor = QueryExecutor::new(schema);
        
        let request = QueryRequest::new(
            "testdb".to_string(),
            "".to_string(),
            TimeRange { start: 0, end: 1000 },
        );
        
        let result = executor.validate_query(&request);
        assert!(result.is_err());
    }

    #[test]
    fn test_query_executor_validate_query_nonexistent_db() {
        let schema = Schema::new();
        let executor = QueryExecutor::new(schema);
        
        let request = QueryRequest::new(
            "nonexistent".to_string(),
            "cpu".to_string(),
            TimeRange { start: 0, end: 1000 },
        );
        
        let result = executor.validate_query(&request);
        assert!(result.is_err());
    }

    #[test]
    fn test_query_executor_execute_select() {
        let mut schema = Schema::new();
        schema.create_database("testdb".to_string()).unwrap();
        schema.create_retention_policy("testdb", crate::schema::RetentionPolicy::new("rp1".to_string(), 86400)).unwrap();
        
        let executor = QueryExecutor::new(schema);
        
        let request = QueryRequest::new(
            "testdb".to_string(),
            "cpu".to_string(),
            TimeRange { start: 0, end: 1000 },
        )
        .with_limit(10);
        
        let rows = vec![
            Row {
                tags: std::collections::HashMap::new(),
                fields: std::collections::HashMap::new(),
                timestamp: 100,
            },
            Row {
                tags: std::collections::HashMap::new(),
                fields: std::collections::HashMap::new(),
                timestamp: 200,
            },
        ];
        
        let results = executor.execute_select(&request, rows).unwrap();
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn test_query_executor_execute_select_with_limit() {
        let mut schema = Schema::new();
        schema.create_database("testdb".to_string()).unwrap();
        schema.create_retention_policy("testdb", crate::schema::RetentionPolicy::new("rp1".to_string(), 86400)).unwrap();
        
        let executor = QueryExecutor::new(schema);
        
        let request = QueryRequest::new(
            "testdb".to_string(),
            "cpu".to_string(),
            TimeRange { start: 0, end: 1000 },
        )
        .with_limit(1);
        
        let rows = vec![
            Row {
                tags: std::collections::HashMap::new(),
                fields: std::collections::HashMap::new(),
                timestamp: 100,
            },
            Row {
                tags: std::collections::HashMap::new(),
                fields: std::collections::HashMap::new(),
                timestamp: 200,
            },
        ];
        
        let results = executor.execute_select(&request, rows).unwrap();
        assert_eq!(results.len(), 1);
    }

    #[test]
    fn test_query_executor_execute_select_with_offset() {
        let mut schema = Schema::new();
        schema.create_database("testdb".to_string()).unwrap();
        schema.create_retention_policy("testdb", crate::schema::RetentionPolicy::new("rp1".to_string(), 86400)).unwrap();
        
        let executor = QueryExecutor::new(schema);
        
        let mut request = QueryRequest::new(
            "testdb".to_string(),
            "cpu".to_string(),
            TimeRange { start: 0, end: 1000 },
        );
        request.offset = Some(1);
        request.limit = Some(10);
        
        let rows = vec![
            Row {
                tags: std::collections::HashMap::new(),
                fields: std::collections::HashMap::new(),
                timestamp: 100,
            },
            Row {
                tags: std::collections::HashMap::new(),
                fields: std::collections::HashMap::new(),
                timestamp: 200,
            },
        ];
        
        let results = executor.execute_select(&request, rows).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].timestamp, 200);
    }

    #[test]
    fn test_query_executor_get_measurement_fields() {
        let mut schema = Schema::new();
        schema.create_database("testdb".to_string()).unwrap();
        schema.create_retention_policy("testdb", crate::schema::RetentionPolicy::new("rp1".to_string(), 86400)).unwrap();
        
        let executor = QueryExecutor::new(schema);
        
        let fields = executor.get_measurement_fields("testdb", "cpu").unwrap();
        assert!(!fields.is_empty());
    }
}
