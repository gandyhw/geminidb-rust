use crate::error::{Error, Result};
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq)]
pub enum Statement {
    Select(SelectStatement),
    CreateDatabase(String),
    DropDatabase(String),
    DropMeasurement(String),
    DropSeries(Option<String>),
    Delete,
    ShowDatabases,
    ShowMeasurements,
    ShowTagKeys(Option<String>),
    ShowFieldKeys(Option<String>),
    ShowSeries,
    Use { database: String },
    Insert { measurement: String, tags: HashMap<String, String>, fields: HashMap<String, f64>, timestamp: Option<i64> },
}

#[derive(Debug, Clone, PartialEq)]
pub struct SelectStatement {
    pub fields: Vec<String>,
    pub measurement: String,
    pub condition: Option<String>,
    pub limit: Option<usize>,
}

pub struct Parser {
    input: String,
    pos: usize,
}

impl Parser {
    pub fn new(input: &str) -> Self {
        Self {
            input: input.to_string(),
            pos: 0,
        }
    }

    fn peek(&self) -> Option<char> {
        self.input.chars().nth(self.pos)
    }

    fn advance(&mut self) -> Option<char> {
        if self.pos >= self.input.len() {
            None
        } else {
            let ch = self.input.chars().nth(self.pos);
            self.pos += 1;
            ch
        }
    }

    fn skip_whitespace(&mut self) {
        while let Some(ch) = self.peek() {
            if ch.is_whitespace() {
                self.pos += 1;
            } else {
                break;
            }
        }
    }

    fn parse_word(&mut self) -> String {
        let start = self.pos;
        while let Some(ch) = self.peek() {
            if ch.is_alphanumeric() || ch == '_' || ch == '*' {
                self.pos += 1;
            } else {
                break;
            }
        }
        self.input[start..self.pos].to_uppercase()
    }

    fn parse_identifier(&mut self) -> Option<String> {
        self.skip_whitespace();
        let start = self.pos;
        if let Some(ch) = self.peek() {
            if ch.is_alphabetic() || ch == '_' {
                self.pos += 1;
                while let Some(ch) = self.peek() {
                    if ch.is_alphanumeric() || ch == '_' {
                        self.pos += 1;
                    } else {
                        break;
                    }
                }
                return Some(self.input[start..self.pos].to_string());
            }
        }
        None
    }

    fn parse_string_value(&mut self) -> Option<String> {
        self.skip_whitespace();
        let start = self.pos;
        while let Some(ch) = self.peek() {
            if ch.is_alphanumeric() || ch == '_' || ch == '-' || ch == '=' || ch == ',' {
                self.pos += 1;
            } else {
                break;
            }
        }
        if start < self.pos {
            Some(self.input[start..self.pos].to_string())
        } else {
            None
        }
    }

    fn parse_field_value(&mut self) -> Option<f64> {
        self.skip_whitespace();
        let start = self.pos;
        while let Some(ch) = self.peek() {
            if ch.is_numeric() || ch == '.' || ch == '-' {
                self.pos += 1;
            } else {
                break;
            }
        }
        if start < self.pos {
            self.input[start..self.pos].parse().ok()
        } else {
            None
        }
    }

    fn parse_tag_value(&mut self) -> Option<String> {
        self.skip_whitespace();
        let start = self.pos;
        while let Some(ch) = self.peek() {
            if ch.is_alphanumeric() || ch == '_' || ch == '-' || ch == '.' {
                self.pos += 1;
            } else {
                break;
            }
        }
        if start < self.pos {
            Some(self.input[start..self.pos].to_string())
        } else {
            None
        }
    }

    fn parse_show_tag_keys_measurement(&mut self) -> Option<String> {
        self.skip_whitespace();
        if self.peek() == Some('F') || self.peek() == Some('f') {
            let start = self.pos;
            let word = self.parse_word();
            if word.to_uppercase() == "FROM" {
                self.skip_whitespace();
                return self.parse_identifier();
            }
            self.pos = start;
        }
        None
    }

    pub fn parse_statement(&mut self) -> Option<Statement> {
        self.skip_whitespace();
        
        let word = self.parse_word();
        
        match word.as_str() {
            "SELECT" | "SELECTALL" => {
                self.skip_whitespace();
                let mut fields = Vec::new();
                loop {
                    let field = self.parse_word();
                    if field.is_empty() || field == "FROM" {
                        if field == "FROM" {
                            self.pos -= 4; // put back "FROM"
                        }
                        break;
                    }
                    fields.push(field);
                    self.skip_whitespace();
                    if self.peek() == Some(',') {
                        self.pos += 1;
                        self.skip_whitespace();
                    }
                }
                
                if self.parse_word() != "FROM" {
                    return None;
                }
                
                let measurement = self.parse_identifier()?;
                
                let mut condition = None;
                let mut limit = None;
                
                loop {
                    self.skip_whitespace();
                    let kw = self.parse_word();
                    match kw.as_str() {
                        "WHERE" => {
                            let start = self.pos;
                            while let Some(ch) = self.peek() {
                                if ch != '\n' && ch != '\r' {
                                    self.pos += 1;
                                } else {
                                    break;
                                }
                            }
                            condition = Some(self.input[start..self.pos].trim().to_string());
                        }
                        "LIMIT" => {
                            limit = self.parse_field_value().map(|v| v as usize);
                        }
                        "" => break,
                        _ => {
                            if self.pos < self.input.len() {
                                continue;
                            }
                            break;
                        }
                    }
                }
                
                Some(Statement::Select(SelectStatement {
                    fields,
                    measurement,
                    condition,
                    limit,
                }))
            }
            "CREATE" => {
                self.skip_whitespace();
                let next = self.parse_word();
                if next == "DATABASE" {
                    self.skip_whitespace();
                    let name = self.parse_identifier()?;
                    Some(Statement::CreateDatabase(name))
                } else {
                    None
                }
            }
            "DROP" => {
                self.skip_whitespace();
                let next = self.parse_word();
                match next.as_str() {
                    "DATABASE" => {
                        self.skip_whitespace();
                        let name = self.parse_identifier()?;
                        Some(Statement::DropDatabase(name))
                    }
                    "MEASUREMENT" => {
                        self.skip_whitespace();
                        let name = self.parse_identifier()?;
                        Some(Statement::DropMeasurement(name))
                    }
                    "SERIES" => {
                        Some(Statement::DropSeries(None))
                    }
                    _ => None,
                }
            }
            "DELETE" => {
                Some(Statement::Delete)
            }
            "SHOW" => {
                self.skip_whitespace();
                let next = self.parse_word();
                match next.as_str() {
                    "DATABASES" => Some(Statement::ShowDatabases),
                    "MEASUREMENTS" => Some(Statement::ShowMeasurements),
                    "SERIES" => Some(Statement::ShowSeries),
                    "TAG" => {
                        self.skip_whitespace();
                        let next = self.parse_word();
                        if next == "KEYS" {
                            let measurement = self.parse_show_tag_keys_measurement();
                            Some(Statement::ShowTagKeys(measurement))
                        } else {
                            None
                        }
                    }
                    "FIELD" => {
                        self.skip_whitespace();
                        let next = self.parse_word();
                        if next == "KEYS" {
                            let measurement = self.parse_show_tag_keys_measurement();
                            Some(Statement::ShowFieldKeys(measurement))
                        } else {
                            None
                        }
                    }
                    _ => None,
                }
            }
            "USE" => {
                self.skip_whitespace();
                let database = self.parse_identifier()?;
                Some(Statement::Use { database })
            }
            "INSERT" => {
                self.skip_whitespace();
                let measurement = self.parse_identifier()?;
                
                let mut tags = HashMap::new();
                let mut fields = HashMap::new();
                let mut timestamp = None;
                
                // Parse tag set
                loop {
                    self.skip_whitespace();
                    if self.peek() == Some(',') {
                        self.pos += 1;
                        self.skip_whitespace();
                        if let Some(key) = self.parse_identifier() {
                            if self.peek() == Some('=') {
                                self.pos += 1;
                                if let Some(value) = self.parse_tag_value() {
                                    tags.insert(key, value);
                                }
                            }
                        }
                    } else if self.peek() == Some(' ') {
                        self.pos += 1;
                        break;
                    } else {
                        break;
                    }
                }
                
                // Parse field set
                loop {
                    self.skip_whitespace();
                    if self.peek() == Some(',') {
                        self.pos += 1;
                        self.skip_whitespace();
                    }
                    // Try to parse field
                    if let Some(key) = self.parse_identifier() {
                        if self.peek() == Some('=') {
                            self.pos += 1;
                            if let Some(value) = self.parse_field_value() {
                                fields.insert(key, value);
                                continue;
                            }
                        }
                    }
                    // If we get here, we're done with fields
                    break;
                }
                
                // Parse timestamp
                self.skip_whitespace();
                if let Some(ts) = self.parse_field_value() {
                    timestamp = Some(ts as i64);
                }
                
                Some(Statement::Insert { measurement, tags, fields, timestamp })
            }
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_select() {
        let mut parser = Parser::new("SELECT * FROM cpu");
        let stmt = parser.parse_statement().unwrap();
        
        match stmt {
            Statement::Select(s) => {
                assert_eq!(s.measurement, "cpu");
                assert!(s.fields.contains(&"*".to_string()));
            }
            _ => panic!("expected Select"),
        }
    }

    #[test]
    fn test_parse_select_with_fields() {
        let mut parser = Parser::new("SELECT usage, temperature FROM cpu");
        let stmt = parser.parse_statement().unwrap();
        
        match stmt {
            Statement::Select(s) => {
                assert_eq!(s.measurement, "cpu");
                assert_eq!(s.fields.len(), 2);
            }
            _ => panic!("expected Select"),
        }
    }

    #[test]
    fn test_parse_select_with_limit() {
        let mut parser = Parser::new("SELECT * FROM cpu LIMIT 10");
        let stmt = parser.parse_statement().unwrap();
        
        match stmt {
            Statement::Select(s) => {
                assert_eq!(s.limit, Some(10));
            }
            _ => panic!("expected Select"),
        }
    }

    #[test]
    fn test_parse_create_database() {
        let mut parser = Parser::new("CREATE DATABASE testdb");
        let stmt = parser.parse_statement().unwrap();
        
        match stmt {
            Statement::CreateDatabase(name) => {
                assert_eq!(name, "testdb");
            }
            _ => panic!("expected CreateDatabase"),
        }
    }

    #[test]
    fn test_parse_drop_database() {
        let mut parser = Parser::new("DROP DATABASE testdb");
        let stmt = parser.parse_statement().unwrap();
        
        match stmt {
            Statement::DropDatabase(name) => {
                assert_eq!(name, "testdb");
            }
            _ => panic!("expected DropDatabase"),
        }
    }

    #[test]
    fn test_parse_show_databases() {
        let mut parser = Parser::new("SHOW DATABASES");
        let stmt = parser.parse_statement().unwrap();
        
        assert_eq!(stmt, Statement::ShowDatabases);
    }

    #[test]
    fn test_parse_use() {
        let mut parser = Parser::new("USE testdb");
        let stmt = parser.parse_statement().unwrap();
        
        match stmt {
            Statement::Use { database } => {
                assert_eq!(database, "testdb");
            }
            _ => panic!("expected Use"),
        }
    }

    #[test]
    fn test_parse_insert() {
        let mut parser = Parser::new("INSERT cpu,host=server1 usage=50.0 1234567890");
        let stmt = parser.parse_statement().unwrap();
        
        match stmt {
            Statement::Insert { measurement, tags, fields, timestamp } => {
                assert_eq!(measurement, "cpu");
                assert_eq!(tags.get("host"), Some(&"server1".to_string()));
                assert_eq!(fields.get("usage"), Some(&50.0));
                assert_eq!(timestamp, Some(1234567890));
            }
            _ => panic!("expected Insert"),
        }
    }
}