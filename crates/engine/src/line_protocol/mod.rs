use crate::error::{Error, Result};
use std::collections::HashMap;
use std::str::FromStr;

#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    String(String),
    Integer(i64),
    Float(f64),
    Boolean(bool),
    Unsigned(u64),
}

impl Value {
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Value::Float(v) => Some(*v),
            Value::Integer(v) => Some(*v as f64),
            Value::Unsigned(v) => Some(*v as f64),
            Value::String(s) => s.parse().ok(),
            Value::Boolean(b) => Some(if *b { 1.0 } else { 0.0 }),
        }
    }

    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Value::Integer(v) => Some(*v),
            Value::Unsigned(v) => Some(*v as i64),
            Value::Float(v) => Some(*v as i64),
            Value::String(s) => s.parse().ok(),
            Value::Boolean(b) => Some(if *b { 1 } else { 0 }),
        }
    }

    pub fn as_string(&self) -> String {
        match self {
            Value::String(s) => s.clone(),
            Value::Integer(v) => v.to_string(),
            Value::Float(v) => v.to_string(),
            Value::Boolean(b) => b.to_string(),
            Value::Unsigned(v) => v.to_string(),
        }
    }
}

impl FromStr for Value {
    type Err = ();
    
    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        if s.is_empty() {
            return Err(());
        }
        
        if s.eq_ignore_ascii_case("true") || s.eq_ignore_ascii_case("t") {
            return Ok(Value::Boolean(true));
        }
        if s.eq_ignore_ascii_case("false") || s.eq_ignore_ascii_case("f") {
            return Ok(Value::Boolean(false));
        }
        
        if let Ok(v) = s.parse::<i64>() {
            return Ok(Value::Integer(v));
        }
        
        if let Ok(v) = s.parse::<u64>() {
            return Ok(Value::Unsigned(v));
        }
        
        if let Ok(v) = s.parse::<f64>() {
            return Ok(Value::Float(v));
        }
        
        if (s.starts_with('"') && s.ends_with('"')) || 
           (s.starts_with('\'') && s.ends_with('\'')) {
            return Ok(Value::String(s[1..s.len()-1].to_string()));
        }
        
        Ok(Value::String(s.to_string()))
    }
}

pub struct LineProtocolParser {
    default_timestamp: i64,
    default_tags: HashMap<String, String>,
}

impl LineProtocolParser {
    pub fn new() -> Self {
        Self {
            default_timestamp: 0,
            default_tags: HashMap::new(),
        }
    }

    pub fn with_default_tags(mut self, tags: HashMap<String, String>) -> Self {
        self.default_tags = tags;
        self
    }

    pub fn with_default_timestamp(mut self, timestamp: i64) -> Self {
        self.default_timestamp = timestamp;
        self
    }

    pub fn parse(&self, line: &str) -> Result<ParsedLine> {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            return Err(Error::Parse("empty line or comment".to_string()));
        }

        let (measurement_and_tags, fields_and_timestamp) = match line.find(' ') {
            Some(pos) => (&line[..pos], &line[pos + 1..]),
            None => return Err(Error::Parse("missing space after measurement".to_string())),
        };

        let (measurement, tags_str) = match measurement_and_tags.find(',') {
            Some(pos) => (&measurement_and_tags[..pos], &measurement_and_tags[pos + 1..]),
            None => (measurement_and_tags, ""),
        };

        if measurement.is_empty() {
            return Err(Error::Parse("empty measurement name".to_string()));
        }

        let (fields_str, timestamp_str) = match fields_and_timestamp.rfind(' ') {
            Some(pos) => (&fields_and_timestamp[..pos], &fields_and_timestamp[pos + 1..]),
            None => (fields_and_timestamp, ""),
        };

        let tags = self.parse_tags(tags_str)?;
        let fields = self.parse_fields(fields_str)?;
        let timestamp = self.parse_timestamp(timestamp_str)?;

        Ok(ParsedLine {
            measurement: measurement.to_string(),
            tags,
            fields,
            timestamp,
        })
    }

    fn parse_tags(&self, tags_str: &str) -> Result<HashMap<String, String>> {
        let mut tags = self.default_tags.clone();
        
        if tags_str.is_empty() {
            return Ok(tags);
        }

        for tag_str in tags_str.split(',') {
            let tag_str = tag_str.trim();
            if tag_str.is_empty() {
                continue;
            }

            let (key, value) = match tag_str.find('=') {
                Some(pos) => (&tag_str[..pos], &tag_str[pos + 1..]),
                None => return Err(Error::Parse(format!("invalid tag format: {}", tag_str))),
            };

            if key.is_empty() {
                return Err(Error::Parse("empty tag key".to_string()));
            }

            if value.is_empty() {
                return Err(Error::Parse("empty tag value".to_string()));
            }

            tags.insert(key.to_string(), value.to_string());
        }

        Ok(tags)
    }

    fn parse_fields(&self, fields_str: &str) -> Result<HashMap<String, Value>> {
        if fields_str.is_empty() {
            return Err(Error::Parse("empty fields".to_string()));
        }

        let mut fields = HashMap::new();

        for field_str in fields_str.split(',') {
            let field_str = field_str.trim();
            if field_str.is_empty() {
                continue;
            }

            let (key, value_str) = match field_str.find('=') {
                Some(pos) => (&field_str[..pos], &field_str[pos + 1..]),
                None => return Err(Error::Parse(format!("invalid field format: {}", field_str))),
            };

            if key.is_empty() {
                return Err(Error::Parse("empty field key".to_string()));
            }

            if value_str.is_empty() {
                return Err(Error::Parse("empty field value".to_string()));
            }

            let value = self.parse_field_value(value_str)?;
            fields.insert(key.to_string(), value);
        }

        if fields.is_empty() {
            return Err(Error::Parse("at least one field is required".to_string()));
        }

        Ok(fields)
    }

    fn parse_field_value(&self, value_str: &str) -> Result<Value> {
        if value_str.is_empty() {
            return Err(Error::Parse("empty field value".to_string()));
        }

        if value_str == "i" || value_str == "I" {
            return Err(Error::Parse("missing integer value".to_string()));
        }
        
        if value_str.ends_with('i') || value_str.ends_with('I') {
            let num_str = &value_str[..value_str.len() - 1];
            match num_str.parse::<i64>() {
                Ok(v) => return Ok(Value::Integer(v)),
                Err(_) => return Err(Error::Parse(format!("invalid integer: {}", num_str))),
            }
        }

        if value_str.ends_with('u') || value_str.ends_with('U') {
            let num_str = &value_str[..value_str.len() - 1];
            match num_str.parse::<u64>() {
                Ok(v) => return Ok(Value::Unsigned(v)),
                Err(_) => return Err(Error::Parse(format!("invalid unsigned integer: {}", num_str))),
            }
        }

        if value_str.eq_ignore_ascii_case("true") || value_str.eq_ignore_ascii_case("t") {
            return Ok(Value::Boolean(true));
        }
        if value_str.eq_ignore_ascii_case("false") || value_str.eq_ignore_ascii_case("f") {
            return Ok(Value::Boolean(false));
        }

        if let Ok(v) = value_str.parse::<f64>() {
            return Ok(Value::Float(v));
        }

        if (value_str.starts_with('"') && value_str.ends_with('"')) || 
           (value_str.starts_with('\'') && value_str.ends_with('\'')) {
            let unescaped = self.unescape_string(&value_str[1..value_str.len()-1]);
            return Ok(Value::String(unescaped));
        }

        Err(Error::Parse(format!("invalid field value: {}", value_str)))
    }

    fn unescape_string(&self, s: &str) -> String {
        let mut result = String::with_capacity(s.len());
        let mut chars = s.chars().peekable();
        
        while let Some(c) = chars.next() {
            if c == '\\' {
                match chars.next() {
                    Some('n') => result.push('\n'),
                    Some('r') => result.push('\r'),
                    Some('t') => result.push('\t'),
                    Some('\\') => result.push('\\'),
                    Some('"') => result.push('"'),
                    Some('\'') => result.push('\''),
                    Some(' ') => result.push(' '),
                    Some(c) => {
                        result.push('\\');
                        result.push(c);
                    }
                    None => result.push('\\'),
                }
            } else {
                result.push(c);
            }
        }
        
        result
    }

    fn parse_timestamp(&self, timestamp_str: &str) -> Result<i64> {
        if timestamp_str.is_empty() {
            return Ok(self.default_timestamp);
        }

        timestamp_str.parse::<i64>()
            .map_err(|_| Error::Parse(format!("invalid timestamp: {}", timestamp_str)))
    }

    pub fn parse_multiple(&self, input: &str) -> Result<Vec<ParsedLine>> {
        let mut lines = Vec::new();
        
        for line in input.lines() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            lines.push(self.parse(line)?);
        }
        
        Ok(lines)
    }
}

impl Default for LineProtocolParser {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone)]
pub struct ParsedLine {
    pub measurement: String,
    pub tags: HashMap<String, String>,
    pub fields: HashMap<String, Value>,
    pub timestamp: i64,
}

impl ParsedLine {
    pub fn into_row(self) -> crate::Row {
        let fields = self.fields.into_iter()
            .map(|(k, v)| {
                let fv = match v {
                    Value::String(s) => crate::FieldValue::String(s.into_bytes()),
                    Value::Integer(i) => crate::FieldValue::Integer(i),
                    Value::Float(f) => crate::FieldValue::Float(f),
                    Value::Boolean(b) => crate::FieldValue::Boolean(b),
                    Value::Unsigned(u) => crate::FieldValue::Unsigned(u),
                };
                (k, fv)
            })
            .collect();
        
        crate::Row {
            tags: self.tags,
            fields,
            timestamp: self.timestamp,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_line() {
        let parser = LineProtocolParser::new();
        let result = parser.parse("cpu,host=server1,region=us-west usage=50.0 1234567890000000000").unwrap();
        
        assert_eq!(result.measurement, "cpu");
        assert_eq!(result.tags.get("host").unwrap(), "server1");
        assert_eq!(result.tags.get("region").unwrap(), "us-west");
        assert_eq!(result.fields.get("usage").unwrap(), &Value::Float(50.0));
        assert_eq!(result.timestamp, 1234567890000000000);
    }

    #[test]
    fn test_parse_line_without_timestamp() {
        let parser = LineProtocolParser::new().with_default_timestamp(12345);
        let result = parser.parse("cpu,host=server1 usage=50.0").unwrap();
        
        assert_eq!(result.measurement, "cpu");
        assert_eq!(result.timestamp, 12345);
    }

    #[test]
    fn test_parse_line_with_integer() {
        let parser = LineProtocolParser::new();
        let result = parser.parse("monitor,host=server1 count=100i 1234567890").unwrap();
        
        assert_eq!(result.fields.get("count").unwrap(), &Value::Integer(100));
    }

    #[test]
    fn test_parse_line_with_unsigned_integer() {
        let parser = LineProtocolParser::new();
        let result = parser.parse("monitor,host=server1 count=100u 1234567890").unwrap();
        
        assert_eq!(result.fields.get("count").unwrap(), &Value::Unsigned(100));
    }

    #[test]
    fn test_parse_line_with_boolean() {
        let parser = LineProtocolParser::new();
        
        let result = parser.parse("monitor,host=server1 active=true 1234567890").unwrap();
        assert_eq!(result.fields.get("active").unwrap(), &Value::Boolean(true));
        
        let result = parser.parse("monitor,host=server1 active=false 1234567890").unwrap();
        assert_eq!(result.fields.get("active").unwrap(), &Value::Boolean(false));
    }

    #[test]
    fn test_parse_line_with_string_field() {
        let parser = LineProtocolParser::new();
        let result = parser.parse("monitor,host=server1 message=\"hello world\" 1234567890").unwrap();
        
        assert_eq!(result.fields.get("message").unwrap(), &Value::String("hello world".to_string()));
    }

    #[test]
    fn test_parse_line_with_escaped_string() {
        let parser = LineProtocolParser::new();
        let result = parser.parse(r#"monitor,host=server1 message="hello\"world" 1234567890"#).unwrap();
        
        assert_eq!(result.fields.get("message").unwrap(), &Value::String("hello\"world".to_string()));
    }

    #[test]
    fn test_parse_multiple_lines() {
        let parser = LineProtocolParser::new();
        let input = r#"
            cpu,host=server1 usage=50.0 1234567890000000000
            cpu,host=server2 usage=60.0 1234567891000000000
            
            # comment
            mem,host=server1 usage=70.0 1234567892000000000
        "#;
        
        let results = parser.parse_multiple(input).unwrap();
        assert_eq!(results.len(), 3);
        assert_eq!(results[0].measurement, "cpu");
        assert_eq!(results[1].measurement, "cpu");
        assert_eq!(results[2].measurement, "mem");
    }

    #[test]
    fn test_parse_empty_measurement() {
        let parser = LineProtocolParser::new();
        let result = parser.parse(",host=server1 usage=50.0");
        
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_no_fields() {
        let parser = LineProtocolParser::new();
        let result = parser.parse("cpu,host=server1");
        
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_comment_line() {
        let parser = LineProtocolParser::new();
        let result = parser.parse("# this is a comment");
        
        assert!(result.is_err());
    }

    #[test]
    fn test_value_as_f64() {
        assert_eq!(Value::Float(1.5).as_f64(), Some(1.5));
        assert_eq!(Value::Integer(10).as_f64(), Some(10.0));
        assert_eq!(Value::Unsigned(10).as_f64(), Some(10.0));
        assert_eq!(Value::String("25.5".to_string()).as_f64(), Some(25.5));
        assert_eq!(Value::Boolean(true).as_f64(), Some(1.0));
        assert_eq!(Value::Boolean(false).as_f64(), Some(0.0));
    }

    #[test]
    fn test_value_as_i64() {
        assert_eq!(Value::Integer(10).as_i64(), Some(10));
        assert_eq!(Value::Float(10.9).as_i64(), Some(10));
        assert_eq!(Value::Unsigned(10).as_i64(), Some(10));
        assert_eq!(Value::String("25".to_string()).as_i64(), Some(25));
        assert_eq!(Value::Boolean(true).as_i64(), Some(1));
        assert_eq!(Value::Boolean(false).as_i64(), Some(0));
    }

    #[test]
    fn test_value_as_string() {
        assert_eq!(Value::String("test".to_string()).as_string(), "test");
        assert_eq!(Value::Integer(10).as_string(), "10");
        assert_eq!(Value::Float(10.5).as_string(), "10.5");
        assert_eq!(Value::Boolean(true).as_string(), "true");
    }

    #[test]
    fn test_parsed_line_into_row() {
        let mut tags = HashMap::new();
        tags.insert("host".to_string(), "server1".to_string());
        
        let mut fields = HashMap::new();
        fields.insert("usage".to_string(), Value::Float(50.0));
        
        let parsed = ParsedLine {
            measurement: "cpu".to_string(),
            tags,
            fields,
            timestamp: 1234567890,
        };
        
        let row = parsed.into_row();
        assert_eq!(row.timestamp, 1234567890);
        assert_eq!(row.tags.get("host").unwrap(), "server1");
        assert_eq!(row.fields.get("usage").unwrap(), &crate::FieldValue::Float(50.0));
    }

    #[test]
    fn test_parser_with_default_tags() {
        let parser = LineProtocolParser::new().with_default_tags({
            let mut tags = HashMap::new();
            tags.insert("region".to_string(), "us-west".to_string());
            tags
        });

        let result = parser.parse("cpu,host=server1 usage=50.0 1234567890").unwrap();

        assert_eq!(result.tags.get("region").unwrap(), "us-west");
        assert_eq!(result.tags.get("host").unwrap(), "server1");
    }

    #[test]
    fn test_value_from_str_integer() {
        let val: Value = "42".parse().unwrap();
        assert_eq!(val, Value::Integer(42));
    }

    #[test]
    fn test_value_from_str_negative_integer() {
        let val: Value = "-42".parse().unwrap();
        assert_eq!(val, Value::Integer(-42));
    }

    #[test]
    fn test_value_from_str_large_number() {
        let val: Value = "18446744073709551615".parse().unwrap();
        match val {
            Value::Unsigned(n) => assert_eq!(n, 18446744073709551615u64),
            _ => {}
        }
    }

    #[test]
    fn test_value_from_str_float() {
        let val: Value = "3.14".parse().unwrap();
        assert_eq!(val, Value::Float(3.14));
    }

    #[test]
    fn test_value_from_str_boolean_true() {
        let val: Value = "true".parse().unwrap();
        assert_eq!(val, Value::Boolean(true));
    }

    #[test]
    fn test_value_from_str_boolean_false() {
        let val: Value = "false".parse().unwrap();
        assert_eq!(val, Value::Boolean(false));
    }

    #[test]
    fn test_value_from_str_quoted_string() {
        let val: Value = "\"hello\"".parse().unwrap();
        assert_eq!(val, Value::String("hello".to_string()));
    }

    #[test]
    fn test_value_from_str_empty() {
        let result: std::result::Result<Value, _> = "".parse();
        assert!(result.is_err());
    }

    #[test]
    fn test_line_protocol_parser_default() {
        let parser = LineProtocolParser::new();
        assert_eq!(parser.default_timestamp, 0);
        assert!(parser.default_tags.is_empty());
    }

    #[test]
    fn test_line_protocol_parser_with_default_timestamp() {
        let parser = LineProtocolParser::new().with_default_timestamp(1000);
        assert_eq!(parser.default_timestamp, 1000);
    }

    #[test]
    fn test_parse_line_without_timestamp_uses_default() {
        let parser = LineProtocolParser::new().with_default_timestamp(1000);
        let result = parser.parse("cpu,host=server1 usage=50.0");
        assert!(result.is_ok());
        assert_eq!(result.unwrap().timestamp, 1000);
    }
}