use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq)]
pub enum Statement {
    Select(SelectStatement),
    CreateDatabase(String),
    CreateRetentionPolicy(CreateRetentionPolicy),
    DropDatabase(String),
    DropMeasurement(String),
    DropSeries { condition: Option<String> },
    Delete { condition: Option<String> },
    ShowDatabases,
    ShowMeasurements,
    ShowRetentionPolicies(Option<String>),
    ShowTagKeys(Option<String>),
    ShowTagValues(Option<String>, String),
    ShowFieldKeys(Option<String>),
    ShowSeries,
    Use { database: String },
    Insert { measurement: String, tags: HashMap<String, String>, fields: HashMap<String, f64>, timestamp: Option<i64> },
    AlterDatabase(AlterDatabaseStatement),
    Set { key: String, value: String },
    Revoke(RevokeStatement),
    Grant(GrantStatement),
}

#[derive(Debug, Clone, PartialEq)]
pub struct AlterDatabaseStatement {
    pub name: String,
    pub rp_name: Option<String>,
    pub duration_seconds: Option<u64>,
    pub replica_count: Option<u32>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SetStatement {
    pub key: String,
    pub value: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RevokeStatement {
    pub privilege: String,
    pub database: Option<String>,
    pub user: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct GrantStatement {
    pub privilege: String,
    pub database: Option<String>,
    pub user: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CreateRetentionPolicy {
    pub name: String,
    pub database: String,
    pub duration_seconds: u64,
    pub replica_count: u32,
    pub default: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub enum AggregateFunc {
    Count(Option<String>),
    Sum(String),
    Mean(String),
    Min(String),
    Max(String),
    First(String),
    Last(String),
}

#[derive(Debug, Clone, PartialEq)]
pub enum Field {
    Raw(String),
    Aggregate(AggregateFunc),
}

#[derive(Debug, Clone, PartialEq)]
pub struct SelectStatement {
    pub fields: Vec<Field>,
    pub measurement: String,
    pub condition: Option<String>,
    pub group_by: Vec<String>,
    pub limit: Option<usize>,
    pub slimit: Option<usize>,
    pub soffset: Option<usize>,
    pub order_by: Option<String>,
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

    #[allow(dead_code)]
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

    #[allow(dead_code)]
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

    fn parse_field_expr(&mut self) -> Option<Field> {
        self.skip_whitespace();
        let start_pos = self.pos;
        let word = self.parse_word();
        
        if word.is_empty() {
            return None;
        }
        
        let upper_word = word.to_uppercase();
        if upper_word == "COUNT" || upper_word == "SUM" || upper_word == "MEAN" || 
           upper_word == "MIN" || upper_word == "MAX" || upper_word == "FIRST" || upper_word == "LAST" {
            self.skip_whitespace();
            if self.peek() == Some('(') {
                self.pos += 1;
                self.skip_whitespace();
                
                if upper_word == "COUNT" && self.peek() == Some('*') {
                    self.pos += 1;
                    self.skip_whitespace();
                    if self.peek() == Some(')') {
                        self.pos += 1;
                        return Some(Field::Aggregate(AggregateFunc::Count(None)));
                    }
                    self.pos = start_pos;
                    return None;
                }
                
                let inner = self.parse_word();
                if inner.is_empty() {
                    self.pos = start_pos;
                    return None;
                }
                self.skip_whitespace();
                if self.peek() == Some(')') {
                    self.pos += 1;
                    let func = match upper_word.as_str() {
                        "COUNT" => AggregateFunc::Count(Some(inner)),
                        "SUM" => AggregateFunc::Sum(inner),
                        "MEAN" => AggregateFunc::Mean(inner),
                        "MIN" => AggregateFunc::Min(inner),
                        "MAX" => AggregateFunc::Max(inner),
                        "FIRST" => AggregateFunc::First(inner),
                        "LAST" => AggregateFunc::Last(inner),
                        _ => {
                            self.pos = start_pos;
                            return None;
                        }
                    };
                    return Some(Field::Aggregate(func));
                }
            }
            self.pos = start_pos;
            return None;
        }
        
        let keywords = ["FROM", "WHERE", "LIMIT", "ORDER", "SLIMIT", "SOFFSET", "GROUP", "OFFSET"];
        if keywords.iter().any(|k| upper_word == *k) {
            self.pos = start_pos;
            return None;
        }
        
        Some(Field::Raw(word))
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

    fn parse_where_condition(&mut self) -> Option<String> {
        self.skip_whitespace();
        let start = self.pos;
        let word = self.parse_word();
        if word.to_uppercase() == "WHERE" {
            self.skip_whitespace();
            let cond_start = self.pos;
            while let Some(_) = self.peek() {
                self.pos += 1;
            }
            if cond_start < self.pos {
                return Some(self.input[cond_start..self.pos].trim().to_string());
            }
        }
        self.pos = start;
        None
    }

    fn parse_word_not_empty(&mut self) -> Option<String> {
        self.skip_whitespace();
        let word = self.parse_word();
        if word.is_empty() {
            None
        } else {
            Some(word)
        }
    }

    fn parse_number(&mut self) -> Option<f64> {
        self.skip_whitespace();
        let start = self.pos;
        while let Some(ch) = self.peek() {
            if ch.is_numeric() || ch == '.' {
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

    fn parse_duration(&mut self) -> u64 {
        self.skip_whitespace();
        if self.peek() == Some('=') {
            self.pos += 1;
        }
        self.skip_whitespace();
        
        let start = self.pos;
        while let Some(ch) = self.peek() {
            if ch.is_numeric() {
                self.pos += 1;
            } else {
                break;
            }
        }
        
        let num: u64 = self.input[start..self.pos].parse().unwrap_or(0);
        
        self.skip_whitespace();
        let unit = self.parse_word().to_uppercase();
        
        match unit.as_str() {
            "s" | "S" | "SEC" | "SECOND" | "SECONDS" => num,
            "m" | "M" | "MIN" | "MINUTE" | "MINUTES" => num * 60,
            "h" | "H" | "HR" | "HOUR" | "HOURS" => num * 3600,
            "d" | "D" | "DAY" | "DAYS" => num * 86400,
            "w" | "W" | "WK" | "WEEK" | "WEEKS" => num * 604800,
            _ => num,
        }
    }

    pub fn parse_condition(&mut self, condition_str: &str) -> Option<crate::FilterExpr> {
        let condition_str = condition_str.trim();
        if condition_str.is_empty() {
            return None;
        }

        let condition_lower = condition_str.to_lowercase();
        
        if let Some(pos) = condition_lower.find(" and ") {
            let left = &condition_str[..pos];
            let right = &condition_str[pos + 5..];
            let left_expr = self.parse_single_condition(left.trim())?;
            let right_expr = self.parse_single_condition(right.trim())?;
            return Some(crate::FilterExpr::And(Box::new(left_expr), Box::new(right_expr)));
        }
        
        if let Some(pos) = condition_lower.find(" or ") {
            let left = &condition_str[..pos];
            let right = &condition_str[pos + 4..];
            let left_expr = self.parse_single_condition(left.trim())?;
            let right_expr = self.parse_single_condition(right.trim())?;
            return Some(crate::FilterExpr::Or(Box::new(left_expr), Box::new(right_expr)));
        }
        
        self.parse_single_condition(condition_str)
    }

    fn parse_single_condition(&self, condition: &str) -> Option<crate::FilterExpr> {
        let condition = condition.trim();
        let condition_lower = condition.to_lowercase();
        
        if condition_lower.starts_with("time") {
            return self.parse_time_condition(condition);
        }
        
        if condition.find(">=").is_some() {
            let parts: Vec<&str> = condition.splitn(2, ">=").collect();
            if parts.len() == 2 {
                let field = parts[0].trim().to_string();
                let value_str = parts[1].trim();
                if let Some(value) = self.parse_value(value_str) {
                    return Some(crate::FilterExpr::Gte(field, value));
                }
            }
        }
        
        if condition.find("<=").is_some() {
            let parts: Vec<&str> = condition.splitn(2, "<=").collect();
            if parts.len() == 2 {
                let field = parts[0].trim().to_string();
                let value_str = parts[1].trim();
                if let Some(value) = self.parse_value(value_str) {
                    return Some(crate::FilterExpr::Lte(field, value));
                }
            }
        }
        
        if condition.find('=').is_some() {
            let parts: Vec<&str> = condition.splitn(2, '=').collect();
            if parts.len() == 2 {
                let field = parts[0].trim().to_string();
                let value_str = parts[1].trim();
                if let Some(value) = self.parse_value(value_str) {
                    return Some(crate::FilterExpr::Eq(field, value));
                }
            }
        }
        
        if condition.find("!=").is_some() {
            let parts: Vec<&str> = condition.splitn(2, "!=").collect();
            if parts.len() == 2 {
                let field = parts[0].trim().to_string();
                let value_str = parts[1].trim();
                if let Some(value) = self.parse_value(value_str) {
                    return Some(crate::FilterExpr::Ne(field, value));
                }
            }
        }
        
        if condition.find('>').is_some() {
            let parts: Vec<&str> = condition.splitn(2, '>').collect();
            if parts.len() == 2 {
                let field = parts[0].trim().to_string();
                let value_str = parts[1].trim();
                if let Some(value) = self.parse_value(value_str) {
                    return Some(crate::FilterExpr::Gt(field, value));
                }
            }
        }
        
        if condition.find('<').is_some() {
            let parts: Vec<&str> = condition.splitn(2, '<').collect();
            if parts.len() == 2 {
                let field = parts[0].trim().to_string();
                let value_str = parts[1].trim();
                if let Some(value) = self.parse_value(value_str) {
                    return Some(crate::FilterExpr::Lt(field, value));
                }
            }
        }
        
        None
    }

    fn parse_time_condition(&self, condition: &str) -> Option<crate::FilterExpr> {
        let condition_lower = condition.to_lowercase();
        
        if condition_lower.contains("now()") {
            let offset_ns = self.parse_duration_from_now(condition)?;
            let now = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);
            let target_time = now - offset_ns;
            
            if condition_lower.contains(">=") || condition_lower.contains("=>") {
                return Some(crate::FilterExpr::Gte("time".to_string(), crate::FieldValue::Integer(target_time)));
            }
            if condition_lower.contains("<=") || condition_lower.contains("=<") {
                return Some(crate::FilterExpr::Lte("time".to_string(), crate::FieldValue::Integer(target_time)));
            }
            if condition_lower.contains('>') && !condition_lower.contains(">=") {
                return Some(crate::FilterExpr::Gt("time".to_string(), crate::FieldValue::Integer(target_time)));
            }
            if condition_lower.contains('<') && !condition_lower.contains("<=") {
                return Some(crate::FilterExpr::Lt("time".to_string(), crate::FieldValue::Integer(target_time)));
            }
            if condition_lower.contains('=') {
                return Some(crate::FilterExpr::Eq("time".to_string(), crate::FieldValue::Integer(target_time)));
            }
        }
        
        None
    }

    fn parse_duration_from_now(&self, condition: &str) -> Option<i64> {
        let condition_lower = condition.to_lowercase();
        
        let offset_str = if let Some(pos) = condition_lower.find("now()") {
            let after_now = &condition[pos + 5..];
            after_now.trim().to_string()
        } else {
            return None;
        };
        
        let offset_str = offset_str.trim();
        
        let mut offset_ns: i64 = 0;
        let mut num_str = String::new();
        let mut unit_found = false;
        
        for c in offset_str.chars() {
            if c.is_numeric() {
                num_str.push(c);
                unit_found = false;
            } else if c.is_alphabetic() || c == ' ' {
                unit_found = true;
                let num: i64 = num_str.parse().unwrap_or(0);
                let unit_lower = offset_str[offset_str.find(&c.to_string()).unwrap_or(0)..].to_lowercase();
                
                if unit_lower.starts_with('s') && !unit_lower.starts_with("sec") {
                    offset_ns = num * 1_000_000_000;
                } else if unit_lower.starts_with("sec") {
                    offset_ns = num * 1_000_000_000;
                } else if unit_lower.starts_with("min") {
                    offset_ns = num * 60 * 1_000_000_000;
                } else if unit_lower.starts_with('h') {
                    offset_ns = num * 3600 * 1_000_000_000;
                } else if unit_lower.starts_with('d') {
                    offset_ns = num * 86400 * 1_000_000_000;
                } else if unit_lower.starts_with('w') {
                    offset_ns = num * 604800 * 1_000_000_000;
                } else if unit_lower.starts_with('m') && !unit_lower.starts_with("min") {
                    offset_ns = num * 60 * 1_000_000_000;
                }
                break;
            }
        }
        
        if num_str.is_empty() || !unit_found {
            return None;
        }
        
        Some(offset_ns)
    }

    fn parse_value(&self, value_str: &str) -> Option<crate::FieldValue> {
        let value_str = value_str.trim();
        
        if value_str.eq_ignore_ascii_case("true") {
            return Some(crate::FieldValue::Boolean(true));
        }
        if value_str.eq_ignore_ascii_case("false") {
            return Some(crate::FieldValue::Boolean(false));
        }
        
        if let Ok(i) = value_str.parse::<i64>() {
            return Some(crate::FieldValue::Integer(i));
        }
        
        if let Ok(f) = value_str.parse::<f64>() {
            return Some(crate::FieldValue::Float(f));
        }
        
        if (value_str.starts_with('\'') && value_str.ends_with('\'')) ||
           (value_str.starts_with('"') && value_str.ends_with('"')) {
            return Some(crate::FieldValue::String(value_str[1..value_str.len()-1].as_bytes().to_vec()));
        }
        
        Some(crate::FieldValue::String(value_str.as_bytes().to_vec()))
    }

    pub fn parse_statement(&mut self) -> Option<Statement> {
        self.skip_whitespace();
        
        let word = self.parse_word();
        
        match word.as_str() {
            "SELECT" | "SELECTALL" => {
                self.skip_whitespace();
                let mut fields = Vec::new();
                loop {
                    let field = self.parse_field_expr();
                    if field.is_none() {
                        break;
                    }
                    fields.push(field.unwrap());
                    self.skip_whitespace();
                    if self.peek() == Some(',') {
                        self.pos += 1;
                        self.skip_whitespace();
                    } else {
                        break;
                    }
                }
                
                if self.parse_word() != "FROM" {
                    return None;
                }
                
                let measurement = self.parse_identifier()?;
                
                let mut condition = None;
                let mut limit = None;
                let mut slimit = None;
                let mut soffset = None;
                let mut order_by = None;
                let mut group_by = Vec::new();
                
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
                        "SLIMIT" => {
                            slimit = self.parse_field_value().map(|v| v as usize);
                        }
                        "SOFFSET" => {
                            soffset = self.parse_field_value().map(|v| v as usize);
                        }
                        "ORDER" => {
                            self.skip_whitespace();
                            let by = self.parse_word();
                            if by.to_uppercase() == "BY" {
                                self.skip_whitespace();
                                order_by = Some(self.parse_word());
                            }
                        }
                        "GROUP" => {
                            self.skip_whitespace();
                            let by = self.parse_word();
                            if by.to_uppercase() == "BY" {
                                self.skip_whitespace();
                                loop {
                                    let tag = self.parse_identifier();
                                    if let Some(t) = tag {
                                        group_by.push(t);
                                        self.skip_whitespace();
                                        if self.peek() == Some(',') {
                                            self.pos += 1;
                                            self.skip_whitespace();
                                        } else {
                                            break;
                                        }
                                    } else {
                                        break;
                                    }
                                }
                            }
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
                    group_by,
                    limit,
                    slimit,
                    soffset,
                    order_by,
                }))
            }
            "CREATE" => {
                self.skip_whitespace();
                let next = self.parse_word();
                if next == "DATABASE" {
                    self.skip_whitespace();
                    let name = self.parse_identifier()?;
                    Some(Statement::CreateDatabase(name))
                } else if next == "RETENTION" {
                    self.skip_whitespace();
                    let next = self.parse_word();
                    if next == "POLICY" || next == "RP" {
                        self.skip_whitespace();
                        let rp_name = self.parse_identifier()?;
                        self.skip_whitespace();
                        
                        let mut database = String::new();
                        let mut duration_seconds = 3600 * 24 * 7;
                        let mut replica_count = 1u32;
                        let mut is_default = false;
                        
                        while let Some(word) = self.parse_word_not_empty() {
                            match word.to_uppercase().as_str() {
                                "ON" => {
                                    database = self.parse_identifier()?;
                                }
                                "DURATION" => {
                                    duration_seconds = self.parse_duration();
                                }
                                "REPLICATION" => {
                                    self.skip_whitespace();
                                    if self.peek() == Some('=') {
                                        self.pos += 1;
                                    }
                                    self.skip_whitespace();
                                    if let Some(n) = self.parse_number() {
                                        replica_count = n as u32;
                                    }
                                }
                                "DEFAULT" => {
                                    is_default = true;
                                }
                                _ => {}
                            }
                        }
                        
                        Some(Statement::CreateRetentionPolicy(CreateRetentionPolicy {
                            name: rp_name,
                            database,
                            duration_seconds,
                            replica_count,
                            default: is_default,
                        }))
                    } else {
                        None
                    }
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
                        self.skip_whitespace();
                        let condition = self.parse_where_condition();
                        Some(Statement::DropSeries { condition })
                    }
                    _ => None,
                }
            }
            "ALTER" => {
                self.skip_whitespace();
                let next = self.parse_word();
                if next == "DATABASE" {
                    self.skip_whitespace();
                    let name = self.parse_identifier()?;
                    
                    let mut rp_name = None;
                    let mut duration_seconds = None;
                    let mut replica_count = None;
                    
                    while let Some(word) = self.parse_word_not_empty() {
                        match word.to_uppercase().as_str() {
                            "SET" => {
                                self.skip_whitespace();
                                let next_word = self.parse_word().to_uppercase();
                                if next_word == "RETENTION" {
                                    self.skip_whitespace();
                                    let next_word2 = self.parse_word().to_uppercase();
                                    if next_word2 == "POLICY" || next_word2 == "RP" {
                                        self.skip_whitespace();
                                        rp_name = self.parse_identifier();
                                    }
                                }
                            }
                            "RETENTION" => {
                                self.skip_whitespace();
                                let next_word = self.parse_word().to_uppercase();
                                if next_word == "POLICY" || next_word == "RP" {
                                    self.skip_whitespace();
                                    rp_name = self.parse_identifier();
                                }
                            }
                            "DURATION" => {
                                self.skip_whitespace();
                                if self.peek() == Some('=') {
                                    self.pos += 1;
                                }
                                self.skip_whitespace();
                                duration_seconds = Some(self.parse_duration());
                            }
                            "REPLICATION" => {
                                self.skip_whitespace();
                                if self.peek() == Some('=') {
                                    self.pos += 1;
                                }
                                self.skip_whitespace();
                                if let Some(n) = self.parse_number() {
                                    replica_count = Some(n as u32);
                                }
                            }
                            _ => {}
                        }
                    }
                    
                    Some(Statement::AlterDatabase(AlterDatabaseStatement {
                        name,
                        rp_name,
                        duration_seconds,
                        replica_count,
                    }))
                } else {
                    None
                }
            }
            "DELETE" => {
                self.skip_whitespace();
                let condition = self.parse_where_condition();
                Some(Statement::Delete { condition })
            }
            "SET" => {
                self.skip_whitespace();
                let key = self.parse_identifier().unwrap_or_default();
                self.skip_whitespace();
                if self.peek() == Some('=') {
                    self.pos += 1;
                }
                self.skip_whitespace();
                let value = self.parse_identifier().unwrap_or_default();
                Some(Statement::Set { key, value })
            }
            "GRANT" => {
                self.skip_whitespace();
                let privilege = self.parse_word();
                self.skip_whitespace();
                let mut database = None;
                let mut user = None;
                
                let next = self.parse_word();
                if next == "ON" {
                    database = self.parse_identifier();
                } else if next == "TO" {
                    user = self.parse_identifier();
                }
                
                Some(Statement::Grant(GrantStatement {
                    privilege,
                    database,
                    user,
                }))
            }
            "REVOKE" => {
                self.skip_whitespace();
                let privilege = self.parse_word();
                self.skip_whitespace();
                let mut database = None;
                let mut user = None;
                
                let next = self.parse_word();
                if next == "ON" {
                    database = self.parse_identifier();
                } else if next == "FROM" {
                    user = self.parse_identifier();
                }
                
                Some(Statement::Revoke(RevokeStatement {
                    privilege,
                    database,
                    user,
                }))
            }
            "SHOW" => {
                self.skip_whitespace();
                let next = self.parse_word();
                match next.as_str() {
                    "DATABASES" => Some(Statement::ShowDatabases),
                    "MEASUREMENTS" => Some(Statement::ShowMeasurements),
                    "SERIES" => Some(Statement::ShowSeries),
                    "RETENTION" => {
                        self.skip_whitespace();
                        let next = self.parse_word();
                        if next == "POLICIES" || next == "RP" {
                            let database = self.parse_word();
                            if database.is_empty() {
                                Some(Statement::ShowRetentionPolicies(None))
                            } else {
                                Some(Statement::ShowRetentionPolicies(Some(database)))
                            }
                        } else {
                            None
                        }
                    }
                    "TAG" => {
                        self.skip_whitespace();
                        let next = self.parse_word();
                        if next == "KEYS" {
                            let measurement = self.parse_show_tag_keys_measurement();
                            Some(Statement::ShowTagKeys(measurement))
                        } else if next == "VALUES" {
                            let measurement = self.parse_show_tag_keys_measurement();
                            let tag_key = self.parse_identifier().unwrap_or_default();
                            Some(Statement::ShowTagValues(measurement, tag_key))
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
                assert!(s.fields.contains(&Field::Raw("*".to_string())));
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
                assert!(matches!(s.fields.get(0), Some(Field::Raw(n)) if n == "USAGE"));
                assert!(matches!(s.fields.get(1), Some(Field::Raw(n)) if n == "TEMPERATURE"));
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

    #[test]
    fn test_parse_show_tag_values() {
        let mut parser = Parser::new("SHOW TAG VALUES FROM cpu");
        let stmt = parser.parse_statement().unwrap();
        
        match stmt {
            crate::influxql::Statement::ShowTagValues(measurement, tag_key) => {
                assert_eq!(measurement, Some("cpu".to_string()));
                assert_eq!(tag_key, "");
            }
            _ => panic!("expected ShowTagValues"),
        }
    }

    #[test]
    fn test_parse_show_tag_keys() {
        let mut parser = Parser::new("SHOW TAG KEYS FROM cpu");
        let stmt = parser.parse_statement().unwrap();
        
        match stmt {
            crate::influxql::Statement::ShowTagKeys(measurement) => {
                assert_eq!(measurement, Some("cpu".to_string()));
            }
            _ => panic!("expected ShowTagKeys"),
        }
    }

    #[test]
    fn test_parse_select_with_where() {
        let mut parser = Parser::new("SELECT * FROM cpu WHERE host = 'server1'");
        let stmt = parser.parse_statement().unwrap();
        
        match stmt {
            Statement::Select(s) => {
                assert_eq!(s.measurement, "cpu");
                assert!(s.condition.is_some());
                let cond = s.condition.unwrap();
                assert!(cond.contains("host"));
                assert!(cond.contains("="));
            }
            _ => panic!("expected Select"),
        }
    }

    #[test]
    fn test_parse_condition_eq() {
        let mut parser = Parser::new("");
        let filter = parser.parse_condition("host = 'server1'");
        assert!(filter.is_some());
        match filter.unwrap() {
            crate::FilterExpr::Eq(field, value) => {
                assert_eq!(field, "host");
                assert_eq!(value, crate::FieldValue::String("server1".as_bytes().to_vec()));
            }
            _ => panic!("expected Eq"),
        }
    }

    #[test]
    fn test_parse_condition_gt() {
        let mut parser = Parser::new("");
        let filter = parser.parse_condition("value > 100");
        assert!(filter.is_some());
        match filter.unwrap() {
            crate::FilterExpr::Gt(field, value) => {
                assert_eq!(field, "value");
                assert_eq!(value, crate::FieldValue::Integer(100));
            }
            _ => panic!("expected Gt"),
        }
    }

    #[test]
    fn test_parse_condition_and() {
        let mut parser = Parser::new("");
        let filter = parser.parse_condition("host = 'server1' AND value > 100");
        assert!(filter.is_some());
        match filter.unwrap() {
            crate::FilterExpr::And(_, _) => {}
            _ => panic!("expected And"),
        }
    }

    #[test]
    fn test_parse_select_with_order_by() {
        let mut parser = Parser::new("SELECT * FROM cpu ORDER BY time DESC");
        let stmt = parser.parse_statement().unwrap();
        
        match stmt {
            Statement::Select(s) => {
                assert_eq!(s.measurement, "cpu");
                assert_eq!(s.order_by, Some("TIME".to_string()));
            }
            _ => panic!("expected Select"),
        }
    }

    #[test]
    fn test_parse_select_with_slimit() {
        let mut parser = Parser::new("SELECT * FROM cpu SLIMIT 1");
        let stmt = parser.parse_statement().unwrap();
        
        match stmt {
            Statement::Select(s) => {
                assert_eq!(s.measurement, "cpu");
                assert_eq!(s.slimit, Some(1));
            }
            _ => panic!("expected Select"),
        }
    }

    #[test]
    fn test_parse_drop_series() {
        let mut parser = Parser::new("DROP SERIES FROM cpu");
        let stmt = parser.parse_statement().unwrap();
        
        match stmt {
            Statement::DropSeries { condition } => {
                assert!(condition.is_none());
            }
            _ => panic!("expected DropSeries"),
        }
    }

    #[test]
    fn test_parse_drop_series_with_where() {
        let mut parser = Parser::new("DROP SERIES WHERE host = 'server1'");
        let stmt = parser.parse_statement().unwrap();
        
        match stmt {
            Statement::DropSeries { condition } => {
                assert!(condition.is_some());
            }
            _ => panic!("expected DropSeries"),
        }
    }

    #[test]
    fn test_parse_delete() {
        let mut parser = Parser::new("DELETE");
        let stmt = parser.parse_statement().unwrap();
        
        match stmt {
            Statement::Delete { condition } => {
                assert!(condition.is_none());
            }
            _ => panic!("expected Delete"),
        }
    }

    #[test]
    fn test_parse_delete_with_where() {
        let mut parser = Parser::new("DELETE WHERE time < 1000");
        let stmt = parser.parse_statement().unwrap();
        
        match stmt {
            Statement::Delete { condition } => {
                assert!(condition.is_some());
            }
            _ => panic!("expected Delete"),
        }
    }

    #[test]
    fn test_parse_select_with_count() {
        let mut parser = Parser::new("SELECT COUNT(value) FROM cpu");
        let stmt = parser.parse_statement().unwrap();
        
        match stmt {
            Statement::Select(s) => {
                assert_eq!(s.measurement, "cpu");
                assert_eq!(s.fields.len(), 1);
                assert!(matches!(s.fields.get(0), Some(Field::Aggregate(AggregateFunc::Count(Some(n)))) if n == "VALUE"));
            }
            _ => panic!("expected Select"),
        }
    }

    #[test]
    fn test_parse_select_with_count_star() {
        let mut parser = Parser::new("SELECT COUNT(*) FROM cpu");
        let stmt = parser.parse_statement().unwrap();
        
        match stmt {
            Statement::Select(s) => {
                assert_eq!(s.measurement, "cpu");
                assert_eq!(s.fields.len(), 1);
                assert!(matches!(s.fields.get(0), Some(Field::Aggregate(AggregateFunc::Count(None)))));
            }
            _ => panic!("expected Select"),
        }
    }

    #[test]
    fn test_parse_select_with_sum() {
        let mut parser = Parser::new("SELECT SUM(value) FROM cpu");
        let stmt = parser.parse_statement().unwrap();
        
        match stmt {
            Statement::Select(s) => {
                assert_eq!(s.measurement, "cpu");
                assert_eq!(s.fields.len(), 1);
                assert!(matches!(s.fields.get(0), Some(Field::Aggregate(AggregateFunc::Sum(n))) if n == "VALUE"));
            }
            _ => panic!("expected Select"),
        }
    }

    #[test]
    fn test_parse_select_with_mean() {
        let mut parser = Parser::new("SELECT MEAN(value) FROM cpu");
        let stmt = parser.parse_statement().unwrap();
        
        match stmt {
            Statement::Select(s) => {
                assert_eq!(s.measurement, "cpu");
                assert_eq!(s.fields.len(), 1);
                assert!(matches!(s.fields.get(0), Some(Field::Aggregate(AggregateFunc::Mean(n))) if n == "VALUE"));
            }
            _ => panic!("expected Select"),
        }
    }

    #[test]
    fn test_parse_select_with_min() {
        let mut parser = Parser::new("SELECT MIN(value) FROM cpu");
        let stmt = parser.parse_statement().unwrap();
        
        match stmt {
            Statement::Select(s) => {
                assert_eq!(s.measurement, "cpu");
                assert_eq!(s.fields.len(), 1);
                assert!(matches!(s.fields.get(0), Some(Field::Aggregate(AggregateFunc::Min(n))) if n == "VALUE"));
            }
            _ => panic!("expected Select"),
        }
    }

    #[test]
    fn test_parse_select_with_max() {
        let mut parser = Parser::new("SELECT MAX(value) FROM cpu");
        let stmt = parser.parse_statement().unwrap();
        
        match stmt {
            Statement::Select(s) => {
                assert_eq!(s.measurement, "cpu");
                assert_eq!(s.fields.len(), 1);
                assert!(matches!(s.fields.get(0), Some(Field::Aggregate(AggregateFunc::Max(n))) if n == "VALUE"));
            }
            _ => panic!("expected Select"),
        }
    }

    #[test]
    fn test_parse_select_with_first() {
        let mut parser = Parser::new("SELECT FIRST(value) FROM cpu");
        let stmt = parser.parse_statement().unwrap();
        
        match stmt {
            Statement::Select(s) => {
                assert_eq!(s.measurement, "cpu");
                assert_eq!(s.fields.len(), 1);
                assert!(matches!(s.fields.get(0), Some(Field::Aggregate(AggregateFunc::First(n))) if n == "VALUE"));
            }
            _ => panic!("expected Select"),
        }
    }

    #[test]
    fn test_parse_select_with_last() {
        let mut parser = Parser::new("SELECT LAST(value) FROM cpu");
        let stmt = parser.parse_statement().unwrap();
        
        match stmt {
            Statement::Select(s) => {
                assert_eq!(s.measurement, "cpu");
                assert_eq!(s.fields.len(), 1);
                assert!(matches!(s.fields.get(0), Some(Field::Aggregate(AggregateFunc::Last(n))) if n == "VALUE"));
            }
            _ => panic!("expected Select"),
        }
    }

    #[test]
    fn test_parse_select_with_multiple_aggregates() {
        let mut parser = Parser::new("SELECT COUNT(value), SUM(value), MEAN(value) FROM cpu");
        let stmt = parser.parse_statement().unwrap();
        
        match stmt {
            Statement::Select(s) => {
                assert_eq!(s.measurement, "cpu");
                assert_eq!(s.fields.len(), 3);
                assert!(matches!(s.fields.get(0), Some(Field::Aggregate(AggregateFunc::Count(Some(n)))) if n == "VALUE"));
                assert!(matches!(s.fields.get(1), Some(Field::Aggregate(AggregateFunc::Sum(n))) if n == "VALUE"));
                assert!(matches!(s.fields.get(2), Some(Field::Aggregate(AggregateFunc::Mean(n))) if n == "VALUE"));
            }
            _ => panic!("expected Select"),
        }
    }

    #[test]
    fn test_parse_select_with_raw_and_aggregate() {
        let mut parser = Parser::new("SELECT host, COUNT(value) FROM cpu");
        let stmt = parser.parse_statement().unwrap();
        
        match stmt {
            Statement::Select(s) => {
                assert_eq!(s.measurement, "cpu");
                assert_eq!(s.fields.len(), 2);
                match s.fields.get(0) {
                    Some(Field::Raw(n)) => assert_eq!(n, "HOST"),
                    _ => panic!("expected Raw field"),
                }
                match s.fields.get(1) {
                    Some(Field::Aggregate(AggregateFunc::Count(Some(n)))) => assert_eq!(n, "VALUE"),
                    _ => panic!("expected Count aggregate"),
                }
            }
            _ => panic!("expected Select"),
        }
    }

    #[test]
    fn test_parse_select_count_with_where() {
        let mut parser = Parser::new("SELECT COUNT(value) FROM cpu WHERE host = 'server1'");
        let stmt = parser.parse_statement().unwrap();
        
        match stmt {
            Statement::Select(s) => {
                assert_eq!(s.measurement, "cpu");
                assert_eq!(s.fields.len(), 1);
                assert!(matches!(s.fields.get(0), Some(Field::Aggregate(AggregateFunc::Count(Some(n)))) if n == "VALUE"));
                assert!(s.condition.is_some());
            }
            _ => panic!("expected Select"),
        }
    }

    #[test]
    fn test_parse_select_with_group_by() {
        let mut parser = Parser::new("SELECT COUNT(value) FROM cpu GROUP BY host");
        let stmt = parser.parse_statement().unwrap();
        
        match stmt {
            Statement::Select(s) => {
                assert_eq!(s.measurement, "cpu");
                assert_eq!(s.fields.len(), 1);
                assert!(matches!(s.fields.get(0), Some(Field::Aggregate(AggregateFunc::Count(Some(n)))) if n == "VALUE"));
                assert_eq!(s.group_by, vec!["host"]);
            }
            _ => panic!("expected Select"),
        }
    }

    #[test]
    fn test_parse_select_with_group_by_multiple_tags() {
        let mut parser = Parser::new("SELECT COUNT(value) FROM cpu GROUP BY host, region");
        let stmt = parser.parse_statement().unwrap();
        
        match stmt {
            Statement::Select(s) => {
                assert_eq!(s.measurement, "cpu");
                assert_eq!(s.group_by, vec!["host", "region"]);
            }
            _ => panic!("expected Select"),
        }
    }
}