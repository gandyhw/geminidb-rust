use crate::error::Result;
use std::collections::HashMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DownsampleInterval {
    Min1 = 60,
    Min5 = 300,
    Min15 = 900,
    Min30 = 1800,
    Hour1 = 3600,
    Hour6 = 21600,
    Day1 = 86400,
}

impl DownsampleInterval {
    pub fn from_seconds(seconds: u64) -> Option<Self> {
        match seconds {
            60 => Some(DownsampleInterval::Min1),
            300 => Some(DownsampleInterval::Min5),
            900 => Some(DownsampleInterval::Min15),
            1800 => Some(DownsampleInterval::Min30),
            3600 => Some(DownsampleInterval::Hour1),
            21600 => Some(DownsampleInterval::Hour6),
            86400 => Some(DownsampleInterval::Day1),
            _ => None,
        }
    }

    pub fn as_seconds(&self) -> u64 {
        *self as u64
    }
}

#[derive(Debug, Clone)]
pub struct DownsampleRule {
    pub name: String,
    pub source_interval: DownsampleInterval,
    pub target_interval: DownsampleInterval,
    pub aggregators: Vec<AggregatorType>,
}

impl DownsampleRule {
    pub fn new(name: String, source: DownsampleInterval, target: DownsampleInterval) -> Self {
        Self {
            name,
            source_interval: source,
            target_interval: target,
            aggregators: Vec::new(),
        }
    }

    pub fn with_aggregators(mut self, aggs: Vec<AggregatorType>) -> Self {
        self.aggregators = aggs;
        self
    }

    pub fn get_bucket_time(&self, timestamp: i64) -> i64 {
        let interval = self.target_interval.as_seconds() as i64;
        (timestamp / interval) * interval
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AggregatorType {
    Sum,
    Min,
    Max,
    Mean,
    First,
    Last,
    Count,
}

impl AggregatorType {
    pub fn as_str(&self) -> &'static str {
        match self {
            AggregatorType::Sum => "sum",
            AggregatorType::Min => "min",
            AggregatorType::Max => "max",
            AggregatorType::Mean => "mean",
            AggregatorType::First => "first",
            AggregatorType::Last => "last",
            AggregatorType::Count => "count",
        }
    }
}

pub struct DownsampleEngine {
    rules: HashMap<String, DownsampleRule>,
}

impl DownsampleEngine {
    pub fn new() -> Self {
        Self {
            rules: HashMap::new(),
        }
    }

    pub fn add_rule(&mut self, rule: DownsampleRule) {
        self.rules.insert(rule.name.clone(), rule);
    }

    pub fn remove_rule(&mut self, name: &str) -> bool {
        self.rules.remove(name).is_some()
    }

    pub fn get_rule(&self, name: &str) -> Option<&DownsampleRule> {
        self.rules.get(name)
    }

    pub fn list_rules(&self) -> Vec<&DownsampleRule> {
        self.rules.values().collect()
    }

    pub fn aggregate(
        &self,
        rule_name: &str,
        values: &[i64],
    ) -> Result<HashMap<AggregatorType, i64>> {
        let rule = self.rules.get(rule_name)
            .ok_or_else(|| crate::Error::InvalidArgument(format!("rule {} not found", rule_name)))?;

        if values.is_empty() {
            let mut result = HashMap::new();
            for agg in &rule.aggregators {
                result.insert(*agg, 0);
            }
            return Ok(result);
        }

        let mut result = HashMap::new();

        for agg in &rule.aggregators {
            let value = match agg {
                AggregatorType::Sum => values.iter().sum(),
                AggregatorType::Min => *values.iter().min().unwrap_or(&0),
                AggregatorType::Max => *values.iter().max().unwrap_or(&0),
                AggregatorType::Mean => values.iter().sum::<i64>() / values.len() as i64,
                AggregatorType::First => values[0],
                AggregatorType::Last => *values.last().unwrap_or(&0),
                AggregatorType::Count => values.len() as i64,
            };
            result.insert(*agg, value);
        }

        Ok(result)
    }

    pub fn downsample_points(
        &self,
        rule_name: &str,
        timestamps: &[i64],
        values: &[i64],
    ) -> Result<Vec<(i64, i64)>> {
        let rule = self.rules.get(rule_name)
            .ok_or_else(|| crate::Error::InvalidArgument(format!("rule {} not found", rule_name)))?;

        if timestamps.len() != values.len() {
            return Err(crate::Error::InvalidArgument(
                "timestamps and values must have same length".to_string(),
            ));
        }

        let mut buckets: HashMap<i64, Vec<i64>> = HashMap::new();

        for (i, &ts) in timestamps.iter().enumerate() {
            let bucket_time = rule.get_bucket_time(ts);
            buckets.entry(bucket_time).or_default().push(values[i]);
        }

        let mut results: Vec<(i64, i64)> = Vec::new();
        let mut keys: Vec<i64> = buckets.keys().cloned().collect();
        keys.sort();

        for bucket_time in keys {
            let bucket_values = buckets.remove(&bucket_time).unwrap();
            let aggregated = self.aggregate_internal(&bucket_values);
            results.push((bucket_time, aggregated));
        }

        Ok(results)
    }

    fn aggregate_internal(&self, values: &[i64]) -> i64 {
        if values.is_empty() {
            return 0;
        }
        values.iter().sum::<i64>() / values.len() as i64
    }
}

impl Default for DownsampleEngine {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_downsample_interval_from_seconds() {
        assert_eq!(DownsampleInterval::from_seconds(60), Some(DownsampleInterval::Min1));
        assert_eq!(DownsampleInterval::from_seconds(300), Some(DownsampleInterval::Min5));
        assert_eq!(DownsampleInterval::from_seconds(3600), Some(DownsampleInterval::Hour1));
        assert_eq!(DownsampleInterval::from_seconds(86400), Some(DownsampleInterval::Day1));
        assert_eq!(DownsampleInterval::from_seconds(999), None);
    }

    #[test]
    fn test_downsample_interval_as_seconds() {
        assert_eq!(DownsampleInterval::Min1.as_seconds(), 60);
        assert_eq!(DownsampleInterval::Min5.as_seconds(), 300);
        assert_eq!(DownsampleInterval::Hour1.as_seconds(), 3600);
        assert_eq!(DownsampleInterval::Day1.as_seconds(), 86400);
    }

    #[test]
    fn test_downsample_rule_new() {
        let rule = DownsampleRule::new(
            "cpu_1m_to_5m".to_string(),
            DownsampleInterval::Min1,
            DownsampleInterval::Min5,
        );
        
        assert_eq!(rule.name, "cpu_1m_to_5m");
        assert_eq!(rule.source_interval, DownsampleInterval::Min1);
        assert_eq!(rule.target_interval, DownsampleInterval::Min5);
        assert!(rule.aggregators.is_empty());
    }

    #[test]
    fn test_downsample_rule_with_aggregators() {
        let rule = DownsampleRule::new(
            "cpu_1m_to_5m".to_string(),
            DownsampleInterval::Min1,
            DownsampleInterval::Min5,
        )
        .with_aggregators(vec![AggregatorType::Mean, AggregatorType::Max]);
        
        assert_eq!(rule.aggregators.len(), 2);
    }

    #[test]
    fn test_downsample_rule_get_bucket_time() {
        let rule = DownsampleRule::new(
            "cpu_1m_to_5m".to_string(),
            DownsampleInterval::Min1,
            DownsampleInterval::Min5,
        );
        
        assert_eq!(rule.get_bucket_time(0), 0);
        assert_eq!(rule.get_bucket_time(60), 0);
        assert_eq!(rule.get_bucket_time(120), 0);
        assert_eq!(rule.get_bucket_time(300), 300);
        assert_eq!(rule.get_bucket_time(301), 300);
    }

    #[test]
    fn test_aggregator_type_as_str() {
        assert_eq!(AggregatorType::Sum.as_str(), "sum");
        assert_eq!(AggregatorType::Min.as_str(), "min");
        assert_eq!(AggregatorType::Max.as_str(), "max");
        assert_eq!(AggregatorType::Mean.as_str(), "mean");
        assert_eq!(AggregatorType::First.as_str(), "first");
        assert_eq!(AggregatorType::Last.as_str(), "last");
        assert_eq!(AggregatorType::Count.as_str(), "count");
    }

    #[test]
    fn test_downsample_engine_new() {
        let engine = DownsampleEngine::new();
        assert!(engine.list_rules().is_empty());
    }

    #[test]
    fn test_downsample_engine_add_remove_rule() {
        let mut engine = DownsampleEngine::new();
        
        let rule = DownsampleRule::new(
            "cpu_1m_to_5m".to_string(),
            DownsampleInterval::Min1,
            DownsampleInterval::Min5,
        );
        
        engine.add_rule(rule);
        assert_eq!(engine.list_rules().len(), 1);
        
        assert!(engine.get_rule("cpu_1m_to_5m").is_some());
        
        assert!(engine.remove_rule("cpu_1m_to_5m"));
        assert!(!engine.remove_rule("nonexistent"));
    }

    #[test]
    fn test_downsample_engine_aggregate() {
        let mut engine = DownsampleEngine::new();
        
        let rule = DownsampleRule::new(
            "cpu".to_string(),
            DownsampleInterval::Min1,
            DownsampleInterval::Min5,
        )
        .with_aggregators(vec![
            AggregatorType::Sum,
            AggregatorType::Min,
            AggregatorType::Max,
            AggregatorType::Mean,
            AggregatorType::First,
            AggregatorType::Last,
            AggregatorType::Count,
        ]);
        
        engine.add_rule(rule);
        
        let result = engine.aggregate("cpu", &[10, 20, 30, 40, 50]).unwrap();
        
        assert_eq!(result[&AggregatorType::Sum], 150);
        assert_eq!(result[&AggregatorType::Min], 10);
        assert_eq!(result[&AggregatorType::Max], 50);
        assert_eq!(result[&AggregatorType::Mean], 30);
        assert_eq!(result[&AggregatorType::First], 10);
        assert_eq!(result[&AggregatorType::Last], 50);
        assert_eq!(result[&AggregatorType::Count], 5);
    }

    #[test]
    fn test_downsample_engine_aggregate_empty() {
        let mut engine = DownsampleEngine::new();
        
        let rule = DownsampleRule::new(
            "cpu".to_string(),
            DownsampleInterval::Min1,
            DownsampleInterval::Min5,
        )
        .with_aggregators(vec![AggregatorType::Sum, AggregatorType::Mean]);
        
        engine.add_rule(rule);
        
        let result = engine.aggregate("cpu", &[]).unwrap();
        
        assert_eq!(result[&AggregatorType::Sum], 0);
        assert_eq!(result[&AggregatorType::Mean], 0);
    }

    #[test]
    fn test_downsample_engine_downsample_points() {
        let mut engine = DownsampleEngine::new();
        
        let rule = DownsampleRule::new(
            "cpu_1m_to_5m".to_string(),
            DownsampleInterval::Min1,
            DownsampleInterval::Min5,
        );
        
        engine.add_rule(rule);
        
        let timestamps = vec![0, 60, 120, 180, 240, 300, 360, 420];
        let values = vec![10, 20, 30, 40, 50, 60, 70, 80];
        
        let result = engine.downsample_points("cpu_1m_to_5m", &timestamps, &values).unwrap();
        
        assert!(!result.is_empty());
        for (ts, _val) in &result {
            assert_eq!(ts % 300, 0);
        }
    }

    #[test]
    fn test_downsample_engine_downsample_mismatched_lengths() {
        let mut engine = DownsampleEngine::new();
        
        let rule = DownsampleRule::new(
            "cpu".to_string(),
            DownsampleInterval::Min1,
            DownsampleInterval::Min5,
        );
        
        engine.add_rule(rule);
        
        let result = engine.downsample_points("cpu", &[0, 60], &[10]);
        assert!(result.is_err());
    }

    #[test]
    fn test_downsample_engine_nonexistent_rule() {
        let engine = DownsampleEngine::new();
        
        let result = engine.aggregate("nonexistent", &[10, 20]);
        assert!(result.is_err());
    }
}
