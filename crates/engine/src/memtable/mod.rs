use crate::error::Result;
use crate::{FieldValue, Row, WriteBatch};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct RowKey {
    pub table: Vec<u8>,
    pub timestamp: i64,
    pub series_key: Vec<u8>,
}

impl RowKey {
    pub fn estimate_size(&self) -> u64 {
        (self.table.len() + self.series_key.len()) as u64 + 16
    }
}

#[derive(Debug, Clone)]
pub struct RowValue {
    pub tags: Vec<u8>,
    pub fields: Vec<u8>,
}

impl RowValue {
    pub fn estimate_size(&self) -> u64 {
        (self.tags.len() + self.fields.len()) as u64
    }
}

pub struct MemTable {
    data: BTreeMap<RowKey, RowValue>,
    size: AtomicU64,
    max_size: u64,
    row_count: AtomicU64,
}

impl MemTable {
    pub fn new(max_size: u64) -> Self {
        Self {
            data: BTreeMap::new(),
            size: AtomicU64::new(0),
            max_size,
            row_count: AtomicU64::new(0),
        }
    }

    pub fn insert(&mut self, batch: WriteBatch) -> Result<()> {
        for row in batch.rows {
            let key = RowKey {
                table: batch.table.as_bytes().to_vec(),
                timestamp: row.timestamp,
                series_key: Self::encode_series_key(&row.tags),
            };

            let value = RowValue {
                tags: serde_json::to_vec(&row.tags).unwrap_or_default(),
                fields: serde_json::to_vec(&row.fields).unwrap_or_default(),
            };

            let entry_size = key.estimate_size() + value.estimate_size();
            self.size.fetch_add(entry_size, Ordering::Relaxed);
            self.row_count.fetch_add(1, Ordering::Relaxed);

            self.data.insert(key, value);
        }
        Ok(())
    }

    fn encode_series_key(tags: &std::collections::HashMap<String, String>) -> Vec<u8> {
        let mut keys: Vec<_> = tags.iter().collect();
        keys.sort();
        keys.iter()
            .map(|(k, v)| format!("{}={}", k, v))
            .collect::<Vec<_>>()
            .join(",")
            .into_bytes()
    }

    pub fn scan(&self, table: &[u8], start: i64, end: i64) -> Result<Vec<(RowKey, RowValue)>> {
        let start_key = RowKey {
            table: table.to_vec(),
            timestamp: start,
            series_key: vec![],
        };
        let end_key = RowKey {
            table: table.to_vec(),
            timestamp: end,
            series_key: vec![],
        };

        let mut results = Vec::new();
        for (key, value) in self.data.range(start_key..end_key) {
            results.push((key.clone(), value.clone()));
        }
        Ok(results)
    }

    pub fn size(&self) -> u64 {
        self.size.load(Ordering::Relaxed)
    }

    pub fn row_count(&self) -> u64 {
        self.row_count.load(Ordering::Relaxed)
    }

    pub fn should_flush(&self) -> bool {
        self.size.load(Ordering::Relaxed) >= self.max_size
    }

    pub fn flush(&mut self) -> Result<Vec<Row>> {
        let mut rows = Vec::with_capacity(self.row_count() as usize);

        for (key, value) in self.data.iter() {
            let tags: std::collections::HashMap<String, String> =
                serde_json::from_slice(&value.tags).unwrap_or_default();
            let fields: std::collections::HashMap<String, FieldValue> =
                serde_json::from_slice(&value.fields).unwrap_or_default();

            rows.push(Row {
                tags,
                fields,
                timestamp: key.timestamp,
            });
        }

        self.data.clear();
        self.size.store(0, Ordering::Relaxed);
        self.row_count.store(0, Ordering::Relaxed);

        Ok(rows)
    }
}
