use roaring::RoaringBitmap;

pub struct SeriesIndex {
    bitmap: RoaringBitmap,
}

impl SeriesIndex {
    pub fn new() -> Self {
        Self {
            bitmap: RoaringBitmap::new(),
        }
    }

    pub fn add(&mut self, series_id: u64) {
        self.bitmap.insert(series_id as u32);
    }

    pub fn contains(&self, series_id: u64) -> bool {
        self.bitmap.contains(series_id as u32)
    }

    pub fn range(&self, start: u64, end: u64) -> RoaringBitmap {
        let start = start as u32;
        let end = end as u32;
        self.bitmap.clone().into_iter()
            .filter(|&x| x >= start && x < end)
            .collect()
    }

    pub fn is_empty(&self) -> bool {
        self.bitmap.is_empty()
    }

    pub fn len(&self) -> u64 {
        self.bitmap.len() as u64
    }

    pub fn cardinality(&self) -> u64 {
        self.bitmap.len() as u64
    }

    pub fn clear(&mut self) {
        self.bitmap.clear();
    }
}

impl Default for SeriesIndex {
    fn default() -> Self {
        Self::new()
    }
}
