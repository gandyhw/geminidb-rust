#[derive(Debug, Clone)]
pub struct BlockMeta {
    pub column_id: u32,
    pub block_id: u32,
    pub offset: u64,
    pub size: u64,
    pub min_time: i64,
    pub max_time: i64,
    pub row_count: u32,
    pub min_value: Vec<u8>,
    pub max_value: Vec<u8>,
}

impl BlockMeta {
    pub fn new(column_id: u32, block_id: u32, offset: u64, size: u64) -> Self {
        Self {
            column_id,
            block_id,
            offset,
            size,
            min_time: i64::MAX,
            max_time: i64::MIN,
            row_count: 0,
            min_value: Vec::new(),
            max_value: Vec::new(),
        }
    }
    
    pub fn update_time(&mut self, timestamp: i64) {
        self.min_time = self.min_time.min(timestamp);
        self.max_time = self.max_time.max(timestamp);
        self.row_count += 1;
    }
    
    pub fn update_value(&mut self, value: &[u8]) {
        if self.min_value.is_empty() || value < &self.min_value {
            self.min_value = value.to_vec();
        }
        if self.max_value.is_empty() || value > &self.max_value {
            self.max_value = value.to_vec();
        }
    }
    
    pub fn contains_time(&self, time: i64) -> bool {
        time >= self.min_time && time <= self.max_time
    }
    
    pub fn overlaps_time_range(&self, start: i64, end: i64) -> bool {
        self.max_time >= start && self.min_time <= end
    }
}

#[derive(Debug, Clone)]
pub struct BlockIndex {
    pub blocks: Vec<BlockMeta>,
}

impl BlockIndex {
    pub fn new() -> Self {
        Self {
            blocks: Vec::new(),
        }
    }
    
    pub fn add_block(&mut self, meta: BlockMeta) {
        self.blocks.push(meta);
    }
    
    pub fn get_block(&self, block_id: u32) -> Option<&BlockMeta> {
        self.blocks.get(block_id as usize)
    }
    
    pub fn find_blocks_by_time(&self, start: i64, end: i64) -> Vec<u32> {
        self.blocks.iter()
            .filter(|b| b.overlaps_time_range(start, end))
            .map(|b| b.block_id)
            .collect()
    }
    
    pub fn len(&self) -> usize {
        self.blocks.len()
    }
    
    pub fn is_empty(&self) -> bool {
        self.blocks.is_empty()
    }
}

impl Default for BlockIndex {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_block_meta_new() {
        let meta = BlockMeta::new(0, 1, 100, 50);
        assert_eq!(meta.column_id, 0);
        assert_eq!(meta.block_id, 1);
        assert_eq!(meta.offset, 100);
        assert_eq!(meta.size, 50);
        assert_eq!(meta.row_count, 0);
    }
    
    #[test]
    fn test_block_meta_update_time() {
        let mut meta = BlockMeta::new(0, 0, 0, 0);
        meta.update_time(1000);
        meta.update_time(2000);
        meta.update_time(1500);
        
        assert_eq!(meta.min_time, 1000);
        assert_eq!(meta.max_time, 2000);
        assert_eq!(meta.row_count, 3);
    }
    
    #[test]
    fn test_block_meta_update_value() {
        let mut meta = BlockMeta::new(0, 0, 0, 0);
        meta.update_value(b"apple");
        meta.update_value(b"banana");
        meta.update_value(b"cherry");
        
        assert_eq!(meta.min_value, b"apple");
        assert_eq!(meta.max_value, b"cherry");
    }
    
    #[test]
    fn test_block_meta_contains_time() {
        let mut meta = BlockMeta::new(0, 0, 0, 0);
        meta.update_time(1000);
        meta.update_time(2000);
        
        assert!(meta.contains_time(1500));
        assert!(meta.contains_time(1000));
        assert!(meta.contains_time(2000));
        assert!(!meta.contains_time(500));
        assert!(!meta.contains_time(2500));
    }
    
    #[test]
    fn test_block_meta_overlaps_time_range() {
        let mut meta = BlockMeta::new(0, 0, 0, 0);
        meta.update_time(1000);
        meta.update_time(2000);
        
        assert!(meta.overlaps_time_range(1500, 2500));
        assert!(meta.overlaps_time_range(500, 1500));
        assert!(!meta.overlaps_time_range(2001, 3000));
    }
    
    #[test]
    fn test_block_index_new() {
        let index = BlockIndex::new();
        assert!(index.is_empty());
        assert_eq!(index.len(), 0);
    }
    
    #[test]
    fn test_block_index_add_block() {
        let mut index = BlockIndex::new();
        index.add_block(BlockMeta::new(0, 0, 100, 50));
        index.add_block(BlockMeta::new(0, 1, 200, 60));
        
        assert_eq!(index.len(), 2);
    }
    
    #[test]
    fn test_block_index_get_block() {
        let mut index = BlockIndex::new();
        index.add_block(BlockMeta::new(0, 0, 100, 50));
        
        let block = index.get_block(0);
        assert!(block.is_some());
        assert_eq!(block.unwrap().offset, 100);
    }
    
    #[test]
    fn test_block_index_find_blocks_by_time() {
        let mut index = BlockIndex::new();
        
        let mut block1 = BlockMeta::new(0, 0, 0, 0);
        block1.update_time(1000);
        block1.update_time(1500);
        index.add_block(block1);
        
        let mut block2 = BlockMeta::new(0, 1, 0, 0);
        block2.update_time(2000);
        block2.update_time(2500);
        index.add_block(block2);
        
        let mut block3 = BlockMeta::new(0, 2, 0, 0);
        block3.update_time(3000);
        block3.update_time(3500);
        index.add_block(block3);
        
        let found = index.find_blocks_by_time(1500, 2750);
        assert_eq!(found.len(), 2);
        assert!(found.contains(&0));
        assert!(found.contains(&1));
    }
}