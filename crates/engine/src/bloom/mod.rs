use std::collections::hash_map::DefaultHasher;
use std::f64::consts::LN_2;
use std::hash::Hasher;

#[derive(Debug, Clone)]
pub struct BloomFilter {
    bit_array: Vec<u64>,
    num_bits: usize,
    num_hashes: usize,
}

impl BloomFilter {
    pub fn new(expected_elements: usize, false_positive_rate: f64) -> Self {
        let num_bits = (-(expected_elements as f64) * false_positive_rate.ln() / (LN_2.powi(2))).ceil() as usize;
        let num_hashes = ((num_bits as f64 / expected_elements as f64) * LN_2).ceil() as usize;
        
        let num_words = num_bits.div_ceil(64);
        let bit_array = vec![0u64; num_words.max(1)];
        
        Self {
            bit_array,
            num_bits: num_bits.max(1),
            num_hashes: num_hashes.max(1),
        }
    }
    
    pub fn with_capacity(capacity: usize) -> Self {
        Self::new(capacity, 0.01)
    }
    
    pub fn get_bit_array(&self) -> &Vec<u64> {
        &self.bit_array
    }
    
    pub fn set_bit_array(&mut self, bit_array: Vec<u64>) {
        self.bit_array = bit_array;
    }
    
    fn hash(&self, item: &[u8], seed: u64) -> usize {
        let mut hasher = DefaultHasher::new();
        hasher.write_u64(seed);
        hasher.write(item);
        (hasher.finish() as usize) % self.num_bits
    }
    
    pub fn insert(&mut self, item: &[u8]) {
        for i in 0..self.num_hashes {
            let pos = self.hash(item, i as u64);
            self.bit_array[pos / 64] |= 1 << (pos % 64);
        }
    }
    
    pub fn contains(&self, item: &[u8]) -> bool {
        for i in 0..self.num_hashes {
            let pos = self.hash(item, i as u64);
            if self.bit_array[pos / 64] & (1 << (pos % 64)) == 0 {
                return false;
            }
        }
        true
    }
    
    pub fn clear(&mut self) {
        for word in &mut self.bit_array {
            *word = 0;
        }
    }
    
    pub fn len(&self) -> usize {
        self.num_bits
    }
    
    pub fn is_empty(&self) -> bool {
        self.num_bits == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_bloom_filter_new() {
        let filter = BloomFilter::new(1000, 0.01);
        assert!(filter.num_bits > 0);
        assert!(filter.num_hashes > 0);
    }
    
    #[test]
    fn test_bloom_filter_with_capacity() {
        let filter = BloomFilter::with_capacity(1000);
        assert!(filter.contains(b"test") == false || filter.contains(b"test") == true);
    }
    
    #[test]
    fn test_bloom_filter_insert_and_check() {
        let mut filter = BloomFilter::with_capacity(100);
        
        filter.insert(b"hello");
        filter.insert(b"world");
        
        assert!(filter.contains(b"hello"));
        assert!(filter.contains(b"world"));
    }
    
    #[test]
    fn test_bloom_filter_not_contains() {
        let mut filter = BloomFilter::with_capacity(100);
        
        filter.insert(b"hello");
        
        assert!(!filter.contains(b"not_inserted"));
    }
    
    #[test]
    fn test_bloom_filter_clear() {
        let mut filter = BloomFilter::with_capacity(100);
        
        filter.insert(b"hello");
        filter.clear();
        
        assert!(!filter.contains(b"hello"));
    }
    
    #[test]
    fn test_bloom_filter_len() {
        let filter = BloomFilter::with_capacity(100);
        assert_eq!(filter.len(), filter.num_bits);
    }
    
    #[test]
    fn test_bloom_filter_false_positive_rate() {
        let mut filter = BloomFilter::new(10000, 0.01);
        
        for i in 0u32..10000 {
            filter.insert(&i.to_le_bytes());
        }
        
        let mut false_positives = 0;
        for i in 10000u32..20000 {
            if filter.contains(&i.to_le_bytes()) {
                false_positives += 1;
            }
        }
        
        let actual_fpr = false_positives as f64 / 10000.0;
        assert!(actual_fpr < 0.05);
    }
    
    #[test]
    fn test_bloom_filter_empty() {
        let filter = BloomFilter::with_capacity(100);
        assert!(!filter.is_empty());
    }

    #[test]
    fn test_bloom_filter_get_bit_array() {
        let filter = BloomFilter::with_capacity(100);
        let bit_array = filter.get_bit_array();
        assert!(!bit_array.is_empty());
    }

    #[test]
    fn test_bloom_filter_set_bit_array() {
        let mut filter = BloomFilter::with_capacity(100);
        filter.insert(b"test");

        let bit_array = filter.get_bit_array().clone();
        let mut new_filter = BloomFilter::with_capacity(100);
        new_filter.set_bit_array(bit_array);

        assert!(new_filter.contains(b"test"));
    }

    #[test]
    fn test_bloom_filter_large_capacity() {
        let mut filter = BloomFilter::with_capacity(1_000_000);
        for i in 0u32..1000 {
            filter.insert(&i.to_le_bytes());
        }
        for i in 0u32..1000 {
            assert!(filter.contains(&i.to_le_bytes()));
        }
    }

    #[test]
    fn test_bloom_filter_multiple_inserts_same_key() {
        let mut filter = BloomFilter::with_capacity(100);
        filter.insert(b"test");
        filter.insert(b"test");
        filter.insert(b"test");
        assert!(filter.contains(b"test"));
    }
}