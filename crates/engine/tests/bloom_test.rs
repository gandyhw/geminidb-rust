use openGemini_engine::bloom::BloomFilter;

#[test]
fn test_bloom_filter_export_import() {
    let mut filter = BloomFilter::with_capacity(100);
    filter.insert(b"series1");
    filter.insert(b"series2");
    
    let bit_array = filter.get_bit_array().clone();
    
    let mut imported_filter = BloomFilter::with_capacity(100);
    imported_filter.set_bit_array(bit_array);
    
    assert!(imported_filter.contains(b"series1"));
    assert!(imported_filter.contains(b"series2"));
}

#[test]
fn test_bloom_filter_many_elements() {
    let mut filter = BloomFilter::with_capacity(100000);
    
    for i in 0..10000 {
        let key = format!("series_{}", i);
        filter.insert(key.as_bytes());
    }
    
    for i in 0..10000 {
        let key = format!("series_{}", i);
        assert!(filter.contains(key.as_bytes()));
    }
}