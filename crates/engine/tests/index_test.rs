use openGemini_engine::index::SeriesIndex;
use roaring::RoaringBitmap;

#[test]
fn test_series_index_new() {
    let index = SeriesIndex::new();
    assert!(index.is_empty());
}

#[test]
fn test_series_index_add() {
    let mut index = SeriesIndex::new();
    index.add(1);
    index.add(2);
    index.add(3);

    assert!(!index.is_empty());
    assert_eq!(index.len(), 3);
}

#[test]
fn test_series_index_contains() {
    let mut index = SeriesIndex::new();
    index.add(1);
    index.add(2);
    index.add(3);

    assert!(index.contains(1));
    assert!(index.contains(2));
    assert!(index.contains(3));
    assert!(!index.contains(4));
}

#[test]
fn test_series_index_range() {
    let mut index = SeriesIndex::new();
    for i in 1..=10 {
        index.add(i);
    }

    let result = index.range(3, 7);
    assert_eq!(result.len(), 4);
    assert!(result.contains(3));
    assert!(result.contains(4));
    assert!(result.contains(5));
    assert!(result.contains(6));
    assert!(!result.contains(7));
}

#[test]
fn test_series_index_range_empty() {
    let mut index = SeriesIndex::new();
    index.add(1);
    index.add(2);
    index.add(3);

    let result = index.range(10, 20);
    assert!(result.is_empty());
}

#[test]
fn test_series_index_clear() {
    let mut index = SeriesIndex::new();
    index.add(1);
    index.add(2);

    index.clear();
    assert!(index.is_empty());
}

#[test]
fn test_series_index_cardinality() {
    let mut index = SeriesIndex::new();
    assert_eq!(index.cardinality(), 0);

    index.add(1);
    index.add(2);
    index.add(3);

    assert_eq!(index.cardinality(), 3);
}
