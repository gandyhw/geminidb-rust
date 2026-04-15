use openGemini_engine::index::SeriesIndex;

#[cfg(test)]
mod benchmarks {
    use super::*;
    use std::time::Instant;

    #[test]
    pub fn bench_series_index_add_1000() {
        let start = Instant::now();
        for _ in 0..100 {
            let mut index = SeriesIndex::new();
            for i in 0..1000 {
                index.add(i as u64);
            }
        }
        let elapsed = start.elapsed();
        println!("series_index_add_1000 (x100): {:?}", elapsed);
    }

    #[test]
    pub fn bench_series_index_add_10000() {
        let start = Instant::now();
        for _ in 0..10 {
            let mut index = SeriesIndex::new();
            for i in 0..10000 {
                index.add(i as u64);
            }
        }
        let elapsed = start.elapsed();
        println!("series_index_add_10000 (x10): {:?}", elapsed);
    }

    #[test]
    pub fn bench_series_index_add_100000() {
        let start = Instant::now();
        let mut index = SeriesIndex::new();
        for i in 0..100000 {
            index.add(i as u64);
        }
        let elapsed = start.elapsed();
        println!("series_index_add_100000: {:?}", elapsed);
    }

    #[test]
    pub fn bench_series_index_contains_100000() {
        let mut index = SeriesIndex::new();
        for i in 0..100000 {
            index.add(i as u64);
        }

        let start = Instant::now();
        for i in 0..100000 {
            std::hint::black_box(index.contains(i as u64));
        }
        let elapsed = start.elapsed();
        println!("series_index_contains_100000: {:?}", elapsed);
    }

    #[test]
    pub fn bench_series_index_range_100000() {
        let mut index = SeriesIndex::new();
        for i in 0..100000 {
            index.add(i as u64);
        }

        let start = Instant::now();
        let _ = std::hint::black_box(index.range(25000, 75000));
        let elapsed = start.elapsed();
        println!("series_index_range_100000: {:?}", elapsed);
    }

    #[test]
    pub fn bench_series_index_cardinality_100000() {
        let mut index = SeriesIndex::new();
        for i in 0..100000 {
            index.add(i as u64);
        }

        let start = Instant::now();
        std::hint::black_box(index.cardinality());
        let elapsed = start.elapsed();
        println!("series_index_cardinality_100000: {:?}", elapsed);
    }
}
