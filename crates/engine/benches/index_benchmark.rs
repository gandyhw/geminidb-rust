use std::hint::black_box;
use criterion::{criterion_group, criterion_main, Criterion};

fn bench_series_index_add(c: &mut Criterion) {
    c.bench_function("series_index_add_1000", |b| {
        b.iter(|| {
            let mut idx = openGemini_engine::index::SeriesIndex::new();
            for i in 0..1000 {
                idx.add(i as u64);
            }
            black_box(idx);
        });
    });

    c.bench_function("series_index_add_10000", |b| {
        b.iter(|| {
            let mut idx = openGemini_engine::index::SeriesIndex::new();
            for i in 0..10000 {
                idx.add(i as u64);
            }
            black_box(idx);
        });
    });

    c.bench_function("series_index_add_100000", |b| {
        b.iter(|| {
            let mut idx = openGemini_engine::index::SeriesIndex::new();
            for i in 0..100000 {
                idx.add(i as u64);
            }
            black_box(idx);
        });
    });
}

fn bench_series_index_contains(c: &mut Criterion) {
    let mut index = openGemini_engine::index::SeriesIndex::new();
    for i in 0..10000 {
        index.add(i as u64);
    }
    
    c.bench_function("series_index_contains_10000", |b| {
        b.iter(|| {
            for i in 0..10000 {
                black_box(index.contains(i as u64));
            }
        });
    });
}

fn bench_series_index_range(c: &mut Criterion) {
    let mut index = openGemini_engine::index::SeriesIndex::new();
    for i in 0..100000 {
        index.add(i as u64);
    }
    
    c.bench_function("series_index_range_100000", |b| {
        b.iter(|| {
            black_box(index.range(25000, 75000));
        });
    });
}

fn bench_series_index_cardinality(c: &mut Criterion) {
    let mut index = openGemini_engine::index::SeriesIndex::new();
    for i in 0..100000 {
        index.add(i as u64);
    }
    
    c.bench_function("series_index_cardinality_100000", |b| {
        b.iter(|| {
            black_box(index.cardinality());
        });
    });
}

criterion_group! {
    name = benches;
    config = Criterion::default();
    targets = bench_series_index_add, bench_series_index_contains, bench_series_index_range, bench_series_index_cardinality
}
criterion_main!(benches);
