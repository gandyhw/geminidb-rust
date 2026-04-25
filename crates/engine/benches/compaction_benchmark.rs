use std::hint::black_box;
use criterion::{criterion_group, criterion_main, Criterion};
use openGemini_engine::config::CompactionConfig;
use openGemini_engine::compaction::{
    CompactionCandidate, CompactionManager, CompactionOptions, CompactionPriority, CompactionResult,
};
use openGemini_engine::tssp::FileMeta;
use tempfile::TempDir;

fn create_test_file_meta(file_id: u64, min_time: i64, max_time: i64, size: u64) -> FileMeta {
    FileMeta {
        file_id,
        min_time,
        max_time,
        size,
        bloom_filter_data: None,
    }
}

fn bench_compaction_needs_compaction(c: &mut Criterion) {
    let config = CompactionConfig::default();
    let manager = CompactionManager::new(config);

    c.bench_function("compaction_needs_compaction_3_files", |b| {
        b.iter(|| {
            let result = manager.needs_compaction(3);
            black_box(result);
        });
    });

    c.bench_function("compaction_needs_compaction_10_files", |b| {
        b.iter(|| {
            let result = manager.needs_compaction(10);
            black_box(result);
        });
    });

    c.bench_function("compaction_needs_compaction_disabled", |b| {
        let disabled_config = CompactionConfig {
            enabled: false,
            max_concurrent: 4,
            trigger_interval_ms: 300_000,
            max_file_age_hours: 24,
        };
        let disabled_manager = CompactionManager::new(disabled_config);
        b.iter(|| {
            let result = disabled_manager.needs_compaction(100);
            black_box(result);
        });
    });
}

fn bench_compaction_select_candidates(c: &mut Criterion) {
    let config = CompactionConfig::default();
    let manager = CompactionManager::new(config);

    let files: Vec<FileMeta> = (0..100)
        .map(|i| create_test_file_meta(i, i as i64 * 100, (i as i64 + 1) * 100, 1024 * 1024))
        .collect();

    c.bench_function("compaction_select_candidates_100_files", |b| {
        let options = CompactionOptions::default();
        b.iter(|| {
            let candidates = manager.select_compaction_candidates(&files, &options);
            black_box(candidates);
        });
    });

    c.bench_function("compaction_select_candidates_1000_files", |b| {
        let large_files: Vec<FileMeta> = (0..1000)
            .map(|i| create_test_file_meta(i, i as i64 * 100, (i as i64 + 1) * 100, 1024 * 1024))
            .collect();
        let options = CompactionOptions::default();
        b.iter(|| {
            let candidates = manager.select_compaction_candidates(&large_files, &options);
            black_box(candidates);
        });
    });
}

fn bench_compaction_priority_calculation(c: &mut Criterion) {
    let config = CompactionConfig::default();
    let manager = CompactionManager::new(config);

    c.bench_function("compaction_priority_low", |b| {
        b.iter(|| {
            let priority = manager.get_compaction_priority(3, 1024);
            black_box(priority);
        });
    });

    c.bench_function("compaction_priority_high", |b| {
        b.iter(|| {
            let priority = manager.get_compaction_priority(15, 2 * 1024 * 1024 * 1024);
            black_box(priority);
        });
    });

    c.bench_function("compaction_priority_critical", |b| {
        b.iter(|| {
            let priority = manager.get_compaction_priority(25, 1024);
            black_box(priority);
        });
    });
}

fn bench_compaction_start_end(c: &mut Criterion) {
    let config = CompactionConfig::default();

    c.bench_function("compaction_start_end_single", |b| {
        b.iter(|| {
            let mut manager = CompactionManager::new(config.clone());
            let started = manager.start_compaction();
            black_box(started);
            manager.end_compaction();
        });
    });

    c.bench_function("compaction_start_end_multiple", |b| {
        b.iter(|| {
            let mut manager = CompactionManager::new(config.clone());
            for _ in 0..4 {
                let _ = manager.start_compaction();
            }
            black_box(());
        });
    });
}

fn bench_candidate_overlap_ratio(c: &mut Criterion) {
    let candidate1 = CompactionCandidate::new(vec![
        create_test_file_meta(1, 100, 300, 1024),
        create_test_file_meta(2, 150, 350, 2048),
    ]);

    let candidate2 = CompactionCandidate::new(vec![
        create_test_file_meta(3, 200, 400, 3072),
        create_test_file_meta(4, 250, 450, 4096),
    ]);

    c.bench_function("candidate_overlap_ratio", |b| {
        b.iter(|| {
            let ratio = candidate1.overlap_ratio(&candidate2);
            black_box(ratio);
        });
    });
}

fn bench_result_compression_ratio(c: &mut Criterion) {
    let mut result = CompactionResult::new();
    result.original_files = vec![
        create_test_file_meta(1, 100, 200, 10 * 1024 * 1024),
        create_test_file_meta(2, 200, 300, 10 * 1024 * 1024),
    ];
    result.bytes_written = 8 * 1024 * 1024;

    c.bench_function("result_compression_ratio", |b| {
        b.iter(|| {
            let ratio = result.compression_ratio();
            black_box(ratio);
        });
    });

    c.bench_function("result_space_saved", |b| {
        b.iter(|| {
            let saved = result.space_saved();
            black_box(saved);
        });
    });
}

fn bench_compaction_level(c: &mut Criterion) {
    c.bench_function("compaction_level_from_file_count_0", |b| {
        b.iter(|| {
            let level = openGemini_engine::compaction::CompactionLevel::from_file_count(0);
            black_box(level);
        });
    });

    c.bench_function("compaction_level_from_file_count_5", |b| {
        b.iter(|| {
            let level = openGemini_engine::compaction::CompactionLevel::from_file_count(5);
            black_box(level);
        });
    });

    c.bench_function("compaction_level_from_file_count_15", |b| {
        b.iter(|| {
            let level = openGemini_engine::compaction::CompactionLevel::from_file_count(15);
            black_box(level);
        });
    });
}

fn bench_estimate_compaction_time(c: &mut Criterion) {
    let config = CompactionConfig::default();
    let manager = CompactionManager::new(config);

    c.bench_function("estimate_compaction_time_1mb", |b| {
        b.iter(|| {
            let duration = manager.estimate_compaction_time(1024 * 1024);
            black_box(duration);
        });
    });

    c.bench_function("estimate_compaction_time_100mb", |b| {
        b.iter(|| {
            let duration = manager.estimate_compaction_time(100 * 1024 * 1024);
            black_box(duration);
        });
    });

    c.bench_function("estimate_compaction_time_1gb", |b| {
        b.iter(|| {
            let duration = manager.estimate_compaction_time(1024 * 1024 * 1024);
            black_box(duration);
        });
    });
}

criterion_group!(
    benches,
    bench_compaction_needs_compaction,
    bench_compaction_select_candidates,
    bench_compaction_priority_calculation,
    bench_compaction_start_end,
    bench_candidate_overlap_ratio,
    bench_result_compression_ratio,
    bench_compaction_level,
    bench_estimate_compaction_time
);
criterion_main!(benches);