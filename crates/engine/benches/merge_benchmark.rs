use std::hint::black_box;
use criterion::{criterion_group, criterion_main, Criterion};
use openGemini_engine::config::{CompactionConfig, TsspConfig, CompressionType};
use openGemini_engine::merge::{MergeCandidate, MergeScheduler, MergeTool};
use openGemini_engine::tssp::FileMeta;
use std::path::PathBuf;

fn create_test_file_meta(file_id: u64, min_time: i64, max_time: i64, size: u64) -> FileMeta {
    FileMeta {
        file_id,
        min_time,
        max_time,
        size,
        bloom_filter_data: None,
    }
}

fn bench_merge_candidate_is_valid(c: &mut Criterion) {
    let files = vec![
        create_test_file_meta(1, 100, 200, 1024),
        create_test_file_meta(2, 200, 300, 2048),
    ];
    let candidate = MergeCandidate::new(files, PathBuf::from("/data"));

    c.bench_function("merge_candidate_is_valid_true", |b| {
        b.iter(|| {
            let result = candidate.is_valid();
            black_box(result);
        });
    });
}

fn bench_merge_candidate_is_valid_false(c: &mut Criterion) {
    let files = vec![create_test_file_meta(1, 100, 200, 1024)];
    let candidate = MergeCandidate::new(files, PathBuf::from("/data"));

    c.bench_function("merge_candidate_is_valid_false", |b| {
        b.iter(|| {
            let result = candidate.is_valid();
            black_box(result);
        });
    });
}

fn bench_merge_candidate_total_size(c: &mut Criterion) {
    let files = vec![
        create_test_file_meta(1, 100, 200, 1024),
        create_test_file_meta(2, 200, 300, 2048),
        create_test_file_meta(3, 300, 400, 4096),
    ];
    let candidate = MergeCandidate::new(files, PathBuf::from("/data"));

    c.bench_function("merge_candidate_total_size", |b| {
        b.iter(|| {
            let size = candidate.total_size();
            black_box(size);
        });
    });
}

fn bench_merge_candidate_time_range(c: &mut Criterion) {
    let files = vec![
        create_test_file_meta(1, 100, 200, 1024),
        create_test_file_meta(2, 50, 300, 2048),
        create_test_file_meta(3, 150, 400, 4096),
    ];
    let candidate = MergeCandidate::new(files, PathBuf::from("/data"));

    c.bench_function("merge_candidate_time_range", |b| {
        b.iter(|| {
            let range = candidate.time_range();
            black_box(range);
        });
    });
}

fn bench_merge_scheduler_can_start(c: &mut Criterion) {
    let config = CompactionConfig::default();
    let scheduler = MergeScheduler::new(&config);

    c.bench_function("merge_scheduler_can_start_true", |b| {
        b.iter(|| {
            let result = scheduler.can_start_merge();
            black_box(result);
        });
    });
}

fn bench_merge_scheduler_start_complete(c: &mut Criterion) {
    let config = CompactionConfig::default();

    c.bench_function("merge_scheduler_start_complete", |b| {
        b.iter(|| {
            let mut scheduler = MergeScheduler::new(&config);
            scheduler.start_merge(1);
            scheduler.complete_merge(1);
        });
    });

    c.bench_function("merge_scheduler_start_multiple", |b| {
        b.iter(|| {
            let mut scheduler = MergeScheduler::new(&config);
            for i in 0..10 {
                let _ = scheduler.start_merge(i);
            }
            black_box(());
        });
    });
}

fn bench_merge_scheduler_in_progress_count(c: &mut Criterion) {
    let config = CompactionConfig::default();

    c.bench_function("merge_scheduler_in_progress_count", |b| {
        b.iter(|| {
            let mut scheduler = MergeScheduler::new(&config);
            for i in 0..4 {
                let _ = scheduler.start_merge(i);
            }
            let count = scheduler.in_progress_count();
            black_box(count);
        });
    });
}

fn bench_merge_scheduler_cancel(c: &mut Criterion) {
    let config = CompactionConfig::default();

    c.bench_function("merge_scheduler_cancel", |b| {
        b.iter(|| {
            let mut scheduler = MergeScheduler::new(&config);
            scheduler.start_merge(1);
            scheduler.cancel_merge(1);
        });
    });
}

fn bench_merge_tool_select_candidates(c: &mut Criterion) {
    let tssp_config = TsspConfig {
        data_dir: PathBuf::from("/data"),
        max_file_size: 256 * 1024 * 1024,
        compression: CompressionType::Snappy,
    };
    let tool = MergeTool::new(tssp_config);

    let files: Vec<FileMeta> = (0..50)
        .map(|i| create_test_file_meta(i, i as i64 * 100, (i as i64 + 1) * 100, 1024 * 1024))
        .collect();

    c.bench_function("merge_tool_select_candidates_50_files", |b| {
        b.iter(|| {
            let candidates = tool.select_merge_candidates(&files, 10);
            black_box(candidates);
        });
    });

    c.bench_function("merge_tool_select_candidates_100_files", |b| {
        let large_files: Vec<FileMeta> = (0..100)
            .map(|i| create_test_file_meta(i, i as i64 * 100, (i as i64 + 1) * 100, 1024 * 1024))
            .collect();
        b.iter(|| {
            let candidates = tool.select_merge_candidates(&large_files, 10);
            black_box(candidates);
        });
    });
}

fn bench_merge_scheduler_max_concurrent_limit(c: &mut Criterion) {
    let config = CompactionConfig {
        enabled: true,
        max_concurrent: 4,
        trigger_interval_ms: 300_000,
        max_file_age_hours: 24,
    };

    c.bench_function("merge_scheduler_max_concurrent_blocking", |b| {
        b.iter(|| {
            let mut scheduler = MergeScheduler::new(&config);
            for i in 0..4 {
                let _ = scheduler.start_merge(i);
            }
            let blocked = scheduler.can_start_merge();
            black_box(blocked);
        });
    });
}

fn bench_merge_scheduler_get_status(c: &mut Criterion) {
    let config = CompactionConfig::default();

    c.bench_function("merge_scheduler_get_status_hit", |b| {
        b.iter(|| {
            let mut scheduler = MergeScheduler::new(&config);
            scheduler.start_merge(42);
            let status = scheduler.get_merge_status(42);
            black_box(status);
        });
    });

    c.bench_function("merge_scheduler_get_status_miss", |b| {
        let scheduler = MergeScheduler::new(&config);
        b.iter(|| {
            let status = scheduler.get_merge_status(999);
            black_box(status);
        });
    });
}

criterion_group!(
    benches,
    bench_merge_candidate_is_valid,
    bench_merge_candidate_is_valid_false,
    bench_merge_candidate_total_size,
    bench_merge_candidate_time_range,
    bench_merge_scheduler_can_start,
    bench_merge_scheduler_start_complete,
    bench_merge_scheduler_in_progress_count,
    bench_merge_scheduler_cancel,
    bench_merge_tool_select_candidates,
    bench_merge_scheduler_max_concurrent_limit,
    bench_merge_scheduler_get_status
);
criterion_main!(benches);