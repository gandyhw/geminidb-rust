use std::hint::black_box;
use criterion::{criterion_group, criterion_main, Criterion};
use openGemini_engine::shard::{ShardInfo, ShardManager, ShardMapper, ShardStatus};
use tempfile::TempDir;

fn bench_shard_mapper_map_shard(c: &mut Criterion) {
    let mapper = ShardMapper::new(3600, 1);

    c.bench_function("shard_mapper_map_shard_0", |b| {
        b.iter(|| {
            let shard_id = mapper.map_shard(0);
            black_box(shard_id);
        });
    });

    c.bench_function("shard_mapper_map_shard_1000", |b| {
        b.iter(|| {
            let shard_id = mapper.map_shard(3600000);
            black_box(shard_id);
        });
    });

    c.bench_function("shard_mapper_map_shard_negative", |b| {
        b.iter(|| {
            let shard_id = mapper.map_shard(-3600);
            black_box(shard_id);
        });
    });
}

fn bench_shard_mapper_calculate_key(c: &mut Criterion) {
    let mapper = ShardMapper::new(3600, 1);

    c.bench_function("shard_mapper_calculate_key", |b| {
        b.iter(|| {
            let key = mapper.calculate_shard_key("testdb", "rp1", 7200);
            black_box(key);
        });
    });
}

fn bench_shard_mapper_to_node(c: &mut Criterion) {
    let mapper = ShardMapper::new(3600, 1);
    let nodes = vec![
        "node1".to_string(),
        "node2".to_string(),
        "node3".to_string(),
    ];

    c.bench_function("shard_mapper_map_to_node", |b| {
        b.iter(|| {
            let node = mapper.map_shard_to_node(0, &nodes);
            black_box(node);
        });
    });

    c.bench_function("shard_mapper_map_to_node_multiple_shards", |b| {
        b.iter(|| {
            for shard_id in 0..100 {
                let node = mapper.map_shard_to_node(shard_id, &nodes);
                black_box(node);
            }
        });
    });
}

fn bench_shard_info_new(c: &mut Criterion) {
    let path = std::path::PathBuf::from("/data/test");

    c.bench_function("shard_info_new", |b| {
        b.iter(|| {
            let info = ShardInfo::new(1, "testdb", "rp1", path.clone());
            black_box(info);
        });
    });
}

fn bench_shard_info_is_active(c: &mut Criterion) {
    let path = std::path::PathBuf::from("/data/test");
    let info = ShardInfo::new(1, "testdb", "rp1", path);

    c.bench_function("shard_info_is_active", |b| {
        b.iter(|| {
            let result = info.is_active();
            black_box(result);
        });
    });
}

fn bench_shard_info_time_range(c: &mut Criterion) {
    let path = std::path::PathBuf::from("/data/test");
    let info = ShardInfo::new(1, "testdb", "rp1", path).with_shard_duration(3600);

    c.bench_function("shard_info_time_range", |b| {
        b.iter(|| {
            let range = info.time_range();
            black_box(range);
        });
    });
}

fn bench_shard_manager_create(c: &mut Criterion) {
    let temp_dir = TempDir::new().unwrap();

    c.bench_function("shard_manager_create_single", |b| {
        b.iter(|| {
            let manager = ShardManager::new(temp_dir.path().to_path_buf());
            let shard = manager.create_shard(1, "testdb", "rp1").unwrap();
            black_box(shard);
        });
    });

    c.bench_function("shard_manager_create_multiple", |b| {
        b.iter(|| {
            let manager = ShardManager::new(temp_dir.path().to_path_buf());
            for i in 0..10 {
                let _ = manager.create_shard(i, "testdb", "rp1");
            }
            black_box(());
        });
    });
}

fn bench_shard_manager_get(c: &mut Criterion) {
    let temp_dir = TempDir::new().unwrap();
    let manager = ShardManager::new(temp_dir.path().to_path_buf());

    for i in 0..100 {
        let _ = manager.create_shard(i, "testdb", "rp1");
    }

    c.bench_function("shard_manager_get_existing", |b| {
        b.iter(|| {
            let shard = manager.get_shard(50);
            black_box(shard);
        });
    });

    c.bench_function("shard_manager_get_nonexistent", |b| {
        b.iter(|| {
            let shard = manager.get_shard(999);
            black_box(shard);
        });
    });
}

fn bench_shard_manager_get_all(c: &mut Criterion) {
    let temp_dir = TempDir::new().unwrap();
    let manager = ShardManager::new(temp_dir.path().to_path_buf());

    for i in 0..100 {
        let _ = manager.create_shard(i, "testdb", "rp1");
    }

    c.bench_function("shard_manager_get_all_100", |b| {
        b.iter(|| {
            let shards = manager.get_all_shards();
            black_box(shards);
        });
    });
}

fn bench_shard_manager_shard_count(c: &mut Criterion) {
    let temp_dir = TempDir::new().unwrap();
    let manager = ShardManager::new(temp_dir.path().to_path_buf());

    for i in 0..50 {
        let _ = manager.create_shard(i, "testdb", "rp1");
    }

    c.bench_function("shard_manager_shard_count", |b| {
        b.iter(|| {
            let count = manager.shard_count();
            black_box(count);
        });
    });
}

fn bench_shard_manager_update_status(c: &mut Criterion) {
    let temp_dir = TempDir::new().unwrap();
    let manager = ShardManager::new(temp_dir.path().to_path_buf());
    let _ = manager.create_shard(1, "testdb", "rp1");

    c.bench_function("shard_manager_update_status", |b| {
        b.iter(|| {
            let result = manager.update_shard_status(1, ShardStatus::ReadOnly);
            black_box(result);
        });
    });
}

fn bench_shard_manager_get_shards_by_db(c: &mut Criterion) {
    let temp_dir = TempDir::new().unwrap();
    let manager = ShardManager::new(temp_dir.path().to_path_buf());

    for i in 0..30 {
        let _ = manager.create_shard(i, "testdb1", "rp1");
        let _ = manager.create_shard(100 + i, "testdb2", "rp1");
    }

    c.bench_function("shard_manager_get_shards_by_db", |b| {
        b.iter(|| {
            let shards = manager.get_shards_by_db("testdb1");
            black_box(shards);
        });
    });
}

fn bench_shard_manager_stats(c: &mut Criterion) {
    let temp_dir = TempDir::new().unwrap();
    let manager = ShardManager::new(temp_dir.path().to_path_buf());

    for i in 0..20 {
        let _ = manager.create_shard(i, "testdb", "rp1");
        let _ = manager.update_shard_metadata(i, (i + 1) as u64 * 1024, (i + 1) as u64 * 100);
    }

    c.bench_function("shard_manager_get_stats", |b| {
        b.iter(|| {
            let stats = manager.get_shard_stats();
            black_box(stats);
        });
    });

    c.bench_function("shard_manager_average_shard_size", |b| {
        let stats = manager.get_shard_stats();
        b.iter(|| {
            let avg = stats.average_shard_size();
            black_box(avg);
        });
    });
}

fn bench_shard_mapper_with_virtual_nodes(c: &mut Criterion) {
    c.bench_function("shard_mapper_with_virtual_nodes_150", |b| {
        b.iter(|| {
            let mapper = ShardMapper::new(3600, 1).with_virtual_nodes(150);
            black_box(mapper);
        });
    });

    c.bench_function("shard_mapper_with_virtual_nodes_500", |b| {
        b.iter(|| {
            let mapper = ShardMapper::new(3600, 1).with_virtual_nodes(500);
            black_box(mapper);
        });
    });
}

criterion_group!(
    benches,
    bench_shard_mapper_map_shard,
    bench_shard_mapper_calculate_key,
    bench_shard_mapper_to_node,
    bench_shard_info_new,
    bench_shard_info_is_active,
    bench_shard_info_time_range,
    bench_shard_manager_create,
    bench_shard_manager_get,
    bench_shard_manager_get_all,
    bench_shard_manager_shard_count,
    bench_shard_manager_update_status,
    bench_shard_manager_get_shards_by_db,
    bench_shard_manager_stats,
    bench_shard_mapper_with_virtual_nodes
);
criterion_main!(benches);