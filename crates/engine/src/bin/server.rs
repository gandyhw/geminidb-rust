use openGemini_engine::{
    Engine, EngineConfig, WalConfig, MemTableConfig, TsspConfig, CompactionConfig,
    HttpServer, HttpConfig,
};
use std::path::PathBuf;

fn main() {
    tracing_subscriber::fmt::init();

    tracing::info!("Starting openGemini-rs server...");

    let data_dir = PathBuf::from("./data");
    let wal_dir = PathBuf::from("./wal");
    let tssp_dir = PathBuf::from("./tssp");

    std::fs::create_dir_all(&data_dir).expect("Failed to create data directory");
    std::fs::create_dir_all(&wal_dir).expect("Failed to create WAL directory");
    std::fs::create_dir_all(&tssp_dir).expect("Failed to create TSSP directory");

    let config = EngineConfig {
        data_dir,
        wal: WalConfig {
            dir: wal_dir,
            file_size: 64 * 1024 * 1024,
            sync_enabled: false,
        },
        memtable: MemTableConfig {
            max_size: 1024 * 1024 * 1024,
            flush_interval_ms: 1000,
        },
        tssp: TsspConfig {
            data_dir: tssp_dir,
            max_file_size: 256 * 1024 * 1024,
            compression: openGemini_engine::CompressionType::Lz4,
        },
        compaction: CompactionConfig::default(),
    };

    let engine = Engine::new(config).expect("Failed to create engine");

    let http_config = HttpConfig {
        bind_addr: "0.0.0.0:8086".parse().unwrap(),
        read_timeout_secs: 30,
        write_timeout_secs: 30,
        max_connections: 100,
        max_concurrent_write_limit: 100,
        max_concurrent_query_limit: 200,
        max_enqueued_write_limit: 50,
        max_enqueued_query_limit: 100,
        write_request_rate_limit: 0.0,
        query_request_rate_limit: 0.0,
        cors_enabled: true,
        auth_enabled: false,
        compression_enabled: true,
    };

    let server = HttpServer::new(http_config).with_engine(engine);

    tracing::info!("HTTP server listening on http://0.0.0.0:8086");
    tracing::info!("Press Ctrl+C to stop");

    server.start().expect("Failed to start HTTP server");

    loop {
        std::thread::sleep(std::time::Duration::from_secs(60));
    }
}
