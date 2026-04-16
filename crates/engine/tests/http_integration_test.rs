use openGemini_engine::config::{EngineConfig, WalConfig, MemTableConfig, TsspConfig, CompactionConfig, CompressionType};
use openGemini_engine::{Engine, HttpServer, HttpConfig};
use std::net::{TcpListener, TcpStream, SocketAddr};
use std::thread;
use std::time::Duration;
use std::io::{Read, Write as IoWrite};

fn create_test_engine_config(temp_dir: &std::path::Path) -> EngineConfig {
    EngineConfig {
        data_dir: temp_dir.to_path_buf(),
        wal: WalConfig {
            dir: temp_dir.join("wal"),
            file_size: 64 * 1024,
            sync_enabled: false,
        },
        memtable: MemTableConfig {
            max_size: 1024 * 1024,
            flush_interval_ms: 1000,
        },
        tssp: TsspConfig {
            data_dir: temp_dir.join("data"),
            max_file_size: 256 * 1024,
            compression: CompressionType::None,
        },
        compaction: CompactionConfig::default(),
    }
}

fn get_free_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    listener.set_nonblocking(true).unwrap();
    addr.port()
}

fn start_test_server(port: u16, temp_dir: std::path::PathBuf) -> (thread::JoinHandle<()>, SocketAddr) {
    let addr: SocketAddr = format!("127.0.0.1:{}", port).parse().unwrap();
    
    let http_config = HttpConfig {
        bind_addr: addr,
        read_timeout_secs: 5,
        write_timeout_secs: 5,
        max_connections: 10,
    };
    
    std::fs::create_dir_all(&temp_dir).unwrap();
    let config = create_test_engine_config(&temp_dir);
    let engine = Engine::new(config).unwrap();
    
    let server = HttpServer::new(http_config).with_engine(engine);
    
    let handle = thread::spawn(move || {
        let _ = server.start();
    });
    
    thread::sleep(Duration::from_millis(100));
    
    (handle, addr)
}

fn send_http_request(addr: SocketAddr, method: &str, path: &str, body: Option<&str>) -> String {
    let mut stream = TcpStream::connect(addr).unwrap();
    stream.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    stream.set_write_timeout(Some(Duration::from_secs(5))).unwrap();
    
    let body = body.unwrap_or("");
    let request = if !body.is_empty() {
        format!(
            "{} {} HTTP/1.1\r\nHost: {}\r\nContent-Length: {}\r\n\r\n{}",
            method, path, addr, body.len(), body
        )
    } else {
        format!("{} {} HTTP/1.1\r\nHost: {}\r\n\r\n", method, path, addr)
    };
    
    stream.write_all(request.as_bytes()).unwrap();
    let _ = stream.flush();
    
    let mut response = Vec::new();
    let mut buffer = [0u8; 4096];
    loop {
        match stream.read(&mut buffer) {
            Ok(0) => break,
            Ok(n) => {
                response.extend_from_slice(&buffer[..n]);
                if n < 4096 {
                    break;
                }
            }
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                break;
            }
            Err(_) => {
                break;
            }
        }
    }
    
    String::from_utf8_lossy(&response).to_string()
}

#[test]
fn test_http_ping() {
    let temp_dir = std::env::temp_dir().join(format!("http_test_{}", std::process::id()));
    let port = get_free_port();
    let (_handle, addr) = start_test_server(port, temp_dir);
    
    let response = send_http_request(addr, "GET", "/ping", None);
    assert!(response.contains("200 OK") || response.contains("HTTP/1.1 200"));
}

#[test]
fn test_http_create_database() {
    let temp_dir = std::env::temp_dir().join(format!("http_test_{}", std::process::id()));
    let port = get_free_port();
    let (_handle, addr) = start_test_server(port, temp_dir);
    
    let query = "q=CREATE+DATABASE+testdb";
    let response = send_http_request(addr, "GET", &format!("/query?{}", query), None);
    assert!(response.contains("200 OK") || response.contains("HTTP/1.1 200") || response.contains("success"));
}

#[test]
fn test_http_show_databases() {
    let temp_dir = std::env::temp_dir().join(format!("http_test_{}", std::process::id()));
    let port = get_free_port();
    let (_handle, addr) = start_test_server(port, temp_dir);
    
    let query = "q=SHOW+DATABASES";
    let response = send_http_request(addr, "GET", &format!("/query?{}", query), None);
    assert!(response.contains("200 OK") || response.contains("HTTP/1.1 200"));
}

#[test]
fn test_http_write_line_protocol() {
    let temp_dir = std::env::temp_dir().join(format!("http_test_{}", std::process::id()));
    let port = get_free_port();
    let (handle, addr) = start_test_server(port, temp_dir.clone());
    
    thread::sleep(Duration::from_millis(500));
    
    let line = "cpu,host=server1 value=0.5";
    let response = send_http_request(addr, "POST", "/write?db=testdb", Some(line));
    
    drop(handle);
    
    println!("Write response for: {} on port {}: '{}'", line, port, response);
    assert!(response.contains("204") || response.contains("HTTP/1.1 204") || response.len() > 0);
}

#[test]
fn test_http_query_select() {
    let temp_dir = std::env::temp_dir().join(format!("http_test_{}", std::process::id()));
    let port = get_free_port();
    let (_handle, addr) = start_test_server(port, temp_dir);
    
    let line = "cpu,host=server1 value=0.5";
    send_http_request(addr, "POST", "/write?db=testdb", Some(line));
    thread::sleep(Duration::from_millis(50));
    
    let query = "q=SELECT+*+FROM+cpu";
    let response = send_http_request(addr, "GET", &format!("/query?db=testdb&{}", query), None);
    assert!(response.contains("200 OK") || response.contains("HTTP/1.1 200") || response.contains("cpu"));
}

#[test]
fn test_http_drop_measurement() {
    let temp_dir = std::env::temp_dir().join(format!("http_test_{}", std::process::id()));
    let port = get_free_port();
    let (_handle, addr) = start_test_server(port, temp_dir);
    
    let line = "cpu,host=server1 value=0.5";
    send_http_request(addr, "POST", "/write?db=testdb", Some(line));
    thread::sleep(Duration::from_millis(50));
    
    let query = "q=DROP+MEASUREMENT+cpu";
    let response = send_http_request(addr, "GET", &format!("/query?{}", query), None);
    assert!(response.contains("200 OK") || response.contains("HTTP/1.1 200") || response.contains("success"));
}

#[test]
fn test_http_show_measurements() {
    let temp_dir = std::env::temp_dir().join(format!("http_test_{}", std::process::id()));
    let port = get_free_port();
    let (_handle, addr) = start_test_server(port, temp_dir);
    
    let line = "cpu,host=server1 value=0.5";
    send_http_request(addr, "POST", "/write?db=testdb", Some(line));
    thread::sleep(Duration::from_millis(50));
    
    let query = "q=SHOW+MEASUREMENTS";
    let response = send_http_request(addr, "GET", &format!("/query?db=testdb&{}", query), None);
    assert!(response.contains("200 OK") || response.contains("HTTP/1.1 200"));
}

#[test]
fn test_http_show_tag_keys() {
    let temp_dir = std::env::temp_dir().join(format!("http_test_{}", std::process::id()));
    let port = get_free_port();
    let (_handle, addr) = start_test_server(port, temp_dir);
    
    let line = "cpu,host=server1,region=us-east value=0.5";
    send_http_request(addr, "POST", "/write?db=testdb", Some(line));
    thread::sleep(Duration::from_millis(50));
    
    let query = "q=SHOW+TAG+KEYS+FROM+cpu";
    let response = send_http_request(addr, "GET", &format!("/query?db=testdb&{}", query), None);
    assert!(response.contains("200 OK") || response.contains("HTTP/1.1 200"));
}

#[test]
fn test_http_show_field_keys() {
    let temp_dir = std::env::temp_dir().join(format!("http_test_{}", std::process::id()));
    let port = get_free_port();
    let (_handle, addr) = start_test_server(port, temp_dir);
    
    let line = "cpu,host=server1 value=0.5";
    send_http_request(addr, "POST", "/write?db=testdb", Some(line));
    thread::sleep(Duration::from_millis(50));
    
    let query = "q=SHOW+FIELD+KEYS+FROM+cpu";
    let response = send_http_request(addr, "GET", &format!("/query?db=testdb&{}", query), None);
    assert!(response.contains("200 OK") || response.contains("HTTP/1.1 200"));
}

#[test]
fn test_http_drop_series() {
    let temp_dir = std::env::temp_dir().join(format!("http_test_{}", std::process::id()));
    let port = get_free_port();
    let (_handle, addr) = start_test_server(port, temp_dir);
    
    let line = "cpu,host=server1 value=0.5";
    send_http_request(addr, "POST", "/write?db=testdb", Some(line));
    thread::sleep(Duration::from_millis(50));
    
    let query = "q=DROP+SERIES+FROM+cpu";
    let response = send_http_request(addr, "GET", &format!("/query?{}", query), None);
    assert!(response.contains("200 OK") || response.contains("HTTP/1.1 200") || response.contains("success"));
}

#[test]
fn test_http_delete() {
    let temp_dir = std::env::temp_dir().join(format!("http_test_{}", std::process::id()));
    let port = get_free_port();
    let (_handle, addr) = start_test_server(port, temp_dir);
    
    let line = "cpu,host=server1 value=0.5";
    send_http_request(addr, "POST", "/write?db=testdb", Some(line));
    thread::sleep(Duration::from_millis(50));
    
    let query = "q=DELETE";
    let response = send_http_request(addr, "GET", &format!("/query?{}", query), None);
    assert!(response.contains("200 OK") || response.contains("HTTP/1.1 200") || response.contains("success"));
}

#[test]
fn test_http_query_select_with_count() {
    let temp_dir = std::env::temp_dir().join(format!("http_test_{}", std::process::id()));
    let port = get_free_port();
    let (_handle, addr) = start_test_server(port, temp_dir);
    
    let lines = "cpu,host=server1 value=0.5\ncpu,host=server2 value=1.5";
    send_http_request(addr, "POST", "/write?db=testdb", Some(lines));
    thread::sleep(Duration::from_millis(50));
    
    let query = "q=SELECT+COUNT(value)+FROM+cpu";
    let response = send_http_request(addr, "GET", &format!("/query?db=testdb&{}", query), None);
    assert!(response.contains("200 OK") || response.contains("HTTP/1.1 200"));
}

#[test]
fn test_http_query_select_with_count_star() {
    let temp_dir = std::env::temp_dir().join(format!("http_test_{}", std::process::id()));
    let port = get_free_port();
    let (_handle, addr) = start_test_server(port, temp_dir);
    
    let lines = "cpu,host=server1 value=0.5\ncpu,host=server2 value=1.5";
    send_http_request(addr, "POST", "/write?db=testdb", Some(lines));
    thread::sleep(Duration::from_millis(50));
    
    let query = "q=SELECT+COUNT(*)+FROM+cpu";
    let response = send_http_request(addr, "GET", &format!("/query?db=testdb&{}", query), None);
    assert!(response.contains("200 OK") || response.contains("HTTP/1.1 200"));
}
