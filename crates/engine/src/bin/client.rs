use std::io::Write;
use std::net::TcpStream;
use std::env;

fn main() {
    let args: Vec<String> = env::args().collect();
    
    if args.len() < 2 {
        println!("Usage: client <command> [args]");
        println!("Commands:");
        println!("  ping                    - Check server status");
        println!("  write <line>            - Write data using line protocol");
        println!("  query <sql>             - Execute InfluxQL query");
        println!("  create_db <name>        - Create database");
        println!("  show_dbs                - Show all databases");
        println!("  show_measurements        - Show all measurements");
        println!("  show_series              - Show all series");
        println!("  show_tag_keys <meas>    - Show tag keys");
        println!("  show_tag_values <meas>  - Show tag values");
        println!("  show_field_keys <meas>  - Show field keys");
        return;
    }

    let command = &args[1];
    let server_addr = "127.0.0.1:8086";

    match command.as_str() {
        "ping" => {
            let response = send_request("GET /ping HTTP/1.1\r\nHost: localhost:8086\r\n\r\n", server_addr);
            println!("{}", response);
        }
        "write" => {
            if args.len() < 3 {
                println!("Usage: client write <line_protocol>");
                return;
            }
            let line = &args[2];
            let request = format!(
                "POST /write?db=_internal HTTP/1.1\r\nHost: localhost:8086\r\nContent-Length: {}\r\n\r\n{}",
                line.len(),
                line
            );
            let response = send_request(&request, server_addr);
            println!("{}", response);
        }
        "query" => {
            if args.len() < 3 {
                println!("Usage: client query <sql>");
                return;
            }
            let sql = &args[2];
            let encoded_sql = url_encode(sql);
            let request = format!(
                "GET /query?q={}&db=_internal HTTP/1.1\r\nHost: localhost:8086\r\n\r\n",
                encoded_sql
            );
            let response = send_request(&request, server_addr);
            println!("{}", response);
        }
        "create_db" => {
            if args.len() < 3 {
                println!("Usage: client create_db <name>");
                return;
            }
            let db_name = &args[2];
            let sql = format!("CREATE DATABASE {}", db_name);
            let encoded_sql = url_encode(&sql);
            let request = format!(
                "GET /query?q={}&db=_internal HTTP/1.1\r\nHost: localhost:8086\r\n\r\n",
                encoded_sql
            );
            let response = send_request(&request, server_addr);
            println!("{}", response);
        }
        "show_dbs" => {
            let request = "GET /query?q=SHOW%20DATABASES&db=_internal HTTP/1.1\r\nHost: localhost:8086\r\n\r\n".to_string();
            let response = send_request(&request, server_addr);
            println!("{}", response);
        }
        "show_measurements" => {
            let request = "GET /query?q=SHOW%20MEASUREMENTS&db=_internal HTTP/1.1\r\nHost: localhost:8086\r\n\r\n".to_string();
            let response = send_request(&request, server_addr);
            println!("{}", response);
        }
        "show_series" => {
            let request = "GET /query?q=SHOW%20SERIES&db=_internal HTTP/1.1\r\nHost: localhost:8086\r\n\r\n".to_string();
            let response = send_request(&request, server_addr);
            println!("{}", response);
        }
        "show_tag_keys" => {
            let measurement = if args.len() >= 3 { &args[2] } else { "" };
            let sql = format!("SHOW TAG KEYS FROM {}", measurement);
            let encoded_sql = url_encode(&sql);
            let request = format!(
                "GET /query?q={}&db=_internal HTTP/1.1\r\nHost: localhost:8086\r\n\r\n",
                encoded_sql
            );
            let response = send_request(&request, server_addr);
            println!("{}", response);
        }
        "show_tag_values" => {
            let measurement = if args.len() >= 3 { &args[2] } else { "" };
            let sql = format!("SHOW TAG VALUES FROM {}", measurement);
            let encoded_sql = url_encode(&sql);
            let request = format!(
                "GET /query?q={}&db=_internal HTTP/1.1\r\nHost: localhost:8086\r\n\r\n",
                encoded_sql
            );
            let response = send_request(&request, server_addr);
            println!("{}", response);
        }
        "show_field_keys" => {
            let measurement = if args.len() >= 3 { &args[2] } else { "" };
            let sql = format!("SHOW FIELD KEYS FROM {}", measurement);
            let encoded_sql = url_encode(&sql);
            let request = format!(
                "GET /query?q={}&db=_internal HTTP/1.1\r\nHost: localhost:8086\r\n\r\n",
                encoded_sql
            );
            let response = send_request(&request, server_addr);
            println!("{}", response);
        }
        _ => {
            println!("Unknown command: {}", command);
        }
    }
}

fn send_request(request: &str, server_addr: &str) -> String {
    let mut stream = match TcpStream::connect(server_addr) {
        Ok(s) => s,
        Err(e) => {
            return format!("Failed to connect to server: {}", e);
        }
    };

    if let Err(e) = stream.write_all(request.as_bytes()) {
        return format!("Failed to send request: {}", e);
    }

    let mut buffer = Vec::new();
    match std::io::Read::read_to_end(&mut stream, &mut buffer) {
        Ok(_) => {}
        Err(e) => {
            return format!("Failed to read response: {}", e);
        }
    }

    String::from_utf8_lossy(&buffer).to_string()
}

fn url_encode(s: &str) -> String {
    let mut result = String::new();
    for c in s.chars() {
        match c {
            'a'..='z' | 'A'..='Z' | '0'..='9' | '-' | '_' | '.' | '~' => {
                result.push(c);
            }
            ' ' => {
                result.push_str("%20");
            }
            _ => {
                result.push_str(&format!("%{:02X}", c as u8));
            }
        }
    }
    result
}
