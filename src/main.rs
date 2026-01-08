use core::str;
use std::{collections::HashMap, io::{Read, Write}, net::TcpListener, sync::{Arc, Mutex}, thread, time::{SystemTime, UNIX_EPOCH}, u64};

struct KV {
    map: Mutex<HashMap<String, (String, u64)>>,
}

fn main() {
    println!("Start");

    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();
    let mut buffer = [0; 1024];
    let kv = Arc::new(KV { map: Mutex::new(HashMap::new()) });

    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
                let kv_ptr = Arc::clone(&kv);
                thread::spawn({ move ||
                    loop {
                        let i = stream.read(&mut buffer).unwrap_or_default();
                        let _ = stream.write(&get_resp(&buffer[..i], &kv_ptr).into_bytes());
                    }
                });
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}

fn get_resp(data: &[u8], kv: &Arc<KV>) -> String {
    let req = str::from_utf8(&data).unwrap_or_default();
    // println!("data recieved: {:?}", req);

    match req {
        "*1\r\n$4\r\nPING\r\n" => String::from("+PONG\r\n"),
        _ if is_array(req) => handle_array(req, kv),
        _ => String::from("ERR"),
    }
}

fn handle_array(req: &str, kv: &Arc<KV>) -> String {
    let elements = parse_bulk_string_array(req);
    if elements.len() < 2 {
        return String::from("ERR");
    }

    let keyword = elements[0].as_str();
    let remaining = elements[1..].to_vec();
    return match keyword {
        "ECHO" => handle_echo(remaining),
        "GET" => build_bulk_string(handle_get(kv, elements[1..].to_vec())),
        "SET" => handle_set(kv, elements[1..].to_vec()),
        _ => String::from("ERR"),
    };
}

fn handle_get(kv: &Arc<KV>, data: Vec<String>) -> String {
    let store = kv.map.lock().unwrap();

    let key = data[0].clone();
    let (value, expiry) = store.get(&key).unwrap_or(&(String::from("ERR"), 0)).clone();
    if value.eq("ERR") || expiry < SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64 {
        return String::from("ERR");
    }

    String::from(value)
}

fn handle_set(kv: &Arc<KV>, data: Vec<String>) -> String {
    let mut expiry = u64::MAX;
    if data.len() > 2 {
        expiry = data[3].parse().unwrap();
        if data[2].clone().to_lowercase() == "ex" {
            expiry *= 1000;
        }
        expiry += SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;
    }
    let mut store = kv.map.lock().unwrap();
    let (key, value) = (data[0].clone(), data[1].clone());
    store.insert(key, (value, expiry));

    String::from("+OK\r\n")
}

fn handle_echo(data: Vec<String>) -> String {
    let resp_str = data.join(" ");
    build_bulk_string(resp_str)
}

fn build_bulk_string(data: String) -> String {
    if data == String::from("ERR") {
        return String::from("$-1\r\n");
    }
    format!("${}\r\n{}\r\n", data.len(), data)
}

fn parse_bulk_string_array(data: &str) -> Vec<String> {
    let mut iter = data.chars();
    iter.next(); // *
    let num_elements = iter.by_ref().take_while(|n| n.is_ascii_digit()).collect::<String>().parse().unwrap();
    iter.next(); // CRLF

    let mut elements = Vec::new();
    for _ in 0..num_elements {
        iter.next(); // $
        let length = iter.by_ref().take_while(|n| n.is_ascii_digit()).collect::<String>().parse().unwrap();
        iter.next(); // CRLF

        let item: String = iter.by_ref().take(length).collect::<String>();
        elements.push(item);
        iter.next(); iter.next(); // CRLF
    }

    elements
}

fn is_array(req: &str) -> bool {
    return req.len() > 1 && req.chars().nth(1).unwrap().is_ascii_digit();
}
