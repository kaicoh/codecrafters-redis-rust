mod resp;

use resp::Resp;
use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();
    let store: Arc<Mutex<HashMap<String, String>>> = Arc::new(Mutex::new(HashMap::new()));

    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
                let store = Arc::clone(&store);

                thread::spawn(move || {
                    let mut buf = [0; 1024];

                    while stream.read(&mut buf).is_ok() {
                        let msg = match buf.iter().position(|v| *v == 0) {
                            Some(pos) => &buf[..pos],
                            None => &buf[..],
                        };

                        match Command::new(msg) {
                            Command::Ping => {
                                let pong = Resp::String("PONG".into()).serialize();
                                stream.write_all(&pong).unwrap();
                            }
                            Command::Echo(arg) => {
                                let val = Resp::String(arg).serialize();
                                stream.write_all(&val).unwrap();
                            }
                            Command::Get { key } => {
                                let val = store
                                    .lock()
                                    .unwrap()
                                    .get(&key)
                                    .map(|v| Resp::BulkString(v.into()))
                                    .unwrap_or(Resp::NullBulkString)
                                    .serialize();
                                stream.write_all(&val).unwrap();
                            }
                            Command::Set { key, value, exp } => {
                                {
                                    let mut store = store.lock().unwrap();
                                    store.insert(key.clone(), value);
                                }

                                if let Some(exp) = exp {
                                    let store = Arc::clone(&store);

                                    thread::spawn(move || {
                                        thread::sleep(Duration::from_millis(exp));
                                        let mut store = store.lock().unwrap();
                                        store.remove(&key);
                                    });
                                }

                                let val = Resp::String("OK".into()).serialize();
                                stream.write_all(&val).unwrap();
                            }
                            _ => {}
                        }

                        buf = [0; 1024];
                    }
                });
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}

#[derive(Debug)]
enum Command {
    Ping,
    Echo(String),
    Get {
        key: String,
    },
    Set {
        key: String,
        value: String,
        exp: Option<u64>,
    },
    Unknown,
}

impl Command {
    fn new(buf: &[u8]) -> Self {
        if buf.is_empty() {
            return Self::Unknown;
        }

        if let Resp::Array(elements) = Resp::parse(buf) {
            let mut elements = elements.into_iter();

            match elements.next() {
                Some(Resp::BulkString(cmd)) => match cmd.to_uppercase().as_str() {
                    "PING" => Self::Ping,
                    "ECHO" => bulk_string(&mut elements)
                        .map(Self::Echo)
                        .unwrap_or(Self::Unknown),
                    "GET" => bulk_string(&mut elements)
                        .map(|key| Self::Get { key })
                        .unwrap_or(Self::Unknown),
                    "SET" => {
                        let key = bulk_string(&mut elements);
                        let value = bulk_string(&mut elements);

                        let exp = if let Some(px) = bulk_string(&mut elements) {
                            if px.to_uppercase().as_str() == "PX" {
                                bulk_string(&mut elements).and_then(|v| v.parse::<u64>().ok())
                            } else {
                                None
                            }
                        } else {
                            None
                        };

                        if let (Some(key), Some(value)) = (key, value) {
                            Self::Set { key, value, exp }
                        } else {
                            Self::Unknown
                        }
                    }
                    _ => Self::Unknown,
                },
                _ => Self::Unknown,
            }
        } else {
            Self::Unknown
        }
    }
}

fn bulk_string(elements: &mut impl Iterator<Item = Resp>) -> Option<String> {
    if let Some(Resp::BulkString(val)) = elements.next() {
        Some(val)
    } else {
        None
    }
}
