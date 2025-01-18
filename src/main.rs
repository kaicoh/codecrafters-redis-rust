use redis_starter_rust as rss;
use rss::{Command, Resp, Store};
use std::io::{Read, Write};
use std::net::TcpListener;
use std::thread;

fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();
    let store = Store::new();

    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
                let store = store.clone();

                thread::spawn(move || {
                    let mut buf = [0; 1024];

                    while stream.read(&mut buf).is_ok() {
                        let msg = trim_trailing_zero(&buf);

                        if !msg.is_empty() {
                            let res = match Command::new(msg) {
                                Ok(cmd) => cmd.run(store.clone()).unwrap_or_else(|err| {
                                    eprintln!("Failed to run command: {err}");
                                    Resp::from(err)
                                }),
                                Err(err) => {
                                    eprintln!("Failed to parse command: {err}");
                                    Resp::from(err)
                                }
                            };

                            if let Err(err) = stream.write_all(&res.serialize()) {
                                eprintln!("Failed to write TCP stream: {err}");
                            }
                        }

                        buf = [0; 1024];
                    }
                });
            }
            Err(err) => {
                eprintln!("Failed to get TCP stream: {err}");
            }
        }
    }
}

fn trim_trailing_zero(buf: &[u8]) -> &[u8] {
    match buf.iter().position(|&v| v == 0) {
        Some(pos) => &buf[..pos],
        None => buf,
    }
}
