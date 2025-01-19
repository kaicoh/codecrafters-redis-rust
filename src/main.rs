use redis_starter_rust as rss;
use rss::{Command, Config, Resp, Store};
use std::env;
use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::Arc;
use std::thread;

fn main() {
    let args: Vec<String> = env::args().collect();
    let config = Config::new(args);

    let listener = TcpListener::bind(config.socket_addr()).unwrap();

    let store = Arc::new(
        Store::new(config)
            .inspect_err(|err| eprintln!("Failed to instantiate store: {err}"))
            .unwrap(),
    );

    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
                let store = Arc::clone(&store);

                thread::spawn(move || {
                    let mut buf = [0; 1024];

                    while stream.read(&mut buf).is_ok() {
                        let msg = trim_trailing_zero(&buf);

                        if !msg.is_empty() {
                            let res = match Command::new(msg) {
                                Ok(cmd) => cmd.run(Arc::clone(&store)).unwrap_or_else(|err| {
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
