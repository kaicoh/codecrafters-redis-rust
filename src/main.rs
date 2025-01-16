use std::io::{Read, Write};
use std::net::TcpListener;

fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
                let mut buf: String = String::new();
                stream.read_to_string(&mut buf).unwrap();

                for _ in 0..count_ping(buf) {
                    stream.write(b"+PONG\r\n").unwrap();
                }

                stream.flush().unwrap();
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}

fn count_ping(buf: String) -> usize {
    let mut count: usize = 0;

    for word in buf.split('\n') {
        if word == "PING" {
            count += 1;
        }
    }

    count
}
