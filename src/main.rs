use std::io::{Read, Write};
use std::net::TcpListener;

fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
                let mut buf = [0; 4];

                while let Ok(_) = stream.read(&mut buf) {
                    if buf == *b"PING" {
                        stream.write_all(b"+PONG\r\n").unwrap();
                    }
                    buf = [0; 4];
                }
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
