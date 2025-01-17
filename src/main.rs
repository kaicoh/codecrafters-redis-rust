use std::io::{Cursor, Read, Seek, SeekFrom, Write};
use std::net::TcpListener;
use std::thread;

const TERM: &[u8] = b"\r\n";

fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
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
                    "ECHO" => {
                        if let Some(Resp::BulkString(val)) = elements.next() {
                            Self::Echo(val)
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

#[derive(Debug, Clone, PartialEq)]
enum Resp {
    String(String),
    BulkString(String),
    Array(Vec<Resp>),
}

impl Resp {
    fn new(tokens: &mut RespToken<'_>) -> Self {
        let token = tokens.next().unwrap();

        if token.starts_with(b"+") {
            let val = std::str::from_utf8(&token[1..]).unwrap();
            Self::String(val.into())
        } else if token.starts_with(b"$") {
            let len: usize = std::str::from_utf8(&token[1..]).unwrap().parse().unwrap();
            let val_token = tokens.next().unwrap();
            let val = std::str::from_utf8(&val_token[..len]).unwrap();
            Self::BulkString(val.into())
        } else if token.starts_with(b"*") {
            let len: usize = std::str::from_utf8(&token[1..]).unwrap().parse().unwrap();
            let mut inner: Vec<Self> = vec![];

            for _ in 0..len {
                let element = Self::new(tokens);
                inner.push(element);
            }

            Self::Array(inner)
        } else {
            unimplemented!()
        }
    }

    fn parse(buf: &[u8]) -> Self {
        let mut tokens = RespToken::new(buf);
        Self::new(&mut tokens)
    }

    fn serialize(self) -> Vec<u8> {
        match self {
            Self::String(val) => format!("+{val}\r\n").into_bytes(),
            Self::BulkString(val) => format!("${}\r\n{val}\r\n", val.len()).into_bytes(),
            Self::Array(vals) => {
                let len = vals.len();
                let elements = vals.into_iter().flat_map(Self::serialize);
                format!("*{len}\r\n")
                    .into_bytes()
                    .into_iter()
                    .chain(elements)
                    .collect()
            }
        }
    }
}

#[derive(Debug)]
struct RespToken<'a> {
    cursor: Cursor<&'a [u8]>,
}

impl<'a> RespToken<'a> {
    fn new(buf: &'a [u8]) -> Self {
        Self {
            cursor: Cursor::new(buf),
        }
    }
}

impl<'a> Iterator for RespToken<'a> {
    type Item = &'a [u8];

    fn next(&mut self) -> Option<Self::Item> {
        let current_pos = self.cursor.position() as usize;
        let bytes = *(self.cursor.get_ref());

        let (_, rest) = bytes.split_at_checked(current_pos)?;

        if rest.is_empty() {
            return None;
        }

        let msg_size = rest.windows(2).position(|chars| chars == TERM)?;

        let next_pos: i64 = (msg_size + 2)
            .try_into()
            .inspect_err(|e| eprintln!("Error! parsing new position: {e}"))
            .ok()?;

        self.cursor
            .seek(SeekFrom::Current(next_pos))
            .inspect_err(|e| eprintln!("Error! seeking next position: {e}"))
            .ok()?;

        Some(&bytes[current_pos..(current_pos + msg_size)])
    }
}
