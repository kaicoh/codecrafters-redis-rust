use super::{Message, RedisResult};
use std::io::Write;
use std::net::TcpStream;

#[derive(Debug)]
pub struct Replica {
    stream: TcpStream,
}

impl Replica {
    pub fn new(stream: &TcpStream) -> RedisResult<Self> {
        Ok(Self {
            stream: stream.try_clone()?,
        })
    }

    pub fn send(&mut self, msg: Message) -> RedisResult<()> {
        self.stream.write_all(msg.as_bytes())?;
        Ok(())
    }
}
