use super::{
    rdb::Rdb,
    utils::{self, Tokens},
    RedisResult, Resp,
};
use std::io::Write;
use std::net::TcpStream;

#[derive(Debug, Clone)]
pub enum IncomingMessage {
    Resp(Vec<Resp>),
    Rdb(Rdb),
    Unknown(Vec<u8>),
}

impl IncomingMessage {
    pub fn new(buf: &[u8]) -> RedisResult<Self> {
        if buf.starts_with(b"*") {
            // Incoming message as RESP is always an array.
            let mut inner: Vec<Resp> = vec![];
            let mut tokens = Tokens::new(buf);

            while !tokens.finished() {
                let resp = Resp::from_tokens(&mut tokens)?;
                inner.push(resp);
            }

            Ok(Self::Resp(inner))
        } else if buf.starts_with(b"$") {
            // Incoming message as RDB is like "$<size>\r\n<contents>".
            let mut tokens = Tokens::new(buf);

            let size = tokens
                .next()
                .map(|token| utils::parse_usize(&token[1..]))
                .transpose()?
                .ok_or(anyhow::anyhow!("Failed to parse RDB file size"))?;
            let contents = tokens
                .next()
                .ok_or(anyhow::anyhow!("Failed to get RDB file contents"))?;
            let rdb = Rdb::new(&contents[..size]);

            Ok(Self::Rdb(rdb))
        } else {
            Ok(Self::Unknown(buf.to_vec()))
        }
    }
}

#[derive(Debug, Clone)]
pub struct OutgoingMessage(Vec<Vec<u8>>);

impl OutgoingMessage {
    pub fn new(bytes: Vec<Vec<u8>>) -> Self {
        Self(bytes)
    }

    pub fn write_to(self, stream: &mut TcpStream) -> std::io::Result<()> {
        for bytes in self.into_iter() {
            stream.write_all(&bytes)?;
        }
        Ok(())
    }
}

impl From<Resp> for OutgoingMessage {
    fn from(resp: Resp) -> Self {
        Self(vec![resp.serialize()])
    }
}

impl From<Vec<Resp>> for OutgoingMessage {
    fn from(resps: Vec<Resp>) -> Self {
        Self(resps.iter().map(Resp::serialize).collect())
    }
}

impl From<Vec<u8>> for OutgoingMessage {
    fn from(bytes: Vec<u8>) -> Self {
        Self(vec![bytes])
    }
}

impl<'a> From<&'a [u8]> for OutgoingMessage {
    fn from(buf: &'a [u8]) -> Self {
        Self(vec![buf.to_vec()])
    }
}

impl IntoIterator for OutgoingMessage {
    type Item = Vec<u8>;
    type IntoIter = std::vec::IntoIter<Vec<u8>>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}
