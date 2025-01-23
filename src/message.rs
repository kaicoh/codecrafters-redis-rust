use super::{
    rdb::Rdb,
    utils::{self, Tokens},
    RedisResult, Resp,
};
use std::fmt;
use std::io::Write;
use std::net::TcpStream;

#[derive(Debug, Clone)]
pub enum IncomingMessage {
    Resp(Resp),
    Rdb(Rdb),
}

impl IncomingMessage {
    pub fn from_buffer(buf: &[u8]) -> RedisResult<Vec<Self>> {
        let mut tokens = Tokens::new(buf);
        let mut messages: Vec<Self> = vec![];

        while !tokens.finished() {
            let message = Self::from_tokens(&mut tokens)?;
            messages.push(message);
        }

        Ok(messages)
    }

    fn from_tokens(tokens: &mut Tokens<'_>) -> RedisResult<Self> {
        if tokens.starts_with(b"*") || tokens.starts_with(b"+") {
            // Incoming message can be a RESP Simple String when handshaking.
            // Except for that, it is always an RESP Array.
            let resp = Resp::from_tokens(tokens)?;
            Ok(Self::Resp(resp))
        } else if tokens.starts_with(b"$") {
            // Incoming message as RDB is like "$<size>\r\n<contents>".
            let size = tokens.next();
            eprintln!("Rdb size: {:?}", size.map(String::from_utf8_lossy));

            let size = size
                .map(|token| utils::parse_usize(&token[1..]))
                .transpose()?
                .ok_or(anyhow::anyhow!("Failed to parse RDB file size"))?;

            let contents = tokens
                .proceed(size)
                .ok_or(anyhow::anyhow!("Failed to get RDB file contents"))?;
            let rdb = Rdb::new(contents);

            Ok(Self::Rdb(rdb))
        } else {
            Err(anyhow::anyhow!("Neither RESP nor RDB parsed").into())
        }
    }
}

impl fmt::Display for IncomingMessage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Resp(resp) => write!(f, "{resp}"),
            Self::Rdb(_) => write!(f, "RDB binary data"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct OutgoingMessage(Vec<Vec<u8>>);

impl OutgoingMessage {
    pub fn new(bytes: Vec<Vec<u8>>) -> Self {
        Self(bytes)
    }

    pub fn empty() -> Self {
        Self(vec![])
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_parses_multiple_messages() {
        let rdb_prefix = b"$88\r\n".to_vec();
        let rdb = b"\x52\x45\x44\x49\x53\x30\x30\x31\x31\xfa\x09\x72\x65\x64\x69\x73\x2D\x76\x65\x72\x06\x36\x2E\x30\x2E\x31\x36\xfe\x00\xfb\x03\x02\x00\x06\x66\x6F\x6F\x62\x61\x72\x06\x62\x61\x7A\x71\x75\x78\xfc\x15\x72\xE7\x07\x8F\x01\x00\x00\x00\x03\x66\x6F\x6F\x03\x62\x61\x72\xfd\x52\xED\x2A\x66\x00\x03\x62\x61\x7A\x03\x71\x75\x78\xff\x89\x3b\xb7\x4e\xf8\x0f\x77\x19".to_vec();
        let resp_ss = b"+OK\r\n".to_vec();

        let bytes: Vec<u8> = rdb_prefix.into_iter().chain(rdb).chain(resp_ss).collect();

        let mut messages = IncomingMessage::from_buffer(&bytes).unwrap().into_iter();
        let message = messages.next().unwrap();
        assert!(matches!(message, IncomingMessage::Rdb(_)));

        let message = messages.next().unwrap();
        assert!(matches!(message, IncomingMessage::Resp(_)));
    }
}
