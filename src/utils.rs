use super::{RedisError, RedisResult};

pub(crate) fn stringify(buf: &[u8]) -> RedisResult<&str> {
    std::str::from_utf8(buf).map_err(RedisError::from)
}

pub(crate) fn parse_usize(buf: &[u8]) -> RedisResult<usize> {
    stringify(buf)?.parse().map_err(RedisError::from)
}
