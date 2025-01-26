use super::Resp;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum RedisError {
    #[error("Resp syntax error")]
    RespSyntax,

    #[error("Failed to parse into string: {0}")]
    ParseString(#[from] std::str::Utf8Error),

    #[error("Failed to parse into integer: {0}")]
    ParseInt(#[from] std::num::ParseIntError),

    #[error("Need {need} arguments, but got {got}")]
    LackOfArgs { need: usize, got: usize },

    #[error("Mutex lock error: {0}")]
    Lock(String),

    #[error("Unknown command received")]
    UnknownCommand,

    #[error("Unexpected encoding")]
    Encoding,

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("ERR The ID specified in XADD must be greater than 0-0")]
    InvalidStreamEntryId00,

    #[error("ERR The ID specified in XADD is equal or smaller than the target stream top item")]
    SmallerStreamEntryId,

    #[error("{0}")]
    Other(#[from] anyhow::Error),
}

impl From<RedisError> for Resp {
    fn from(error: RedisError) -> Self {
        Self::SE(format!("{error}"))
    }
}
