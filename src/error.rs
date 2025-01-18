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
}

impl From<RedisError> for Resp {
    fn from(error: RedisError) -> Self {
        Self::SE(format!("{error}"))
    }
}
