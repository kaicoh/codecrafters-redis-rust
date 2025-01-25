mod stream;
pub use stream::{RedisStream, StreamEntry};

use super::{RedisError, RedisResult};
use std::time::SystemTime;

#[derive(Debug, Clone)]
pub enum Value {
    String {
        value: String,
        exp: Option<SystemTime>,
    },
    Stream(RedisStream),
}

impl Value {
    pub fn expired(&self) -> bool {
        match self {
            Self::String { exp, .. } => {
                if let Some(&exp) = exp.as_ref() {
                    SystemTime::now() >= exp
                } else {
                    false
                }
            }
            _ => false,
        }
    }

    pub fn type_name(&self) -> &str {
        match self {
            Self::String { .. } => "string",
            Self::Stream(_) => "stream",
        }
    }
}

impl std::fmt::Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::String { value, .. } => {
                write!(f, "{value}")
            }
            Self::Stream(map) => {
                write!(f, "{map:?}")
            }
        }
    }
}
