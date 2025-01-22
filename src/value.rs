use super::{RedisResult, Resp};
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Value {
    String {
        value: String,
        exp: Option<SystemTime>,
    },
}

impl Value {
    pub(crate) fn expired(&self) -> bool {
        match self {
            Self::String { exp, .. } => {
                if let Some(&exp) = exp.as_ref() {
                    SystemTime::now() >= exp
                } else {
                    false
                }
            }
        }
    }

    pub(crate) fn to_resp(&self, key: &str) -> RedisResult<Resp> {
        match self {
            Self::String { value, exp } => {
                if let Some(exp) = exp.as_ref() {
                    let exp = exp
                        .duration_since(UNIX_EPOCH)
                        .map_err(anyhow::Error::new)?
                        .as_millis();
                    Ok(vec![
                        "SET".into(),
                        format!("{key}"),
                        value.clone(),
                        "px".into(),
                        format!("{exp}"),
                    ]
                    .into())
                } else {
                    Ok(vec!["SET".into(), format!("{key}"), value.clone()].into())
                }
            }
        }
    }
}

impl std::fmt::Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::String { value, .. } => {
                write!(f, "{value}")
            }
        }
    }
}
