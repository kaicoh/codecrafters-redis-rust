use super::{RedisResult, Resp};
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct RedisValue {
    pub(crate) value: String,
    pub(crate) exp: Option<SystemTime>,
}

impl RedisValue {
    pub(crate) fn expired(&self) -> bool {
        if let Some(&exp) = self.exp.as_ref() {
            SystemTime::now() >= exp
        } else {
            false
        }
    }

    pub(crate) fn to_resp(&self, key: &str) -> RedisResult<Resp> {
        if let Some(exp) = self.exp.as_ref() {
            let exp = exp
                .duration_since(UNIX_EPOCH)
                .map_err(anyhow::Error::new)?
                .as_millis();
            Ok(vec![
                "SET".into(),
                format!("{key}"),
                self.value.clone(),
                "px".into(),
                format!("{exp}"),
            ]
            .into())
        } else {
            Ok(vec!["SET".into(), format!("{key}"), self.value.clone()].into())
        }
    }
}
