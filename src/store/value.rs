use std::time::SystemTime;

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
}
