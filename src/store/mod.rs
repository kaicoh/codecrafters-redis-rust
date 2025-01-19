mod rdb;
mod value;

use super::{utils, Config, RedisError, RedisResult};
use rdb::Rdb;
use std::collections::HashMap;
use std::sync::{Mutex, MutexGuard};
use std::time::{Duration, SystemTime};
use value::RedisValue;

#[derive(Debug)]
pub struct Store(Mutex<Inner>);

#[derive(Debug, Clone)]
struct Inner {
    db: HashMap<String, RedisValue>,
    rdb: Rdb,
}

impl Store {
    pub fn new(config: Config) -> RedisResult<Self> {
        let rdb = Rdb::new(config)?;
        let inner = Inner {
            db: rdb.db().clone(),
            rdb,
        };
        Ok(Self(Mutex::new(inner)))
    }

    pub fn get(&self, key: &str) -> RedisResult<Option<String>> {
        let mut inner = self.lock()?;
        let value = match inner.db.get(key) {
            Some(v) => {
                if v.expired() {
                    inner.db.remove(key);
                    None
                } else {
                    Some(v.value.clone())
                }
            }
            _ => None,
        };
        Ok(value)
    }

    pub fn keys(&self) -> RedisResult<Vec<String>> {
        let inner = self.lock()?;
        let keys: Vec<String> = inner.db.keys().map(|v| v.to_string()).collect();
        Ok(keys)
    }

    pub fn set(&self, key: &str, value: String, exp: Option<u64>) -> RedisResult<()> {
        let mut inner = self.lock()?;
        let value = RedisValue {
            value,
            exp: exp.map(|n| SystemTime::now() + Duration::from_millis(n)),
        };
        inner.db.insert(key.into(), value);
        Ok(())
    }

    pub fn rdb_dir(&self) -> RedisResult<Option<String>> {
        let inner = self.lock()?;
        Ok(inner.rdb.dir().clone())
    }

    pub fn rdb_dbfilename(&self) -> RedisResult<Option<String>> {
        let inner = self.lock()?;
        Ok(inner.rdb.dbfilename().clone())
    }

    fn lock(&self) -> RedisResult<MutexGuard<'_, Inner>> {
        self.0
            .lock()
            .map_err(|err| RedisError::Lock(format!("{err}")))
    }
}
