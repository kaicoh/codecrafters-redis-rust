mod enc;
mod file;

use super::{utils, Config, RedisError, RedisResult, RedisValue};
use file::{RdbElement, RdbFile};
use std::collections::HashMap;
use std::fs::File;

#[derive(Debug, Clone, Default)]
pub struct Rdb {
    dir: Option<String>,
    dbfilename: Option<String>,
    db: HashMap<String, RedisValue>,
}

impl Rdb {
    pub fn new(config: Config) -> RedisResult<Self> {
        let Config { dir, dbfilename } = config;
        let mut rdb = Self {
            dir,
            dbfilename,
            ..Default::default()
        };

        if let (Some(dir), Some(dbfilename)) = (rdb.dir.as_ref(), rdb.dbfilename.as_ref()) {
            let path = format!("{dir}/{dbfilename}");
            let file = RdbFile::new(File::open(path)?);

            for el in file {
                if let RdbElement::HashTableEntry { key, value, exp } = el {
                    let value = RedisValue { value, exp };
                    rdb.db.insert(key, value);
                }
            }
        }

        Ok(rdb)
    }

    pub fn dir(&self) -> &Option<String> {
        &self.dir
    }

    pub fn dbfilename(&self) -> &Option<String> {
        &self.dbfilename
    }

    pub fn db(&self) -> &HashMap<String, RedisValue> {
        &self.db
    }
}
