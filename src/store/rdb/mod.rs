mod enc;
mod file;

use super::{utils, Config, RedisError, RedisResult, RedisValue};
use file::{RdbElement, RdbFile};
use std::collections::HashMap;
use std::fs::File;
use std::io::ErrorKind;

#[derive(Debug, Clone, Default)]
pub struct Rdb(HashMap<String, RedisValue>);

impl Rdb {
    pub fn new(config: &Config) -> RedisResult<Self> {
        let Config {
            dir, dbfilename, ..
        } = config;
        let mut rdb: HashMap<String, RedisValue> = HashMap::new();

        if let (Some(dir), Some(dbfilename)) = (dir, dbfilename) {
            let path = format!("{dir}/{dbfilename}");

            match File::open(path) {
                Ok(f) => {
                    for el in RdbFile::new(f) {
                        if let RdbElement::HashTableEntry { key, value, exp } = el {
                            let value = RedisValue { value, exp };
                            rdb.insert(key, value);
                        }
                    }
                }
                Err(err) if err.kind() == ErrorKind::NotFound => {
                    eprintln!("Not found rdb file");
                    return Ok(Self(rdb));
                }
                Err(err) => {
                    return Err(RedisError::from(err));
                }
            }
        }

        Ok(Self(rdb))
    }

    pub fn db(&self) -> &HashMap<String, RedisValue> {
        &self.0
    }
}
