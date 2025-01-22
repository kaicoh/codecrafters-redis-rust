mod enc;
mod file;

use super::{utils, value::Value, Config, RedisError, RedisResult};
use file::{RdbElement, RdbFile};
use std::collections::HashMap;
use std::fs::File;
use std::io::{ErrorKind, Read};

#[derive(Debug, Clone, Default)]
pub struct Rdb(HashMap<String, Value>);

impl Rdb {
    pub(crate) fn new<R: Read>(r: R) -> Self {
        let mut rdb: HashMap<String, Value> = HashMap::new();

        for el in RdbFile::new(r) {
            if let RdbElement::HashTableEntry { key, value, exp } = el {
                let value = Value::String { value, exp };
                rdb.insert(key, value);
            }
        }
        Self(rdb)
    }

    pub(crate) fn from_conf(config: &Config) -> RedisResult<Self> {
        let Config {
            dir, dbfilename, ..
        } = config;

        if let (Some(dir), Some(dbfilename)) = (dir, dbfilename) {
            let path = format!("{dir}/{dbfilename}");

            match File::open(path) {
                Ok(f) => Ok(Self::new(f)),
                Err(err) if err.kind() == ErrorKind::NotFound => {
                    eprintln!("Not found rdb file");
                    Ok(Self::default())
                }
                Err(err) => Err(RedisError::from(err)),
            }
        } else {
            Ok(Self::default())
        }
    }

    pub(crate) fn db(&self) -> &HashMap<String, Value> {
        &self.0
    }
}
