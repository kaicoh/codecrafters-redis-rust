use super::{message::OutgoingMessage, rdb::Rdb, value::Value, Config, RedisError, RedisResult};
use std::collections::HashMap;
use std::net::TcpStream;
use std::sync::{Mutex, MutexGuard};
use std::time::{Duration, SystemTime};

#[derive(Debug)]
pub struct Store(Mutex<Inner>);

#[derive(Debug)]
struct Inner {
    db: HashMap<String, Value>,
    config: Config,
    replicas: Vec<TcpStream>,
    ack: usize,
}

impl Store {
    pub fn new(config: &Config) -> RedisResult<Self> {
        let rdb = Rdb::from_conf(config)?;
        let inner = Inner {
            db: rdb.db().clone(),
            config: config.clone(),
            replicas: vec![],
            ack: 0,
        };
        Ok(Self(Mutex::new(inner)))
    }

    pub fn port(&self) -> RedisResult<u16> {
        let inner = self.lock()?;
        Ok(inner.config.port)
    }

    pub fn get(&self, key: &str) -> RedisResult<Option<String>> {
        let mut inner = self.lock()?;
        let value = match inner.db.get(key) {
            Some(v) => {
                if v.expired() {
                    inner.db.remove(key);
                    None
                } else {
                    Some(v.to_string())
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
        let value = Value::String {
            value,
            exp: exp.map(|n| SystemTime::now() + Duration::from_millis(n)),
        };
        inner.db.insert(key.into(), value.clone());

        for stream in inner.replicas.iter_mut() {
            let msg: OutgoingMessage = value.to_resp(key)?.into();

            if let Err(err) = msg.write_to(stream) {
                eprintln!("Failed to sync replica. {err}");
            }
        }
        Ok(())
    }

    pub fn rdb_dir(&self) -> RedisResult<Option<String>> {
        let inner = self.lock()?;
        Ok(inner.config.dir.clone())
    }

    pub fn rdb_dbfilename(&self) -> RedisResult<Option<String>> {
        let inner = self.lock()?;
        Ok(inner.config.dbfilename.clone())
    }

    pub fn role(&self) -> RedisResult<&str> {
        let inner = self.lock()?;
        let role = match inner.config.master {
            Some(_) => "slave",
            None => "master",
        };
        Ok(role)
    }

    pub fn repl_id(&self) -> RedisResult<&str> {
        Ok("8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb")
    }

    pub fn repl_offset(&self) -> RedisResult<usize> {
        Ok(0)
    }

    pub fn rdb(&self, _offset: usize) -> RedisResult<Vec<u8>> {
        let empty_rdb = vec![
            0x52, 0x45, 0x44, 0x49, 0x53, 0x30, 0x30, 0x31, 0x31, 0xfa, 0x09, 0x72, 0x65, 0x64,
            0x69, 0x73, 0x2d, 0x76, 0x65, 0x72, 0x05, 0x37, 0x2e, 0x32, 0x2e, 0x30, 0xfa, 0x0a,
            0x72, 0x65, 0x64, 0x69, 0x73, 0x2d, 0x62, 0x69, 0x74, 0x73, 0xc0, 0x40, 0xfa, 0x05,
            0x63, 0x74, 0x69, 0x6d, 0x65, 0xc2, 0x6d, 0x08, 0xbc, 0x65, 0xfa, 0x08, 0x75, 0x73,
            0x65, 0x64, 0x2d, 0x6d, 0x65, 0x6d, 0xc2, 0xb0, 0xc4, 0x10, 0x00, 0xfa, 0x08, 0x61,
            0x6f, 0x66, 0x2d, 0x62, 0x61, 0x73, 0x65, 0xc0, 0x00, 0xff, 0xf0, 0x6e, 0x3b, 0xfe,
            0xc0, 0xff, 0x5a, 0xa2,
        ];
        Ok(empty_rdb)
    }

    pub fn save_replica_stream(&self, stream: &TcpStream) -> RedisResult<()> {
        let mut inner = self.lock()?;
        inner.replicas.push(stream.try_clone()?);
        Ok(())
    }

    pub fn ack_offset(&self) -> RedisResult<usize> {
        let inner = self.lock()?;
        Ok(inner.ack)
    }

    pub fn add_ack_offset(&self, size: usize) -> RedisResult<()> {
        let mut inner = self.lock()?;
        inner.ack += size;
        Ok(())
    }

    pub fn num_of_replicas(&self) -> RedisResult<usize> {
        let inner = self.lock()?;
        Ok(inner.replicas.len())
    }

    fn lock(&self) -> RedisResult<MutexGuard<'_, Inner>> {
        self.0
            .lock()
            .map_err(|err| RedisError::Lock(format!("{err}")))
    }
}
