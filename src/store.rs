use super::{message::OutgoingMessage, rdb::Rdb, value::Value, Config, RedisResult};
use std::collections::HashMap;
use std::time::{Duration, SystemTime};
use tokio::sync::{mpsc::Sender, Mutex, MutexGuard};

#[derive(Debug)]
pub struct Store(Mutex<Inner>);

#[derive(Debug)]
struct Inner {
    db: HashMap<String, Value>,
    config: Config,
    replicas: Vec<Sender<Vec<u8>>>,
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

    pub async fn port(&self) -> u16 {
        let inner = self.lock().await;
        inner.config.port
    }

    pub async fn get(&self, key: &str) -> Option<String> {
        let mut inner = self.lock().await;
        match inner.db.get(key) {
            Some(v) => {
                if v.expired() {
                    inner.db.remove(key);
                    None
                } else {
                    Some(v.to_string())
                }
            }
            _ => None,
        }
    }

    pub async fn keys(&self) -> Vec<String> {
        let inner = self.lock().await;
        inner.db.keys().map(|v| v.to_string()).collect()
    }

    pub async fn set(&self, key: &str, value: String, exp: Option<u64>) {
        let mut inner = self.lock().await;
        let value = Value::String {
            value,
            exp: exp.map(|n| SystemTime::now() + Duration::from_millis(n)),
        };
        inner.db.insert(key.into(), value.clone());

        let msg: OutgoingMessage = match value.to_resp(key) {
            Ok(resp) => resp.into(),
            Err(err) => {
                eprintln!("Failed to build message: {err}");
                return;
            }
        };

        for msg in msg.into_iter() {
            for tx in inner.replicas.iter() {
                if let Err(err) = tx.send(msg.clone()).await {
                    eprintln!("Error sending message to replica. {err}");
                }
            }
        }
    }

    pub async fn rdb_dir(&self) -> Option<String> {
        let inner = self.lock().await;
        inner.config.dir.clone()
    }

    pub async fn rdb_dbfilename(&self) -> Option<String> {
        let inner = self.lock().await;
        inner.config.dbfilename.clone()
    }

    pub async fn role(&self) -> &str {
        let inner = self.lock().await;
        match inner.config.master {
            Some(_) => "slave",
            None => "master",
        }
    }

    pub fn repl_id(&self) -> &str {
        "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
    }

    pub fn repl_offset(&self) -> usize {
        0
    }

    pub fn rdb(&self, _offset: usize) -> Vec<u8> {
        vec![
            0x52, 0x45, 0x44, 0x49, 0x53, 0x30, 0x30, 0x31, 0x31, 0xfa, 0x09, 0x72, 0x65, 0x64,
            0x69, 0x73, 0x2d, 0x76, 0x65, 0x72, 0x05, 0x37, 0x2e, 0x32, 0x2e, 0x30, 0xfa, 0x0a,
            0x72, 0x65, 0x64, 0x69, 0x73, 0x2d, 0x62, 0x69, 0x74, 0x73, 0xc0, 0x40, 0xfa, 0x05,
            0x63, 0x74, 0x69, 0x6d, 0x65, 0xc2, 0x6d, 0x08, 0xbc, 0x65, 0xfa, 0x08, 0x75, 0x73,
            0x65, 0x64, 0x2d, 0x6d, 0x65, 0x6d, 0xc2, 0xb0, 0xc4, 0x10, 0x00, 0xfa, 0x08, 0x61,
            0x6f, 0x66, 0x2d, 0x62, 0x61, 0x73, 0x65, 0xc0, 0x00, 0xff, 0xf0, 0x6e, 0x3b, 0xfe,
            0xc0, 0xff, 0x5a, 0xa2,
        ]
    }

    pub async fn subscribe(&self, tx: Sender<Vec<u8>>) {
        let mut inner = self.lock().await;
        inner.replicas.push(tx);
    }

    pub async fn ack_offset(&self) -> usize {
        let inner = self.lock().await;
        inner.ack
    }

    pub async fn add_ack_offset(&self, size: usize) {
        let mut inner = self.lock().await;
        inner.ack += size;
    }

    pub async fn num_of_replicas(&self) -> usize {
        let inner = self.lock().await;
        inner.replicas.len()
    }

    async fn lock(&self) -> MutexGuard<'_, Inner> {
        self.0.lock().await
    }
}
