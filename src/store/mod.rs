mod replica;

use super::{message::OutgoingMessage, rdb::Rdb, value::Value, Config, RedisResult, Resp};
use replica::{Replica, WaitSignal};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::{Duration, SystemTime};
use tokio::sync::{
    mpsc::{self, Sender},
    Mutex, MutexGuard,
};

#[derive(Debug)]
pub struct Store(Mutex<Inner>);

#[derive(Debug)]
struct Inner {
    db: HashMap<String, Value>,
    config: Config,
    replicas: HashMap<SocketAddr, Replica>,
    ack: usize,
}

impl Store {
    pub fn new(config: &Config) -> RedisResult<Self> {
        Ok(Self(Mutex::new(Inner::new(config)?)))
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
            for (_, replica) in inner.replicas.iter_mut() {
                replica.send(msg.clone()).await
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

    pub async fn subscribe(&self, addr: SocketAddr, tx: Sender<Vec<u8>>) {
        let mut inner = self.lock().await;
        inner.add_replica(addr, tx);
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
        inner.num_of_replicas()
    }

    pub async fn receive_replica_ack(&self, addr: SocketAddr, ack: usize) {
        let mut inner = self.lock().await;
        if let Some(replica) = inner.replicas.get_mut(&addr) {
            replica.receive_ack(ack).await;
        }
    }

    pub async fn wait(&self, num_replicas: usize, exp: u64) -> i64 {
        let (mut synced, mut rx) = {
            let mut inner = self.lock().await;
            let unsynced = inner.replicas.iter().filter_map(is_unsynced).count();

            let (tx, rx) = mpsc::channel::<WaitSignal>(unsynced + 1);
            for replica in inner.replicas.iter_mut().filter_map(is_unsynced_mut) {
                let tx = tx.clone();
                replica.add_wait_callback(tx).await;
                replica.send_getack().await;
            }

            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(exp)).await;
                if tx.send(WaitSignal::Timeout).await.is_err() {
                    eprintln!("Receiver dropped before timeout");
                }
            });

            let synced = inner.replicas.iter().filter_map(is_synced).count();
            (synced, rx)
        };

        if synced >= num_replicas {
            return synced as i64;
        }

        while let Some(sig) = rx.recv().await {
            match sig {
                WaitSignal::Synced => {
                    synced += 1;
                    println!("Synced signal received! Now {synced} replicas are synced");

                    if synced >= num_replicas {
                        break;
                    }
                }
                WaitSignal::Timeout => {
                    println!("Wait timeout");
                    break;
                }
            }
        }
        synced as i64
    }

    async fn lock(&self) -> MutexGuard<'_, Inner> {
        self.0.lock().await
    }
}

impl Inner {
    fn new(config: &Config) -> RedisResult<Self> {
        let rdb = Rdb::from_conf(config)?;
        Ok(Self {
            db: rdb.db().clone(),
            config: config.clone(),
            replicas: HashMap::new(),
            ack: 0,
        })
    }

    fn num_of_replicas(&self) -> usize {
        self.replicas.len()
    }

    fn add_replica(&mut self, addr: SocketAddr, tx: Sender<Vec<u8>>) {
        self.replicas.insert(addr, Replica::new(tx));
    }
}

fn is_synced<'a>((_, replica): (&'a SocketAddr, &'a Replica)) -> Option<&'a Replica> {
    if replica.is_synced() {
        Some(replica)
    } else {
        None
    }
}

fn is_unsynced<'a>((_, replica): (&'a SocketAddr, &'a Replica)) -> Option<&'a Replica> {
    if !replica.is_synced() {
        Some(replica)
    } else {
        None
    }
}

fn is_unsynced_mut<'a>((_, replica): (&'a SocketAddr, &'a mut Replica)) -> Option<&'a mut Replica> {
    if !replica.is_synced() {
        Some(replica)
    } else {
        None
    }
}
