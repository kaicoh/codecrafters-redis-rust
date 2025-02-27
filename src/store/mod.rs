mod replica;
mod transaction;

use super::{
    message::OutgoingMessage,
    rdb::Rdb,
    value::{RedisStream, StreamEntry, StreamEntryId, StreamEntryIdFactor, Value},
    Command, Config, RedisError, RedisResult, Resp,
};
use replica::{Replica, WaitSignal};
use std::net::SocketAddr;
use std::time::{Duration, SystemTime};
use std::{collections::HashMap, time::UNIX_EPOCH};
use tokio::sync::{
    mpsc::{self, Sender},
    Mutex, MutexGuard,
};
use transaction::Transaction;

#[derive(Debug)]
pub struct Store(Mutex<Inner>);

#[derive(Debug)]
struct Inner {
    db: HashMap<String, Value>,
    config: Config,
    replicas: HashMap<SocketAddr, Replica>,
    ack: usize,
    stream_subscribers: HashMap<String, Vec<Sender<()>>>,
    transactions: HashMap<SocketAddr, Transaction>,
}

impl Store {
    pub fn new(config: &Config) -> RedisResult<Self> {
        Ok(Self(Mutex::new(Inner::new(config)?)))
    }

    pub async fn port(&self) -> u16 {
        let inner = self.lock().await;
        inner.config.port
    }

    pub async fn keys(&self) -> Vec<String> {
        let inner = self.lock().await;
        inner.db.keys().map(|v| v.to_string()).collect()
    }

    pub async fn get(&self, key: &str) -> Option<Value> {
        let mut inner = self.lock().await;
        match inner.db.get(key) {
            Some(v) => {
                if v.expired() {
                    inner.db.remove(key);
                    None
                } else {
                    Some(v.clone())
                }
            }
            _ => None,
        }
    }

    pub async fn get_string(&self, key: &str) -> Option<String> {
        self.get(key).await.and_then(|v| match v {
            Value::String { value, .. } => Some(value),
            _ => None,
        })
    }

    pub async fn set_string(&self, key: &str, value: String, exp: Option<u64>) {
        let v = Value::String {
            value: value.clone(),
            exp: exp.map(|n| SystemTime::now() + Duration::from_millis(n)),
        };
        self.set(key, v).await;

        let msg = msg_set_string(key, value, exp);
        self.send_to_replicas(msg).await
    }

    pub async fn increment(&self, key: &str) -> RedisResult<i64> {
        let (value, exp) = match self.get(key).await {
            Some(Value::String { value, exp }) => {
                let num = value.parse::<i64>().map_err(|_| {
                    RedisError::from(anyhow::anyhow!(
                        "ERR value is not an integer or out of range"
                    ))
                })?;
                let value = (num + 1).to_string();
                let exp = exp.map(|time| {
                    time.duration_since(UNIX_EPOCH)
                        .expect("SystemTime before UNIX EPOCH!")
                        .as_millis() as u64
                });
                (value, exp)
            }
            _ => ("1".to_string(), None),
        };
        self.set_string(key, value.clone(), exp).await;
        value.parse().map_err(RedisError::from)
    }

    pub async fn start_queuing(&self, addr: SocketAddr) {
        let mut inner = self.lock().await;
        inner.transactions.insert(addr, Transaction::new());
    }

    pub async fn is_queuing(&self, addr: SocketAddr) -> bool {
        self.lock().await.transactions.contains_key(&addr)
    }

    pub async fn queue(&self, addr: SocketAddr, cmd: Command) {
        let mut inner = self.lock().await;
        if let Some(transaction) = inner.transactions.get_mut(&addr) {
            transaction.push(cmd);
        }
    }

    pub async fn drain_trans(&self, addr: SocketAddr) -> Vec<Command> {
        let mut inner = self.lock().await;
        inner
            .transactions
            .remove(&addr)
            .map(|tran| tran.unwrap())
            .unwrap_or_default()
    }

    pub async fn set_stream(
        &self,
        key: &str,
        id: String,
        values: HashMap<String, String>,
    ) -> RedisResult<StreamEntryId> {
        let id_factor = StreamEntryIdFactor::new(&id)?;

        let mut stream = self.get_stream(key).await?;
        let id = id_factor.try_into_id(&stream)?;
        let entry = StreamEntry::new(id, values);
        stream.push(entry.clone())?;

        let value = Value::Stream(stream);
        self.set(key, value).await;

        let msg = msg_set_stream(key, entry);
        self.send_to_replicas(msg).await;
        self.notify_subscribers(key).await;

        Ok(id)
    }

    pub async fn query_stream(
        &self,
        key: &str,
        start: String,
        end: String,
    ) -> RedisResult<Vec<StreamEntry>> {
        let start = StreamEntryIdFactor::new(&start)?;
        let end = StreamEntryIdFactor::new(&end)?;

        let stream = self.get_stream(key).await?;
        let mut entries: Vec<StreamEntry> = vec![];
        for entry in stream.query(start, end)? {
            entries.push(entry.clone());
        }
        Ok(entries)
    }

    pub async fn find_stream(&self, key: &str, start: String) -> RedisResult<Option<StreamEntry>> {
        let start = StreamEntryIdFactor::new(&start)?;
        self.get_stream(key).await?.find(start).map(|v| v.cloned())
    }

    pub async fn parse_find_stream_args(
        &self,
        args: Vec<(String, String)>,
    ) -> RedisResult<Vec<(String, String)>> {
        let mut responses: Vec<(String, String)> = vec![];
        for (key, start) in args {
            if start.as_str() == "$" {
                let start = self
                    .get_stream(&key)
                    .await?
                    .last_id()
                    .map(|v| format!("{v}"))
                    .unwrap_or("0-0".to_string());
                responses.push((key, start));
            } else {
                responses.push((key, start));
            }
        }
        Ok(responses)
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
        // NOTE:
        // You have to release lock not to block any other actions.
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

    pub async fn subscribe_stream(&self, key: &str, sender: Sender<()>) {
        let mut inner = self.lock().await;
        match inner.stream_subscribers.get_mut(key) {
            Some(senders) => {
                senders.push(sender);
            }
            None => {
                inner.stream_subscribers.insert(key.into(), vec![sender]);
            }
        }
    }

    async fn lock(&self) -> MutexGuard<'_, Inner> {
        self.0.lock().await
    }

    async fn set(&self, key: &str, value: Value) {
        let mut inner = self.lock().await;
        inner.db.insert(key.into(), value);
    }

    async fn get_stream(&self, key: &str) -> RedisResult<RedisStream> {
        match self.get(key).await {
            Some(Value::Stream(stream)) => Ok(stream),
            None => Ok(RedisStream::new()),
            _ => Err(anyhow::anyhow!("Key {key} is not a stream").into()),
        }
    }

    async fn send_to_replicas(&self, msg: OutgoingMessage) {
        let mut inner = self.0.lock().await;
        for msg in msg.into_iter() {
            for (_, replica) in inner.replicas.iter_mut() {
                replica.send(msg.clone()).await
            }
        }
    }

    async fn notify_subscribers(&self, key: &str) {
        let subscribers = {
            let mut inner = self.lock().await;
            match inner.stream_subscribers.get_mut(key) {
                Some(values) => {
                    let mut senders: Vec<Sender<()>> = vec![];
                    senders.append(values);
                    senders
                }
                None => vec![],
            }
        };
        for sender in subscribers {
            if sender.send(()).await.is_err() {
                eprintln!("Receiver has been dropped before sending messsage");
            }
        }
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
            stream_subscribers: HashMap::new(),
            transactions: HashMap::new(),
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

fn msg_set_string(key: &str, value: String, exp: Option<u64>) -> OutgoingMessage {
    let resp: Resp = if let Some(exp) = exp {
        vec![
            "SET".into(),
            format!("{key}"),
            value,
            "px".into(),
            format!("{exp}"),
        ]
        .into()
    } else {
        vec!["SET".into(), format!("{key}"), value].into()
    };

    OutgoingMessage::from(resp)
}

fn msg_set_stream(key: &str, entry: StreamEntry) -> OutgoingMessage {
    let mut tokens: Vec<String> = vec!["XADD".into(), key.into(), format!("{}", entry.id())];
    for (key, value) in entry.values().iter() {
        tokens.push(key.into());
        tokens.push(value.into());
    }
    OutgoingMessage::from(Resp::from(tokens))
}
