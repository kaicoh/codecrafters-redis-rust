use super::{value::StreamEntry, OutgoingMessage, RedisError, RedisResult, Resp, Store};
use std::{collections::HashMap, time::Duration};
use std::{net::SocketAddr, sync::Arc};
use tokio::sync::{mpsc, oneshot::Sender};
use tokio::time::sleep;

#[derive(Debug, Clone, Copy)]
pub struct ContextBuilder {
    mode: CommandMode,
    addr: SocketAddr,
}

impl ContextBuilder {
    pub fn build(&self, sender: Sender<OutgoingMessage>) -> Context {
        Context {
            mode: self.mode,
            addr: self.addr,
            sender: Some(sender),
        }
    }
}

#[derive(Debug)]
pub struct Context {
    mode: CommandMode,
    addr: SocketAddr,
    sender: Option<Sender<OutgoingMessage>>,
}

impl Context {
    pub fn builder(mode: CommandMode, addr: SocketAddr) -> ContextBuilder {
        ContextBuilder { mode, addr }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum CommandMode {
    Normal,
    Sync,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Command {
    Ping,
    Echo(String),
    Get {
        key: String,
    },
    Set {
        key: String,
        value: String,
        exp: Option<u64>,
    },
    Incr {
        key: String,
    },
    Type {
        key: String,
    },
    Multi,
    Exec,
    Discard,
    Xadd {
        key: String,
        id: String,
        values: HashMap<String, String>,
    },
    Xrange {
        key: String,
        start: String,
        end: String,
    },
    Xread {
        block: Option<u64>,
        stream: Vec<(String, String)>,
    },
    ConfigGet(String),
    Keys,
    Wait {
        num_replicas: usize,
        exp: u64,
    },
    Info,
    ReplConf {
        key: String,
        value: String,
    },
    Psync,
    Unknown,
}

impl Command {
    pub fn new(resp: Resp) -> RedisResult<Self> {
        let args = command_args(resp);
        Self::from_args(args)
    }

    pub async fn execute(self, store: Arc<Store>, mut ctx: Context) {
        let msg = if self.need_queue(&store, ctx.addr).await {
            store.queue(ctx.addr, self).await;
            Resp::SS("QUEUED".into()).into()
        } else if matches!(self, Self::Exec) {
            if store.is_queuing(ctx.addr).await {
                let mut resps: Vec<Resp> = vec![];

                for cmd in store.drain_trans(ctx.addr).await {
                    match cmd.run(Arc::clone(&store), &mut ctx).await {
                        Ok(Some(resp)) => {
                            resps.push(resp);
                        }
                        Ok(None) => {
                            println!("No return message");
                        }
                        Err(err) => {
                            resps.push(Resp::from(err));
                        }
                    }
                }

                Resp::A(resps).into()
            } else {
                Resp::SE("ERR EXEC without MULTI".into()).into()
            }
        } else if matches!(self, Self::Discard) {
            if store.is_queuing(ctx.addr).await {
                let _ = store.drain_trans(ctx.addr).await;
                Resp::SS("OK".into()).into()
            } else {
                Resp::SE("ERR DISCARD without MULTI".into()).into()
            }
        } else {
            let need_return = self.return_message(ctx.mode);

            self.run(store, &mut ctx)
                .await
                .unwrap_or_else(|err| {
                    eprintln!("Failed to run command. {err}");
                    Some(Resp::from(err))
                })
                .map(|v| {
                    if need_return {
                        OutgoingMessage::from(v)
                    } else {
                        OutgoingMessage::empty()
                    }
                })
                .unwrap_or_else(OutgoingMessage::empty)
        };

        if let Some(sender) = ctx.sender {
            if sender.send(msg).is_err() {
                eprintln!("Oneshot receiver dropped before sending message!");
            }
        }
    }

    pub async fn run(self, store: Arc<Store>, ctx: &mut Context) -> RedisResult<Option<Resp>> {
        let opt = match self {
            Self::Ping => Some(Resp::SS("PONG".into())),
            Self::Echo(val) => Some(Resp::BS(Some(val))),
            Self::Get { key } => {
                let value = store
                    .get_string(&key)
                    .await
                    .map(|v| Resp::BS(Some(v)))
                    .unwrap_or(Resp::BS(None));
                Some(value)
            }
            Self::Set { key, value, exp } => {
                store.set_string(&key, value, exp).await;
                Some(Resp::SS("OK".into()))
            }
            Self::Incr { key } => {
                let num = store.increment(&key).await?;
                Some(Resp::I(num))
            }
            Self::Type { key } => {
                let value = store
                    .get(&key)
                    .await
                    .map(|value| Resp::SS(value.type_name().into()))
                    .unwrap_or(Resp::SS("none".into()));
                Some(value)
            }
            Self::Multi => {
                store.start_queuing(ctx.addr).await;
                Some(Resp::SS("OK".into()))
            }
            Self::Xadd { key, id, values } => {
                let resp = store
                    .set_stream(&key, id, values)
                    .await
                    .map(|id| Resp::BS(Some(format!("{id}"))))?;
                Some(resp)
            }
            Self::Xrange { key, start, end } => {
                let entries = store.query_stream(&key, start, end).await?;
                Some(Resp::from(entries))
            }
            Self::Xread { block, stream } => {
                let stream = store.parse_find_stream_args(stream).await?;
                match block {
                    Some(0) => {
                        if let Some(sender) = ctx.sender.take() {
                            tokio::spawn(async move {
                                let store_cp = Arc::clone(&store);
                                let stream_cp = stream.clone();

                                let mut msg: Option<OutgoingMessage> =
                                    read_stream(store_cp, stream_cp)
                                        .await
                                        .map(|v| Resp::from(v).into());

                                while msg.is_none() {
                                    // 1. Ask store to inform after adding any stream entry.
                                    let (tx, mut rx) = mpsc::channel::<()>(stream.len());
                                    for (key, _) in stream.iter() {
                                        let tx = tx.clone();
                                        store.subscribe_stream(key, tx).await;
                                    }

                                    // 2. Wait until notification from the store arrives.
                                    if rx.recv().await.is_none() {
                                        eprintln!("Mpsc sender dropped before sending");
                                        return;
                                    }

                                    // 3. Get stream again.
                                    let store_cp = Arc::clone(&store);
                                    let stream_cp = stream.clone();

                                    msg = read_stream(store_cp, stream_cp)
                                        .await
                                        .map(|v| Resp::from(v).into());
                                }

                                if sender.send(msg.unwrap()).is_err() {
                                    eprintln!("Oneshot receiver dropped before sending");
                                }
                            });
                        }
                        None
                    }
                    Some(milli) => {
                        if let Some(sender) = ctx.sender.take() {
                            tokio::spawn(async move {
                                sleep(Duration::from_millis(milli)).await;

                                let msg: OutgoingMessage = read_stream(store, stream)
                                    .await
                                    .map(Resp::from)
                                    .unwrap_or(Resp::BS(None))
                                    .into();

                                if sender.send(msg).is_err() {
                                    eprintln!("Oneshot receiver dropped before sending");
                                }
                            });
                        }
                        None
                    }
                    _ => {
                        let resp = read_stream(store, stream)
                            .await
                            .map(Resp::from)
                            .unwrap_or(Resp::BS(None));
                        Some(resp)
                    }
                }
            }
            Self::ConfigGet(key) => {
                let val = match key.as_str() {
                    "dir" => store.rdb_dir().await,
                    "dbfilename" => store.rdb_dbfilename().await,
                    _ => None,
                };

                let resp = Resp::A(vec![
                    Resp::BS(Some(key)),
                    val.map(|v| Resp::BS(Some(v))).unwrap_or(Resp::BS(None)),
                ]);
                Some(resp)
            }
            Self::Keys => {
                let resp = Resp::A(
                    store
                        .keys()
                        .await
                        .into_iter()
                        .map(|v| Resp::BS(Some(v)))
                        .collect(),
                );
                Some(resp)
            }
            Self::Wait { num_replicas, exp } => {
                let synced = store.wait(num_replicas, exp).await;
                Some(Resp::I(synced))
            }
            Self::Info => {
                let role = store.role().await;
                let repl_id = store.repl_id();
                let repl_offset = store.repl_offset();
                let resp = Resp::BS(Some(format!(
                    "role:{role}\r\nmaster_repl_offset:{repl_offset}\r\nmaster_replid:{repl_id}"
                )));
                Some(resp)
            }
            Self::ReplConf { key, value } => match key.to_uppercase().as_str() {
                "GETACK" => {
                    let resp = Resp::A(vec![
                        Resp::BS(Some("REPLCONF".into())),
                        Resp::BS(Some("ACK".into())),
                        Resp::BS(Some(format!("{}", store.ack_offset().await))),
                    ]);
                    Some(resp)
                }
                "ACK" => {
                    let ack = value.parse::<usize>()?;
                    store.receive_replica_ack(ctx.addr, ack).await;
                    None
                }
                _ => Some(Resp::SS("OK".into())),
            },
            Self::Psync => {
                let repl_id = store.repl_id();
                let repl_offset = store.repl_offset();

                let order = Resp::SS(format!("FULLRESYNC {repl_id} {repl_offset}"));
                let rdb = store.rdb(repl_offset);
                let rdb_serialized: Vec<u8> = format!("${}\r\n", rdb.len())
                    .into_bytes()
                    .into_iter()
                    .chain(rdb)
                    .collect();

                Some(Resp::RAW(vec![order.serialize(), rdb_serialized]))
            }
            _ => {
                return Err(RedisError::UnknownCommand);
            }
        };

        Ok(opt)
    }

    fn from_args(args: Vec<String>) -> RedisResult<Self> {
        let cmd = if let Some(first) = args.first() {
            match first.to_uppercase().as_str() {
                "PING" => Self::Ping,
                "ECHO" => {
                    let arg = args
                        .get(1)
                        .ok_or(RedisError::LackOfArgs { need: 1, got: 0 })?;
                    Self::Echo(arg.into())
                }
                "GET" => {
                    let key = args
                        .get(1)
                        .ok_or(RedisError::LackOfArgs { need: 1, got: 0 })?
                        .to_string();
                    Self::Get { key }
                }
                "SET" => {
                    let key = args
                        .get(1)
                        .ok_or(RedisError::LackOfArgs { need: 2, got: 0 })?
                        .to_string();
                    let value = args
                        .get(2)
                        .ok_or(RedisError::LackOfArgs { need: 2, got: 1 })?
                        .to_string();
                    let exp = args
                        .get(3)
                        .and_then(|opt| {
                            if opt.to_uppercase().as_str() == "PX" {
                                args.get(4)
                            } else {
                                None
                            }
                        })
                        .and_then(|v| v.parse::<u64>().ok());
                    Self::Set { key, value, exp }
                }
                "INCR" => {
                    let key = args
                        .get(1)
                        .ok_or(RedisError::LackOfArgs { need: 1, got: 0 })?
                        .to_string();
                    Self::Incr { key }
                }
                "TYPE" => {
                    let key = args
                        .get(1)
                        .ok_or(RedisError::LackOfArgs { need: 1, got: 0 })?
                        .to_string();
                    Self::Type { key }
                }
                "MULTI" => Self::Multi,
                "EXEC" => Self::Exec,
                "DISCARD" => Self::Discard,
                "XADD" => {
                    if args.len() < 5 {
                        return Err(RedisError::LackOfArgs {
                            need: 5,
                            got: args.len(),
                        });
                    }

                    let key = args
                        .get(1)
                        .ok_or(RedisError::LackOfArgs { need: 5, got: 0 })?
                        .to_string();
                    let id = args
                        .get(2)
                        .ok_or(RedisError::LackOfArgs { need: 5, got: 1 })?
                        .to_string();
                    let values = into_hashmap(&args[3..]);

                    Self::Xadd { key, id, values }
                }
                "XRANGE" => {
                    let key = args
                        .get(1)
                        .ok_or(RedisError::LackOfArgs { need: 3, got: 0 })?
                        .to_string();
                    let start = args
                        .get(2)
                        .ok_or(RedisError::LackOfArgs { need: 3, got: 1 })?
                        .to_string();
                    let end = args
                        .get(3)
                        .ok_or(RedisError::LackOfArgs { need: 3, got: 2 })?
                        .to_string();

                    Self::Xrange { key, start, end }
                }
                "XREAD" => {
                    let (block, stream) = xread_args(&args[1..])?;
                    Self::Xread { block, stream }
                }
                "CONFIG" => match args.get(1) {
                    Some(cmd) if cmd.to_uppercase().as_str() == "GET" => {
                        let key = args
                            .get(2)
                            .cloned()
                            .ok_or(RedisError::LackOfArgs { need: 1, got: 0 })?;
                        Self::ConfigGet(key)
                    }
                    _ => Self::Unknown,
                },
                "KEYS" => Self::Keys,
                "WAIT" => {
                    let num_replicas = args
                        .get(1)
                        .ok_or(RedisError::LackOfArgs { need: 2, got: 0 })?
                        .parse::<usize>()?;
                    let exp = args
                        .get(2)
                        .ok_or(RedisError::LackOfArgs { need: 2, got: 1 })?
                        .parse::<u64>()?;
                    Self::Wait { num_replicas, exp }
                }
                "INFO" => Self::Info,
                "REPLCONF" => {
                    let key = args
                        .get(1)
                        .ok_or(RedisError::LackOfArgs { need: 2, got: 0 })?
                        .to_string();
                    let value = args
                        .get(2)
                        .ok_or(RedisError::LackOfArgs { need: 2, got: 1 })?
                        .to_string();
                    Self::ReplConf { key, value }
                }
                "PSYNC" => Self::Psync,
                _ => Self::Unknown,
            }
        } else {
            Self::Unknown
        };

        Ok(cmd)
    }

    pub fn store_connection(&self) -> bool {
        matches!(self, Self::Psync)
    }

    fn return_message(&self, mode: CommandMode) -> bool {
        if mode == CommandMode::Sync {
            matches!(self, Self::Info | Self::ReplConf { .. })
        } else {
            true
        }
    }

    async fn need_queue(&self, store: &Arc<Store>, addr: SocketAddr) -> bool {
        store.is_queuing(addr).await && !matches!(self, Self::Exec | Self::Discard)
    }
}

fn command_args(message: Resp) -> Vec<String> {
    match message {
        Resp::A(args) => args
            .into_iter()
            .filter_map(|el| {
                if let Resp::BS(Some(arg)) = el {
                    Some(arg)
                } else {
                    None
                }
            })
            .collect(),
        _ => vec![],
    }
}

fn into_hashmap(values: &[String]) -> HashMap<String, String> {
    let mut map: HashMap<String, String> = HashMap::new();

    for chunk in values.chunks(2) {
        if chunk.len() == 2 {
            let key = chunk.first().unwrap();
            let value = chunk.get(1).unwrap();
            map.insert(key.into(), value.into());
        }
    }

    map
}

type XreadArgs = (Option<u64>, Vec<(String, String)>);
fn xread_args(values: &[String]) -> RedisResult<XreadArgs> {
    let stream_pos = arg_starts_at(values, "streams").ok_or(RedisError::from(anyhow::anyhow!(
        "argument streams is required"
    )))?;
    match arg_starts_at(values, "block") {
        Some(block_pos) if block_pos < stream_pos => {
            let block = values
                .get(block_pos)
                .ok_or(RedisError::from(anyhow::anyhow!(
                    "Not found block argument value"
                )))?
                .parse::<u64>()?;
            let stream = zip_pairs(&values[stream_pos..]);
            Ok((Some(block), stream))
        }
        Some(block_pos) => {
            let block = values
                .get(block_pos)
                .ok_or(RedisError::from(anyhow::anyhow!(
                    "Not found block argument value"
                )))?
                .parse::<u64>()?;
            let stream = zip_pairs(&values[stream_pos..(block_pos - 1)]);
            Ok((Some(block), stream))
        }
        None => {
            let stream = zip_pairs(&values[stream_pos..]);
            Ok((None, stream))
        }
    }
}

fn arg_starts_at(values: &[String], arg: &str) -> Option<usize> {
    values.iter().position(|v| v.as_str() == arg).map(|v| v + 1)
}

fn zip_pairs(values: &[String]) -> Vec<(String, String)> {
    let center = values.len() / 2;
    values[..center]
        .iter()
        .zip(values[center..].iter())
        .map(|(key, id)| (key.into(), id.into()))
        .collect()
}

async fn read_stream(
    store: Arc<Store>,
    pairs: Vec<(String, String)>,
) -> Option<Vec<(String, StreamEntry)>> {
    let mut responses: Vec<(String, StreamEntry)> = vec![];
    for (key, start) in pairs {
        match store.find_stream(&key, start).await {
            Ok(Some(entry)) => {
                responses.push((key, entry));
            }
            _ => {
                eprintln!("No stream found.");
                return None;
            }
        }
    }
    Some(responses)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_parses_ping_command() {
        let args = vec!["PING".to_string()];
        let cmd = Command::from_args(args).unwrap();
        let expected = Command::Ping;
        assert_eq!(cmd, expected);
    }

    #[test]
    fn it_parses_echo_command() {
        let args = vec!["Echo".to_string(), "foo".to_string()];
        let cmd = Command::from_args(args).unwrap();
        let expected = Command::Echo("foo".into());
        assert_eq!(cmd, expected);
    }

    #[test]
    fn it_parses_get_command() {
        let args = vec!["GET".to_string(), "foo".to_string()];
        let cmd = Command::from_args(args).unwrap();
        let expected = Command::Get { key: "foo".into() };
        assert_eq!(cmd, expected);
    }

    #[test]
    fn it_parses_set_command() {
        let args = vec!["SET".to_string(), "foo".to_string(), "bar".to_string()];
        let cmd = Command::from_args(args).unwrap();
        let expected = Command::Set {
            key: "foo".into(),
            value: "bar".into(),
            exp: None,
        };
        assert_eq!(cmd, expected);

        let args = vec![
            "SET".to_string(),
            "foo".to_string(),
            "bar".to_string(),
            "px".to_string(),
            "100".to_string(),
        ];
        let cmd = Command::from_args(args).unwrap();
        let expected = Command::Set {
            key: "foo".into(),
            value: "bar".into(),
            exp: Some(100),
        };
        assert_eq!(cmd, expected);
    }

    #[test]
    fn it_parses_config_get_command() {
        let args = vec!["CONFIG".to_string(), "GET".to_string(), "foo".to_string()];
        let cmd = Command::from_args(args).unwrap();
        let expected = Command::ConfigGet("foo".into());
        assert_eq!(cmd, expected);
    }

    #[test]
    fn it_parses_keys_command() {
        let args = vec!["KEYS".to_string(), "*".to_string()];
        let cmd = Command::from_args(args).unwrap();
        let expected = Command::Keys;
        assert_eq!(cmd, expected);
    }

    #[test]
    fn it_parses_info_command() {
        let args = vec!["INFO".to_string()];
        let cmd = Command::from_args(args).unwrap();
        let expected = Command::Info;
        assert_eq!(cmd, expected);
    }

    #[test]
    fn it_parses_replconf_command() {
        let args = vec![
            "REPLCONF".to_string(),
            "listening-port".to_string(),
            "6380".to_string(),
        ];
        let cmd = Command::from_args(args).unwrap();
        let expected = Command::ReplConf {
            key: "listening-port".into(),
            value: "6380".into(),
        };
        assert_eq!(cmd, expected);
    }

    #[test]
    fn it_parses_psync_command() {
        let args = vec!["PSYNC".to_string()];
        let cmd = Command::from_args(args).unwrap();
        let expected = Command::Psync;
        assert_eq!(cmd, expected);
    }

    #[test]
    fn it_parses_wait_command() {
        let args = vec!["WAIT".to_string(), "7".to_string(), "500".to_string()];
        let cmd = Command::from_args(args).unwrap();
        let expected = Command::Wait {
            num_replicas: 7,
            exp: 500,
        };
        assert_eq!(cmd, expected);
    }

    #[test]
    fn it_parses_type_command() {
        let args = vec!["TYPE".to_string(), "some_key".to_string()];
        let cmd = Command::from_args(args).unwrap();
        let expected = Command::Type {
            key: "some_key".into(),
        };
        assert_eq!(cmd, expected);
    }

    #[test]
    fn it_parses_xadd_command() {
        let args = vec![
            "XADD".to_string(),
            "stream_key".to_string(),
            "0-1".to_string(),
            "foo".to_string(),
            "bar".to_string(),
        ];
        let cmd = Command::from_args(args).unwrap();
        let expected = Command::Xadd {
            key: "stream_key".into(),
            id: "0-1".into(),
            values: [("foo".to_string(), "bar".to_string())].into(),
        };
        assert_eq!(cmd, expected);
    }

    #[test]
    fn it_parses_xrange_command() {
        let args = vec![
            "XRANGE".to_string(),
            "stream_key".to_string(),
            "1526985054069".to_string(),
            "1526985054079".to_string(),
        ];
        let cmd = Command::from_args(args).unwrap();
        let expected = Command::Xrange {
            key: "stream_key".into(),
            start: "1526985054069".into(),
            end: "1526985054079".into(),
        };
        assert_eq!(cmd, expected);
    }

    #[test]
    fn it_parses_xread_command() {
        let args = vec![
            "XREAD".to_string(),
            "streams".to_string(),
            "stream_key".to_string(),
            "1526985054069".to_string(),
        ];
        let cmd = Command::from_args(args).unwrap();
        let expected = Command::Xread {
            block: None,
            stream: vec![("stream_key".into(), "1526985054069".into())],
        };
        assert_eq!(cmd, expected);

        let args = vec![
            "XREAD".to_string(),
            "streams".to_string(),
            "stream_key".to_string(),
            "other_stream_key".to_string(),
            "0-0".to_string(),
            "0-1".to_string(),
        ];
        let cmd = Command::from_args(args).unwrap();
        let expected = Command::Xread {
            block: None,
            stream: vec![
                ("stream_key".into(), "0-0".into()),
                ("other_stream_key".into(), "0-1".into()),
            ],
        };
        assert_eq!(cmd, expected);

        let args = vec![
            "XREAD".to_string(),
            "block".to_string(),
            "1000".to_string(),
            "streams".to_string(),
            "stream_key".to_string(),
            "other_stream_key".to_string(),
            "0-0".to_string(),
            "0-1".to_string(),
        ];
        let cmd = Command::from_args(args).unwrap();
        let expected = Command::Xread {
            block: Some(1000),
            stream: vec![
                ("stream_key".into(), "0-0".into()),
                ("other_stream_key".into(), "0-1".into()),
            ],
        };
        assert_eq!(cmd, expected);

        let args = vec![
            "XREAD".to_string(),
            "streams".to_string(),
            "stream_key".to_string(),
            "other_stream_key".to_string(),
            "0-0".to_string(),
            "0-1".to_string(),
            "block".to_string(),
            "1000".to_string(),
        ];
        let cmd = Command::from_args(args).unwrap();
        let expected = Command::Xread {
            block: Some(1000),
            stream: vec![
                ("stream_key".into(), "0-0".into()),
                ("other_stream_key".into(), "0-1".into()),
            ],
        };
        assert_eq!(cmd, expected);
    }

    #[test]
    fn it_parses_incr_command() {
        let args = vec!["INCR".to_string(), "some_key".to_string()];
        let cmd = Command::from_args(args).unwrap();
        let expected = Command::Incr {
            key: "some_key".into(),
        };
        assert_eq!(cmd, expected);
    }

    #[test]
    fn it_parses_multi_command() {
        let args = vec!["MULTI".to_string()];
        let cmd = Command::from_args(args).unwrap();
        let expected = Command::Multi;
        assert_eq!(cmd, expected);
    }

    #[test]
    fn it_parses_exec_command() {
        let args = vec!["EXEC".to_string()];
        let cmd = Command::from_args(args).unwrap();
        let expected = Command::Exec;
        assert_eq!(cmd, expected);
    }

    #[test]
    fn it_parses_discard_command() {
        let args = vec!["DISCARD".to_string()];
        let cmd = Command::from_args(args).unwrap();
        let expected = Command::Discard;
        assert_eq!(cmd, expected);
    }
}
