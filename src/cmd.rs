use super::{value::Value, OutgoingMessage, RedisError, RedisResult, Resp, Store};
use std::collections::HashMap;
use std::{net::SocketAddr, sync::Arc};

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Context {
    mode: CommandMode,
    addr: SocketAddr,
}

impl Context {
    pub fn new(mode: CommandMode, addr: SocketAddr) -> Self {
        Self { mode, addr }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum CommandMode {
    Normal,
    Sync,
}

#[derive(Debug, PartialEq)]
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
    Type {
        key: String,
    },
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
        kind: String,
        key: String,
        start: String,
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

    pub async fn run(self, store: Arc<Store>, ctx: Context) -> RedisResult<OutgoingMessage> {
        let Context { mode, addr } = ctx;
        let should_return = self.return_message(mode);

        let message: OutgoingMessage = match self {
            Self::Ping => Resp::SS("PONG".into()).into(),
            Self::Echo(val) => Resp::BS(Some(val)).into(),
            Self::Get { key } => store
                .get(&key)
                .await
                .and_then(|v| match v {
                    Value::String { value, .. } => Some(value),
                    _ => None,
                })
                .map(|v| Resp::BS(Some(v)))
                .unwrap_or(Resp::BS(None))
                .into(),
            Self::Set { key, value, exp } => {
                store.set_string(&key, value, exp).await;
                Resp::SS("OK".into()).into()
            }
            Self::Type { key } => store
                .get(&key)
                .await
                .map(|value| Resp::SS(value.type_name().into()))
                .unwrap_or(Resp::SS("none".into()))
                .into(),
            Self::Xadd { key, id, values } => {
                match store.set_stream(&key, id, values).await {
                    Ok(id) => Resp::BS(Some(format!("{id}"))).into(),
                    Err(RedisError::InvalidStreamEntryId00) => Resp::SE("ERR The ID specified in XADD must be greater than 0-0".into()).into(),
                    Err(RedisError::SmallerStreamEntryId) => Resp::SE("ERR The ID specified in XADD is equal or smaller than the target stream top item".into()).into(),
                    Err(err) => {
                        return Err(err);
                    }
                }
            }
            Self::Xrange { key, start, end } => {
                let entries = store.query_stream(&key, start, end).await?;
                Resp::from(entries).into()
            }
            Self::Xread { key, start, .. } => {
                let entry = store.find_stream(&key, start).await?;
                Resp::from((key, entry)).into()
            }
            Self::ConfigGet(key) => {
                let val = match key.as_str() {
                    "dir" => store.rdb_dir().await,
                    "dbfilename" => store.rdb_dbfilename().await,
                    _ => None,
                };

                Resp::A(vec![
                    Resp::BS(Some(key)),
                    val.map(|v| Resp::BS(Some(v))).unwrap_or(Resp::BS(None)),
                ])
                .into()
            }
            Self::Keys => Resp::A(
                store
                    .keys()
                    .await
                    .into_iter()
                    .map(|v| Resp::BS(Some(v)))
                    .collect(),
            )
            .into(),
            Self::Wait { num_replicas, exp } => {
                let synced = store.wait(num_replicas, exp).await;
                Resp::I(synced).into()
            }
            Self::Info => {
                let role = store.role().await;
                let repl_id = store.repl_id();
                let repl_offset = store.repl_offset();
                Resp::BS(Some(format!(
                    "role:{role}\r\nmaster_repl_offset:{repl_offset}\r\nmaster_replid:{repl_id}"
                )))
                .into()
            }
            Self::ReplConf { key, value } => match key.to_uppercase().as_str() {
                "GETACK" => Resp::A(vec![
                    Resp::BS(Some("REPLCONF".into())),
                    Resp::BS(Some("ACK".into())),
                    Resp::BS(Some(format!("{}", store.ack_offset().await))),
                ])
                .into(),
                "ACK" => {
                    let ack = value.parse::<usize>()?;
                    store.receive_replica_ack(addr, ack).await;
                    OutgoingMessage::empty()
                }
                _ => Resp::SS("OK".into()).into(),
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

                OutgoingMessage::new(vec![order.serialize(), rdb_serialized])
            }
            _ => {
                return Err(RedisError::UnknownCommand);
            }
        };

        if should_return {
            Ok(message)
        } else {
            Ok(OutgoingMessage::empty())
        }
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
                "TYPE" => {
                    let key = args
                        .get(1)
                        .ok_or(RedisError::LackOfArgs { need: 1, got: 0 })?
                        .to_string();
                    Self::Type { key }
                }
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
                    let kind = args
                        .get(1)
                        .ok_or(RedisError::LackOfArgs { need: 3, got: 0 })?
                        .to_string();
                    let key = args
                        .get(2)
                        .ok_or(RedisError::LackOfArgs { need: 3, got: 0 })?
                        .to_string();
                    let start = args
                        .get(3)
                        .ok_or(RedisError::LackOfArgs { need: 3, got: 1 })?
                        .to_string();

                    Self::Xread { kind, key, start }
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
            "stream".to_string(),
            "stream_key".to_string(),
            "1526985054069".to_string(),
        ];
        let cmd = Command::from_args(args).unwrap();
        let expected = Command::Xread {
            kind: "stream".into(),
            key: "stream_key".into(),
            start: "1526985054069".into(),
        };
        assert_eq!(cmd, expected);
    }
}
