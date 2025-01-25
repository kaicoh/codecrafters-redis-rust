use super::{OutgoingMessage, RedisError, RedisResult, Resp, Store};
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
                .map(|v| Resp::BS(Some(v)))
                .unwrap_or(Resp::BS(None))
                .into(),
            Self::Set { key, value, exp } => {
                store.set(&key, value, exp).await;
                Resp::SS("OK".into()).into()
            }
            Self::Type { key } => store
                .get(&key)
                .await
                .map(|_| Resp::SS("string".into()))
                .unwrap_or(Resp::SS("none".into()))
                .into(),
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
}
