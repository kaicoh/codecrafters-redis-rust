use super::{OutgoingMessage, RedisError, RedisResult, Resp, Store};
use std::sync::Arc;

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
    ConfigGet(String),
    Keys,
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

    pub fn run(self, store: Arc<Store>, mode: CommandMode) -> RedisResult<OutgoingMessage> {
        let should_return = self.return_message(mode);

        let message: OutgoingMessage = match self {
            Self::Ping => Resp::SS("PONG".into()).into(),
            Self::Echo(val) => Resp::BS(Some(val)).into(),
            Self::Get { key } => store
                .get(&key)?
                .map(|v| Resp::BS(Some(v)))
                .unwrap_or(Resp::BS(None))
                .into(),
            Self::Set { key, value, exp } => store
                .set(&key, value, exp)
                .map(|_| Resp::SS("OK".into()))?
                .into(),
            Self::ConfigGet(key) => {
                let val = match key.as_str() {
                    "dir" => store.rdb_dir()?,
                    "dbfilename" => store.rdb_dbfilename()?,
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
                    .keys()?
                    .into_iter()
                    .map(|v| Resp::BS(Some(v)))
                    .collect(),
            )
            .into(),
            Self::Info => {
                let role = store.role()?;
                let repl_id = store.repl_id()?;
                let repl_offset = store.repl_offset()?;
                Resp::BS(Some(format!(
                    "role:{role}\r\nmaster_repl_offset:{repl_offset}\r\nmaster_replid:{repl_id}"
                )))
                .into()
            }
            Self::ReplConf { key, .. } => match key.to_uppercase().as_str() {
                "GETACK" => Resp::A(vec![
                    Resp::BS(Some("REPLCONF".into())),
                    Resp::BS(Some("ACK".into())),
                    Resp::BS(Some(format!("{}", store.ack_offset()?))),
                ])
                .into(),
                _ => Resp::SS("OK".into()).into(),
            },
            Self::Psync => {
                let repl_id = store.repl_id()?;
                let repl_offset = store.repl_offset()?;

                let order = Resp::SS(format!("FULLRESYNC {repl_id} {repl_offset}"));
                let rdb = store.rdb(repl_offset)?;
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
}
