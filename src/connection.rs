use super::{Command, CommandMode, IncomingMessage, RedisResult, Resp, Store, BUF_SIZE};
use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::{
    mpsc::{channel, Receiver},
    Arc,
};

#[derive(Debug)]
pub struct Connection {
    stream: TcpStream,
    mode: CommandMode,
}

impl Connection {
    pub fn new(stream: TcpStream, mode: CommandMode) -> Self {
        Self { stream, mode }
    }

    pub fn start_streaming(mut self, store: &Arc<Store>) -> RedisResult<()> {
        let (tx, rx) = channel::<IncomingMessage>();
        let mut stream = self.stream.try_clone()?;

        std::thread::spawn(move || {
            let mut buf = [0; BUF_SIZE];

            while let Ok(size) = stream.read(&mut buf) {
                if size > 0 {
                    println!("Get {size} byte data!");

                    match IncomingMessage::from_buffer(&buf[..size]) {
                        Ok(messages) => {
                            for message in messages {
                                if let Err(err) = tx.send(message) {
                                    eprintln!("Failed to send incoming message: {err}");
                                    break;
                                }
                            }
                        }
                        Err(err) => {
                            eprintln!("ERROR parsing incoming message. {err}")
                        }
                    }
                }
                buf = [0; BUF_SIZE];
            }
        });

        if self.mode == CommandMode::Sync {
            self.handshake(&rx, store.port()?)?;
        }

        let store = Arc::clone(store);
        let mut stream = self.stream;
        let mode = self.mode;

        std::thread::spawn(move || {
            while let Ok(message) = rx.recv() {
                match message {
                    IncomingMessage::Resp(resp) => {
                        let size = resp.len();

                        match Command::new(resp) {
                            Ok(cmd) => {
                                if cmd.store_connection() {
                                    if let Err(err) = store.save_replica_stream(&stream) {
                                        eprintln!("Failed to save replica connection. {err}");
                                    }
                                }

                                let msg = cmd.run(Arc::clone(&store), mode).unwrap_or_else(|err| {
                                    eprintln!("Failed to run command: {err}");
                                    Resp::from(err).into()
                                });

                                if let Err(err) = msg.write_to(&mut stream) {
                                    eprintln!("Failed to write TCP stream: {err}");
                                }

                                if let Err(err) = store.add_ack_offset(size) {
                                    eprintln!("Failed to count processed data. {err}");
                                }
                            }
                            Err(err) => {
                                eprintln!("Failed to get command from RESP. {err}");
                            }
                        }
                    }
                    IncomingMessage::Rdb(_) => {
                        println!("Received RDB file");
                    }
                }
            }
            eprintln!("Something went wrong in receiving incoming message");
        });

        Ok(())
    }

    fn handshake(&mut self, rx: &Receiver<IncomingMessage>, port: u16) -> RedisResult<()> {
        let msg = vec!["PING".to_string()];
        self.send_resp(msg)?;
        println!("Received ping response: {}", rx.recv()?);

        let msg = vec![
            "REPLCONF".to_string(),
            "listening-port".to_string(),
            format!("{port}"),
        ];
        self.send_resp(msg)?;
        println!("Received replconf(listening-port) response: {}", rx.recv()?);

        let msg = vec![
            "REPLCONF".to_string(),
            "capa".to_string(),
            "psync2".to_string(),
        ];
        self.send_resp(msg)?;
        println!("Received replconf(capa) response: {}", rx.recv()?);

        let msg = vec!["PSYNC".to_string(), "?".to_string(), "-1".to_string()];
        self.send_resp(msg)?;
        println!("Received psync response: {}", rx.recv()?);

        Ok(())
    }

    fn send_resp(&mut self, msg: Vec<String>) -> RedisResult<()> {
        self.stream.write_all(&Resp::from(msg).serialize())?;
        Ok(())
    }
}
