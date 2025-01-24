use super::{Command, CommandMode, IncomingMessage, RedisResult, Resp, Store, BUF_SIZE};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{tcp::OwnedWriteHalf, TcpStream};
use tokio::sync::mpsc::{self, Receiver};

#[derive(Debug)]
pub struct Connection {
    stream: TcpStream,
    mode: CommandMode,
}

impl Connection {
    pub fn new(stream: TcpStream, mode: CommandMode) -> Self {
        Self { stream, mode }
    }

    pub async fn start_streaming(self, store: &Arc<Store>) -> RedisResult<()> {
        let Self { stream, mode } = self;
        let addr = stream.peer_addr()?;
        let store = Arc::clone(store);
        let (mut rs, mut ws) = stream.into_split();
        let (tx_in, mut rx_in) = mpsc::channel::<IncomingMessage>(100);
        let (tx_by, mut rx_by) = mpsc::channel::<Vec<u8>>(100);

        tokio::spawn(async move {
            let mut buf = [0; BUF_SIZE];

            while let Ok(size) = rs.read(&mut buf).await {
                if size > 0 {
                    println!("Get {size} byte data!");

                    match IncomingMessage::from_buffer(&buf[..size]) {
                        Ok(messages) => {
                            for message in messages {
                                if let Err(err) = tx_in.send(message).await {
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

        if mode == CommandMode::Sync {
            // handshaking process
            ping(&mut ws, &mut rx_in).await?;
            repl_conf(&mut ws, &mut rx_in, store.port().await).await?;
            psync(&mut ws, &mut rx_in).await?;
        }

        tokio::spawn(async move {
            while let Some(msg) = rx_by.recv().await {
                if let Err(err) = ws.write_all(&msg).await {
                    eprintln!("Error sending message to {addr}. {err}");
                }
            }
            eprintln!("Channel closed. Stop reading bytes from {addr}");
        });

        tokio::spawn(async move {
            while let Some(msg) = rx_in.recv().await {
                match msg {
                    IncomingMessage::Resp(resp) => {
                        let size = resp.len();

                        match Command::new(resp) {
                            Ok(cmd) => {
                                if cmd.store_connection() {
                                    store.subscribe(tx_by.clone()).await;
                                }

                                let msg =
                                    cmd.run(Arc::clone(&store), mode)
                                        .await
                                        .unwrap_or_else(|err| {
                                            eprintln!("Failed to run command: {err}");
                                            Resp::from(err).into()
                                        });

                                for bytes in msg.into_iter() {
                                    if let Err(err) = tx_by.send(bytes).await {
                                        eprintln!("Failed to send data: {err}");
                                    }
                                }

                                store.add_ack_offset(size).await;
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
            eprintln!("Channel closed. Stop reading IncomingMessage from {addr}");
        });

        Ok(())
    }
}

async fn ping(ws: &mut OwnedWriteHalf, rx: &mut Receiver<IncomingMessage>) -> RedisResult<()> {
    let msg = vec!["PING".to_string()];
    send_resp(ws, msg).await?;
    let recv = rx
        .recv()
        .await
        .expect("Error expected receiving PONG after sending PING");
    println!("Received! PING response: {recv}");
    Ok(())
}

async fn repl_conf(
    ws: &mut OwnedWriteHalf,
    rx: &mut Receiver<IncomingMessage>,
    port: u16,
) -> RedisResult<()> {
    let msg = vec![
        "REPLCONF".to_string(),
        "listening-port".to_string(),
        format!("{port}"),
    ];
    send_resp(ws, msg).await?;
    let recv = rx
        .recv()
        .await
        .expect("Error expected receiving OK after sending REPLCONF listening-port");
    println!("Received! REPLCONF listening-port response: {recv}");

    let msg = vec![
        "REPLCONF".to_string(),
        "capa".to_string(),
        "psync2".to_string(),
    ];
    send_resp(ws, msg).await?;
    let recv = rx
        .recv()
        .await
        .expect("Error expected receiving OK after sending REPLCONF capa");
    println!("Received! REPLCONF capa response: {recv}");

    Ok(())
}

async fn psync(ws: &mut OwnedWriteHalf, rx: &mut Receiver<IncomingMessage>) -> RedisResult<()> {
    let msg = vec!["PSYNC".to_string(), "?".to_string(), "-1".to_string()];
    send_resp(ws, msg).await?;
    let recv = rx
        .recv()
        .await
        .expect("Error expected receiving FULLRESYNC after sending PSYNC");
    println!("Received! PSYNC response: {recv}");
    Ok(())
}

async fn send_resp(ws: &mut OwnedWriteHalf, msg: Vec<String>) -> RedisResult<()> {
    ws.write_all(&Resp::from(msg).serialize()).await?;
    Ok(())
}
