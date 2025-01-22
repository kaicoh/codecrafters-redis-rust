use redis_starter_rust as rss;
use rss::{replica, Command, Config, IncomingMessage, Resp, Store, BUF_SIZE};
use std::env;
use std::io::Read;
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
use std::thread;

fn main() {
    let args: Vec<String> = env::args().collect();
    let config = Config::new(args);

    let master_stream = replica::connect_master(&config)
        .inspect_err(|err| eprintln!("ERROR: connecting master. {err}"))
        .unwrap();

    serve(config, master_stream);
}

fn serve(config: Config, master_stream: Option<TcpStream>) {
    let listener = TcpListener::bind(config.socket_addr()).unwrap();

    let store = Arc::new(
        Store::new(config)
            .inspect_err(|err| eprintln!("Failed to instantiate store: {err}"))
            .unwrap(),
    );

    if let Some(stream) = master_stream {
        handle_stream(stream, &store);
    }

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                handle_stream(stream, &store);
            }
            Err(err) => {
                eprintln!("Failed to get TCP stream: {err}");
            }
        }
    }
}

fn handle_stream(mut stream: TcpStream, store: &Arc<Store>) {
    let store = Arc::clone(store);

    thread::spawn(move || {
        let mut buf = [0; BUF_SIZE];

        while let Ok(n) = stream.read(&mut buf) {
            if n > 0 {
                let store = Arc::clone(&store);

                match IncomingMessage::new(&buf[..n]) {
                    Ok(IncomingMessage::Resp(resps)) => {
                        let commands = resps.into_iter().filter_map(|resp| {
                            Command::new(resp)
                                .inspect_err(|err| {
                                    eprintln!("Failed to get command from RESP message. {err}")
                                })
                                .ok()
                        });

                        for cmd in commands {
                            if cmd.store_connection() {
                                if let Err(err) = store.save_replica_stream(&stream) {
                                    eprintln!("Failed to save replica connection. {err}");
                                }
                            }

                            let store = Arc::clone(&store);

                            let msg = cmd.run(store).unwrap_or_else(|err| {
                                eprintln!("Failed to run command: {err}");
                                Resp::from(err).into()
                            });

                            if let Err(err) = msg.write_to(&mut stream) {
                                eprintln!("Failed to write TCP stream: {err}");
                            }
                        }
                    }
                    Ok(IncomingMessage::Rdb(_)) => {
                        println!("Received RDB file!");
                    }
                    Ok(IncomingMessage::Unknown(bytes)) => {
                        eprintln!(
                            "Received unknown message: {}",
                            String::from_utf8_lossy(&bytes)
                        )
                    }
                    Err(err) => {
                        eprintln!("Failed to parse incoming message: {err}");
                    }
                }
            }

            buf = [0; BUF_SIZE];
        }
    });
}
