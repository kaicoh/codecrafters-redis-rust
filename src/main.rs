use redis_starter_rust as rss;
use rss::{replica, Command, Config, IncomingMessage, RedisResult, Resp, Store, BUF_SIZE};
use std::env;
use std::io::{Read, Write};
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

                let commands = match get_commands(&buf[..n]) {
                    Ok(commands) => commands,
                    Err(err) => {
                        eprintln!("Failed to get commands from incoming message. {err}");
                        eprintln!("Incoming message: {:?}", &buf[..n]);
                        continue;
                    }
                };

                for cmd in commands {
                    if cmd.store_connection() {
                        if let Err(err) = store.save_replica_stream(&stream) {
                            eprintln!("Failed to save replica connection. {err}");
                        }
                    }

                    let store = Arc::clone(&store);

                    let responses = cmd.run(store).unwrap_or_else(|err| {
                        eprintln!("Failed to run command: {err}");
                        vec![Resp::from(err).into()]
                    });

                    for res in responses {
                        if let Err(err) = stream.write_all(res.as_bytes()) {
                            eprintln!("Failed to write TCP stream: {err}");
                        }
                    }
                }
            }

            buf = [0; BUF_SIZE];
        }
    });
}

fn get_commands(buf: &[u8]) -> RedisResult<Vec<Command>> {
    let mut commands: Vec<Command> = vec![];
    let messages = IncomingMessage::new(buf)?;

    for resp in messages.into_iter() {
        let command = Command::new(resp)?;
        commands.push(command);
    }

    Ok(commands)
}
