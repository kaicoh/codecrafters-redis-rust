use redis_starter_rust as rss;
use rss::{Command, Config, Resp, Store};
use std::env;
use std::io::{self, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
use std::thread;

const BUF_SIZE: usize = 1024;

fn main() {
    let args: Vec<String> = env::args().collect();
    let config = Config::new(args);

    if let Err(err) = connect_master(&config) {
        eprintln!("ERROR: connecting master: {err}");
        std::process::exit(1);
    }

    serve(config);
}

fn connect_master(config: &Config) -> io::Result<()> {
    if let Some(&addr) = config.master_addr().as_ref() {
        let mut stream = TcpStream::connect(addr)?;

        // Send PING
        let msg = Resp::A(vec![Resp::BS(Some("PING".into()))]);
        send_to_master(&mut stream, msg)?;

        // Send REPLCONF listening-port
        let msg = Resp::A(vec![
            Resp::BS(Some("REPLCONF".into())),
            Resp::BS(Some("listening-port".into())),
            Resp::BS(Some(format!("{}", config.port))),
        ]);
        send_to_master(&mut stream, msg)?;

        // Send REPLCONF capa
        let msg = Resp::A(vec![
            Resp::BS(Some("REPLCONF".into())),
            Resp::BS(Some("capa".into())),
            Resp::BS(Some("psync2".into())),
        ]);
        send_to_master(&mut stream, msg)?;

        // Send PSYNC
        let msg = Resp::A(vec![
            Resp::BS(Some("PSYNC".into())),
            Resp::BS(Some("?".into())),
            Resp::BS(Some("-1".into())),
        ]);
        send_to_master(&mut stream, msg)?;
    }
    Ok(())
}

fn serve(config: Config) {
    let listener = TcpListener::bind(config.socket_addr()).unwrap();

    let store = Arc::new(
        Store::new(config)
            .inspect_err(|err| eprintln!("Failed to instantiate store: {err}"))
            .unwrap(),
    );

    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
                let store = Arc::clone(&store);

                thread::spawn(move || {
                    let mut buf = [0; BUF_SIZE];

                    while let Ok(n) = stream.read(&mut buf) {
                        if n > 0 {
                            let res = match Command::new(&buf[..n]) {
                                Ok(cmd) => cmd.run(Arc::clone(&store)).unwrap_or_else(|err| {
                                    eprintln!("Failed to run command: {err}");
                                    Resp::from(err)
                                }),
                                Err(err) => {
                                    eprintln!("Failed to parse command: {err}");
                                    Resp::from(err)
                                }
                            };

                            if let Err(err) = stream.write_all(&res.serialize()) {
                                eprintln!("Failed to write TCP stream: {err}");
                            }
                        }

                        buf = [0; BUF_SIZE];
                    }
                });
            }
            Err(err) => {
                eprintln!("Failed to get TCP stream: {err}");
            }
        }
    }
}

fn send_to_master(stream: &mut TcpStream, msg: Resp) -> io::Result<()> {
    stream.write_all(&msg.serialize())?;

    let mut buf = [0; BUF_SIZE];
    let n = stream.read(&mut buf[..])?;

    match Resp::new(&buf[..n]) {
        Ok(res) => println!("Received from master: {res}"),
        Err(err) => eprintln!("Failed to get RESP message from the response: {err}"),
    }

    Ok(())
}
