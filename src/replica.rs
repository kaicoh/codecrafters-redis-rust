use super::{Config, Resp, BUF_SIZE};
use std::io::{self, Read, Write};
use std::net::TcpStream;

pub fn connect_master(config: &Config) -> io::Result<Option<TcpStream>> {
    match config.master_addr() {
        Some(addr) => {
            let mut stream = TcpStream::connect(addr)?;
            handshake(&mut stream, config.port)?;
            Ok(Some(stream))
        }
        None => Ok(None),
    }
}

fn handshake(stream: &mut TcpStream, port: u16) -> io::Result<()> {
    send_ping(stream)?;
    send_replconf(stream, port)?;
    send_psync(stream)
}

fn send_ping(stream: &mut TcpStream) -> io::Result<()> {
    let msg = vec!["PING".to_string()];
    send_resp(stream, msg)
}

fn send_replconf(stream: &mut TcpStream, port: u16) -> io::Result<()> {
    let msg = vec![
        "REPLCONF".to_string(),
        "listening-port".to_string(),
        format!("{port}"),
    ];
    send_resp(stream, msg)?;

    let msg = vec![
        "REPLCONF".to_string(),
        "capa".to_string(),
        "psync2".to_string(),
    ];
    send_resp(stream, msg)
}

fn send_psync(stream: &mut TcpStream) -> io::Result<()> {
    let msg = vec!["PSYNC".to_string(), "?".to_string(), "-1".to_string()];
    send_resp(stream, msg)
}

fn send_resp(stream: &mut TcpStream, args: Vec<String>) -> io::Result<()> {
    stream.write_all(&Resp::from(args).serialize())?;

    let mut buf = [0; BUF_SIZE];
    let size = stream.read(&mut buf[..])?;

    if size > 0 {
        match Resp::new(&buf[..size]) {
            Ok(res) => println!("Received from master: {res}"),
            Err(err) => eprintln!("Failed to get RESP message from the response: {err}"),
        }
    }

    Ok(())
}
