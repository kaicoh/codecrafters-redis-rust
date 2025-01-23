use redis_starter_rust as rss;
use rss::{CommandMode, Config, Connection, RedisResult, Store};
use std::env;
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;

fn main() {
    let args: Vec<String> = env::args().collect();
    let config = Config::new(args);

    if let Err(err) = serve(config) {
        eprintln!("{err}");
    }
}

fn serve(config: Config) -> RedisResult<()> {
    let listener = TcpListener::bind(config.socket_addr())?;
    let store = Arc::new(Store::new(&config)?);

    if let Some(addr) = config.master_addr() {
        let stream = TcpStream::connect(addr)?;
        let conn = Connection::new(stream, CommandMode::Sync);
        conn.start_streaming(&store)?;
    }

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                let conn = Connection::new(stream, CommandMode::Normal);
                conn.start_streaming(&store)?;
            }
            Err(err) => {
                eprintln!("Failed to get TCP stream: {err}");
            }
        }
    }

    Ok(())
}
