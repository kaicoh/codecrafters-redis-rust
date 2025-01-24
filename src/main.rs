use redis_starter_rust as rss;
use rss::{CommandMode, Config, Connection, RedisResult, Store};
use std::env;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};

#[tokio::main]
async fn main() {
    let args: Vec<String> = env::args().collect();
    let config = Config::new(args);

    if let Err(err) = serve(config).await {
        eprintln!("{err}");
    }
}

async fn serve(config: Config) -> RedisResult<()> {
    let listener = TcpListener::bind(config.socket_addr()).await?;
    let store = Arc::new(Store::new(&config)?);

    if let Some(addr) = config.master_addr() {
        let stream = TcpStream::connect(addr).await?;
        let conn = Connection::new(stream, CommandMode::Sync);
        conn.start_streaming(&store).await?;
    }

    while let Ok((stream, _)) = listener.accept().await {
        let conn = Connection::new(stream, CommandMode::Normal);
        conn.start_streaming(&store).await?;
    }

    Ok(())
}
