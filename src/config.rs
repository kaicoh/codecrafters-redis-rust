use std::net::{IpAddr, Ipv4Addr, SocketAddr, ToSocketAddrs};

#[derive(Debug, Clone)]
pub struct Config {
    pub dir: Option<String>,
    pub dbfilename: Option<String>,
    pub port: u16,
    pub master: Option<SocketAddr>,
}

impl Config {
    pub fn new(args: Vec<String>) -> Self {
        Self {
            dir: get_arg(&args, "--dir"),
            dbfilename: get_arg(&args, "--dbfilename"),
            port: get_arg(&args, "--port")
                .and_then(|v| v.parse::<u16>().ok())
                .unwrap_or(6379),
            master: get_arg(&args, "--replicaof")
                .and_then(|v| v.replace(" ", ":").to_socket_addrs().ok())
                .and_then(|mut v| v.next()),
        }
    }

    pub fn socket_addr(&self) -> SocketAddr {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), self.port)
    }
}

fn get_arg(args: &[String], opt: &str) -> Option<String> {
    args.iter()
        .position(|v| v.as_str() == opt)
        .and_then(|pos| args.get(pos + 1).cloned())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_gets_arg() {
        let args: Vec<String> = vec![
            "bin".into(),
            "--dir".into(),
            "/tmp/redis-data".into(),
            "--dbfilename".into(),
            "dump.rdb".into(),
        ];

        let dir = get_arg(&args, "--dir");
        assert_eq!(dir, Some("/tmp/redis-data".into()));

        let dbfilename = get_arg(&args, "--dbfilename");
        assert_eq!(dbfilename, Some("dump.rdb".into()));
    }
}
