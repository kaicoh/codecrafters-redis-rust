use super::{RedisError, RedisResult};
use std::sync::{Arc, Mutex, MutexGuard};

#[derive(Debug, Clone)]
pub struct Config {
    inner: Arc<Mutex<RedisConfig>>,
}

impl Config {
    pub fn new(args: Vec<String>) -> Self {
        Self {
            inner: Arc::new(Mutex::new(RedisConfig::new(args))),
        }
    }

    pub fn dir(&self) -> RedisResult<Option<String>> {
        self.lock().map(|inner| inner.dir.clone())
    }

    pub fn dbfilename(&self) -> RedisResult<Option<String>> {
        self.lock().map(|inner| inner.dbfilename.clone())
    }

    fn lock(&self) -> RedisResult<MutexGuard<RedisConfig>> {
        self.inner
            .lock()
            .map_err(|e| RedisError::Lock(format!("{e}")))
    }
}

#[derive(Debug, Clone)]
struct RedisConfig {
    dir: Option<String>,
    dbfilename: Option<String>,
}

impl RedisConfig {
    fn new(args: Vec<String>) -> Self {
        Self {
            dir: get_arg(&args, "--dir"),
            dbfilename: get_arg(&args, "--dbfilename"),
        }
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
