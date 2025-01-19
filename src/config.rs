#[derive(Debug, Clone)]
pub struct Config {
    pub dir: Option<String>,
    pub dbfilename: Option<String>,
}

impl Config {
    pub fn new(args: Vec<String>) -> Self {
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
