mod iterator;

use super::{utils, RedisError, RedisResult};
use iterator::RespToken;
use std::fmt;

const TERM: &str = "\r\n";

#[derive(Debug, Clone, PartialEq)]
pub enum Resp {
    /// SimpleString
    SS(String),
    /// SimpleError
    SE(String),
    /// BulkString
    BS(Option<String>),
    /// Array
    A(Vec<Resp>),
}

impl fmt::Display for Resp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::SS(val) => write!(f, "{val}"),
            Self::SE(val) => write!(f, "{val}"),
            Self::BS(Some(val)) => write!(f, "{val}"),
            Self::BS(None) => write!(f, ""),
            Self::A(els) => {
                let els = els
                    .iter()
                    .map(|el| format!("{el}"))
                    .collect::<Vec<String>>()
                    .join(", ");
                write!(f, "[{els}]")
            }
        }
    }
}

impl Resp {
    pub fn new(buf: &[u8]) -> RedisResult<Self> {
        let mut tokens = RespToken::new(buf);
        Self::from_tokens(&mut tokens)
    }

    pub fn serialize(&self) -> Vec<u8> {
        match self {
            Self::SS(val) => format!("+{val}{TERM}").into_bytes(),
            Self::SE(val) => format!("-{val}{TERM}").into_bytes(),
            Self::BS(Some(val)) => format!("${}{TERM}{val}{TERM}", val.len()).into_bytes(),
            Self::BS(None) => format!("$-1{TERM}").into_bytes(),
            Self::A(vals) => {
                let len = vals.len();
                let elements = vals.iter().flat_map(Self::serialize);
                format!("*{len}{TERM}")
                    .into_bytes()
                    .into_iter()
                    .chain(elements)
                    .collect()
            }
        }
    }

    fn from_tokens(tokens: &mut RespToken<'_>) -> RedisResult<Self> {
        match tokens.next() {
            Some(token) if token.starts_with(b"+") => {
                let val = utils::stringify(&token[1..])?;
                Ok(Self::SS(val.into()))
            }
            Some(token) if token.starts_with(b"-") => {
                let val = utils::stringify(&token[1..])?;
                Ok(Self::SE(val.into()))
            }
            Some(token) if token == b"$-1" => Ok(Self::BS(None)),
            Some(token) if token.starts_with(b"$") => {
                let len = utils::parse_usize(&token[1..])?;
                tokens
                    .next()
                    .ok_or(RedisError::RespSyntax)
                    .and_then(|v| utils::stringify(&v[..len]))
                    .map(|v| Self::BS(Some(v.into())))
            }
            Some(token) if token.starts_with(b"*") => {
                let len = utils::parse_usize(&token[1..])?;
                let mut elements: Vec<Self> = vec![];

                for _ in 0..len {
                    let element = Self::from_tokens(tokens)?;
                    elements.push(element);
                }

                Ok(Self::A(elements))
            }
            _ => Err(RedisError::RespSyntax),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_parses_into_simple_string() {
        let bytes = b"+OK\r\n";
        let actual = Resp::new(bytes).unwrap();
        let expected = Resp::SS("OK".into());
        assert_eq!(actual, expected);
    }

    #[test]
    fn it_parses_into_simple_error() {
        let bytes = b"-ERR unknown command 'asdf'\r\n";
        let actual = Resp::new(bytes).unwrap();
        let expected = Resp::SE("ERR unknown command 'asdf'".into());
        assert_eq!(actual, expected);
    }

    #[test]
    fn it_parses_into_bulk_string() {
        let bytes = b"$5\r\nhello\r\n";
        let actual = Resp::new(bytes).unwrap();
        let expected = Resp::BS(Some("hello".into()));
        assert_eq!(actual, expected);

        let bytes = b"$0\r\n\r\n";
        let actual = Resp::new(bytes).unwrap();
        let expected = Resp::BS(Some("".into()));
        assert_eq!(actual, expected);

        let bytes = b"$-1\r\n";
        let actual = Resp::new(bytes).unwrap();
        let expected = Resp::BS(None);
        assert_eq!(actual, expected);
    }

    #[test]
    fn it_parses_into_array() {
        let bytes = b"*0\r\n";
        let actual = Resp::new(bytes).unwrap();
        let expected = Resp::A(vec![]);
        assert_eq!(actual, expected);

        let bytes = b"*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n";
        let actual = Resp::new(bytes).unwrap();
        let expected = Resp::A(vec![
            Resp::BS(Some("hello".into())),
            Resp::BS(Some("world".into())),
        ]);
        assert_eq!(actual, expected);

        let bytes = b"*2\r\n*3\r\n+one\r\n+two\r\n+three\r\n*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n";
        let actual = Resp::new(bytes).unwrap();
        let expected = Resp::A(vec![
            Resp::A(vec![
                Resp::SS("one".into()),
                Resp::SS("two".into()),
                Resp::SS("three".into()),
            ]),
            Resp::A(vec![
                Resp::BS(Some("hello".into())),
                Resp::BS(Some("world".into())),
            ]),
        ]);
        assert_eq!(actual, expected);
    }

    #[test]
    fn it_serializes_into_simple_string() {
        let val = Resp::SS("OK".into());
        let actual = val.serialize();
        let expected = b"+OK\r\n";
        assert_eq!(actual, expected);
    }

    #[test]
    fn it_serializes_into_simple_error() {
        let val = Resp::SE("ERR unknown command 'asdf'".into());
        let actual = val.serialize();
        let expected = b"-ERR unknown command 'asdf'\r\n";
        assert_eq!(actual, expected);
    }

    #[test]
    fn it_serializes_into_bulk_string() {
        let val = Resp::BS(Some("hello".into()));
        let actual = val.serialize();
        let expected = b"$5\r\nhello\r\n";
        assert_eq!(actual, expected);

        let val = Resp::BS(Some("".into()));
        let actual = val.serialize();
        let expected = b"$0\r\n\r\n";
        assert_eq!(actual, expected);

        let val = Resp::BS(None);
        let actual = val.serialize();
        let expected = b"$-1\r\n";
        assert_eq!(actual, expected);
    }

    #[test]
    fn it_serializes_into_array() {
        let val = Resp::A(vec![]);
        let actual = val.serialize();
        let expected = b"*0\r\n";
        assert_eq!(actual, expected);

        let val = Resp::A(vec![
            Resp::BS(Some("hello".into())),
            Resp::BS(Some("world".into())),
        ]);
        let actual = val.serialize();
        let expected = b"*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n";
        assert_eq!(actual, expected);

        let val = Resp::A(vec![
            Resp::A(vec![
                Resp::SS("one".into()),
                Resp::SS("two".into()),
                Resp::SS("three".into()),
            ]),
            Resp::A(vec![
                Resp::BS(Some("hello".into())),
                Resp::BS(Some("world".into())),
            ]),
        ]);
        let actual = val.serialize();
        let expected =
            b"*2\r\n*3\r\n+one\r\n+two\r\n+three\r\n*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n";
        assert_eq!(actual, expected);
    }
}
