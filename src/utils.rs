use super::{RedisError, RedisResult};
use std::io::{Cursor, Seek, SeekFrom};

pub(crate) fn stringify(buf: &[u8]) -> RedisResult<&str> {
    std::str::from_utf8(buf).map_err(RedisError::from)
}

pub(crate) fn parse_usize(buf: &[u8]) -> RedisResult<usize> {
    stringify(buf)?.parse().map_err(RedisError::from)
}

pub(crate) const TERM: &str = "\r\n";

#[derive(Debug)]
pub(crate) struct Tokens<'a> {
    cursor: Cursor<&'a [u8]>,
}

impl<'a> Tokens<'a> {
    pub(crate) fn new(buf: &'a [u8]) -> Self {
        Self {
            cursor: Cursor::new(buf),
        }
    }

    pub(crate) fn proceed(&mut self, len: usize) -> Option<&'a [u8]> {
        let current = self.current_position();
        let bytes = *(self.cursor.get_ref());
        let buf_size = bytes.len();

        let len = std::cmp::min(len, buf_size - current);
        seek(&mut self.cursor, len)?;
        Some(&bytes[current..current + len])
    }

    pub(crate) fn starts_with(&self, bytes: &[u8]) -> bool {
        if self.finished() {
            false
        } else {
            let current = self.current_position();
            let len = bytes.len();
            *bytes == self.buf()[current..current + len]
        }
    }

    pub(crate) fn finished(&self) -> bool {
        self.current_position() >= self.buf().len()
    }

    fn current_position(&self) -> usize {
        self.cursor.position() as usize
    }

    fn buf(&self) -> &[u8] {
        self.cursor.get_ref()
    }
}

impl<'a> Iterator for Tokens<'a> {
    type Item = &'a [u8];

    fn next(&mut self) -> Option<Self::Item> {
        if self.finished() {
            return None;
        }

        let current_pos = self.current_position();
        let bytes = *(self.cursor.get_ref());

        let len_to_next_term = &bytes[current_pos..]
            .windows(TERM.len())
            .position(|nums| nums == TERM.as_bytes());

        match len_to_next_term {
            Some(msg_size) => {
                seek(&mut self.cursor, msg_size + 2)?;
                Some(&bytes[current_pos..(current_pos + msg_size)])
            }
            None => {
                seek(&mut self.cursor, bytes.len() - current_pos)?;
                Some(&bytes[current_pos..])
            }
        }
    }
}

fn seek<T: AsRef<[u8]>>(cursor: &mut Cursor<T>, len: usize) -> Option<()> {
    let next_pos = len
        .try_into()
        .inspect_err(|err| eprintln!("Parsing error. {err}"))
        .ok()?;
    cursor
        .seek(SeekFrom::Current(next_pos))
        .inspect_err(|err| eprintln!("Seek error: {err}"))
        .map(drop)
        .ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_bytes() {
        let bytes = b"";
        let mut tokens = Tokens::new(bytes);

        let item = tokens.next();
        assert_eq!(item, None);
    }

    #[test]
    fn no_terminator() {
        let bytes = b"one";
        let mut tokens = Tokens::new(bytes);

        let item = tokens.next();
        assert_eq!(item, Some(b"one" as &[u8]));
        let item = tokens.next();
        assert_eq!(item, None);
    }

    #[test]
    fn end_with_terminator() {
        let bytes = b"one\r\n";
        let mut tokens = Tokens::new(bytes);

        let item = tokens.next();
        assert_eq!(item, Some(b"one" as &[u8]));
        let item = tokens.next();
        assert_eq!(item, None);
    }

    #[test]
    fn one_terminator_without_end() {
        let bytes = b"one\r\ntwo";
        let mut tokens = Tokens::new(bytes);

        let item = tokens.next();
        assert_eq!(item, Some(b"one" as &[u8]));
        let item = tokens.next();
        assert_eq!(item, Some(b"two" as &[u8]));
        let item = tokens.next();
        assert_eq!(item, None);
    }

    #[test]
    fn one_terminator() {
        let bytes = b"one\r\ntwo\r\n";
        let mut tokens = Tokens::new(bytes);

        let item = tokens.next();
        assert_eq!(item, Some(b"one" as &[u8]));
        let item = tokens.next();
        assert_eq!(item, Some(b"two" as &[u8]));
        let item = tokens.next();
        assert_eq!(item, None);
    }

    #[test]
    fn multiple_terminators_without_end() {
        let bytes = b"one\r\ntwo\r\nthree\r\nfour";
        let mut tokens = Tokens::new(bytes);

        let item = tokens.next();
        assert_eq!(item, Some(b"one" as &[u8]));
        let item = tokens.next();
        assert_eq!(item, Some(b"two" as &[u8]));
        let item = tokens.next();
        assert_eq!(item, Some(b"three" as &[u8]));
        let item = tokens.next();
        assert_eq!(item, Some(b"four" as &[u8]));
        let item = tokens.next();
        assert_eq!(item, None);
    }

    #[test]
    fn multiple_terminators() {
        let bytes = b"one\r\ntwo\r\nthree\r\nfour\r\n";
        let mut tokens = Tokens::new(bytes);

        let item = tokens.next();
        assert_eq!(item, Some(b"one" as &[u8]));
        let item = tokens.next();
        assert_eq!(item, Some(b"two" as &[u8]));
        let item = tokens.next();
        assert_eq!(item, Some(b"three" as &[u8]));
        let item = tokens.next();
        assert_eq!(item, Some(b"four" as &[u8]));
        let item = tokens.next();
        assert_eq!(item, None);
    }

    #[test]
    fn check_finished_with_trailing_terminator() {
        let bytes = b"one\r\ntwo\r\n";
        let mut tokens = Tokens::new(bytes);

        assert!(!tokens.finished());
        let item = tokens.next();
        assert_eq!(item, Some(b"one" as &[u8]));

        assert!(!tokens.finished());
        let item = tokens.next();
        assert_eq!(item, Some(b"two" as &[u8]));

        assert!(tokens.finished());
        let item = tokens.next();
        assert_eq!(item, None);
    }

    #[test]
    fn check_finished_without_trailing_terminator() {
        let bytes = b"one\r\ntwo";
        let mut tokens = Tokens::new(bytes);

        assert!(!tokens.finished());
        let item = tokens.next();
        assert_eq!(item, Some(b"one" as &[u8]));

        assert!(!tokens.finished());
        let item = tokens.next();
        assert_eq!(item, Some(b"two" as &[u8]));

        assert!(tokens.finished());
        let item = tokens.next();
        assert_eq!(item, None);
    }

    #[test]
    fn it_checks_starts() {
        let bytes = b"one\r\ntwo";
        let mut tokens = Tokens::new(bytes);

        assert!(tokens.starts_with(b"o"));

        let _ = tokens.next();

        assert!(tokens.starts_with(b"t"));
    }
}
