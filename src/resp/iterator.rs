use super::TERM;
use std::io::{Cursor, Seek, SeekFrom};

#[derive(Debug)]
pub(crate) struct RespToken<'a> {
    cursor: Cursor<&'a [u8]>,
}

impl<'a> RespToken<'a> {
    pub(crate) fn new(buf: &'a [u8]) -> Self {
        Self {
            cursor: Cursor::new(buf),
        }
    }
}

impl<'a> Iterator for RespToken<'a> {
    type Item = &'a [u8];

    fn next(&mut self) -> Option<Self::Item> {
        let current_pos = self.cursor.position() as usize;
        let bytes = *(self.cursor.get_ref());

        let (_, rest) = bytes.split_at_checked(current_pos)?;

        if rest.is_empty() {
            return None;
        }

        let msg_size = rest.windows(2).position(|chars| chars == TERM.as_bytes())?;

        let next_pos: i64 = (msg_size + 2)
            .try_into()
            .inspect_err(|e| eprintln!("Error! parsing new position: {e}"))
            .ok()?;

        self.cursor
            .seek(SeekFrom::Current(next_pos))
            .inspect_err(|e| eprintln!("Error! seeking next position: {e}"))
            .ok()?;

        Some(&bytes[current_pos..(current_pos + msg_size)])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_bytes() {
        let bytes = b"";
        let mut splitter = RespToken::new(bytes);

        let item = splitter.next();
        assert_eq!(item, None);
    }

    #[test]
    fn no_terminator() {
        let bytes = b"one";
        let mut splitter = RespToken::new(bytes);

        let item = splitter.next();
        assert_eq!(item, None);
    }

    #[test]
    fn end_with_terminator() {
        let bytes = b"one\r\n";
        let mut splitter = RespToken::new(bytes);

        let item = splitter.next();
        assert_eq!(item, Some(b"one" as &[u8]));
        let item = splitter.next();
        assert_eq!(item, None);
    }

    #[test]
    fn one_terminator_without_end() {
        let bytes = b"one\r\ntwo";
        let mut splitter = RespToken::new(bytes);

        let item = splitter.next();
        assert_eq!(item, Some(b"one" as &[u8]));
        let item = splitter.next();
        assert_eq!(item, None);
    }

    #[test]
    fn one_terminator() {
        let bytes = b"one\r\ntwo\r\n";
        let mut splitter = RespToken::new(bytes);

        let item = splitter.next();
        assert_eq!(item, Some(b"one" as &[u8]));
        let item = splitter.next();
        assert_eq!(item, Some(b"two" as &[u8]));
        let item = splitter.next();
        assert_eq!(item, None);
    }

    #[test]
    fn multiple_terminators_without_end() {
        let bytes = b"one\r\ntwo\r\nthree\r\nfour";
        let mut splitter = RespToken::new(bytes);

        let item = splitter.next();
        assert_eq!(item, Some(b"one" as &[u8]));
        let item = splitter.next();
        assert_eq!(item, Some(b"two" as &[u8]));
        let item = splitter.next();
        assert_eq!(item, Some(b"three" as &[u8]));
        let item = splitter.next();
        assert_eq!(item, None);
    }

    #[test]
    fn multiple_terminators() {
        let bytes = b"one\r\ntwo\r\nthree\r\nfour\r\n";
        let mut splitter = RespToken::new(bytes);

        let item = splitter.next();
        assert_eq!(item, Some(b"one" as &[u8]));
        let item = splitter.next();
        assert_eq!(item, Some(b"two" as &[u8]));
        let item = splitter.next();
        assert_eq!(item, Some(b"three" as &[u8]));
        let item = splitter.next();
        assert_eq!(item, Some(b"four" as &[u8]));
        let item = splitter.next();
        assert_eq!(item, None);
    }
}
