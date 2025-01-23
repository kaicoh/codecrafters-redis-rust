use super::{
    enc::{EncSize, EncString},
    utils, RedisResult,
};
use std::io::{ErrorKind, Read};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[derive(Debug, PartialEq)]
pub enum RdbElement {
    Header(String),
    Meta {
        key: String,
        value: String,
    },
    DbIndex(usize),
    HashTableSize {
        entries: usize,
        expires: usize,
    },
    HashTableEntry {
        key: String,
        value: String,
        exp: Option<SystemTime>,
    },
    Checksum([u8; 8]),
}

#[derive(Debug)]
pub struct RdbFile<R: Read> {
    inner: R,
    eof: bool,
}

impl<R: Read> RdbFile<R> {
    pub fn new(r: R) -> Self {
        Self {
            inner: r,
            eof: false,
        }
    }
}

impl<R: Read> Iterator for RdbFile<R> {
    type Item = RdbElement;

    fn next(&mut self) -> Option<Self::Item> {
        if self.eof {
            return None;
        }

        let mut one_byte = [0u8; 1];
        match self.inner.read_exact(&mut one_byte) {
            Ok(_) => match one_byte {
                [0x52] => read_header(&mut self.inner),
                [0xfa] => read_metadata(&mut self.inner),
                [0xfe] => read_db_index(&mut self.inner),
                [0xfb] => read_hash_size(&mut self.inner),
                [0x00] => read_hash_entry(&mut self.inner),
                [0xfc] => read_hash_entry_exp_millis(&mut self.inner),
                [0xfd] => read_hash_entry_exp_secs(&mut self.inner),
                [0xff] => {
                    self.eof = true;
                    read_eof(&mut self.inner)
                }
                _ => {
                    eprintln!("Unexpected first byte: {one_byte:#?}");
                    None
                }
            },
            Err(err) if err.kind() == ErrorKind::UnexpectedEof => None,
            Err(err) => {
                eprintln!("Failed to read rdb file: {err}");
                None
            }
        }
    }
}

fn read_header<R: Read>(r: &mut R) -> Option<RdbElement> {
    let mut buf = [0u8; 8];
    r.read_exact(&mut buf)
        .inspect_err(|err| eprintln!("Failed to read rdb header: {err}"))
        .ok()?;

    let mut header = [0u8; 9];
    header[0] = 0x52;
    header[1..].copy_from_slice(&buf);

    utils::stringify(&header)
        .inspect_err(|err| eprintln!("Failed to stringify rdb header: {err}"))
        .map(|v| RdbElement::Header(v.into()))
        .ok()
}

fn read_metadata<R: Read>(r: &mut R) -> Option<RdbElement> {
    let key = EncString::new(r)
        .inspect_err(|err| eprintln!("Failed to read rdb metadata key: {err}"))
        .ok()?
        .value()
        .to_string();
    let value = EncString::new(r)
        .inspect_err(|err| eprintln!("Failed to read rdb metadata value: {err}"))
        .ok()?
        .value()
        .to_string();
    Some(RdbElement::Meta { key, value })
}

fn read_db_index<R: Read>(r: &mut R) -> Option<RdbElement> {
    EncSize::new(r)
        .inspect_err(|err| eprintln!("Failed to read rdb database index value: {err}"))
        .ok()?
        .value()
        .map(RdbElement::DbIndex)
}

fn read_hash_size<R: Read>(r: &mut R) -> Option<RdbElement> {
    let entries = EncSize::new(r)
        .inspect_err(|err| eprintln!("Failed to read rdb hash table size (entries): {err}"))
        .ok()?
        .value()?;
    let expires = EncSize::new(r)
        .inspect_err(|err| eprintln!("Failed to read rdb hash table size (expires): {err}"))
        .ok()?
        .value()?;
    Some(RdbElement::HashTableSize { entries, expires })
}

fn read_hash_entry<R: Read>(r: &mut R) -> Option<RdbElement> {
    let key = EncString::new(r)
        .inspect_err(|err| eprintln!("Failed to read rdb hash table entry's key: {err}"))
        .ok()?
        .value()
        .to_string();
    let value = EncString::new(r)
        .inspect_err(|err| eprintln!("Failed to read rdb hash table entry's value: {err}"))
        .ok()?
        .value()
        .to_string();
    Some(RdbElement::HashTableEntry {
        key,
        value,
        exp: None,
    })
}

fn read_hash_entry_exp_millis<R: Read>(r: &mut R) -> Option<RdbElement> {
    let mut buf = [0u8; 8];
    r.read_exact(&mut buf)
        .inspect_err(|err| {
            eprintln!("Failed to read rdb hash table entry's expiry (milliseconds): {err}")
        })
        .ok()?;
    let exp = UNIX_EPOCH + Duration::from_millis(u64::from_le_bytes(buf));

    // Proceed to one byte to use `read_hash_entry` function
    if !next_one_byte(r).is_ok_and(|v| v == 0u8) {
        eprintln!("FC entry doesn't start with 0x00");
        return None;
    }

    if let Some(RdbElement::HashTableEntry { key, value, .. }) = read_hash_entry(r) {
        Some(RdbElement::HashTableEntry {
            key,
            value,
            exp: Some(exp),
        })
    } else {
        eprintln!("Failed to read rdb hash table entry");
        None
    }
}

fn read_hash_entry_exp_secs<R: Read>(r: &mut R) -> Option<RdbElement> {
    let mut buf = [0u8; 4];
    r.read_exact(&mut buf)
        .inspect_err(|err| {
            eprintln!("Failed to read rdb hash table entry's expiry (seconds): {err}")
        })
        .ok()?;
    let exp = UNIX_EPOCH + Duration::from_secs(u32::from_le_bytes(buf) as u64);

    // Proceed to one byte to use `read_hash_entry` function
    if !next_one_byte(r).is_ok_and(|v| v == 0u8) {
        eprintln!("FD entry doesn't start with 0x00");
        return None;
    }

    if let Some(RdbElement::HashTableEntry { key, value, .. }) = read_hash_entry(r) {
        Some(RdbElement::HashTableEntry {
            key,
            value,
            exp: Some(exp),
        })
    } else {
        eprintln!("Failed to read rdb hash table entry");
        None
    }
}

fn read_eof<R: Read>(r: &mut R) -> Option<RdbElement> {
    let mut buf = [0u8; 8];
    r.read_exact(&mut buf)
        .inspect_err(|err| eprintln!("Failed to read rdb checksum: {err}"))
        .ok()?;
    Some(RdbElement::Checksum(buf))
}

fn next_one_byte<R: Read>(r: &mut R) -> RedisResult<u8> {
    let mut buf = [0u8; 1];
    r.read_exact(&mut buf)?;
    let [byte] = buf;
    Ok(byte)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn it_iterates_whole_file() {
        let bytes = b"\x52\x45\x44\x49\x53\x30\x30\x31\x31\xfa\x09\x72\x65\x64\x69\x73\x2D\x76\x65\x72\x06\x36\x2E\x30\x2E\x31\x36\xfe\x00\xfb\x03\x02\x00\x06\x66\x6F\x6F\x62\x61\x72\x06\x62\x61\x7A\x71\x75\x78\xfc\x15\x72\xE7\x07\x8F\x01\x00\x00\x00\x03\x66\x6F\x6F\x03\x62\x61\x72\xfd\x52\xED\x2A\x66\x00\x03\x62\x61\x7A\x03\x71\x75\x78\xff\x89\x3b\xb7\x4e\xf8\x0f\x77\x19";
        let buf = Cursor::new(bytes);

        let mut f = RdbFile::new(buf);

        let expected = RdbElement::Header("REDIS0011".into());
        assert_eq!(f.next().unwrap(), expected);

        let expected = RdbElement::Meta {
            key: "redis-ver".into(),
            value: "6.0.16".into(),
        };
        assert_eq!(f.next().unwrap(), expected);

        let expected = RdbElement::DbIndex(0);
        assert_eq!(f.next().unwrap(), expected);

        let expected = RdbElement::HashTableSize {
            entries: 3,
            expires: 2,
        };
        assert_eq!(f.next().unwrap(), expected);

        let expected = RdbElement::HashTableEntry {
            key: "foobar".into(),
            value: "bazqux".into(),
            exp: None,
        };
        assert_eq!(f.next().unwrap(), expected);

        let expected = RdbElement::HashTableEntry {
            key: "foo".into(),
            value: "bar".into(),
            exp: Some(UNIX_EPOCH + Duration::from_millis(1713824559637)),
        };
        assert_eq!(f.next().unwrap(), expected);

        let expected = RdbElement::HashTableEntry {
            key: "baz".into(),
            value: "qux".into(),
            exp: Some(UNIX_EPOCH + Duration::from_secs(1714089298)),
        };
        assert_eq!(f.next().unwrap(), expected);

        let expected = RdbElement::Checksum([0x89, 0x3b, 0xb7, 0x4e, 0xf8, 0x0f, 0x77, 0x19]);
        assert_eq!(f.next().unwrap(), expected);

        assert_eq!(f.next(), None);
    }

    #[test]
    fn it_reads_header() {
        let bytes = b"\x45\x44\x49\x53\x30\x30\x31\x31";
        let mut buf = Cursor::new(bytes);

        let actual = read_header(&mut buf).unwrap();
        let expected = RdbElement::Header("REDIS0011".into());
        assert_eq!(actual, expected);
    }

    #[test]
    fn it_reads_metadata() {
        let bytes = b"\x09\x72\x65\x64\x69\x73\x2D\x76\x65\x72\x06\x36\x2E\x30\x2E\x31\x36";
        let mut buf = Cursor::new(bytes);

        let actual = read_metadata(&mut buf).unwrap();
        let expected = RdbElement::Meta {
            key: "redis-ver".into(),
            value: "6.0.16".into(),
        };
        assert_eq!(actual, expected);
    }

    #[test]
    fn it_reads_db_index() {
        let bytes = b"\x00";
        let mut buf = Cursor::new(bytes);

        let actual = read_db_index(&mut buf).unwrap();
        let expected = RdbElement::DbIndex(0);
        assert_eq!(actual, expected);
    }

    #[test]
    fn it_reads_hash_table_size() {
        let bytes = b"\x03\x02";
        let mut buf = Cursor::new(bytes);

        let actual = read_hash_size(&mut buf).unwrap();
        let expected = RdbElement::HashTableSize {
            entries: 3,
            expires: 2,
        };
        assert_eq!(actual, expected);
    }

    #[test]
    fn it_reads_hash_table_entry() {
        let bytes = b"\x06\x66\x6F\x6F\x62\x61\x72\x06\x62\x61\x7A\x71\x75\x78";
        let mut buf = Cursor::new(bytes);

        let actual = read_hash_entry(&mut buf).unwrap();
        let expected = RdbElement::HashTableEntry {
            key: "foobar".into(),
            value: "bazqux".into(),
            exp: None,
        };
        assert_eq!(actual, expected);
    }

    #[test]
    fn it_reads_hash_table_entry_exp_millis() {
        let bytes = b"\x15\x72\xE7\x07\x8F\x01\x00\x00\x00\x03\x66\x6F\x6F\x03\x62\x61\x72";
        let mut buf = Cursor::new(bytes);

        let actual = read_hash_entry_exp_millis(&mut buf).unwrap();
        let expected = RdbElement::HashTableEntry {
            key: "foo".into(),
            value: "bar".into(),
            exp: Some(UNIX_EPOCH + Duration::from_millis(1713824559637)),
        };
        assert_eq!(actual, expected);
    }

    #[test]
    fn it_reads_hash_table_entry_exp_secs() {
        let bytes = b"\x52\xED\x2A\x66\x00\x03\x62\x61\x7A\x03\x71\x75\x78";
        let mut buf = Cursor::new(bytes);

        let actual = read_hash_entry_exp_secs(&mut buf).unwrap();
        let expected = RdbElement::HashTableEntry {
            key: "baz".into(),
            value: "qux".into(),
            exp: Some(UNIX_EPOCH + Duration::from_secs(1714089298)),
        };
        assert_eq!(actual, expected);
    }

    #[test]
    fn it_reads_eof() {
        let bytes = b"\x89\x3b\xb7\x4e\xf8\x0f\x77\x19";
        let mut buf = Cursor::new(bytes);

        let actual = read_eof(&mut buf).unwrap();
        let expected = RdbElement::Checksum([0x89, 0x3b, 0xb7, 0x4e, 0xf8, 0x0f, 0x77, 0x19]);
        assert_eq!(actual, expected);
    }
}
