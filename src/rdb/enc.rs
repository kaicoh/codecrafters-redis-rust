use super::{utils, RedisError, RedisResult};
use std::io::Read;

const MASK_FIRST_TWO: u8 = 0b11000000;
const MASK_LAST_SIX: u8 = 0b00111111;

#[derive(Debug, PartialEq)]
pub(crate) enum EncSize {
    Integer(usize),
    String(String),
}

impl EncSize {
    pub(crate) fn new<R: Read>(r: &mut R) -> RedisResult<Self> {
        let mut one_byte = [0u8; 1];
        r.read_exact(&mut one_byte)?;

        let [byte0] = one_byte;
        match byte0 & MASK_FIRST_TWO {
            0b00000000 => Ok(Self::Integer(size_0b00(byte0))),
            0b01000000 => {
                let mut buf = [0u8; 1];
                r.read_exact(&mut buf)?;
                let [byte1] = buf;
                Ok(Self::Integer(size_0b01([byte0, byte1])))
            }
            0b10000000 => {
                let mut buf = [0u8; 4];
                r.read_exact(&mut buf)?;
                let [byte1, byte2, byte3, byte4] = buf;
                Ok(Self::Integer(size_0b10([
                    byte0, byte1, byte2, byte3, byte4,
                ])))
            }
            _ => match byte0 {
                0xc0 => {
                    let mut buf = [0u8; 1];
                    r.read_exact(&mut buf)?;
                    Ok(Self::String(u8::from_le_bytes(buf).to_string()))
                }
                0xc1 => {
                    let mut buf = [0u8; 2];
                    r.read_exact(&mut buf)?;
                    Ok(Self::String(u16::from_le_bytes(buf).to_string()))
                }
                0xc2 => {
                    let mut buf = [0u8; 4];
                    r.read_exact(&mut buf)?;
                    Ok(Self::String(u32::from_le_bytes(buf).to_string()))
                }
                _ => {
                    eprintln!(
                        "Any bytes starts with {byte0} are not supported by the size encoding"
                    );
                    Err(RedisError::Encoding)
                }
            },
        }
    }

    pub(crate) fn value(&self) -> Option<usize> {
        if let Self::Integer(value) = self {
            Some(*value)
        } else {
            None
        }
    }
}

fn size_0b00(num: u8) -> usize {
    (num & MASK_LAST_SIX).into()
}

fn size_0b01(nums: [u8; 2]) -> usize {
    let [one, two] = nums;
    let bytes: [u8; 8] = [0, 0, 0, 0, 0, 0, one & MASK_LAST_SIX, two];
    usize::from_be_bytes(bytes)
}

fn size_0b10(nums: [u8; 5]) -> usize {
    let [_, one, two, three, four] = nums;
    let bytes: [u8; 8] = [0, 0, 0, 0, one, two, three, four];
    usize::from_be_bytes(bytes)
}

#[derive(Debug, PartialEq)]
pub(crate) struct EncString(String);

impl EncString {
    pub(crate) fn new<R: Read>(r: &mut R) -> RedisResult<Self> {
        match EncSize::new(r)? {
            EncSize::Integer(size) => {
                let mut buf = vec![0; size];
                r.read_exact(&mut buf)?;

                utils::stringify(&buf).map(|v| Self(v.into()))
            }
            EncSize::String(value) => Ok(Self(value)),
        }
    }

    pub(crate) fn value(&self) -> &str {
        self.0.as_str()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn encoded_size_prefixed_with_0b00() {
        assert_eq!(size_0b00(0x0a), 10);

        let bytes = [0x0a];
        let mut buf = Cursor::new(bytes);

        let actual = EncSize::new(&mut buf).unwrap();
        let expected = EncSize::Integer(10);
        assert_eq!(actual, expected);
    }

    #[test]
    fn encoded_size_prefixed_with_0b01() {
        assert_eq!(size_0b01([0x42, 0xbc]), 700);

        let bytes = [0x42, 0xbc];
        let mut buf = Cursor::new(bytes);

        let actual = EncSize::new(&mut buf).unwrap();
        let expected = EncSize::Integer(700);
        assert_eq!(actual, expected);
    }

    #[test]
    fn encoded_size_prefixed_with_0b10() {
        assert_eq!(size_0b10([0x80, 0x00, 0x00, 0x42, 0x68]), 17000);

        let bytes = [0x80, 0x00, 0x00, 0x42, 0x68];
        let mut buf = Cursor::new(bytes);

        let actual = EncSize::new(&mut buf).unwrap();
        let expected = EncSize::Integer(17000);
        assert_eq!(actual, expected);
    }

    #[test]
    fn encoded_size_prefixed_with_0xc0() {
        let bytes = [0xc0, 0x7b];
        let mut buf = Cursor::new(bytes);

        let actual = EncSize::new(&mut buf).unwrap();
        let expected = EncSize::String("123".into());
        assert_eq!(actual, expected);
    }

    #[test]
    fn encoded_size_prefixed_with_0xc1() {
        let bytes = [0xc1, 0x39, 0x30];
        let mut buf = Cursor::new(bytes);

        let actual = EncSize::new(&mut buf).unwrap();
        let expected = EncSize::String("12345".into());
        assert_eq!(actual, expected);
    }

    #[test]
    fn encoded_size_prefixed_with_0xc2() {
        let bytes = [0xc2, 0x87, 0xd6, 0x12, 0x00];
        let mut buf = Cursor::new(bytes);

        let actual = EncSize::new(&mut buf).unwrap();
        let expected = EncSize::String("1234567".into());
        assert_eq!(actual, expected);
    }

    #[test]
    fn encoded_string_prefixed_with_encoded_size() {
        let bytes = [
            0x0d, 0x48, 0x65, 0x6c, 0x6c, 0x6f, 0x2c, 0x20, 0x57, 0x6f, 0x72, 0x6c, 0x64, 0x21,
        ];
        let mut buf = Cursor::new(bytes);

        let actual = EncString::new(&mut buf).unwrap();
        let expected = EncString("Hello, World!".into());
        assert_eq!(actual, expected);
    }

    #[test]
    fn encoded_string_prefixed_with_0xc0() {
        let bytes = [0xc0, 0x7b];
        let mut buf = Cursor::new(bytes);

        let actual = EncString::new(&mut buf).unwrap();
        let expected = EncString("123".into());
        assert_eq!(actual, expected);
    }

    #[test]
    fn encoded_string_prefixed_with_0xc1() {
        let bytes = [0xc1, 0x39, 0x30];
        let mut buf = Cursor::new(bytes);

        let actual = EncString::new(&mut buf).unwrap();
        let expected = EncString("12345".into());
        assert_eq!(actual, expected);
    }

    #[test]
    fn encoded_string_prefixed_with_0xc2() {
        let bytes = [0xc2, 0x87, 0xd6, 0x12, 0x00];
        let mut buf = Cursor::new(bytes);

        let actual = EncString::new(&mut buf).unwrap();
        let expected = EncString("1234567".into());
        assert_eq!(actual, expected);
    }
}
