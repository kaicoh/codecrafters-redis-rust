use super::{RedisError, RedisResult};
use std::cmp::Ordering;
use std::collections::HashMap;
use std::fmt;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone)]
pub struct RedisStream(Vec<StreamEntry>);

impl RedisStream {
    pub fn new() -> Self {
        Self(vec![])
    }

    pub fn push(&mut self, entry: StreamEntry) -> RedisResult<()> {
        if self.valid_id(entry.id()) {
            self.0.push(entry);
            Ok(())
        } else {
            Err(RedisError::SmallerStreamEntryId)
        }
    }

    fn valid_id(&self, id: StreamEntryId) -> bool {
        match self.last_id() {
            Some(last_id) => last_id < id,
            None => true,
        }
    }

    fn last_id(&self) -> Option<StreamEntryId> {
        self.0.last().map(StreamEntry::id)
    }
}

impl fmt::Display for RedisStream {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "entries")?;

        for entry in self.0.iter() {
            writeln!(f, "{entry}")?;
        }

        Ok(())
    }
}

impl Default for RedisStream {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamEntry {
    id: StreamEntryId,
    values: HashMap<String, String>,
}

impl StreamEntry {
    pub fn new(id: StreamEntryId, values: HashMap<String, String>) -> Self {
        Self { id, values }
    }

    pub fn id(&self) -> StreamEntryId {
        self.id
    }

    pub fn values(&self) -> &HashMap<String, String> {
        &self.values
    }
}

impl PartialOrd for StreamEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for StreamEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        self.id.cmp(&other.id)
    }
}

impl fmt::Display for StreamEntry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "  - id: {}", self.id)?;

        for (key, value) in self.values.iter() {
            writeln!(f, "    {key}: {value}")?;
        }

        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StreamEntryId(u64, u64);

impl fmt::Display for StreamEntryId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}-{}", self.0, self.1)
    }
}

impl PartialOrd for StreamEntryId {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for StreamEntryId {
    fn cmp(&self, other: &Self) -> Ordering {
        if self.0 == other.0 {
            self.1.cmp(&other.1)
        } else {
            self.0.cmp(&other.0)
        }
    }
}

#[derive(Debug)]
pub enum StreamEntryIdFactor {
    ValidId(u64, u64),
    Timestamp(u64),
}

impl StreamEntryIdFactor {
    pub fn new(value: &str) -> RedisResult<Self> {
        value.to_string().try_into()
    }

    pub fn try_into_id(self, stream: &RedisStream) -> RedisResult<StreamEntryId> {
        match self {
            Self::ValidId(t0, s0) => {
                let id = StreamEntryId(t0, s0);

                if stream.valid_id(id) {
                    Ok(id)
                } else {
                    Err(RedisError::SmallerStreamEntryId)
                }
            }
            Self::Timestamp(t0) => match stream.last_id() {
                Some(StreamEntryId(t1, _)) if t0 < t1 => Err(RedisError::SmallerStreamEntryId),
                Some(StreamEntryId(t1, s1)) if t0 == t1 => Ok(StreamEntryId(t1, s1 + 1)),
                //_ => Ok(StreamEntryId(t0, 0)),
                _ => {
                    let id = if t0 == 0 {
                        StreamEntryId(0, 1)
                    } else {
                        StreamEntryId(t0, 0)
                    };
                    Ok(id)
                }
            },
        }
    }
}

impl TryFrom<String> for StreamEntryIdFactor {
    type Error = RedisError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        if value.as_str() == "*" {
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_err(|_| anyhow::anyhow!("SystemTime before UNIX EPOCH!"))?
                .as_millis() as u64;
            return Ok(Self::Timestamp(now));
        }

        let mut tokens = value.split('-');

        let first = tokens
            .next()
            .ok_or(anyhow::anyhow!("invalid stream entry id"))?
            .parse::<u64>()?;
        let second = match tokens.next() {
            Some("*") => {
                return Ok(Self::Timestamp(first));
            }
            Some(token) => token.parse::<u64>()?,
            None => {
                return Err(anyhow::anyhow!("invalid stream entry id").into());
            }
        };

        if tokens.next().is_some() {
            Err(anyhow::anyhow!("invalid stream entry id").into())
        } else if first == 0 && second == 0 {
            Err(RedisError::InvalidStreamEntryId00)
        } else {
            Ok(Self::ValidId(first, second))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_sorts_entry_ids() {
        let id0 = StreamEntryId(1, 1);
        let id1 = StreamEntryId(1, 2);
        assert!(id0 < id1);
    }
}
