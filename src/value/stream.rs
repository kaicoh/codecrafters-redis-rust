use super::{RedisError, RedisResult};
use std::cmp::Ordering;
use std::collections::HashMap;
use std::fmt;

#[derive(Debug, Clone)]
pub struct RedisStream(Vec<StreamEntry>);

impl RedisStream {
    pub fn new(entry: StreamEntry) -> Self {
        Self(vec![entry])
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamEntry {
    id: StreamEntryId,
    values: HashMap<String, String>,
}

impl StreamEntry {
    pub fn new(id: String, values: HashMap<String, String>) -> RedisResult<Self> {
        Ok(Self {
            id: id.try_into()?,
            values,
        })
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

impl TryFrom<String> for StreamEntryId {
    type Error = RedisError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        let mut tokens = value.split('-');

        let first = tokens
            .next()
            .ok_or(anyhow::anyhow!("invalid stream entry id"))?
            .parse::<u64>()?;
        let second = tokens
            .next()
            .ok_or(anyhow::anyhow!("invalid stream entry id"))?
            .parse::<u64>()?;

        if tokens.next().is_some() {
            Err(anyhow::anyhow!("invalid stream entry id").into())
        } else if first == 0 && second == 0 {
            Err(RedisError::InvalidStreamEntryId00)
        } else {
            Ok(Self(first, second))
        }
    }
}

impl TryFrom<&str> for StreamEntryId {
    type Error = RedisError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        value.to_string().try_into()
    }
}

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
