use super::{RedisError, RedisResult, Resp};
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

    pub fn query(
        &self,
        start: StreamEntryIdFactor,
        end: StreamEntryIdFactor,
    ) -> RedisResult<impl Iterator<Item = &StreamEntry>> {
        let start = start.as_start()?;
        let end = end.as_end()?;
        Ok(self
            .0
            .iter()
            .filter(move |e| start <= e.id() && e.id() <= end))
    }

    pub fn find(&self, start: StreamEntryIdFactor) -> RedisResult<Option<&StreamEntry>> {
        let start = start.as_start()?;
        Ok(self.0.iter().find(move |e| start < e.id()))
    }

    pub fn last_id(&self) -> Option<StreamEntryId> {
        self.0.last().map(StreamEntry::id)
    }

    fn valid_id(&self, id: StreamEntryId) -> bool {
        match self.last_id() {
            Some(last_id) => last_id < id,
            None => true,
        }
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

impl From<StreamEntry> for Resp {
    fn from(entry: StreamEntry) -> Self {
        let StreamEntry { id, values } = entry;

        let mut elements: Vec<Resp> = vec![];

        for (key, value) in values {
            elements.push(Resp::BS(Some(key)));
            elements.push(Resp::BS(Some(value)));
        }

        Resp::A(vec![Resp::BS(Some(format!("{id}"))), Resp::A(elements)])
    }
}

impl From<Vec<StreamEntry>> for Resp {
    fn from(resps: Vec<StreamEntry>) -> Self {
        Resp::A(resps.into_iter().map(Resp::from).collect())
    }
}

impl From<(String, StreamEntry)> for Resp {
    fn from((key, entry): (String, StreamEntry)) -> Self {
        Resp::A(vec![Resp::BS(Some(key)), Resp::A(vec![Resp::from(entry)])])
    }
}

impl From<Vec<(String, StreamEntry)>> for Resp {
    fn from(values: Vec<(String, StreamEntry)>) -> Self {
        Resp::A(values.into_iter().map(Resp::from).collect())
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
    MayValidId(u64, u64),
    Timestamp(u64),
    RangeFromBeginning,
    RangeToEnd,
}

impl StreamEntryIdFactor {
    pub fn new(value: &str) -> RedisResult<Self> {
        value.to_string().try_into()
    }

    pub fn try_into_id(self, stream: &RedisStream) -> RedisResult<StreamEntryId> {
        match self {
            Self::MayValidId(0, 0) => Err(RedisError::InvalidStreamEntryId00),
            Self::MayValidId(t0, s0) => {
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
            Self::RangeFromBeginning => {
                Err(anyhow::anyhow!("\"-\" cannot be used as stream entry id").into())
            }
            Self::RangeToEnd => {
                Err(anyhow::anyhow!("\"+\" cannot be used as stream entry id").into())
            }
        }
    }

    pub fn as_start(&self) -> RedisResult<StreamEntryId> {
        match self {
            Self::MayValidId(t0, s0) => Ok(StreamEntryId(*t0, *s0)),
            Self::Timestamp(0) | Self::RangeFromBeginning => Ok(StreamEntryId(0, 1)),
            Self::Timestamp(t0) => Ok(StreamEntryId(*t0, 0)),
            Self::RangeToEnd => Err(anyhow::anyhow!(
                "\"+\" cannot be used as the start of stream entry id range"
            )
            .into()),
        }
    }

    pub fn as_end(&self) -> RedisResult<StreamEntryId> {
        match self {
            Self::MayValidId(t0, s0) => Ok(StreamEntryId(*t0, *s0)),
            Self::Timestamp(t0) => Ok(StreamEntryId(*t0, u64::MAX)),
            Self::RangeFromBeginning => Err(anyhow::anyhow!(
                "\"-\" cannot be used as the end of stream entry id range"
            )
            .into()),
            Self::RangeToEnd => Ok(StreamEntryId(u64::MAX, u64::MAX)),
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

        if value.as_str() == "-" {
            return Ok(Self::RangeFromBeginning);
        }

        if value.as_str() == "+" {
            return Ok(Self::RangeToEnd);
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
        } else {
            Ok(Self::MayValidId(first, second))
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
