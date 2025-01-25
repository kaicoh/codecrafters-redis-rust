use std::cmp::Ordering;
use std::collections::HashMap;
use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamEntry {
    id: String,
    values: HashMap<String, String>,
}

impl StreamEntry {
    pub fn new(id: &str, values: HashMap<String, String>) -> Self {
        Self {
            id: id.into(),
            values,
        }
    }

    pub fn id(&self) -> &str {
        self.id.as_str()
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
