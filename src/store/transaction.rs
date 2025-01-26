use super::Command;

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct Transaction(Vec<Command>);

impl Transaction {
    pub fn new() -> Self {
        Self(vec![])
    }
}
