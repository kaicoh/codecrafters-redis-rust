use super::Command;

#[derive(Debug, Clone)]
pub struct Transaction(Vec<Command>);

impl Transaction {
    pub fn new() -> Self {
        Self(vec![])
    }

    pub fn push(&mut self, cmd: Command) {
        self.0.push(cmd);
    }

    pub fn unwrap(self) -> Vec<Command> {
        self.0
    }
}
