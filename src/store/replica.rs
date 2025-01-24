use super::{OutgoingMessage, Resp};
use tokio::sync::mpsc::Sender;

#[derive(Debug, Clone)]
enum SyncStatus {
    Reached(usize),
    Behind(usize),
}

#[derive(Debug, Clone)]
pub(crate) struct Replica {
    sender: Sender<Vec<u8>>,
    status: SyncStatus,
    wait_callbacks: Option<Vec<WaitCallback>>,
}

impl Replica {
    pub(crate) fn new(sender: Sender<Vec<u8>>) -> Self {
        Self {
            sender,
            status: SyncStatus::Reached(0),
            wait_callbacks: Some(vec![]),
        }
    }

    pub(crate) async fn send(&mut self, msg: impl Into<OutgoingMessage>) {
        let mut sent: usize = 0;
        let msg: OutgoingMessage = msg.into();

        for msg in msg.into_iter() {
            let size = msg.len();

            match self.sender.send(msg).await {
                Ok(_) => {
                    sent += size;
                }
                Err(_) => {
                    eprintln!("Receiver dropped");
                }
            }
        }

        let ack_sent = self.ack_sent() + sent;
        self.status = SyncStatus::Behind(ack_sent);
    }

    pub(crate) async fn send_getack(&mut self) {
        let msg: Resp = vec![
            "REPLCONF".to_string(),
            "GETACK".to_string(),
            "*".to_string(),
        ]
        .into();
        self.send(msg).await
    }

    pub(crate) async fn receive_ack(&mut self, received: usize) {
        if self.ack_sent() <= received {
            self.status = SyncStatus::Reached(received);
        } else {
            self.status = SyncStatus::Behind(received);
        }

        let mut callbacks: Vec<WaitCallback> = vec![];

        if let Some(cbs) = self.wait_callbacks.take() {
            for cb in cbs {
                if cb.target_ack <= received {
                    if cb.tx.send(WaitSignal::Synced).await.is_err() {
                        eprintln!("Receiver dropped");
                    }
                } else {
                    callbacks.push(cb);
                }
            }
        }

        self.wait_callbacks = Some(callbacks);
    }

    pub(crate) async fn add_wait_callback(&mut self, tx: Sender<WaitSignal>) {
        if self.is_synced() {
            if tx.send(WaitSignal::Synced).await.is_err() {
                eprintln!("Receiver dropped");
            }
            return;
        }

        let target_ack = self.ack_sent();
        if let Some(cbs) = self.wait_callbacks.as_mut() {
            let cb = WaitCallback { tx, target_ack };
            cbs.push(cb);
        }
    }

    pub(crate) fn is_synced(&self) -> bool {
        matches!(self.status, SyncStatus::Reached(_))
    }

    fn ack_sent(&self) -> usize {
        match self.status {
            SyncStatus::Reached(byte) => byte,
            SyncStatus::Behind(byte) => byte,
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) enum WaitSignal {
    Synced,
    Timeout,
}

#[derive(Debug, Clone)]
struct WaitCallback {
    tx: Sender<WaitSignal>,
    target_ack: usize,
}
