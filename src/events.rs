use std::sync::Arc;
use tokio::sync::broadcast;

#[derive(Debug, Clone)]
pub struct CommitNotification {
    pub doc_id: String,
    pub commit_id: String,
    pub timestamp: u64,
}

#[derive(Clone)]
pub struct CommitBroadcaster {
    sender: Arc<broadcast::Sender<CommitNotification>>,
}

impl CommitBroadcaster {
    pub fn new(capacity: usize) -> Self {
        let (sender, _) = broadcast::channel(capacity);
        Self {
            sender: Arc::new(sender),
        }
    }

    pub fn subscribe(&self) -> broadcast::Receiver<CommitNotification> {
        self.sender.subscribe()
    }

    pub fn notify(&self, notification: CommitNotification) {
        // Ignore errors when there are no active subscribers
        let _ = self.sender.send(notification);
    }
}
