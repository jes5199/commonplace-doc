use std::sync::Arc;
use tokio::sync::broadcast;

/// Receive from a broadcast channel, logging lag warnings and returning None on close.
///
/// This helper consolidates the common pattern of:
/// - On Ok(msg): return Some(msg)
/// - On Lagged: log warning and continue to next message
/// - On Closed: return None
///
/// Usage:
/// ```ignore
/// while let Some(msg) = recv_broadcast(&mut rx, "MQTT handler").await {
///     // handle msg
/// }
/// ```
pub async fn recv_broadcast<T: Clone>(rx: &mut broadcast::Receiver<T>, context: &str) -> Option<T> {
    loop {
        match rx.recv().await {
            Ok(msg) => return Some(msg),
            Err(broadcast::error::RecvError::Lagged(n)) => {
                tracing::warn!("{} lagged by {} messages", context, n);
                // Continue to receive next message
            }
            Err(broadcast::error::RecvError::Closed) => {
                return None;
            }
        }
    }
}

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
