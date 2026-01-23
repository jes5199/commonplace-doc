use std::sync::Arc;
use tokio::sync::broadcast;

/// Result of receiving from a broadcast channel.
#[derive(Debug, Clone)]
pub enum BroadcastRecvResult<T> {
    /// Successfully received a message.
    Message(T),
    /// Receiver lagged behind and lost messages. Contains the number of missed messages
    /// and the next available message (if any).
    Lagged {
        /// Number of messages that were lost due to lag.
        missed_count: u64,
        /// The next message available after the lag (if channel not closed).
        next_message: Option<T>,
    },
    /// Channel was closed.
    Closed,
}

/// Receive from a broadcast channel with lag detection.
///
/// Unlike `recv_broadcast`, this function returns immediately on lag, allowing
/// callers to perform async resync operations before continuing.
///
/// Usage:
/// ```ignore
/// loop {
///     match recv_broadcast_with_lag(&mut rx, "MQTT handler").await {
///         BroadcastRecvResult::Message(msg) => {
///             // handle msg
///         }
///         BroadcastRecvResult::Lagged { missed_count, next_message } => {
///             info!("Lost {} messages, resyncing...", missed_count);
///             resync_from_server().await;
///             if let Some(msg) = next_message {
///                 // handle the next message after resync
///             }
///         }
///         BroadcastRecvResult::Closed => break,
///     }
/// }
/// ```
pub async fn recv_broadcast_with_lag<T: Clone>(
    rx: &mut broadcast::Receiver<T>,
    context: &str,
) -> BroadcastRecvResult<T> {
    match rx.recv().await {
        Ok(msg) => BroadcastRecvResult::Message(msg),
        Err(broadcast::error::RecvError::Lagged(n)) => {
            tracing::warn!("{} lagged by {} messages", context, n);
            // Try to get the next available message
            let next = match rx.recv().await {
                Ok(msg) => Some(msg),
                Err(broadcast::error::RecvError::Lagged(n2)) => {
                    tracing::warn!("{} lagged again by {} more messages", context, n2);
                    // Keep trying until we get a message or closed
                    loop {
                        match rx.recv().await {
                            Ok(msg) => break Some(msg),
                            Err(broadcast::error::RecvError::Lagged(n3)) => {
                                tracing::warn!("{} still lagging by {} messages", context, n3);
                                continue;
                            }
                            Err(broadcast::error::RecvError::Closed) => break None,
                        }
                    }
                }
                Err(broadcast::error::RecvError::Closed) => None,
            };
            BroadcastRecvResult::Lagged {
                missed_count: n,
                next_message: next,
            }
        }
        Err(broadcast::error::RecvError::Closed) => BroadcastRecvResult::Closed,
    }
}

/// Receive from a broadcast channel, logging lag warnings and returning None on close.
///
/// This helper consolidates the common pattern of:
/// - On Ok(msg): return Some(msg)
/// - On Lagged: log warning, optionally invoke callback, and continue to next message
/// - On Closed: return None
///
/// # Arguments
/// * `rx` - The broadcast receiver
/// * `context` - A string describing this receiver for logging
/// * `on_lagged` - Optional callback invoked when messages are lost due to lag.
///   Use this to trigger resync from server when needed.
///
/// Usage:
/// ```ignore
/// // Basic usage (no resync callback)
/// while let Some(msg) = recv_broadcast(&mut rx, "MQTT handler", None::<fn(u64)>).await {
///     // handle msg
/// }
///
/// // With resync callback
/// while let Some(msg) = recv_broadcast(&mut rx, "MQTT handler", Some(|n| {
///     info!("Lost {} messages, triggering resync", n);
///     // trigger resync...
/// })).await {
///     // handle msg
/// }
/// ```
pub async fn recv_broadcast<T: Clone, F: FnMut(u64)>(
    rx: &mut broadcast::Receiver<T>,
    context: &str,
    mut on_lagged: Option<F>,
) -> Option<T> {
    loop {
        match rx.recv().await {
            Ok(msg) => return Some(msg),
            Err(broadcast::error::RecvError::Lagged(n)) => {
                tracing::warn!("{} lagged by {} messages", context, n);
                if let Some(ref mut callback) = on_lagged {
                    tracing::info!("{} invoking resync callback due to lag", context);
                    callback(n);
                }
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
