//! WebSocket module for real-time Yjs sync.
//!
//! Provides WebSocket endpoint at `/ws/docs/{id}` with subprotocol negotiation:
//! - `y-websocket`: Standard Yjs sync protocol for browser tools (Tiptap, Monaco)
//! - `commonplace`: Extended protocol with commit metadata and blue/red ports

pub mod connection;
pub mod handler;
pub mod protocol;
pub mod room;

use crate::document::DocumentStore;
use crate::events::CommitBroadcaster;
use crate::store::CommitStore;
use axum::routing::get;
use axum::Router;
use handler::WsState;
use room::RoomManager;
use std::sync::Arc;

/// Create the WebSocket router.
pub fn router(
    doc_store: Arc<DocumentStore>,
    commit_store: Option<Arc<CommitStore>>,
    broadcaster: Option<CommitBroadcaster>,
    _fs_root: Option<String>,
) -> Router {
    let room_manager = Arc::new(RoomManager::new(
        doc_store,
        commit_store,
        broadcaster.clone(),
    ));

    // Spawn background task to listen for commit notifications
    if let Some(bc) = broadcaster {
        let rm = room_manager.clone();
        tokio::spawn(async move {
            commit_listener(rm, bc).await;
        });
    }

    let state = WsState { room_manager };

    Router::new()
        .route("/ws/docs/:id", get(handler::ws_handler))
        .with_state(state)
}

/// Background task to listen for commit notifications and forward to WebSocket clients.
async fn commit_listener(room_manager: Arc<RoomManager>, broadcaster: CommitBroadcaster) {
    let mut rx = broadcaster.subscribe();

    loop {
        match rx.recv().await {
            Ok(notification) => {
                // Forward to all rooms (each room filters by doc_id)
                let rooms = room_manager.get_all_rooms().await;
                for room in rooms {
                    room.handle_commit_notification(&notification).await;
                }
            }
            Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                tracing::warn!("WebSocket commit listener lagged by {} messages", n);
            }
            Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                tracing::info!("Commit broadcaster closed, stopping WebSocket listener");
                break;
            }
        }
    }
}
