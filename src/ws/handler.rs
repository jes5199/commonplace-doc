//! WebSocket connection handlers.

use super::connection::{OutgoingMessage, WsConnection};
use super::protocol::{
    self, ProtocolMode, WsMessage, SUBPROTOCOL_COMMONPLACE, SUBPROTOCOL_Y_WEBSOCKET,
};
use super::room::RoomManager;
use crate::document::DocumentStore;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;
use std::sync::Arc;
use tokio::sync::{mpsc, RwLock};
use tokio::time::{interval, Duration};
use tracing::{debug, error, info, warn};
use yrs::updates::encoder::Encode;

/// Keep-alive ping interval (30 seconds)
const PING_INTERVAL: Duration = Duration::from_secs(30);

/// Timeout for considering a connection dead (90 seconds = 3 missed pings)
const CONNECTION_TIMEOUT: Duration = Duration::from_secs(90);

/// WebSocket state shared across handlers.
#[derive(Clone)]
pub struct WsState {
    pub room_manager: Arc<RoomManager>,
    pub doc_store: Arc<DocumentStore>,
    pub fs_root: Option<String>,
}

/// Handle WebSocket upgrade request.
pub async fn ws_handler(
    ws: WebSocketUpgrade,
    State(state): State<WsState>,
    Path(doc_id): Path<String>,
    headers: HeaderMap,
) -> Result<impl IntoResponse, StatusCode> {
    // Check if document exists
    let room = state.room_manager.get_or_create_room(&doc_id).await;

    // Negotiate subprotocol
    let protocol = negotiate_protocol(&headers);

    info!(
        doc_id = %doc_id,
        protocol = ?protocol,
        "WebSocket upgrade request"
    );

    // Upgrade the connection
    Ok(ws
        .protocols([SUBPROTOCOL_Y_WEBSOCKET, SUBPROTOCOL_COMMONPLACE])
        .on_upgrade(move |socket| handle_socket(socket, state, doc_id, protocol, room)))
}

/// Handle WebSocket upgrade request by filesystem path.
pub async fn ws_path_handler(
    ws: WebSocketUpgrade,
    State(state): State<WsState>,
    Path(path): Path<String>,
    headers: HeaderMap,
) -> Result<impl IntoResponse, StatusCode> {
    // Resolve path to document ID
    let fs_root_id = state
        .fs_root
        .as_ref()
        .ok_or(StatusCode::SERVICE_UNAVAILABLE)?;

    let doc_id = crate::path::resolve_path_to_doc_id(&state.doc_store, fs_root_id, &path)
        .await
        .ok_or(StatusCode::NOT_FOUND)?;

    // Get or create room for the document
    let room = state.room_manager.get_or_create_room(&doc_id).await;

    // Negotiate subprotocol
    let protocol = negotiate_protocol(&headers);

    info!(
        path = %path,
        doc_id = %doc_id,
        protocol = ?protocol,
        "WebSocket upgrade request (path)"
    );

    // Upgrade the connection
    Ok(ws
        .protocols([SUBPROTOCOL_Y_WEBSOCKET, SUBPROTOCOL_COMMONPLACE])
        .on_upgrade(move |socket| handle_socket(socket, state, doc_id, protocol, room)))
}

/// Negotiate the WebSocket subprotocol from headers.
fn negotiate_protocol(headers: &HeaderMap) -> ProtocolMode {
    // Check Sec-WebSocket-Protocol header
    if let Some(protocols) = headers.get("sec-websocket-protocol") {
        if let Ok(s) = protocols.to_str() {
            // Client sends comma-separated list of preferred protocols
            for proto in s.split(',').map(|p| p.trim()) {
                if proto == SUBPROTOCOL_COMMONPLACE {
                    return ProtocolMode::Commonplace;
                }
                if proto == SUBPROTOCOL_Y_WEBSOCKET {
                    return ProtocolMode::YWebSocket;
                }
            }
        }
    }

    // Default to y-websocket for maximum compatibility
    ProtocolMode::YWebSocket
}

/// Handle an established WebSocket connection.
async fn handle_socket(
    mut socket: WebSocket,
    state: WsState,
    doc_id: String,
    protocol: ProtocolMode,
    room: Arc<super::room::Room>,
) {
    // Create channel for outgoing messages (bounded for backpressure)
    let (tx, mut rx) = mpsc::channel::<OutgoingMessage>(256);

    // Create connection
    let conn = Arc::new(RwLock::new(WsConnection::new(doc_id.clone(), protocol, tx)));

    let conn_id = conn.read().await.id.clone();
    info!(conn_id = %conn_id, doc_id = %doc_id, "WebSocket connected");

    // Add to room
    room.add_connection(conn.clone()).await;

    // Initial sync: send our state vector and full state
    if let Err(e) = send_initial_sync(&conn, &room, &mut socket).await {
        warn!(conn_id = %conn_id, "Failed initial sync: {}", e);
    }

    // Keep-alive ping interval
    let mut ping_interval = interval(PING_INTERVAL);
    ping_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    // Main loop: handle incoming and outgoing messages
    loop {
        tokio::select! {
            // Handle outgoing messages from channel
            Some(msg) = rx.recv() => {
                let ws_msg = match msg {
                    OutgoingMessage::Binary(data) => Message::Binary(data),
                    OutgoingMessage::Close => {
                        let _ = socket.close().await;
                        break;
                    }
                };
                if let Err(e) = socket.send(ws_msg).await {
                    debug!("Failed to send WebSocket message: {}", e);
                    break;
                }
            }

            // Keep-alive ping
            _ = ping_interval.tick() => {
                // Check if connection has timed out
                let last_activity = conn.read().await.last_activity;
                if last_activity.elapsed() > CONNECTION_TIMEOUT {
                    warn!(conn_id = %conn_id, "Connection timed out (no activity for {:?})", CONNECTION_TIMEOUT);
                    let _ = socket.close().await;
                    break;
                }

                // Send ping
                if let Err(e) = socket.send(Message::Ping(vec![])).await {
                    debug!(conn_id = %conn_id, "Failed to send ping: {}", e);
                    break;
                }
            }

            // Handle incoming messages
            msg = socket.recv() => {
                match msg {
                    Some(Ok(Message::Binary(data))) => {
                        conn.write().await.touch();
                        if let Err(e) = handle_binary_message(&conn, &data, &room).await {
                            warn!(conn_id = %conn_id, "Error handling message: {}", e);
                        }
                    }
                    Some(Ok(Message::Ping(_))) => {
                        // Pong is handled automatically by axum
                        conn.write().await.touch();
                        debug!(conn_id = %conn_id, "Received ping");
                    }
                    Some(Ok(Message::Pong(_))) => {
                        conn.write().await.touch();
                        debug!(conn_id = %conn_id, "Received pong");
                    }
                    Some(Ok(Message::Close(_))) => {
                        info!(conn_id = %conn_id, "Client initiated close");
                        break;
                    }
                    Some(Ok(Message::Text(text))) => {
                        // y-websocket uses binary, but some clients might send text
                        conn.write().await.touch();
                        if let Err(e) = handle_binary_message(&conn, text.as_bytes(), &room).await {
                            warn!(conn_id = %conn_id, "Error handling text message: {}", e);
                        }
                    }
                    Some(Err(e)) => {
                        error!(conn_id = %conn_id, "WebSocket error: {}", e);
                        break;
                    }
                    None => {
                        info!(conn_id = %conn_id, "WebSocket stream ended");
                        break;
                    }
                }
            }
        }
    }

    // Cleanup
    info!(conn_id = %conn_id, doc_id = %doc_id, "WebSocket disconnected");
    room.remove_connection(&conn_id).await;

    // Cleanup empty rooms periodically (could be moved to a background task)
    state.room_manager.cleanup_empty_rooms().await;
}

/// Send initial sync to a new connection.
async fn send_initial_sync(
    _conn: &Arc<RwLock<WsConnection>>,
    room: &Arc<super::room::Room>,
    socket: &mut WebSocket,
) -> Result<(), super::room::RoomError> {
    // Get server's state vector
    let sv = room.get_state_vector().await?;

    // Send SyncStep1 (our state vector) - asking client what we're missing
    let sync_step1 = protocol::encode_sync_step1(&sv);
    let _ = socket.send(Message::Binary(sync_step1)).await;

    // Send SyncStep2 with full state (so client gets everything)
    // Use properly encoded empty state vector to get full document
    let empty_sv = yrs::StateVector::default().encode_v1();
    let full_state = room.handle_sync_step1(&empty_sv).await?;
    let _ = socket.send(Message::Binary(full_state)).await;

    Ok(())
}

/// Handle a binary WebSocket message.
async fn handle_binary_message(
    conn: &Arc<RwLock<WsConnection>>,
    data: &[u8],
    room: &Arc<super::room::Room>,
) -> Result<(), String> {
    let msg = protocol::decode_message(data).map_err(|e| e.to_string())?;
    let conn_id = conn.read().await.id.clone();

    match msg {
        WsMessage::SyncStep1 { state_vector } => {
            // Client is asking what updates we have that they don't
            let response = room
                .handle_sync_step1(&state_vector)
                .await
                .map_err(|e| e.to_string())?;

            // Send response to the specific connection
            conn.read().await.try_send_binary(response);
        }
        WsMessage::SyncStep2 { update } => {
            // Client is sending us updates we're missing
            let commit_meta = conn.write().await.take_commit_meta();
            room.handle_update_with_meta(&conn_id, &update, commit_meta)
                .await
                .map_err(|e| e.to_string())?;
        }
        WsMessage::Update { update } => {
            // Incremental update from client
            let commit_meta = conn.write().await.take_commit_meta();
            room.handle_update_with_meta(&conn_id, &update, commit_meta)
                .await
                .map_err(|e| e.to_string())?;
        }
        WsMessage::Awareness { data: _ } => {
            // Awareness is deferred - just ignore for now
            debug!("Ignoring awareness message (not implemented)");
        }
        WsMessage::CommitMeta {
            parent_cid,
            timestamp: _,
            author,
            message,
        } => {
            // Store commit metadata for the next update
            let mut conn = conn.write().await;
            if conn.is_commonplace_mode() {
                conn.set_commit_meta(parent_cid, author, message);
                debug!(conn_id = %conn_id, "Stored pending commit metadata");
            } else {
                debug!(conn_id = %conn_id, "Ignoring CommitMeta in y-websocket mode");
            }
        }
        WsMessage::BlueEvent { .. } => {
            // BlueEvents are server-to-client only
            debug!("Ignoring BlueEvent from client (server-to-client only)");
        }
        WsMessage::RedEvent {
            event_type,
            payload,
        } => {
            // Broadcast RedEvent to all other clients
            room.broadcast_red_event(&conn_id, &event_type, &payload)
                .await;
        }
    }

    Ok(())
}
