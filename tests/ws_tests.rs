//! WebSocket integration tests for y-websocket and commonplace protocols.

use futures_util::{SinkExt, StreamExt};
use std::net::SocketAddr;
use std::sync::Once;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::Message;
use yrs::updates::decoder::Decode;
use yrs::{GetString, Text, Transact, Update};

const TIMEOUT: Duration = Duration::from_secs(5);

static INIT: Once = Once::new();

fn init_tracing() {
    INIT.call_once(|| {
        tracing_subscriber::fmt()
            .with_env_filter("commonplace_doc=debug")
            .with_test_writer()
            .try_init()
            .ok();
    });
}

/// Start a test server and return its address.
async fn start_test_server() -> SocketAddr {
    let app = commonplace_doc::create_router();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    // Give the server a moment to start
    tokio::time::sleep(Duration::from_millis(50)).await;

    addr
}

/// Start a test server with commit store.
async fn start_test_server_with_store() -> (SocketAddr, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("commits.redb");
    let store = commonplace_doc::store::CommitStore::new(&path).unwrap();
    let app = commonplace_doc::create_router_with_store(Some(store));

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    tokio::time::sleep(Duration::from_millis(50)).await;

    (addr, dir)
}

/// Create a document via HTTP and return its ID.
async fn create_document(addr: &SocketAddr, content_type: &str) -> String {
    let client = reqwest::Client::new();
    let response = client
        .post(format!("http://{}/docs", addr))
        .header("content-type", content_type)
        .send()
        .await
        .unwrap();

    let json: serde_json::Value = response.json().await.unwrap();
    json["id"].as_str().unwrap().to_string()
}

/// Get document content via HTTP.
async fn get_document_content(addr: &SocketAddr, doc_id: &str) -> String {
    let client = reqwest::Client::new();
    let response = client
        .get(format!("http://{}/docs/{}", addr, doc_id))
        .send()
        .await
        .unwrap();

    response.text().await.unwrap()
}

/// Receive next WebSocket message with timeout.
async fn recv_ws_msg<S>(read: &mut S) -> Option<Vec<u8>>
where
    S: StreamExt<Item = Result<Message, tokio_tungstenite::tungstenite::Error>> + Unpin,
{
    loop {
        match tokio::time::timeout(TIMEOUT, read.next()).await {
            Ok(Some(Ok(Message::Binary(data)))) => return Some(data),
            Ok(Some(Ok(Message::Close(_)))) => return None,
            Ok(Some(Ok(_))) => continue, // Skip ping/pong/text
            Ok(Some(Err(e))) => panic!("WebSocket error: {}", e),
            Ok(None) => return None, // Stream ended
            Err(_) => panic!("Timeout waiting for WebSocket message"),
        }
    }
}

/// Decode a y-websocket sync message.
/// Returns (message_type, sync_type, payload).
fn decode_sync_message(data: &[u8]) -> (u8, u8, Vec<u8>) {
    assert!(data.len() >= 2, "message too short");
    let msg_type = data[0];
    let sync_type = data[1];

    // Decode var-length payload
    let mut rest = &data[2..];
    let len = decode_var_uint(&mut rest);
    let payload = rest[..len].to_vec();

    (msg_type, sync_type, payload)
}

/// Decode variable-length unsigned integer.
fn decode_var_uint(data: &mut &[u8]) -> usize {
    let mut result: usize = 0;
    let mut shift = 0;
    loop {
        let byte = data[0];
        *data = &data[1..];
        result |= ((byte & 0x7F) as usize) << shift;
        if byte & 0x80 == 0 {
            break;
        }
        shift += 7;
    }
    result
}

/// Encode an Update message.
fn encode_update(update: &[u8]) -> Vec<u8> {
    let mut out = vec![0u8, 2u8]; // Sync message type 0, Update subtype 2
    encode_var_bytes(update, &mut out);
    out
}

/// Encode variable-length bytes.
fn encode_var_bytes(bytes: &[u8], out: &mut Vec<u8>) {
    encode_var_uint(bytes.len() as u64, out);
    out.extend_from_slice(bytes);
}

/// Encode variable-length unsigned integer.
fn encode_var_uint(value: u64, out: &mut Vec<u8>) {
    let mut v = value;
    loop {
        let mut byte = (v & 0x7F) as u8;
        v >>= 7;
        if v != 0 {
            byte |= 0x80;
        }
        out.push(byte);
        if v == 0 {
            break;
        }
    }
}

/// Simple test to verify document creation and retrieval works
#[tokio::test]
async fn test_document_http_roundtrip() {
    let addr = start_test_server().await;

    // Create a document
    let doc_id = create_document(&addr, "text/plain").await;
    assert!(!doc_id.is_empty());

    // Verify we can retrieve it
    let content = get_document_content(&addr, &doc_id).await;
    assert_eq!(content, ""); // Empty text document
}

#[tokio::test]
async fn test_ws_connect_and_initial_sync() {
    init_tracing();
    let addr = start_test_server().await;

    // Create a text document
    let doc_id = create_document(&addr, "text/plain").await;

    // Connect via WebSocket with y-websocket subprotocol
    let mut request = format!("ws://{}/ws/docs/{}", addr, doc_id)
        .into_client_request()
        .unwrap();
    request
        .headers_mut()
        .insert("Sec-WebSocket-Protocol", "y-websocket".parse().unwrap());

    let (ws_stream, response) = tokio_tungstenite::connect_async(request).await.unwrap();

    // Verify the server selected y-websocket protocol
    let protocol = response
        .headers()
        .get("Sec-WebSocket-Protocol")
        .map(|v| v.to_str().unwrap());
    assert_eq!(protocol, Some("y-websocket"));

    let (mut write, mut read) = ws_stream.split();

    // Should receive SyncStep1 (server's state vector)
    let data1 = recv_ws_msg(&mut read).await.expect("expected SyncStep1");
    let (msg_type, sync_type, _sv) = decode_sync_message(&data1);
    assert_eq!(msg_type, 0); // Sync
    assert_eq!(sync_type, 0); // SyncStep1

    // Should receive SyncStep2 (full document state)
    let data2 = recv_ws_msg(&mut read).await.expect("expected SyncStep2");
    let (msg_type2, sync_type2, _state) = decode_sync_message(&data2);
    assert_eq!(msg_type2, 0); // Sync
    assert_eq!(sync_type2, 1); // SyncStep2

    // Close cleanly
    write.close().await.unwrap();
}

#[tokio::test]
async fn test_ws_send_update_and_verify_content() {
    let (addr, _dir) = start_test_server_with_store().await;

    // Create a text document
    let doc_id = create_document(&addr, "text/plain").await;

    // Connect via WebSocket
    let mut request = format!("ws://{}/ws/docs/{}", addr, doc_id)
        .into_client_request()
        .unwrap();
    request
        .headers_mut()
        .insert("Sec-WebSocket-Protocol", "y-websocket".parse().unwrap());

    let (ws_stream, _) = tokio_tungstenite::connect_async(request).await.unwrap();
    let (mut write, mut read) = ws_stream.split();

    // Consume initial sync messages
    let _ = recv_ws_msg(&mut read).await; // SyncStep1
    let _ = recv_ws_msg(&mut read).await; // SyncStep2

    // Create a Yjs update that inserts "hello"
    let ydoc = yrs::Doc::new();
    let text = ydoc.get_or_insert_text("content");
    let update = {
        let mut txn = ydoc.transact_mut();
        text.push(&mut txn, "hello");
        txn.encode_update_v1()
    };

    // Send the update
    let ws_update = encode_update(&update);
    write.send(Message::Binary(ws_update)).await.unwrap();

    // Give server time to process
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Verify document content via HTTP
    let content = get_document_content(&addr, &doc_id).await;
    assert_eq!(content, "hello");

    write.close().await.unwrap();
}

#[tokio::test]
async fn test_ws_two_clients_bidirectional_sync() {
    let addr = start_test_server().await;

    // Create a text document
    let doc_id = create_document(&addr, "text/plain").await;

    // Connect Client A
    let mut request_a = format!("ws://{}/ws/docs/{}", addr, doc_id)
        .into_client_request()
        .unwrap();
    request_a
        .headers_mut()
        .insert("Sec-WebSocket-Protocol", "y-websocket".parse().unwrap());

    let (ws_stream_a, _) = tokio_tungstenite::connect_async(request_a).await.unwrap();
    let (mut write_a, mut read_a) = ws_stream_a.split();

    // Consume initial sync for A
    let _ = recv_ws_msg(&mut read_a).await; // SyncStep1
    let _ = recv_ws_msg(&mut read_a).await; // SyncStep2

    // Connect Client B
    let mut request_b = format!("ws://{}/ws/docs/{}", addr, doc_id)
        .into_client_request()
        .unwrap();
    request_b
        .headers_mut()
        .insert("Sec-WebSocket-Protocol", "y-websocket".parse().unwrap());

    let (ws_stream_b, _) = tokio_tungstenite::connect_async(request_b).await.unwrap();
    let (mut write_b, mut read_b) = ws_stream_b.split();

    // Consume initial sync for B
    let _ = recv_ws_msg(&mut read_b).await; // SyncStep1
    let _ = recv_ws_msg(&mut read_b).await; // SyncStep2

    // Client A sends an update: "hello"
    let ydoc_a = yrs::Doc::new();
    let text_a = ydoc_a.get_or_insert_text("content");
    let update_a = {
        let mut txn = ydoc_a.transact_mut();
        text_a.push(&mut txn, "hello");
        txn.encode_update_v1()
    };
    write_a
        .send(Message::Binary(encode_update(&update_a)))
        .await
        .unwrap();

    // Client B should receive the update
    let data_b = recv_ws_msg(&mut read_b)
        .await
        .expect("expected update on B");
    let (msg_type, sync_type, payload) = decode_sync_message(&data_b);
    assert_eq!(msg_type, 0); // Sync
    assert_eq!(sync_type, 2); // Update

    // Apply update to client B's doc and verify
    let ydoc_b = yrs::Doc::new();
    let text_b = ydoc_b.get_or_insert_text("content");
    {
        let mut txn = ydoc_b.transact_mut();
        txn.apply_update(Update::decode_v1(&payload).unwrap());
    }
    let content_b = {
        let txn = ydoc_b.transact();
        text_b.get_string(&txn)
    };
    assert_eq!(content_b, "hello");

    // Client B sends an update: append " world"
    let update_b = {
        let mut txn = ydoc_b.transact_mut();
        text_b.push(&mut txn, " world");
        txn.encode_update_v1()
    };
    write_b
        .send(Message::Binary(encode_update(&update_b)))
        .await
        .unwrap();

    // Client A should receive the update
    let data_a = recv_ws_msg(&mut read_a)
        .await
        .expect("expected update on A");
    let (msg_type2, sync_type2, payload2) = decode_sync_message(&data_a);
    assert_eq!(msg_type2, 0); // Sync
    assert_eq!(sync_type2, 2); // Update

    // Apply update to client A's doc and verify
    {
        let mut txn = ydoc_a.transact_mut();
        txn.apply_update(Update::decode_v1(&payload2).unwrap());
    }
    let content_a = {
        let txn = ydoc_a.transact();
        text_a.get_string(&txn)
    };
    assert_eq!(content_a, "hello world");

    // Clean up
    write_a.close().await.unwrap();
    write_b.close().await.unwrap();
}

#[tokio::test]
async fn test_ws_reconnect_receives_missed_updates() {
    let (addr, _dir) = start_test_server_with_store().await;

    // Create a text document
    let doc_id = create_document(&addr, "text/plain").await;

    // Client connects and receives initial state
    let mut request1 = format!("ws://{}/ws/docs/{}", addr, doc_id)
        .into_client_request()
        .unwrap();
    request1
        .headers_mut()
        .insert("Sec-WebSocket-Protocol", "y-websocket".parse().unwrap());

    let (ws_stream1, _) = tokio_tungstenite::connect_async(request1).await.unwrap();
    let (mut write1, mut read1) = ws_stream1.split();

    // Consume initial sync
    let _ = recv_ws_msg(&mut read1).await; // SyncStep1
    let _ = recv_ws_msg(&mut read1).await; // SyncStep2

    // Client sends update: "first"
    let ydoc1 = yrs::Doc::new();
    let text1 = ydoc1.get_or_insert_text("content");
    let update1 = {
        let mut txn = ydoc1.transact_mut();
        text1.push(&mut txn, "first");
        txn.encode_update_v1()
    };
    write1
        .send(Message::Binary(encode_update(&update1)))
        .await
        .unwrap();

    // Give server time to process
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Client disconnects
    write1.close().await.unwrap();
    drop(read1);

    // Modify document via HTTP while client is disconnected
    let client = reqwest::Client::new();

    // Create update for " second"
    let ydoc_http = yrs::Doc::new();
    let text_http = ydoc_http.get_or_insert_text("content");
    {
        let mut txn = ydoc_http.transact_mut();
        txn.apply_update(Update::decode_v1(&update1).unwrap());
    }
    let update_http = {
        let mut txn = ydoc_http.transact_mut();
        text_http.push(&mut txn, " second");
        txn.encode_update_v1()
    };
    let update_b64 = commonplace_doc::b64::encode(&update_http);

    let _edit_response = client
        .post(format!("http://{}/docs/{}/edit", addr, doc_id))
        .header("content-type", "application/json")
        .body(serde_json::json!({ "update": update_b64 }).to_string())
        .send()
        .await
        .unwrap();

    // Client reconnects
    let mut request2 = format!("ws://{}/ws/docs/{}", addr, doc_id)
        .into_client_request()
        .unwrap();
    request2
        .headers_mut()
        .insert("Sec-WebSocket-Protocol", "y-websocket".parse().unwrap());

    let (ws_stream2, _) = tokio_tungstenite::connect_async(request2).await.unwrap();
    let (_write2, mut read2) = ws_stream2.split();

    // Consume SyncStep1
    let _ = recv_ws_msg(&mut read2).await;

    // SyncStep2 should contain the full document state including "first second"
    let data2 = recv_ws_msg(&mut read2).await.expect("expected SyncStep2");
    let (_, _, state) = decode_sync_message(&data2);

    // Apply state to a fresh doc
    let ydoc_final = yrs::Doc::new();
    let text_final = ydoc_final.get_or_insert_text("content");
    {
        let mut txn = ydoc_final.transact_mut();
        txn.apply_update(Update::decode_v1(&state).unwrap());
    }
    let content = {
        let txn = ydoc_final.transact();
        text_final.get_string(&txn)
    };

    assert_eq!(content, "first second");
}

#[tokio::test]
async fn test_ws_subprotocol_y_websocket() {
    let addr = start_test_server().await;
    let doc_id = create_document(&addr, "text/plain").await;

    // Request y-websocket protocol
    let mut request = format!("ws://{}/ws/docs/{}", addr, doc_id)
        .into_client_request()
        .unwrap();
    request
        .headers_mut()
        .insert("Sec-WebSocket-Protocol", "y-websocket".parse().unwrap());

    let (ws_stream, response) = tokio_tungstenite::connect_async(request).await.unwrap();

    let protocol = response
        .headers()
        .get("Sec-WebSocket-Protocol")
        .map(|v| v.to_str().unwrap());
    assert_eq!(protocol, Some("y-websocket"));

    let (mut write, _) = ws_stream.split();
    write.close().await.unwrap();
}

#[tokio::test]
async fn test_ws_subprotocol_commonplace() {
    let addr = start_test_server().await;
    let doc_id = create_document(&addr, "text/plain").await;

    // Request commonplace protocol
    let mut request = format!("ws://{}/ws/docs/{}", addr, doc_id)
        .into_client_request()
        .unwrap();
    request
        .headers_mut()
        .insert("Sec-WebSocket-Protocol", "commonplace".parse().unwrap());

    let (ws_stream, response) = tokio_tungstenite::connect_async(request).await.unwrap();

    let protocol = response
        .headers()
        .get("Sec-WebSocket-Protocol")
        .map(|v| v.to_str().unwrap());
    assert_eq!(protocol, Some("commonplace"));

    let (mut write, _) = ws_stream.split();
    write.close().await.unwrap();
}

#[tokio::test]
async fn test_ws_default_protocol_is_y_websocket() {
    let addr = start_test_server().await;
    let doc_id = create_document(&addr, "text/plain").await;

    // Connect without specifying protocol
    let request = format!("ws://{}/ws/docs/{}", addr, doc_id)
        .into_client_request()
        .unwrap();

    let (ws_stream, response) = tokio_tungstenite::connect_async(request).await.unwrap();

    // Should default to y-websocket (or no header if not specified)
    let protocol = response
        .headers()
        .get("Sec-WebSocket-Protocol")
        .map(|v| v.to_str().unwrap());
    // y-websocket is the default for maximum compatibility
    assert!(
        protocol.is_none() || protocol == Some("y-websocket"),
        "Expected no protocol or y-websocket, got {:?}",
        protocol
    );

    let (mut write, _) = ws_stream.split();
    write.close().await.unwrap();
}
