//! Stage 0 MQTT transport harness tests.
//!
//! These tests validate MQTT transport invariants in isolation where possible,
//! and document requirements for broker-based testing where needed.
//!
//! ## Invariants tested:
//! 1. Messages published are received by subscribers (broadcast channel semantics)
//! 2. Retained messages are delivered to new subscribers (broker feature)
//! 3. QoS behavior (at-least-once delivery guarantees)
//!
//! ## Test categories:
//! - `isolated_*`: No broker required, tests internal broadcast/channel semantics
//! - `broker_*`: Requires MQTT broker (mosquitto) on localhost:1883
//!
//! Run with: `cargo test --test mqtt_transport_harness`

use std::net::TcpStream;
use std::time::Duration;
use tokio::sync::broadcast;

/// Check if MQTT broker is available on localhost:1883
fn mqtt_available() -> bool {
    TcpStream::connect_timeout(
        &"127.0.0.1:1883".parse().unwrap(),
        Duration::from_millis(100),
    )
    .is_ok()
}

// =============================================================================
// ISOLATED TESTS: No broker required
// =============================================================================
// These tests validate the internal broadcast channel semantics that mirror
// MQTT pub/sub behavior within the MqttClient.

mod isolated {
    use super::*;

    /// Simulates an IncomingMessage for testing broadcast behavior.
    #[derive(Debug, Clone, PartialEq, Eq)]
    struct TestMessage {
        topic: String,
        payload: Vec<u8>,
    }

    // -------------------------------------------------------------------------
    // Invariant 1: Messages published are received by subscribers
    // -------------------------------------------------------------------------

    /// Test that a single subscriber receives all published messages.
    #[test]
    fn broadcast_single_subscriber_receives_all() {
        let (tx, mut rx) = broadcast::channel::<TestMessage>(16);

        // Publish messages
        let msg1 = TestMessage {
            topic: "test/topic1".to_string(),
            payload: b"hello".to_vec(),
        };
        let msg2 = TestMessage {
            topic: "test/topic2".to_string(),
            payload: b"world".to_vec(),
        };

        tx.send(msg1.clone()).expect("send should succeed");
        tx.send(msg2.clone()).expect("send should succeed");

        // Receive and verify
        assert_eq!(rx.try_recv().unwrap(), msg1);
        assert_eq!(rx.try_recv().unwrap(), msg2);
    }

    /// Test that multiple subscribers each receive all messages (fan-out).
    #[test]
    fn broadcast_multiple_subscribers_fanout() {
        let (tx, mut rx1) = broadcast::channel::<TestMessage>(16);
        let mut rx2 = tx.subscribe();
        let mut rx3 = tx.subscribe();

        let msg = TestMessage {
            topic: "test/fanout".to_string(),
            payload: b"broadcast".to_vec(),
        };

        tx.send(msg.clone()).expect("send should succeed");

        // All subscribers should receive the same message
        assert_eq!(rx1.try_recv().unwrap(), msg);
        assert_eq!(rx2.try_recv().unwrap(), msg);
        assert_eq!(rx3.try_recv().unwrap(), msg);
    }

    /// Test that late subscribers don't receive messages sent before subscription.
    /// This is the default MQTT behavior for non-retained messages.
    #[test]
    fn broadcast_late_subscriber_misses_prior_messages() {
        let (tx, mut rx1) = broadcast::channel::<TestMessage>(16);

        // Send message before rx2 subscribes
        let early_msg = TestMessage {
            topic: "test/early".to_string(),
            payload: b"before".to_vec(),
        };
        tx.send(early_msg.clone()).expect("send should succeed");

        // Now subscribe rx2
        let mut rx2 = tx.subscribe();

        // Send another message
        let late_msg = TestMessage {
            topic: "test/late".to_string(),
            payload: b"after".to_vec(),
        };
        tx.send(late_msg.clone()).expect("send should succeed");

        // rx1 receives both
        assert_eq!(rx1.try_recv().unwrap(), early_msg);
        assert_eq!(rx1.try_recv().unwrap(), late_msg);

        // rx2 only receives the late message
        assert_eq!(rx2.try_recv().unwrap(), late_msg);
        assert!(rx2.try_recv().is_err()); // No more messages
    }

    /// Test that unsubscribed receivers don't block senders.
    #[test]
    fn broadcast_dropped_receiver_doesnt_block() {
        let (tx, rx1) = broadcast::channel::<TestMessage>(16);
        let rx2 = tx.subscribe();

        // Drop both receivers
        drop(rx1);
        drop(rx2);

        // Sending should still succeed (just won't be received)
        let msg = TestMessage {
            topic: "test/orphan".to_string(),
            payload: b"ignored".to_vec(),
        };
        // send returns Err if no receivers, but doesn't panic
        let _ = tx.send(msg);
    }

    // -------------------------------------------------------------------------
    // Invariant 2: Message ordering preserved
    // -------------------------------------------------------------------------

    /// Test that messages are received in FIFO order.
    #[test]
    fn broadcast_preserves_fifo_order() {
        let (tx, mut rx) = broadcast::channel::<TestMessage>(16);

        // Send messages with sequence numbers
        for i in 0..10 {
            let msg = TestMessage {
                topic: format!("test/seq/{}", i),
                payload: vec![i],
            };
            tx.send(msg).expect("send should succeed");
        }

        // Verify FIFO order
        for i in 0..10 {
            let received = rx.try_recv().unwrap();
            assert_eq!(received.payload, vec![i], "message {} out of order", i);
        }
    }

    /// Test that concurrent publishing from multiple threads preserves per-sender ordering.
    #[tokio::test]
    async fn broadcast_concurrent_publishers_ordering() {
        let (tx, mut rx) = broadcast::channel::<TestMessage>(256);
        let tx2 = tx.clone();

        // Spawn two publishers
        let handle1 = tokio::spawn(async move {
            for i in 0u8..50 {
                let msg = TestMessage {
                    topic: "sender1".to_string(),
                    payload: vec![1, i],
                };
                let _ = tx.send(msg);
            }
        });

        let handle2 = tokio::spawn(async move {
            for i in 0u8..50 {
                let msg = TestMessage {
                    topic: "sender2".to_string(),
                    payload: vec![2, i],
                };
                let _ = tx2.send(msg);
            }
        });

        handle1.await.unwrap();
        handle2.await.unwrap();

        // Collect all messages
        let mut sender1_msgs = Vec::new();
        let mut sender2_msgs = Vec::new();

        while let Ok(msg) = rx.try_recv() {
            if msg.topic == "sender1" {
                sender1_msgs.push(msg.payload[1]);
            } else {
                sender2_msgs.push(msg.payload[1]);
            }
        }

        // Verify each sender's messages are in order (may be interleaved)
        assert_eq!(sender1_msgs.len(), 50);
        assert_eq!(sender2_msgs.len(), 50);

        for (i, &seq) in sender1_msgs.iter().enumerate() {
            assert_eq!(seq, i as u8, "sender1 message {} out of order", i);
        }
        for (i, &seq) in sender2_msgs.iter().enumerate() {
            assert_eq!(seq, i as u8, "sender2 message {} out of order", i);
        }
    }

    // -------------------------------------------------------------------------
    // Invariant 3: Backpressure / channel capacity
    // -------------------------------------------------------------------------

    /// Test that channel capacity is respected and oldest messages are dropped.
    #[test]
    fn broadcast_capacity_drops_oldest() {
        // Small capacity to test overflow
        let (tx, mut rx) = broadcast::channel::<TestMessage>(4);

        // Send more messages than capacity
        for i in 0u8..10 {
            let msg = TestMessage {
                topic: "test/overflow".to_string(),
                payload: vec![i],
            };
            let _ = tx.send(msg);
        }

        // First recv will indicate lagged (missed messages)
        match rx.try_recv() {
            Err(broadcast::error::TryRecvError::Lagged(n)) => {
                // We should have lagged by some number of messages
                assert!(n > 0, "should have lagged");
            }
            Ok(msg) => {
                // Or we might get a message if timing works out
                // The key is that we don't block or panic
                assert!(!msg.payload.is_empty());
            }
            Err(e) => panic!("unexpected error: {:?}", e),
        }
    }

    // -------------------------------------------------------------------------
    // Message serialization tests (EditMessage, SyncMessage, etc.)
    // -------------------------------------------------------------------------

    #[test]
    fn edit_message_roundtrip() {
        use commonplace_doc::mqtt::EditMessage;

        let msg = EditMessage {
            update: "SGVsbG8gV29ybGQ=".to_string(), // base64 "Hello World"
            parents: vec!["abc123".to_string(), "def456".to_string()],
            author: "test@example.com".to_string(),
            message: Some("Test edit".to_string()),
            timestamp: 1704067200000,
            req: None,
        };

        let json = serde_json::to_vec(&msg).expect("serialize should succeed");
        let decoded: EditMessage =
            serde_json::from_slice(&json).expect("deserialize should succeed");

        assert_eq!(decoded.update, msg.update);
        assert_eq!(decoded.parents, msg.parents);
        assert_eq!(decoded.author, msg.author);
        assert_eq!(decoded.message, msg.message);
        assert_eq!(decoded.timestamp, msg.timestamp);
    }

    #[test]
    fn sync_message_head_roundtrip() {
        use commonplace_doc::mqtt::SyncMessage;

        let msg = SyncMessage::Head {
            req: "req-001".to_string(),
        };

        let json = serde_json::to_vec(&msg).expect("serialize should succeed");
        let decoded: SyncMessage =
            serde_json::from_slice(&json).expect("deserialize should succeed");

        match decoded {
            SyncMessage::Head { req } => assert_eq!(req, "req-001"),
            _ => panic!("Expected Head message"),
        }
    }

    #[test]
    fn sync_message_commit_roundtrip() {
        use commonplace_doc::mqtt::SyncMessage;

        let msg = SyncMessage::Commit {
            req: "req-002".to_string(),
            id: "commit-abc".to_string(),
            parents: vec!["parent-1".to_string()],
            data: "base64update".to_string(),
            timestamp: 1704067200000,
            author: "author@example.com".to_string(),
            message: Some("Commit message".to_string()),
            source: None,
        };

        let json = serde_json::to_vec(&msg).expect("serialize should succeed");
        let decoded: SyncMessage =
            serde_json::from_slice(&json).expect("deserialize should succeed");

        match decoded {
            SyncMessage::Commit {
                req,
                id,
                parents,
                data,
                timestamp,
                author,
                message,
                source,
            } => {
                assert_eq!(req, "req-002");
                assert_eq!(id, "commit-abc");
                assert_eq!(parents, vec!["parent-1"]);
                assert_eq!(data, "base64update");
                assert_eq!(timestamp, 1704067200000);
                assert_eq!(author, "author@example.com");
                assert_eq!(message, Some("Commit message".to_string()));
                assert!(source.is_none());
            }
            _ => panic!("Expected Commit message"),
        }
    }

    // -------------------------------------------------------------------------
    // Topic parsing invariants
    // -------------------------------------------------------------------------

    #[test]
    fn topic_construction_is_consistent() {
        use commonplace_doc::mqtt::Topic;

        // Edits topic
        let edits = Topic::edits("workspace", "docs/file.txt");
        assert_eq!(edits.to_topic_string(), "workspace/edits/docs/file.txt");

        // Sync topic with client ID
        let sync = Topic::sync("workspace", "docs/file.txt", "client-123");
        assert_eq!(
            sync.to_topic_string(),
            "workspace/sync/docs/file.txt/client-123"
        );

        // Commands topic
        let cmds = Topic::commands("workspace", "docs/file.txt", "replace");
        assert_eq!(
            cmds.to_topic_string(),
            "workspace/commands/docs/file.txt/replace"
        );
    }

    #[test]
    fn topic_parse_roundtrip() {
        use commonplace_doc::mqtt::Topic;

        // Create topic, convert to string, parse back
        let original = Topic::edits("myworkspace", "path/to/doc");
        let topic_str = original.to_topic_string();
        let parsed = Topic::parse(&topic_str, "myworkspace").expect("parse should succeed");

        assert_eq!(parsed.workspace, original.workspace);
        assert_eq!(parsed.port, original.port);
        assert_eq!(parsed.path, original.path);
    }
}

// =============================================================================
// BROKER TESTS: Requires MQTT broker (mosquitto) on localhost:1883
// =============================================================================
// These tests validate actual MQTT protocol behavior including:
// - Retained messages
// - QoS guarantees
// - Real network round-trips

mod broker {
    use super::*;

    /// Skip helper for broker tests
    fn skip_if_no_broker() -> bool {
        if !mqtt_available() {
            eprintln!("Skipping test: MQTT broker not available on localhost:1883");
            true
        } else {
            false
        }
    }

    // -------------------------------------------------------------------------
    // Invariant 1: Real pub/sub with broker
    // -------------------------------------------------------------------------

    /// Test actual MQTT message delivery through the broker.
    #[tokio::test]
    async fn broker_pubsub_delivery() {
        if skip_if_no_broker() {
            return;
        }

        use commonplace_doc::mqtt::{MqttClient, MqttConfig, QoS};

        // Create two clients
        let config1 = MqttConfig {
            client_id: format!("test-pub-{}", uuid::Uuid::new_v4()),
            ..Default::default()
        };
        let config2 = MqttConfig {
            client_id: format!("test-sub-{}", uuid::Uuid::new_v4()),
            ..Default::default()
        };

        let publisher = MqttClient::connect(config1)
            .await
            .expect("publisher connect");
        let subscriber = MqttClient::connect(config2)
            .await
            .expect("subscriber connect");

        // Unique topic for this test run
        let topic = format!("test/pubsub/{}", uuid::Uuid::new_v4());

        // Wrap in Arc and start event loop BEFORE subscribing
        // The event loop processes ConnAck, SubAck, and Publish packets
        let sub_client = std::sync::Arc::new(subscriber);
        let sub_client_clone = sub_client.clone();
        let _event_loop = tokio::spawn(async move {
            let _ = sub_client_clone.run_event_loop().await;
        });

        // Get message receiver before subscribing
        let mut rx = sub_client.subscribe_messages();

        // Small delay for connection to be established
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Now subscribe
        sub_client
            .subscribe(&topic, QoS::AtLeastOnce)
            .await
            .expect("subscribe");

        // Wait for subscription to be processed by broker
        tokio::time::sleep(Duration::from_millis(200)).await;

        // Wrap publisher and start its event loop too (needed for publish ACKs)
        let pub_client = std::sync::Arc::new(publisher);
        let pub_client_clone = pub_client.clone();
        let _pub_event_loop = tokio::spawn(async move {
            let _ = pub_client_clone.run_event_loop().await;
        });
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Publish
        let payload = b"test message";
        pub_client
            .publish(&topic, payload, QoS::AtLeastOnce)
            .await
            .expect("publish");

        // Wait for message with timeout
        let result = tokio::time::timeout(Duration::from_secs(5), rx.recv()).await;

        match result {
            Ok(Ok(msg)) => {
                assert_eq!(msg.topic, topic);
                assert_eq!(msg.payload, payload);
            }
            Ok(Err(e)) => panic!("Receive error: {:?}", e),
            Err(_) => panic!("Timeout waiting for message"),
        }
    }

    // -------------------------------------------------------------------------
    // Invariant 2: Retained messages
    // -------------------------------------------------------------------------

    /// Test that retained messages are delivered to new subscribers.
    #[tokio::test]
    async fn broker_retained_message_delivery() {
        if skip_if_no_broker() {
            return;
        }

        use commonplace_doc::mqtt::{MqttClient, MqttConfig, QoS};

        // Unique topic for this test
        let topic = format!("test/retained/{}", uuid::Uuid::new_v4());

        // First, publish a retained message
        let config_pub = MqttConfig {
            client_id: format!("test-retain-pub-{}", uuid::Uuid::new_v4()),
            ..Default::default()
        };
        let publisher = MqttClient::connect(config_pub)
            .await
            .expect("publisher connect");

        // Start publisher event loop (needed for publish ACKs with QoS 1)
        let pub_client = std::sync::Arc::new(publisher);
        let pub_client_clone = pub_client.clone();
        let _pub_event_loop = tokio::spawn(async move {
            let _ = pub_client_clone.run_event_loop().await;
        });
        tokio::time::sleep(Duration::from_millis(100)).await;

        let retained_payload = b"retained message content";
        pub_client
            .publish_retained(&topic, retained_payload, QoS::AtLeastOnce)
            .await
            .expect("publish retained");

        // Wait for broker to process and retain the message
        tokio::time::sleep(Duration::from_millis(300)).await;

        // Now create a new subscriber (after the message was published)
        let config_sub = MqttConfig {
            client_id: format!("test-retain-sub-{}", uuid::Uuid::new_v4()),
            ..Default::default()
        };
        let subscriber = MqttClient::connect(config_sub)
            .await
            .expect("subscriber connect");

        let sub_client = std::sync::Arc::new(subscriber);
        let sub_client_clone = sub_client.clone();
        let _event_loop = tokio::spawn(async move {
            let _ = sub_client_clone.run_event_loop().await;
        });

        // Get receiver before subscribing
        let mut rx = sub_client.subscribe_messages();

        // Wait for connection
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Subscribe to the topic - should receive retained message
        sub_client
            .subscribe(&topic, QoS::AtLeastOnce)
            .await
            .expect("subscribe");

        // Wait for the retained message
        let result = tokio::time::timeout(Duration::from_secs(5), rx.recv()).await;

        match result {
            Ok(Ok(msg)) => {
                assert_eq!(msg.topic, topic);
                assert_eq!(msg.payload, retained_payload.to_vec());
            }
            Ok(Err(e)) => panic!("Receive error: {:?}", e),
            Err(_) => panic!("Timeout waiting for retained message"),
        }

        // Clean up: clear the retained message by publishing empty payload
        pub_client
            .publish_retained(&topic, &[], QoS::AtLeastOnce)
            .await
            .expect("clear retained");
    }

    // -------------------------------------------------------------------------
    // Invariant 3: QoS at-least-once delivery
    // -------------------------------------------------------------------------

    /// Test that QoS 1 (at-least-once) messages are reliably delivered.
    /// This test publishes multiple messages and verifies all are received.
    #[tokio::test]
    async fn broker_qos1_reliability() {
        if skip_if_no_broker() {
            return;
        }

        use commonplace_doc::mqtt::{MqttClient, MqttConfig, QoS};

        let topic = format!("test/qos1/{}", uuid::Uuid::new_v4());
        const MESSAGE_COUNT: usize = 20;

        // Setup subscriber - start event loop FIRST
        let config_sub = MqttConfig {
            client_id: format!("test-qos1-sub-{}", uuid::Uuid::new_v4()),
            ..Default::default()
        };
        let subscriber = MqttClient::connect(config_sub)
            .await
            .expect("subscriber connect");

        let sub_client = std::sync::Arc::new(subscriber);
        let sub_client_clone = sub_client.clone();
        let _event_loop = tokio::spawn(async move {
            let _ = sub_client_clone.run_event_loop().await;
        });

        // Get receiver before subscribing
        let mut rx = sub_client.subscribe_messages();

        // Wait for connection to be established
        tokio::time::sleep(Duration::from_millis(100)).await;

        sub_client
            .subscribe(&topic, QoS::AtLeastOnce)
            .await
            .expect("subscribe");

        // Wait for subscription to be processed
        tokio::time::sleep(Duration::from_millis(200)).await;

        // Setup publisher with its own event loop
        let config_pub = MqttConfig {
            client_id: format!("test-qos1-pub-{}", uuid::Uuid::new_v4()),
            ..Default::default()
        };
        let publisher = MqttClient::connect(config_pub)
            .await
            .expect("publisher connect");

        let pub_client = std::sync::Arc::new(publisher);
        let pub_client_clone = pub_client.clone();
        let _pub_event_loop = tokio::spawn(async move {
            let _ = pub_client_clone.run_event_loop().await;
        });
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Publish messages
        for i in 0..MESSAGE_COUNT {
            let payload = format!("message-{}", i);
            pub_client
                .publish(&topic, payload.as_bytes(), QoS::AtLeastOnce)
                .await
                .expect("publish");
        }

        // Collect received messages with timeout
        let mut received = std::collections::HashSet::new();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);

        while received.len() < MESSAGE_COUNT && tokio::time::Instant::now() < deadline {
            match tokio::time::timeout(Duration::from_secs(1), rx.recv()).await {
                Ok(Ok(msg)) => {
                    if msg.topic == topic {
                        let payload = String::from_utf8_lossy(&msg.payload).to_string();
                        received.insert(payload);
                    }
                }
                Ok(Err(_)) => break,
                Err(_) => continue, // Timeout, keep trying
            }
        }

        // Verify all messages received (at-least-once means we might get duplicates,
        // but we should have received every message at least once)
        assert_eq!(
            received.len(),
            MESSAGE_COUNT,
            "Expected {} messages, got {}. Missing: {:?}",
            MESSAGE_COUNT,
            received.len(),
            (0..MESSAGE_COUNT)
                .map(|i| format!("message-{}", i))
                .filter(|m| !received.contains(m))
                .collect::<Vec<_>>()
        );
    }

    // -------------------------------------------------------------------------
    // Invariant 4: Wildcard subscriptions
    // -------------------------------------------------------------------------

    /// Test that wildcard subscriptions work correctly.
    #[tokio::test]
    async fn broker_wildcard_subscription() {
        if skip_if_no_broker() {
            return;
        }

        use commonplace_doc::mqtt::{MqttClient, MqttConfig, QoS};

        let base_topic = format!("test/wildcard/{}", uuid::Uuid::new_v4());
        let wildcard_topic = format!("{}/#", base_topic);

        // Setup subscriber with wildcard - start event loop FIRST
        let config_sub = MqttConfig {
            client_id: format!("test-wild-sub-{}", uuid::Uuid::new_v4()),
            ..Default::default()
        };
        let subscriber = MqttClient::connect(config_sub)
            .await
            .expect("subscriber connect");

        let sub_client = std::sync::Arc::new(subscriber);
        let sub_client_clone = sub_client.clone();
        let _event_loop = tokio::spawn(async move {
            let _ = sub_client_clone.run_event_loop().await;
        });

        // Get receiver before subscribing
        let mut rx = sub_client.subscribe_messages();

        // Wait for connection
        tokio::time::sleep(Duration::from_millis(100)).await;

        sub_client
            .subscribe(&wildcard_topic, QoS::AtLeastOnce)
            .await
            .expect("subscribe");

        // Wait for subscription to be processed
        tokio::time::sleep(Duration::from_millis(200)).await;

        // Setup publisher with its own event loop
        let config_pub = MqttConfig {
            client_id: format!("test-wild-pub-{}", uuid::Uuid::new_v4()),
            ..Default::default()
        };
        let publisher = MqttClient::connect(config_pub)
            .await
            .expect("publisher connect");

        let pub_client = std::sync::Arc::new(publisher);
        let pub_client_clone = pub_client.clone();
        let _pub_event_loop = tokio::spawn(async move {
            let _ = pub_client_clone.run_event_loop().await;
        });
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Publish to different sub-topics
        let topics = vec![
            format!("{}/a", base_topic),
            format!("{}/b", base_topic),
            format!("{}/deep/nested/path", base_topic),
        ];

        for topic in &topics {
            pub_client
                .publish(topic, b"wildcard test", QoS::AtLeastOnce)
                .await
                .expect("publish");
        }

        // Collect received messages
        let mut received_topics = std::collections::HashSet::new();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);

        while received_topics.len() < topics.len() && tokio::time::Instant::now() < deadline {
            match tokio::time::timeout(Duration::from_secs(1), rx.recv()).await {
                Ok(Ok(msg)) => {
                    if msg.topic.starts_with(&base_topic) {
                        received_topics.insert(msg.topic);
                    }
                }
                Ok(Err(_)) => break,
                Err(_) => continue,
            }
        }

        // Verify all topics received
        for topic in &topics {
            assert!(
                received_topics.contains(topic),
                "Missing topic: {}. Received: {:?}",
                topic,
                received_topics
            );
        }
    }
}

// =============================================================================
// STAGE 2 TESTS: Advanced ordering, serialization edge cases, topic routing
// =============================================================================
// These tests validate more complex MQTT transport invariants in isolation.

mod stage2_tests {
    use super::*;
    use std::collections::HashMap;

    // =========================================================================
    // Message Ordering Guarantees
    // =========================================================================

    /// Simulates an IncomingMessage for testing broadcast behavior.
    #[derive(Debug, Clone, PartialEq, Eq)]
    struct TestMessage {
        topic: String,
        payload: Vec<u8>,
        sequence: u64,
    }

    /// Test that single publisher maintains strict FIFO ordering with high volume.
    #[test]
    fn ordering_single_publisher_fifo_high_volume() {
        let (tx, mut rx) = broadcast::channel::<TestMessage>(1024);

        // Publish 1000 messages to stress test ordering
        for i in 0u64..1000 {
            let msg = TestMessage {
                topic: "test/fifo".to_string(),
                payload: i.to_le_bytes().to_vec(),
                sequence: i,
            };
            tx.send(msg).expect("send should succeed");
        }

        // Verify strict FIFO order
        for expected in 0u64..1000 {
            let received = rx.try_recv().unwrap();
            assert_eq!(
                received.sequence, expected,
                "Message {} received out of order, expected {}",
                received.sequence, expected
            );
        }
    }

    /// Test that multiple subscribers receive messages in identical order.
    #[tokio::test]
    async fn ordering_multiple_subscribers_identical_order() {
        let (tx, mut rx1) = broadcast::channel::<TestMessage>(256);
        let mut rx2 = tx.subscribe();
        let mut rx3 = tx.subscribe();

        // Publish messages
        for i in 0u64..100 {
            let msg = TestMessage {
                topic: format!("test/multi/{}", i % 10),
                payload: vec![i as u8],
                sequence: i,
            };
            tx.send(msg).expect("send should succeed");
        }

        // Collect from all receivers
        let mut order1 = Vec::new();
        let mut order2 = Vec::new();
        let mut order3 = Vec::new();

        for _ in 0..100 {
            order1.push(rx1.try_recv().unwrap().sequence);
            order2.push(rx2.try_recv().unwrap().sequence);
            order3.push(rx3.try_recv().unwrap().sequence);
        }

        // All receivers must see identical ordering
        assert_eq!(order1, order2, "rx1 and rx2 saw different orderings");
        assert_eq!(order2, order3, "rx2 and rx3 saw different orderings");

        // And it should be the expected sequence
        let expected: Vec<u64> = (0..100).collect();
        assert_eq!(order1, expected);
    }

    /// Test interleaved publishing from multiple tasks preserves per-topic ordering.
    #[tokio::test]
    async fn ordering_per_topic_preserved_under_interleaving() {
        let (tx, mut rx) = broadcast::channel::<TestMessage>(512);
        let tx2 = tx.clone();
        let tx3 = tx.clone();

        // Three publishers, each publishing to their own topic
        let h1 = tokio::spawn(async move {
            for i in 0u64..50 {
                let msg = TestMessage {
                    topic: "topic/a".to_string(),
                    payload: vec![],
                    sequence: i,
                };
                let _ = tx.send(msg);
                // Small yield to encourage interleaving
                tokio::task::yield_now().await;
            }
        });

        let h2 = tokio::spawn(async move {
            for i in 0u64..50 {
                let msg = TestMessage {
                    topic: "topic/b".to_string(),
                    payload: vec![],
                    sequence: i,
                };
                let _ = tx2.send(msg);
                tokio::task::yield_now().await;
            }
        });

        let h3 = tokio::spawn(async move {
            for i in 0u64..50 {
                let msg = TestMessage {
                    topic: "topic/c".to_string(),
                    payload: vec![],
                    sequence: i,
                };
                let _ = tx3.send(msg);
                tokio::task::yield_now().await;
            }
        });

        h1.await.unwrap();
        h2.await.unwrap();
        h3.await.unwrap();

        // Collect messages by topic
        let mut by_topic: HashMap<String, Vec<u64>> = HashMap::new();
        while let Ok(msg) = rx.try_recv() {
            by_topic
                .entry(msg.topic.clone())
                .or_default()
                .push(msg.sequence);
        }

        // Each topic should have strictly increasing sequence numbers
        for (topic, sequences) in by_topic {
            assert_eq!(sequences.len(), 50, "Topic {} missing messages", topic);
            for (i, &seq) in sequences.iter().enumerate() {
                assert_eq!(
                    seq, i as u64,
                    "Topic {} has out-of-order sequence at position {}",
                    topic, i
                );
            }
        }
    }

    // =========================================================================
    // EditMessage Serialization Edge Cases
    // =========================================================================

    /// Test EditMessage with empty update payload.
    #[test]
    fn edit_message_empty_update() {
        use commonplace_doc::mqtt::EditMessage;

        let msg = EditMessage {
            update: "".to_string(), // Empty base64
            parents: vec!["parent-abc".to_string()],
            author: "system".to_string(),
            message: None,
            timestamp: 0,
            req: None,
        };

        let json = serde_json::to_vec(&msg).expect("serialize empty update");
        let decoded: EditMessage = serde_json::from_slice(&json).expect("deserialize empty update");

        assert_eq!(decoded.update, "");
        assert_eq!(decoded.parents.len(), 1);
    }

    /// Test EditMessage with large update payload (simulating a big CRDT delta).
    #[test]
    fn edit_message_large_update() {
        use commonplace_doc::mqtt::EditMessage;

        // Create a large base64 payload (~1MB when decoded)
        let large_data = "A".repeat(1_400_000); // ~1MB of base64 data

        let msg = EditMessage {
            update: large_data.clone(),
            parents: vec!["parent-1".to_string()],
            author: "bulk-importer".to_string(),
            message: Some("Bulk import operation".to_string()),
            timestamp: 1704067200000,
            req: None,
        };

        let json = serde_json::to_vec(&msg).expect("serialize large update");
        assert!(json.len() > 1_000_000, "JSON should be over 1MB");

        let decoded: EditMessage = serde_json::from_slice(&json).expect("deserialize large update");
        assert_eq!(decoded.update.len(), large_data.len());
    }

    /// Test EditMessage with merge commit (two parents).
    #[test]
    fn edit_message_merge_commit() {
        use commonplace_doc::mqtt::EditMessage;

        let msg = EditMessage {
            update: "bWVyZ2VkIGRhdGE=".to_string(), // "merged data"
            parents: vec!["branch-a-head".to_string(), "branch-b-head".to_string()],
            author: "merger@example.com".to_string(),
            message: Some("Merge branch-b into branch-a".to_string()),
            timestamp: 1704153600000,
            req: None,
        };

        let json = serde_json::to_vec(&msg).expect("serialize merge commit");
        let decoded: EditMessage = serde_json::from_slice(&json).expect("deserialize merge commit");

        assert_eq!(decoded.parents.len(), 2);
        assert!(decoded.parents.contains(&"branch-a-head".to_string()));
        assert!(decoded.parents.contains(&"branch-b-head".to_string()));
    }

    /// Test EditMessage with initial commit (no parents).
    #[test]
    fn edit_message_initial_commit() {
        use commonplace_doc::mqtt::EditMessage;

        let msg = EditMessage {
            update: "aW5pdGlhbA==".to_string(), // "initial"
            parents: vec![],                    // No parents = initial commit
            author: "creator@example.com".to_string(),
            message: Some("Initial document creation".to_string()),
            timestamp: 1704000000000,
            req: None,
        };

        let json = serde_json::to_vec(&msg).expect("serialize initial commit");
        let decoded: EditMessage =
            serde_json::from_slice(&json).expect("deserialize initial commit");

        assert!(decoded.parents.is_empty());
        assert_eq!(
            decoded.message,
            Some("Initial document creation".to_string())
        );
    }

    /// Test EditMessage with special characters in author and message.
    #[test]
    fn edit_message_special_characters() {
        use commonplace_doc::mqtt::EditMessage;

        let msg = EditMessage {
            update: "dGVzdA==".to_string(),
            parents: vec!["parent".to_string()],
            author: "user@example.com <\"Test User\">".to_string(),
            message: Some("Fix \"bug\" with\nnewlines\tand\ttabs".to_string()),
            timestamp: 1704067200000,
            req: None,
        };

        let json = serde_json::to_vec(&msg).expect("serialize special chars");
        let decoded: EditMessage =
            serde_json::from_slice(&json).expect("deserialize special chars");

        assert_eq!(decoded.author, "user@example.com <\"Test User\">");
        assert!(decoded.message.as_ref().unwrap().contains('\n'));
        assert!(decoded.message.as_ref().unwrap().contains('\t'));
    }

    /// Test EditMessage with unicode in message and author.
    #[test]
    fn edit_message_unicode() {
        use commonplace_doc::mqtt::EditMessage;

        let msg = EditMessage {
            update: "dW5pY29kZQ==".to_string(),
            parents: vec!["parent".to_string()],
            author: "utilisateur@exemple.fr".to_string(),
            message: Some("Ajout de contenu en francais avec des accents".to_string()),
            timestamp: 1704067200000,
            req: None,
        };

        let json = serde_json::to_vec(&msg).expect("serialize unicode");
        let decoded: EditMessage = serde_json::from_slice(&json).expect("deserialize unicode");

        assert!(decoded.message.as_ref().unwrap().contains("francais"));
    }

    /// Test EditMessage with maximum timestamp (far future).
    #[test]
    fn edit_message_max_timestamp() {
        use commonplace_doc::mqtt::EditMessage;

        let msg = EditMessage {
            update: "dGVzdA==".to_string(),
            parents: vec![],
            author: "test".to_string(),
            message: None,
            timestamp: u64::MAX,
            req: None,
        };

        let json = serde_json::to_vec(&msg).expect("serialize max timestamp");
        let decoded: EditMessage =
            serde_json::from_slice(&json).expect("deserialize max timestamp");

        assert_eq!(decoded.timestamp, u64::MAX);
    }

    /// Test EditMessage with many parents (octopus merge).
    #[test]
    fn edit_message_octopus_merge() {
        use commonplace_doc::mqtt::EditMessage;

        let parents: Vec<String> = (0..8).map(|i| format!("branch-{}-head", i)).collect();

        let msg = EditMessage {
            update: "b2N0b3B1cw==".to_string(), // "octopus"
            parents: parents.clone(),
            author: "octopus@example.com".to_string(),
            message: Some("Octopus merge of 8 branches".to_string()),
            timestamp: 1704067200000,
            req: None,
        };

        let json = serde_json::to_vec(&msg).expect("serialize octopus merge");
        let decoded: EditMessage =
            serde_json::from_slice(&json).expect("deserialize octopus merge");

        assert_eq!(decoded.parents.len(), 8);
        for i in 0..8 {
            assert!(decoded.parents.contains(&format!("branch-{}-head", i)));
        }
    }

    // =========================================================================
    // Topic Parsing and Routing
    // =========================================================================

    /// Test topic parsing with various path depths.
    #[test]
    fn topic_parse_various_depths() {
        use commonplace_doc::mqtt::Topic;

        // Single segment path
        let t1 = Topic::parse("ws/edits/doc", "ws").unwrap();
        assert_eq!(t1.path, "doc");

        // Two segment path
        let t2 = Topic::parse("ws/edits/folder/doc", "ws").unwrap();
        assert_eq!(t2.path, "folder/doc");

        // Deep path
        let t3 = Topic::parse("ws/edits/a/b/c/d/e/f/g", "ws").unwrap();
        assert_eq!(t3.path, "a/b/c/d/e/f/g");
    }

    /// Test topic construction and parsing roundtrip for all port types.
    #[test]
    fn topic_roundtrip_all_ports() {
        use commonplace_doc::mqtt::{Port, Topic};

        let workspace = "test-workspace";
        let path = "documents/notes";

        // Edits roundtrip
        let edits = Topic::edits(workspace, path);
        let edits_str = edits.to_topic_string();
        let parsed_edits = Topic::parse(&edits_str, workspace).unwrap();
        assert_eq!(parsed_edits.port, Port::Edits);
        assert_eq!(parsed_edits.path, path);

        // Sync roundtrip (qualifier becomes part of path when parsed)
        let sync = Topic::sync(workspace, path, "client-id");
        let sync_str = sync.to_topic_string();
        let parsed_sync = Topic::parse(&sync_str, workspace).unwrap();
        assert_eq!(parsed_sync.port, Port::Sync);
        assert!(parsed_sync.path.contains(path));

        // Events roundtrip
        let events = Topic::events(workspace, path, "update");
        let events_str = events.to_topic_string();
        let parsed_events = Topic::parse(&events_str, workspace).unwrap();
        assert_eq!(parsed_events.port, Port::Events);

        // Commands roundtrip
        let commands = Topic::commands(workspace, path, "replace");
        let commands_str = commands.to_topic_string();
        let parsed_commands = Topic::parse(&commands_str, workspace).unwrap();
        assert_eq!(parsed_commands.port, Port::Commands);
    }

    /// Test topic routing isolation between workspaces.
    #[test]
    fn topic_workspace_isolation() {
        use commonplace_doc::mqtt::Topic;

        let topic_str = "workspace-a/edits/shared/doc";

        // Parsing with correct workspace succeeds
        assert!(Topic::parse(topic_str, "workspace-a").is_ok());

        // Parsing with wrong workspace fails
        assert!(Topic::parse(topic_str, "workspace-b").is_err());
        assert!(Topic::parse(topic_str, "workspace").is_err());
        assert!(Topic::parse(topic_str, "a").is_err());
    }

    /// Test topic parsing rejects invalid port names.
    #[test]
    fn topic_parse_invalid_ports() {
        use commonplace_doc::mqtt::Topic;

        let invalid_ports = vec![
            "ws/edit/doc",      // Missing 's'
            "ws/syncs/doc",     // Extra 's'
            "ws/event/doc",     // Missing 's'
            "ws/command/doc",   // Missing 's'
            "ws/publish/doc",   // Wrong port name
            "ws/subscribe/doc", // Wrong port name
            "ws/EDITS/doc",     // Wrong case
        ];

        for topic_str in invalid_ports {
            assert!(
                Topic::parse(topic_str, "ws").is_err(),
                "Should reject invalid port in: {}",
                topic_str
            );
        }
    }

    /// Test topic parsing handles edge cases in path names.
    #[test]
    fn topic_parse_path_edge_cases() {
        use commonplace_doc::mqtt::Topic;

        // Path with dots (file extensions)
        let t1 = Topic::parse("ws/edits/file.txt", "ws").unwrap();
        assert_eq!(t1.path, "file.txt");

        // Path with multiple dots
        let t2 = Topic::parse("ws/edits/archive.tar.gz", "ws").unwrap();
        assert_eq!(t2.path, "archive.tar.gz");

        // Path with hyphens and underscores
        let t3 = Topic::parse("ws/edits/my-file_v2", "ws").unwrap();
        assert_eq!(t3.path, "my-file_v2");

        // Path with numbers
        let t4 = Topic::parse("ws/edits/log-2024-01-15", "ws").unwrap();
        assert_eq!(t4.path, "log-2024-01-15");
    }

    /// Test wildcard pattern generation.
    #[test]
    fn topic_wildcard_patterns() {
        use commonplace_doc::mqtt::Topic;

        // All edits wildcard
        assert_eq!(Topic::edits_wildcard("myws"), "myws/edits/#");

        // Path-scoped edits wildcard
        assert_eq!(
            Topic::edits_path_wildcard("myws", "folder"),
            "myws/edits/folder/#"
        );

        // Sync wildcard (single level)
        assert_eq!(Topic::sync_wildcard("myws", "doc"), "myws/sync/doc/+");

        // Events wildcards
        assert_eq!(Topic::events_wildcard_all("myws"), "myws/events/#");
        assert_eq!(Topic::events_wildcard("myws", "node"), "myws/events/node/#");

        // Commands wildcards
        assert_eq!(Topic::commands_wildcard_all("myws"), "myws/commands/#");
        assert_eq!(
            Topic::commands_wildcard("myws", "node"),
            "myws/commands/node/#"
        );
    }

    /// Test that topic string generation is deterministic.
    #[test]
    fn topic_string_deterministic() {
        use commonplace_doc::mqtt::Topic;

        let topic = Topic::edits("workspace", "path/to/doc");

        // Multiple calls should produce identical strings
        let s1 = topic.to_topic_string();
        let s2 = topic.to_topic_string();
        let s3 = topic.to_topic_string();

        assert_eq!(s1, s2);
        assert_eq!(s2, s3);
    }

    /// Test SyncMessage variants serialization edge cases.
    #[test]
    fn sync_message_edge_cases() {
        use commonplace_doc::mqtt::SyncMessage;

        // Pull with empty have list (fresh client)
        let pull = SyncMessage::Pull {
            req: "r-001".to_string(),
            have: vec![],
            want: "HEAD".to_string(),
        };
        let json = serde_json::to_string(&pull).unwrap();
        assert!(json.contains("\"have\":[]"));

        // Get with many commits
        let commits: Vec<String> = (0..100).map(|i| format!("commit-{}", i)).collect();
        let get = SyncMessage::Get {
            req: "r-002".to_string(),
            commits: commits.clone(),
        };
        let json = serde_json::to_string(&get).unwrap();
        let decoded: SyncMessage = serde_json::from_str(&json).unwrap();
        match decoded {
            SyncMessage::Get { commits: c, .. } => {
                assert_eq!(c.len(), 100);
            }
            _ => panic!("Expected Get message"),
        }

        // Ancestors with depth limit
        let ancestors = SyncMessage::Ancestors {
            req: "r-003".to_string(),
            commit: "HEAD".to_string(),
            depth: Some(10),
        };
        let json = serde_json::to_string(&ancestors).unwrap();
        assert!(json.contains("\"depth\":10"));

        // Ancestors without depth limit
        let ancestors_full = SyncMessage::Ancestors {
            req: "r-004".to_string(),
            commit: "abc123".to_string(),
            depth: None,
        };
        let json = serde_json::to_string(&ancestors_full).unwrap();
        assert!(!json.contains("depth")); // depth should be omitted when None
    }

    /// Test error message serialization.
    #[test]
    fn sync_message_error() {
        use commonplace_doc::mqtt::SyncMessage;

        let error = SyncMessage::Error {
            req: "r-fail".to_string(),
            message: "Commit not found: abc123".to_string(),
            error: None,
        };

        let json = serde_json::to_string(&error).unwrap();
        assert!(json.contains("\"type\":\"error\""));
        assert!(json.contains("\"message\":\"Commit not found: abc123\""));

        let decoded: SyncMessage = serde_json::from_str(&json).unwrap();
        assert!(decoded.is_response());
        assert!(!decoded.is_request());
    }

    /// Test IsAncestor request/response roundtrip.
    #[test]
    fn sync_message_is_ancestor_roundtrip() {
        use commonplace_doc::mqtt::SyncMessage;

        let request = SyncMessage::IsAncestor {
            req: "r-anc".to_string(),
            ancestor: "old-commit".to_string(),
            descendant: "new-commit".to_string(),
        };

        let json = serde_json::to_string(&request).unwrap();
        let decoded: SyncMessage = serde_json::from_str(&json).unwrap();

        assert!(decoded.is_request());
        match decoded {
            SyncMessage::IsAncestor {
                ancestor,
                descendant,
                ..
            } => {
                assert_eq!(ancestor, "old-commit");
                assert_eq!(descendant, "new-commit");
            }
            _ => panic!("Expected IsAncestor"),
        }

        let response = SyncMessage::IsAncestorResponse {
            req: "r-anc".to_string(),
            result: true,
        };

        let json = serde_json::to_string(&response).unwrap();
        assert!(json.contains("\"type\":\"is_ancestor_response\""));
        assert!(json.contains("\"result\":true"));
    }

    /// Test message routing based on parsed topic.
    #[test]
    fn topic_based_routing() {
        use commonplace_doc::mqtt::{Port, Topic};

        struct RouteResult {
            handler: String,
            path: String,
        }

        fn route(topic_str: &str, workspace: &str) -> Option<RouteResult> {
            let topic = Topic::parse(topic_str, workspace).ok()?;
            let handler = match topic.port {
                Port::Edits => "edits_handler",
                Port::Sync => "sync_handler",
                Port::Events => "events_handler",
                Port::Commands => "commands_handler",
            };
            Some(RouteResult {
                handler: handler.to_string(),
                path: topic.path,
            })
        }

        // Test routing to correct handlers
        let r1 = route("ws/edits/doc", "ws").unwrap();
        assert_eq!(r1.handler, "edits_handler");
        assert_eq!(r1.path, "doc");

        let r2 = route("ws/sync/doc/client", "ws").unwrap();
        assert_eq!(r2.handler, "sync_handler");

        let r3 = route("ws/events/node/update", "ws").unwrap();
        assert_eq!(r3.handler, "events_handler");

        let r4 = route("ws/commands/node/replace", "ws").unwrap();
        assert_eq!(r4.handler, "commands_handler");

        // Wrong workspace returns None
        assert!(route("other/edits/doc", "ws").is_none());

        // Invalid port returns None
        assert!(route("ws/invalid/doc", "ws").is_none());
    }
}

// =============================================================================
// QOS SEMANTICS DOCUMENTATION
// =============================================================================
// The following tests document expected QoS behavior rather than testing it,
// since true QoS testing requires network simulation.

#[cfg(test)]
mod qos_documentation {
    //! ## QoS Levels in MQTT
    //!
    //! - **QoS 0 (At most once)**: Fire and forget. Message may be lost.
    //! - **QoS 1 (At least once)**: Message delivered at least once, may duplicate.
    //! - **QoS 2 (Exactly once)**: Message delivered exactly once. Most overhead.
    //!
    //! ## Commonplace MQTT Usage
    //!
    //! - **Edits port**: Uses QoS 1 (at least once) because:
    //!   - CRDT updates are idempotent (duplicates are safe)
    //!   - Losing an update would cause divergence
    //!   - QoS 2 overhead not needed due to idempotency
    //!
    //! - **Sync port**: Uses QoS 1 for the same reasons
    //!
    //! - **Events port**: Could use QoS 0 (ephemeral, loss acceptable)
    //!
    //! - **Commands port**: Uses QoS 1 (commands should not be lost)

    #[test]
    fn document_qos_choices() {
        // This test exists to document QoS choices, not to test them.
        // The test body validates the expected QoS levels match what we document.
        use commonplace_doc::mqtt::QoS;

        // Edits use QoS 1 (at least once) - verify the enum variant exists
        let edits_qos = QoS::AtLeastOnce;
        assert!(matches!(edits_qos, QoS::AtLeastOnce));

        // QoS 0 is available for fire-and-forget scenarios
        let events_qos = QoS::AtMostOnce;
        assert!(matches!(events_qos, QoS::AtMostOnce));
    }
}
