//! Trait abstractions for MQTT publishing and commit persistence.
//!
//! These traits decouple the CRDT module from concrete `MqttClient` and
//! `CommitStore` types, enabling the crdt/ code to be extracted into its
//! own crate.

use crate::commit::Commit;

/// Abstraction for publishing retained MQTT messages.
///
/// All usages in crdt/ call `publish_retained` with `QoS::AtLeastOnce`,
/// so QoS is not part of the trait interface.
pub trait MqttPublisher: Send + Sync {
    fn publish_retained(
        &self,
        topic: &str,
        payload: &[u8],
    ) -> impl std::future::Future<Output = Result<(), String>> + Send;
}

/// Abstraction for persisting commits to a store.
pub trait CommitPersistence: Send + Sync {
    fn store_commit(
        &self,
        commit: &Commit,
    ) -> impl std::future::Future<Output = Result<String, String>> + Send;

    fn set_document_head(
        &self,
        doc_id: &str,
        cid: &str,
    ) -> impl std::future::Future<Output = Result<(), String>> + Send;

    fn store_commit_and_set_head(
        &self,
        doc_id: &str,
        commit: &Commit,
    ) -> impl std::future::Future<Output = Result<(String, u64), String>> + Send;
}

/// Build an MQTT edits topic string.
///
/// Replaces `Topic::edits(workspace, path).to_topic_string()`.
pub fn edits_topic(workspace: &str, path: &str) -> String {
    format!("{}/edits/{}", workspace, path)
}
