//! Test-only stub types for trait type inference.

use commonplace_types::commit::Commit;
use commonplace_types::traits::{CommitPersistence, MqttPublisher};

/// No-op MQTT publisher for tests that pass `None`.
pub struct NoOpPublisher;

impl MqttPublisher for NoOpPublisher {
    async fn publish_retained(&self, _topic: &str, _payload: &[u8]) -> Result<(), String> {
        Ok(())
    }
}

/// No-op commit store for tests that pass `None`.
pub struct NoOpStore;

impl CommitPersistence for NoOpStore {
    async fn store_commit(&self, _commit: &Commit) -> Result<String, String> {
        Ok("test-cid".to_string())
    }

    async fn set_document_head(&self, _doc_id: &str, _cid: &str) -> Result<(), String> {
        Ok(())
    }

    async fn store_commit_and_set_head(
        &self,
        _doc_id: &str,
        _commit: &Commit,
    ) -> Result<(String, u64), String> {
        Ok(("test-cid".to_string(), 1))
    }
}
