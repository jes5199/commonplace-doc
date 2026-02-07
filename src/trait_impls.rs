//! Trait implementations for MqttPublisher and CommitPersistence.
//!
//! These bridge the abstract traits (in commonplace-types) to the concrete
//! types (MqttClient, CommitStore) in the main crate.

use crate::mqtt::MqttClient;
use crate::store::CommitStore;
use commonplace_types::commit::Commit;
use commonplace_types::traits::{CommitPersistence, MqttPublisher};
use rumqttc::QoS;

impl MqttPublisher for MqttClient {
    async fn publish_retained(&self, topic: &str, payload: &[u8]) -> Result<(), String> {
        MqttClient::publish_retained(self, topic, payload, QoS::AtLeastOnce)
            .await
            .map_err(|e| e.to_string())
    }
}

impl CommitPersistence for CommitStore {
    async fn store_commit(&self, commit: &Commit) -> Result<String, String> {
        CommitStore::store_commit(self, commit)
            .await
            .map_err(|e| e.to_string())
    }

    async fn set_document_head(&self, doc_id: &str, cid: &str) -> Result<(), String> {
        CommitStore::set_document_head(self, doc_id, cid)
            .await
            .map_err(|e| e.to_string())
    }

    async fn store_commit_and_set_head(
        &self,
        doc_id: &str,
        commit: &Commit,
    ) -> Result<(String, u64), String> {
        CommitStore::store_commit_and_set_head(self, doc_id, commit)
            .await
            .map_err(|e| e.to_string())
    }
}
