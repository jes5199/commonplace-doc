use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::sse::{Event, KeepAlive, Sse},
    routing::get,
    Json, Router,
};
use futures::Stream;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::convert::Infallible;
use std::sync::Arc;
use tokio::time::Duration;

use crate::document::DocumentStore;
use crate::events::CommitBroadcaster;
use crate::store::{CommitStore, StoreError};

#[derive(Clone)]
pub struct SseState {
    pub doc_store: Arc<DocumentStore>,
    pub commit_store: Option<Arc<CommitStore>>,
    pub broadcaster: Option<CommitBroadcaster>,
}

pub fn router(
    doc_store: Arc<DocumentStore>,
    commit_store: Option<Arc<CommitStore>>,
    broadcaster: Option<CommitBroadcaster>,
) -> Router {
    let state = SseState {
        doc_store,
        commit_store,
        broadcaster,
    };

    Router::new()
        // Document change history endpoints
        .route("/documents/:id/changes", get(get_document_changes))
        .route("/documents/changes", get(get_documents_changes))
        .route("/documents/:id/stream", get(stream_document_changes))
        .route("/documents/stream", get(stream_documents_changes))
        .with_state(state)
}

// ============================================================================
// Document change history handlers
// ============================================================================

#[derive(Deserialize)]
struct SinceQuery {
    #[serde(default)]
    since: Option<u64>,
}

#[derive(Deserialize)]
struct MultiDocQuery {
    doc_ids: String,
    #[serde(default)]
    since: Option<u64>,
}

#[derive(Serialize, Clone)]
struct CommitChange {
    doc_id: String,
    commit_id: String,
    timestamp: u64,
    url: String,
}

#[derive(Serialize)]
struct CommitChangesResponse {
    changes: Vec<CommitChange>,
}

async fn get_document_changes(
    State(state): State<SseState>,
    Path(doc_id): Path<String>,
    Query(query): Query<SinceQuery>,
) -> Result<Json<CommitChangesResponse>, StatusCode> {
    let commit_store = state
        .commit_store
        .as_ref()
        .ok_or(StatusCode::NOT_IMPLEMENTED)?;

    if state.doc_store.get_document(&doc_id).await.is_none() {
        return Err(StatusCode::NOT_FOUND);
    }

    let since = query.since.unwrap_or(0);
    let changes = collect_changes_for_docs(commit_store, &[doc_id], since)
        .await
        .map_err(map_store_error)?;

    Ok(Json(CommitChangesResponse { changes }))
}

async fn get_documents_changes(
    State(state): State<SseState>,
    Query(query): Query<MultiDocQuery>,
) -> Result<Json<CommitChangesResponse>, StatusCode> {
    let commit_store = state
        .commit_store
        .as_ref()
        .ok_or(StatusCode::NOT_IMPLEMENTED)?;

    let doc_ids = parse_doc_ids(&query.doc_ids);
    if doc_ids.is_empty() {
        return Err(StatusCode::BAD_REQUEST);
    }

    for doc_id in &doc_ids {
        if state.doc_store.get_document(doc_id).await.is_none() {
            return Err(StatusCode::NOT_FOUND);
        }
    }

    let since = query.since.unwrap_or(0);
    let changes = collect_changes_for_docs(commit_store, &doc_ids, since)
        .await
        .map_err(map_store_error)?;

    Ok(Json(CommitChangesResponse { changes }))
}

async fn stream_document_changes(
    State(state): State<SseState>,
    Path(doc_id): Path<String>,
    Query(query): Query<SinceQuery>,
) -> Result<Sse<impl Stream<Item = Result<Event, Infallible>>>, StatusCode> {
    stream_changes(state, vec![doc_id], query.since.unwrap_or(0)).await
}

async fn stream_documents_changes(
    State(state): State<SseState>,
    Query(query): Query<MultiDocQuery>,
) -> Result<Sse<impl Stream<Item = Result<Event, Infallible>>>, StatusCode> {
    let doc_ids = parse_doc_ids(&query.doc_ids);
    if doc_ids.is_empty() {
        return Err(StatusCode::BAD_REQUEST);
    }

    for doc_id in &doc_ids {
        if state.doc_store.get_document(doc_id).await.is_none() {
            return Err(StatusCode::NOT_FOUND);
        }
    }

    stream_changes(state, doc_ids, query.since.unwrap_or(0)).await
}

async fn stream_changes(
    state: SseState,
    doc_ids: Vec<String>,
    since: u64,
) -> Result<Sse<impl Stream<Item = Result<Event, Infallible>>>, StatusCode> {
    let commit_store = state
        .commit_store
        .as_ref()
        .ok_or(StatusCode::NOT_IMPLEMENTED)?;
    let broadcaster = state
        .broadcaster
        .as_ref()
        .ok_or(StatusCode::NOT_IMPLEMENTED)?;

    let initial = collect_changes_for_docs(commit_store, &doc_ids, since)
        .await
        .map_err(map_store_error)?;

    let mut seen: HashSet<String> = initial
        .iter()
        .map(|change| format!("{}:{}", change.doc_id, change.commit_id))
        .collect();
    let doc_filter: HashSet<String> = doc_ids.iter().cloned().collect();
    let mut receiver = broadcaster.subscribe();

    let stream = async_stream::stream! {
        for change in initial {
            if let Ok(data) = serde_json::to_string(&change) {
                yield Ok(Event::default().event("commit").data(data));
            }
        }

        loop {
            match receiver.recv().await {
                Ok(notification) => {
                    if !doc_filter.contains(&notification.doc_id) {
                        continue;
                    }

                    if notification.timestamp < since {
                        continue;
                    }

                    let key = format!("{}:{}", notification.doc_id, notification.commit_id);
                    if !seen.insert(key) {
                        continue;
                    }

                    let change = CommitChange {
                        doc_id: notification.doc_id.clone(),
                        commit_id: notification.commit_id.clone(),
                        timestamp: notification.timestamp,
                        url: commit_url(&notification.doc_id, &notification.commit_id),
                    };

                    if let Ok(data) = serde_json::to_string(&change) {
                        yield Ok(Event::default().event("commit").data(data));
                    }
                }
                Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                    // Skip missed messages; clients can resubscribe with a newer timestamp
                    continue;
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                    break;
                }
            }
        }
    };

    Ok(Sse::new(stream).keep_alive(KeepAlive::new().interval(Duration::from_secs(30))))
}

async fn collect_changes_for_docs(
    commit_store: &CommitStore,
    doc_ids: &[String],
    since: u64,
) -> Result<Vec<CommitChange>, StoreError> {
    let mut changes = Vec::new();

    for doc_id in doc_ids {
        let commits = commit_store.get_commits_since(doc_id, since).await?;
        for (commit_id, commit) in commits {
            let url = commit_url(doc_id, &commit_id);
            changes.push(CommitChange {
                doc_id: doc_id.clone(),
                commit_id,
                timestamp: commit.timestamp,
                url,
            });
        }
    }

    changes.sort_by(|a, b| {
        a.timestamp
            .cmp(&b.timestamp)
            .then_with(|| a.doc_id.cmp(&b.doc_id))
            .then_with(|| a.commit_id.cmp(&b.commit_id))
    });

    // Deduplicate in case multiple paths produced the same change
    changes.dedup_by(|a, b| a.doc_id == b.doc_id && a.commit_id == b.commit_id);

    Ok(changes)
}

fn commit_url(doc_id: &str, commit_id: &str) -> String {
    format!("commonplace://document/{}/commit/{}", doc_id, commit_id)
}

fn parse_doc_ids(ids: &str) -> Vec<String> {
    let mut unique = HashSet::new();
    let mut result = Vec::new();

    for id in ids.split(',').map(|s| s.trim()).filter(|s| !s.is_empty()) {
        if unique.insert(id) {
            result.push(id.to_string());
        }
    }

    result
}

fn map_store_error(_: StoreError) -> StatusCode {
    StatusCode::INTERNAL_SERVER_ERROR
}
