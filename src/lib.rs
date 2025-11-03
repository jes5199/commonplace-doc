pub mod api;
pub mod cli;
pub mod commit;
pub mod document;
pub mod events;
pub mod sse;
pub mod store;

use axum::{routing::get, Router};
use document::DocumentStore;
use events::CommitBroadcaster;
use std::sync::Arc;
use store::CommitStore;
use tower_http::cors::CorsLayer;

async fn health_check() -> &'static str {
    "OK"
}

pub fn create_router_with_store(store: Option<CommitStore>) -> Router {
    let doc_store = Arc::new(DocumentStore::new());
    let commit_store = store.map(|s| Arc::new(s));
    let commit_broadcaster = commit_store.as_ref().map(|_| CommitBroadcaster::new(1024));

    Router::new()
        .route("/health", get(health_check))
        .merge(api::router(
            doc_store.clone(),
            commit_store.clone(),
            commit_broadcaster.clone(),
        ))
        .merge(sse::router(doc_store, commit_store, commit_broadcaster))
        .layer(CorsLayer::permissive())
}

pub fn create_router() -> Router {
    create_router_with_store(None)
}
