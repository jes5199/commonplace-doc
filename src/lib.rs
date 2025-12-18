pub mod api;
pub mod b64;
pub mod cli;
pub mod commit;
pub mod diff;
pub mod document;
pub mod events;
pub mod node;
pub mod replay;
pub mod sse;
pub mod store;

use axum::{routing::get, Router};
use document::DocumentStore;
use events::CommitBroadcaster;
use node::NodeRegistry;
use std::sync::Arc;
use store::CommitStore;
use tower_http::cors::CorsLayer;

async fn health_check() -> &'static str {
    "OK"
}

pub fn create_router_with_store(store: Option<CommitStore>) -> Router {
    let doc_store = Arc::new(DocumentStore::new());
    let commit_store = store.map(Arc::new);
    let commit_broadcaster = commit_store.as_ref().map(|_| CommitBroadcaster::new(1024));
    let node_registry = Arc::new(NodeRegistry::new());

    Router::new()
        .route("/health", get(health_check))
        .merge(api::router(
            doc_store.clone(),
            commit_store.clone(),
            commit_broadcaster.clone(),
            node_registry.clone(),
        ))
        .merge(sse::router(
            doc_store,
            commit_store,
            commit_broadcaster,
            node_registry,
        ))
        .layer(CorsLayer::permissive())
}

pub fn create_router() -> Router {
    create_router_with_store(None)
}
