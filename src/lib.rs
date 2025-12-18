pub mod api;
pub mod b64;
pub mod cli;
pub mod commit;
pub mod document;
pub mod node;
pub mod sse;
pub mod store;

use axum::{routing::get, Router};
use node::NodeRegistry;
use std::sync::Arc;
use store::CommitStore;
use tower_http::cors::CorsLayer;

async fn health_check() -> &'static str {
    "OK"
}

pub fn create_router_with_store(store: Option<CommitStore>) -> Router {
    let node_registry = Arc::new(NodeRegistry::new());

    Router::new()
        .route("/health", get(health_check))
        .merge(api::router(store, node_registry.clone()))
        .nest("/sse", sse::router(node_registry))
        .layer(CorsLayer::permissive())
}

pub fn create_router() -> Router {
    create_router_with_store(None)
}
