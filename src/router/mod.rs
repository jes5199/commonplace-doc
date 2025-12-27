//! Router Documents module.
//!
//! Provides declarative graph wiring via router documents.
//! When `--router <node-id>` is specified, the server watches the designated
//! JSON document and creates/removes wires based on its content.

mod error;
mod manager;
mod schema;

pub use error::RouterError;
pub use manager::RouterManager;
pub use schema::{Edge, NodeSpec, RouterSchema};
