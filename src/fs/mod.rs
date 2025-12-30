//! Filesystem-in-JSON module.
//!
//! Provides a filesystem abstraction backed by a JSON document node.
//! When `--fs-root <node-id>` is specified, the server watches the designated
//! JSON document and creates document nodes for entries declared in it.

mod error;
mod reconciler;
mod schema;

pub use error::FsError;
pub use reconciler::{FilesystemReconciler, MigrationResult};
pub use schema::{DirEntry, DocEntry, Entry, FsSchema};
