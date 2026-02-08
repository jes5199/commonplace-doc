//! Schema operations for commonplace sync.
//!
//! This crate provides directory scanning, content type detection,
//! schema I/O, and UUID mapping utilities. Transport-dependent
//! functions remain in the main crate's sync::schema shim.

pub mod content_type;
pub mod directory;
pub mod schema_io;
pub mod uuid_map;
