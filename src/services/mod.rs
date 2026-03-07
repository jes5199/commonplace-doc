//! Service layer for business logic.
//!
//! This module provides service types that encapsulate business logic,
//! separating it from HTTP handler concerns.

pub mod document;
pub mod event_log;

pub use document::{DocumentService, ReplaceResult, ServiceError};
