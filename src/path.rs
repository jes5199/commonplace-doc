//! Path resolution utilities for resolving filesystem paths to document IDs.

use crate::document::DocumentStore;
use std::sync::Arc;

/// Resolve a filesystem path to a document ID, following subdirectory documents.
///
/// This traverses the directory structure stored in commonplace documents,
/// resolving each path segment until reaching the target file.
pub async fn resolve_path_to_doc_id(
    doc_store: &Arc<DocumentStore>,
    fs_root_id: &str,
    path: &str,
) -> Option<String> {
    let segments: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();

    if segments.is_empty() {
        return Some(fs_root_id.to_string());
    }

    let mut current_doc_id = fs_root_id.to_string();

    for (i, segment) in segments.iter().enumerate() {
        let doc = doc_store.get_document(&current_doc_id).await?;
        let schema: serde_json::Value = serde_json::from_str(&doc.content).ok()?;

        let root = schema.get("root")?;
        let entries = root.get("entries")?;

        if entries.is_null() {
            return None;
        }

        let entry = entries.get(*segment)?;
        let entry_type = entry.get("type")?.as_str()?;
        let is_last = i == segments.len() - 1;

        if is_last {
            if entry_type != "doc" {
                return None;
            }
            return entry
                .get("node_id")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string())
                .or_else(|| Some(format!("{}:{}", fs_root_id, path)));
        } else {
            if entry_type != "dir" {
                return None;
            }
            // All directories are node-backed (inline subdirectories were deprecated)
            let node_id = entry.get("node_id")?.as_str()?;
            current_doc_id = node_id.to_string();
        }
    }

    None
}
