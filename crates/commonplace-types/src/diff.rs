//! Diff-to-Yjs module for computing minimal text diffs and generating Yrs updates.
//!
//! This module provides character-level diffing using the `similar` crate and
//! converts the diff operations into Yrs Text operations to produce a minimal
//! Yjs update.

use crate::b64;
use similar::{ChangeTag, TextDiff};
use yrs::updates::decoder::Decode;
use yrs::{Doc, ReadTxn, Text, TextRef, Transact, XmlFragment, XmlTextPrelim};

/// Text root name used in Yrs documents (must match DocumentNode)
const TEXT_ROOT_NAME: &str = "content";

/// Error type for diff operations
#[derive(Debug)]
pub enum DiffError {
    /// Yrs operation failed
    YrsOperationFailed(String),
    /// Base64 encoding failed
    EncodingFailed(String),
}

impl std::fmt::Display for DiffError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DiffError::YrsOperationFailed(msg) => write!(f, "Yrs operation failed: {}", msg),
            DiffError::EncodingFailed(msg) => write!(f, "Encoding failed: {}", msg),
        }
    }
}

impl std::error::Error for DiffError {}

/// Summary statistics for a diff
#[derive(Debug, Default, Clone)]
pub struct DiffSummary {
    /// Number of characters inserted
    pub chars_inserted: usize,
    /// Number of characters deleted
    pub chars_deleted: usize,
    /// Number of unchanged characters
    pub unchanged_chars: usize,
}

/// Result of computing a diff and generating Yjs update
#[derive(Debug)]
pub struct DiffResult {
    /// The Yjs update bytes
    pub update_bytes: Vec<u8>,
    /// Base64-encoded update
    pub update_b64: String,
    /// Number of operations applied
    pub operation_count: usize,
    /// Summary of changes
    pub summary: DiffSummary,
}

/// Compute character-level diff and generate Yjs update.
///
/// This function:
/// 1. Creates a Yrs Doc with old_content
/// 2. Computes character-level diff between old and new
/// 3. Applies minimal insert/delete operations
/// 4. Returns the update that transforms old to new
///
/// # Arguments
/// * `old_content` - The current document content
/// * `new_content` - The desired new content
///
/// # Returns
/// A `DiffResult` containing the Yjs update and statistics
pub fn compute_diff_update(old_content: &str, new_content: &str) -> Result<DiffResult, DiffError> {
    // Create base doc with old content
    // Use fixed client_id for base_doc - this establishes initial state with
    // predictable character IDs. Only the target_doc needs a unique ID.
    let base_doc = Doc::with_client_id(1);
    let base_text = base_doc.get_or_insert_text(TEXT_ROOT_NAME);
    {
        let mut txn = base_doc.transact_mut();
        base_text.push(&mut txn, old_content);
    }

    // Create target doc and sync to base state
    // CRITICAL: Must use unique client ID (Doc::new()) to avoid conflicts
    // when multiple concurrent replace requests hit the server
    let target_doc = Doc::new();
    let target_text = target_doc.get_or_insert_text(TEXT_ROOT_NAME);

    // Sync target_doc to match base_doc state
    let base_state = {
        let txn = base_doc.transact();
        txn.encode_state_as_update_v1(&yrs::StateVector::default())
    };
    {
        let update = yrs::Update::decode_v1(&base_state)
            .map_err(|e| DiffError::YrsOperationFailed(e.to_string()))?;
        let mut txn = target_doc.transact_mut();
        txn.apply_update(update);
    }

    // Compute character-level diff
    let diff = TextDiff::from_chars(old_content, new_content);

    let mut summary = DiffSummary::default();

    // Collect operations for batch application
    let ops = collect_diff_operations(&diff, &mut summary);
    let operation_count = ops.len();

    // Apply operations and encode update
    let update_bytes = {
        let mut txn = target_doc.transact_mut();
        apply_diff_operations(&target_text, &mut txn, &ops);
        txn.encode_update_v1()
    };

    let update_b64 = b64::encode(&update_bytes);

    Ok(DiffResult {
        update_bytes,
        update_b64,
        operation_count,
        summary,
    })
}

/// Compute character-level diff using actual Yjs state as base.
///
/// Unlike `compute_diff_update`, this function uses the actual Yjs state
/// (from parent commits) as the base, ensuring the resulting update is
/// compatible when replayed with the original commits.
///
/// # Arguments
/// * `base_state_bytes` - The Yjs state update bytes from replaying parent commits
/// * `old_content` - The text content extracted from the base state
/// * `new_content` - The desired new content
///
/// # Returns
/// A `DiffResult` containing the Yjs update and statistics
pub fn compute_diff_update_with_base(
    base_state_bytes: &[u8],
    old_content: &str,
    new_content: &str,
) -> Result<DiffResult, DiffError> {
    // Create target doc and sync to actual base state
    // CRITICAL: Must use unique client ID (Doc::new()) to avoid CRDT corruption
    // when concurrent replace requests generate updates from the same base state
    let target_doc = Doc::new();
    let target_text = target_doc.get_or_insert_text(TEXT_ROOT_NAME);

    // Apply the actual base state from parent commits
    {
        let update = yrs::Update::decode_v1(base_state_bytes)
            .map_err(|e| DiffError::YrsOperationFailed(e.to_string()))?;
        let mut txn = target_doc.transact_mut();
        txn.apply_update(update);
    }

    // Compute character-level diff
    let diff = TextDiff::from_chars(old_content, new_content);

    let mut summary = DiffSummary::default();

    // Collect operations for batch application
    let ops = collect_diff_operations(&diff, &mut summary);
    let operation_count = ops.len();

    // Apply operations and encode update
    let update_bytes = {
        let mut txn = target_doc.transact_mut();
        apply_diff_operations(&target_text, &mut txn, &ops);
        txn.encode_update_v1()
    };

    let update_b64 = b64::encode(&update_bytes);

    Ok(DiffResult {
        update_bytes,
        update_b64,
        operation_count,
        summary,
    })
}

/// Strip XML header and root wrapper to extract inner content.
///
/// Handles formats like:
/// - `<?xml version="1.0" encoding="UTF-8"?><root>content</root>` -> `content`
/// - `<?xml ...?><root/>` -> ``
fn strip_xml_wrapper(xml: &str) -> &str {
    // Strip XML declaration if present
    let without_decl = if let Some(pos) = xml.find("?>") {
        &xml[pos + 2..]
    } else {
        xml
    };

    // Strip root element
    let trimmed = without_decl.trim();

    // Handle self-closing root: <root/>
    if trimmed == "<root/>" {
        return "";
    }

    // Handle <root>...</root>
    if let Some(start) = trimmed.find("<root>") {
        let after_open = &trimmed[start + 6..];
        if let Some(end) = after_open.rfind("</root>") {
            return &after_open[..end];
        }
    }

    // Fallback: return as-is if no root wrapper found
    trimmed
}

/// Compute diff update for XML documents using XmlFragment.
///
/// This function handles XML content by:
/// 1. Stripping the XML header and root wrapper to get inner content
/// 2. Creating XmlFragment with XmlText for the inner content
/// 3. Generating an update that replaces old content with new
///
/// # Arguments
/// * `old_content` - The current XML document (with header and root)
/// * `new_content` - The desired new XML document (with header and root)
///
/// # Returns
/// A `DiffResult` containing the Yjs update for XmlFragment
pub fn compute_xml_diff_update(
    old_content: &str,
    new_content: &str,
) -> Result<DiffResult, DiffError> {
    let old_inner = strip_xml_wrapper(old_content);
    let new_inner = strip_xml_wrapper(new_content);

    // Create base doc with old content
    // Use fixed client_id for base_doc - this establishes initial state with
    // predictable character IDs. Only the target_doc needs a unique ID.
    let base_doc = Doc::with_client_id(1);
    let base_fragment = base_doc.get_or_insert_xml_fragment(TEXT_ROOT_NAME);
    {
        let mut txn = base_doc.transact_mut();
        if !old_inner.is_empty() {
            base_fragment.push_back(&mut txn, XmlTextPrelim::new(old_inner));
        }
    }

    // Create target doc and sync to base state
    // CRITICAL: Must use unique client ID (Doc::new()) to avoid conflicts
    let target_doc = Doc::new();
    let target_fragment = target_doc.get_or_insert_xml_fragment(TEXT_ROOT_NAME);

    // Sync target_doc to match base_doc state
    let base_state = {
        let txn = base_doc.transact();
        txn.encode_state_as_update_v1(&yrs::StateVector::default())
    };
    {
        let update = yrs::Update::decode_v1(&base_state)
            .map_err(|e| DiffError::YrsOperationFailed(e.to_string()))?;
        let mut txn = target_doc.transact_mut();
        txn.apply_update(update);
    }

    // Compute summary from text diff (for statistics)
    let diff = TextDiff::from_chars(old_inner, new_inner);
    let mut summary = DiffSummary::default();
    for change in diff.iter_all_changes() {
        match change.tag() {
            ChangeTag::Equal => summary.unchanged_chars += change.value().len(),
            ChangeTag::Delete => summary.chars_deleted += change.value().len(),
            ChangeTag::Insert => summary.chars_inserted += change.value().len(),
        }
    }

    // Apply XmlFragment operations: clear old content and add new
    let update_bytes = {
        let mut txn = target_doc.transact_mut();

        // Remove all existing children
        let len = target_fragment.len(&txn);
        if len > 0 {
            target_fragment.remove_range(&mut txn, 0, len);
        }

        // Add new content as XmlText
        if !new_inner.is_empty() {
            target_fragment.push_back(&mut txn, XmlTextPrelim::new(new_inner));
        }

        txn.encode_update_v1()
    };

    let update_b64 = b64::encode(&update_bytes);
    let operation_count = if old_inner != new_inner { 1 } else { 0 };

    Ok(DiffResult {
        update_bytes,
        update_b64,
        operation_count,
        summary,
    })
}

/// Compute diff update for XML documents using actual server Yjs state.
///
/// This function is similar to `compute_xml_diff_update` but starts from the
/// server's actual Yjs state rather than creating a fresh document. This ensures
/// proper CRDT merging when the server's state may have received other updates.
///
/// # Arguments
/// * `base_state_bytes` - The Yjs state update bytes from the server
/// * `old_content` - The XML document content (with header and root) from the server
/// * `new_content` - The desired new XML document (with header and root)
///
/// # Returns
/// A `DiffResult` containing the Yjs update for XmlFragment
pub fn compute_xml_diff_update_with_base(
    base_state_bytes: &[u8],
    old_content: &str,
    new_content: &str,
) -> Result<DiffResult, DiffError> {
    let old_inner = strip_xml_wrapper(old_content);
    let new_inner = strip_xml_wrapper(new_content);

    // Create target doc and sync to actual base state
    // CRITICAL: Must use unique client ID (Doc::new()) to avoid CRDT corruption
    // when concurrent replace requests generate updates from the same base state
    let target_doc = Doc::new();
    let target_fragment = target_doc.get_or_insert_xml_fragment(TEXT_ROOT_NAME);

    // Apply the actual base state from server
    {
        let update = yrs::Update::decode_v1(base_state_bytes)
            .map_err(|e| DiffError::YrsOperationFailed(e.to_string()))?;
        let mut txn = target_doc.transact_mut();
        txn.apply_update(update);
    }

    // Compute summary from text diff (for statistics)
    let diff = TextDiff::from_chars(old_inner, new_inner);
    let mut summary = DiffSummary::default();
    for change in diff.iter_all_changes() {
        match change.tag() {
            ChangeTag::Equal => summary.unchanged_chars += change.value().len(),
            ChangeTag::Delete => summary.chars_deleted += change.value().len(),
            ChangeTag::Insert => summary.chars_inserted += change.value().len(),
        }
    }

    // Apply XmlFragment operations: clear old content and add new
    let update_bytes = {
        let mut txn = target_doc.transact_mut();

        // Remove all existing children
        let len = target_fragment.len(&txn);
        if len > 0 {
            target_fragment.remove_range(&mut txn, 0, len);
        }

        // Add new content as XmlText
        if !new_inner.is_empty() {
            target_fragment.push_back(&mut txn, XmlTextPrelim::new(new_inner));
        }

        txn.encode_update_v1()
    };

    let update_b64 = b64::encode(&update_bytes);
    let operation_count = if old_inner != new_inner { 1 } else { 0 };

    Ok(DiffResult {
        update_bytes,
        update_b64,
        operation_count,
        summary,
    })
}

/// A batched diff operation
#[derive(Debug)]
enum DiffOp {
    Delete { pos: usize, len: usize },
    Insert { pos: usize, text: String },
}

/// Collect and batch diff operations for efficiency.
///
/// This function processes character-level changes and batches consecutive
/// operations of the same type together for efficiency.
fn collect_diff_operations<'a>(
    diff: &TextDiff<'a, 'a, 'a, str>,
    summary: &mut DiffSummary,
) -> Vec<DiffOp> {
    let mut ops: Vec<DiffOp> = Vec::new();
    let mut cursor = 0usize; // Position in the RESULT text

    // Accumulate contiguous operations
    let mut pending_delete_start: Option<usize> = None;
    let mut pending_delete_len: usize = 0;
    let mut pending_insert_start: Option<usize> = None;
    let mut pending_insert_text = String::new();

    for change in diff.iter_all_changes() {
        match change.tag() {
            ChangeTag::Equal => {
                // Flush pending operations
                if let Some(pos) = pending_delete_start.take() {
                    ops.push(DiffOp::Delete {
                        pos,
                        len: pending_delete_len,
                    });
                    pending_delete_len = 0;
                }
                if let Some(pos) = pending_insert_start.take() {
                    ops.push(DiffOp::Insert {
                        pos,
                        text: std::mem::take(&mut pending_insert_text),
                    });
                }

                let len = change.value().len();
                cursor += len;
                summary.unchanged_chars += len;
            }
            ChangeTag::Delete => {
                // Flush insert first (delete at same position should come first)
                if let Some(pos) = pending_insert_start.take() {
                    ops.push(DiffOp::Insert {
                        pos,
                        text: std::mem::take(&mut pending_insert_text),
                    });
                }

                // Start or continue delete batch
                if pending_delete_start.is_none() {
                    pending_delete_start = Some(cursor);
                }
                pending_delete_len += change.value().len();
                summary.chars_deleted += change.value().len();
                // Note: cursor doesn't advance for deletes (we're tracking result position)
            }
            ChangeTag::Insert => {
                // Flush delete first
                if let Some(pos) = pending_delete_start.take() {
                    ops.push(DiffOp::Delete {
                        pos,
                        len: pending_delete_len,
                    });
                    pending_delete_len = 0;
                }

                // Start or continue insert batch
                if pending_insert_start.is_none() {
                    pending_insert_start = Some(cursor);
                }
                let ch = change.value();
                pending_insert_text.push_str(ch);
                cursor += ch.len();
                summary.chars_inserted += ch.len();
            }
        }
    }

    // Flush remaining
    if let Some(pos) = pending_delete_start.take() {
        ops.push(DiffOp::Delete {
            pos,
            len: pending_delete_len,
        });
    }
    if let Some(pos) = pending_insert_start.take() {
        ops.push(DiffOp::Insert {
            pos,
            text: pending_insert_text,
        });
    }

    ops
}

/// Apply diff operations to a Yrs Text
///
/// Operations are applied directly without offset adjustment because positions
/// are calculated during collection to reflect the document state as modified.
fn apply_diff_operations(text: &TextRef, txn: &mut yrs::TransactionMut<'_>, ops: &[DiffOp]) {
    for op in ops {
        match op {
            DiffOp::Delete { pos, len } => {
                text.remove_range(txn, *pos as u32, *len as u32);
            }
            DiffOp::Insert { pos, text: content } => {
                text.insert(txn, *pos as u32, content);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use yrs::GetString;

    #[test]
    fn test_simple_append() {
        let result = compute_diff_update("hello", "hello world").unwrap();
        assert!(result.operation_count > 0);
        assert_eq!(result.summary.chars_inserted, 6); // " world"
        assert_eq!(result.summary.chars_deleted, 0);
    }

    #[test]
    fn test_simple_delete() {
        let result = compute_diff_update("hello world", "hello").unwrap();
        assert!(result.operation_count > 0);
        assert_eq!(result.summary.chars_deleted, 6); // " world"
        assert_eq!(result.summary.chars_inserted, 0);
    }

    #[test]
    fn test_replacement() {
        let old = "hello world";
        let new = "hello rust";
        let result = compute_diff_update(old, new).unwrap();
        assert!(result.operation_count > 0);
        // Note: similar may find common chars (e.g., 'r') making the diff more minimal
        // What matters is the update produces correct results
        assert!(result.summary.chars_deleted > 0);
        assert!(result.summary.chars_inserted > 0);

        // Verify the update works correctly
        let doc = Doc::with_client_id(1);
        let text = doc.get_or_insert_text(TEXT_ROOT_NAME);
        {
            let mut txn = doc.transact_mut();
            text.push(&mut txn, old);
        }
        {
            let update = yrs::Update::decode_v1(&result.update_bytes).unwrap();
            let mut txn = doc.transact_mut();
            txn.apply_update(update);
        }
        let txn = doc.transact();
        assert_eq!(text.get_string(&txn), new);
    }

    #[test]
    fn test_no_change() {
        let result = compute_diff_update("hello", "hello").unwrap();
        assert_eq!(result.summary.chars_inserted, 0);
        assert_eq!(result.summary.chars_deleted, 0);
        assert_eq!(result.operation_count, 0);
    }

    #[test]
    fn test_empty_to_content() {
        let result = compute_diff_update("", "hello").unwrap();
        assert_eq!(result.summary.chars_inserted, 5);
        assert_eq!(result.summary.chars_deleted, 0);
    }

    #[test]
    fn test_content_to_empty() {
        let result = compute_diff_update("hello", "").unwrap();
        assert_eq!(result.summary.chars_deleted, 5);
        assert_eq!(result.summary.chars_inserted, 0);
    }

    #[test]
    fn test_update_applies_correctly() {
        let old = "The quick brown fox";
        let new = "The slow brown dog";

        let result = compute_diff_update(old, new).unwrap();

        // Create a fresh doc with old content and apply the update
        let doc = Doc::with_client_id(1);
        let text = doc.get_or_insert_text(TEXT_ROOT_NAME);
        {
            let mut txn = doc.transact_mut();
            text.push(&mut txn, old);
        }

        // Apply the computed update
        {
            let update = yrs::Update::decode_v1(&result.update_bytes).unwrap();
            let mut txn = doc.transact_mut();
            txn.apply_update(update);
        }

        // Verify result
        let txn = doc.transact();
        let final_text = text.get_string(&txn);
        assert_eq!(final_text, new);
    }

    #[test]
    fn test_unicode_content() {
        let old = "Hello 世界";
        let new = "Hello 🌍 World";

        let result = compute_diff_update(old, new).unwrap();

        // Verify the update applies correctly
        let doc = Doc::with_client_id(1);
        let text = doc.get_or_insert_text(TEXT_ROOT_NAME);
        {
            let mut txn = doc.transact_mut();
            text.push(&mut txn, old);
        }

        {
            let update = yrs::Update::decode_v1(&result.update_bytes).unwrap();
            let mut txn = doc.transact_mut();
            txn.apply_update(update);
        }

        let txn = doc.transact();
        let final_text = text.get_string(&txn);
        assert_eq!(final_text, new);
    }

    #[test]
    fn test_insert_in_middle() {
        let old = "hello world";
        let new = "hello beautiful world";

        let result = compute_diff_update(old, new).unwrap();

        let doc = Doc::with_client_id(1);
        let text = doc.get_or_insert_text(TEXT_ROOT_NAME);
        {
            let mut txn = doc.transact_mut();
            text.push(&mut txn, old);
        }

        {
            let update = yrs::Update::decode_v1(&result.update_bytes).unwrap();
            let mut txn = doc.transact_mut();
            txn.apply_update(update);
        }

        let txn = doc.transact();
        let final_text = text.get_string(&txn);
        assert_eq!(final_text, new);
    }

    #[test]
    fn test_multiple_changes() {
        let old = "The quick brown fox jumps over the lazy dog";
        let new = "A slow red cat leaps over a sleepy cat";

        let result = compute_diff_update(old, new).unwrap();

        let doc = Doc::with_client_id(1);
        let text = doc.get_or_insert_text(TEXT_ROOT_NAME);
        {
            let mut txn = doc.transact_mut();
            text.push(&mut txn, old);
        }

        {
            let update = yrs::Update::decode_v1(&result.update_bytes).unwrap();
            let mut txn = doc.transact_mut();
            txn.apply_update(update);
        }

        let txn = doc.transact();
        let final_text = text.get_string(&txn);
        assert_eq!(final_text, new);
    }

    // XML diff tests

    #[test]
    fn test_xml_strip_wrapper() {
        assert_eq!(
            strip_xml_wrapper(r#"<?xml version="1.0" encoding="UTF-8"?><root><hello/></root>"#),
            "<hello/>"
        );
        assert_eq!(
            strip_xml_wrapper(r#"<?xml version="1.0" encoding="UTF-8"?><root/>"#),
            ""
        );
        assert_eq!(
            strip_xml_wrapper(r#"<?xml version="1.0"?><root>content</root>"#),
            "content"
        );
    }

    #[test]
    fn test_xml_diff_simple_change() {
        let old = r#"<?xml version="1.0" encoding="UTF-8"?><root><hello>world</hello></root>"#;
        let new = r#"<?xml version="1.0" encoding="UTF-8"?><root><hello>rust</hello></root>"#;

        let result = compute_xml_diff_update(old, new).unwrap();
        assert!(result.operation_count > 0);

        // Verify the update applies correctly
        let doc = Doc::with_client_id(1);
        let fragment = doc.get_or_insert_xml_fragment(TEXT_ROOT_NAME);
        {
            let mut txn = doc.transact_mut();
            fragment.push_back(&mut txn, XmlTextPrelim::new("<hello>world</hello>"));
        }
        {
            let update = yrs::Update::decode_v1(&result.update_bytes).unwrap();
            let mut txn = doc.transact_mut();
            txn.apply_update(update);
        }
        let txn = doc.transact();
        assert_eq!(fragment.get_string(&txn), "<hello>rust</hello>");
    }

    #[test]
    fn test_xml_diff_empty_to_content() {
        let old = r#"<?xml version="1.0" encoding="UTF-8"?><root/>"#;
        let new = r#"<?xml version="1.0" encoding="UTF-8"?><root><data>test</data></root>"#;

        let result = compute_xml_diff_update(old, new).unwrap();
        assert!(result.operation_count > 0);
        assert!(result.summary.chars_inserted > 0);
    }

    #[test]
    fn test_xml_diff_content_to_empty() {
        let old = r#"<?xml version="1.0" encoding="UTF-8"?><root><data>test</data></root>"#;
        let new = r#"<?xml version="1.0" encoding="UTF-8"?><root/>"#;

        let result = compute_xml_diff_update(old, new).unwrap();
        assert!(result.operation_count > 0);
        assert!(result.summary.chars_deleted > 0);
    }

    #[test]
    fn test_xml_diff_no_change() {
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?><root><same/></root>"#;

        let result = compute_xml_diff_update(xml, xml).unwrap();
        assert_eq!(result.operation_count, 0);
        assert_eq!(result.summary.chars_inserted, 0);
        assert_eq!(result.summary.chars_deleted, 0);
    }

    #[test]
    fn test_xml_diff_server_scenario() {
        // This test simulates EXACTLY what the server does:
        // 1. Server creates doc with empty XmlFragment (no children)
        // 2. diff is computed against old content
        // 3. Update is applied to server's doc

        let old = r#"<?xml version="1.0" encoding="UTF-8"?><root/>"#;
        let new = r#"<?xml version="1.0" encoding="UTF-8"?><root><filetree><file name="test.txt"/></filetree></root>"#;

        // Step 1: Create server doc EXACTLY like DocumentStore does
        // (empty XmlFragment, no XmlText added)
        let server_doc = Doc::with_client_id(1);
        let _server_fragment = server_doc.get_or_insert_xml_fragment(TEXT_ROOT_NAME);
        // NOTE: Server does NOT add any XmlText - just creates empty XmlFragment

        // Step 2: Compute diff update
        let result = compute_xml_diff_update(old, new).unwrap();

        // Step 3: Apply update to server doc
        {
            let update = yrs::Update::decode_v1(&result.update_bytes).unwrap();
            let mut txn = server_doc.transact_mut();
            txn.apply_update(update);
        }

        // Step 4: Read content back
        let txn = server_doc.transact();
        let fragment = txn.get_xml_fragment(TEXT_ROOT_NAME).unwrap();
        let inner = fragment.get_string(&txn);

        assert_eq!(inner, r#"<filetree><file name="test.txt"/></filetree>"#);
    }

    #[test]
    fn test_newlines_preserved() {
        // Test that newlines are preserved in content
        let old = "";
        let new = "/typing\nHello World\n/unset typing\n";

        let result = compute_diff_update(old, new).unwrap();

        // Verify the update applies correctly and preserves newlines
        let doc = Doc::with_client_id(1);
        let text = doc.get_or_insert_text(TEXT_ROOT_NAME);
        {
            let mut txn = doc.transact_mut();
            text.push(&mut txn, old);
        }

        {
            let update = yrs::Update::decode_v1(&result.update_bytes).unwrap();
            let mut txn = doc.transact_mut();
            txn.apply_update(update);
        }

        let txn = doc.transact();
        let final_text = text.get_string(&txn);
        assert_eq!(final_text, new, "Newlines should be preserved in CRDT text");
    }

    #[test]
    fn test_newlines_in_replacement() {
        // Test replacing content that has newlines
        let old = "line1\nline2\nline3";
        let new = "line1\nmodified\nline3\nline4";

        let result = compute_diff_update(old, new).unwrap();

        let doc = Doc::with_client_id(1);
        let text = doc.get_or_insert_text(TEXT_ROOT_NAME);
        {
            let mut txn = doc.transact_mut();
            text.push(&mut txn, old);
        }

        {
            let update = yrs::Update::decode_v1(&result.update_bytes).unwrap();
            let mut txn = doc.transact_mut();
            txn.apply_update(update);
        }

        let txn = doc.transact();
        let final_text = text.get_string(&txn);
        assert_eq!(
            final_text, new,
            "Newlines should be preserved after replacement"
        );
    }
}
