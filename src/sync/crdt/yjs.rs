//! Yjs update creation utilities for the sync client.
//!
//! This module provides functions to create Yjs CRDT updates for text and JSON content.

use crate::b64;
use crate::content_type::ContentType;
use yrs::any::Any;
use yrs::types::ToJson;
use yrs::updates::decoder::Decode;
use yrs::{Array, Doc, Map, ReadTxn, Text, Transact, Update, WriteTxn};

/// Name of the root text/map element in Yjs documents
pub const TEXT_ROOT_NAME: &str = "content";

/// Create a Yjs update that sets the full text content
pub fn create_yjs_text_update(content: &str) -> String {
    let doc = Doc::with_client_id(1);
    let text = doc.get_or_insert_text(TEXT_ROOT_NAME);
    let update = {
        let mut txn = doc.transact_mut();
        text.push(&mut txn, content);
        txn.encode_update_v1()
    };
    base64_encode(&update)
}

/// Create a Yjs update for structured content based on ContentType.
/// Dispatches to the appropriate update function for JSON vs JSONL content.
/// Returns a base64-encoded Yjs update.
pub fn create_yjs_structured_update(
    content_type: ContentType,
    content: &str,
    base_state: Option<&str>,
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    match content_type {
        ContentType::Json | ContentType::JsonArray => create_yjs_json_update(content, base_state),
        ContentType::Jsonl => create_yjs_jsonl_update(content, base_state),
        ContentType::Text | ContentType::Xml => {
            Err("create_yjs_structured_update only handles JSON/JSONL content types".into())
        }
    }
}

/// Create a Yjs update that applies a JSON replacement.
/// Supports object roots (Y.Map) and array roots (Y.Array).
///
/// When base_state is provided, it applies the state first so that removals
/// create proper CRDT tombstones for the server's existing items.
///
/// When additive_only is true, only inserts/updates keys without removing
/// missing keys. This is used for schema syncs where multiple clients
/// each contribute their own entries.
pub fn create_yjs_json_update(
    new_json: &str,
    base_state: Option<&str>,
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    create_yjs_json_update_impl(new_json, base_state, false)
}

/// Create a Yjs update that merges JSON additively (no deletions).
/// Used for schema syncs where multiple sync clients contribute entries.
pub fn create_yjs_json_merge(
    new_json: &str,
    base_state: Option<&str>,
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    create_yjs_json_update_impl(new_json, base_state, true)
}

/// Create a Yjs update that deletes a specific key from a JSON object.
/// Used to propagate file deletions to the schema without affecting other entries.
///
/// # Errors
/// Returns an error if base_state is None or invalid - base state is required to locate
/// and delete the existing entry. Without it, we can't create a proper deletion tombstone.
pub fn create_yjs_json_delete_key(
    key_path: &str,
    base_state: Option<&str>,
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    // Base state is REQUIRED for deletion - without it, we can't find the existing entry
    // to create a proper deletion tombstone
    let state_b64 = base_state
        .ok_or("Cannot delete schema entry: base state is required but was not provided")?;

    let state_bytes = base64_decode(state_b64).map_err(|e| {
        format!(
            "Cannot delete schema entry: failed to decode base state: {}",
            e
        )
    })?;

    if state_bytes.is_empty() {
        return Err("Cannot delete schema entry: base state is empty".into());
    }

    let doc = Doc::with_client_id(1);

    // Apply base state (critical for proper deletion tombstones)
    let update_result = Update::decode_v1(&state_bytes).map_err(|e| {
        format!(
            "Cannot delete schema entry: failed to decode Yjs update: {}",
            e
        )
    })?;
    {
        let mut txn = doc.transact_mut();
        txn.apply_update(update_result);
    }

    let update = {
        let mut txn = doc.transact_mut();
        let map = txn.get_or_insert_map(TEXT_ROOT_NAME);

        // Navigate to the parent and delete the key
        // Support simple top-level key or "root.entries.{path}" where path can be:
        // - "filename.txt" (top-level file)
        // - "subdir/nested.txt" (nested in inline subdirectory)
        if let Some(rest) = key_path.strip_prefix("root.entries.") {
            // Schema path: root.entries.{path}
            // The path might contain slashes for nested entries in inline subdirectories
            delete_nested_entry(&mut txn, map, rest);
        } else if !key_path.contains('.') {
            // Simple top-level key (no dots)
            map.remove(&mut txn, key_path);
        }

        txn.encode_update_v1()
    };

    Ok(base64_encode(&update))
}

/// Delete a nested entry from a schema's entries hierarchy.
/// Path can be "file.txt" or "subdir/nested.txt" (multiple levels).
fn delete_nested_entry(txn: &mut yrs::TransactionMut, map: yrs::MapRef, path: &str) {
    let parts: Vec<&str> = path.split('/').collect();
    if parts.is_empty() {
        return;
    }

    if let Some(yrs::types::Value::Any(Any::Map(root_map))) = map.get(txn, "root") {
        if let Some(Any::Map(entries_map)) = root_map.get("entries") {
            // Clone entries for modification
            let new_entries = delete_from_entries_recursive(&parts, entries_map);

            // Rebuild root with updated entries
            let mut new_root: std::collections::HashMap<String, Any> = root_map
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect();
            new_root.insert("entries".to_string(), Any::Map(new_entries.into()));

            map.insert(txn, "root", Any::Map(new_root.into()));
        }
    }
}

/// Recursively navigate entries and delete the target key.
/// Returns the updated entries map.
fn delete_from_entries_recursive(
    path_parts: &[&str],
    entries: &std::sync::Arc<std::collections::HashMap<String, Any>>,
) -> std::collections::HashMap<String, Any> {
    let mut new_entries: std::collections::HashMap<String, Any> = entries
        .iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();

    if path_parts.len() == 1 {
        // Last part - delete this key
        new_entries.remove(path_parts[0]);
    } else if path_parts.len() > 1 {
        // Navigate into subdirectory
        let subdir_name = path_parts[0];
        if let Some(Any::Map(subdir_map)) = entries.get(subdir_name) {
            if let Some(Any::Map(sub_entries)) = subdir_map.get("entries") {
                // Recursively delete from subdirectory's entries
                let updated_sub_entries =
                    delete_from_entries_recursive(&path_parts[1..], sub_entries);

                // Rebuild the subdirectory entry
                let mut new_subdir: std::collections::HashMap<String, Any> = subdir_map
                    .iter()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect();
                new_subdir.insert("entries".to_string(), Any::Map(updated_sub_entries.into()));

                new_entries.insert(subdir_name.to_string(), Any::Map(new_subdir.into()));
            }
        }
    }

    new_entries
}

fn create_yjs_json_update_impl(
    new_json: &str,
    base_state: Option<&str>,
    additive_only: bool,
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    let mut new_value: serde_json::Value = serde_json::from_str(new_json)?;

    let doc = Doc::with_client_id(1);

    // Apply base state if provided (critical for proper deletion tombstones)
    if let Some(state_b64) = base_state {
        if let Ok(state_bytes) = base64_decode(state_b64) {
            if !state_bytes.is_empty() {
                if let Ok(update) = Update::decode_v1(&state_bytes) {
                    let mut txn = doc.transact_mut();
                    txn.apply_update(update);
                }
            }
        }
    }

    // For additive merge, deep merge the new JSON with existing content
    // This preserves entries from other sync clients when multiple clients
    // push updates to the same schema document
    if additive_only {
        let txn = doc.transact();
        if let Some(map) = txn.get_map(TEXT_ROOT_NAME) {
            let existing_json = map.to_json(&txn);
            if let serde_json::Value::Object(existing_obj) = any_to_json_value(existing_json) {
                if let serde_json::Value::Object(ref mut new_obj) = new_value {
                    deep_merge_objects(new_obj, &existing_obj);
                }
            }
        }
    }

    let update = {
        let mut txn = doc.transact_mut();

        match new_value {
            serde_json::Value::Object(obj) => {
                let map = txn.get_or_insert_map(TEXT_ROOT_NAME);

                // For full replacement, remove keys not in new JSON
                if !additive_only {
                    let existing_keys: std::collections::HashSet<String> =
                        map.keys(&txn).map(|k| k.to_string()).collect();
                    let new_keys: std::collections::HashSet<String> = obj.keys().cloned().collect();

                    for key in existing_keys.difference(&new_keys) {
                        map.remove(&mut txn, key.as_str());
                    }
                }

                for (key, val) in obj {
                    let any_val = json_value_to_any(val);
                    map.insert(&mut txn, key.as_str(), any_val);
                }
            }
            serde_json::Value::Array(items) => {
                let array = txn.get_or_insert_array(TEXT_ROOT_NAME);

                // For full replacement, clear existing items first
                if !additive_only {
                    let len = array.len(&txn);
                    if len > 0 {
                        array.remove_range(&mut txn, 0, len);
                    }
                }

                for item in items {
                    let any_val = json_value_to_any(item);
                    array.push_back(&mut txn, any_val);
                }
            }
            _ => {
                return Err("JSON root must be an object or array".into());
            }
        }

        txn.encode_update_v1()
    };

    Ok(base64_encode(&update))
}

/// Deep merge objects, preserving entries from the existing object when not in new.
/// This specifically handles schema merges where `root.entries` should combine
/// entries from multiple sync clients.
///
/// node_id fields receive special treatment: existing non-null node_ids are never
/// overwritten. This prevents UUID divergence when multiple sync clients create
/// different UUIDs for the same directory due to corrupted or stale local state.
/// See CP-7hmh.
fn deep_merge_objects(
    new_obj: &mut serde_json::Map<String, serde_json::Value>,
    existing_obj: &serde_json::Map<String, serde_json::Value>,
) {
    for (key, existing_val) in existing_obj {
        match new_obj.get_mut(key) {
            Some(new_val) => {
                // Preserve existing non-null node_ids (CP-7hmh).
                // Once a UUID is assigned to a directory, it must not be overwritten
                // by a different UUID from another sync client.
                if key == "node_id" && !existing_val.is_null() {
                    *new_val = existing_val.clone();
                } else if let (
                    serde_json::Value::Object(new_inner),
                    serde_json::Value::Object(existing_inner),
                ) = (new_val, existing_val)
                {
                    // Both have this key as objects - recursively merge
                    deep_merge_objects(new_inner, existing_inner);
                }
                // Otherwise keep the new value (it takes precedence)
            }
            None => {
                // Key only in existing - preserve it
                new_obj.insert(key.clone(), existing_val.clone());
            }
        }
    }
}

/// Convert serde_json::Value to yrs::Any
pub fn json_value_to_any(value: serde_json::Value) -> Any {
    match value {
        serde_json::Value::Null => Any::Null,
        serde_json::Value::Bool(b) => Any::Bool(b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Any::BigInt(i)
            } else if let Some(f) = n.as_f64() {
                Any::Number(f)
            } else {
                Any::Null
            }
        }
        serde_json::Value::String(s) => Any::String(s.into()),
        serde_json::Value::Array(arr) => {
            let items: Vec<Any> = arr.into_iter().map(json_value_to_any).collect();
            Any::Array(items.into())
        }
        serde_json::Value::Object(obj) => {
            let mut map = std::collections::HashMap::new();
            for (k, v) in obj {
                map.insert(k, json_value_to_any(v));
            }
            Any::Map(map.into())
        }
    }
}

/// Create a Yjs update that transforms text from old content to new content,
/// using a base Yjs state to ensure proper CRDT merge with concurrent edits.
///
/// This function:
/// 1. Applies the base state to a fresh Yjs document
/// 2. Computes character-level diff from old_content to new_content
/// 3. Applies diff operations to the Yjs document
/// 4. Returns the update that can be merged with any concurrent changes
pub fn create_yjs_text_diff_update(
    base_state_b64: &str,
    old_content: &str,
    new_content: &str,
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    use similar::{ChangeTag, TextDiff};

    // Decode base state
    let base_state_bytes = base64_decode(base_state_b64)?;

    // Create doc and apply base state
    let doc = Doc::with_client_id(2); // Different client ID for merge
    let text = doc.get_or_insert_text(TEXT_ROOT_NAME);

    if !base_state_bytes.is_empty() {
        let update = Update::decode_v1(&base_state_bytes)
            .map_err(|e| format!("Failed to decode base state: {}", e))?;
        let mut txn = doc.transact_mut();
        txn.apply_update(update);
    }

    // Compute character-level diff
    let diff = TextDiff::from_chars(old_content, new_content);

    // Apply diff operations
    let update_bytes = {
        let mut txn = doc.transact_mut();
        let mut pos = 0u32;

        for change in diff.iter_all_changes() {
            match change.tag() {
                ChangeTag::Equal => {
                    // Move position forward by the number of chars
                    pos += change.value().chars().count() as u32;
                }
                ChangeTag::Delete => {
                    // Delete characters at current position
                    let len = change.value().chars().count() as u32;
                    text.remove_range(&mut txn, pos, len);
                    // Position stays the same after delete
                }
                ChangeTag::Insert => {
                    // Insert characters at current position
                    text.insert(&mut txn, pos, change.value());
                    pos += change.value().chars().count() as u32;
                }
            }
        }

        txn.encode_update_v1()
    };

    Ok(base64_encode(&update_bytes))
}

/// Create a Yjs update for JSONL content (newline-delimited JSON).
/// Each non-empty line is parsed as a JSON object and stored in a Y.Array.
pub fn create_yjs_jsonl_update(
    content: &str,
    base_state: Option<&str>,
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    let doc = Doc::with_client_id(1);

    // Apply base state if provided
    if let Some(state_b64) = base_state {
        if let Ok(state_bytes) = base64_decode(state_b64) {
            if !state_bytes.is_empty() {
                if let Ok(update) = Update::decode_v1(&state_bytes) {
                    let mut txn = doc.transact_mut();
                    txn.apply_update(update);
                }
            }
        }
    }

    let update = {
        let mut txn = doc.transact_mut();
        let array = txn.get_or_insert_array(TEXT_ROOT_NAME);

        // Clear existing content
        let len = array.len(&txn);
        if len > 0 {
            array.remove_range(&mut txn, 0, len);
        }

        // Parse each non-empty line as JSON
        for line in content.lines() {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            let value: serde_json::Value = serde_json::from_str(line)?;
            let any_val = json_value_to_any(value);
            array.push_back(&mut txn, any_val);
        }

        txn.encode_update_v1()
    };

    Ok(base64_encode(&update))
}

/// Convert Y.Array content to JSONL format (one JSON object per line).
pub fn yjs_array_to_jsonl(
    state_b64: &str,
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    let state_bytes = base64_decode(state_b64)?;

    let doc = Doc::new();
    if !state_bytes.is_empty() {
        let update = Update::decode_v1(&state_bytes)?;
        let mut txn = doc.transact_mut();
        txn.apply_update(update);
    }

    let txn = doc.transact();
    let array = txn
        .get_array(TEXT_ROOT_NAME)
        .ok_or("No content array found")?;

    // Get JSON representation and convert to lines
    let any_array = array.to_json(&txn);
    let json_value = serde_json::to_value(&any_array)?;

    let mut lines = Vec::new();
    if let serde_json::Value::Array(items) = json_value {
        for item in items {
            lines.push(serde_json::to_string(&item)?);
        }
    }

    let mut content = lines.join("\n");
    if !content.is_empty() {
        content.push('\n');
    }
    Ok(content)
}

/// Convert a Y.Doc's root map/array into a JSON value.
pub fn doc_to_json_value(doc: &Doc) -> Option<serde_json::Value> {
    let txn = doc.transact();

    if let Some(map) = txn.get_map(TEXT_ROOT_NAME) {
        let any = map.to_json(&txn);
        return Some(any_to_json_value(any));
    }

    if let Some(array) = txn.get_array(TEXT_ROOT_NAME) {
        let any = array.to_json(&txn);
        return Some(any_to_json_value(any));
    }

    None
}

/// Convert yrs::Any to serde_json::Value
pub fn any_to_json_value(any: Any) -> serde_json::Value {
    match any {
        Any::Null | Any::Undefined => serde_json::Value::Null,
        Any::Bool(b) => serde_json::Value::Bool(b),
        Any::Number(n) => serde_json::json!(n),
        Any::BigInt(i) => serde_json::json!(i),
        Any::String(s) => serde_json::Value::String(s.to_string()),
        Any::Buffer(b) => {
            // Encode binary as base64 string
            serde_json::Value::String(base64_encode(&b))
        }
        Any::Array(arr) => {
            let items: Vec<serde_json::Value> =
                arr.iter().cloned().map(any_to_json_value).collect();
            serde_json::Value::Array(items)
        }
        Any::Map(map) => {
            let obj: serde_json::Map<String, serde_json::Value> = map
                .iter()
                .map(|(k, v)| (k.clone(), any_to_json_value(v.clone())))
                .collect();
            serde_json::Value::Object(obj)
        }
    }
}

/// Base64 encoding using crate::b64
pub fn base64_encode(data: &[u8]) -> String {
    b64::encode(data)
}

/// Base64 decoding using crate::b64
pub fn base64_decode(data: &str) -> Result<Vec<u8>, b64::DecodeError> {
    b64::decode(data)
}

#[cfg(test)]
mod tests {
    use super::*;

    mod yjs_updates {
        use super::*;

        #[test]
        fn test_create_yjs_text_update_empty() {
            let update = create_yjs_text_update("");
            // Should produce a valid base64 string
            assert!(!update.is_empty());
            // Should be decodable
            assert!(base64_decode(&update).is_ok());
        }

        #[test]
        fn test_create_yjs_text_update_simple() {
            let update = create_yjs_text_update("hello world");
            assert!(!update.is_empty());
            assert!(base64_decode(&update).is_ok());
        }

        #[test]
        fn test_create_yjs_text_update_unicode() {
            let update = create_yjs_text_update("Hello 世界 🌍");
            assert!(!update.is_empty());
            assert!(base64_decode(&update).is_ok());
        }

        #[test]
        fn test_create_yjs_json_update_object() {
            let result = create_yjs_json_update(r#"{"key": "value"}"#, None);
            assert!(result.is_ok());
            let update = result.unwrap();
            assert!(!update.is_empty());
            assert!(base64_decode(&update).is_ok());
        }

        #[test]
        fn test_create_yjs_json_update_array() {
            let result = create_yjs_json_update(r#"[1, 2, 3]"#, None);
            assert!(result.is_ok());
            let update = result.unwrap();
            assert!(!update.is_empty());
        }

        #[test]
        fn test_create_yjs_json_update_nested() {
            let result = create_yjs_json_update(r#"{"nested": {"deep": [1, 2, 3]}}"#, None);
            assert!(result.is_ok());
        }

        #[test]
        fn test_create_yjs_json_update_primitive_fails() {
            let result = create_yjs_json_update(r#""just a string""#, None);
            assert!(result.is_err());
        }

        #[test]
        fn test_create_yjs_json_update_invalid_json() {
            let result = create_yjs_json_update("not valid json", None);
            assert!(result.is_err());
        }

        #[test]
        fn test_json_value_to_any_null() {
            let any = json_value_to_any(serde_json::Value::Null);
            assert!(matches!(any, Any::Null));
        }

        #[test]
        fn test_json_value_to_any_bool() {
            let any = json_value_to_any(serde_json::Value::Bool(true));
            assert!(matches!(any, Any::Bool(true)));
        }

        #[test]
        fn test_json_value_to_any_integer() {
            let any = json_value_to_any(serde_json::json!(42));
            assert!(matches!(any, Any::BigInt(42)));
        }

        #[test]
        fn test_json_value_to_any_float() {
            let any = json_value_to_any(serde_json::json!(2.5));
            match any {
                Any::Number(n) => assert!((n - 2.5).abs() < 0.001),
                _ => panic!("Expected Number"),
            }
        }

        #[test]
        fn test_json_value_to_any_string() {
            let any = json_value_to_any(serde_json::json!("hello"));
            match any {
                Any::String(s) => assert_eq!(s.as_ref(), "hello"),
                _ => panic!("Expected String"),
            }
        }

        #[test]
        fn test_json_value_to_any_array() {
            let any = json_value_to_any(serde_json::json!([1, 2, 3]));
            assert!(matches!(any, Any::Array(_)));
        }

        #[test]
        fn test_json_value_to_any_object() {
            let any = json_value_to_any(serde_json::json!({"key": "value"}));
            assert!(matches!(any, Any::Map(_)));
        }
    }

    mod utilities {
        use super::*;

        #[test]
        fn test_base64_roundtrip_empty() {
            let data = b"";
            let encoded = base64_encode(data);
            let decoded = base64_decode(&encoded).unwrap();
            assert_eq!(decoded, data);
        }

        #[test]
        fn test_base64_roundtrip_simple() {
            let data = b"hello world";
            let encoded = base64_encode(data);
            let decoded = base64_decode(&encoded).unwrap();
            assert_eq!(decoded, data);
        }

        #[test]
        fn test_base64_roundtrip_binary() {
            let data: Vec<u8> = (0..=255).collect();
            let encoded = base64_encode(&data);
            let decoded = base64_decode(&encoded).unwrap();
            assert_eq!(decoded, data);
        }

        #[test]
        fn test_base64_decode_invalid() {
            let result = base64_decode("not valid base64!!!");
            assert!(result.is_err());
        }
    }

    mod jsonl {
        use super::*;

        #[test]
        fn test_create_yjs_jsonl_update_empty() {
            let result = create_yjs_jsonl_update("", None);
            assert!(result.is_ok());
        }

        #[test]
        fn test_create_yjs_jsonl_update_single_line() {
            let result = create_yjs_jsonl_update(r#"{"key": "value"}"#, None);
            assert!(result.is_ok());
        }

        #[test]
        fn test_create_yjs_jsonl_update_multiple_lines() {
            let content = r#"{"a": 1}
{"b": 2}
{"c": 3}"#;
            let result = create_yjs_jsonl_update(content, None);
            assert!(result.is_ok());
        }

        #[test]
        fn test_create_yjs_jsonl_update_with_blank_lines() {
            let content = r#"{"a": 1}

{"b": 2}
"#;
            let result = create_yjs_jsonl_update(content, None);
            assert!(result.is_ok());
        }

        #[test]
        fn test_create_yjs_jsonl_update_invalid_json() {
            let result = create_yjs_jsonl_update("not json", None);
            assert!(result.is_err());
        }

        #[test]
        fn test_jsonl_roundtrip() {
            let content = r#"{"a":1}
{"b":2}
{"c":3}"#;
            let update = create_yjs_jsonl_update(content, None).unwrap();
            let result = yjs_array_to_jsonl(&update).unwrap();
            // Normalize and compare
            let original_lines: Vec<&str> = content.lines().collect();
            let result_lines: Vec<&str> = result.lines().collect();
            assert_eq!(original_lines.len(), result_lines.len());
            for (orig, res) in original_lines.iter().zip(result_lines.iter()) {
                let orig_val: serde_json::Value = serde_json::from_str(orig).unwrap();
                let res_val: serde_json::Value = serde_json::from_str(res).unwrap();
                assert_eq!(orig_val, res_val);
            }
        }

        #[test]
        fn test_any_to_json_null() {
            assert_eq!(any_to_json_value(Any::Null), serde_json::Value::Null);
        }

        #[test]
        fn test_any_to_json_bool() {
            assert_eq!(any_to_json_value(Any::Bool(true)), serde_json::json!(true));
        }

        #[test]
        fn test_any_to_json_number() {
            assert_eq!(
                any_to_json_value(Any::Number(1.234)),
                serde_json::json!(1.234)
            );
        }

        #[test]
        fn test_any_to_json_bigint() {
            assert_eq!(any_to_json_value(Any::BigInt(42)), serde_json::json!(42));
        }

        #[test]
        fn test_any_to_json_string() {
            assert_eq!(
                any_to_json_value(Any::String("hello".into())),
                serde_json::json!("hello")
            );
        }
    }

    mod ymap_roundtrip {
        use super::*;
        use yrs::types::ToJson;

        /// Test that simulates the exact server-sync flow for JSON documents:
        /// 1. Server creates a Y.Map document (like fs-root with ContentType::Json)
        /// 2. Server sends its initial state to sync client
        /// 3. Sync creates update with nested JSON (using create_yjs_json_update)
        /// 4. Server applies the update
        /// 5. Server reads back the content - should be valid nested JSON
        #[test]
        fn test_ymap_server_sync_roundtrip() {
            // Step 1: Server creates document with Y.Map at "content" (like ContentType::Json)
            let server_doc = Doc::with_client_id(0);
            {
                let mut txn = server_doc.transact_mut();
                txn.get_or_insert_map(TEXT_ROOT_NAME);
            }

            // Step 2: Server's initial state (sent to sync client)
            let server_state = {
                let txn = server_doc.transact();
                txn.encode_state_as_update_v1(&yrs::StateVector::default())
            };
            let server_state_b64 = base64_encode(&server_state);

            // Step 3: Sync creates update with nested JSON
            let nested_json = r#"{"version":1,"root":{"type":"dir","entries":{"test.txt":{"type":"doc","node_id":"abc-123"}}}}"#;
            let sync_update_b64 = create_yjs_json_update(nested_json, Some(&server_state_b64))
                .expect("Should create update");
            let sync_update = base64_decode(&sync_update_b64).expect("Should decode");

            // Step 4: Server applies the update
            {
                let update = Update::decode_v1(&sync_update).expect("Should decode update");
                let mut txn = server_doc.transact_mut();
                txn.apply_update(update);
            }

            // Step 5: Server reads back content (like document.rs does for ContentType::Json)
            let content = {
                let txn = server_doc.transact();
                let root = txn
                    .root_refs()
                    .find(|(name, _)| *name == TEXT_ROOT_NAME)
                    .map(|(_, value)| value);

                match root {
                    Some(yrs::types::Value::YMap(map)) => {
                        let any = map.to_json(&txn);
                        serde_json::to_string(&any).expect("Should serialize")
                    }
                    other => panic!("Expected YMap, got {:?}", other),
                }
            };

            // Verify the content is valid JSON with nested structure
            let parsed: serde_json::Value =
                serde_json::from_str(&content).expect("Content should be valid JSON");

            // Check structure
            assert_eq!(parsed["version"], 1);
            assert!(
                parsed["root"].is_object(),
                "root should be an object, not a string"
            );
            assert_eq!(parsed["root"]["type"], "dir");
            assert!(parsed["root"]["entries"].is_object());
            assert_eq!(parsed["root"]["entries"]["test.txt"]["node_id"], "abc-123");
        }

        /// Test that nested Any::Map values are preserved through the update cycle
        #[test]
        fn test_nested_any_map_preserved() {
            let doc1 = Doc::with_client_id(1);
            let doc2 = Doc::with_client_id(2);

            // Insert nested structure into doc1
            let update1 = {
                let mut txn = doc1.transact_mut();
                let map = txn.get_or_insert_map(TEXT_ROOT_NAME);

                // Create nested object
                let nested: serde_json::Value = serde_json::json!({
                    "level1": {
                        "level2": {
                            "value": "deep"
                        }
                    }
                });

                if let serde_json::Value::Object(obj) = nested {
                    for (key, val) in obj {
                        let any_val = json_value_to_any(val);
                        map.insert(&mut txn, key.as_str(), any_val);
                    }
                }

                txn.encode_update_v1()
            };

            // Apply to doc2
            {
                let update = Update::decode_v1(&update1).expect("decode");
                let mut txn = doc2.transact_mut();
                txn.apply_update(update);
            }

            // Read from doc2
            let content = {
                let txn = doc2.transact();
                let map = txn.get_map(TEXT_ROOT_NAME).expect("map exists");
                let any = map.to_json(&txn);
                serde_json::to_string(&any).expect("serialize")
            };

            let parsed: serde_json::Value = serde_json::from_str(&content).expect("parse");
            assert!(parsed["level1"].is_object(), "level1 should be object");
            assert!(
                parsed["level1"]["level2"].is_object(),
                "level2 should be object"
            );
            assert_eq!(parsed["level1"]["level2"]["value"], "deep");
        }

        /// Test that node_id values survive the Yjs roundtrip
        #[test]
        fn test_node_id_preserved_in_yjs() {
            // Schema with explicit node_id (this is what sync client pushes)
            let schema_json = r#"{"version":1,"root":{"type":"dir","entries":{"hello.txt":{"type":"doc","node_id":"MY-EXPLICIT-NODE-ID-123","content_type":"text/plain"}}}}"#;

            // Simulate server document with empty Y.Map
            let server_doc = Doc::with_client_id(0);
            {
                let mut txn = server_doc.transact_mut();
                txn.get_or_insert_map(TEXT_ROOT_NAME);
            }

            let server_state = {
                let txn = server_doc.transact();
                txn.encode_state_as_update_v1(&yrs::StateVector::default())
            };
            let server_state_b64 = base64_encode(&server_state);

            // Create update using the sync client's function
            let update_b64 = create_yjs_json_update(schema_json, Some(&server_state_b64))
                .expect("Should create update");
            let update = base64_decode(&update_b64).expect("decode");

            // Apply update to server doc (simulating what server does)
            {
                let update = Update::decode_v1(&update).expect("decode update");
                let mut txn = server_doc.transact_mut();
                txn.apply_update(update);
            }

            // Read result from Y.Map (what server returns)
            let txn = server_doc.transact();
            let content_map = txn.get_map(TEXT_ROOT_NAME).expect("map exists");
            let content_any = content_map.to_json(&txn);

            // Convert to JSON and verify node_id is preserved
            let json_str = serde_json::to_string(&content_any).expect("serialize");
            let parsed: serde_json::Value = serde_json::from_str(&json_str).expect("parse");

            // Navigate to hello.txt and check node_id
            let node_id = &parsed["root"]["entries"]["hello.txt"]["node_id"];
            assert_eq!(
                node_id, "MY-EXPLICIT-NODE-ID-123",
                "node_id should be preserved after Yjs roundtrip, got: {}",
                node_id
            );
        }
    }

    mod deep_merge {
        use super::*;

        #[test]
        fn test_deep_merge_preserves_existing_node_id() {
            // Server has bartleby with node_id "OLD-UUID"
            // Local has bartleby with node_id "NEW-UUID"
            // After merge, server's "OLD-UUID" should win
            let mut new_obj: serde_json::Map<String, serde_json::Value> =
                serde_json::from_str(r#"{"node_id": "NEW-UUID", "type": "dir"}"#).unwrap();
            let existing_obj: serde_json::Map<String, serde_json::Value> =
                serde_json::from_str(r#"{"node_id": "OLD-UUID", "type": "dir"}"#).unwrap();

            deep_merge_objects(&mut new_obj, &existing_obj);

            assert_eq!(
                new_obj["node_id"], "OLD-UUID",
                "Existing node_id must be preserved"
            );
        }

        #[test]
        fn test_deep_merge_allows_new_node_id_when_server_null() {
            // Server has bartleby with node_id null (not yet assigned)
            // Local has bartleby with node_id "NEW-UUID"
            // Local value should win (first assignment)
            let mut new_obj: serde_json::Map<String, serde_json::Value> =
                serde_json::from_str(r#"{"node_id": "NEW-UUID", "type": "dir"}"#).unwrap();
            let existing_obj: serde_json::Map<String, serde_json::Value> =
                serde_json::from_str(r#"{"node_id": null, "type": "dir"}"#).unwrap();

            deep_merge_objects(&mut new_obj, &existing_obj);

            assert_eq!(
                new_obj["node_id"], "NEW-UUID",
                "New node_id should be kept when server has null"
            );
        }

        #[test]
        fn test_deep_merge_fills_null_from_server() {
            // Local has bartleby with node_id null
            // Server has bartleby with node_id "SERVER-UUID"
            // Server value should fill in the null
            let mut new_obj: serde_json::Map<String, serde_json::Value> =
                serde_json::from_str(r#"{"node_id": null, "type": "dir"}"#).unwrap();
            let existing_obj: serde_json::Map<String, serde_json::Value> =
                serde_json::from_str(r#"{"node_id": "SERVER-UUID", "type": "dir"}"#).unwrap();

            deep_merge_objects(&mut new_obj, &existing_obj);

            assert_eq!(
                new_obj["node_id"], "SERVER-UUID",
                "Server node_id should fill null"
            );
        }

        #[test]
        fn test_deep_merge_preserves_node_id_in_nested_schema() {
            // Full schema merge: new has different node_id for bartleby entry
            let mut new_obj: serde_json::Map<String, serde_json::Value> = serde_json::from_str(
                r#"{
                    "version": 1,
                    "root": {
                        "type": "dir",
                        "entries": {
                            "bartleby": {
                                "type": "dir",
                                "node_id": "NEW-UUID"
                            }
                        }
                    }
                }"#,
            )
            .unwrap();

            let existing_obj: serde_json::Map<String, serde_json::Value> = serde_json::from_str(
                r#"{
                    "version": 1,
                    "root": {
                        "type": "dir",
                        "entries": {
                            "bartleby": {
                                "type": "dir",
                                "node_id": "OLD-UUID"
                            },
                            "extra": {
                                "type": "doc",
                                "node_id": "EXTRA-UUID"
                            }
                        }
                    }
                }"#,
            )
            .unwrap();

            deep_merge_objects(&mut new_obj, &existing_obj);

            // bartleby's node_id should be preserved from server
            assert_eq!(
                new_obj["root"]["entries"]["bartleby"]["node_id"], "OLD-UUID",
                "Nested node_id must be preserved from server"
            );
            // Extra entry from server should be preserved (additive)
            assert_eq!(
                new_obj["root"]["entries"]["extra"]["node_id"], "EXTRA-UUID",
                "Server-only entries should be preserved"
            );
        }

        #[test]
        fn test_deep_merge_non_node_id_scalars_new_wins() {
            // For non-node_id fields, new value should take precedence
            let mut new_obj: serde_json::Map<String, serde_json::Value> =
                serde_json::from_str(r#"{"type": "doc", "content_type": "text/plain"}"#).unwrap();
            let existing_obj: serde_json::Map<String, serde_json::Value> =
                serde_json::from_str(r#"{"type": "dir", "content_type": "application/json"}"#)
                    .unwrap();

            deep_merge_objects(&mut new_obj, &existing_obj);

            assert_eq!(
                new_obj["type"], "doc",
                "Non-node_id scalars: new should win"
            );
            assert_eq!(new_obj["content_type"], "text/plain");
        }
    }
}
