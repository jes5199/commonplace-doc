//! Yjs update creation utilities for the sync client.
//!
//! This module provides functions to create Yjs CRDT updates for text and JSON content.

use base64::{engine::general_purpose::STANDARD, Engine};
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

/// Create a Yjs update that applies a full JSON replacement.
/// Supports object roots (Y.Map) and array roots (Y.Array).
///
/// When base_state is provided, it applies the state first so that removals
/// create proper CRDT tombstones for the server's existing items.
pub fn create_yjs_json_update(
    new_json: &str,
    base_state: Option<&str>,
) -> Result<String, Box<dyn std::error::Error>> {
    let new_value: serde_json::Value = serde_json::from_str(new_json)?;

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

    let update = {
        let mut txn = doc.transact_mut();

        match new_value {
            serde_json::Value::Object(obj) => {
                let map = txn.get_or_insert_map(TEXT_ROOT_NAME);
                let existing_keys: std::collections::HashSet<String> =
                    map.keys(&txn).map(|k| k.to_string()).collect();
                let new_keys: std::collections::HashSet<String> = obj.keys().cloned().collect();

                for key in existing_keys.difference(&new_keys) {
                    map.remove(&mut txn, key.as_str());
                }

                for (key, val) in obj {
                    let any_val = json_value_to_any(val);
                    map.insert(&mut txn, key.as_str(), any_val);
                }
            }
            serde_json::Value::Array(items) => {
                let array = txn.get_or_insert_array(TEXT_ROOT_NAME);
                let len = array.len(&txn);
                if len > 0 {
                    array.remove_range(&mut txn, 0, len);
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
) -> Result<String, Box<dyn std::error::Error>> {
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
pub fn yjs_array_to_jsonl(state_b64: &str) -> Result<String, Box<dyn std::error::Error>> {
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

    Ok(lines.join("\n"))
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

/// Simple base64 encoding (matching server's b64 module)
pub fn base64_encode(data: &[u8]) -> String {
    STANDARD.encode(data)
}

/// Simple base64 decoding (matching server's b64 module)
pub fn base64_decode(data: &str) -> Result<Vec<u8>, base64::DecodeError> {
    STANDARD.decode(data)
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
                any_to_json_value(Any::Number(3.14)),
                serde_json::json!(3.14)
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
}
