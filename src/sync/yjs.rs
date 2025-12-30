//! Yjs update creation utilities for the sync client.
//!
//! This module provides functions to create Yjs CRDT updates for text and JSON content.

use base64::{engine::general_purpose::STANDARD, Engine};
use yrs::any::Any;
use yrs::updates::decoder::Decode;
use yrs::{Array, Doc, Map, Text, Transact, Update, WriteTxn};

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
            let any = json_value_to_any(serde_json::json!(3.14));
            match any {
                Any::Number(n) => assert!((n - 3.14).abs() < 0.001),
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
}
