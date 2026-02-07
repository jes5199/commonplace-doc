//! YMap-based schema storage for CRDT peer sync.
//!
//! This module implements schema storage using native Yjs YMap types instead of
//! JSON text. YMap provides proper CRDT semantics: concurrent file additions
//! merge correctly instead of one overwriting the other.
//!
//! Schema structure (matches FsSchema JSON format for server compatibility):
//! ```text
//! YMap "content" {
//!   "version": 1
//!   "root": YMap {
//!     "type": "dir"
//!     "entries": YMap {
//!       "foo.txt": YMap { "type": "doc", "node_id": "uuid-1" }
//!       "subdir": YMap { "type": "dir", "node_id": "uuid-2" }
//!     }
//!   }
//! }
//! ```
//!
//! The "content" root name matches what the server expects for JSON content type.
//! When the server calls `map.to_json()`, it produces valid FsSchema JSON.
//!
//! See: docs/plans/2026-01-21-crdt-peer-sync-design.md

use std::collections::HashMap;
use std::sync::Arc;
use yrs::{Any, Doc, Map, MapRef, ReadTxn, Transact, TransactionMut, WriteTxn};

/// Entry type in the schema.
#[derive(Debug, Clone, PartialEq)]
pub enum SchemaEntryType {
    Doc,
    Dir,
}

impl SchemaEntryType {
    #[allow(dead_code)]
    fn as_str(&self) -> &'static str {
        match self {
            SchemaEntryType::Doc => "doc",
            SchemaEntryType::Dir => "dir",
        }
    }

    fn from_str(s: &str) -> Option<Self> {
        match s {
            "doc" => Some(SchemaEntryType::Doc),
            "dir" => Some(SchemaEntryType::Dir),
            _ => None,
        }
    }
}

/// A schema entry (file or directory).
#[derive(Debug, Clone)]
pub struct SchemaEntry {
    pub entry_type: SchemaEntryType,
    pub node_id: Option<String>,
}

/// Get the "entries" YMap from a schema Y.Doc, creating full structure if needed.
///
/// Creates the FsSchema-compatible structure:
/// content -> { version: 1, root: { type: "dir", entries: {...} } }
///
/// When the schema was loaded from server (via cyan sync or `create_yjs_json_merge`),
/// the nested maps may be stored as `Any::Map` (static/immutable) rather than native
/// `YMap` (live CRDT). This function migrates `Any::Map` to `YMap` while preserving
/// existing entries, preventing data loss when new entries are added.
fn get_or_create_entries<'a>(txn: &mut TransactionMut<'a>) -> MapRef {
    // Get or create "content" (the root name server expects for JSON docs)
    let content = txn.get_or_insert_map("content");

    // Ensure version is set
    // Use Any::BigInt explicitly to ensure integer serialization (1 not 1.0)
    // When i64 converts to Any, it becomes Any::Number (float) for values within safe range
    if content.get(txn, "version").is_none() {
        content.insert(txn, "version", Any::BigInt(1));
    }

    // Phase 1: Check if "root" needs migration from Any::Map to YMap.
    // We must release the immutable borrow before doing mutable inserts.
    let root_migration_data = match content.get(txn, "root") {
        Some(yrs::Value::YMap(_)) => None, // Already a YMap, no migration needed
        Some(yrs::Value::Any(Any::Map(any_map))) => {
            // Extract data for migration: entries Any::Map
            Some(any_map.get("entries").and_then(|e| {
                if let Any::Map(entries) = e {
                    Some(entries.clone())
                } else {
                    None
                }
            }))
        }
        _ => Some(None), // Needs creation, no data to migrate
    };

    // Phase 2: Create/migrate "root" if needed
    let schema_root = if let Some(existing_entries_data) = root_migration_data {
        let root_map = content.insert(txn, "root", yrs::MapPrelim::<Any>::new());
        root_map.insert(txn, "type", "dir");

        // Migrate entries from Any::Map to YMap
        if let Some(entries_any) = existing_entries_data {
            let entries_map = root_map.insert(txn, "entries", yrs::MapPrelim::<Any>::new());
            migrate_any_map_entries(txn, &entries_map, &entries_any);
        }
        root_map
    } else {
        // Already a YMap, retrieve it
        match content.get(txn, "root") {
            Some(yrs::Value::YMap(map)) => map,
            _ => unreachable!("checked above"),
        }
    };

    // Ensure root has type="dir"
    if schema_root.get(txn, "type").is_none() {
        schema_root.insert(txn, "type", "dir");
    }

    // Phase 3: Check if "entries" needs migration from Any::Map to YMap
    let entries_migration_data = match schema_root.get(txn, "entries") {
        Some(yrs::Value::YMap(_)) => None,
        Some(yrs::Value::Any(Any::Map(any_map))) => Some(Some(any_map.clone())),
        _ => Some(None),
    };

    // Phase 4: Create/migrate "entries" if needed
    if let Some(existing_data) = entries_migration_data {
        let entries_map = schema_root.insert(txn, "entries", yrs::MapPrelim::<Any>::new());
        if let Some(entries_any) = existing_data {
            migrate_any_map_entries(txn, &entries_map, &entries_any);
        }
        entries_map
    } else {
        match schema_root.get(txn, "entries") {
            Some(yrs::Value::YMap(map)) => map,
            _ => unreachable!("checked above"),
        }
    }
}

/// Migrate entries from an Any::Map into a live YMap, preserving all entry data.
///
/// Each entry is expected to be an Any::Map with fields like "type", "node_id", etc.
/// These are converted to nested YMap entries for proper CRDT semantics.
fn migrate_any_map_entries(
    txn: &mut TransactionMut<'_>,
    entries_map: &MapRef,
    source: &Arc<HashMap<String, Any>>,
) {
    for (key, value) in source.as_ref() {
        if let Any::Map(entry_map) = value {
            let entry = entries_map.insert(txn, key.as_str(), yrs::MapPrelim::<Any>::new());
            for (entry_key, entry_value) in entry_map.as_ref() {
                entry.insert(txn, entry_key.as_str(), entry_value.clone());
            }
        }
    }
}

/// Get the "entries" YMap from a schema Y.Doc (read-only).
fn get_entries<T: ReadTxn>(txn: &T) -> Option<MapRef> {
    let content = txn.get_map("content")?;
    let schema_root = match content.get(txn, "root") {
        Some(yrs::Value::YMap(map)) => map,
        _ => return None,
    };
    match schema_root.get(txn, "entries") {
        Some(yrs::Value::YMap(map)) => Some(map),
        _ => None,
    }
}

/// Get the "entries" as an Any::Map from a schema Y.Doc (read-only).
///
/// Fallback for schemas created by `create_yjs_json_merge()`, which stores
/// nested objects as `Any::Map` instead of native YMap types.
fn get_entries_any<T: ReadTxn>(txn: &T) -> Option<Arc<HashMap<String, Any>>> {
    let content = txn.get_map("content")?;
    let root_map = match content.get(txn, "root") {
        Some(yrs::Value::Any(Any::Map(map))) => map,
        _ => return None,
    };
    match root_map.get("entries") {
        Some(Any::Map(entries)) => Some(entries.clone()),
        _ => None,
    }
}

/// Parse a SchemaEntry from an Any::Map.
fn schema_entry_from_any_map(map: &HashMap<String, Any>) -> Option<SchemaEntry> {
    let type_str = match map.get("type") {
        Some(Any::String(s)) => s.to_string(),
        _ => return None,
    };
    let entry_type = SchemaEntryType::from_str(&type_str)?;
    let node_id = match map.get("node_id") {
        Some(Any::String(s)) => Some(s.to_string()),
        _ => None,
    };
    Some(SchemaEntry {
        entry_type,
        node_id,
    })
}

/// Add a file entry to the schema.
pub fn add_file(doc: &Doc, filename: &str, node_id: &str) {
    let mut txn = doc.transact_mut();
    let entries = get_or_create_entries(&mut txn);

    // Create the entry map for this file
    let entry_map = entries.insert(&mut txn, filename, yrs::MapPrelim::<Any>::new());
    entry_map.insert(&mut txn, "type", "doc");
    entry_map.insert(&mut txn, "node_id", node_id);
}

/// Add a directory entry to the schema.
///
/// If the entry already exists and `node_id` is None, the existing node_id
/// is preserved. This prevents scan_directory() from overwriting server-assigned
/// node_ids when the local .commonplace.json doesn't have them yet. See CP-urp5.
pub fn add_directory(doc: &Doc, dirname: &str, node_id: Option<&str>) {
    let mut txn = doc.transact_mut();
    let entries = get_or_create_entries(&mut txn);

    // If node_id is None and entry already exists with a node_id, preserve it
    let existing_node_id = if node_id.is_none() {
        match entries.get(&txn, dirname) {
            Some(yrs::Value::YMap(existing)) => match existing.get(&txn, "node_id") {
                Some(yrs::Value::Any(Any::String(s))) => Some(s.to_string()),
                _ => None,
            },
            Some(yrs::Value::Any(Any::Map(existing))) => match existing.get("node_id") {
                Some(Any::String(s)) => Some(s.to_string()),
                _ => None,
            },
            _ => None,
        }
    } else {
        None
    };

    let effective_node_id = node_id.map(|s| s.to_string()).or(existing_node_id);

    // Create the entry map for this directory
    let entry_map = entries.insert(&mut txn, dirname, yrs::MapPrelim::<Any>::new());
    entry_map.insert(&mut txn, "type", "dir");
    if let Some(ref id) = effective_node_id {
        entry_map.insert(&mut txn, "node_id", id.as_str());
    }
}

/// Remove an entry from the schema.
pub fn remove_entry(doc: &Doc, name: &str) {
    let mut txn = doc.transact_mut();
    if let Some(entries) = get_entries(&txn) {
        entries.remove(&mut txn, name);
    }
}

/// Get an entry from the schema.
pub fn get_entry(doc: &Doc, name: &str) -> Option<SchemaEntry> {
    let txn = doc.transact();

    // Try YMap path first
    if let Some(entries) = get_entries(&txn) {
        match entries.get(&txn, name) {
            Some(yrs::Value::YMap(entry_map)) => {
                let entry_type = match entry_map.get(&txn, "type") {
                    Some(yrs::Value::Any(Any::String(s))) => SchemaEntryType::from_str(s.as_ref())?,
                    _ => return None,
                };

                let node_id = match entry_map.get(&txn, "node_id") {
                    Some(yrs::Value::Any(Any::String(s))) => Some(s.to_string()),
                    _ => None,
                };

                return Some(SchemaEntry {
                    entry_type,
                    node_id,
                });
            }
            Some(yrs::Value::Any(Any::Map(entry_map))) => {
                return schema_entry_from_any_map(&entry_map);
            }
            _ => {}
        }
    }

    // Fallback: try Any::Map path
    if let Some(entries) = get_entries_any(&txn) {
        if let Some(Any::Map(entry_map)) = entries.get(name) {
            return schema_entry_from_any_map(entry_map);
        }
    }

    None
}

/// List all entries in the schema.
pub fn list_entries(doc: &Doc) -> HashMap<String, SchemaEntry> {
    let txn = doc.transact();
    let mut result = HashMap::new();

    // Try YMap path first (native Yjs types)
    if let Some(entries) = get_entries(&txn) {
        for (key, value) in entries.iter(&txn) {
            match value {
                yrs::Value::YMap(entry_map) => {
                    let entry_type = match entry_map.get(&txn, "type") {
                        Some(yrs::Value::Any(Any::String(s))) => {
                            match SchemaEntryType::from_str(s.as_ref()) {
                                Some(t) => t,
                                None => continue,
                            }
                        }
                        _ => continue,
                    };

                    let node_id = match entry_map.get(&txn, "node_id") {
                        Some(yrs::Value::Any(Any::String(s))) => Some(s.to_string()),
                        _ => None,
                    };

                    result.insert(
                        key.to_string(),
                        SchemaEntry {
                            entry_type,
                            node_id,
                        },
                    );
                }
                yrs::Value::Any(Any::Map(entry_map)) => {
                    if let Some(entry) = schema_entry_from_any_map(&entry_map) {
                        result.insert(key.to_string(), entry);
                    }
                }
                _ => {}
            }
        }
    }

    // Fallback: try Any::Map path (for schemas from create_yjs_json_merge)
    if result.is_empty() {
        if let Some(entries) = get_entries_any(&txn) {
            for (key, value) in entries.as_ref() {
                if let Any::Map(entry_map) = value {
                    if let Some(entry) = schema_entry_from_any_map(entry_map) {
                        result.insert(key.to_string(), entry);
                    }
                }
            }
        }
    }

    result
}

/// List all entry names in the schema (lightweight version of list_entries).
/// Used for before/after comparison to detect explicit deletions.
pub fn list_entry_names(doc: &Doc) -> std::collections::HashSet<String> {
    let txn = doc.transact();
    let mut names = std::collections::HashSet::new();

    if let Some(entries) = get_entries(&txn) {
        for (key, _value) in entries.iter(&txn) {
            names.insert(key.to_string());
        }
    }

    // Fallback: try Any::Map path
    if names.is_empty() {
        if let Some(entries) = get_entries_any(&txn) {
            for key in entries.keys() {
                names.insert(key.to_string());
            }
        }
    }

    names
}

/// Check if the doc uses the new YMap format (has YMap at "content" with "root" submap).
/// Also returns true for Any::Map format (from create_yjs_json_merge).
pub fn is_ymap_format(doc: &Doc) -> bool {
    let txn = doc.transact();
    // Check for YMap or Any::Map at "content" with "root" submap (new format)
    if let Some(content) = txn.get_map("content") {
        matches!(
            content.get(&txn, "root"),
            Some(yrs::Value::YMap(_)) | Some(yrs::Value::Any(Any::Map(_)))
        )
    } else {
        false
    }
}

/// Check if the doc uses the old JSON text format (has YText at "content").
pub fn is_json_text_format(doc: &Doc) -> bool {
    let txn = doc.transact();
    txn.get_text("content").is_some()
}

/// Migrate from JSON text format to YMap format.
///
/// Returns true if migration was performed, false if already in YMap format.
pub fn migrate_from_json_text(doc: &Doc) -> Result<bool, String> {
    use commonplace_types::fs::{Entry, FsSchema};
    use yrs::{GetString, Text};

    // Check if already in YMap format
    if is_ymap_format(doc) {
        return Ok(false);
    }

    // Check if in JSON text format
    let txn = doc.transact();
    let content = match txn.get_text("content") {
        Some(text) => text.get_string(&txn),
        None => return Ok(false), // Empty doc, nothing to migrate
    };
    drop(txn);

    if content.is_empty() {
        return Ok(false);
    }

    // Parse the JSON schema
    let schema: FsSchema = serde_json::from_str(&content)
        .map_err(|e| format!("Failed to parse schema JSON: {}", e))?;

    // Migrate entries to YMap format
    if let Some(Entry::Dir(dir)) = &schema.root {
        if let Some(entries) = &dir.entries {
            for (name, entry) in entries {
                match entry {
                    Entry::Doc(doc_entry) => {
                        if let Some(ref node_id) = doc_entry.node_id {
                            add_file(doc, name, node_id);
                        }
                    }
                    Entry::Dir(dir_entry) => {
                        add_directory(doc, name, dir_entry.node_id.as_deref());
                    }
                }
            }
        }
    }

    // Remove the old JSON text content
    {
        let mut txn = doc.transact_mut();
        if let Some(text) = txn.get_text("content") {
            let len = text.len(&txn);
            if len > 0 {
                text.remove_range(&mut txn, 0, len);
            }
        }
    }

    Ok(true)
}

/// Initialize a Y.Doc schema from an existing FsSchema.
///
/// This is used when creating a new DirectorySyncState for a directory that
/// already has a .commonplace.json file. It ensures the Y.Doc CRDT state
/// starts with the existing schema entries, preventing data loss when we
/// later write the Y.Doc back to .commonplace.json.
pub fn from_fs_schema(doc: &Doc, schema: &commonplace_types::fs::FsSchema) {
    use commonplace_types::fs::Entry;

    if let Some(Entry::Dir(dir)) = &schema.root {
        if let Some(entries) = &dir.entries {
            for (name, entry) in entries {
                match entry {
                    Entry::Doc(doc_entry) => {
                        if let Some(node_id) = &doc_entry.node_id {
                            add_file(doc, name, node_id);
                        }
                    }
                    Entry::Dir(dir_entry) => {
                        add_directory(doc, name, dir_entry.node_id.as_deref());
                    }
                }
            }
        }
    }
}

/// Convert YMap schema back to FsSchema for compatibility with existing code.
pub fn to_fs_schema(doc: &Doc) -> commonplace_types::fs::FsSchema {
    use commonplace_types::fs::{DirEntry, DocEntry, Entry, FsSchema};

    let entries_map = list_entries(doc);

    if entries_map.is_empty() {
        return FsSchema {
            version: 1,
            root: None,
        };
    }

    let mut entries = HashMap::new();
    for (name, entry) in entries_map {
        let fs_entry = match entry.entry_type {
            SchemaEntryType::Doc => Entry::Doc(DocEntry {
                node_id: entry.node_id,
                content_type: None,
            }),
            SchemaEntryType::Dir => Entry::Dir(DirEntry {
                node_id: entry.node_id,
                entries: None,
                content_type: None,
            }),
        };
        entries.insert(name, fs_entry);
    }

    FsSchema {
        version: 1,
        root: Some(Entry::Dir(DirEntry {
            node_id: None,
            entries: Some(entries),
            content_type: None,
        })),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use yrs::types::ToJson;
    use yrs::updates::decoder::Decode;

    #[test]
    fn test_add_and_get_file() {
        let doc = Doc::new();
        add_file(&doc, "test.txt", "uuid-123");

        let entry = get_entry(&doc, "test.txt").expect("Entry should exist");
        assert_eq!(entry.entry_type, SchemaEntryType::Doc);
        assert_eq!(entry.node_id, Some("uuid-123".to_string()));
    }

    #[test]
    fn test_add_and_get_directory() {
        let doc = Doc::new();
        add_directory(&doc, "subdir", Some("uuid-456"));

        let entry = get_entry(&doc, "subdir").expect("Entry should exist");
        assert_eq!(entry.entry_type, SchemaEntryType::Dir);
        assert_eq!(entry.node_id, Some("uuid-456".to_string()));
    }

    #[test]
    fn test_remove_entry() {
        let doc = Doc::new();
        add_file(&doc, "test.txt", "uuid-123");
        assert!(get_entry(&doc, "test.txt").is_some());

        remove_entry(&doc, "test.txt");
        assert!(get_entry(&doc, "test.txt").is_none());
    }

    #[test]
    fn test_list_entries() {
        let doc = Doc::new();
        add_file(&doc, "file1.txt", "uuid-1");
        add_file(&doc, "file2.txt", "uuid-2");
        add_directory(&doc, "subdir", Some("uuid-3"));

        let entries = list_entries(&doc);
        assert_eq!(entries.len(), 3);
        assert!(entries.contains_key("file1.txt"));
        assert!(entries.contains_key("file2.txt"));
        assert!(entries.contains_key("subdir"));
    }

    #[test]
    fn test_concurrent_adds_merge() {
        // Simulate two clients adding files concurrently
        // Both clients start from the same initial state (empty schema with content/root/entries structure)

        // Create initial doc with the FsSchema-compatible structure
        let initial_doc = Doc::new();
        {
            let mut txn = initial_doc.transact_mut();
            let content = txn.get_or_insert_map("content");
            content.insert(&mut txn, "version", 1_i64);
            let schema_root = content.insert(&mut txn, "root", yrs::MapPrelim::<Any>::new());
            schema_root.insert(&mut txn, "type", "dir");
            schema_root.insert(&mut txn, "entries", yrs::MapPrelim::<Any>::new());
        }
        let initial_state = {
            let txn = initial_doc.transact();
            txn.encode_state_as_update_v1(&yrs::StateVector::default())
        };

        // Client 1 starts from initial state
        let doc1 = Doc::with_client_id(1);
        {
            let update = yrs::Update::decode_v1(&initial_state).unwrap();
            let mut txn = doc1.transact_mut();
            txn.apply_update(update);
        }

        // Client 2 starts from same initial state
        let doc2 = Doc::with_client_id(2);
        {
            let update = yrs::Update::decode_v1(&initial_state).unwrap();
            let mut txn = doc2.transact_mut();
            txn.apply_update(update);
        }

        // Get state vectors before concurrent edits
        let sv1 = { doc1.transact().state_vector() };
        let sv2 = { doc2.transact().state_vector() };

        // Client 1 adds file1
        add_file(&doc1, "file1.txt", "uuid-1");
        let update1 = {
            let txn = doc1.transact();
            txn.encode_state_as_update_v1(&sv2) // Changes since sv2
        };

        // Client 2 adds file2
        add_file(&doc2, "file2.txt", "uuid-2");
        let update2 = {
            let txn = doc2.transact();
            txn.encode_state_as_update_v1(&sv1) // Changes since sv1
        };

        // Merge updates bidirectionally
        {
            let update = yrs::Update::decode_v1(&update2).unwrap();
            let mut txn = doc1.transact_mut();
            txn.apply_update(update);
        }
        {
            let update = yrs::Update::decode_v1(&update1).unwrap();
            let mut txn = doc2.transact_mut();
            txn.apply_update(update);
        }

        // Both docs should have both files
        let entries1 = list_entries(&doc1);
        let entries2 = list_entries(&doc2);

        assert_eq!(
            entries1.len(),
            2,
            "doc1 should have 2 entries: {:?}",
            entries1
        );
        assert_eq!(
            entries2.len(),
            2,
            "doc2 should have 2 entries: {:?}",
            entries2
        );
        assert!(entries1.contains_key("file1.txt"));
        assert!(entries1.contains_key("file2.txt"));
        assert!(entries2.contains_key("file1.txt"));
        assert!(entries2.contains_key("file2.txt"));
    }

    #[test]
    fn test_is_ymap_format() {
        let doc = Doc::new();
        assert!(!is_ymap_format(&doc));

        add_file(&doc, "test.txt", "uuid-123");
        assert!(is_ymap_format(&doc));
    }

    #[test]
    fn test_to_fs_schema() {
        let doc = Doc::new();
        add_file(&doc, "file1.txt", "uuid-1");
        add_directory(&doc, "subdir", Some("uuid-2"));

        let schema = to_fs_schema(&doc);
        assert_eq!(schema.version, 1);
        assert!(schema.root.is_some());

        if let Some(commonplace_types::fs::Entry::Dir(dir)) = &schema.root {
            let entries = dir.entries.as_ref().unwrap();
            assert_eq!(entries.len(), 2);
            assert!(entries.contains_key("file1.txt"));
            assert!(entries.contains_key("subdir"));
        } else {
            panic!("Expected Dir entry");
        }
    }

    /// Test that YMap schema structure produces valid FsSchema JSON when server calls to_json().
    ///
    /// This is the critical integration test: the server reads JSON content by calling
    /// `map.to_json()` on the YMap at "content" root. This test verifies that our YMap
    /// structure produces JSON that parses as a valid FsSchema.
    #[test]
    fn test_ymap_to_json_produces_valid_fs_schema() {
        use commonplace_types::fs::FsSchema;

        let doc = Doc::new();
        add_file(&doc, "test.txt", "uuid-123");
        add_directory(&doc, "mydir", Some("uuid-456"));

        // Get the "content" YMap (same as server does for JSON content type)
        let txn = doc.transact();
        let content = txn.get_map("content").expect("content map should exist");

        // Convert to JSON (same as server does in document.rs:200)
        let any = content.to_json(&txn);
        let json_str = serde_json::to_string(&any).expect("to_json result should be serializable");

        // Parse as FsSchema (proves server can read it correctly)
        let schema: FsSchema =
            serde_json::from_str(&json_str).expect("JSON should parse as FsSchema");

        // Verify structure
        assert_eq!(schema.version, 1);
        assert!(schema.root.is_some());

        if let Some(commonplace_types::fs::Entry::Dir(dir)) = &schema.root {
            let entries = dir.entries.as_ref().expect("should have entries");
            assert_eq!(entries.len(), 2, "should have 2 entries");

            // Check file entry
            let file_entry = entries.get("test.txt").expect("test.txt should exist");
            if let commonplace_types::fs::Entry::Doc(doc_entry) = file_entry {
                assert_eq!(doc_entry.node_id, Some("uuid-123".to_string()));
            } else {
                panic!("test.txt should be a Doc entry");
            }

            // Check dir entry
            let dir_entry = entries.get("mydir").expect("mydir should exist");
            if let commonplace_types::fs::Entry::Dir(sub_dir) = dir_entry {
                assert_eq!(sub_dir.node_id, Some("uuid-456".to_string()));
            } else {
                panic!("mydir should be a Dir entry");
            }
        } else {
            panic!("Expected Dir root entry");
        }
    }

    /// Test that empty schema produces valid empty FsSchema JSON.
    #[test]
    fn test_empty_ymap_to_json() {
        use commonplace_types::fs::FsSchema;

        let doc = Doc::new();
        // Trigger creation of the FsSchema structure without adding entries
        {
            let mut txn = doc.transact_mut();
            let _ = get_or_create_entries(&mut txn);
        }

        // Get the "content" YMap
        let txn = doc.transact();
        let content = txn.get_map("content").expect("content map should exist");

        // Convert to JSON
        let any = content.to_json(&txn);
        let json_str = serde_json::to_string(&any).expect("should serialize");

        // Parse as FsSchema
        let schema: FsSchema = serde_json::from_str(&json_str).expect("should parse as FsSchema");

        assert_eq!(schema.version, 1);
        assert!(schema.root.is_some());

        if let Some(commonplace_types::fs::Entry::Dir(dir)) = &schema.root {
            // Empty entries map
            let entries = dir.entries.as_ref().expect("should have entries");
            assert!(entries.is_empty(), "entries should be empty");
        } else {
            panic!("Expected Dir root entry");
        }
    }

    #[test]
    fn test_add_directory_none_preserves_existing_node_id() {
        // CP-urp5: calling add_directory with node_id=None should NOT overwrite
        // an existing entry that has a valid node_id.
        let doc = Doc::new();

        // Add directory with a node_id
        add_directory(&doc, "subdir", Some("uuid-456"));
        let entry = get_entry(&doc, "subdir").expect("Entry should exist");
        assert_eq!(entry.node_id, Some("uuid-456".to_string()));

        // Now add the same directory with node_id=None (simulates scan_directory
        // returning None because .commonplace.json doesn't have the node_id yet)
        add_directory(&doc, "subdir", None);

        // The existing node_id should be PRESERVED, not overwritten
        let entry = get_entry(&doc, "subdir").expect("Entry should still exist");
        assert_eq!(
            entry.node_id,
            Some("uuid-456".to_string()),
            "CP-urp5: add_directory with None must not overwrite existing node_id"
        );
    }

    #[test]
    fn test_add_directory_some_overwrites_existing_node_id() {
        // When add_directory is called with a DIFFERENT Some(node_id),
        // it should update the node_id (this is a legitimate update).
        let doc = Doc::new();

        add_directory(&doc, "subdir", Some("uuid-old"));
        let entry = get_entry(&doc, "subdir").expect("Entry should exist");
        assert_eq!(entry.node_id, Some("uuid-old".to_string()));

        // Update with a new node_id
        add_directory(&doc, "subdir", Some("uuid-new"));
        let entry = get_entry(&doc, "subdir").expect("Entry should exist");
        assert_eq!(entry.node_id, Some("uuid-new".to_string()));
    }
}

/// Test that root_refs() finds YMap after applying update.
#[test]
fn test_root_refs_after_update() {
    use base64::{engine::general_purpose::STANDARD, Engine};
    use yrs::updates::decoder::Decode;
    use yrs::{types::ToJson, ReadTxn, Update};

    // This is the sync client update
    let sync_update_b64 = "CQKajuDcDQAhAJmEvqcFAw5kZWJ1Zy10ZXN0LnR4dAEAAgLn4YWyCwAhAJmEvqcFAw5jbGVhbi10ZXN0LnR4dAEAAgLPo+6wCwAhAJmEvqcFAw5maW5hbC10ZXN0LnR4dAEAAgPW07jGCgAnAJmEvqcFAw50b3BpYy10ZXN0LnR4dAEoANbTuMYKAAR0eXBlAXcDZG9jKADW07jGCgAHbm9kZV9pZAF3JDY5OTJiNjA1LWFiYTgtNDMyMC1iZjY3LWI0NDViNTRmOTVmOAKohNHiCAAhAJmEvqcFAw9tcXR0LWNyZWF0ZS50eHQBAAICs7merggAIQCZhL6nBQMPc2VydmVyLXRlc3QudHh0AQACD5mEvqcFACgBB2NvbnRlbnQHdmVyc2lvbgF6AAAAAAAAAAEnAQdjb250ZW50BHJvb3QBKACZhL6nBQEEdHlwZQF3A2RpcicAmYS+pwUBB2VudHJpZXMBJwCZhL6nBQMNdGVzdC1ub3RlLnR4dAEoAJmEvqcFBAR0eXBlAXcDZG9jKACZhL6nBQQHbm9kZV9pZAF3JGIxZmIzYWMyLWNjNGQtNGQ1MC1iMDc3LTQ3YWEwYmY0NWJlNCcAmYS+pwUDDnRyYWNlLXRlc3QudHh0ASgAmYS+pwUHBHR5cGUBdwNkb2MoAJmEvqcFBwdub2RlX2lkAXckNDY2YzdhMzYtNTVhMy00Mzg1LThiYzctY2NhODE3MTA0MmNlIQCZhL6nBQMQdHJpZ2dlci10ZXN0LnR4dAEAAicAmYS+pwUDCWlucHV0LnR4dAEoAJmEvqcFDQR0eXBlAXcDZG9jKACZhL6nBQ0Hbm9kZV9pZAF3JDBjYTIxMGQwLTQzNGYtNDNlYy1hZWQ5LTVhNTM4ZGMzYmI1NwO00POTBQAnAJmEvqcFAw9zY2hlbWEtdGVzdC50eHQBKAC00POTBQAEdHlwZQF3A2RvYygAtNDzkwUAB25vZGVfaWQBdyQ5OWRiYWNlMy0zMDIzLTRkZmEtYWEwMy0xMWZhM2NmNzA5MWIC8a6q1gMAIQCZhL6nBQMObXF0dC10cmFjZS50eHQBAAIHqITR4ggBAAPxrqrWAwEAA5mEvqcFAQoDs7merggBAAOajuDcDQEAA8+j7rALAQAD5+GFsgsBAAM=";

    let update_bytes = STANDARD.decode(sync_update_b64).expect("decode b64");

    // Simulate server: pre-create empty YMap
    let doc = Doc::with_client_id(1);
    {
        let mut txn = doc.transact_mut();
        txn.get_or_insert_map("content");
    }

    // Apply update
    {
        let update = Update::decode_v1(&update_bytes).expect("decode update");
        let mut txn = doc.transact_mut();
        txn.apply_update(update);
    }

    // Now check root_refs
    let txn = doc.transact();
    let roots: Vec<_> = txn.root_refs().collect();
    eprintln!("root_refs count: {}", roots.len());
    for (name, value) in &roots {
        eprintln!("  root '{}': {:?}", name, value);
    }

    // Find "content" specifically
    let content_root = txn
        .root_refs()
        .find(|(name, _)| *name == "content")
        .map(|(_, value)| value);

    match content_root {
        Some(yrs::types::Value::YMap(map)) => {
            let any = map.to_json(&txn);
            let json = serde_json::to_string_pretty(&any).unwrap();
            eprintln!("Found YMap, JSON:\n{}", &json[..json.len().min(500)]);
        }
        Some(other) => {
            eprintln!("Found non-YMap: {:?}", other);
        }
        None => {
            eprintln!("No 'content' root found!");
        }
    }
}
