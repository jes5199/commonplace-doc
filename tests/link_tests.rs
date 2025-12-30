//! Integration tests for commonplace-link

use commonplace_doc::fs::{DocEntry, Entry, FsSchema};
use std::fs;
use tempfile::TempDir;

/// Create a test schema with a single file
fn create_test_schema(entries: Vec<(&str, Option<&str>)>) -> FsSchema {
    let mut entry_map = std::collections::HashMap::new();
    for (name, node_id) in entries {
        entry_map.insert(
            name.to_string(),
            Entry::Doc(DocEntry {
                node_id: node_id.map(|s| s.to_string()),
                content_type: None,
            }),
        );
    }

    FsSchema {
        version: 1,
        root: Some(Entry::Dir(commonplace_doc::fs::DirEntry {
            entries: Some(entry_map),
            node_id: None,
            content_type: None,
        })),
    }
}

#[test]
fn test_schema_with_shared_node_id() {
    // Test that two entries can share the same node_id
    let schema = create_test_schema(vec![
        ("config.json", Some("shared-uuid")),
        ("settings.json", Some("shared-uuid")),
    ]);

    let json = serde_json::to_string_pretty(&schema).unwrap();

    // Parse it back
    let parsed: FsSchema = serde_json::from_str(&json).unwrap();

    // Both entries should have the same node_id
    if let Some(Entry::Dir(root)) = parsed.root {
        let entries = root.entries.unwrap();

        let config = entries.get("config.json").unwrap();
        let settings = entries.get("settings.json").unwrap();

        if let (Entry::Doc(config_doc), Entry::Doc(settings_doc)) = (config, settings) {
            assert_eq!(config_doc.node_id, settings_doc.node_id);
            assert_eq!(config_doc.node_id, Some("shared-uuid".to_string()));
        } else {
            panic!("Expected Doc entries");
        }
    } else {
        panic!("Expected Dir root");
    }
}

#[test]
fn test_schema_roundtrip_with_links() {
    // Create a schema, modify it to add a link, verify it survives roundtrip
    let temp_dir = TempDir::new().unwrap();
    let schema_path = temp_dir.path().join(".commonplace.json");

    // Write initial schema
    let initial = create_test_schema(vec![("source.txt", None)]);
    let initial_json = serde_json::to_string_pretty(&initial).unwrap();
    fs::write(&schema_path, &initial_json).unwrap();

    // Read it back
    let content = fs::read_to_string(&schema_path).unwrap();
    let mut schema: FsSchema = serde_json::from_str(&content).unwrap();

    // Simulate what link tool does: add node_id to source, create link entry
    if let Some(Entry::Dir(ref mut root)) = schema.root {
        if let Some(ref mut entries) = root.entries {
            // Update source with node_id
            if let Some(Entry::Doc(ref mut source)) = entries.get_mut("source.txt") {
                source.node_id = Some("test-uuid-123".to_string());
            }

            // Add link entry with same node_id
            entries.insert(
                "link.txt".to_string(),
                Entry::Doc(DocEntry {
                    node_id: Some("test-uuid-123".to_string()),
                    content_type: None,
                }),
            );
        }
    }

    // Write modified schema
    let modified_json = serde_json::to_string_pretty(&schema).unwrap();
    fs::write(&schema_path, &modified_json).unwrap();

    // Read and verify
    let final_content = fs::read_to_string(&schema_path).unwrap();
    let final_schema: FsSchema = serde_json::from_str(&final_content).unwrap();

    if let Some(Entry::Dir(root)) = final_schema.root {
        let entries = root.entries.unwrap();
        assert!(entries.contains_key("source.txt"));
        assert!(entries.contains_key("link.txt"));

        if let (Some(Entry::Doc(source)), Some(Entry::Doc(link))) =
            (entries.get("source.txt"), entries.get("link.txt"))
        {
            assert_eq!(source.node_id, link.node_id);
            assert_eq!(source.node_id, Some("test-uuid-123".to_string()));
        }
    }
}
