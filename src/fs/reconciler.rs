//! Filesystem reconciler: watches the fs-root document and creates documents for entries.

use super::error::FsError;
use super::schema::{DirEntry, Entry, FsSchema};
use crate::document::{ContentType, DocumentStore};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::RwLock;

/// Result of a schema migration operation.
pub struct MigrationResult {
    /// The migrated schema as JSON string
    pub schema: String,
    /// Whether any migration was performed
    pub migrated: bool,
    /// Number of inline subdirectories that were migrated
    pub subdirs_migrated: usize,
}

/// Manages the filesystem abstraction layer.
///
/// Parses the fs-root document JSON and ensures documents exist for each
/// entry declared in the filesystem schema.
pub struct FilesystemReconciler {
    /// The fs-root document ID
    fs_root_id: String,
    /// Reference to document store
    document_store: Arc<DocumentStore>,
    /// Last successfully parsed schema (kept on parse errors)
    last_valid_schema: RwLock<Option<FsSchema>>,
    /// Set of document IDs we've already created
    known_documents: RwLock<HashSet<String>>,
    /// Last valid schemas for node-backed directories (document_id -> schema)
    last_valid_node_schemas: RwLock<std::collections::HashMap<String, FsSchema>>,
}

impl FilesystemReconciler {
    /// Create a new FilesystemReconciler.
    pub fn new(fs_root_id: String, document_store: Arc<DocumentStore>) -> Self {
        Self {
            fs_root_id,
            document_store,
            last_valid_schema: RwLock::new(None),
            known_documents: RwLock::new(HashSet::new()),
            last_valid_node_schemas: RwLock::new(std::collections::HashMap::new()),
        }
    }

    /// Reconcile current state: parse JSON, collect entries, create missing documents.
    ///
    /// This method also handles migration of inline subdirectories to separate documents.
    /// When inline subdirectories are found, they are extracted into new documents and
    /// the parent schema is updated with `node_id` references.
    ///
    /// Note: Uses cycle detection to prevent infinite recursion on cyclic node-backed dirs.
    pub async fn reconcile(&self, content: &str) -> Result<(), FsError> {
        // 0. First, migrate any inline subdirectories
        let migration_result = self.migrate_inline_subdirectories(content).await?;

        let effective_content = if migration_result.migrated {
            tracing::info!(
                "Migrated {} inline subdirectories to separate documents",
                migration_result.subdirs_migrated
            );

            // Update fs-root document with migrated schema
            if let Err(e) = self
                .document_store
                .set_content(&self.fs_root_id, &migration_result.schema)
                .await
            {
                tracing::warn!("Failed to update fs-root with migrated schema: {:?}", e);
                // Continue with original content if update fails
                content.to_string()
            } else {
                migration_result.schema
            }
        } else {
            content.to_string()
        };

        // 1. Parse JSON (use effective content which may be migrated)
        let schema: FsSchema = serde_json::from_str(&effective_content)
            .map_err(|e| FsError::ParseError(e.to_string()))?;

        // 2. Validate version
        if schema.version != 1 {
            return Err(FsError::UnsupportedVersion(schema.version));
        }

        // 3. Validate root is a directory (if present)
        if let Some(ref root) = schema.root {
            if !matches!(root, Entry::Dir(_)) {
                return Err(FsError::SchemaError("root must be a directory".to_string()));
            }
        }

        // 4. Collect all entries from schema (with cycle detection)
        let mut ignored_dirs = HashSet::new();
        let mut recursion_stack = HashSet::new();
        let entries = if let Some(ref root) = schema.root {
            self.collect_entries_with_dirs(root, "", &mut ignored_dirs, &mut recursion_stack)
                .await?
        } else {
            vec![]
        };

        // 5. For each entry, ensure document exists
        let mut known = self.known_documents.write().await;
        for (path, doc_id, content_type) in entries {
            // Check if document exists in store (not just known_documents)
            // This handles the case where a document was deleted externally
            let doc_exists = self.document_store.get_document(&doc_id).await.is_some();

            if !doc_exists {
                self.document_store
                    .get_or_create_with_id(&doc_id, content_type)
                    .await;

                tracing::info!("Reconciler created document: {} -> {}", path, doc_id);
            }

            // Track in known_documents regardless
            known.insert(doc_id.clone());
        }

        // 6. Update last valid schema
        *self.last_valid_schema.write().await = Some(schema);

        Ok(())
    }

    /// Walk the entry tree, collecting entries and tracking document-backed directory IDs.
    /// Uses recursion_stack to detect cycles (same document in current path), while
    /// doc_backed_dirs collects all unique document-backed dirs for tracking.
    async fn collect_entries_with_dirs(
        &self,
        entry: &Entry,
        current_path: &str,
        doc_backed_dirs: &mut HashSet<String>,
        recursion_stack: &mut HashSet<String>,
    ) -> Result<Vec<(String, String, ContentType)>, FsError> {
        let mut results = vec![];

        match entry {
            Entry::Doc(doc) => {
                let doc_id = doc
                    .node_id
                    .clone()
                    .unwrap_or_else(|| self.derive_doc_id(current_path));
                let content_type = doc
                    .content_type
                    .as_deref()
                    .and_then(ContentType::from_mime)
                    .unwrap_or(ContentType::Json);
                results.push((current_path.to_string(), doc_id, content_type));
            }
            Entry::Dir(dir) => {
                // Spec: document-backed and inline forms are mutually exclusive
                if dir.node_id.is_some() && dir.entries.is_some() {
                    return Err(FsError::SchemaError(format!(
                        "Directory at '{}' has both node_id and entries (mutually exclusive)",
                        if current_path.is_empty() {
                            "/"
                        } else {
                            current_path
                        }
                    )));
                }

                // Handle document-backed directory
                if let Some(ref doc_id) = dir.node_id {
                    let content_type = dir
                        .content_type
                        .as_deref()
                        .and_then(ContentType::from_mime)
                        .unwrap_or(ContentType::Json);

                    // First, ensure the document exists
                    results.push((
                        current_path.to_string(),
                        doc_id.clone(),
                        content_type.clone(),
                    ));

                    // Track this as a document-backed directory
                    doc_backed_dirs.insert(doc_id.clone());

                    // Check for cycles - only skip if this document is in current recursion path
                    // (same document at different paths is allowed - multi-mount)
                    if recursion_stack.contains(doc_id) {
                        tracing::warn!(
                            "Cycle detected: document-backed dir {} in current path, skipping",
                            doc_id
                        );
                    } else {
                        // Add to recursion stack before descending
                        recursion_stack.insert(doc_id.clone());

                        // Try to recursively process its content
                        if let Some(child_entries) = self
                            .collect_doc_backed_dir_entries_with_dirs(
                                doc_id,
                                current_path,
                                doc_backed_dirs,
                                recursion_stack,
                            )
                            .await
                        {
                            results.extend(child_entries);
                        }

                        // Remove from recursion stack after returning
                        recursion_stack.remove(doc_id);
                    }
                }

                // Handle inline entries
                if let Some(ref entries) = dir.entries {
                    for (name, child) in entries {
                        // Validate name
                        Entry::validate_name(name)?;

                        let child_path = if current_path.is_empty() {
                            name.clone()
                        } else {
                            format!("{}/{}", current_path, name)
                        };
                        results.extend(
                            Box::pin(self.collect_entries_with_dirs(
                                child,
                                &child_path,
                                doc_backed_dirs,
                                recursion_stack,
                            ))
                            .await?,
                        );
                    }
                }
            }
        }

        Ok(results)
    }

    /// Try to fetch and parse a document-backed directory's content.
    async fn collect_doc_backed_dir_entries_with_dirs(
        &self,
        doc_id: &str,
        base_path: &str,
        doc_backed_dirs: &mut HashSet<String>,
        recursion_stack: &mut HashSet<String>,
    ) -> Option<Vec<(String, String, ContentType)>> {
        // Try to get existing document - not an error if it doesn't exist yet
        let doc = self.document_store.get_document(doc_id).await?;

        let content = doc.content;

        // Empty content is not an error (document just hasn't been populated)
        if content.is_empty() || content == "{}" {
            return None;
        }

        // Try to parse as filesystem schema - fall back to cached on error
        let schema: FsSchema = match serde_json::from_str(&content) {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!(
                    "Failed to parse document-backed dir {} at {}: {}",
                    doc_id,
                    base_path,
                    e
                );
                // Try to use cached schema for this document
                let cache = self.last_valid_node_schemas.read().await;
                if let Some(cached) = cache.get(doc_id) {
                    cached.clone()
                } else {
                    return None;
                }
            }
        };

        // Validate version - fall back to cached on unsupported version
        if schema.version != 1 {
            tracing::warn!(
                "Unsupported version {} in document-backed dir {} at {}",
                schema.version,
                doc_id,
                base_path
            );
            // Try to use cached schema for this document
            let cache = self.last_valid_node_schemas.read().await;
            if let Some(cached) = cache.get(doc_id) {
                // Use cached schema instead
                return self
                    .collect_from_valid_doc_schema(
                        cached,
                        base_path,
                        doc_backed_dirs,
                        recursion_stack,
                    )
                    .await;
            }
            return None;
        }

        // Validate root is a directory - fall back to cached on invalid root
        if let Some(ref root) = schema.root {
            if !matches!(root, Entry::Dir(_)) {
                tracing::warn!(
                    "Invalid root (not a directory) in document-backed dir {} at {}",
                    doc_id,
                    base_path
                );
                // Try to use cached schema for this document
                let cache = self.last_valid_node_schemas.read().await;
                if let Some(cached) = cache.get(doc_id) {
                    return self
                        .collect_from_valid_doc_schema(
                            cached,
                            base_path,
                            doc_backed_dirs,
                            recursion_stack,
                        )
                        .await;
                }
                return None;
            }
        }

        // Cache this valid schema
        {
            let mut cache = self.last_valid_node_schemas.write().await;
            cache.insert(doc_id.to_string(), schema.clone());
        }

        // Recursively collect from the nested root
        self.collect_from_valid_doc_schema(&schema, base_path, doc_backed_dirs, recursion_stack)
            .await
    }

    /// Collect entries from a validated document schema.
    async fn collect_from_valid_doc_schema(
        &self,
        schema: &FsSchema,
        base_path: &str,
        doc_backed_dirs: &mut HashSet<String>,
        recursion_stack: &mut HashSet<String>,
    ) -> Option<Vec<(String, String, ContentType)>> {
        if let Some(ref root) = schema.root {
            match Box::pin(self.collect_entries_with_dirs(
                root,
                base_path,
                doc_backed_dirs,
                recursion_stack,
            ))
            .await
            {
                Ok(entries) => Some(entries),
                Err(e) => {
                    tracing::warn!("Error collecting entries at {}: {}", base_path, e);
                    None
                }
            }
        } else {
            Some(vec![])
        }
    }

    /// Derive document ID from path: `<fs-root-id>:<path>`.
    fn derive_doc_id(&self, path: &str) -> String {
        if path.is_empty() {
            // Root entry without explicit doc_id - use fs-root itself
            self.fs_root_id.clone()
        } else {
            format!("{}:{}", self.fs_root_id, path)
        }
    }

    /// Get the fs-root document ID.
    pub fn fs_root_id(&self) -> &str {
        &self.fs_root_id
    }

    /// Migrate inline subdirectories to separate documents.
    ///
    /// Walks the schema tree, and for each directory with inline `entries`,
    /// creates a new document with those entries and replaces the inline
    /// entries with a `node_id` reference.
    ///
    /// Returns the migrated schema and whether any migration occurred.
    pub async fn migrate_inline_subdirectories(
        &self,
        content: &str,
    ) -> Result<MigrationResult, FsError> {
        // Parse schema
        let schema: FsSchema =
            serde_json::from_str(content).map_err(|e| FsError::ParseError(e.to_string()))?;

        if schema.version != 1 {
            return Err(FsError::UnsupportedVersion(schema.version));
        }

        // If no root, nothing to migrate
        let Some(root) = schema.root else {
            return Ok(MigrationResult {
                schema: content.to_string(),
                migrated: false,
                subdirs_migrated: 0,
            });
        };

        // Migrate the root entry
        let (migrated_root, count) = self.migrate_entry(root).await?;

        if count == 0 {
            return Ok(MigrationResult {
                schema: content.to_string(),
                migrated: false,
                subdirs_migrated: 0,
            });
        }

        // Build new schema
        let new_schema = FsSchema {
            version: 1,
            root: Some(migrated_root),
        };

        let schema_json = serde_json::to_string_pretty(&new_schema)
            .map_err(|e| FsError::SchemaError(format!("Failed to serialize schema: {}", e)))?;

        Ok(MigrationResult {
            schema: schema_json,
            migrated: true,
            subdirs_migrated: count,
        })
    }

    /// Migrate a single entry, returning the migrated entry and count of migrations.
    async fn migrate_entry(&self, entry: Entry) -> Result<(Entry, usize), FsError> {
        match entry {
            Entry::Doc(doc) => Ok((Entry::Doc(doc), 0)),
            Entry::Dir(dir) => self.migrate_dir_entry(dir, "").await,
        }
    }

    /// Migrate a directory entry and its children.
    ///
    /// `current_path` is the path to this directory, used to derive stable document IDs.
    async fn migrate_dir_entry(
        &self,
        dir: DirEntry,
        current_path: &str,
    ) -> Result<(Entry, usize), FsError> {
        // If already node-backed (no inline entries), nothing to do
        let Some(entries) = dir.entries else {
            return Ok((Entry::Dir(dir), 0));
        };

        // Process each child entry
        let mut new_entries: HashMap<String, Entry> = HashMap::new();
        let mut total_count = 0;

        for (name, child) in entries {
            let child_path = if current_path.is_empty() {
                name.clone()
            } else {
                format!("{}/{}", current_path, name)
            };

            match child {
                Entry::Doc(doc) => {
                    // Docs don't need migration, just copy
                    new_entries.insert(name, Entry::Doc(doc));
                }
                Entry::Dir(child_dir) => {
                    if child_dir.entries.is_some() {
                        // This is an inline subdirectory - migrate it!
                        let (migrated_child, child_count) =
                            Box::pin(self.migrate_dir_entry(child_dir, &child_path)).await?;

                        // Derive stable document ID from path (not random UUID)
                        // This ensures same ID on restart for idempotent migration
                        let new_doc_id = self.derive_doc_id(&child_path);

                        // Build the subdirectory's schema
                        let subdir_schema = FsSchema {
                            version: 1,
                            root: Some(migrated_child),
                        };

                        let subdir_json =
                            serde_json::to_string_pretty(&subdir_schema).map_err(|e| {
                                FsError::SchemaError(format!("Failed to serialize: {}", e))
                            })?;

                        // Create the document (or get existing)
                        self.document_store
                            .get_or_create_with_id(&new_doc_id, ContentType::Json)
                            .await;

                        // Set its content
                        if let Err(e) = self
                            .document_store
                            .set_content(&new_doc_id, &subdir_json)
                            .await
                        {
                            tracing::warn!(
                                "Failed to set content for subdirectory document {}: {:?}",
                                new_doc_id,
                                e
                            );
                        } else {
                            tracing::info!(
                                "Migrated inline subdirectory '{}' to document {}",
                                name,
                                new_doc_id
                            );
                        }

                        // Replace with node_id reference
                        new_entries.insert(
                            name,
                            Entry::Dir(DirEntry {
                                entries: None,
                                node_id: Some(new_doc_id),
                                content_type: Some("application/json".to_string()),
                            }),
                        );

                        total_count += 1 + child_count;
                    } else {
                        // Already node-backed, just copy
                        new_entries.insert(name, Entry::Dir(child_dir));
                    }
                }
            }
        }

        Ok((
            Entry::Dir(DirEntry {
                entries: Some(new_entries),
                node_id: dir.node_id,
                content_type: dir.content_type,
            }),
            total_count,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_derive_doc_id() {
        let store = Arc::new(DocumentStore::new());
        let reconciler = FilesystemReconciler::new("my-fs".to_string(), store);

        assert_eq!(
            reconciler.derive_doc_id("notes/ideas.txt"),
            "my-fs:notes/ideas.txt"
        );
        assert_eq!(reconciler.derive_doc_id("file.txt"), "my-fs:file.txt");
        assert_eq!(reconciler.derive_doc_id(""), "my-fs");
    }

    #[tokio::test]
    async fn test_migrate_no_subdirs() {
        let store = Arc::new(DocumentStore::new());
        let reconciler = FilesystemReconciler::new("fs-root".to_string(), store);

        // Schema with no subdirectories - no migration needed
        let content = r#"{
            "version": 1,
            "root": {
                "type": "dir",
                "entries": {
                    "file.txt": {"type": "doc"}
                }
            }
        }"#;

        let result = reconciler
            .migrate_inline_subdirectories(content)
            .await
            .unwrap();

        assert!(!result.migrated);
        assert_eq!(result.subdirs_migrated, 0);
    }

    #[tokio::test]
    async fn test_migrate_inline_subdir() {
        let store = Arc::new(DocumentStore::new());
        let reconciler = FilesystemReconciler::new("fs-root".to_string(), store.clone());

        // Schema with inline subdirectory
        let content = r#"{
            "version": 1,
            "root": {
                "type": "dir",
                "entries": {
                    "file.txt": {"type": "doc"},
                    "notes": {
                        "type": "dir",
                        "entries": {
                            "todo.txt": {"type": "doc"}
                        }
                    }
                }
            }
        }"#;

        let result = reconciler
            .migrate_inline_subdirectories(content)
            .await
            .unwrap();

        assert!(result.migrated);
        assert_eq!(result.subdirs_migrated, 1);

        // Check that the migrated schema has node_id instead of entries
        let migrated_schema: FsSchema = serde_json::from_str(&result.schema).unwrap();
        if let Some(Entry::Dir(root)) = migrated_schema.root {
            let entries = root.entries.unwrap();
            if let Some(Entry::Dir(notes)) = entries.get("notes") {
                assert!(notes.node_id.is_some());
                assert!(notes.entries.is_none());
                // The node_id should be derived from path
                assert_eq!(notes.node_id.as_deref(), Some("fs-root:notes"));
            } else {
                panic!("Expected notes directory");
            }
        } else {
            panic!("Expected root directory");
        }

        // Check that the subdirectory document was created
        let subdir_doc = store.get_document("fs-root:notes").await;
        assert!(subdir_doc.is_some());

        // Check subdirectory content
        let subdir_content = subdir_doc.unwrap().content;
        let subdir_schema: FsSchema = serde_json::from_str(&subdir_content).unwrap();
        assert_eq!(subdir_schema.version, 1);
        if let Some(Entry::Dir(subdir_root)) = subdir_schema.root {
            let entries = subdir_root.entries.unwrap();
            assert!(entries.contains_key("todo.txt"));
        } else {
            panic!("Expected subdirectory root");
        }
    }

    #[tokio::test]
    async fn test_migrate_nested_subdirs() {
        let store = Arc::new(DocumentStore::new());
        let reconciler = FilesystemReconciler::new("fs-root".to_string(), store.clone());

        // Schema with nested subdirectories
        let content = r#"{
            "version": 1,
            "root": {
                "type": "dir",
                "entries": {
                    "level1": {
                        "type": "dir",
                        "entries": {
                            "level2": {
                                "type": "dir",
                                "entries": {
                                    "deep.txt": {"type": "doc"}
                                }
                            }
                        }
                    }
                }
            }
        }"#;

        let result = reconciler
            .migrate_inline_subdirectories(content)
            .await
            .unwrap();

        assert!(result.migrated);
        assert_eq!(result.subdirs_migrated, 2); // level1 and level2

        // Check that both subdirectory documents were created
        assert!(store.get_document("fs-root:level1").await.is_some());
        assert!(store.get_document("fs-root:level1/level2").await.is_some());
    }

    #[tokio::test]
    async fn test_migrate_already_node_backed() {
        let store = Arc::new(DocumentStore::new());
        let reconciler = FilesystemReconciler::new("fs-root".to_string(), store);

        // Schema where subdirectory already has node_id
        let content = r#"{
            "version": 1,
            "root": {
                "type": "dir",
                "entries": {
                    "external": {
                        "type": "dir",
                        "node_id": "existing-doc-id"
                    }
                }
            }
        }"#;

        let result = reconciler
            .migrate_inline_subdirectories(content)
            .await
            .unwrap();

        // Already node-backed, no migration needed
        assert!(!result.migrated);
        assert_eq!(result.subdirs_migrated, 0);
    }
}
