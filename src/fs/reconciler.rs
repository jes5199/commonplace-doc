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
    /// Number of UUIDs generated for entries without node_id
    pub uuids_generated: usize,
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
    /// Set of document IDs that are node-backed directories
    /// (separate from last_valid_node_schemas since those are only populated after
    /// the directory content is parsed, but we need to track dirs immediately)
    node_backed_dir_ids: RwLock<HashSet<String>>,
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
            node_backed_dir_ids: RwLock::new(HashSet::new()),
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
            if migration_result.subdirs_migrated > 0 {
                tracing::info!(
                    "Migrated {} inline subdirectories to separate documents",
                    migration_result.subdirs_migrated
                );
            }
            if migration_result.uuids_generated > 0 {
                tracing::info!(
                    "Generated {} UUIDs for entries without node_id",
                    migration_result.uuids_generated
                );
            }

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

        // 6. Replace the set of node-backed directory IDs with current ones
        // (ignored_dirs contains all directories with entries: null and node_id)
        // We replace rather than extend to avoid stale IDs from removed directories
        {
            let mut node_dirs = self.node_backed_dir_ids.write().await;
            *node_dirs = ignored_dirs;
        }

        // 7. Update last valid schema
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
                // After migration, all docs should have node_id
                let doc_id = doc.node_id.clone().unwrap_or_else(|| {
                    tracing::warn!(
                        "Doc at '{}' missing node_id - migration should have assigned one",
                        current_path
                    );
                    // Fallback: generate a new UUID (shouldn't happen after migration)
                    uuid::Uuid::new_v4().to_string()
                });
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

    /// Get the fs-root document ID.
    pub fn fs_root_id(&self) -> &str {
        &self.fs_root_id
    }

    /// Check if a document ID is a known node-backed directory.
    ///
    /// Returns true if this document was identified as a node-backed directory
    /// during schema parsing (has `entries: null` and a `node_id`).
    pub async fn is_node_backed_directory(&self, doc_id: &str) -> bool {
        let dirs = self.node_backed_dir_ids.read().await;
        dirs.contains(doc_id)
    }

    /// Get the content of the fs-root document for re-reconciliation.
    pub async fn get_fs_root_content(&self) -> Option<String> {
        self.document_store
            .get_document(&self.fs_root_id)
            .await
            .map(|doc| doc.content)
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
                uuids_generated: 0,
            });
        };

        // Migrate the root entry
        let (migrated_root, subdirs_count, uuids_count) = self.migrate_entry(root).await?;

        if subdirs_count == 0 && uuids_count == 0 {
            return Ok(MigrationResult {
                schema: content.to_string(),
                migrated: false,
                subdirs_migrated: 0,
                uuids_generated: 0,
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
            subdirs_migrated: subdirs_count,
            uuids_generated: uuids_count,
        })
    }

    /// Migrate a single entry, returning the migrated entry and (subdirs_migrated, uuids_generated).
    async fn migrate_entry(&self, entry: Entry) -> Result<(Entry, usize, usize), FsError> {
        match entry {
            Entry::Doc(mut doc) => {
                // Generate UUID if doc doesn't have node_id
                let uuid_count = if doc.node_id.is_none() {
                    doc.node_id = Some(uuid::Uuid::new_v4().to_string());
                    1
                } else {
                    0
                };
                Ok((Entry::Doc(doc), 0, uuid_count))
            }
            Entry::Dir(dir) => self.migrate_dir_entry(dir, "").await,
        }
    }

    /// Migrate a directory entry and its children.
    ///
    /// Returns (migrated_entry, subdirs_migrated, uuids_generated).
    async fn migrate_dir_entry(
        &self,
        dir: DirEntry,
        current_path: &str,
    ) -> Result<(Entry, usize, usize), FsError> {
        // If already node-backed (no inline entries), just ensure it has a UUID
        let Some(entries) = dir.entries else {
            // Node-backed dir without entries - ensure it has a UUID
            if dir.node_id.is_some() {
                return Ok((Entry::Dir(dir), 0, 0));
            } else {
                // Generate UUID for node-backed dir without node_id
                return Ok((
                    Entry::Dir(DirEntry {
                        entries: None,
                        node_id: Some(uuid::Uuid::new_v4().to_string()),
                        content_type: dir.content_type,
                    }),
                    0,
                    1,
                ));
            }
        };

        // Process each child entry
        let mut new_entries: HashMap<String, Entry> = HashMap::new();
        let mut total_subdirs = 0;
        let mut total_uuids = 0;

        for (name, child) in entries {
            let child_path = if current_path.is_empty() {
                name.clone()
            } else {
                format!("{}/{}", current_path, name)
            };

            match child {
                Entry::Doc(mut doc) => {
                    // Ensure doc has UUID
                    if doc.node_id.is_none() {
                        doc.node_id = Some(uuid::Uuid::new_v4().to_string());
                        total_uuids += 1;
                    }
                    new_entries.insert(name, Entry::Doc(doc));
                }
                Entry::Dir(child_dir) => {
                    if child_dir.entries.is_some() {
                        // This is an inline subdirectory - migrate it!
                        let (migrated_child, child_subdirs, child_uuids) =
                            Box::pin(self.migrate_dir_entry(child_dir, &child_path)).await?;

                        // Generate a UUID for the new subdirectory document
                        let new_doc_id = uuid::Uuid::new_v4().to_string();

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

                        total_subdirs += 1 + child_subdirs;
                        total_uuids += child_uuids;
                    } else {
                        // Already node-backed - ensure it has a UUID
                        if child_dir.node_id.is_some() {
                            new_entries.insert(name, Entry::Dir(child_dir));
                        } else {
                            new_entries.insert(
                                name,
                                Entry::Dir(DirEntry {
                                    entries: None,
                                    node_id: Some(uuid::Uuid::new_v4().to_string()),
                                    content_type: child_dir.content_type,
                                }),
                            );
                            total_uuids += 1;
                        }
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
            total_subdirs,
            total_uuids,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Check if a string looks like a UUID (36 chars with hyphens)
    fn is_uuid_format(s: &str) -> bool {
        s.len() == 36 && s.chars().filter(|c| *c == '-').count() == 4
    }

    #[tokio::test]
    async fn test_migrate_no_subdirs_generates_uuids() {
        let store = Arc::new(DocumentStore::new());
        let reconciler = FilesystemReconciler::new("fs-root".to_string(), store);

        // Schema with no subdirectories but doc without node_id - UUID should be generated
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

        // Should migrate because UUID was generated for file.txt
        assert!(result.migrated);
        assert_eq!(result.subdirs_migrated, 0);
        assert_eq!(result.uuids_generated, 1);

        // Verify the generated node_id is a UUID
        let migrated_schema: FsSchema = serde_json::from_str(&result.schema).unwrap();
        if let Some(Entry::Dir(root)) = migrated_schema.root {
            let entries = root.entries.unwrap();
            if let Some(Entry::Doc(doc)) = entries.get("file.txt") {
                assert!(doc.node_id.is_some());
                assert!(is_uuid_format(doc.node_id.as_ref().unwrap()));
            } else {
                panic!("Expected file.txt doc");
            }
        } else {
            panic!("Expected root directory");
        }
    }

    #[tokio::test]
    async fn test_migrate_no_changes_when_all_have_node_ids() {
        let store = Arc::new(DocumentStore::new());
        let reconciler = FilesystemReconciler::new("fs-root".to_string(), store);

        // Schema where everything already has node_id
        let content = r#"{
            "version": 1,
            "root": {
                "type": "dir",
                "entries": {
                    "file.txt": {"type": "doc", "node_id": "existing-uuid"}
                }
            }
        }"#;

        let result = reconciler
            .migrate_inline_subdirectories(content)
            .await
            .unwrap();

        // No migration needed
        assert!(!result.migrated);
        assert_eq!(result.subdirs_migrated, 0);
        assert_eq!(result.uuids_generated, 0);
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
        // file.txt and todo.txt both get UUIDs
        assert_eq!(result.uuids_generated, 2);

        // Check that the migrated schema has node_id instead of entries
        let migrated_schema: FsSchema = serde_json::from_str(&result.schema).unwrap();
        let notes_node_id: String;
        if let Some(Entry::Dir(root)) = &migrated_schema.root {
            let entries = root.entries.as_ref().unwrap();
            if let Some(Entry::Dir(notes)) = entries.get("notes") {
                assert!(notes.node_id.is_some());
                assert!(notes.entries.is_none());
                // The node_id should be a UUID, not a derived path
                notes_node_id = notes.node_id.clone().unwrap();
                assert!(is_uuid_format(&notes_node_id));
            } else {
                panic!("Expected notes directory");
            }
            // Also check file.txt got a UUID
            if let Some(Entry::Doc(file)) = entries.get("file.txt") {
                assert!(file.node_id.is_some());
                assert!(is_uuid_format(file.node_id.as_ref().unwrap()));
            } else {
                panic!("Expected file.txt doc");
            }
        } else {
            panic!("Expected root directory");
        }

        // Check that the subdirectory document was created with the UUID
        let subdir_doc = store.get_document(&notes_node_id).await;
        assert!(subdir_doc.is_some());

        // Check subdirectory content
        let subdir_content = subdir_doc.unwrap().content;
        let subdir_schema: FsSchema = serde_json::from_str(&subdir_content).unwrap();
        assert_eq!(subdir_schema.version, 1);
        if let Some(Entry::Dir(subdir_root)) = subdir_schema.root {
            let entries = subdir_root.entries.unwrap();
            assert!(entries.contains_key("todo.txt"));
            // todo.txt should also have a UUID
            if let Some(Entry::Doc(todo)) = entries.get("todo.txt") {
                assert!(todo.node_id.is_some());
                assert!(is_uuid_format(todo.node_id.as_ref().unwrap()));
            }
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
        assert_eq!(result.uuids_generated, 1); // deep.txt

        // Get the UUID for level1 from the migrated schema
        let migrated_schema: FsSchema = serde_json::from_str(&result.schema).unwrap();
        let level1_id = if let Some(Entry::Dir(root)) = &migrated_schema.root {
            let entries = root.entries.as_ref().unwrap();
            if let Some(Entry::Dir(level1)) = entries.get("level1") {
                level1.node_id.clone().unwrap()
            } else {
                panic!("Expected level1 directory");
            }
        } else {
            panic!("Expected root directory");
        };

        // Check level1 document was created
        let level1_doc = store.get_document(&level1_id).await;
        assert!(level1_doc.is_some());

        // Get level2 UUID from level1's content
        let level1_content = level1_doc.unwrap().content;
        let level1_schema: FsSchema = serde_json::from_str(&level1_content).unwrap();
        let level2_id = if let Some(Entry::Dir(root)) = &level1_schema.root {
            let entries = root.entries.as_ref().unwrap();
            if let Some(Entry::Dir(level2)) = entries.get("level2") {
                level2.node_id.clone().unwrap()
            } else {
                panic!("Expected level2 directory");
            }
        } else {
            panic!("Expected level1 root directory");
        };

        // Check level2 document was created
        assert!(store.get_document(&level2_id).await.is_some());

        // Verify both IDs are UUIDs
        assert!(is_uuid_format(&level1_id));
        assert!(is_uuid_format(&level2_id));
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
