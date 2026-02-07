//! CRDT-related modules for peer sync.
//!
//! Re-exports from the `commonplace-crdt` crate. Migration functions that
//! depend on `SyncStateFile` (main crate only) are defined here.

// Re-export everything from the standalone crate
pub use commonplace_crdt::ack_handler;
pub use commonplace_crdt::crdt_merge;
pub use commonplace_crdt::crdt_new_file;
pub use commonplace_crdt::crdt_publish;
pub use commonplace_crdt::crdt_state;
pub use commonplace_crdt::sync_state_machine;
pub use commonplace_crdt::yjs;
pub use commonplace_crdt::ymap_schema;

// Re-export all public items
pub use commonplace_crdt::*;

// ---------------------------------------------------------------------------
// Migration functions (kept here because they depend on SyncStateFile)
// ---------------------------------------------------------------------------

use crate::sync::state_file::SyncStateFile;
use commonplace_types::fs::{Entry, FsSchema};
use commonplace_types::sync::error::SyncResult;
use commonplace_types::SCHEMA_FILENAME;
use std::collections::HashMap;
use std::io;
use std::path::Path;
use tokio::fs;
use tracing::{debug, warn};
use uuid::Uuid;

/// Migrate from old SyncStateFile format to new DirectorySyncState.
///
/// The old format stored:
/// - server, node_id, last_synced_cid, files (with hash, last_cid, inode_key)
///
/// The new format stores:
/// - schema state with Y.Doc
/// - file states with Y.Docs
///
/// Migration creates empty Y.Docs but preserves CID information.
/// File UUIDs are looked up from the `.commonplace.json` schema file.
pub async fn migrate_from_old_state(
    old_state: &SyncStateFile,
    directory: &Path,
) -> SyncResult<DirectorySyncState> {
    // Parse the node_id as UUID
    let schema_node_id = Uuid::parse_str(&old_state.node_id)?;

    let mut new_state = DirectorySyncState::new(schema_node_id);

    // Preserve the last synced CID as both head_cid and local_head_cid
    // (we assume they're in sync at migration time)
    new_state.schema.head_cid = old_state.last_synced_cid.clone();
    new_state.schema.local_head_cid = old_state.last_synced_cid.clone();

    // Load schema to look up file UUIDs
    let schema_path = directory.join(SCHEMA_FILENAME);
    let schema_entries: Option<HashMap<String, Entry>> = if schema_path.exists() {
        match fs::read_to_string(&schema_path).await {
            Ok(content) => match serde_json::from_str::<FsSchema>(&content) {
                Ok(fs_schema) => match &fs_schema.root {
                    Some(Entry::Dir(dir)) => dir.entries.clone(),
                    _ => None,
                },
                Err(e) => {
                    warn!(
                        "Failed to parse {} during migration: {}",
                        schema_path.display(),
                        e
                    );
                    None
                }
            },
            Err(e) => {
                warn!(
                    "Failed to read {} during migration: {}",
                    schema_path.display(),
                    e
                );
                None
            }
        }
    } else {
        debug!(
            "No schema file at {} during migration",
            schema_path.display()
        );
        None
    };

    // Migrate file states
    for (path, file_state) in &old_state.files {
        // Look up the UUID from the schema (authoritative source)
        let file_node_id = if let Some(ref entries) = schema_entries {
            if let Some(Entry::Doc(doc)) = entries.get(path) {
                if let Some(ref node_id_str) = doc.node_id {
                    match Uuid::parse_str(node_id_str) {
                        Ok(uuid) => {
                            debug!(
                                "Using schema UUID {} for file '{}' during migration",
                                uuid, path
                            );
                            uuid
                        }
                        Err(e) => {
                            warn!(
                                "Invalid UUID '{}' for '{}' in schema: {}, generating new one",
                                node_id_str, path, e
                            );
                            Uuid::new_v4()
                        }
                    }
                } else {
                    warn!(
                        "File '{}' in schema has no node_id, generating new UUID",
                        path
                    );
                    Uuid::new_v4()
                }
            } else {
                warn!(
                    "File '{}' not found in schema during migration, generating new UUID",
                    path
                );
                Uuid::new_v4()
            }
        } else {
            warn!(
                "No schema available during migration for '{}', generating new UUID",
                path
            );
            Uuid::new_v4()
        };

        let mut peer_state = CrdtPeerState::new(file_node_id);
        peer_state.head_cid = file_state.last_cid.clone();
        peer_state.local_head_cid = file_state.last_cid.clone();
        // Y.Doc state will be populated on first sync

        new_state.files.insert(path.clone(), peer_state);
    }

    debug!(
        "Migrated old state with {} files to new CRDT state",
        new_state.files.len()
    );

    Ok(new_state)
}

/// Attempt to load state, migrating from old format if necessary.
pub async fn load_or_migrate(
    directory: &Path,
    schema_node_id: Uuid,
) -> io::Result<DirectorySyncState> {
    // First try loading new format
    if let Some(state) = DirectorySyncState::load(directory).await? {
        return Ok(state);
    }

    // Check for old format state file (sibling dotfile)
    let old_state_path = SyncStateFile::state_file_path(directory);
    if old_state_path.exists() {
        if let Some(old_state) = SyncStateFile::load(&old_state_path).await? {
            match migrate_from_old_state(&old_state, directory).await {
                Ok(new_state) => {
                    // Save the migrated state
                    new_state.save(directory).await?;
                    warn!(
                        "Migrated old sync state to new CRDT format: {}",
                        directory.display()
                    );
                    return Ok(new_state);
                }
                Err(e) => {
                    warn!("Failed to migrate old state: {}", e);
                    // Fall through to create new state
                }
            }
        }
    }

    // No existing state, create new
    Ok(DirectorySyncState::new(schema_node_id))
}
