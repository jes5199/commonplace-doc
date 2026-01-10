//! Client helper for checking commit ancestry.

use reqwest::Client;
use serde::Deserialize;
use std::collections::HashSet;
use uuid::Uuid;

use super::urls::build_is_ancestor_url;

#[derive(Debug, Deserialize)]
struct AncestorResponse {
    is_ancestor: bool,
}

/// Result of comparing two commit CIDs for sync direction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncDirection {
    /// Local is ahead of server (server CID is ancestor of local CID) - push local
    Push,
    /// Server is ahead of local (local CID is ancestor of server CID) - pull server
    Pull,
    /// Both have same CID - already in sync
    InSync,
    /// Neither is ancestor of the other - diverged, needs merge
    Diverged,
}

/// Check if `ancestor` is an ancestor of `descendant` in the commit DAG.
///
/// Returns Ok(true) if ancestor is in the history of descendant.
pub async fn is_ancestor(
    client: &Client,
    server_url: &str,
    doc_id: &str,
    ancestor: &str,
    descendant: &str,
) -> Result<bool, reqwest::Error> {
    // Parse doc_id as UUID, or use a dummy if it's a path-based ID
    let uuid = Uuid::parse_str(doc_id).unwrap_or_else(|_| Uuid::nil());
    let url = build_is_ancestor_url(server_url, &uuid, ancestor, descendant);
    let response: AncestorResponse = client.get(&url).send().await?.json().await?;
    Ok(response.is_ancestor)
}

/// Determine sync direction by comparing local and server CIDs.
///
/// - If local_cid is None: server is ahead (or both new), pull from server
/// - If server_cid is None: local is ahead, push to server
/// - If same: in sync
/// - Otherwise: check ancestry to determine direction
pub async fn determine_sync_direction(
    client: &Client,
    server_url: &str,
    doc_id: &str,
    local_cid: Option<&str>,
    server_cid: Option<&str>,
) -> Result<SyncDirection, reqwest::Error> {
    match (local_cid, server_cid) {
        // Both have no commits - new document, push local content
        (None, None) => Ok(SyncDirection::Push),
        // Local has no CID but server does - server is ahead
        (None, Some(_)) => Ok(SyncDirection::Pull),
        // Local has CID but server doesn't - local is ahead (shouldn't happen normally)
        (Some(_), None) => Ok(SyncDirection::Push),
        // Both have CIDs - check if they're the same or need ancestry check
        (Some(local), Some(server)) => {
            if local == server {
                return Ok(SyncDirection::InSync);
            }
            // Check if server is ancestor of local (local is ahead)
            if is_ancestor(client, server_url, doc_id, server, local).await? {
                return Ok(SyncDirection::Push);
            }
            // Check if local is ancestor of server (server is ahead)
            if is_ancestor(client, server_url, doc_id, local, server).await? {
                return Ok(SyncDirection::Pull);
            }
            // Neither is ancestor - diverged
            Ok(SyncDirection::Diverged)
        }
    }
}

/// Check if all commits in `pending` are ancestors of `descendant`.
///
/// Returns true only if every pending commit is an ancestor.
/// Used to verify local edits are included in server's merged state.
pub async fn all_are_ancestors(
    client: &Client,
    server_url: &str,
    doc_id: &Uuid,
    pending: &HashSet<String>,
    descendant: &str,
) -> Result<bool, reqwest::Error> {
    for ancestor in pending {
        let url = build_is_ancestor_url(server_url, doc_id, ancestor, descendant);
        let response: AncestorResponse = client.get(&url).send().await?.json().await?;

        if !response.is_ancestor {
            tracing::debug!(
                ancestor,
                descendant,
                "pending commit is not ancestor of incoming"
            );
            return Ok(false);
        }
    }

    Ok(true)
}
