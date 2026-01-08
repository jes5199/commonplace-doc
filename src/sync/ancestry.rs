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
