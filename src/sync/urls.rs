//! URL building utilities for the sync client.
//!
//! This module provides functions to encode node IDs and paths for use in URLs,
//! and to build URLs for various API endpoints.

/// URL-encode a node ID for use in URL paths.
/// Node IDs are typically UUIDs which don't need encoding, but this function
/// handles any special characters that may appear in custom IDs.
pub fn encode_node_id(node_id: &str) -> String {
    urlencoding::encode(node_id).into_owned()
}

/// URL-encode a path for use in `/files/*path` endpoints.
/// Each path segment is encoded individually to preserve the `/` separators
/// while encoding special characters within segments.
pub fn encode_path(path: &str) -> String {
    path.split('/')
        .map(|segment| urlencoding::encode(segment).into_owned())
        .collect::<Vec<_>>()
        .join("/")
}

/// Normalize a path to use forward slashes regardless of OS.
///
/// Schema paths always use forward slashes, but on Windows `to_string_lossy()`
/// produces backslashes. This normalizes to forward slashes so node IDs match
/// between the client and server.
pub fn normalize_path(path: &str) -> String {
    path.replace('\\', "/")
}

/// Build URL for getting document HEAD
pub fn build_head_url(server: &str, path_or_id: &str, use_paths: bool) -> String {
    if use_paths {
        format!("{}/files/{}/head", server, encode_path(path_or_id))
    } else {
        format!("{}/docs/{}/head", server, encode_node_id(path_or_id))
    }
}

/// Build URL for document edit
pub fn build_edit_url(server: &str, path_or_id: &str, use_paths: bool) -> String {
    if use_paths {
        format!("{}/files/{}/edit", server, encode_path(path_or_id))
    } else {
        format!("{}/docs/{}/edit", server, encode_node_id(path_or_id))
    }
}

/// Build URL for document replace
pub fn build_replace_url(
    server: &str,
    path_or_id: &str,
    parent_cid: &str,
    use_paths: bool,
) -> String {
    if use_paths {
        format!(
            "{}/files/{}/replace?parent_cid={}&author=sync-client",
            server,
            encode_path(path_or_id),
            parent_cid
        )
    } else {
        format!(
            "{}/docs/{}/replace?parent_cid={}&author=sync-client",
            server,
            encode_node_id(path_or_id),
            parent_cid
        )
    }
}

/// Build URL for SSE subscription
pub fn build_sse_url(server: &str, path_or_id: &str, use_paths: bool) -> String {
    if use_paths {
        format!("{}/sse/files/{}", server, encode_path(path_or_id))
    } else {
        format!("{}/sse/docs/{}", server, encode_node_id(path_or_id))
    }
}

/// Build URL for forking a document
pub fn build_fork_url(server: &str, source_node: &str, at_commit: Option<&str>) -> String {
    let mut url = format!("{}/docs/{}/fork", server, encode_node_id(source_node));
    if let Some(commit) = at_commit {
        url = format!("{}?at_commit={}", url, commit);
    }
    url
}

/// Build URL for ancestry check endpoint
pub fn build_is_ancestor_url(
    base: &str,
    doc_id: &uuid::Uuid,
    ancestor: &str,
    descendant: &str,
) -> String {
    format!(
        "{}/docs/{}/is-ancestor?ancestor={}&descendant={}",
        base.trim_end_matches('/'),
        doc_id,
        urlencoding::encode(ancestor),
        urlencoding::encode(descendant)
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    mod url_encoding {
        use super::*;

        #[test]
        fn test_encode_node_id_simple() {
            assert_eq!(encode_node_id("test-node"), "test-node");
        }

        #[test]
        fn test_encode_node_id_with_slashes() {
            assert_eq!(
                encode_node_id("fs-root:notes/todo.txt"),
                "fs-root%3Anotes%2Ftodo.txt"
            );
        }

        #[test]
        fn test_encode_node_id_with_spaces() {
            assert_eq!(encode_node_id("my node"), "my%20node");
        }

        #[test]
        fn test_encode_node_id_empty() {
            assert_eq!(encode_node_id(""), "");
        }

        #[test]
        fn test_encode_path_simple() {
            assert_eq!(encode_path("notes/todo.txt"), "notes/todo.txt");
        }

        #[test]
        fn test_encode_path_with_spaces() {
            assert_eq!(
                encode_path("my notes/my file.txt"),
                "my%20notes/my%20file.txt"
            );
        }

        #[test]
        fn test_encode_path_nested() {
            assert_eq!(encode_path("a/b/c/d.txt"), "a/b/c/d.txt");
        }

        #[test]
        fn test_encode_path_special_chars() {
            assert_eq!(encode_path("notes/file#1.txt"), "notes/file%231.txt");
        }

        #[test]
        fn test_normalize_path_forward_slashes() {
            assert_eq!(normalize_path("notes/todo.txt"), "notes/todo.txt");
        }

        #[test]
        fn test_normalize_path_backslashes() {
            assert_eq!(normalize_path("notes\\todo.txt"), "notes/todo.txt");
        }

        #[test]
        fn test_normalize_path_mixed() {
            assert_eq!(normalize_path("notes\\sub/file.txt"), "notes/sub/file.txt");
        }
    }

    mod url_builders {
        use super::*;

        const SERVER: &str = "http://localhost:5199";

        #[test]
        fn test_build_head_url_with_paths() {
            let url = build_head_url(SERVER, "notes/todo.txt", true);
            assert_eq!(url, "http://localhost:5199/files/notes/todo.txt/head");
        }

        #[test]
        fn test_build_head_url_with_id() {
            let url = build_head_url(SERVER, "abc-123", false);
            assert_eq!(url, "http://localhost:5199/docs/abc-123/head");
        }

        #[test]
        fn test_build_head_url_id_with_special_chars() {
            let url = build_head_url(SERVER, "fs-root:notes/todo.txt", false);
            assert_eq!(
                url,
                "http://localhost:5199/docs/fs-root%3Anotes%2Ftodo.txt/head"
            );
        }

        #[test]
        fn test_build_edit_url_with_paths() {
            let url = build_edit_url(SERVER, "notes/todo.txt", true);
            assert_eq!(url, "http://localhost:5199/files/notes/todo.txt/edit");
        }

        #[test]
        fn test_build_edit_url_with_id() {
            let url = build_edit_url(SERVER, "abc-123", false);
            assert_eq!(url, "http://localhost:5199/docs/abc-123/edit");
        }

        #[test]
        fn test_build_replace_url_with_paths() {
            let url = build_replace_url(SERVER, "notes/todo.txt", "cid123", true);
            assert_eq!(
                url,
                "http://localhost:5199/files/notes/todo.txt/replace?parent_cid=cid123&author=sync-client"
            );
        }

        #[test]
        fn test_build_replace_url_with_id() {
            let url = build_replace_url(SERVER, "abc-123", "cid456", false);
            assert_eq!(
                url,
                "http://localhost:5199/docs/abc-123/replace?parent_cid=cid456&author=sync-client"
            );
        }

        #[test]
        fn test_build_sse_url_with_paths() {
            let url = build_sse_url(SERVER, "notes/todo.txt", true);
            assert_eq!(url, "http://localhost:5199/sse/files/notes/todo.txt");
        }

        #[test]
        fn test_build_sse_url_with_id() {
            let url = build_sse_url(SERVER, "abc-123", false);
            assert_eq!(url, "http://localhost:5199/sse/docs/abc-123");
        }

        #[test]
        fn test_build_urls_with_spaces_in_path() {
            let url = build_head_url(SERVER, "my notes/my file.txt", true);
            assert_eq!(
                url,
                "http://localhost:5199/files/my%20notes/my%20file.txt/head"
            );
        }

        #[test]
        fn test_build_fork_url_simple() {
            let url = build_fork_url(SERVER, "my-node", None);
            assert_eq!(url, "http://localhost:5199/docs/my-node/fork");
        }

        #[test]
        fn test_build_fork_url_with_commit() {
            let url = build_fork_url(SERVER, "my-node", Some("abc123"));
            assert_eq!(
                url,
                "http://localhost:5199/docs/my-node/fork?at_commit=abc123"
            );
        }

        #[test]
        fn test_build_is_ancestor_url() {
            let url = build_is_ancestor_url(
                "http://localhost:5199",
                &uuid::Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap(),
                "abc123",
                "def456",
            );
            assert_eq!(
                url,
                "http://localhost:5199/docs/550e8400-e29b-41d4-a716-446655440000/is-ancestor?ancestor=abc123&descendant=def456"
            );
        }
    }
}
