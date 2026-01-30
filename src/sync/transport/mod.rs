//! Transport layer for sync operations.
//!
//! This module contains transport-related functionality for the sync client:
//! - HTTP client operations (client.rs)
//! - URL building utilities (urls.rs)
//! - SSE (Server-Sent Events) handling (sse.rs)
//! - Commit ancestry checking (ancestry.rs)
//! - MQTT subscription tasks (subscriptions.rs)
//! - MQTT command handling (commands.rs)
//! - Missing parent alerts (missing_parent.rs)
//! - Peer fallback for sync requests (peer_fallback.rs)

pub mod ancestry;
pub mod client;
pub mod commands;
pub mod missing_parent;
pub mod peer_fallback;
pub mod shadow;
pub mod sse;
pub mod subscriptions;
pub mod urls;

// Re-export public items from submodules

// From ancestry
pub use ancestry::{all_are_ancestors, determine_sync_direction, is_ancestor, SyncDirection};

// From client
pub use client::{
    delete_schema_entry, discover_fs_root, fetch_head, fork_node, push_content_by_type,
    push_file_content, push_json_content, push_jsonl_content, push_schema_to_server,
    refresh_from_head, resolve_path_to_uuid_http, DiscoverFsRootError, FetchHeadError,
};

// From commands
pub use commands::{spawn_command_listener, CommandEntry};

// From missing_parent
pub use missing_parent::{
    check_and_alert_missing_parents, handle_missing_parent_alert, new_rate_limiter,
    publish_missing_parent_alert, subscribe_to_missing_parent_alerts, RebroadcastRateLimiter,
    SharedRateLimiter, REBROADCAST_MAX_JITTER_MS, REBROADCAST_MIN_INTERVAL,
};

// From peer_fallback
pub use peer_fallback::{
    handle_peer_sync_request, new_peer_fallback_handler, subscribe_for_peer_fallback,
    PeerFallbackHandler, PeerFallbackStats, PendingRequest, SharedPeerFallbackHandler,
};

// From shadow
#[cfg(unix)]
pub use shadow::{
    atomic_write_with_shadow, handle_shadow_write, shadow_write_handler_task,
    write_inbound_with_checks, write_inbound_with_checks_atomic, InboundWriteError,
    InboundWriteResult,
};

// From sse
pub use sse::{handle_server_edit, handle_server_edit_with_flock, sse_task, sse_task_with_flock};
#[cfg(unix)]
pub use sse::{handle_server_edit_with_tracker, sse_task_with_tracker};

// From subscriptions
pub use subscriptions::{
    directory_mqtt_task, spawn_subdir_mqtt_task, subdir_mqtt_task, trace_timeline,
    CrdtFileSyncContext, TimelineMilestone,
};

// From urls
pub use urls::{
    build_commits_url, build_edit_url, build_fork_url, build_head_at_commit_url, build_head_url,
    build_health_url, build_info_url, build_is_ancestor_url, build_replace_url, build_sse_url,
    encode_node_id, encode_path, normalize_path,
};
