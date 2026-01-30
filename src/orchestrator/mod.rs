mod config;
mod config_reconciler;
mod discovered_manager;
mod discovery;
mod manager;
mod mqtt_watcher;
mod process_discovery;
mod process_utils;
mod schema_visitor;
mod script_resolver;
mod spawn;
mod status;

pub use config::{OrchestratorConfig, ProcessConfig, RestartMode, RestartPolicy};
pub use config_reconciler::{ConfigReconciler, ReconcileResult};
pub use discovered_manager::{
    DiscoveredProcessManager, DiscoveredProcessState, ManagedDiscoveredProcess,
};
pub use discovery::{CommandSpec, DiscoveredProcess, ProcessesConfig};
pub use manager::{ManagedProcess, ProcessManager, ProcessState};
pub use mqtt_watcher::{MqttDocumentWatcher, WatchEvent};
pub use process_discovery::{DiscoveryError, DiscoveryResult, ProcessDiscoveryService};
pub use schema_visitor::{visit_entries, SchemaVisitor, VisitorError};
pub use script_resolver::{ResolverError, ScriptResolver, ScriptWatchMap};
pub use spawn::{spawn_managed_process, spawn_managed_process_with_logging, SpawnResult};
pub use status::{get_process_cwd, OrchestratorStatus, ProcessStatus, LEGACY_STATUS_FILE_PATH};
