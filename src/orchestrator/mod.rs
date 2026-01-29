mod config;
mod discovered_manager;
mod discovery;
mod manager;
mod process_utils;
mod spawn;
mod status;

pub use config::{OrchestratorConfig, ProcessConfig, RestartMode, RestartPolicy};
pub use discovered_manager::{
    DiscoveredProcessManager, DiscoveredProcessState, ManagedDiscoveredProcess,
};
pub use discovery::{CommandSpec, DiscoveredProcess, ProcessesConfig};
pub use manager::{ManagedProcess, ProcessManager, ProcessState};
pub use spawn::{spawn_managed_process, spawn_managed_process_with_logging, SpawnResult};
pub use status::{get_process_cwd, OrchestratorStatus, ProcessStatus, LEGACY_STATUS_FILE_PATH};
