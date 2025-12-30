mod config;
mod discovery;
mod manager;

pub use config::{OrchestratorConfig, ProcessConfig, RestartMode, RestartPolicy};
pub use discovery::{
    CommandSpec, DiscoveredProcess, DiscoveredProcessManager, DiscoveredProcessState,
    ManagedDiscoveredProcess, ProcessesConfig,
};
pub use manager::{ManagedProcess, ProcessManager, ProcessState};
