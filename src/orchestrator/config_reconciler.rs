//! Configuration reconciliation for process management.
//!
//! This module provides pure computation logic for determining what actions
//! are needed when reconciling running processes with new configuration.
//! It does not perform any mutations - it only computes the required changes.

use super::discovery::{DiscoveredProcess, ProcessesConfig};
use std::collections::{HashMap, HashSet};

/// Result of reconciling old config with new config.
///
/// This struct contains all the actions that should be taken to bring
/// the running processes in sync with the new configuration.
#[derive(Debug, Default)]
pub struct ReconcileResult {
    /// Processes to stop (process name)
    pub to_stop: Vec<String>,
    /// Processes to start (process name, config)
    pub to_start: Vec<(String, DiscoveredProcess)>,
    /// Processes to restart (process name, new config)
    pub to_restart: Vec<(String, DiscoveredProcess)>,
    /// Processes that are unchanged
    pub unchanged: Vec<String>,
}

/// Pure computation for process configuration reconciliation.
///
/// This struct provides static methods for determining what changes are needed
/// when process configuration changes. It does not hold any state and performs
/// no side effects.
pub struct ConfigReconciler;

impl ConfigReconciler {
    /// Check if a process configuration has changed in a way that requires restart.
    ///
    /// Returns `true` if the process should be restarted due to configuration changes.
    pub fn process_config_changed(old: &DiscoveredProcess, new: &DiscoveredProcess) -> bool {
        // Check if command changed
        let old_cmd = old
            .command
            .as_ref()
            .map(|c| (c.program().to_string(), c.args().join(" ")));
        let new_cmd = new
            .command
            .as_ref()
            .map(|c| (c.program().to_string(), c.args().join(" ")));
        if old_cmd != new_cmd {
            return true;
        }

        // Check if sandbox-exec changed
        if old.sandbox_exec != new.sandbox_exec {
            return true;
        }

        // Check if evaluate changed
        if old.evaluate != new.evaluate {
            return true;
        }

        // Check if cwd changed
        if old.cwd != new.cwd {
            return true;
        }

        // Check if owns changed
        if old.owns != new.owns {
            return true;
        }

        false
    }

    /// Compute the document path for a process based on its configuration.
    ///
    /// Priority: explicit path > owns > base_path
    /// - sandbox-exec processes sync at the directory containing their __processes.json
    /// - evaluate processes use base_path for script URL, NOT owns (owns is just output)
    pub fn compute_document_path(config: &DiscoveredProcess, base_path: &str) -> String {
        if let Some(ref explicit_path) = config.path {
            if explicit_path == "/" || explicit_path.is_empty() {
                base_path.to_string()
            } else {
                format!("{}/{}", base_path, explicit_path.trim_start_matches('/'))
            }
        } else if config.evaluate.is_some() {
            // evaluate processes: script is relative to base_path, not owns
            base_path.to_string()
        } else if let Some(ref owns) = config.owns {
            format!("{}/{}", base_path, owns)
        } else {
            // sandbox-exec and command processes sync at base_path
            base_path.to_string()
        }
    }

    /// Compute what actions are needed to reconcile running processes with new config.
    ///
    /// This method compares the currently running processes against a new configuration
    /// and determines which processes need to be stopped, started, restarted, or left alone.
    ///
    /// # Arguments
    /// * `running_processes` - Map of process name to its current configuration
    /// * `new_config` - The new ProcessesConfig to reconcile against
    /// * `source_filter` - Optional source path to filter running processes.
    ///   If provided, only processes from this source are considered.
    ///
    /// # Returns
    /// A `ReconcileResult` containing the lists of processes to stop, start, restart,
    /// and those that are unchanged.
    pub fn reconcile(
        running_processes: &HashMap<String, DiscoveredProcess>,
        new_config: &ProcessesConfig,
        source_filter: Option<&str>,
    ) -> ReconcileResult {
        let mut result = ReconcileResult::default();

        // Get names of currently running processes (optionally filtered by source)
        let current_names: HashSet<String> = if let Some(_source) = source_filter {
            // When we have source filtering, caller should have pre-filtered
            running_processes.keys().cloned().collect()
        } else {
            running_processes.keys().cloned().collect()
        };

        let new_names: HashSet<String> = new_config.processes.keys().cloned().collect();

        // Find processes to remove (in current but not in new)
        result.to_stop = current_names.difference(&new_names).cloned().collect();

        // Find processes to add (in new but not in current)
        for name in new_names.difference(&current_names) {
            let config = new_config.processes.get(name).unwrap().clone();
            result.to_start.push((name.clone(), config));
        }

        // Find processes that exist in both and may need restart
        for name in current_names.intersection(&new_names) {
            let old_config = running_processes.get(name).unwrap();
            let new_config_entry = new_config.processes.get(name).unwrap();

            if Self::process_config_changed(old_config, new_config_entry) {
                result
                    .to_restart
                    .push((name.clone(), new_config_entry.clone()));
            } else {
                result.unchanged.push(name.clone());
            }
        }

        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::orchestrator::discovery::CommandSpec;
    use std::path::PathBuf;

    fn make_process_with_command(cmd: &str) -> DiscoveredProcess {
        DiscoveredProcess {
            comment: None,
            command: Some(CommandSpec::Simple(cmd.to_string())),
            sandbox_exec: None,
            path: None,
            owns: None,
            cwd: None,
            evaluate: None,
            log_listener: None,
        }
    }

    fn make_sandbox_process(exec: &str) -> DiscoveredProcess {
        DiscoveredProcess {
            comment: None,
            command: None,
            sandbox_exec: Some(exec.to_string()),
            path: None,
            owns: None,
            cwd: None,
            evaluate: None,
            log_listener: None,
        }
    }

    fn make_evaluate_process(script: &str, owns: Option<&str>) -> DiscoveredProcess {
        DiscoveredProcess {
            comment: None,
            command: None,
            sandbox_exec: None,
            path: None,
            owns: owns.map(String::from),
            cwd: None,
            evaluate: Some(script.to_string()),
            log_listener: None,
        }
    }

    #[test]
    fn test_process_config_changed_command() {
        let old = make_process_with_command("python test.py");
        let new = make_process_with_command("python test.py --verbose");

        assert!(ConfigReconciler::process_config_changed(&old, &new));
    }

    #[test]
    fn test_process_config_unchanged_command() {
        let old = make_process_with_command("python test.py");
        let new = make_process_with_command("python test.py");

        assert!(!ConfigReconciler::process_config_changed(&old, &new));
    }

    #[test]
    fn test_process_config_changed_sandbox_exec() {
        let old = make_sandbox_process("node server.js");
        let new = make_sandbox_process("node server.js --port 8080");

        assert!(ConfigReconciler::process_config_changed(&old, &new));
    }

    #[test]
    fn test_process_config_changed_evaluate() {
        let old = make_evaluate_process("script.ts", None);
        let new = make_evaluate_process("new_script.ts", None);

        assert!(ConfigReconciler::process_config_changed(&old, &new));
    }

    #[test]
    fn test_process_config_changed_cwd() {
        let mut old = make_sandbox_process("node server.js");
        old.cwd = Some(PathBuf::from("/app"));

        let mut new = make_sandbox_process("node server.js");
        new.cwd = Some(PathBuf::from("/app/v2"));

        assert!(ConfigReconciler::process_config_changed(&old, &new));
    }

    #[test]
    fn test_process_config_changed_owns() {
        let mut old = make_sandbox_process("node server.js");
        old.owns = Some("output.txt".to_string());

        let mut new = make_sandbox_process("node server.js");
        new.owns = Some("result.txt".to_string());

        assert!(ConfigReconciler::process_config_changed(&old, &new));
    }

    #[test]
    fn test_compute_document_path_explicit() {
        let mut config = make_sandbox_process("node server.js");
        config.path = Some("/custom/path".to_string());

        let path = ConfigReconciler::compute_document_path(&config, "workspace");
        assert_eq!(path, "workspace/custom/path");
    }

    #[test]
    fn test_compute_document_path_explicit_root() {
        let mut config = make_sandbox_process("node server.js");
        config.path = Some("/".to_string());

        let path = ConfigReconciler::compute_document_path(&config, "workspace");
        assert_eq!(path, "workspace");
    }

    #[test]
    fn test_compute_document_path_evaluate() {
        // evaluate processes use base_path, not owns
        let config = make_evaluate_process("script.ts", Some("output.txt"));

        let path = ConfigReconciler::compute_document_path(&config, "workspace/app");
        assert_eq!(path, "workspace/app");
    }

    #[test]
    fn test_compute_document_path_owns() {
        let mut config = make_sandbox_process("node server.js");
        config.owns = Some("output.txt".to_string());

        let path = ConfigReconciler::compute_document_path(&config, "workspace");
        assert_eq!(path, "workspace/output.txt");
    }

    #[test]
    fn test_compute_document_path_base_only() {
        let config = make_sandbox_process("node server.js");

        let path = ConfigReconciler::compute_document_path(&config, "workspace");
        assert_eq!(path, "workspace");
    }

    #[test]
    fn test_reconcile_no_changes() {
        let mut running = HashMap::new();
        running.insert("proc1".to_string(), make_sandbox_process("node app.js"));

        let mut new_config = ProcessesConfig {
            processes: HashMap::new(),
        };
        new_config
            .processes
            .insert("proc1".to_string(), make_sandbox_process("node app.js"));

        let result = ConfigReconciler::reconcile(&running, &new_config, None);

        assert!(result.to_stop.is_empty());
        assert!(result.to_start.is_empty());
        assert!(result.to_restart.is_empty());
        assert_eq!(result.unchanged, vec!["proc1"]);
    }

    #[test]
    fn test_reconcile_add_process() {
        let running: HashMap<String, DiscoveredProcess> = HashMap::new();

        let mut new_config = ProcessesConfig {
            processes: HashMap::new(),
        };
        new_config
            .processes
            .insert("new_proc".to_string(), make_sandbox_process("node app.js"));

        let result = ConfigReconciler::reconcile(&running, &new_config, None);

        assert!(result.to_stop.is_empty());
        assert_eq!(result.to_start.len(), 1);
        assert_eq!(result.to_start[0].0, "new_proc");
        assert!(result.to_restart.is_empty());
        assert!(result.unchanged.is_empty());
    }

    #[test]
    fn test_reconcile_remove_process() {
        let mut running = HashMap::new();
        running.insert("old_proc".to_string(), make_sandbox_process("node app.js"));

        let new_config = ProcessesConfig {
            processes: HashMap::new(),
        };

        let result = ConfigReconciler::reconcile(&running, &new_config, None);

        assert_eq!(result.to_stop, vec!["old_proc"]);
        assert!(result.to_start.is_empty());
        assert!(result.to_restart.is_empty());
        assert!(result.unchanged.is_empty());
    }

    #[test]
    fn test_reconcile_restart_process() {
        let mut running = HashMap::new();
        running.insert("proc1".to_string(), make_sandbox_process("node app.js"));

        let mut new_config = ProcessesConfig {
            processes: HashMap::new(),
        };
        new_config.processes.insert(
            "proc1".to_string(),
            make_sandbox_process("node app.js --port 8080"),
        );

        let result = ConfigReconciler::reconcile(&running, &new_config, None);

        assert!(result.to_stop.is_empty());
        assert!(result.to_start.is_empty());
        assert_eq!(result.to_restart.len(), 1);
        assert_eq!(result.to_restart[0].0, "proc1");
        assert!(result.unchanged.is_empty());
    }

    #[test]
    fn test_reconcile_mixed_changes() {
        let mut running = HashMap::new();
        running.insert(
            "unchanged".to_string(),
            make_sandbox_process("node unchanged.js"),
        );
        running.insert(
            "to_restart".to_string(),
            make_sandbox_process("node old.js"),
        );
        running.insert(
            "to_remove".to_string(),
            make_sandbox_process("node remove.js"),
        );

        let mut new_config = ProcessesConfig {
            processes: HashMap::new(),
        };
        new_config.processes.insert(
            "unchanged".to_string(),
            make_sandbox_process("node unchanged.js"),
        );
        new_config.processes.insert(
            "to_restart".to_string(),
            make_sandbox_process("node new.js"),
        );
        new_config
            .processes
            .insert("new_proc".to_string(), make_sandbox_process("node add.js"));

        let result = ConfigReconciler::reconcile(&running, &new_config, None);

        assert_eq!(result.to_stop, vec!["to_remove"]);
        assert_eq!(result.to_start.len(), 1);
        assert_eq!(result.to_start[0].0, "new_proc");
        assert_eq!(result.to_restart.len(), 1);
        assert_eq!(result.to_restart[0].0, "to_restart");
        assert_eq!(result.unchanged, vec!["unchanged"]);
    }

    // =========================================================================
    // Additional edge case tests
    // =========================================================================

    #[test]
    fn test_reconcile_both_empty() {
        let running: HashMap<String, DiscoveredProcess> = HashMap::new();
        let new_config = ProcessesConfig {
            processes: HashMap::new(),
        };

        let result = ConfigReconciler::reconcile(&running, &new_config, None);

        assert!(result.to_stop.is_empty());
        assert!(result.to_start.is_empty());
        assert!(result.to_restart.is_empty());
        assert!(result.unchanged.is_empty());
    }

    #[test]
    fn test_reconcile_old_empty_new_has_processes() {
        let running: HashMap<String, DiscoveredProcess> = HashMap::new();
        let mut new_config = ProcessesConfig {
            processes: HashMap::new(),
        };
        new_config
            .processes
            .insert("proc1".to_string(), make_sandbox_process("node app1.js"));
        new_config
            .processes
            .insert("proc2".to_string(), make_sandbox_process("node app2.js"));

        let result = ConfigReconciler::reconcile(&running, &new_config, None);

        assert!(result.to_stop.is_empty());
        assert_eq!(result.to_start.len(), 2);
        assert!(result.to_restart.is_empty());
        assert!(result.unchanged.is_empty());
    }

    #[test]
    fn test_reconcile_new_empty_old_has_processes() {
        let mut running = HashMap::new();
        running.insert("proc1".to_string(), make_sandbox_process("node app1.js"));
        running.insert("proc2".to_string(), make_sandbox_process("node app2.js"));

        let new_config = ProcessesConfig {
            processes: HashMap::new(),
        };

        let result = ConfigReconciler::reconcile(&running, &new_config, None);

        assert_eq!(result.to_stop.len(), 2);
        assert!(result.to_start.is_empty());
        assert!(result.to_restart.is_empty());
        assert!(result.unchanged.is_empty());
    }

    #[test]
    fn test_process_config_changed_all_fields_unchanged() {
        // Test that when ALL fields are identical, we get false
        let mut old = make_sandbox_process("node server.js");
        old.cwd = Some(PathBuf::from("/app"));
        old.owns = Some("output.txt".to_string());
        old.path = Some("/custom".to_string());

        let mut new = make_sandbox_process("node server.js");
        new.cwd = Some(PathBuf::from("/app"));
        new.owns = Some("output.txt".to_string());
        new.path = Some("/custom".to_string());

        assert!(!ConfigReconciler::process_config_changed(&old, &new));
    }

    #[test]
    fn test_process_config_changed_path_field() {
        // path field isn't checked in process_config_changed
        // (since path is used for document routing, not process execution)
        let mut old = make_sandbox_process("node server.js");
        old.path = Some("/old/path".to_string());

        let mut new = make_sandbox_process("node server.js");
        new.path = Some("/new/path".to_string());

        // path changes don't trigger restart in process_config_changed
        // (this is intentional - path affects routing, not the process itself)
        assert!(!ConfigReconciler::process_config_changed(&old, &new));
    }

    #[test]
    fn test_process_config_changed_none_to_some() {
        // Test transition from None to Some for optional fields
        let old = make_sandbox_process("node server.js");

        let mut new = make_sandbox_process("node server.js");
        new.owns = Some("output.txt".to_string());

        assert!(ConfigReconciler::process_config_changed(&old, &new));
    }

    #[test]
    fn test_process_config_changed_some_to_none() {
        // Test transition from Some to None for optional fields
        let mut old = make_sandbox_process("node server.js");
        old.cwd = Some(PathBuf::from("/app"));

        let new = make_sandbox_process("node server.js");

        assert!(ConfigReconciler::process_config_changed(&old, &new));
    }

    #[test]
    fn test_process_config_changed_command_to_sandbox_exec() {
        // Test switching from command mode to sandbox-exec mode
        let old = make_process_with_command("node server.js");
        let new = make_sandbox_process("node server.js");

        assert!(ConfigReconciler::process_config_changed(&old, &new));
    }

    #[test]
    fn test_process_config_changed_sandbox_exec_to_evaluate() {
        // Test switching from sandbox-exec to evaluate
        let old = make_sandbox_process("node server.js");
        let new = make_evaluate_process("script.ts", Some("output.txt"));

        assert!(ConfigReconciler::process_config_changed(&old, &new));
    }

    #[test]
    fn test_compute_document_path_empty_explicit_path() {
        let mut config = make_sandbox_process("node server.js");
        config.path = Some("".to_string());

        let path = ConfigReconciler::compute_document_path(&config, "workspace");
        assert_eq!(path, "workspace");
    }

    #[test]
    fn test_compute_document_path_nested_base_path() {
        let config = make_sandbox_process("node server.js");

        let path = ConfigReconciler::compute_document_path(&config, "workspace/subdir/app");
        assert_eq!(path, "workspace/subdir/app");
    }

    #[test]
    fn test_compute_document_path_explicit_overrides_owns() {
        // When both explicit path and owns are set, explicit path takes priority
        let mut config = make_sandbox_process("node server.js");
        config.path = Some("/specific".to_string());
        config.owns = Some("output.txt".to_string());

        let path = ConfigReconciler::compute_document_path(&config, "workspace");
        assert_eq!(path, "workspace/specific");
    }
}
