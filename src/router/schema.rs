//! Router document schema.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Port type for wiring.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum PortType {
    /// Blue port (edits)
    Blue,
    /// Red port (events)
    Red,
    /// Both ports
    #[default]
    Both,
}

/// Node specification in the router document.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeSpec {
    /// Node type (only "document" is supported)
    #[serde(rename = "type")]
    pub node_type: String,
    /// Content type (MIME type)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content_type: Option<String>,
}

/// Edge specification in the router document.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Edge {
    /// Source node ID
    pub from: String,
    /// Target node ID
    pub to: String,
    /// Port type (defaults to "both")
    #[serde(default)]
    pub port: PortType,
}

/// Router document schema.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RouterSchema {
    /// Schema version (must be 1)
    pub version: u32,
    /// Node specifications (optional hints for node creation)
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub nodes: HashMap<String, NodeSpec>,
    /// Edge specifications (authoritative wiring list)
    pub edges: Vec<Edge>,
}

impl RouterSchema {
    /// Validate the schema.
    pub fn validate(&self) -> Result<(), String> {
        // Validate version
        if self.version != 1 {
            return Err(format!("Unsupported version: {}", self.version));
        }

        // Validate node types
        for (id, spec) in &self.nodes {
            if spec.node_type != "document" {
                return Err(format!(
                    "Unsupported node type '{}' for node '{}'",
                    spec.node_type, id
                ));
            }
        }

        // Validate edges reference valid nodes or implicit nodes
        for edge in &self.edges {
            if edge.from.is_empty() {
                return Err("Edge 'from' cannot be empty".to_string());
            }
            if edge.to.is_empty() {
                return Err("Edge 'to' cannot be empty".to_string());
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_minimal_schema() {
        let json = r#"{"version": 1, "edges": []}"#;
        let schema: RouterSchema = serde_json::from_str(json).unwrap();
        assert_eq!(schema.version, 1);
        assert!(schema.nodes.is_empty());
        assert!(schema.edges.is_empty());
    }

    #[test]
    fn test_parse_full_schema() {
        let json = r#"{
            "version": 1,
            "nodes": {
                "doc-a": { "type": "document", "content_type": "text/plain" },
                "doc-b": { "type": "document", "content_type": "application/json" }
            },
            "edges": [
                { "from": "doc-a", "to": "doc-b", "port": "blue" },
                { "from": "doc-b", "to": "doc-a", "port": "red" }
            ]
        }"#;

        let schema: RouterSchema = serde_json::from_str(json).unwrap();
        assert_eq!(schema.version, 1);
        assert_eq!(schema.nodes.len(), 2);
        assert_eq!(schema.edges.len(), 2);

        assert_eq!(schema.edges[0].from, "doc-a");
        assert_eq!(schema.edges[0].to, "doc-b");
        assert_eq!(schema.edges[0].port, PortType::Blue);

        assert_eq!(schema.edges[1].from, "doc-b");
        assert_eq!(schema.edges[1].to, "doc-a");
        assert_eq!(schema.edges[1].port, PortType::Red);
    }

    #[test]
    fn test_default_port_is_both() {
        let json = r#"{
            "version": 1,
            "edges": [{ "from": "a", "to": "b" }]
        }"#;

        let schema: RouterSchema = serde_json::from_str(json).unwrap();
        assert_eq!(schema.edges[0].port, PortType::Both);
    }

    #[test]
    fn test_validate_unsupported_version() {
        let schema = RouterSchema {
            version: 2,
            nodes: HashMap::new(),
            edges: vec![],
        };
        assert!(schema.validate().is_err());
    }

    #[test]
    fn test_validate_unsupported_node_type() {
        let mut nodes = HashMap::new();
        nodes.insert(
            "test".to_string(),
            NodeSpec {
                node_type: "unknown".to_string(),
                content_type: None,
            },
        );
        let schema = RouterSchema {
            version: 1,
            nodes,
            edges: vec![],
        };
        assert!(schema.validate().is_err());
    }
}
