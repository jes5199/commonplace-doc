//! commonplace-init: Initialize a repo/branch workspace
//!
//! Creates a repo container directory with a `main` branch inside it.
//! The repo dir is just a directory document in the root tree; each
//! branch is a subdirectory entry with its own schema.
//!
//! Usage:
//!   commonplace init myapp
//!
//! Creates:
//!   /myapp/          (repo container — directory doc)
//!   /myapp/main/     (main branch — directory doc, initially empty)

use clap::Parser;
use commonplace_doc::cli::InitArgs;
use commonplace_doc::sync::client::{discover_fs_root, fetch_head, push_schema_to_server};
use commonplace_doc::DEFAULT_SERVER_URL;
use commonplace_types::config::{resolve_field, CommonplaceConfig};
use reqwest::Client;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = InitArgs::parse();

    let config = CommonplaceConfig::load().unwrap_or_default();
    let server = resolve_field(args.server.clone(), config.server.as_deref(), DEFAULT_SERVER_URL);

    // Validate repo name
    if args.name.is_empty()
        || args.name.contains('/')
        || args.name == "."
        || args.name == ".."
        || args.name.starts_with('_')
    {
        eprintln!("Invalid repo name: '{}' (must not contain '/', start with '_', or be '.'/'..'')", args.name);
        std::process::exit(1);
    }

    let client = Client::new();

    // 1. Discover the fs-root UUID
    let root_id = discover_fs_root(&client, &server).await.map_err(|e| {
        format!(
            "Cannot discover fs-root: {}. Is the server running at {}?",
            e, &server
        )
    })?;

    println!("Found root: {}", root_id);

    // 2. Check if repo already exists in root schema
    let root_head = fetch_head(&client, &server, &root_id, false)
        .await
        .map_err(|e| format!("Cannot read root schema: {}", e))?;

    if let Some(ref head) = root_head {
        if let Ok(schema) = serde_json::from_str::<serde_json::Value>(&head.content) {
            if let Some(entries) = schema
                .get("root")
                .and_then(|r| r.get("entries"))
                .and_then(|e| e.as_object())
            {
                if entries.contains_key(&args.name) {
                    eprintln!(
                        "Repo '{}' already exists in the root schema.",
                        args.name
                    );
                    std::process::exit(1);
                }
            }
        }
    }

    // 3. Create the main branch directory document
    let main_branch_id = create_directory_doc(&client, &server).await?;
    println!(
        "Created main branch doc: {}",
        &main_branch_id[..8.min(main_branch_id.len())]
    );

    // 4. Initialize the main branch schema (empty directory)
    let main_schema = serde_json::json!({
        "version": 1,
        "root": {
            "type": "dir",
            "entries": {}
        }
    });
    push_schema_to_server(
        &client,
        &server,
        &main_branch_id,
        &main_schema.to_string(),
        "commonplace-init",
    )
    .await
    .map_err(|e| format!("Failed to set main branch schema: {}", e))?;

    // 5. Create the repo container directory document
    let repo_id = create_directory_doc(&client, &server).await?;
    println!(
        "Created repo doc: {}",
        &repo_id[..8.min(repo_id.len())]
    );

    // 6. Set repo schema with main branch entry
    let repo_schema = serde_json::json!({
        "version": 1,
        "root": {
            "type": "dir",
            "entries": {
                "main": {
                    "type": "dir",
                    "node_id": main_branch_id
                }
            }
        }
    });
    push_schema_to_server(
        &client,
        &server,
        &repo_id,
        &repo_schema.to_string(),
        "commonplace-init",
    )
    .await
    .map_err(|e| format!("Failed to set repo schema: {}", e))?;

    // 7. Add repo to root schema
    let root_schema = serde_json::json!({
        "version": 1,
        "root": {
            "type": "dir",
            "entries": {
                &args.name: {
                    "type": "dir",
                    "node_id": repo_id
                }
            }
        }
    });
    push_schema_to_server(
        &client,
        &server,
        &root_id,
        &root_schema.to_string(),
        "commonplace-init",
    )
    .await
    .map_err(|e| format!("Failed to update root schema: {}", e))?;

    println!(
        "\nRepo '{}' initialized with branch 'main'.",
        args.name
    );
    println!(
        "  /{}/       → repo container ({})",
        args.name,
        &repo_id[..8.min(repo_id.len())]
    );
    println!(
        "  /{}/main/  → main branch ({})",
        args.name,
        &main_branch_id[..8.min(main_branch_id.len())]
    );
    println!("\nThe sync agent will materialize these directories automatically.");

    Ok(())
}

/// Create a new JSON directory document on the server.
async fn create_directory_doc(
    client: &Client,
    server: &str,
) -> Result<String, Box<dyn std::error::Error>> {
    let create_url = format!("{}/docs", server);
    let resp = client
        .post(&create_url)
        .json(&serde_json::json!({
            "content_type": "application/json"
        }))
        .send()
        .await?;

    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        return Err(format!("Failed to create directory doc: {} - {}", status, body).into());
    }

    let body: serde_json::Value = resp.json().await?;
    let id = body
        .get("id")
        .and_then(|v| v.as_str())
        .ok_or("Server response missing 'id' field")?;

    Ok(id.to_string())
}
