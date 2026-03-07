//! commonplace-event: Manage red event logs for synced files
//!
//! Usage:
//!   commonplace-event append <target> -t <event_type> -s <source> [-p <payload>]
//!   commonplace-event log <target> [--since N] [-n N] [-t <event_type>]
//!   commonplace-event tail <target>
//!
//! <target> can be an event log UUID or a file path (resolved via workspace schema).

use clap::Parser;
use commonplace_doc::cli::{EventArgs, EventCommand};
use reqwest::Client;
use serde::{Deserialize, Serialize};

#[derive(Deserialize)]
struct AppendResponse {
    event_log_id: String,
}

#[derive(Deserialize, Serialize)]
struct EventEntry {
    timestamp: String,
    event_type: String,
    source: String,
    payload: serde_json::Value,
}

#[derive(Deserialize)]
struct EventsResponse {
    events: Vec<EventEntry>,
}

#[derive(Serialize)]
struct JsonLogOutput {
    target: String,
    events: Vec<EventEntry>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = EventArgs::parse();
    let client = Client::new();

    match args.command {
        EventCommand::Append {
            target,
            event_type,
            source,
            payload,
        } => {
            let target_id = resolve_target(&target)?;
            let payload_value: serde_json::Value = serde_json::from_str(&payload)
                .map_err(|e| format!("Invalid JSON payload: {}", e))?;

            let url = format!("{}/docs/{}/events", args.server, target_id);
            let resp = client
                .post(&url)
                .json(&serde_json::json!({
                    "event_type": event_type,
                    "source": source,
                    "payload": payload_value,
                }))
                .send()
                .await
                .map_err(|e| format!("Failed to append event: {}", e))?;

            if !resp.status().is_success() {
                let status = resp.status();
                let body = resp.text().await.unwrap_or_default();
                eprintln!("Failed to append event: HTTP {} {}", status, body);
                std::process::exit(1);
            }

            let result: AppendResponse = resp.json().await?;

            if args.json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&serde_json::json!({
                        "event_log_id": result.event_log_id,
                        "event_type": event_type,
                        "source": source,
                    }))?
                );
            } else {
                println!("Event appended to log: {}", result.event_log_id);
            }
        }

        EventCommand::Log {
            target,
            since,
            limit,
            event_type,
        } => {
            let target_id = resolve_target(&target)?;

            let mut url = format!("{}/docs/{}/events", args.server, target_id);
            let mut params = Vec::new();
            if let Some(s) = since {
                params.push(format!("since={}", s));
            }
            if let Some(n) = limit {
                params.push(format!("limit={}", n));
            }
            if let Some(ref et) = event_type {
                params.push(format!("event_type={}", urlencoding::encode(et)));
            }
            if !params.is_empty() {
                url = format!("{}?{}", url, params.join("&"));
            }

            let resp = client
                .get(&url)
                .send()
                .await
                .map_err(|e| format!("Failed to read events: {}", e))?;

            if !resp.status().is_success() {
                let status = resp.status();
                let body = resp.text().await.unwrap_or_default();
                eprintln!("Failed to read events: HTTP {} {}", status, body);
                std::process::exit(1);
            }

            let result: EventsResponse = resp.json().await?;

            if args.json {
                let output = JsonLogOutput {
                    target: target_id,
                    events: result.events,
                };
                println!("{}", serde_json::to_string_pretty(&output)?);
            } else if result.events.is_empty() {
                eprintln!("No events found.");
            } else {
                for event in &result.events {
                    println!(
                        "\x1b[90m{}\x1b[0m \x1b[33m{}\x1b[0m \x1b[36m{}\x1b[0m",
                        event.timestamp, event.event_type, event.source
                    );
                    if event.payload != serde_json::json!({}) {
                        println!(
                            "  {}",
                            serde_json::to_string(&event.payload).unwrap_or_default()
                        );
                    }
                }
                eprintln!("\n{} events", result.events.len());
            }
        }

        EventCommand::Tail { target } => {
            let target_id = resolve_target(&target)?;
            let url = format!("{}/sse/docs/{}/events", args.server, target_id);

            eprintln!("Tailing events for {}... (Ctrl+C to stop)", target_id);

            use futures::StreamExt;
            use reqwest_eventsource::{Event as SseEvent, EventSource};

            let request_builder = client.get(&url);
            let mut es = EventSource::new(request_builder)?;

            while let Some(event) = es.next().await {
                match event {
                    Ok(SseEvent::Open) => {}
                    Ok(SseEvent::Message(msg)) => {
                        if msg.event == "event" {
                            if args.json {
                                println!("{}", msg.data);
                            } else if let Ok(entry) =
                                serde_json::from_str::<EventEntry>(&msg.data)
                            {
                                println!(
                                    "\x1b[90m{}\x1b[0m \x1b[33m{}\x1b[0m \x1b[36m{}\x1b[0m",
                                    entry.timestamp, entry.event_type, entry.source
                                );
                                if entry.payload != serde_json::json!({}) {
                                    println!(
                                        "  {}",
                                        serde_json::to_string(&entry.payload).unwrap_or_default()
                                    );
                                }
                            } else {
                                println!("{}", msg.data);
                            }
                        } else if msg.event == "error" {
                            eprintln!("Server error: {}", msg.data);
                        }
                    }
                    Err(e) => {
                        eprintln!("SSE error: {}", e);
                        break;
                    }
                }
            }
        }
    }

    Ok(())
}

/// Resolve a target string to a document UUID.
///
/// If the target parses as a UUID, use it directly.
/// Otherwise, try to resolve it as a file path via workspace schema.
fn resolve_target(target: &str) -> Result<String, Box<dyn std::error::Error>> {
    // Try UUID first
    if uuid::Uuid::parse_str(target).is_ok() {
        return Ok(target.to_string());
    }

    // Try path resolution via workspace schema
    use commonplace_doc::workspace::resolve_path_to_uuid;
    use std::path::Path;

    let (uuid, _workspace_root, _rel_path) = resolve_path_to_uuid(Path::new(target))
        .map_err(|e| format!("Cannot resolve '{}': {:?}", target, e))?;

    // We have the file's node_id. Now look up the event_log from the schema entry.
    // For now, try using the node_id directly (user can pass event_log UUID explicitly).
    // TODO: Add event_log UUID resolution from schema
    Ok(uuid)
}
