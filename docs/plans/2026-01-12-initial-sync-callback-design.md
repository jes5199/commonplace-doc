# CP-2wie: Initial Sync Finished Callback Design

## Problem

Sandbox processes may start before initial sync completes, causing race conditions where processes see incomplete or empty files. The orchestrator currently polls the schema to detect sync completion, which is inefficient and imprecise.

## Solution

Add an MQTT event that sync publishes when initial sync completes. The orchestrator subscribes to this event and starts dependent processes only after receiving it.

## Design Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Approach | MQTT events | Leverages existing infrastructure, fits reactive architecture |
| Timing | After schema + all files synced | Sandbox processes get complete content before starting |
| Fallback | Polling when no MQTT | Backward compatibility, existing code works |
| Payload | Include stats | Useful for observability without bloat |
| Topic | `{workspace}/events/sync/initial-complete` | Follows existing `{workspace}/{port}/{path}` pattern |
| Detection | Auto-detect from config | If `mqtt_broker` set, use MQTT; otherwise poll |

## Event Specification

**Topic**: `{workspace}/events/sync/initial-complete`

**Payload**:
```json
{
  "fs_root_id": "workspace",
  "files_synced": 42,
  "strategy": "local",
  "duration_ms": 1523
}
```

**QoS**: AtLeastOnce (QoS 1)

## Implementation

### Sync Client (`src/bin/sync.rs`)

Add event struct to `src/sync/types.rs`:
```rust
#[derive(Serialize, Deserialize)]
pub struct InitialSyncComplete {
    pub fs_root_id: String,
    pub files_synced: usize,
    pub strategy: String,
    pub duration_ms: u64,
}
```

In `run_directory_mode` and `run_exec_mode`, after initial sync completes:
1. Capture `start_time = Instant::now()` at function start
2. After "Initial sync complete" log, publish event if MQTT available:
```rust
if let Some(mqtt) = &mqtt_client {
    let event = InitialSyncComplete {
        fs_root_id: fs_root_id.clone(),
        files_synced: synced_count,
        strategy: initial_sync.clone(),
        duration_ms: start_time.elapsed().as_millis() as u64,
    };
    let topic = format!("{}/events/sync/initial-complete", fs_root_id);
    let payload = serde_json::to_string(&event)?;
    mqtt.publish(&topic, payload.as_bytes(), QoS::AtLeastOnce).await;
}
```

### Orchestrator (`src/bin/orchestrator.rs`)

Add new wait function:
```rust
async fn wait_for_sync_via_mqtt(
    mqtt_client: &MqttClient,
    fs_root_id: &str,
    timeout: Duration,
) -> Result<InitialSyncComplete, Error> {
    let topic = format!("{}/events/sync/initial-complete", fs_root_id);
    mqtt_client.subscribe(&topic, QoS::AtLeastOnce).await?;

    tokio::select! {
        msg = mqtt_client.recv() => {
            // Parse payload - crash if malformed (indicates bug)
            serde_json::from_slice(&msg.payload)?
        }
        _ = tokio::time::sleep(timeout) => {
            Err(Error::Timeout("Initial sync did not complete"))
        }
    }
}
```

Modify startup flow:
```rust
let sync_complete = if let Some(broker) = &config.mqtt_broker {
    match MqttClient::connect(broker).await {
        Ok(mqtt) => wait_for_sync_via_mqtt(&mqtt, &fs_root_id, timeout).await,
        Err(e) => {
            warn!("MQTT connect failed, falling back to polling: {}", e);
            wait_for_sync_initial_push(&client, &server, &fs_root_id).await
        }
    }
} else {
    wait_for_sync_initial_push(&client, &server, &fs_root_id).await
};
```

## Error Handling

| Scenario | Behavior |
|----------|----------|
| MQTT connection fails | Log warning, fall back to polling |
| Sync crashes before event | Timeout triggers, sync restart policy retries |
| Malformed event payload | Crash (indicates bug in sync) |
| Timeout waiting for event | Error, existing timeout behavior |

## Testing

1. **Unit test**: `InitialSyncComplete` serialization/deserialization
2. **Integration test**: Verify event received before sandbox starts
3. **Fallback test**: Verify polling works without `mqtt_broker`

## Flow Diagrams

### With MQTT
```
Orchestrator                    Sync                    MQTT
    |                            |                       |
    |-- subscribe to event ----->|                       |
    |-- start sync ------------->|                       |
    |                            |-- initial sync ------>|
    |                            |-- publish event ----->|
    |<-- receive event ----------------------------------|
    |-- start sandbox processes  |                       |
```

### Without MQTT (fallback)
```
Orchestrator                    Sync                    Server
    |                            |                       |
    |-- start sync ------------->|                       |
    |                            |-- push schema ------->|
    |-- poll schema HEAD -------------------------------->|
    |<-- schema has content ------------------------------|
    |-- start sandbox processes  |                       |
```

## Related Issues

- Blocks: CP-fi7t (race condition fix)
- Related: CP-g1oo (MQTT topic path audit)
