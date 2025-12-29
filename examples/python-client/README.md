# Python Client for Commonplace

This example demonstrates how external processes can participate as first-class citizens in the Commonplace reactive graph via MQTT.

## The Four-Port Model

Every node in Commonplace communicates through four ports:

| Port | Color | Topic Pattern | Purpose |
|------|-------|---------------|---------|
| Edits | Blue | `{path}/edits` | Persistent Yjs CRDT updates |
| Sync | Cyan | `{path}/sync/{client-id}` | History catch-up protocol |
| Events | Red | `{path}/events/{name}` | Ephemeral broadcasts |
| Commands | Magenta | `{path}/commands/{verb}` | Commands to the owner |

An external process "becomes" a file by subscribing to its ports and handling messages according to this protocol.

## Prerequisites

- Python 3.10+
- [uv](https://docs.astral.sh/uv/) for package management
- MQTT broker (e.g., [Mosquitto](https://mosquitto.org/))
- Commonplace server running with MQTT enabled

## Installation

```bash
cd examples/python-client
uv sync
```

## Running the Counter Example

The counter example demonstrates all four ports:

```bash
uv run python counter_example.py
```

Or with custom settings:

```bash
uv run python counter_example.py --path "myapp/counter.json" --broker localhost --port 1883
```

## Testing with Mosquitto

### Send Commands

```bash
# Increment by 1
mosquitto_pub -t "examples/counter.json/commands/increment" \
  -m '{"payload": {}, "source": "test-client"}'

# Increment by 5
mosquitto_pub -t "examples/counter.json/commands/increment" \
  -m '{"payload": {"amount": 5}, "source": "test-client"}'

# Decrement
mosquitto_pub -t "examples/counter.json/commands/decrement" \
  -m '{"payload": {"amount": 2}, "source": "test-client"}'

# Reset to zero
mosquitto_pub -t "examples/counter.json/commands/reset" \
  -m '{"payload": {}, "source": "test-client"}'

# Get current value (broadcasts without changing)
mosquitto_pub -t "examples/counter.json/commands/get" \
  -m '{"payload": {}, "source": "test-client"}'
```

### Watch Events

```bash
# Watch all events from the counter
mosquitto_sub -t "examples/counter.json/events/#" -v

# Watch edits (Yjs updates)
mosquitto_sub -t "examples/counter.json/edits" -v
```

## Building Your Own File Process

Extend `FileProcess` to create your own file-owning process:

```python
from file_process import FileProcess

class MyProcess(FileProcess):
    def __init__(self):
        super().__init__(
            path="myapp/myfile.json",
            broker_host="localhost",
            broker_port=1883,
            content_type="application/json",
        )

        # Register command handlers
        self.register_command("do-thing", self.on_do_thing)
        self.register_command("another-thing", self.on_another_thing)

    def on_do_thing(self, payload: dict) -> None:
        # Handle the command
        result = payload.get("value", 0) * 2

        # Publish an edit (persistent change)
        self.publish_edit(
            {"result": result},
            message="Computed result",
        )

        # Broadcast an event (ephemeral notification)
        self.broadcast_event("thing-done", {"result": result})

    def on_another_thing(self, payload: dict) -> None:
        # Get current document content
        content = self.get_content()
        print(f"Current content: {content}")

if __name__ == "__main__":
    process = MyProcess()
    process.start(blocking=True)
```

## Message Formats

### Edit Message (Blue Port)

```json
{
  "update": "base64-encoded-yjs-update",
  "parents": ["parent-commit-id"],
  "author": "client-id",
  "message": "Optional commit message",
  "timestamp": 1704067200000
}
```

### Command Message (Magenta Port)

```json
{
  "payload": { "amount": 5 },
  "source": "sender-client-id"
}
```

### Event Message (Red Port)

```json
{
  "payload": { "old": 0, "new": 5 },
  "source": "owner-client-id"
}
```

### Sync Messages (Cyan Port)

Request HEAD:
```json
{"type": "head", "req": "request-id"}
```

Response:
```json
{"type": "head", "req": "request-id", "commit": "commit-id"}
```

## Architecture Notes

- **Ownership is convention-based**: Whoever subscribes to a path's commands is considered the owner
- **Edits use Yjs CRDTs**: All document changes go through the Yjs update mechanism
- **Events are fire-and-forget**: No persistence, QoS 0
- **Sync protocol is git-like**: Request HEAD, get commits, replay to reconstruct state

## Dependencies

- `paho-mqtt>=2.0.0` - MQTT client
- `y-py>=0.6.0` - Python bindings for Yjs CRDTs
