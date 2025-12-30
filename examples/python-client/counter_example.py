"""
CounterExample - Demonstrates an external process participating in the reactive graph.

This example creates a counter that:
- Owns the path "examples/counter.json"
- Accepts commands: increment, decrement, reset, get
- Broadcasts events: value-changed
- Publishes edits: JSON updates to {"counter": N}

Usage:
    uv run python counter_example.py

Test with mosquitto:
    # Send commands
    mosquitto_pub -t "examples/counter.json/commands/increment" \
        -m '{"payload": {"amount": 5}, "source": "test-client"}'

    mosquitto_pub -t "examples/counter.json/commands/reset" \
        -m '{"payload": {}, "source": "test-client"}'

    # Watch events
    mosquitto_sub -t "examples/counter.json/events/#" -v
"""

from file_process import FileProcess


class CounterExample(FileProcess):
    """
    A counter that demonstrates all four MQTT ports.

    This process "becomes" the file at examples/counter.json by:
    1. Subscribing to its command and edit ports (claiming ownership)
    2. Syncing any existing history
    3. Listening for commands and broadcasting events
    4. Publishing edits when the counter changes
    """

    def __init__(
        self,
        path: str = "examples/counter.json",
        broker_host: str = "localhost",
        broker_port: int = 1883,
    ):
        super().__init__(
            path=path,
            broker_host=broker_host,
            broker_port=broker_port,
            content_type="application/json",
        )

        # Initialize counter value (will be synced from document on start)
        self._counter = 0

        # Register command handlers
        self.register_command("increment", self._on_increment)
        self.register_command("decrement", self._on_decrement)
        self.register_command("reset", self._on_reset)
        self.register_command("get", self._on_get)

    def start(self, blocking: bool = True) -> None:
        """Start the counter process, syncing state from any existing document."""
        # Call parent start (connects, syncs history)
        super().start(blocking=False)

        # Initialize counter from synced document content
        self._sync_counter_from_doc()

        print(f"[{self.path}] Counter initialized to {self._counter}")

        if blocking:
            # Use parent's main loop which processes the work queue
            self._run_main_loop()

    def _sync_counter_from_doc(self) -> None:
        """Sync local counter state from the YDoc content."""
        content = self.get_content()
        if "counter" in content:
            self._counter = int(content["counter"])

    def _process_work_item(self, item: tuple) -> None:
        """Process work item, syncing counter after edits."""
        # Call parent to apply the Yjs update or handle command
        super()._process_work_item(item)

        # If it was an edit, sync our local counter from the updated document
        if item[0] == "edit":
            self._sync_counter_from_doc()
            print(f"[{self.path}] Counter synced to {self._counter}")

    def _on_increment(self, payload: dict) -> None:
        """Handle increment command."""
        amount = payload.get("amount", 1)
        old_value = self._counter
        self._counter += amount

        print(f"[{self.path}] Counter: {old_value} + {amount} = {self._counter}")

        # Publish the edit (persist the new value)
        self.publish_edit(
            {"counter": self._counter},
            message=f"Increment by {amount}",
        )

        # Broadcast the change event
        self.broadcast_event("value-changed", {
            "old": old_value,
            "new": self._counter,
            "delta": amount,
        })

    def _on_decrement(self, payload: dict) -> None:
        """Handle decrement command."""
        amount = payload.get("amount", 1)
        old_value = self._counter
        self._counter -= amount

        print(f"[{self.path}] Counter: {old_value} - {amount} = {self._counter}")

        # Publish the edit
        self.publish_edit(
            {"counter": self._counter},
            message=f"Decrement by {amount}",
        )

        # Broadcast the change event
        self.broadcast_event("value-changed", {
            "old": old_value,
            "new": self._counter,
            "delta": -amount,
        })

    def _on_reset(self, payload: dict) -> None:
        """Handle reset command."""
        old_value = self._counter
        self._counter = 0

        print(f"[{self.path}] Counter reset: {old_value} -> 0")

        # Publish the edit
        self.publish_edit(
            {"counter": self._counter},
            message="Reset counter",
        )

        # Broadcast the change event
        self.broadcast_event("value-changed", {
            "old": old_value,
            "new": 0,
            "delta": -old_value,
        })

    def _on_get(self, payload: dict) -> None:
        """Handle get command - broadcasts current value without changing it."""
        print(f"[{self.path}] Get request, current value: {self._counter}")

        # Just broadcast the current value (no edit, no change)
        self.broadcast_event("current-value", {
            "value": self._counter,
        })


def main():
    """Run the counter example."""
    import argparse
    import os

    # Read environment variables for defaults
    default_path = os.environ.get("COMMONPLACE_PATH", "examples/counter.json")

    # Parse COMMONPLACE_MQTT if present (format: host:port or mqtt://host:port)
    mqtt_env = os.environ.get("COMMONPLACE_MQTT", "")
    # Strip URL schemes if present
    for scheme in ("mqtt://", "tcp://", "ssl://"):
        if mqtt_env.startswith(scheme):
            mqtt_env = mqtt_env[len(scheme):]
            break
    if mqtt_env and ":" in mqtt_env:
        default_broker, port_str = mqtt_env.rsplit(":", 1)
        try:
            default_port = int(port_str)
        except ValueError:
            # Port not a number, use whole thing as broker
            default_broker = mqtt_env
            default_port = 1883
    else:
        default_broker = mqtt_env if mqtt_env else "localhost"
        default_port = 1883

    parser = argparse.ArgumentParser(
        description="Counter example demonstrating external process as first-class citizen"
    )
    parser.add_argument(
        "--path",
        default=default_path,
        help=f"Path to own (default: {default_path})",
    )
    parser.add_argument(
        "--broker",
        default=default_broker,
        help=f"MQTT broker host (default: {default_broker})",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=default_port,
        help=f"MQTT broker port (default: {default_port})",
    )

    args = parser.parse_args()

    print("=" * 60)
    print("Counter Example - External Process as First-Class Citizen")
    print("=" * 60)
    print()
    print("This process demonstrates the four-port model:")
    print("  - Blue (edits): Publishes Yjs updates when counter changes")
    print("  - Cyan (sync): Syncs history on startup")
    print("  - Red (events): Broadcasts value-changed events")
    print("  - Magenta (commands): Accepts increment/decrement/reset/get")
    print()
    print("Test commands with mosquitto_pub:")
    print(f'  mosquitto_pub -t "{args.path}/commands/increment" \\')
    print('    -m \'{"payload": {"amount": 5}, "source": "test"}\'')
    print()
    print("Watch events with mosquitto_sub:")
    print(f'  mosquitto_sub -t "{args.path}/events/#" -v')
    print()
    print("=" * 60)

    counter = CounterExample(
        path=args.path,
        broker_host=args.broker,
        broker_port=args.port,
    )

    counter.start(blocking=True)


if __name__ == "__main__":
    main()
