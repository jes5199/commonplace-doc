"""
FileProcess - Base class for external processes that own a file path.

This module provides the foundation for building external MQTT clients that
participate as first-class citizens in the commonplace reactive graph.

A FileProcess "becomes" a file by:
1. Claiming a path by subscribing to its ports
2. Creating the document if it doesn't exist
3. Listening for commands (magenta port)
4. Publishing edits (blue port)
5. Broadcasting events (red port)
6. Syncing history on startup (cyan port)
"""

import json
import base64
import uuid
import time
import threading
import queue
from typing import Callable, Optional
from dataclasses import dataclass, field

import paho.mqtt.client as mqtt
import y_py as Y


# Topic patterns
def edits_topic(path: str) -> str:
    """Topic for publishing/receiving edits (blue port)."""
    return f"{path}/edits"


def commands_topic(path: str) -> str:
    """Topic pattern for receiving commands (magenta port)."""
    return f"{path}/commands/#"


def events_topic(path: str, event_name: str) -> str:
    """Topic for broadcasting events (red port)."""
    return f"{path}/events/{event_name}"


def sync_topic(path: str, client_id: str) -> str:
    """Topic for sync protocol (cyan port)."""
    return f"{path}/sync/{client_id}"


@dataclass
class SyncState:
    """Tracks sync protocol state."""
    pending_requests: dict = field(default_factory=dict)
    commits: dict = field(default_factory=dict)  # id -> commit data
    head: Optional[str] = None


class FileProcess:
    """
    Base class for a process that owns a file path in the commonplace system.

    Subclass this and implement command handlers to create your own file process.

    Example:
        class MyProcess(FileProcess):
            def __init__(self):
                super().__init__("examples/myfile.json")
                self.register_command("do-thing", self.on_do_thing)

            def on_do_thing(self, payload: dict):
                # Handle the command
                self.broadcast_event("thing-done", {"result": "success"})
    """

    def __init__(
        self,
        path: str,
        broker_host: str = "localhost",
        broker_port: int = 1883,
        content_type: str = "application/json",
    ):
        """
        Initialize a file process.

        Args:
            path: The file path to own (e.g., "examples/counter.json")
            broker_host: MQTT broker hostname
            broker_port: MQTT broker port
            content_type: MIME type for the document
        """
        self.path = path
        self.broker_host = broker_host
        self.broker_port = broker_port
        self.content_type = content_type
        self.client_id = f"file-process-{uuid.uuid4()}"

        # MQTT client setup
        self._mqtt = mqtt.Client(
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
            client_id=self.client_id,
        )
        self._mqtt.on_connect = self._on_connect
        self._mqtt.on_message = self._on_message

        # State
        self._ydoc = Y.YDoc()
        self._current_head: Optional[str] = None
        self._command_handlers: dict[str, Callable[[dict], None]] = {}
        self._sync_state = SyncState()
        self._connected = threading.Event()
        self._ready = threading.Event()
        self._shutdown = threading.Event()

        # Work queue for thread-safe YDoc operations
        # MQTT callbacks run in a background thread, but YDoc is not thread-safe.
        # Commands are queued here and processed in the main thread.
        self._work_queue: queue.Queue = queue.Queue()

    def register_command(self, verb: str, handler: Callable[[dict], None]) -> None:
        """
        Register a handler for a command verb.

        Args:
            verb: The command verb (e.g., "increment", "reset")
            handler: Function that takes the command payload dict
        """
        self._command_handlers[verb] = handler

    def start(self, blocking: bool = True) -> None:
        """
        Start the file process.

        This will:
        1. Connect to the MQTT broker
        2. Subscribe to this path's ports
        3. Sync history if available
        4. Start listening for commands

        Args:
            blocking: If True, block until shutdown. If False, return after setup.
        """
        print(f"[{self.path}] Starting file process...")
        print(f"[{self.path}] Client ID: {self.client_id}")

        # Connect to broker
        self._mqtt.connect(self.broker_host, self.broker_port)
        self._mqtt.loop_start()

        # Wait for connection
        if not self._connected.wait(timeout=5.0):
            raise RuntimeError("Failed to connect to MQTT broker")

        print(f"[{self.path}] Connected to MQTT broker")

        # Subscribe to ports
        self._claim_path()

        # Request sync
        self._sync_history()

        # Wait for sync to complete (with timeout)
        self._ready.wait(timeout=2.0)

        print(f"[{self.path}] File process ready. Listening for commands...")

        if blocking:
            self._run_main_loop()

    def _run_main_loop(self) -> None:
        """Run the main event loop, processing queued work items."""
        try:
            while not self._shutdown.is_set():
                try:
                    # Wait for work with timeout to allow shutdown checks
                    work_item = self._work_queue.get(timeout=0.1)
                    self._process_work_item(work_item)
                except queue.Empty:
                    pass

        except KeyboardInterrupt:
            print(f"\n[{self.path}] Shutting down...")
            self.stop()

    def _process_work_item(self, item: tuple) -> None:
        """Process a single work item from the queue."""
        item_type, *args = item

        if item_type == "command":
            verb, payload, source = args
            if verb in self._command_handlers:
                print(f"[{self.path}] Received command: {verb} from {source or 'unknown'}")
                self._command_handlers[verb](payload)
            else:
                print(f"[{self.path}] Unknown command: {verb}")

        elif item_type == "edit":
            # External edit from another client
            update_bytes, author = args
            Y.apply_update(self._ydoc, update_bytes)
            print(f"[{self.path}] Applied edit from {author}")

    def stop(self) -> None:
        """Stop the file process and disconnect from MQTT."""
        self._shutdown.set()
        self._mqtt.loop_stop()
        self._mqtt.disconnect()

    def _claim_path(self) -> None:
        """Subscribe to this path's ports to claim ownership."""
        # Subscribe to commands
        self._mqtt.subscribe(commands_topic(self.path), qos=1)
        print(f"[{self.path}] Subscribed to commands")

        # Subscribe to edits (to stay in sync with external changes)
        self._mqtt.subscribe(edits_topic(self.path), qos=1)
        print(f"[{self.path}] Subscribed to edits")

        # Subscribe to our sync response channel
        self._mqtt.subscribe(sync_topic(self.path, self.client_id), qos=0)
        print(f"[{self.path}] Subscribed to sync channel")

    def _sync_history(self) -> None:
        """Request HEAD to sync document history."""
        req_id = f"head-{uuid.uuid4()}"

        request = {
            "type": "head",
            "req": req_id,
        }

        self._sync_state.pending_requests[req_id] = "head"

        topic = sync_topic(self.path, self.client_id)
        self._mqtt.publish(topic, json.dumps(request).encode(), qos=0)
        print(f"[{self.path}] Requested HEAD sync")

    def _on_connect(self, client, userdata, flags, reason_code, properties) -> None:
        """Handle MQTT connection."""
        if reason_code == 0:
            self._connected.set()
        else:
            print(f"[{self.path}] Connection failed: {reason_code}")

    def _on_message(self, client, userdata, msg) -> None:
        """Route incoming MQTT messages to appropriate handlers."""
        topic = msg.topic

        try:
            # Command message
            if "/commands/" in topic:
                verb = topic.split("/commands/")[-1]
                self._handle_command(verb, msg.payload)

            # Edit message (from another client)
            elif topic == edits_topic(self.path):
                self._handle_edit(msg.payload)

            # Sync response
            elif topic == sync_topic(self.path, self.client_id):
                self._handle_sync(msg.payload)

        except Exception as e:
            print(f"[{self.path}] Error handling message on {topic}: {e}")

    def _handle_command(self, verb: str, payload: bytes) -> None:
        """Queue command for processing in main thread."""
        try:
            message = json.loads(payload)
            data = message.get("payload", {})
            source = message.get("source")

            # Queue the command for main thread processing
            # (YDoc operations must happen in main thread)
            self._work_queue.put(("command", verb, data, source))

        except json.JSONDecodeError as e:
            print(f"[{self.path}] Invalid command JSON: {e}")

    def _handle_edit(self, payload: bytes) -> None:
        """Queue incoming edit for processing in main thread."""
        try:
            message = json.loads(payload)
            update_b64 = message.get("update")
            author = message.get("author", "unknown")

            # Skip our own edits
            if author == self.client_id:
                return

            if update_b64:
                update_bytes = base64.b64decode(update_b64)
                # Queue the edit for main thread processing
                # (YDoc operations must happen in main thread)
                self._work_queue.put(("edit", update_bytes, author))

        except Exception as e:
            print(f"[{self.path}] Error applying edit: {e}")

    def _handle_sync(self, payload: bytes) -> None:
        """Handle sync protocol responses."""
        try:
            message = json.loads(payload)
            msg_type = message.get("type")
            req_id = message.get("req")

            if msg_type == "head_response":
                commit = message.get("commit")
                if commit:
                    self._current_head = commit
                    print(f"[{self.path}] HEAD: {commit[:12]}...")
                    # Could request full history here with ancestors
                else:
                    print(f"[{self.path}] Empty document (no HEAD)")
                self._ready.set()

            elif msg_type == "head":
                # This is a request echo (we subscribed to our own topic)
                # Ignore it - the server will respond with head_response
                pass

            elif msg_type == "commit":
                # Store commit for replay
                commit_id = message.get("id")
                self._sync_state.commits[commit_id] = message
                print(f"[{self.path}] Received commit: {commit_id[:12]}...")

            elif msg_type == "done":
                # All commits received, replay them
                commits = message.get("commits", [])
                print(f"[{self.path}] Sync complete: {len(commits)} commits")
                self._replay_commits(commits)
                self._ready.set()

            elif msg_type == "error":
                error = message.get("message", "Unknown error")
                print(f"[{self.path}] Sync error: {error}")
                self._ready.set()

        except Exception as e:
            print(f"[{self.path}] Error handling sync: {e}")
            self._ready.set()

    def _replay_commits(self, commit_ids: list[str]) -> None:
        """Replay commits to reconstruct document state."""
        for commit_id in commit_ids:
            commit = self._sync_state.commits.get(commit_id)
            if commit:
                update_b64 = commit.get("data")
                if update_b64:
                    update_bytes = base64.b64decode(update_b64)
                    Y.apply_update(self._ydoc, update_bytes)

        if commit_ids:
            self._current_head = commit_ids[-1]

    def publish_edit(self, content: dict, message: Optional[str] = None) -> None:
        """
        Publish an edit to update the document content.

        This creates a Yjs update and publishes it to the edits topic.

        Args:
            content: The new document content (as a dict for JSON documents)
            message: Optional commit message
        """
        # Create Yjs update
        with self._ydoc.begin_transaction() as txn:
            # Get or create the root map
            root = self._ydoc.get_map("content")

            # Update each key in the content
            for key, value in content.items():
                root.set(txn, key, value)

        # Get the update
        update_bytes = Y.encode_state_as_update(self._ydoc)
        update_b64 = base64.b64encode(update_bytes).decode()

        # Build edit message
        edit = {
            "update": update_b64,
            "parents": [self._current_head] if self._current_head else [],
            "author": self.client_id,
            "timestamp": int(time.time() * 1000),
        }

        if message:
            edit["message"] = message

        # Publish
        topic = edits_topic(self.path)
        self._mqtt.publish(topic, json.dumps(edit).encode(), qos=1)

        print(f"[{self.path}] Published edit: {message or 'update'}")

    def broadcast_event(self, event_name: str, payload: dict) -> None:
        """
        Broadcast an event from this node.

        Events are ephemeral broadcasts on the red port.

        Args:
            event_name: The event name (e.g., "value-changed")
            payload: The event payload
        """
        message = {
            "payload": payload,
            "source": self.client_id,
        }

        topic = events_topic(self.path, event_name)
        self._mqtt.publish(topic, json.dumps(message).encode(), qos=0)

        print(f"[{self.path}] Broadcast event: {event_name}")

    def get_content(self) -> dict:
        """
        Get the current document content.

        Returns:
            The document content as a dict
        """
        root = self._ydoc.get_map("content")
        return dict(root)
