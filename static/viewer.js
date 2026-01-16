// Commonplace Document Viewer
// Displays documents with live WebSocket updates

import * as Y from 'https://esm.sh/yjs@13.6.8';

(function() {
    'use strict';

    // State
    let docId = null;
    let contentType = 'text/plain';
    let ws = null;
    let ydoc = null;
    let reconnectTimeout = null;
    let reconnectDelay = 1000;

    // DOM elements
    const titleEl = document.getElementById('title');
    const statusEl = document.getElementById('status');
    const contentEl = document.getElementById('content');

    // Command bar elements (created dynamically)
    let commandBarEl = null;
    let verbInput = null;
    let payloadInput = null;
    let sendButton = null;
    let commandLogEl = null;

    // Parse document ID from URL
    function parseDocId() {
        const path = window.location.pathname;

        // /view/docs/:id
        const docsMatch = path.match(/^\/view\/docs\/(.+)$/);
        if (docsMatch) {
            return { type: 'id', value: docsMatch[1] };
        }

        // /view/files/*path
        const filesMatch = path.match(/^\/view\/files\/(.+)$/);
        if (filesMatch) {
            return { type: 'path', value: filesMatch[1] };
        }

        return null;
    }

    // Fetch document content via HTTP
    async function fetchDocument(doc) {
        const url = doc.type === 'id'
            ? `/docs/${encodeURIComponent(doc.value)}/head`
            : `/files/${doc.value}/head`;

        const response = await fetch(url);
        if (!response.ok) {
            throw new Error(`HTTP ${response.status}: ${response.statusText}`);
        }

        // HEAD endpoint returns JSON with {cid, content, state}
        const data = await response.json();

        // Guess content type from file extension
        if (doc.type === 'path') {
            const ext = doc.value.split('.').pop()?.toLowerCase();
            if (ext === 'json') contentType = 'application/json';
            else if (ext === 'xml') contentType = 'application/xml';
            else if (ext === 'html' || ext === 'htm') contentType = 'text/html';
            else contentType = 'text/plain';
        }

        return data.content || '';
    }

    // Connect to WebSocket
    function connectWebSocket(doc) {
        const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
        const wsPath = doc.type === 'id'
            ? `/ws/docs/${encodeURIComponent(doc.value)}`
            : `/ws/files/${doc.value}`;

        const wsUrl = `${protocol}//${window.location.host}${wsPath}`;

        ws = new WebSocket(wsUrl, ['y-websocket']);

        ws.binaryType = 'arraybuffer';

        ws.onopen = () => {
            setStatus('connected', 'Connected');
            reconnectDelay = 1000;
        };

        ws.onclose = () => {
            setStatus('disconnected', 'Disconnected');
            scheduleReconnect(doc);
        };

        ws.onerror = (err) => {
            console.error('WebSocket error:', err);
        };

        ws.onmessage = async (event) => {
            // On any message, re-fetch content (simpler than applying Yjs updates)
            try {
                const content = await fetchDocument(doc);
                const text = ydoc.getText('content');
                // Clear and re-insert
                ydoc.transact(() => {
                    text.delete(0, text.length);
                    text.insert(0, content);
                });
                renderContent();
            } catch (e) {
                console.error('Failed to refresh content:', e);
            }
        };
    }

    // Handle incoming WebSocket message
    function handleMessage(data) {
        if (data.length < 1) return;

        const messageType = data[0];
        const payload = data.slice(1);

        // y-websocket message types
        const MSG_SYNC = 0;
        const MSG_AWARENESS = 1;

        if (messageType === MSG_SYNC) {
            handleSyncMessage(payload);
        } else if (messageType === MSG_AWARENESS) {
            // Ignore awareness for now
        }
    }

    // Handle sync message
    function handleSyncMessage(data) {
        if (data.length < 1) return;

        const syncType = data[0];
        const payload = data.slice(1);

        const SYNC_STEP1 = 0;
        const SYNC_STEP2 = 1;
        const SYNC_UPDATE = 2;

        if (syncType === SYNC_STEP1) {
            // Server asking for our state - send empty state vector
            sendSyncStep2();
        } else if (syncType === SYNC_STEP2 || syncType === SYNC_UPDATE) {
            // Apply update
            applyUpdate(payload);
        }
    }

    // Send SyncStep2 with empty update (we have nothing to send)
    function sendSyncStep2() {
        if (!ws || ws.readyState !== WebSocket.OPEN) return;

        // Send our state vector (empty)
        const sv = Y.encodeStateVector(ydoc);
        const msg = new Uint8Array(2 + sv.length);
        msg[0] = 0; // MSG_SYNC
        msg[1] = 0; // SYNC_STEP1
        msg.set(sv, 2);
        ws.send(msg);
    }

    // Apply a Yjs update
    function applyUpdate(update) {
        try {
            Y.applyUpdate(ydoc, update);
            renderContent();
        } catch (e) {
            console.error('Failed to apply update:', e);
        }
    }

    // Get text content from Yjs doc
    function getContent() {
        const text = ydoc.getText('content');
        return text.toString();
    }

    // Render content based on type
    function renderContent() {
        const content = getContent();
        const type = contentType.split(';')[0].trim();

        let html;
        switch (type) {
            case 'application/json':
                html = renderJson(content);
                break;
            case 'application/xml':
            case 'text/xml':
                html = renderXml(content);
                break;
            case 'text/html':
            case 'application/xhtml+xml':
                html = renderHtml(content);
                break;
            default:
                html = renderText(content);
        }

        contentEl.innerHTML = html;
    }

    // Render JSON with syntax highlighting
    function renderJson(content) {
        try {
            // Parse and re-stringify for formatting
            const obj = JSON.parse(content);
            const formatted = JSON.stringify(obj, null, 2);
            return `<pre>${highlightJson(escapeHtml(formatted))}</pre>`;
        } catch (e) {
            return `<pre>${escapeHtml(content)}</pre>`;
        }
    }

    // Simple JSON syntax highlighting
    function highlightJson(text) {
        return text
            .replace(/"([^"]+)":/g, '<span class="json-key">"$1"</span>:')
            .replace(/: "([^"]*)"/g, ': <span class="json-string">"$1"</span>')
            .replace(/: (-?\d+\.?\d*)/g, ': <span class="json-number">$1</span>')
            .replace(/: (true|false)/g, ': <span class="json-boolean">$1</span>')
            .replace(/: (null)/g, ': <span class="json-null">$1</span>');
    }

    // Render XML with syntax highlighting
    function renderXml(content) {
        const escaped = escapeHtml(content);
        const highlighted = escaped
            .replace(/&lt;(\/?[\w:-]+)/g, '&lt;<span class="xml-tag">$1</span>')
            .replace(/(\w+)=(&quot;[^&]*&quot;)/g, '<span class="xml-attr">$1</span>=<span class="xml-value">$2</span>')
            .replace(/&lt;!--[\s\S]*?--&gt;/g, '<span class="xml-comment">$&</span>');
        return `<pre>${highlighted}</pre>`;
    }

    // Render HTML in an iframe with commonplace command support
    function renderHtml(content) {
        // Inject script to handle forms with data-cp-command attribute
        const origin = window.location.origin;
        const commandScript = `
<script>
(function() {
    const DOC_PATH = ${JSON.stringify(docId)};
    const ORIGIN = ${JSON.stringify(origin)};

    document.addEventListener('DOMContentLoaded', function() {
        document.querySelectorAll('form[data-cp-command]').forEach(function(form) {
            form.addEventListener('submit', async function(e) {
                e.preventDefault();

                const verb = form.dataset.cpCommand;
                const formData = new FormData(form);
                const payload = {};

                formData.forEach(function(value, key) {
                    payload[key] = value;
                });

                const encodedPath = DOC_PATH.split('/').map(encodeURIComponent).join('/');
                const commandUrl = ORIGIN + '/commands/' + encodedPath + '/' + encodeURIComponent(verb);

                try {
                    const response = await fetch(commandUrl, {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({ payload: payload, source: 'xhtml-form' }),
                    });

                    if (!response.ok) {
                        console.error('Command failed:', response.status, await response.text());
                    } else {
                        // Clear form on success
                        form.reset();
                    }
                } catch (err) {
                    console.error('Command error:', err);
                }
            });
        });
    });
})();
<\/script>`;

        // Inject before </body> or </html> or at end
        let modifiedContent = content;
        if (content.includes('</body>')) {
            modifiedContent = content.replace('</body>', commandScript + '</body>');
        } else if (content.includes('</html>')) {
            modifiedContent = content.replace('</html>', commandScript + '</html>');
        } else {
            modifiedContent = content + commandScript;
        }

        const blob = new Blob([modifiedContent], { type: 'text/html' });
        const url = URL.createObjectURL(blob);
        // allow-same-origin needed for fetch, allow-forms for form submission
        return `<iframe class="html-frame" src="${url}" sandbox="allow-scripts allow-same-origin allow-forms"></iframe>`;
    }

    // Render plain text
    function renderText(content) {
        return `<pre>${escapeHtml(content)}</pre>`;
    }

    // Escape HTML entities
    function escapeHtml(text) {
        const div = document.createElement('div');
        div.textContent = text;
        return div.innerHTML;
    }

    // Set connection status
    function setStatus(state, text) {
        statusEl.className = `status ${state}`;
        statusEl.textContent = text;
    }

    // Schedule reconnection
    function scheduleReconnect(doc) {
        if (reconnectTimeout) {
            clearTimeout(reconnectTimeout);
        }

        setStatus('reconnecting', `Reconnecting in ${reconnectDelay/1000}s...`);

        reconnectTimeout = setTimeout(() => {
            connectWebSocket(doc);
        }, reconnectDelay);

        // Exponential backoff, max 30s
        reconnectDelay = Math.min(reconnectDelay * 2, 30000);
    }

    // Show error
    function showError(message) {
        contentEl.innerHTML = `<div class="error">${escapeHtml(message)}</div>`;
    }

    // Create command bar UI
    function createCommandBar() {
        commandBarEl = document.createElement('footer');
        commandBarEl.id = 'command-bar';
        commandBarEl.innerHTML = `
            <div class="command-input-row">
                <input type="text" id="verb-input" placeholder="verb" autocomplete="off">
                <input type="text" id="payload-input" placeholder='{"key": "value"} (optional JSON)' autocomplete="off">
                <button id="send-button">Send</button>
            </div>
            <div id="command-log"></div>
        `;
        document.body.appendChild(commandBarEl);

        verbInput = document.getElementById('verb-input');
        payloadInput = document.getElementById('payload-input');
        sendButton = document.getElementById('send-button');
        commandLogEl = document.getElementById('command-log');

        sendButton.addEventListener('click', sendCommand);
        verbInput.addEventListener('keydown', (e) => {
            if (e.key === 'Enter') sendCommand();
        });
        payloadInput.addEventListener('keydown', (e) => {
            if (e.key === 'Enter') sendCommand();
        });
    }

    // Send command via HTTP
    async function sendCommand() {
        const verb = verbInput.value.trim();
        if (!verb) {
            logCommand('error', 'Please enter a verb');
            return;
        }

        // Parse payload if provided
        let payload = {};
        const payloadStr = payloadInput.value.trim();
        if (payloadStr) {
            try {
                payload = JSON.parse(payloadStr);
            } catch (e) {
                logCommand('error', `Invalid JSON payload: ${e.message}`);
                return;
            }
        }

        // Build the command URL
        // Note: docId may contain slashes (paths like "bartleby/history.jsonl")
        // We encode each segment separately to preserve the path structure
        const encodedPath = docId.split('/').map(encodeURIComponent).join('/');
        const commandUrl = `/commands/${encodedPath}/${encodeURIComponent(verb)}`;

        try {
            sendButton.disabled = true;
            sendButton.textContent = 'Sending...';

            const response = await fetch(commandUrl, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ payload, source: 'web-viewer' }),
            });

            if (!response.ok) {
                const errorText = await response.text();
                throw new Error(`HTTP ${response.status}: ${errorText}`);
            }

            const result = await response.json();
            logCommand('success', `Sent: ${verb}`, payload);

            // Clear inputs on success
            verbInput.value = '';
            payloadInput.value = '';

        } catch (e) {
            logCommand('error', `Failed to send command: ${e.message}`);
        } finally {
            sendButton.disabled = false;
            sendButton.textContent = 'Send';
        }
    }

    // Log command to the command log area
    function logCommand(type, message, payload = null) {
        const entry = document.createElement('div');
        entry.className = `command-log-entry ${type}`;

        const time = new Date().toLocaleTimeString();
        let content = `<span class="log-time">[${time}]</span> ${escapeHtml(message)}`;
        if (payload && Object.keys(payload).length > 0) {
            content += ` <span class="log-payload">${escapeHtml(JSON.stringify(payload))}</span>`;
        }
        entry.innerHTML = content;

        commandLogEl.insertBefore(entry, commandLogEl.firstChild);

        // Keep only last 10 entries
        while (commandLogEl.children.length > 10) {
            commandLogEl.removeChild(commandLogEl.lastChild);
        }
    }

    // Initialize
    async function init() {
        const doc = parseDocId();

        if (!doc) {
            titleEl.textContent = 'Error';
            showError('Invalid URL. Use /view/docs/:id or /view/files/*path');
            return;
        }

        titleEl.textContent = doc.value;
        docId = doc.value;

        // Initialize Yjs document
        ydoc = new Y.Doc();

        try {
            // Fetch initial content
            const content = await fetchDocument(doc);

            // Initialize Yjs text with fetched content
            const text = ydoc.getText('content');
            text.insert(0, content);

            // Render immediately
            renderContent();

            // Create command bar
            createCommandBar();

            // Connect WebSocket for live updates
            connectWebSocket(doc);

        } catch (e) {
            console.error('Failed to load document:', e);
            showError(`Failed to load document: ${e.message}`);
            setStatus('disconnected', 'Error');
        }
    }

    // Start
    init();
})();
