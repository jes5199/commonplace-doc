// Form process: generates an HTML form with embedded messages
// On submit: append to messages.jsonl, then re-render HTML
// The viewer's Yjs subscription pushes HTML updates to the browser

const SERVER = Deno.env.get("COMMONPLACE_SERVER") || "http://localhost:5199";

interface Message {
  timestamp: string;
  message: string;
}

function escapeHtml(text: string): string {
  return text
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

function renderHtml(messages: Message[]): string {
  const messageHtml = messages.map(msg => `
    <div class="message">
      <span class="time">${new Date(msg.timestamp).toLocaleTimeString()}</span>
      <span class="text">${escapeHtml(msg.message)}</span>
    </div>`).join("");

  return `<!DOCTYPE html>
<html>
<head>
  <meta charset="UTF-8">
  <title>Messages</title>
  <style>
    body { font-family: sans-serif; padding: 2rem; max-width: 500px; margin: 0 auto; }
    input[type="text"] { width: 100%; padding: 0.5rem; margin: 0.5rem 0; box-sizing: border-box; }
    button { padding: 0.5rem 1rem; cursor: pointer; }
    #messages { margin-top: 1rem; }
    .message { padding: 0.5rem; margin: 0.25rem 0; background: #f0f0f0; border-radius: 4px; }
    .message .time { color: #666; font-size: 0.8em; margin-right: 0.5rem; }
  </style>
</head>
<body>
  <h1>Messages</h1>
  <form data-cp-command="submit">
    <input type="text" name="message" placeholder="Type a message..." autofocus>
    <button type="submit">Send</button>
  </form>
  <div id="messages">${messageHtml || "<p>No messages yet.</p>"}</div>
</body>
</html>`;
}

async function loadMessages(): Promise<Message[]> {
  try {
    const res = await fetch(`${SERVER}/files/form/messages.jsonl`);
    if (res.ok) {
      const text = await res.text();
      if (text.trim()) {
        return text.trim().split("\n").map(line => JSON.parse(line));
      }
    }
  } catch (e) {
    console.error("Failed to load messages:", e);
  }
  return [];
}

async function saveMessage(entry: Message): Promise<boolean> {
  // Fetch current content and HEAD cid
  let current = "";
  let parentCid = "";
  try {
    const res = await fetch(`${SERVER}/files/form/messages.jsonl/head`);
    if (res.ok) {
      const head = await res.json();
      current = head.content || "";
      parentCid = head.cid;
    }
  } catch (e) {
    console.error("Failed to fetch messages:", e);
    return false;
  }

  // Append and write back
  const updated = current + JSON.stringify(entry) + "\n";
  try {
    const url = parentCid
      ? `${SERVER}/files/form/messages.jsonl/replace?parent_cid=${parentCid}`
      : `${SERVER}/files/form/messages.jsonl/replace`;
    const res = await fetch(url, {
      method: "POST",
      headers: { "Content-Type": "text/plain" },
      body: updated,
    });
    if (!res.ok) {
      console.error("Failed to save message:", await res.text());
      return false;
    }
    return true;
  } catch (e) {
    console.error("Failed to save message:", e);
    return false;
  }
}

async function updateHtml(): Promise<void> {
  const messages = await loadMessages();
  const html = renderHtml(messages);
  await commonplace.output.set(html, { message: "render messages" });
}

// Initialize with current messages
await updateHtml();

// Handle submit command
commonplace.onCommand("submit", async (payload: unknown) => {
  const data = payload as { message?: string };
  const timestamp = new Date().toISOString();

  if (!data.message) {
    console.log(`[${timestamp}] Empty message, ignoring`);
    return;
  }

  console.log(`[${timestamp}] Received: ${data.message}`);

  const entry: Message = { timestamp, message: data.message };

  if (await saveMessage(entry)) {
    await updateHtml();
  }
});
