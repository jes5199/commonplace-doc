// AI Chat backend - handles messages and manages conversation log
// Messages are stored in messages.jsonl with a reply field for async responses

// Use the global 'commonplace' SDK injected by the loader
// deno-lint-ignore no-explicit-any
const cp = (globalThis as any).commonplace;

interface Message {
  message: string;
  timestamp: string;
  reply: string | null;
}

// Handle incoming "send" commands from the chat form
cp.onCommand("send", async (payload) => {
  const { message } = payload as { message: string };

  if (!message || !message.trim()) {
    console.log("[chat] Ignoring empty message");
    return;
  }

  console.log(`[chat] Received message: ${message}`);

  // Get current messages
  const messages = (await cp.output.get() || []) as Message[];

  // Append new message with reply: null (to be filled in later by another process)
  messages.push({
    message: message.trim(),
    timestamp: new Date().toISOString(),
    reply: null,
  });

  // Save updated messages
  await cp.output.set(messages);
  console.log(`[chat] Message saved, total messages: ${messages.length}`);
});

console.log("[chat] AI Chat backend starting...");

// Start the SDK and keep process alive
await cp.start();
