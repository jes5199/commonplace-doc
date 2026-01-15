// Echo process: receives commands and appends them to output
// Uses evaluate mode with the commonplace SDK

import { cp } from "http://localhost:5199/sdk/mod.ts";

// Helper to append a command to output
async function echoCommand(verb: string, payload: unknown) {
  const timestamp = new Date().toISOString();
  const entry = `[${timestamp}] ${verb}: ${JSON.stringify(payload)}\n`;

  // Get current content and append
  const current = await cp.output.get() as string;
  await cp.output.set(current + entry, { message: `echo: ${verb}` });

  console.log(`Echoed: ${verb}`);
}

// Register handlers for common test verbs
cp.onCommand("hello", (payload) => echoCommand("hello", payload));
cp.onCommand("test", (payload) => echoCommand("test", payload));
cp.onCommand("ping", (payload) => echoCommand("ping", payload));
cp.onCommand("echo", (payload) => echoCommand("echo", payload));

console.log("Echo process starting...");

// Start the SDK (connects to MQTT, activates subscriptions)
await cp.start();

// Initialize output file after connecting
await cp.output.set("# Echo Output\n\n", { message: "initialize" });

console.log("Echo process ready, waiting for commands...");

// Keep process alive
await new Promise(() => {});
