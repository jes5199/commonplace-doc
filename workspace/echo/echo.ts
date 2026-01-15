// Echo process: receives commands and appends them to output
// Uses evaluate mode with the commonplace SDK

import { cp } from "http://localhost:5199/sdk/mod.ts";

// Handle any command by appending to output
cp.onCommand("*", async (verb: string, payload: unknown) => {
  const timestamp = new Date().toISOString();
  const entry = `[${timestamp}] ${verb}: ${JSON.stringify(payload)}\n`;

  // Get current content and append
  const current = await cp.output.get() as string;
  await cp.output.set(current + entry, { message: `echo: ${verb}` });

  console.log(`Echoed: ${verb}`);
});

// Initialize output file (uses HTTP, doesn't need MQTT)
await cp.output.set("# Echo Output\n\n", { message: "initialize" });

// Start the SDK - connects to MQTT and keeps process alive when handlers are registered
await cp.start();
