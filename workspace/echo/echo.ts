// Echo process: receives commands and appends them to output
// Uses evaluate mode with the commonplace SDK
// Demonstrates auto-start: no explicit cp.start() needed

import { cp } from "commonplace";

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

// SDK auto-starts after this script completes because onCommand() was called
