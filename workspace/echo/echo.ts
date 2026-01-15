// Echo process: receives commands and appends them to output
// Uses evaluate mode - 'commonplace' is injected as a global (no import needed)

// Handle any command by appending to output
commonplace.onCommand("*", async (verb: string, payload: unknown) => {
  const timestamp = new Date().toISOString();
  const entry = `[${timestamp}] ${verb}: ${JSON.stringify(payload)}\n`;

  // Get current content and append
  const current = await commonplace.output.get() as string;
  await commonplace.output.set(current + entry, { message: `echo: ${verb}` });

  console.log(`Echoed: ${verb}`);
});

// Initialize output file
await commonplace.output.set("# Echo Output\n\n", { message: "initialize" });
