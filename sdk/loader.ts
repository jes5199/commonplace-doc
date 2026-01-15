// Loader script for evaluate mode
// Injects 'commonplace' as a global, then imports the user's script

import { cp } from "./mod.ts";

// Set commonplace as a global (aliased from cp)
(globalThis as unknown as Record<string, unknown>).commonplace = cp;

// Get the user's script URL from environment
const scriptUrl = Deno.env.get("COMMONPLACE_SCRIPT");
if (!scriptUrl) {
  console.error("[loader] COMMONPLACE_SCRIPT environment variable not set");
  Deno.exit(1);
}

// Dynamically import and run the user's script
await import(scriptUrl);
