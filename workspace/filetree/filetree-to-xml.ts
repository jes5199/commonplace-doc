// Filetree to XML renderer
// Watches for schema changes and outputs the directory tree as XML
import { cp } from "http://localhost:3000/sdk/mod.ts";

interface SchemaEntry {
  type: "doc" | "dir";
  node_id?: string;
  content_type?: string;
  entries?: Record<string, SchemaEntry> | null;
}

interface Schema {
  version: number;
  root: SchemaEntry;
}

// Cache of all schemas by path
const schemas = new Map<string, Schema>();

// Build XML tree from schemas
function buildXml(): string {
  const lines: string[] = ['<?xml version="1.0" encoding="UTF-8"?>'];
  lines.push('<filetree>');

  // Start from root schema
  const rootSchema = schemas.get(".commonplace.json");
  if (rootSchema?.root?.entries) {
    renderEntries(rootSchema.root.entries, 1, lines, "");
  }

  lines.push('</filetree>');
  return lines.join('\n');
}

function renderEntries(
  entries: Record<string, SchemaEntry>,
  indent: number,
  lines: string[],
  pathPrefix: string
): void {
  const pad = '  '.repeat(indent);

  // Sort entries: directories first, then files
  const sorted = Object.entries(entries).sort(([aName, a], [bName, b]) => {
    if (a.type === b.type) return aName.localeCompare(bName);
    return a.type === 'dir' ? -1 : 1;
  });

  for (const [name, entry] of sorted) {
    if (entry.type === 'dir') {
      // Check if we have the subdirectory's schema cached
      const fullPath = pathPrefix ? `${pathPrefix}/${name}` : name;
      const subSchemaPath = `${fullPath}/.commonplace.json`;
      const subSchema = schemas.get(subSchemaPath);

      if (subSchema?.root?.entries) {
        lines.push(`${pad}<dir name="${escapeXml(name)}">`);
        renderEntries(subSchema.root.entries, indent + 1, lines, fullPath);
        lines.push(`${pad}</dir>`);
      } else {
        // No cached schema, render as empty dir
        lines.push(`${pad}<dir name="${escapeXml(name)}"/>`);
      }
    } else {
      lines.push(`${pad}<file name="${escapeXml(name)}"/>`);
    }
  }
}

function escapeXml(s: string): string {
  return s
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;');
}

// Write output via HTTP (bypasses MQTT which server isn't subscribed to)
async function writeOutput(xml: string, message: string): Promise<void> {
  const server = Deno.env.get("COMMONPLACE_SERVER") || "http://localhost:3000";
  const outputPath = Deno.env.get("COMMONPLACE_OUTPUT");

  if (!outputPath) {
    console.warn("[filetree] No COMMONPLACE_OUTPUT set");
    return;
  }

  try {
    const url = `${server}/files/${outputPath.replace(/^\//, '')}/replace`;
    const res = await fetch(url, {
      method: "POST",
      headers: { "Content-Type": "application/xml" },
      body: xml,
    });

    if (!res.ok) {
      console.error(`[filetree] Failed to write output: ${res.status} ${res.statusText}`);
    } else {
      const result = await res.json();
      console.log(`[filetree] Output written (${xml.length} bytes, cid: ${result.cid?.slice(0, 8)}...)`);
    }
  } catch (e) {
    console.error(`[filetree] Error writing output:`, e);
  }
}

// Handle schema updates
async function handleSchemaChange(path: string, content: unknown): Promise<void> {
  console.log(`[filetree] Schema changed: ${path}`);

  try {
    const schema = content as Schema;
    schemas.set(path, schema);

    // Rebuild and output XML
    const xml = buildXml();
    await writeOutput(xml, "Updated filetree");
    console.log(`[filetree] Output updated (${xml.length} bytes)`);
  } catch (e) {
    console.error(`[filetree] Error processing schema:`, e);
  }
}

// Watch for all schema changes using wildcard
// Pattern: # matches all paths, we filter for .commonplace.json
cp.watch("#", (path, content) => {
  if (path.endsWith(".commonplace.json")) {
    // Convert absolute path to relative (strip leading /)
    const relativePath = path.startsWith("/") ? path.slice(1) : path;
    handleSchemaChange(relativePath, content);
  }
});

await cp.start();

// Fetch initial schemas
async function fetchInitialSchemas(): Promise<void> {
  const server = Deno.env.get("COMMONPLACE_SERVER") || "http://localhost:3000";

  try {
    // First get the fs-root ID
    const fsRootRes = await fetch(`${server}/fs-root`);
    if (!fsRootRes.ok) {
      console.error(`[filetree] Failed to get fs-root`);
      return;
    }
    const { id: fsRootId } = await fsRootRes.json();
    console.log(`[filetree] fs-root ID: ${fsRootId}`);

    // Fetch root schema using docs API
    const rootRes = await fetch(`${server}/docs/${fsRootId}/head`);
    if (!rootRes.ok) {
      console.error(`[filetree] Failed to fetch root schema`);
      return;
    }
    const rootHead = await rootRes.json();
    const rootSchema = JSON.parse(rootHead.content);
    schemas.set(".commonplace.json", rootSchema);
    console.log(`[filetree] Loaded root schema`);

    // Fetch subdirectory schemas
    if (rootSchema.root?.entries) {
      for (const [name, entry] of Object.entries(rootSchema.root.entries)) {
        const e = entry as SchemaEntry;
        if (e.type === 'dir' && e.node_id) {
          try {
            const subRes = await fetch(`${server}/docs/${e.node_id}/head`);
            if (subRes.ok) {
              const subHead = await subRes.json();
              const subSchema = JSON.parse(subHead.content);
              schemas.set(`${name}/.commonplace.json`, subSchema);
              console.log(`[filetree] Loaded schema for ${name}`);
            }
          } catch {
            // Subdirectory schema not available
          }
        }
      }
    }

    // Output initial XML via HTTP (cp.output.set uses MQTT/Text, doesn't work with XmlFragment)
    const xml = buildXml();
    await writeOutput(xml, "Initial filetree");
    console.log(`[filetree] Initial output (${xml.length} bytes)`);
  } catch (e) {
    console.error(`[filetree] Error fetching initial schemas:`, e);
  }
}

await fetchInitialSchemas();
console.log("[filetree] Watching for changes...");

// Keep process alive - MQTT subscriptions need this
await new Promise(() => {});
