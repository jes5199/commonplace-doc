// Filter commonplace-issues.jsonl using the new typed SDK API
import { cp } from "../sdk/mod.ts";

// Define issue type for type safety
interface Issue {
  id: string;
  title: string;
  status: string;
  priority: number;
  issue_type: string;
  [key: string]: unknown;
}

const input = cp.doc("beads/commonplace-issues.jsonl");

async function filterAndOutput(issues: Issue[]) {
  // Filter to open issues - no manual JSON parsing needed!
  const openIssues = issues.filter(issue => issue.status === "open");

  // Output array directly - SDK serializes to JSONL automatically!
  await cp.output.set(openIssues, { message: "Filtered open issues" });
  console.log(`[filter-open] Filtered ${issues.length} issues -> ${openIssues.length} open`);
}

// Subscribe to changes - callback receives parsed array
input.onChange((content) => {
  filterAndOutput(content as Issue[]);
});

await cp.start();

// Get initial content - returns parsed array, not string!
const issues = await input.get() as Issue[];
await filterAndOutput(issues);

console.log("[filter-open] Watching for changes...");
