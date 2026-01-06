import { cp } from "http://localhost:3000/sdk/mod.ts";

const input = cp.doc("test-eval/input.txt");

input.onChange(async (content) => {
  const upper = content.toUpperCase();
  await cp.output.set(upper, { message: "Transformed to uppercase" });
  cp.emit("transformed", { length: upper.length });
});

cp.onCommand("refresh", async () => {
  const content = await input.get();
  await cp.output.set(content.toUpperCase());
});

await cp.start();
