import assert from "node:assert/strict";
import { access } from "node:fs/promises";
import path from "node:path";
import test from "node:test";

async function exists(filePath: string): Promise<boolean> {
  try {
    await access(filePath);
    return true;
  } catch {
    return false;
  }
}

test("scalp v2 cron route files exist for active and compatibility handlers", async () => {
  const root = process.cwd();
  const expectedRouteFiles = [
    "pages/api/scalp/v2/cron/cycle.ts",
    "pages/api/scalp/v2/cron/execute.ts",
    "pages/api/scalp/v2/cron/reconcile.ts",
    "pages/api/scalp/v2/cron/load-candles.ts",
    "pages/api/scalp/v2/cron/research.ts",
    "pages/api/scalp/v2/cron/promote.ts",
    "pages/api/scalp/v2/cron/discover.ts",
    "pages/api/scalp/v2/cron/evaluate.ts",
    "pages/api/scalp/v2/cron/worker.ts",
  ];

  for (const relativePath of expectedRouteFiles) {
    const absolutePath = path.join(root, relativePath);
    assert.equal(
      await exists(absolutePath),
      true,
      `expected cron route file to exist: ${relativePath}`,
    );
  }
});
