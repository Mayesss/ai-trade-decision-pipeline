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

test("cycle-era scalp API route files are removed in async-job cutover", async () => {
  const root = process.cwd();
  const removedRoutes = [
    "pages/api/scalp/cron/orchestrate-pipeline.ts",
    "pages/api/scalp/cron/prepare-and-start-cycle.ts",
    "pages/api/scalp/cron/research-cycle-start.ts",
    "pages/api/scalp/cron/research-cycle-worker.ts",
    "pages/api/scalp/cron/research-cycle-aggregate.ts",
    "pages/api/scalp/cron/research-cycle-sync-gates.ts",
    "pages/api/scalp/cron/research-preflight.ts",
    "pages/api/scalp/research/cycle.ts",
    "pages/api/scalp/cron/research-report.ts",
    "pages/api/scalp/research/report.ts",
  ];

  for (const relativePath of removedRoutes) {
    const absolutePath = path.join(root, relativePath);
    assert.equal(
      await exists(absolutePath),
      false,
      `expected removed route file to stay deleted: ${relativePath}`,
    );
  }
});
