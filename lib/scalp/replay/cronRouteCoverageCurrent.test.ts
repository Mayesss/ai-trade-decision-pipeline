import assert from "node:assert/strict";
import { access, readFile } from "node:fs/promises";
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
    "pages/api/scalp/composer/cron/cycle.ts",
    "pages/api/scalp/composer/cron/execute.ts",
    "pages/api/scalp/composer/cron/reconcile.ts",
    "pages/api/scalp/composer/cron/load-candles.ts",
    "pages/api/scalp/composer/cron/research.ts",
    "pages/api/scalp/composer/cron/promote.ts",
    "pages/api/scalp/composer/cron/discover.ts",
    "pages/api/scalp/composer/cron/evaluate.ts",
    "pages/api/scalp/composer/cron/worker.ts",
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

test("production cron paths avoid direct full candle-history reads", async () => {
  const root = process.cwd();
  const checkedFiles = [
    "pages/api/scalp/composer/cron/execute.ts",
    "lib/scalp/composer/pipeline.ts",
  ];

  for (const relativePath of checkedFiles) {
    const source = await readFile(path.join(root, relativePath), "utf8");
    assert.equal(
      source.includes("loadScalpCandleHistory("),
      false,
      `expected ${relativePath} to use range/tail/stats helpers instead of full candle history`,
    );
  }
});
