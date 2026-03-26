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

test("scalp scheduling uses v2 namespace cron routes", async () => {
  const root = process.cwd();
  const expectedRouteFiles = [
    "pages/api/scalp/cron/discover-symbols.ts",
    "pages/api/scalp/cron/load-candles.ts",
    "pages/api/scalp/cron/prepare.ts",
    "pages/api/scalp/cron/v2/discover.ts",
    "pages/api/scalp/cron/v2/load-candles.ts",
    "pages/api/scalp/cron/v2/prepare.ts",
    "pages/api/scalp/cron/worker.ts",
    "pages/api/scalp/cron/promotion.ts",
  ];

  for (const relativePath of expectedRouteFiles) {
    const absolutePath = path.join(root, relativePath);
    assert.equal(
      await exists(absolutePath),
      true,
      `expected async job route file to exist: ${relativePath}`,
    );
  }

  const vercelConfigPath = path.join(root, "vercel.json");
  const vercelConfigRaw = await readFile(vercelConfigPath, "utf8");
  const vercelConfig = JSON.parse(vercelConfigRaw) as {
    crons?: Array<{ path?: string }>;
  };

  const cronPaths = (Array.isArray(vercelConfig.crons) ? vercelConfig.crons : [])
    .map((row) => String(row?.path || "").trim())
    .filter(Boolean);

  const requiredCronPrefixes = [
    "/api/scalp/v2/cron/discover",
    "/api/scalp/v2/cron/evaluate",
    "/api/scalp/v2/cron/promote",
    "/api/scalp/v2/cron/execute",
    "/api/scalp/v2/cron/reconcile",
    "/api/scalp/v2/cron/cycle",
  ];
  for (const prefix of requiredCronPrefixes) {
    assert.equal(
      cronPaths.some((value) => value.startsWith(prefix)),
      true,
      `expected vercel cron entry for: ${prefix}`,
    );
  }

  const forbiddenCronPrefixes = [
    "/api/scalp/cron/execute-deployments",
    "/api/scalp/cron/worker",
    "/api/scalp/cron/promotion",
    "/api/scalp/cron/v2/discover",
    "/api/scalp/cron/v2/load-candles",
    "/api/scalp/cron/v2/prepare",
    "/api/scalp/cron/discover-symbols",
    "/api/scalp/cron/load-candles",
    "/api/scalp/cron/prepare",
    "/api/scalp/cron/orchestrate-pipeline",
    "/api/scalp/cron/prepare-and-start-cycle",
    "/api/scalp/cron/research-cycle-start",
    "/api/scalp/cron/research-cycle-worker",
    "/api/scalp/cron/research-cycle-aggregate",
    "/api/scalp/cron/research-cycle-sync-gates",
    "/api/scalp/cron/research-preflight",
    "/api/scalp/cron/research-report",
  ];
  for (const prefix of forbiddenCronPrefixes) {
    assert.equal(
      cronPaths.some((value) => value.startsWith(prefix)),
      false,
      `unexpected legacy cycle cron entry: ${prefix}`,
    );
  }
});
