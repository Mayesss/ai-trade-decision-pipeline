import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import path from "node:path";
import test from "node:test";

test("vercel scalp cron scheduling is v2 split-jobs without cycle cron", async () => {
  const root = process.cwd();
  const vercelConfigPath = path.join(root, "vercel.json");
  const vercelConfigRaw = await readFile(vercelConfigPath, "utf8");
  const vercelConfig = JSON.parse(vercelConfigRaw) as {
    crons?: Array<{ path?: string }>;
  };

  const cronPaths = (Array.isArray(vercelConfig.crons) ? vercelConfig.crons : [])
    .map((row) => String(row?.path || "").trim())
    .filter(Boolean);

  assert.equal(
    cronPaths.some((row) => row.startsWith("/api/scalp/v2/cron/execute?dryRun=false")),
    true,
  );
  assert.equal(
    cronPaths.some((row) => row.startsWith("/api/scalp/v2/cron/worker")),
    true,
  );
  assert.equal(
    cronPaths.some((row) => row.startsWith("/api/scalp/v2/cron/cycle")),
    false,
  );
  assert.equal(
    cronPaths.some((row) => row.startsWith("/api/scalp/cron/execute-deployments")),
    false,
  );
  assert.equal(
    cronPaths.some((row) => row.startsWith("/api/scalp/cron/worker")),
    false,
  );
  assert.equal(
    cronPaths.some((row) => row.startsWith("/api/scalp/cron/promotion")),
    false,
  );
});
