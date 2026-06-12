import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import path from "node:path";
import test from "node:test";

type VercelCron = { path?: string; schedule?: string };

async function loadCronPaths(): Promise<string[]> {
  const root = process.cwd();
  const vercelConfigPath = path.join(root, "vercel.json");
  const raw = await readFile(vercelConfigPath, "utf8");
  const parsed = JSON.parse(raw) as { crons?: VercelCron[] };
  return (Array.isArray(parsed.crons) ? parsed.crons : [])
    .map((row) => String(row?.path || "").trim())
    .filter(Boolean);
}

function requirePathByPrefix(paths: string[], prefix: string): string {
  const row = paths.find((value) => value.startsWith(prefix));
  assert.ok(row, `expected vercel cron entry for: ${prefix}`);
  return row;
}

test("vercel scalp cron topology keeps only live and lightweight maintenance crons", async () => {
  const cronPaths = await loadCronPaths();

  requirePathByPrefix(cronPaths, "/api/scalp/composer/cron/execute?dryRun=false");
  requirePathByPrefix(cronPaths, "/api/scalp/composer/cron/reconcile");
  requirePathByPrefix(cronPaths, "/api/scalp/regimes/cron/build-regimes?liveOnly=true&forceValidity=true");
  requirePathByPrefix(cronPaths, "/api/scalp/research/cron/load-live-candles");
  requirePathByPrefix(cronPaths, "/api/scalp/research/cron/promote");

  const forbiddenPrefixes = [
    "/api/scalp/cron/execute-deployments",
    "/api/scalp/cron/worker",
    "/api/scalp/cron/promotion",
    "/api/scalp/composer/cron/cycle",
    "/api/scalp/composer/cron/discover",
    "/api/scalp/composer/cron/evaluate",
    "/api/scalp/composer/cron/worker",
    "/api/scalp/composer/cron/research",
    "/api/scalp/composer/cron/promote",
    "/api/scalp/composer/cron/load-candles",
    "/api/scalp/research/cron/evaluate",
    "/api/scalp/research/cron/preflight-candles",
    "/api/scalp/research/cron/sunday-rollover",
    "/api/scalp/research/cron/trim-tail",
    "/api/scalp/research/cron/cull-bottom",
  ];
  for (const prefix of forbiddenPrefixes) {
    assert.equal(
      cronPaths.some((value) => value.startsWith(prefix)),
      false,
      `unexpected cron entry for: ${prefix}`,
    );
  }
});
