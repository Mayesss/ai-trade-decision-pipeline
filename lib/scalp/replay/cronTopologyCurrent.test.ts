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

  requirePathByPrefix(cronPaths, "/api/scalp/v2/cron/execute?dryRun=false");
  requirePathByPrefix(cronPaths, "/api/scalp/v2/cron/reconcile");
  requirePathByPrefix(cronPaths, "/api/scalp/v4/cron/build-regimes?liveOnly=true&forceValidity=true");
  requirePathByPrefix(cronPaths, "/api/scalp/v5/cron/load-live-candles");
  requirePathByPrefix(cronPaths, "/api/scalp/v5/cron/promote");

  const forbiddenPrefixes = [
    "/api/scalp/cron/execute-deployments",
    "/api/scalp/cron/worker",
    "/api/scalp/cron/promotion",
    "/api/scalp/v2/cron/cycle",
    "/api/scalp/v2/cron/discover",
    "/api/scalp/v2/cron/evaluate",
    "/api/scalp/v2/cron/worker",
    "/api/scalp/v2/cron/research",
    "/api/scalp/v2/cron/promote",
    "/api/scalp/v2/cron/load-candles",
    "/api/scalp/v5/cron/evaluate",
    "/api/scalp/v5/cron/preflight-candles",
    "/api/scalp/v5/cron/sunday-rollover",
    "/api/scalp/v5/cron/trim-tail",
    "/api/scalp/v5/cron/cull-bottom",
  ];
  for (const prefix of forbiddenPrefixes) {
    assert.equal(
      cronPaths.some((value) => value.startsWith(prefix)),
      false,
      `unexpected cron entry for: ${prefix}`,
    );
  }
});
