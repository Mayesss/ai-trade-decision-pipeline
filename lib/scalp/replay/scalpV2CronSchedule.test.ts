import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import path from "node:path";
import test from "node:test";

test("scalp v2 cycle cron is scheduled", async () => {
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
    cronPaths.some((value) => value.startsWith("/api/scalp/v2/cron/cycle")),
    true,
  );
});
