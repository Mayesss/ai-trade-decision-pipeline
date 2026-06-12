#!/usr/bin/env node

import nextEnv from "@next/env";

import {
  runDayRobustnessBatch,
  resolveDayRobustnessPolicy,
} from "../lib/scalp/composer/dayRobustness";
import { resolveScalpV2CompletedWeekWindowToUtc } from "../lib/scalp/composer/weekWindows";

const { loadEnvConfig } = nextEnv;
loadEnvConfig(process.cwd());

if (process.env.SCALP_PG_USE_HTTP === undefined) {
  process.env.SCALP_PG_USE_HTTP = "1";
}

function envBool(name: string, fallback: boolean): boolean {
  const raw = String(process.env[name] || "").trim().toLowerCase();
  if (!raw) return fallback;
  if (["1", "true", "yes", "on"].includes(raw)) return true;
  if (["0", "false", "no", "off"].includes(raw)) return false;
  return fallback;
}

function envInt(name: string, fallback: number, min: number, max: number): number {
  const raw = String(process.env[name] || "").trim();
  if (!raw) return fallback;
  const n = Math.floor(Number(raw));
  if (!Number.isFinite(n)) return fallback;
  return Math.max(min, Math.min(max, n));
}

const policy = resolveDayRobustnessPolicy();
const limit = envInt("DAY_ROBUSTNESS_BATCH_SIZE", policy.maxCandidates, 1, 5_000);
const maxBatches = envInt("DAY_ROBUSTNESS_MAX_BATCHES", 1, 1, 10_000);
const dryRun = envBool("DAY_ROBUSTNESS_DRY_RUN", false);
const extended = envBool("DAY_ROBUSTNESS_EXTENDED", false);
const includeFailed = envBool("DAY_ROBUSTNESS_INCLUDE_FAILED", false);
const windowToTs = resolveScalpV2CompletedWeekWindowToUtc(Date.now());
const owner = `day-robustness-local:${process.pid}:${Date.now().toString(36)}`;

console.log("Day composer finalist robustness runner");
console.log(`windowToTs=${new Date(windowToTs).toISOString()} weeks=${extended ? policy.extendedWeeks : policy.weeks} batchSize=${limit} maxBatches=${maxBatches} dryRun=${dryRun}`);
console.log(`readOrder=${process.env.SCALP_DAY_ROBUSTNESS_READ_ORDER || "pg"} owner=${owner}`);

let totalSelected = 0;
let totalProcessed = 0;
let totalPassed = 0;
let totalFailed = 0;
let totalErrors = 0;

for (let batch = 1; batch <= maxBatches; batch += 1) {
  const startedAt = Date.now();
  console.log(`\n--- Robustness batch ${batch} ---`);
  const result = await runDayRobustnessBatch({
    windowToTs,
    limit,
    dryRun,
    extended,
    includeFailed,
    lockOwner: owner,
    onProgress(event) {
      const phase = String(event.phase || "");
      if (phase === "candidate_done" || phase === "candidate_error") {
        const processed = Number(event.processed || 0);
        const total = Number(event.total || 0);
        const label = phase === "candidate_done"
          ? `${event.symbol} passed=${event.passed} trades=${event.trades} netR=${Number(event.netR || 0).toFixed(2)}`
          : `${event.symbol} error=${event.error}`;
        console.log(`  ${processed}/${total} ${label}`);
      }
    },
  });
  totalSelected += result.selected;
  totalProcessed += result.processed;
  totalPassed += result.passed;
  totalFailed += result.failed;
  totalErrors += result.errors;
  console.log(
    `batch done in ${((Date.now() - startedAt) / 1000).toFixed(1)}s selected=${result.selected} processed=${result.processed} passed=${result.passed} failed=${result.failed} errors=${result.errors}`,
  );
  if (result.selected === 0) break;
}

console.log("\nRobustness summary");
console.log(JSON.stringify({
  selected: totalSelected,
  processed: totalProcessed,
  passed: totalPassed,
  failed: totalFailed,
  errors: totalErrors,
  dryRun,
  extended,
}, null, 2));
