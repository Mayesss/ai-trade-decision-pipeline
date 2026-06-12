#!/usr/bin/env node
import {
  applyScalpRegimeHysteresis,
  buildScalpRegimeClassifierValidityReport,
  classifyScalpRegimeRawRegimes,
  loadOrRefreshScalpRegimeWeeklyBars,
  loadScalpRegimeDeploymentSymbols,
  runScalpRegimeWeeklyRegimeBuild,
  SCALP_REGIME_CLASSIFIER_VERSION,
  upsertScalpRegimeSnapshots,
  type ScalpRegimeVenue,
} from "../lib/scalp/regimes";

function parseArgs(argv: string[]): Record<string, string | boolean> {
  const out: Record<string, string | boolean> = {};
  for (let i = 0; i < argv.length; i += 1) {
    const token = argv[i]!;
    if (!token.startsWith("--")) continue;
    const key = token.slice(2);
    const next = argv[i + 1];
    if (!next || next.startsWith("--")) out[key] = true;
    else {
      out[key] = next;
      i += 1;
    }
  }
  return out;
}

function csv(value: unknown): string[] {
  return String(value || "")
    .split(",")
    .map((row) => row.trim().toUpperCase())
    .filter(Boolean);
}

function venue(value: unknown): ScalpRegimeVenue {
  return String(value || "").toLowerCase() === "capital" ? "capital" : "bitget";
}

const WEEK_MS = 7 * 24 * 60 * 60_000;
const HISTORY_WEEKS = Math.max(64, Math.min(156, Math.floor(Number(process.env.SCALP_REGIME_INCREMENTAL_BOOTSTRAP_WEEKS || 72))));

async function loadWeekly(symbol: string, v: ScalpRegimeVenue, fromMs: number, toMs: number) {
  return loadOrRefreshScalpRegimeWeeklyBars({
    symbol,
    venue: v,
    fromMs,
    toMs,
    auditSource: "script_scalp_v4_build_regimes",
  }).catch(() => []);
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  const dryRun = Boolean(args.dryRun);
  const force = Boolean(args.force || args.forceValidity);
  const explicitVenues = args.venues || args.venue ? csv(args.venues || args.venue).map(venue) : null;
  const explicitSymbols = args.symbols ? csv(args.symbols) : null;
  const classifierVersion = String(args.classifierVersion || SCALP_REGIME_CLASSIFIER_VERSION);

  // Default path: auto-discover from deployments and apply.
  if (!explicitSymbols) {
    if (dryRun) {
      const targets = await loadScalpRegimeDeploymentSymbols();
      const filtered = explicitVenues ? targets.filter((row) => explicitVenues.includes(row.venue)) : targets;
      console.log(JSON.stringify({
        ok: true,
        dryRun: true,
        classifierVersion,
        symbolsRequested: filtered.length,
        targets: filtered,
      }, null, 2));
      return;
    }
    const targets = await loadScalpRegimeDeploymentSymbols();
    const filtered = explicitVenues ? targets.filter((row) => explicitVenues.includes(row.venue)) : targets;
    const result = await runScalpRegimeWeeklyRegimeBuild({
      symbols: filtered,
      classifierVersion,
      forceValidity: force,
    });
    if (result.validityFailures.length > 0 && !force) {
      console.log(JSON.stringify({ ...result, ok: false, reason: "classifier_validity_failed" }, null, 2));
      process.exit(1);
    }
    console.log(JSON.stringify({ ...result, ok: true }, null, 2));
    return;
  }

  // Explicit-symbols path retained for selective re-builds.
  const symbols = explicitSymbols;
  const venues = explicitVenues || ["bitget", "capital"];
  const toMs = Date.now();
  const fromMs = Math.max(0, toMs - HISTORY_WEEKS * WEEK_MS);
  const shared = {
    usdJpy: await loadWeekly("USDJPY", "capital", fromMs, toMs),
    audJpy: await loadWeekly("AUDJPY", "capital", fromMs, toMs),
    btcUsdt: await loadWeekly("BTCUSDT", "bitget", fromMs, toMs),
  };
  const results: unknown[] = [];
  const rowsToSave: ReturnType<typeof applyScalpRegimeHysteresis> = [];
  let saved = 0;
  for (const v of venues) {
    for (const symbol of symbols) {
      const weeklyBars = await loadWeekly(symbol, v, fromMs, toMs);
      if (!weeklyBars.length) {
        results.push({ venue: v, symbol, skipped: true, reason: "missing_symbol_history" });
        continue;
      }
      const raw = classifyScalpRegimeRawRegimes({
        venue: v,
        symbol,
        weeklyBars,
        marketContext: shared,
        options: { classifierVersion },
      });
      const snapshots = applyScalpRegimeHysteresis(raw);
      const report = buildScalpRegimeClassifierValidityReport({
        snapshots,
        marketBarsByName: {
          [symbol]: weeklyBars,
          BTCUSDT: shared.btcUsdt,
          USDJPY: shared.usdJpy,
          AUDJPY: shared.audJpy,
        },
      });
      rowsToSave.push(...snapshots);
      results.push({ venue: v, symbol, weeks: weeklyBars.length, snapshots: snapshots.length, validity: report });
    }
  }
  const invalid = results.filter((row) => row && typeof row === "object" && (row as any).validity?.passed === false);
  if (invalid.length > 0 && !force) {
    console.log(JSON.stringify({ ok: false, dryRun, classifierVersion, reason: "classifier_validity_failed", invalid, results }, null, 2));
    process.exit(1);
  }
  if (!dryRun) saved = await upsertScalpRegimeSnapshots(rowsToSave);
  console.log(JSON.stringify({ ok: true, dryRun, forced: force, classifierVersion, saved, results }, null, 2));
}

main().catch((err) => {
  console.error(err?.stack || err?.message || String(err));
  process.exit(1);
});
