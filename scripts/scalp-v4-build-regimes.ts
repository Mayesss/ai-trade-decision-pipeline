#!/usr/bin/env node
import {
  applyScalpV4Hysteresis,
  buildScalpV4ClassifierValidityReport,
  buildScalpV4WeeklyBars,
  classifyScalpV4RawRegimes,
  loadScalpV4DeploymentSymbols,
  runScalpV4WeeklyRegimeBuild,
  SCALP_V4_CLASSIFIER_VERSION,
  upsertScalpV4RegimeSnapshots,
  type ScalpV4Venue,
} from "../lib/scalp-v4";
import { loadScalpCandleHistory } from "../lib/scalp/candleHistory";

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

function venue(value: unknown): ScalpV4Venue {
  return String(value || "").toLowerCase() === "capital" ? "capital" : "bitget";
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  const dryRun = Boolean(args.dryRun);
  const force = Boolean(args.force || args.forceValidity);
  const explicitVenues = args.venues || args.venue ? csv(args.venues || args.venue).map(venue) : null;
  const explicitSymbols = args.symbols ? csv(args.symbols) : null;
  const classifierVersion = String(args.classifierVersion || SCALP_V4_CLASSIFIER_VERSION);

  // Default path: auto-discover from deployments and apply.
  if (!explicitSymbols) {
    if (dryRun) {
      const targets = await loadScalpV4DeploymentSymbols();
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
    const targets = await loadScalpV4DeploymentSymbols();
    const filtered = explicitVenues ? targets.filter((row) => explicitVenues.includes(row.venue)) : targets;
    const result = await runScalpV4WeeklyRegimeBuild({
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
  const shared = {
    usdJpy: await loadScalpCandleHistory("USDJPY", "1m").then((row) => buildScalpV4WeeklyBars(row.record?.candles || [])).catch(() => []),
    audJpy: await loadScalpCandleHistory("AUDJPY", "1m").then((row) => buildScalpV4WeeklyBars(row.record?.candles || [])).catch(() => []),
    btcUsdt: await loadScalpCandleHistory("BTCUSDT", "1m").then((row) => buildScalpV4WeeklyBars(row.record?.candles || [])).catch(() => []),
  };
  const results: unknown[] = [];
  const rowsToSave: ReturnType<typeof applyScalpV4Hysteresis> = [];
  let saved = 0;
  for (const v of venues) {
    for (const symbol of symbols) {
      const weeklyBars = await loadScalpCandleHistory(symbol, "1m").then((row) => buildScalpV4WeeklyBars(row.record?.candles || [])).catch(() => []);
      if (!weeklyBars.length) {
        results.push({ venue: v, symbol, skipped: true, reason: "missing_symbol_history" });
        continue;
      }
      const raw = classifyScalpV4RawRegimes({
        venue: v,
        symbol,
        weeklyBars,
        marketContext: shared,
        options: { classifierVersion },
      });
      const snapshots = applyScalpV4Hysteresis(raw);
      const report = buildScalpV4ClassifierValidityReport({
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
  if (!dryRun) saved = await upsertScalpV4RegimeSnapshots(rowsToSave);
  console.log(JSON.stringify({ ok: true, dryRun, forced: force, classifierVersion, saved, results }, null, 2));
}

main().catch((err) => {
  console.error(err?.stack || err?.message || String(err));
  process.exit(1);
});
