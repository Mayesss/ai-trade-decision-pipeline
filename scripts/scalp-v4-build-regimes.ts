#!/usr/bin/env node
import {
  applyScalpV4Hysteresis,
  buildScalpV4ClassifierValidityReport,
  buildScalpV4WeeklyBars,
  classifyScalpV4RawRegimes,
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

async function loadWeekly(symbol: string) {
  const history = await loadScalpCandleHistory(symbol, "1m");
  return buildScalpV4WeeklyBars(history.record?.candles || []);
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  const apply = Boolean(args.apply);
  const force = Boolean(args.force);
  const venues = csv(args.venues || args.venue || "capital,bitget").map(venue);
  const symbols = csv(args.symbols || "EURUSD,BTCUSDT");
  const classifierVersion = String(args.classifierVersion || SCALP_V4_CLASSIFIER_VERSION);
  const shared = {
    usdJpy: await loadWeekly("USDJPY").catch(() => []),
    audJpy: await loadWeekly("AUDJPY").catch(() => []),
    btcUsdt: await loadWeekly("BTCUSDT").catch(() => []),
  };
  const results: unknown[] = [];
  const rowsToSave: ReturnType<typeof applyScalpV4Hysteresis> = [];
  let saved = 0;
  for (const v of venues) {
    for (const symbol of symbols) {
      const weeklyBars = await loadWeekly(symbol).catch(() => []);
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
      results.push({
        venue: v,
        symbol,
        weeks: weeklyBars.length,
        snapshots: snapshots.length,
        saved: 0,
        validity: report,
      });
    }
  }
  const invalid = results.filter((row) => {
    const validity = row && typeof row === "object" ? (row as any).validity : null;
    return validity && validity.passed === false;
  });
  if (invalid.length > 0 && !force) {
    console.log(JSON.stringify({
      ok: false,
      dryRun: !apply,
      classifierVersion,
      saved,
      sharedCoverage: {
        usdJpyWeeks: shared.usdJpy.length,
        audJpyWeeks: shared.audJpy.length,
        btcUsdtWeeks: shared.btcUsdt.length,
      },
      reason: "classifier_validity_failed",
      invalid,
      results,
    }, null, 2));
    process.exit(1);
  }
  if (apply) saved = await upsertScalpV4RegimeSnapshots(rowsToSave);
  console.log(JSON.stringify({
    ok: true,
    dryRun: !apply,
    forced: force,
    classifierVersion,
    saved,
    sharedCoverage: {
      usdJpyWeeks: shared.usdJpy.length,
      audJpyWeeks: shared.audJpy.length,
      btcUsdtWeeks: shared.btcUsdt.length,
    },
    results,
  }, null, 2));
}

main().catch((err) => {
  console.error(err?.stack || err?.message || String(err));
  process.exit(1);
});
