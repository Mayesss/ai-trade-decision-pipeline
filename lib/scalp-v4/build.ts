import { loadScalpCandleHistory } from "../scalp/candleHistory";
import {
  applyScalpV4Hysteresis,
  buildScalpV4WeeklyBars,
  classifyScalpV4RawRegimes,
  SCALP_V4_CLASSIFIER_VERSION,
} from "./classifier";
import {
  loadScalpV4DeploymentSymbols,
  loadScalpV4SymbolsWithSnapshotForWeek,
  upsertScalpV4RegimeSnapshots,
} from "./pg";
import { buildScalpV4ClassifierValidityReport } from "./sanity";
import type { ScalpV4MarketContext, ScalpV4Venue, ScalpV4WeeklyBar } from "./types";
import { startOfUtcWeekMondayMs } from "./week";

export interface ScalpV4WeeklyBuildResult {
  built: boolean;
  reason: string | null;
  classifierVersion: string;
  weekStartMs: number;
  symbolsRequested: number;
  symbolsClassified: number;
  symbolsSaved: number;
  validityFailures: Array<{ venue: ScalpV4Venue; symbol: string; reason: string | null; epochCount: number; cellCount: number }>;
  sharedCoverage: { usdJpyWeeks: number; audJpyWeeks: number; btcUsdtWeeks: number };
}

async function loadWeekly(symbol: string): Promise<ScalpV4WeeklyBar[]> {
  try {
    const history = await loadScalpCandleHistory(symbol, "1m");
    return buildScalpV4WeeklyBars(history.record?.candles || []);
  } catch {
    return [];
  }
}

export async function runScalpV4WeeklyRegimeBuild(params: {
  symbols?: Array<{ venue: ScalpV4Venue; symbol: string }>;
  classifierVersion?: string;
  forceValidity?: boolean;
}): Promise<ScalpV4WeeklyBuildResult> {
  const classifierVersion = params.classifierVersion || SCALP_V4_CLASSIFIER_VERSION;
  const targets = params.symbols && params.symbols.length > 0
    ? params.symbols
    : await loadScalpV4DeploymentSymbols();
  const weekStartMs = startOfUtcWeekMondayMs(Date.now());
  if (!targets.length) {
    return {
      built: false,
      reason: "no_deployment_symbols",
      classifierVersion,
      weekStartMs,
      symbolsRequested: 0,
      symbolsClassified: 0,
      symbolsSaved: 0,
      validityFailures: [],
      sharedCoverage: { usdJpyWeeks: 0, audJpyWeeks: 0, btcUsdtWeeks: 0 },
    };
  }
  const shared: ScalpV4MarketContext = {
    usdJpy: await loadWeekly("USDJPY"),
    audJpy: await loadWeekly("AUDJPY"),
    btcUsdt: await loadWeekly("BTCUSDT"),
  };
  const validityFailures: ScalpV4WeeklyBuildResult["validityFailures"] = [];
  const allSnapshots = [];
  let classified = 0;
  for (const target of targets) {
    const weeklyBars = await loadWeekly(target.symbol);
    if (!weeklyBars.length) continue;
    const raw = classifyScalpV4RawRegimes({
      venue: target.venue,
      symbol: target.symbol,
      weeklyBars,
      marketContext: shared,
      options: { classifierVersion },
    });
    const snapshots = applyScalpV4Hysteresis(raw);
    const validity = buildScalpV4ClassifierValidityReport({
      snapshots,
      marketBarsByName: {
        [target.symbol]: weeklyBars,
        BTCUSDT: shared.btcUsdt || [],
        USDJPY: shared.usdJpy || [],
        AUDJPY: shared.audJpy || [],
      },
    });
    classified += 1;
    if (!validity.passed && !params.forceValidity) {
      validityFailures.push({
        venue: target.venue,
        symbol: target.symbol,
        reason: validity.reason,
        epochCount: validity.epochCount,
        cellCount: validity.cellCount,
      });
      continue;
    }
    allSnapshots.push(...snapshots);
  }
  const saved = allSnapshots.length > 0 ? await upsertScalpV4RegimeSnapshots(allSnapshots) : 0;
  return {
    built: saved > 0,
    reason: validityFailures.length > 0 && saved === 0 ? "all_symbols_failed_validity" : null,
    classifierVersion,
    weekStartMs,
    symbolsRequested: targets.length,
    symbolsClassified: classified,
    symbolsSaved: saved,
    validityFailures,
    sharedCoverage: {
      usdJpyWeeks: (shared.usdJpy || []).length,
      audJpyWeeks: (shared.audJpy || []).length,
      btcUsdtWeeks: (shared.btcUsdt || []).length,
    },
  };
}

export async function ensureScalpV4WeeklyRegimesBuilt(params: {
  classifierVersion?: string;
  forceValidity?: boolean;
} = {}): Promise<{ skipped: boolean; reason: string; result?: ScalpV4WeeklyBuildResult }> {
  const classifierVersion = params.classifierVersion || SCALP_V4_CLASSIFIER_VERSION;
  const weekStartMs = startOfUtcWeekMondayMs(Date.now());
  const targets = await loadScalpV4DeploymentSymbols();
  if (!targets.length) return { skipped: true, reason: "no_deployment_symbols" };
  const present = await loadScalpV4SymbolsWithSnapshotForWeek({ classifierVersion, weekStartMs });
  const missing = targets.filter((row) => !present.has(`${row.venue}:${row.symbol}`));
  if (missing.length === 0) {
    return { skipped: true, reason: "regimes_already_built_for_week" };
  }
  const result = await runScalpV4WeeklyRegimeBuild({
    symbols: missing,
    classifierVersion,
    forceValidity: params.forceValidity,
  });
  return { skipped: false, reason: "built", result };
}
