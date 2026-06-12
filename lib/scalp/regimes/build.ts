import { loadScalpCandleHistoryWeeklyBars } from "../candleHistory";
import {
  applyScalpV4Hysteresis,
  buildScalpV4WeeklyBars,
  classifyScalpV4RawRegimes,
  SCALP_V4_CLASSIFIER_VERSION,
} from "./classifier";
import {
  backfillScalpV4WeeklyBarsFromCandleHistory,
  loadScalpV4WeeklyBars,
  loadScalpV4DeploymentSymbols,
  loadScalpV4SymbolsWithSnapshotForWeek,
  upsertScalpV4WeeklyBars,
  upsertScalpV4RegimeSnapshots,
} from "./pg";
import { buildScalpV4ClassifierValidityReport } from "./sanity";
import type { ScalpV4MarketContext, ScalpV4Venue, ScalpV4WeeklyBar } from "./types";
import { SCALP_V4_ONE_WEEK_MS, startOfUtcWeekMondayMs } from "./week";

const V4_INCREMENTAL_BOOTSTRAP_WEEKS = Math.max(
  64,
  Math.min(156, Math.floor(Number(process.env.SCALP_V4_INCREMENTAL_BOOTSTRAP_WEEKS || 72))),
);

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

export async function loadOrRefreshScalpV4WeeklyBars(params: {
  symbol: string;
  venue: ScalpV4Venue;
  fromMs: number;
  toMs: number;
  classificationWeekStartMs?: number;
  auditSource?: string;
}): Promise<ScalpV4WeeklyBar[]> {
  const classificationWeekStartMs = params.classificationWeekStartMs ?? startOfUtcWeekMondayMs(params.toMs);
  const recentFromMs = Math.max(0, classificationWeekStartMs - SCALP_V4_ONE_WEEK_MS);
  try {
    const recentHistory = await loadScalpCandleHistoryWeeklyBars(
      params.symbol,
      "1m",
      recentFromMs,
      params.toMs,
      {
        venue: params.venue,
        maxBrokerRangeDays: 15,
        requireCoverageRatio: 0.5,
        auditSource: params.auditSource || "v4_regime_build_recent",
      },
    );
    const recentBars = buildScalpV4WeeklyBars(recentHistory.record?.candles || []);
    if (recentBars.length > 0) {
      await upsertScalpV4WeeklyBars({
        venue: params.venue,
        symbol: params.symbol,
        bars: recentBars,
        source: recentHistory.diagnostics?.source || "recent_candles",
      }).catch(() => 0);
    }
  } catch {
    // A broker/KV/PG recent refresh failure should not block reading compact bars.
  }

  const toWeekMs = startOfUtcWeekMondayMs(params.toMs) + SCALP_V4_ONE_WEEK_MS;
  let compactBars = await loadScalpV4WeeklyBars({
    venue: params.venue,
    symbol: params.symbol,
    fromMs: params.fromMs,
    toMs: toWeekMs,
  }).catch(() => []);

  if (compactBars.length < V4_INCREMENTAL_BOOTSTRAP_WEEKS) {
    await backfillScalpV4WeeklyBarsFromCandleHistory({
      venue: params.venue,
      symbol: params.symbol,
      fromMs: params.fromMs,
      toMs: toWeekMs,
    }).catch(() => 0);
    compactBars = await loadScalpV4WeeklyBars({
      venue: params.venue,
      symbol: params.symbol,
      fromMs: params.fromMs,
      toMs: toWeekMs,
    }).catch(() => compactBars);
  }

  return compactBars
    .slice()
    .sort((a, b) => a.weekStartMs - b.weekStartMs)
    .filter((row) => row.weekStartMs < classificationWeekStartMs);
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
  const fromMs = Math.max(0, weekStartMs - V4_INCREMENTAL_BOOTSTRAP_WEEKS * 7 * 24 * 60 * 60_000);
  const toMs = Date.now();
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
    usdJpy: await loadOrRefreshScalpV4WeeklyBars({ symbol: "USDJPY", venue: "capital", fromMs, toMs, classificationWeekStartMs: weekStartMs }),
    audJpy: await loadOrRefreshScalpV4WeeklyBars({ symbol: "AUDJPY", venue: "capital", fromMs, toMs, classificationWeekStartMs: weekStartMs }),
    btcUsdt: await loadOrRefreshScalpV4WeeklyBars({ symbol: "BTCUSDT", venue: "bitget", fromMs, toMs, classificationWeekStartMs: weekStartMs }),
  };
  const validityFailures: ScalpV4WeeklyBuildResult["validityFailures"] = [];
  const allSnapshots = [];
  let classified = 0;
  for (const target of targets) {
    const weeklyBars = await loadOrRefreshScalpV4WeeklyBars({
      symbol: target.symbol,
      venue: target.venue,
      fromMs,
      toMs,
      classificationWeekStartMs: weekStartMs,
    });
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
    const currentSnapshot = snapshots.find((row) => row.weekStartMs === weekStartMs);
    if (currentSnapshot) allSnapshots.push(currentSnapshot);
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
