import {
  fetchCapitalCandlesByEpicDateRange,
  resolveCapitalEpicRuntime,
} from "../capital";
import {
  loadScalpCandleHistoryStatsBulk,
  saveScalpCandleHistory,
} from "../scalp/candleHistory";
import { fetchBitgetCandlesByEpicDateRange } from "../scalp/bitgetHistory";

import type { ScalpV2Venue } from "./types";

const ONE_DAY_MS = 24 * 60 * 60 * 1000;

function normalizeSymbol(value: unknown): string {
  return String(value || "")
    .trim()
    .toUpperCase()
    .replace(/[^A-Z0-9._-]/g, "");
}

function toPositiveInt(value: unknown, fallback: number, max: number): number {
  const n = Math.floor(Number(value));
  if (!Number.isFinite(n) || n <= 0) return fallback;
  return Math.max(1, Math.min(max, n));
}

function resolveLoadCandlesFetchUpperBoundMs(nowMs: number): number {
  const safeNowMs = Math.max(0, Math.floor(Number(nowMs) || 0));
  const date = new Date(safeNowMs);
  const dayStartMs = Date.UTC(
    date.getUTCFullYear(),
    date.getUTCMonth(),
    date.getUTCDate(),
  );
  if (new Date(dayStartMs).getUTCDay() !== 0) return safeNowMs;
  return Math.max(0, dayStartMs - 1);
}

type ScalpV2LoadCandlesScope = {
  venue: ScalpV2Venue;
  symbol: string;
};

type ScalpV2LoadCandlesResult = {
  ok: boolean;
  busy: boolean;
  jobKind: "load_candles";
  processed: number;
  succeeded: number;
  retried: number;
  failed: number;
  pendingAfter: number;
  downstreamRequested: boolean;
  progressLabel: string;
  details: Record<string, unknown>;
};

function normalizeScopes(params: {
  scopes?: ScalpV2LoadCandlesScope[];
  symbols?: string[];
}): ScalpV2LoadCandlesScope[] {
  const out = new Map<string, ScalpV2LoadCandlesScope>();
  for (const scope of params.scopes || []) {
    const venue = scope.venue === "capital" ? "capital" : "bitget";
    const symbol = normalizeSymbol(scope.symbol);
    if (!symbol) continue;
    out.set(`${venue}:${symbol}`, { venue, symbol });
  }
  for (const symbolRaw of params.symbols || []) {
    const symbol = normalizeSymbol(symbolRaw);
    if (!symbol) continue;
    out.set(`bitget:${symbol}`, { venue: "bitget", symbol });
  }
  return Array.from(out.values());
}

// V2-native load-candles job used by /api/scalp/v2/cron/load-candles.
export async function runScalpV2LoadCandlesPipelineJob(params: {
  batchSize?: number;
  maxAttempts?: number;
  offset?: number;
  scopes?: ScalpV2LoadCandlesScope[];
  symbols?: string[];
}): Promise<ScalpV2LoadCandlesResult> {
  const scopes = normalizeScopes(params);
  const batchSize = toPositiveInt(params.batchSize, 6, 200);
  const maxAttempts = toPositiveInt(params.maxAttempts, 5, 30);
  const offset = Math.max(
    0,
    Math.min(
      scopes.length,
      Math.floor(Number(params.offset) || 0),
    ),
  );
  // Stage C evaluates 12 completed weeks; cold-start candle bootstraps
  // should cover at least that horizon plus one buffer week.
  const minColdStartLookbackDays = 14 * 7;
  const lookbackDays = toPositiveInt(
    process.env.SCALP_V2_LOAD_CANDLES_LOOKBACK_DAYS,
    minColdStartLookbackDays,
    365,
  );
  const fetchWindowMinutes = toPositiveInt(
    process.env.SCALP_V2_LOAD_CANDLES_FETCH_WINDOW_MINUTES,
    360,
    525_600,
  );
  const incrementalOverlapMinutes = toPositiveInt(
    process.env.SCALP_V2_LOAD_CANDLES_INCREMENTAL_OVERLAP_MINUTES,
    180,
    10_080,
  );
  const incrementalOverlapMs = incrementalOverlapMinutes * 60 * 1000;
  const toTsMs = resolveLoadCandlesFetchUpperBoundMs(Date.now());
  const fromTsMs = Math.max(0, toTsMs - lookbackDays * ONE_DAY_MS);
  const selected = scopes.slice(offset, offset + batchSize);

  let processed = 0;
  let succeeded = 0;
  let failed = 0;
  const errors: Array<{
    venue: string;
    symbol: string;
    message: string;
    fetchFromTsMs?: number;
    fetchToTsMs?: number;
    existingLatestTsMs?: number | null;
  }> = [];
  const perScope: Array<{
    venue: string;
    symbol: string;
    fetchFromTsMs: number;
    fetchToTsMs: number;
    coldStartBootstrap: boolean;
    existingCount: number;
    incomingCount: number;
    mergedCount: number;
    existingLatestTsMs: number | null;
  }> = [];
  const statsRows = await loadScalpCandleHistoryStatsBulk(
    Array.from(new Set(selected.map((scope) => scope.symbol))),
    "1m",
  ).catch(() => []);
  const statsBySymbol = new Map<
    string,
    { toTsMs: number | null; candleCount: number }
  >();
  for (const row of statsRows || []) {
    const symbol = normalizeSymbol(row.symbol);
    if (!symbol) continue;
    statsBySymbol.set(symbol, {
      toTsMs: Number.isFinite(Number(row.toTsMs))
        ? Math.floor(Number(row.toTsMs))
        : null,
      candleCount: Math.max(0, Math.floor(Number(row.candleCount) || 0)),
    });
  }

  for (const scope of selected) {
    processed += 1;
    try {
      const stats = statsBySymbol.get(scope.symbol);
      const existingCount = Math.max(
        0,
        Math.floor(Number(stats?.candleCount) || 0),
      );
      const existingLatestTsMsRaw = Number(stats?.toTsMs || 0);
      const existingLatestTsMs =
        Number.isFinite(existingLatestTsMsRaw) && existingLatestTsMsRaw > 0
          ? Math.floor(existingLatestTsMsRaw)
          : null;
      const coldStartBootstrap = !existingLatestTsMs || existingCount <= 0;
      const fetchWindowFromTsMs = Math.max(
        fromTsMs,
        toTsMs - fetchWindowMinutes * 60 * 1000,
      );
      // Cold-start scopes need a full lookback bootstrap; incremental windows
      // are used only after at least one valid latest candle is present.
      const fetchFromTsMs = coldStartBootstrap
        ? fromTsMs
        : Math.max(fetchWindowFromTsMs, existingLatestTsMs - incrementalOverlapMs);
      const fetchToTsMs = toTsMs;
      let epic = scope.symbol;
      let incoming: Array<[number, number, number, number, number, number]> = [];
      if (fetchToTsMs > fetchFromTsMs) {
        if (scope.venue === "capital") {
          const resolved = await resolveCapitalEpicRuntime(scope.symbol);
          epic = String(resolved.epic || scope.symbol).trim().toUpperCase();
          incoming = (await fetchCapitalCandlesByEpicDateRange(
            epic,
            "1m",
            fetchFromTsMs,
            fetchToTsMs,
            {
              maxRequests: Math.max(40, maxAttempts * 30),
            },
          )) as Array<[number, number, number, number, number, number]>;
        } else {
          incoming = await fetchBitgetCandlesByEpicDateRange(
            scope.symbol,
            "1m",
            fetchFromTsMs,
            fetchToTsMs,
            {
              maxRequests: Math.max(60, maxAttempts * 80),
            },
          );
        }
      }
      if (incoming.length > 0) {
        await saveScalpCandleHistory({
          symbol: scope.symbol,
          timeframe: "1m",
          epic,
          source: "bitget",
          candles: incoming,
        });
      }
      perScope.push({
        venue: scope.venue,
        symbol: scope.symbol,
        fetchFromTsMs,
        fetchToTsMs,
        coldStartBootstrap,
        existingCount,
        incomingCount: incoming.length,
        mergedCount: incoming.length,
        existingLatestTsMs,
      });
      succeeded += 1;
    } catch (err: any) {
      failed += 1;
      errors.push({
        venue: scope.venue,
        symbol: scope.symbol,
        message: err?.message || String(err),
      });
    }
  }

  const nextOffset = offset + processed;
  return {
    ok: failed <= 0,
    busy: false,
    jobKind: "load_candles",
    processed,
    succeeded,
    retried: 0,
    failed,
    pendingAfter: Math.max(0, scopes.length - nextOffset),
    downstreamRequested: false,
    progressLabel: `v2_native_load_candles:${succeeded}/${processed}`,
    details: {
      lookbackDays,
      timeframe: "1m",
      fromTsMs,
      toTsMs,
      fetchWindowMinutes,
      incrementalOverlapMinutes,
      maxAttempts,
      batchSize,
      offset,
      nextOffset,
      scopeCount: scopes.length,
      perScope,
      errors,
    },
  };
}
