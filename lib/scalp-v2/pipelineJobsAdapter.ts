import {
  fetchCapitalCandlesByEpicDateRange,
  resolveCapitalEpicRuntime,
} from "../capital";
import {
  loadScalpCandleHistoryInRange,
  mergeScalpCandleHistory,
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
  const lookbackDays = toPositiveInt(
    process.env.SCALP_V2_LOAD_CANDLES_LOOKBACK_DAYS,
    35,
    365,
  );
  const toTsMs = resolveLoadCandlesFetchUpperBoundMs(Date.now());
  const fromTsMs = Math.max(0, toTsMs - lookbackDays * ONE_DAY_MS);
  const selected = scopes.slice(offset, offset + batchSize);

  let processed = 0;
  let succeeded = 0;
  let failed = 0;
  const errors: Array<{ venue: string; symbol: string; message: string }> = [];

  for (const scope of selected) {
    processed += 1;
    try {
      let epic = scope.symbol;
      let incoming: Array<[number, number, number, number, number, number]> = [];
      if (scope.venue === "capital") {
        const resolved = await resolveCapitalEpicRuntime(scope.symbol);
        epic = String(resolved.epic || scope.symbol).trim().toUpperCase();
        incoming = (await fetchCapitalCandlesByEpicDateRange(
          epic,
          "1m",
          fromTsMs,
          toTsMs,
          {
            maxRequests: Math.max(40, maxAttempts * 30),
          },
        )) as Array<[number, number, number, number, number, number]>;
      } else {
        incoming = await fetchBitgetCandlesByEpicDateRange(
          scope.symbol,
          "1m",
          fromTsMs,
          toTsMs,
          {
            maxRequests: Math.max(60, maxAttempts * 80),
          },
        );
      }
      const existing = await loadScalpCandleHistoryInRange(
        scope.symbol,
        "1m",
        fromTsMs,
        toTsMs,
      ).catch(() => null);
      const merged = mergeScalpCandleHistory(
        existing?.record?.candles || [],
        incoming || [],
      );
      await saveScalpCandleHistory({
        symbol: scope.symbol,
        timeframe: "1m",
        epic,
        source: "bitget",
        candles: merged,
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
      maxAttempts,
      batchSize,
      offset,
      nextOffset,
      scopeCount: scopes.length,
      errors,
    },
  };
}
