import {
  fetchCapitalCandlesByEpicDateRange,
  resolveCapitalEpicRuntime,
} from "../capital";
import { fetchBitgetCandlesByEpicDateRange } from "../scalp/bitgetHistory";
import {
  loadScalpCandleHistoryInRange,
  saveScalpCandleHistory,
} from "../scalp/candleHistory";
import { ensureScalpSymbolMarketMetadata } from "../scalp/symbolMarketMetadataSync";
import type { ScalpCandle } from "../scalp/types";
import type { ScalpV4Venue } from "./types";

const ONE_MINUTE_MS = 60_000;
const WEEK = 7 * 24 * 60 * 60 * 1000;

export interface ScalpV4CandleCoverageStatus {
  ok: boolean;
  reason: string | null;
  candleCount: number;
  expectedCandles: number;
  coverageRatio: number;
  firstTsMs: number | null;
  lastTsMs: number | null;
}

export interface ScalpV4CandleCoverageEnsureResult {
  candles: ScalpCandle[];
  coverage: ScalpV4CandleCoverageStatus;
  fetchedCandles: number;
  savedChunks: number;
  attempted: boolean;
  error: string | null;
}

function normalizeFetchedCandles(rows: unknown[]): ScalpCandle[] {
  return (rows || [])
    .map((row) => {
      const value = Array.isArray(row) ? row : [];
      const ts = Number(value[0]);
      const open = Number(value[1]);
      const high = Number(value[2]);
      const low = Number(value[3]);
      const close = Number(value[4]);
      const volume = Number(value[5] ?? 0);
      if (![ts, open, high, low, close].every((n) => Number.isFinite(n) && n > 0)) {
        return null;
      }
      return [
        Math.floor(ts),
        open,
        high,
        low,
        close,
        Number.isFinite(volume) ? volume : 0,
      ] as ScalpCandle;
    })
    .filter((row): row is ScalpCandle => Boolean(row))
    .sort((a, b) => a[0] - b[0]);
}

export function assessScalpV4CandleCoverage(params: {
  candles: ScalpCandle[];
  fromMs: number;
  toMs: number;
  minCoverageRatio: number;
}): ScalpV4CandleCoverageStatus {
  const fromMs = Math.max(0, Math.floor(Number(params.fromMs) || 0));
  const toMs = Math.max(fromMs + ONE_MINUTE_MS, Math.floor(Number(params.toMs) || 0));
  const candles = (params.candles || [])
    .filter((row) => {
      const ts = Number(row?.[0] || 0);
      return Number.isFinite(ts) && ts >= fromMs && ts < toMs;
    })
    .sort((a, b) => a[0] - b[0]);
  const expectedCandles = Math.max(1, Math.floor((toMs - fromMs) / ONE_MINUTE_MS));
  const candleCount = candles.length;
  const coverageRatio = candleCount / expectedCandles;
  const firstTsMs = candleCount > 0 ? Number(candles[0]?.[0]) : null;
  const lastTsMs = candleCount > 0 ? Number(candles[candleCount - 1]?.[0]) : null;
  const minCoverageRatio = Math.max(0.1, Math.min(1, Number(params.minCoverageRatio) || 0.65));
  const edgeToleranceMs = WEEK;
  const startsNearWindow = firstTsMs !== null && firstTsMs <= fromMs + edgeToleranceMs;
  const endsNearWindow = lastTsMs !== null && lastTsMs >= toMs - edgeToleranceMs;
  const ok = candleCount > 0 && coverageRatio >= minCoverageRatio && startsNearWindow && endsNearWindow;
  const reason = ok
    ? null
    : candleCount <= 0
      ? "missing_candles"
      : !startsNearWindow
        ? "missing_window_start"
        : !endsNearWindow
          ? "missing_window_end"
          : "insufficient_candle_coverage";
  return {
    ok,
    reason,
    candleCount,
    expectedCandles,
    coverageRatio,
    firstTsMs,
    lastTsMs,
  };
}

async function fetchRange(params: {
  venue: ScalpV4Venue;
  symbol: string;
  fromMs: number;
  toMs: number;
  maxRequests: number;
}): Promise<{ epic: string; candles: ScalpCandle[] }> {
  if (params.venue === "capital") {
    const resolved = await resolveCapitalEpicRuntime(params.symbol);
    const epic = String(resolved.epic || params.symbol).trim().toUpperCase();
    const raw = await fetchCapitalCandlesByEpicDateRange(
      epic,
      "1m",
      params.fromMs,
      params.toMs,
      {
        maxPerRequest: 1000,
        maxRequests: params.maxRequests,
      },
    );
    return { epic, candles: normalizeFetchedCandles(raw) };
  }

  const metadata = await ensureScalpSymbolMarketMetadata(params.symbol, {
    fetchIfMissing: true,
    venue: "bitget",
  });
  const epic = metadata?.epic || params.symbol;
  const candles = await fetchBitgetCandlesByEpicDateRange(
    epic,
    "1m",
    params.fromMs,
    params.toMs,
    {
      maxPerRequest: 200,
      maxRequests: params.maxRequests,
    },
  );
  return { epic, candles };
}

export async function ensureScalpV4CandleCoverage(params: {
  venue: ScalpV4Venue;
  symbol: string;
  fromMs: number;
  toMs: number;
  existingCandles?: ScalpCandle[];
  minCoverageRatio?: number;
  chunkWeeks?: number;
  maxRequestsPerChunk?: number;
}): Promise<ScalpV4CandleCoverageEnsureResult> {
  const fromMs = Math.max(0, Math.floor(Number(params.fromMs) || 0));
  const toMs = Math.max(fromMs + ONE_MINUTE_MS, Math.floor(Number(params.toMs) || 0));
  const minCoverageRatio = Math.max(0.1, Math.min(1, Number(params.minCoverageRatio) || 0.65));
  const initialCandles = params.existingCandles || [];
  const initialCoverage = assessScalpV4CandleCoverage({
    candles: initialCandles,
    fromMs,
    toMs,
    minCoverageRatio,
  });
  if (initialCoverage.ok) {
    return {
      candles: initialCandles,
      coverage: initialCoverage,
      fetchedCandles: 0,
      savedChunks: 0,
      attempted: false,
      error: null,
    };
  }

  const chunkWeeks = Math.max(1, Math.min(26, Math.floor(Number(params.chunkWeeks) || 8)));
  const chunkMs = chunkWeeks * WEEK;
  const maxRequestsPerChunk = Math.max(
    40,
    Math.min(5000, Math.floor(Number(params.maxRequestsPerChunk) || 1200)),
  );
  let fetchedCandles = 0;
  let savedChunks = 0;
  let lastError: string | null = null;

  for (let cursor = fromMs; cursor < toMs; cursor += chunkMs) {
    const chunkFromMs = cursor;
    const chunkToMs = Math.min(toMs, cursor + chunkMs - ONE_MINUTE_MS);
    try {
      const fetched = await fetchRange({
        venue: params.venue,
        symbol: params.symbol,
        fromMs: chunkFromMs,
        toMs: chunkToMs,
        maxRequests: maxRequestsPerChunk,
      });
      if (!fetched.candles.length) continue;
      fetchedCandles += fetched.candles.length;
      await saveScalpCandleHistory({
        symbol: params.symbol,
        timeframe: "1m",
        epic: fetched.epic,
        source: "bitget",
        candles: fetched.candles,
      });
      savedChunks += 1;
    } catch (err: any) {
      lastError = String(err?.message || err || "candle_backfill_failed").slice(0, 500);
    }
  }

  const reloaded = await loadScalpCandleHistoryInRange(params.symbol, "1m", fromMs, toMs);
  const candles = (reloaded.record?.candles || []) as ScalpCandle[];
  const coverage = assessScalpV4CandleCoverage({
    candles,
    fromMs,
    toMs,
    minCoverageRatio,
  });
  return {
    candles,
    coverage,
    fetchedCandles,
    savedChunks,
    attempted: true,
    error: coverage.ok ? null : lastError,
  };
}
