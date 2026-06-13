import nextEnv from "@next/env";

import {
  fetchCapitalCandlesByEpicDateRange,
  resolveCapitalEpicRuntime,
} from "../lib/capital";
import { fetchBitgetCandlesByEpicDateRange } from "../lib/scalp/bitgetHistory";
import {
  loadScalpCandleHistoryInRange,
  mergeScalpCandleHistory,
  saveScalpCandleHistory,
} from "../lib/scalp/candleHistory";
import { scalpPrisma } from "../lib/scalp/pg/client";
import { sql } from "../lib/scalp/pg/sql";
import { ensureScalpSymbolMarketMetadata } from "../lib/scalp/symbolMarketMetadataSync";
import type { ScalpCandle } from "../lib/scalp/types";
import {
  assessScalpRegimeCandleCoverage,
  type ScalpRegimeCandleCoverageStatus,
} from "../lib/scalp/regimes/candleCoverage";
import type { ScalpRegimeVenue } from "../lib/scalp/regimes/types";

const { loadEnvConfig } = nextEnv;
loadEnvConfig(process.cwd());

const MINUTE = 60_000;
const WEEK = 7 * 24 * 60 * 60 * 1000;

function envNumber(name: string, fallback: number): number {
  const n = Number(process.env[name]);
  return Number.isFinite(n) ? n : fallback;
}

function normalizeSymbol(value: unknown): string {
  return String(value || "")
    .trim()
    .toUpperCase()
    .replace(/[^A-Z0-9._-]/g, "");
}

function normalizeVenue(value: unknown): ScalpRegimeVenue {
  return String(value || "").trim().toLowerCase() === "capital" ? "capital" : "bitget";
}

function toPlainCoverage(coverage: ScalpRegimeCandleCoverageStatus): Record<string, unknown> {
  return {
    ok: coverage.ok,
    reason: coverage.reason,
    candleCount: coverage.candleCount,
    expectedCandles: coverage.expectedCandles,
    coveragePct: Number((coverage.coverageRatio * 100).toFixed(1)),
    firstTs: coverage.firstTsMs ? new Date(coverage.firstTsMs).toISOString() : null,
    lastTs: coverage.lastTsMs ? new Date(coverage.lastTsMs).toISOString() : null,
  };
}

async function loadStageCScopes(): Promise<Array<{ venue: ScalpRegimeVenue; symbol: string; candidates: number }>> {
  const rows = await scalpPrisma().$queryRaw<Array<{ venue: string; symbol: string; candidates: bigint }>>(sql`
    SELECT venue, symbol, COUNT(*)::bigint AS candidates
    FROM scalp_v2_candidates
    WHERE COALESCE((metadata_json->'worker'->>'finalPass')::boolean, FALSE)
       OR COALESCE((metadata_json->'worker'->'stageC'->>'passed')::boolean, FALSE)
    GROUP BY venue, symbol
    ORDER BY COUNT(*) DESC, symbol ASC;
  `);
  return rows
    .map((row) => ({
      venue: normalizeVenue(row.venue),
      symbol: normalizeSymbol(row.symbol),
      candidates: Number(row.candidates || 0),
    }))
    .filter((row) => Boolean(row.symbol));
}

async function fetchCandles(params: {
  venue: ScalpRegimeVenue;
  symbol: string;
  fromMs: number;
  toMs: number;
  maxRequests: number;
}): Promise<{ epic: string; candles: ScalpCandle[] }> {
  if (params.venue === "capital") {
    const resolved = await resolveCapitalEpicRuntime(params.symbol);
    const epic = String(resolved.epic || params.symbol).trim().toUpperCase();
    const rows = await fetchCapitalCandlesByEpicDateRange(
      epic,
      "1m",
      params.fromMs,
      params.toMs,
      { maxPerRequest: 1000, maxRequests: params.maxRequests },
    );
    return { epic, candles: rows as ScalpCandle[] };
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
    { maxPerRequest: 200, maxRequests: params.maxRequests },
  );
  return { epic, candles };
}

function countCandlesInRange(candles: ScalpCandle[], fromMs: number, toMs: number): number {
  return candles.filter((row) => {
    const ts = Number(row?.[0] || 0);
    return Number.isFinite(ts) && ts >= fromMs && ts < toMs;
  }).length;
}

async function backfillScope(params: {
  venue: ScalpRegimeVenue;
  symbol: string;
  windowFromMs: number;
  windowToMs: number;
  targetCoverageRatio: number;
  chunkWeeks: number;
  maxRequestsPerChunk: number;
}): Promise<{ fetched: number; savedChunks: number; before: ScalpRegimeCandleCoverageStatus; after: ScalpRegimeCandleCoverageStatus }> {
  const history = await loadScalpCandleHistoryInRange(
    params.symbol,
    "1m",
    params.windowFromMs,
    params.windowToMs,
  );
  let candles = (history.record?.candles || []) as ScalpCandle[];
  const before = assessScalpRegimeCandleCoverage({
    candles,
    fromMs: params.windowFromMs,
    toMs: params.windowToMs,
    minCoverageRatio: params.targetCoverageRatio,
  });
  if (before.ok) {
    return { fetched: 0, savedChunks: 0, before, after: before };
  }

  let fetchedTotal = 0;
  let savedChunks = 0;
  const chunkMs = params.chunkWeeks * WEEK;
  for (let chunkFrom = params.windowFromMs; chunkFrom < params.windowToMs; chunkFrom += chunkMs) {
    const chunkTo = Math.min(params.windowToMs, chunkFrom + chunkMs - MINUTE);
    const expectedChunkCandles = Math.max(1, Math.floor((chunkTo - chunkFrom) / MINUTE));
    const existingChunkCandles = countCandlesInRange(candles, chunkFrom, chunkTo);
    if (existingChunkCandles >= Math.floor(expectedChunkCandles * 0.9)) {
      continue;
    }
    console.log(JSON.stringify({
      event: "chunk_fetch_start",
      venue: params.venue,
      symbol: params.symbol,
      from: new Date(chunkFrom).toISOString(),
      to: new Date(chunkTo).toISOString(),
      existingChunkCandles,
      expectedChunkCandles,
    }));
    const fetched = await fetchCandles({
      venue: params.venue,
      symbol: params.symbol,
      fromMs: chunkFrom,
      toMs: chunkTo,
      maxRequests: params.maxRequestsPerChunk,
    });
    console.log(JSON.stringify({
      event: "chunk_fetch_done",
      venue: params.venue,
      symbol: params.symbol,
      from: new Date(chunkFrom).toISOString(),
      to: new Date(chunkTo).toISOString(),
      fetched: fetched.candles.length,
    }));
    if (!fetched.candles.length) continue;
    await saveScalpCandleHistory({
      symbol: params.symbol,
      timeframe: "1m",
      epic: fetched.epic,
      source: "bitget",
      candles: fetched.candles,
    });
    candles = mergeScalpCandleHistory(candles, fetched.candles);
    fetchedTotal += fetched.candles.length;
    savedChunks += 1;
    const coverage = assessScalpRegimeCandleCoverage({
      candles,
      fromMs: params.windowFromMs,
      toMs: params.windowToMs,
      minCoverageRatio: params.targetCoverageRatio,
    });
    if (coverage.ok) {
      return { fetched: fetchedTotal, savedChunks, before, after: coverage };
    }
  }

  const after = assessScalpRegimeCandleCoverage({
    candles,
    fromMs: params.windowFromMs,
    toMs: params.windowToMs,
    minCoverageRatio: params.targetCoverageRatio,
  });
  return { fetched: fetchedTotal, savedChunks, before, after };
}

async function main() {
  const now = Date.now();
  const windowToMs = now - (now % WEEK);
  // Backfill depth in weeks. Default 104 (legacy regime walk-forward sweep);
  // the M30/H1 composer bulk only needs stageC(48w) + holdout(24w) = ~72w, so
  // pass SCALP_REGIME_STAGEC_BACKFILL_WEEKS=76 for that scope.
  const backfillWeeks = Math.max(
    1,
    Math.min(208, Math.floor(envNumber("SCALP_REGIME_STAGEC_BACKFILL_WEEKS", 104))),
  );
  const windowFromMs = windowToMs - backfillWeeks * WEEK;
  const targetCoverageRatio = Math.max(
    0.1,
    Math.min(1, envNumber("SCALP_REGIME_STAGEC_BACKFILL_TARGET_COVERAGE", 0.65)),
  );
  const chunkWeeks = Math.max(
    1,
    Math.min(26, Math.floor(envNumber("SCALP_REGIME_STAGEC_BACKFILL_CHUNK_WEEKS", 8))),
  );
  const maxRequestsPerChunk = Math.max(
    40,
    Math.min(5000, Math.floor(envNumber("SCALP_REGIME_STAGEC_BACKFILL_MAX_REQUESTS_PER_CHUNK", 1200))),
  );
  const limit = Math.max(0, Math.floor(envNumber("SCALP_REGIME_STAGEC_BACKFILL_LIMIT", 0)));
  // Explicit-symbol override: backfill a specific list (e.g. shallow symbols
  // not yet in the stage-C scope) instead of querying stage-C-passing scopes.
  // SCALP_REGIME_STAGEC_BACKFILL_SYMBOLS="PEPEUSDT,EURJPY", _VENUE=bitget|capital
  const explicitSymbols = String(process.env.SCALP_REGIME_STAGEC_BACKFILL_SYMBOLS || "")
    .split(",")
    .map((s) => normalizeSymbol(s))
    .filter(Boolean);
  const scopes = explicitSymbols.length
    ? explicitSymbols.map((symbol) => ({
        venue: normalizeVenue(process.env.SCALP_REGIME_STAGEC_BACKFILL_VENUE),
        symbol,
        candidates: 0,
      }))
    : await loadStageCScopes();
  const selected = limit > 0 ? scopes.slice(0, limit) : scopes;
  console.log(JSON.stringify({
    event: "start",
    windowFrom: new Date(windowFromMs).toISOString(),
    windowTo: new Date(windowToMs).toISOString(),
    targetCoverageRatio,
    chunkWeeks,
    maxRequestsPerChunk,
    scopes: selected.length,
  }));

  let completed = 0;
  let usable = 0;
  let fetchedTotal = 0;
  for (const scope of selected) {
    const startedAt = Date.now();
    console.log(JSON.stringify({ event: "symbol_start", ...scope, completed, total: selected.length }));
    try {
      const result = await backfillScope({
        venue: scope.venue,
        symbol: scope.symbol,
        windowFromMs,
        windowToMs,
        targetCoverageRatio,
        chunkWeeks,
        maxRequestsPerChunk,
      });
      completed += 1;
      if (result.after.ok) usable += 1;
      fetchedTotal += result.fetched;
      console.log(JSON.stringify({
        event: "symbol_done",
        ...scope,
        fetched: result.fetched,
        savedChunks: result.savedChunks,
        durationSec: Number(((Date.now() - startedAt) / 1000).toFixed(1)),
        before: toPlainCoverage(result.before),
        after: toPlainCoverage(result.after),
        completed,
        usable,
        fetchedTotal,
      }));
    } catch (err: any) {
      completed += 1;
      console.log(JSON.stringify({
        event: "symbol_error",
        ...scope,
        message: String(err?.message || err || "unknown_error").slice(0, 500),
        completed,
      }));
    }
  }
  console.log(JSON.stringify({ event: "done", completed, usable, fetchedTotal }));
  await scalpPrisma().$disconnect();
}

main().catch(async (err) => {
  console.error(JSON.stringify({ event: "fatal", message: err?.message || String(err) }));
  await scalpPrisma().$disconnect().catch(() => {});
  process.exit(1);
});
