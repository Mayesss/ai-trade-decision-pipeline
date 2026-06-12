import { isScalpPgConfigured, scalpPrisma } from "../pg/client";
import { sql } from "../pg/sql";
import { runScalpComposerLoadCandlesPipelineJob } from "../composer/pipelineJobsAdapter";
import type { ScalpComposerVenue } from "../composer/types";
import { startOfUtcWeekMondayMs } from "../regimes/week";
import { resolveScalpResearchConfig } from "./index";

const WEEK_MS = 7 * 24 * 60 * 60_000;

export type ScalpResearchCandlePreflightScope = {
  venue: ScalpComposerVenue;
  symbol: string;
};

export type ScalpResearchCandlePreflightFailure = ScalpResearchCandlePreflightScope & {
  reason: "missing_week_bucket" | "insufficient_week_candles";
  candles: number;
  minCandles: number;
  firstTsMs: number | null;
  lastTsMs: number | null;
};

export type ScalpResearchCandlePreflightResult = {
  ready: boolean;
  checked: number;
  staleBefore: ScalpResearchCandlePreflightFailure[];
  loaded: {
    processed: number;
    succeeded: number;
    failed: number;
  };
  staleAfter: ScalpResearchCandlePreflightFailure[];
  removedSymbols: ScalpResearchCandlePreflightScope[];
  blockingFailures: ScalpResearchCandlePreflightFailure[];
  targetWeekStartMs: number;
  targetWeekEndMs: number;
  minCandles: {
    bitget: number;
    capital: number;
  };
};

export function resolveScalpResearchPreflightWeek(params: {
  nowMs?: number;
  holdoutWeeks?: number;
} = {}): { targetWeekStartMs: number; targetWeekEndMs: number; holdoutFromMs: number; holdoutToMs: number } {
  const nowMs = Math.floor(Number(params.nowMs || Date.now()));
  const holdoutWeeks = Math.max(1, Math.floor(Number(params.holdoutWeeks || resolveScalpResearchConfig().holdoutWeeks)));
  const weekStart = startOfUtcWeekMondayMs(nowMs);
  const holdoutToMs = new Date(nowMs).getUTCDay() === 0 ? weekStart + WEEK_MS : weekStart;
  const holdoutFromMs = holdoutToMs - holdoutWeeks * WEEK_MS;
  return {
    holdoutFromMs,
    holdoutToMs,
    targetWeekStartMs: holdoutToMs - WEEK_MS,
    targetWeekEndMs: holdoutToMs,
  };
}

export function isScalpResearchRemovedBitgetSymbolError(message: unknown): boolean {
  const normalized = String(message || "").toLowerCase();
  return normalized.includes("bitget") && normalized.includes("40309") && normalized.includes("symbol has been removed");
}

function normalizeVenue(value: unknown): ScalpComposerVenue {
  return String(value || "").trim().toLowerCase() === "capital" ? "capital" : "bitget";
}

function normalizeSymbol(value: unknown): string {
  return String(value || "")
    .trim()
    .toUpperCase()
    .replace(/[^A-Z0-9._-]/g, "");
}

function minCandlesForVenue(venue: ScalpComposerVenue, params: { bitget: number; capital: number }): number {
  return venue === "capital" ? params.capital : params.bitget;
}

export function summarizeScalpResearchCandleCoverage(params: {
  scopes: ScalpResearchCandlePreflightScope[];
  coverageRows: Array<{
    symbol: string;
    candleCount: number | bigint | null;
    firstTsMs: number | bigint | null;
    lastTsMs: number | bigint | null;
  }>;
  minCandles: { bitget: number; capital: number };
}): ScalpResearchCandlePreflightFailure[] {
  const bySymbol = new Map<string, { candleCount: number; firstTsMs: number | null; lastTsMs: number | null }>();
  for (const row of params.coverageRows || []) {
    const symbol = normalizeSymbol(row.symbol);
    if (!symbol) continue;
    bySymbol.set(symbol, {
      candleCount: Math.max(0, Math.floor(Number(row.candleCount || 0))),
      firstTsMs: Number.isFinite(Number(row.firstTsMs)) ? Math.floor(Number(row.firstTsMs)) : null,
      lastTsMs: Number.isFinite(Number(row.lastTsMs)) ? Math.floor(Number(row.lastTsMs)) : null,
    });
  }
  const failures: ScalpResearchCandlePreflightFailure[] = [];
  for (const scopeRaw of params.scopes) {
    const venue = normalizeVenue(scopeRaw.venue);
    const symbol = normalizeSymbol(scopeRaw.symbol);
    if (!symbol) continue;
    const minCandles = minCandlesForVenue(venue, params.minCandles);
    const coverage = bySymbol.get(symbol);
    const candles = coverage?.candleCount ?? 0;
    if (!coverage || candles <= 0) {
      failures.push({
        venue,
        symbol,
        reason: "missing_week_bucket",
        candles: 0,
        minCandles,
        firstTsMs: null,
        lastTsMs: null,
      });
      continue;
    }
    if (candles < minCandles) {
      failures.push({
        venue,
        symbol,
        reason: "insufficient_week_candles",
        candles,
        minCandles,
        firstTsMs: coverage.firstTsMs,
        lastTsMs: coverage.lastTsMs,
      });
    }
  }
  return failures;
}

async function loadActiveV5CandleScopes(): Promise<ScalpResearchCandlePreflightScope[]> {
  if (!isScalpPgConfigured()) return [];
  const db = scalpPrisma();
  const rows = await db.$queryRaw<Array<{ venue: string; symbol: string }>>(sql`
    SELECT DISTINCT d.venue, d.symbol
    FROM scalp_v2_deployments d
    WHERE d.candidate_id IS NOT NULL
      AND d.symbol IS NOT NULL
      AND d.symbol <> ''
      AND NOT EXISTS (
        SELECT 1
        FROM scalp_v2_candidates c
        WHERE c.id = d.candidate_id
          AND c.metadata_json->'scopeRemoval'->>'reason' = 'bitget_symbol_removed_no_candles'
      )
    ORDER BY d.venue, d.symbol;
  `);
  return rows
    .map((row) => ({ venue: normalizeVenue(row.venue), symbol: normalizeSymbol(row.symbol) }))
    .filter((row) => row.symbol.length > 0);
}

async function loadWeekCoverage(params: {
  scopes: ScalpResearchCandlePreflightScope[];
  targetWeekStartMs: number;
}): Promise<Array<{ symbol: string; candleCount: number | bigint | null; firstTsMs: number | bigint | null; lastTsMs: number | bigint | null }>> {
  if (!isScalpPgConfigured() || params.scopes.length === 0) return [];
  const symbols = Array.from(new Set(params.scopes.map((scope) => normalizeSymbol(scope.symbol)).filter(Boolean)));
  if (symbols.length === 0) return [];
  const db = scalpPrisma();
  return await db.$queryRaw(sql`
    SELECT
      symbol,
      COALESCE(SUM(candle_count), 0)::bigint AS "candleCount",
      MIN(first_ts_ms)::bigint AS "firstTsMs",
      MAX(last_ts_ms)::bigint AS "lastTsMs"
    FROM scalp_candle_history_weeks
    WHERE timeframe = '1m'
      AND week_start = to_timestamp(${params.targetWeekStartMs} / 1000.0)
      AND symbol = ANY(${symbols}::text[])
    GROUP BY symbol;
  `);
}

async function markRemovedBitgetSymbols(params: {
  scopes: ScalpResearchCandlePreflightScope[];
  source: string;
}): Promise<ScalpResearchCandlePreflightScope[]> {
  const removed = params.scopes
    .map((scope) => ({ venue: normalizeVenue(scope.venue), symbol: normalizeSymbol(scope.symbol) }))
    .filter((scope) => scope.venue === "bitget" && scope.symbol.length > 0);
  if (!isScalpPgConfigured() || removed.length === 0) return removed;
  const symbols = Array.from(new Set(removed.map((scope) => scope.symbol)));
  const source = String(params.source || "v5_candle_preflight").trim() || "v5_candle_preflight";
  const db = scalpPrisma();
  await db.$executeRaw(sql`
    WITH affected AS (
      SELECT DISTINCT d.candidate_id
      FROM scalp_v2_deployments d
      WHERE d.venue = 'bitget'
        AND d.symbol = ANY(${symbols}::text[])
        AND d.candidate_id IS NOT NULL
    )
    UPDATE scalp_v2_candidates c
    SET
      status = 'rejected',
      metadata_json = COALESCE(metadata_json, '{}'::jsonb) || jsonb_build_object(
        'scopeRemoval',
        jsonb_build_object(
          'reason', 'bitget_symbol_removed_no_candles',
          'source', ${source},
          'markedAtMs', floor(extract(epoch from now()) * 1000)::bigint
        )
      ),
      updated_at = NOW()
    FROM affected a
    WHERE c.id = a.candidate_id;
  `);
  await db.$executeRaw(sql`
    UPDATE scalp_v2_deployments
    SET
      enabled = FALSE,
      v5_enabled = FALSE,
      v5_lease_until = NULL,
      promotion_gate = COALESCE(promotion_gate, '{}'::jsonb) || jsonb_build_object(
        'scopeRemoval',
        jsonb_build_object(
          'reason', 'bitget_symbol_removed_no_candles',
          'source', ${source},
          'markedAtMs', floor(extract(epoch from now()) * 1000)::bigint
        )
      ),
      updated_at = NOW()
    WHERE venue = 'bitget'
      AND symbol = ANY(${symbols}::text[])
      AND candidate_id IS NOT NULL;
  `);
  return removed;
}

export async function runScalpResearchCandlePreflight(params: {
  nowMs?: number;
  batchSize?: number;
  maxAttempts?: number;
  minBitgetWeekCandles?: number;
  minCapitalWeekCandles?: number;
  auditTrigger?: string | null;
} = {}): Promise<ScalpResearchCandlePreflightResult> {
  const cfg = resolveScalpResearchConfig();
  const week = resolveScalpResearchPreflightWeek({ nowMs: params.nowMs, holdoutWeeks: cfg.holdoutWeeks });
  const minCandles = {
    bitget: Math.max(0, Math.floor(Number(params.minBitgetWeekCandles ?? 8_000))),
    capital: Math.max(0, Math.floor(Number(params.minCapitalWeekCandles ?? 6_000))),
  };
  let scopes = await loadActiveV5CandleScopes();
  let coverage = await loadWeekCoverage({ scopes, targetWeekStartMs: week.targetWeekStartMs });
  const staleBefore = summarizeScalpResearchCandleCoverage({ scopes, coverageRows: coverage, minCandles });
  let loaded = { processed: 0, succeeded: 0, failed: 0 };
  let removedSymbols: ScalpResearchCandlePreflightScope[] = [];

  if (staleBefore.length > 0) {
    const staleScopes = staleBefore.map((row) => ({ venue: row.venue, symbol: row.symbol }));
    const loadResult = await runScalpComposerLoadCandlesPipelineJob({
      scopes: staleScopes,
      batchSize: Math.max(1, Math.min(200, Math.floor(Number(params.batchSize ?? 200)))),
      maxAttempts: Math.max(1, Math.min(30, Math.floor(Number(params.maxAttempts ?? 10)))),
      auditSource: "v5_preflight_candles",
      auditTrigger: params.auditTrigger || "preflight",
    });
    loaded = {
      processed: loadResult.processed,
      succeeded: loadResult.succeeded,
      failed: loadResult.failed,
    };
    const details = (loadResult.details || {}) as Record<string, unknown>;
    const errors = Array.isArray(details.errors) ? details.errors as Array<Record<string, unknown>> : [];
    const removed = errors
      .filter((error) => normalizeVenue(error.venue) === "bitget" && isScalpResearchRemovedBitgetSymbolError(error.message))
      .map((error) => ({ venue: "bitget" as const, symbol: normalizeSymbol(error.symbol) }))
      .filter((scope) => scope.symbol.length > 0);
    removedSymbols = await markRemovedBitgetSymbols({ scopes: removed, source: "v5_candle_preflight" });
  }

  scopes = await loadActiveV5CandleScopes();
  coverage = await loadWeekCoverage({ scopes, targetWeekStartMs: week.targetWeekStartMs });
  const staleAfter = summarizeScalpResearchCandleCoverage({ scopes, coverageRows: coverage, minCandles });

  return {
    ready: staleAfter.length === 0,
    checked: scopes.length,
    staleBefore,
    loaded,
    staleAfter,
    removedSymbols,
    blockingFailures: staleAfter,
    targetWeekStartMs: week.targetWeekStartMs,
    targetWeekEndMs: week.targetWeekEndMs,
    minCandles,
  };
}
