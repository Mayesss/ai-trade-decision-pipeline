import { Prisma } from "@prisma/client";

import {
  fetchCapitalCandlesByEpicDateRange,
  resolveCapitalEpicRuntime,
} from "../capital";
import { bitgetFetch, resolveProductType } from "../bitget";
import {
  loadScalpCandleHistory,
  mergeScalpCandleHistory,
  saveScalpCandleHistory,
  timeframeToMs,
} from "./candleHistory";
import {
  listScalpDeploymentRegistryEntries,
  upsertScalpDeploymentRegistryEntriesBulk,
  type ScalpDeploymentPromotionGate,
  type ScalpDeploymentRegistryEntry,
  type ScalpForwardValidationMetrics,
} from "./deploymentRegistry";
import {
  buildBestEligibleTuneDeploymentIdSet,
  buildGlobalSymbolRankedDeploymentIdSet,
  buildForwardValidationByCandidateFromTasks,
  evaluateFreshCompletedDeploymentWeeks,
  evaluateWeeklyRobustnessGate,
  type ScalpWeeklyRobustnessMetrics,
  type SyncResearchWeeklyPolicy,
} from "./promotionPolicy";
import { buildScalpResearchTuneVariants } from "./researchTuner";
import { runScalpReplay } from "./replay/harness";
import { buildScalpReplayRuntimeFromDeployment } from "./replay/runtimeConfig";
import { isScalpPgConfigured, scalpPrisma } from "./pg/client";
import {
  resolveScalpDeployment,
  resolveScalpDeploymentVenueFromId,
} from "./deployments";
import { listScalpStrategies } from "./strategies/registry";
import {
  loadScalpSymbolDiscoveryPolicy,
  resolveCompletedWeekCoverageStartMs,
  resolveRecommendedStrategiesForSymbol,
  runScalpSymbolDiscoveryCycle,
} from "./symbolDiscovery";
import { pipSizeForScalpSymbol } from "./marketData";
import { ensureScalpSymbolMarketMetadata } from "./symbolMarketMetadataSync";
import { loadScalpSymbolMarketMetadata } from "./symbolMarketMetadataStore";
import type { ScalpCandle } from "./types";
import type { ScalpVenue } from "./venue";

const ONE_MINUTE_MS = 60_000;
const ONE_DAY_MS = 24 * 60 * 60 * 1000;
const ONE_WEEK_MS = 7 * ONE_DAY_MS;
const BITGET_HISTORY_CANDLES_MAX_LIMIT = 200;

export const SCALP_PIPELINE_JOB_KINDS = [
  "discover",
  "load_candles",
  "prepare",
  "worker",
  "promotion",
] as const;

export type ScalpPipelineJobKind = (typeof SCALP_PIPELINE_JOB_KINDS)[number];

export type ScalpPipelineQueueStatus =
  | "pending"
  | "running"
  | "succeeded"
  | "retry_wait"
  | "failed";

export interface ScalpPipelineJobExecutionResult {
  ok: boolean;
  busy: boolean;
  jobKind: ScalpPipelineJobKind;
  processed: number;
  succeeded: number;
  retried: number;
  failed: number;
  pendingAfter: number;
  downstreamRequested: boolean;
  progressLabel: string | null;
  details: Record<string, unknown>;
  error?: string;
}

export interface ScalpPipelineJobHealth {
  jobKind: ScalpPipelineJobKind;
  status: string;
  locked: boolean;
  lastRunAtMs: number | null;
  lastSuccessAtMs: number | null;
  nextRunAtMs: number | null;
  lastError: string | null;
  progressLabel: string | null;
  progress: Record<string, unknown> | null;
  queue: {
    pending: number;
    running: number;
    retryWait: number;
    failed: number;
    succeeded: number;
  };
}

export interface ScalpDeploymentWeeklyMetricSnapshotRow {
  deploymentId: string;
  symbol: string;
  strategyId: string;
  tuneId: string;
  weekStartMs: number;
  weekEndMs: number;
  status: string;
  attempts: number;
  startedAtMs: number | null;
  finishedAtMs: number | null;
  errorCode: string | null;
  errorMessage: string | null;
  trades: number | null;
  netR: number | null;
  expectancyR: number | null;
  profitFactor: number | null;
  maxDrawdownR: number | null;
}

function toFiniteNumber(value: unknown, fallback = 0): number {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

function toPositiveInt(
  value: unknown,
  fallback: number,
  max = Number.POSITIVE_INFINITY,
): number {
  const n = Math.floor(Number(value));
  if (!Number.isFinite(n) || n <= 0) return fallback;
  return Math.max(1, Math.min(max, n));
}

function toBoundedPercent(value: unknown, fallback: number): number {
  const n = Number(value);
  if (!Number.isFinite(n)) return fallback;
  return Math.max(0, Math.min(100, n));
}

function envBool(name: string, fallback: boolean): boolean {
  const raw = String(process.env[name] || "")
    .trim()
    .toLowerCase();
  if (!raw) return fallback;
  if (["1", "true", "yes", "on"].includes(raw)) return true;
  if (["0", "false", "no", "off"].includes(raw)) return false;
  return fallback;
}

function envNumber(name: string, fallback: number): number {
  const n = Number(process.env[name]);
  if (!Number.isFinite(n)) return fallback;
  return n;
}

function isScalpPipelineBitgetOnlyEnabled(): boolean {
  return envBool("SCALP_PIPELINE_BITGET_ONLY", true);
}

function normalizeSymbol(value: unknown): string {
  return String(value || "")
    .trim()
    .toUpperCase()
    .replace(/[^A-Z0-9._-]/g, "");
}

function isBitgetPipelineSymbol(symbolRaw: string): boolean {
  const symbol = normalizeSymbol(symbolRaw);
  if (!symbol) return false;
  const productType = String(resolveProductType() || "usdt-futures")
    .trim()
    .toLowerCase();
  if (productType === "usdc-futures") return symbol.endsWith("USDC");
  if (productType === "coin-futures") return symbol.endsWith("USD");
  return symbol.endsWith("USDT");
}

async function resolvePipelineDeploymentVenue(
  symbolRaw: string,
): Promise<ScalpVenue> {
  const symbol = normalizeSymbol(symbolRaw);
  if (!symbol) return "capital";
  if (isScalpPipelineBitgetOnlyEnabled() && isBitgetPipelineSymbol(symbol)) {
    return "bitget";
  }

  const history = await loadScalpCandleHistory(symbol, "1m");
  if (history.record?.source === "bitget" || history.record?.source === "capital") {
    return history.record.source;
  }

  const metadata = await loadScalpSymbolMarketMetadata(symbol);
  if (metadata?.source === "bitget" || metadata?.source === "capital") {
    return metadata.source;
  }

  return isBitgetPipelineSymbol(symbol) ? "bitget" : "capital";
}

function asJsonObject(value: unknown): Record<string, unknown> | null {
  if (!value || typeof value !== "object" || Array.isArray(value)) return null;
  return value as Record<string, unknown>;
}

function asTsMs(value: unknown): number | null {
  const n = Number(value);
  if (!Number.isFinite(n) || n <= 0) return null;
  return Math.floor(n);
}

function asNullableFiniteNumber(value: unknown): number | null {
  if (value === null || value === undefined) return null;
  const n = Number(value);
  if (!Number.isFinite(n)) return null;
  return n;
}

function startOfUtcDay(tsMs: number): number {
  const d = new Date(tsMs);
  return Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate());
}

function startOfWeekMondayUtc(tsMs: number): number {
  const dayStartMs = startOfUtcDay(tsMs);
  const dayOfWeek = new Date(dayStartMs).getUTCDay();
  const daysSinceMonday = (dayOfWeek + 6) % 7;
  return dayStartMs - daysSinceMonday * ONE_DAY_MS;
}

function resolveLastCompletedWeekBoundsUtc(nowMs: number): {
  startCurrentWeekMondayMs: number;
  lastSundayEndMs: number;
} {
  const startCurrentWeekMondayMs = startOfWeekMondayUtc(nowMs);
  return {
    startCurrentWeekMondayMs,
    lastSundayEndMs: startCurrentWeekMondayMs - 1,
  };
}

function resolveRequiredSuccessiveWeeks(): number {
  return Math.max(
    13,
    Math.min(
      52,
      toPositiveInt(process.env.SCALP_PIPELINE_REQUIRED_SUCCESSIVE_WEEKS, 13),
    ),
  );
}

function resolvePromotionFreshWeeks(): number {
  return Math.max(
    12,
    Math.min(
      52,
      toPositiveInt(envNumber("SCALP_PROMOTION_FRESH_WEEKS", 12), 12),
    ),
  );
}

function findEarliestMissingCompletedWeekStartMs(
  candles: ScalpCandle[],
  nowMs: number,
  requiredWeeks: number,
): number | null {
  if (requiredWeeks <= 0) return null;
  const { startCurrentWeekMondayMs, lastSundayEndMs } =
    resolveLastCompletedWeekBoundsUtc(nowMs);
  const firstRequiredWeekStartMs =
    startCurrentWeekMondayMs - requiredWeeks * ONE_WEEK_MS;
  const presentWeekIndexes = new Set<number>();
  for (const candle of candles) {
    const ts = Number(candle?.[0] || 0);
    if (!Number.isFinite(ts) || ts <= 0) continue;
    if (ts < firstRequiredWeekStartMs || ts > lastSundayEndMs) continue;
    const index = Math.floor((ts - firstRequiredWeekStartMs) / ONE_WEEK_MS);
    if (index >= 0 && index < requiredWeeks) {
      presentWeekIndexes.add(index);
    }
  }
  for (let i = 0; i < requiredWeeks; i += 1) {
    if (!presentWeekIndexes.has(i)) {
      return firstRequiredWeekStartMs + i * ONE_WEEK_MS;
    }
  }
  return null;
}

function countCoveredCompletedWeeks(
  candles: ScalpCandle[],
  nowMs: number,
  requiredWeeks: number,
): { covered: number; latestWeekStartMs: number | null } {
  const { startCurrentWeekMondayMs, lastSundayEndMs } =
    resolveLastCompletedWeekBoundsUtc(nowMs);
  const firstRequiredWeekStartMs =
    startCurrentWeekMondayMs - requiredWeeks * ONE_WEEK_MS;
  const presentWeekIndexes = new Set<number>();
  let latestWeekStartMs: number | null = null;
  for (const candle of candles) {
    const ts = Number(candle?.[0] || 0);
    if (!Number.isFinite(ts) || ts <= 0) continue;
    if (ts < firstRequiredWeekStartMs || ts > lastSundayEndMs) continue;
    const weekStartMs = startOfWeekMondayUtc(ts);
    const index = Math.floor(
      (weekStartMs - firstRequiredWeekStartMs) / ONE_WEEK_MS,
    );
    if (index >= 0 && index < requiredWeeks) {
      presentWeekIndexes.add(index);
      latestWeekStartMs =
        latestWeekStartMs === null
          ? weekStartMs
          : Math.max(latestWeekStartMs, weekStartMs);
    }
  }
  return { covered: presentWeekIndexes.size, latestWeekStartMs };
}

function normalizeFetchedCandles(rows: unknown[]): ScalpCandle[] {
  return rows
    .map((row) => {
      const value = Array.isArray(row) ? row : [];
      const ts = Number(value[0]);
      const open = Number(value[1]);
      const high = Number(value[2]);
      const low = Number(value[3]);
      const close = Number(value[4]);
      const volume = Number(value[5] ?? 0);
      if (
        ![ts, open, high, low, close].every((v) => Number.isFinite(v) && v > 0)
      )
        return null;
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

function toReplayCandles(
  candles: ScalpCandle[],
  spreadPips: number,
): Array<{
  ts: number;
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
  spreadPips: number;
}> {
  return candles.map((row) => ({
    ts: Number(row[0]),
    open: Number(row[1]),
    high: Number(row[2]),
    low: Number(row[3]),
    close: Number(row[4]),
    volume: Number(row[5] || 0),
    spreadPips: Number.isFinite(spreadPips) ? spreadPips : 0,
  }));
}

function normalizeBitgetHistoryGranularity(timeframe: string): string {
  const normalized = String(timeframe || "").trim();
  const lower = normalized.toLowerCase();
  if (lower === "1m") return "1m";
  if (lower === "3m") return "3m";
  if (lower === "5m") return "5m";
  if (lower === "15m") return "15m";
  if (lower === "30m") return "30m";
  if (lower === "1h") return "1H";
  if (lower === "2h") return "2H";
  if (lower === "4h") return "4H";
  if (lower === "6h") return "6H";
  if (lower === "12h") return "12H";
  if (lower === "1d") return "1D";
  if (lower === "1w" || lower === "4d") return "1W";
  if (lower === "1mo" || lower === "1mth" || lower === "1month") return "1M";
  return normalized || "1m";
}

async function fetchBitgetCandlesByEpicDateRange(
  epic: string,
  timeframe: string,
  fromTsMs: number,
  toTsMs: number,
  opts: {
    maxPerRequest?: number;
    maxRequests?: number;
  } = {},
): Promise<ScalpCandle[]> {
  const symbol = normalizeSymbol(epic);
  if (!symbol) return [];

  const startMs = Math.floor(Math.min(fromTsMs, toTsMs));
  const endMs = Math.floor(Math.max(fromTsMs, toTsMs));
  if (
    !(Number.isFinite(startMs) && Number.isFinite(endMs) && endMs > startMs)
  ) {
    return [];
  }

  const granularity = normalizeBitgetHistoryGranularity(timeframe);
  const timeframeMs = Math.max(ONE_MINUTE_MS, timeframeToMs("1m"));
  const requestLimit = Math.max(
    20,
    Math.min(
      BITGET_HISTORY_CANDLES_MAX_LIMIT,
      Math.floor(opts.maxPerRequest ?? BITGET_HISTORY_CANDLES_MAX_LIMIT),
    ),
  );
  const maxRequests = Math.max(40, Math.floor(opts.maxRequests ?? 800));
  const requestSpanBars = Math.max(220, requestLimit + 20);
  const requestSpanMs = requestSpanBars * timeframeMs;
  const productType = String(resolveProductType() || "usdt-futures")
    .trim()
    .toUpperCase();

  const candlesByTs = new Map<number, ScalpCandle>();
  let cursorEnd = endMs;
  let requests = 0;
  while (cursorEnd >= startMs) {
    if (requests >= maxRequests) {
      throw new Error(`bitget_history_max_requests_reached_for_${symbol}`);
    }
    const startTime = Math.max(
      startMs,
      cursorEnd - requestSpanMs + timeframeMs,
    );
    const rows = await bitgetFetch(
      "GET",
      "/api/v2/mix/market/history-candles",
      {
        symbol,
        productType,
        granularity,
        limit: requestLimit,
        startTime,
        endTime: cursorEnd,
      },
    );
    requests += 1;

    const parsedRows = Array.isArray(rows)
      ? normalizeFetchedCandles(rows).filter(
          (row) => row[0] >= startMs && row[0] <= endMs,
        )
      : [];
    if (!parsedRows.length) {
      if (startTime <= startMs) break;
      cursorEnd = startTime - timeframeMs;
      continue;
    }

    let oldestTs = Number.POSITIVE_INFINITY;
    for (const candle of parsedRows) {
      candlesByTs.set(candle[0], candle);
      if (candle[0] < oldestTs) oldestTs = candle[0];
    }
    if (!Number.isFinite(oldestTs)) break;
    if (oldestTs >= cursorEnd) {
      cursorEnd -= requestSpanMs;
    } else {
      cursorEnd = oldestTs - 1;
    }
  }

  return Array.from(candlesByTs.values()).sort((a, b) => a[0] - b[0]);
}

async function ensurePipelineJobRow(
  jobKind: ScalpPipelineJobKind,
): Promise<void> {
  const db = scalpPrisma();
  await db.$executeRaw(Prisma.sql`
        INSERT INTO scalp_pipeline_jobs(job_kind, status, next_run_at, created_at, updated_at)
        VALUES(${jobKind}, 'idle', NOW(), NOW(), NOW())
        ON CONFLICT(job_kind) DO NOTHING;
    `);
}

async function acquirePipelineJobLock(params: {
  jobKind: ScalpPipelineJobKind;
  lockToken: string;
  lockMs: number;
}): Promise<boolean> {
  await ensurePipelineJobRow(params.jobKind);
  const db = scalpPrisma();
  const rows = await db.$queryRaw<Array<{ jobKind: string }>>(Prisma.sql`
        UPDATE scalp_pipeline_jobs
        SET
            status = 'running',
            lock_token = ${params.lockToken},
            lock_expires_at = NOW() + make_interval(secs => ${Math.max(5, Math.floor(params.lockMs / 1000))}),
            running_since = COALESCE(running_since, NOW()),
            attempts = attempts + 1,
            last_run_at = NOW(),
            updated_at = NOW()
        WHERE job_kind = ${params.jobKind}
          AND (lock_expires_at IS NULL OR lock_expires_at < NOW())
        RETURNING job_kind AS "jobKind";
    `);
  return rows.length > 0;
}

async function pulsePipelineJobProgress(params: {
  jobKind: ScalpPipelineJobKind;
  lockToken: string;
  lockMs: number;
  progressLabel?: string | null;
  progress?: Record<string, unknown> | null;
}): Promise<void> {
  const db = scalpPrisma();
  await db.$executeRaw(Prisma.sql`
        UPDATE scalp_pipeline_jobs
        SET
            lock_expires_at = NOW() + make_interval(secs => ${Math.max(5, Math.floor(params.lockMs / 1000))}),
            progress_label = ${params.progressLabel || null},
            progress_json = ${params.progress ? JSON.stringify(params.progress) : null}::jsonb,
            updated_at = NOW()
        WHERE job_kind = ${params.jobKind}
          AND lock_token = ${params.lockToken};
    `);
}

async function releasePipelineJobLock(params: {
  jobKind: ScalpPipelineJobKind;
  lockToken: string;
  success: boolean;
  nextRunAtMs?: number | null;
  lastError?: string | null;
  progressLabel?: string | null;
  progress?: Record<string, unknown> | null;
}): Promise<void> {
  const nextRunAt =
    typeof params.nextRunAtMs === "number" &&
    Number.isFinite(params.nextRunAtMs) &&
    params.nextRunAtMs > 0
      ? new Date(params.nextRunAtMs)
      : null;
  const db = scalpPrisma();
  await db.$executeRaw(Prisma.sql`
        UPDATE scalp_pipeline_jobs
        SET
            status = ${params.success ? "idle" : "failed"},
            lock_token = NULL,
            lock_expires_at = NULL,
            running_since = NULL,
            next_run_at = COALESCE(${nextRunAt}, next_run_at),
            last_success_at = CASE WHEN ${params.success} THEN NOW() ELSE last_success_at END,
            last_error = ${params.lastError || null},
            progress_label = ${params.progressLabel || null},
            progress_json = ${params.progress ? JSON.stringify(params.progress) : null}::jsonb,
            updated_at = NOW()
        WHERE job_kind = ${params.jobKind}
          AND lock_token = ${params.lockToken};
    `);
}

async function runWithPipelineJobLock(
  jobKind: ScalpPipelineJobKind,
  run: (ctx: {
    lockToken: string;
    lockMs: number;
  }) => Promise<Omit<ScalpPipelineJobExecutionResult, "jobKind" | "busy">>,
): Promise<ScalpPipelineJobExecutionResult> {
  if (!isScalpPgConfigured()) {
    return {
      ok: false,
      busy: false,
      jobKind,
      processed: 0,
      succeeded: 0,
      retried: 0,
      failed: 0,
      pendingAfter: 0,
      downstreamRequested: false,
      progressLabel: null,
      details: {},
      error: "scalp_pg_not_configured",
    };
  }
  const lockMs = Math.max(
    30_000,
    Math.min(
      20 * 60_000,
      toPositiveInt(process.env.SCALP_PIPELINE_JOB_LOCK_MS, 6 * 60_000),
    ),
  );
  const lockToken = `${jobKind}:${Date.now()}:${Math.floor(Math.random() * 1_000_000)}`;
  const acquired = await acquirePipelineJobLock({ jobKind, lockToken, lockMs });
  if (!acquired) {
    return {
      ok: true,
      busy: true,
      jobKind,
      processed: 0,
      succeeded: 0,
      retried: 0,
      failed: 0,
      pendingAfter: 0,
      downstreamRequested: false,
      progressLabel: "busy",
      details: {},
    };
  }

  try {
    const result = await run({ lockToken, lockMs });
    await releasePipelineJobLock({
      jobKind,
      lockToken,
      success: result.ok,
      lastError: result.ok ? null : String(result.details?.error || ""),
      progressLabel: result.progressLabel || null,
      progress: result.details,
    });
    return {
      ...result,
      busy: false,
      jobKind,
    };
  } catch (err: any) {
    const error = String(err?.message || err || "pipeline_job_failed");
    await releasePipelineJobLock({
      jobKind,
      lockToken,
      success: false,
      lastError: error.slice(0, 500),
      progressLabel: "failed",
      progress: { error },
    });
    return {
      ok: false,
      busy: false,
      jobKind,
      processed: 0,
      succeeded: 0,
      retried: 0,
      failed: 0,
      pendingAfter: 0,
      downstreamRequested: false,
      progressLabel: "failed",
      details: { error },
      error,
    };
  }
}

async function countPendingLoadSymbols(): Promise<number> {
  const db = scalpPrisma();
  const rows = await db.$queryRaw<
    Array<{ count: bigint | number | string }>
  >(Prisma.sql`
        SELECT COUNT(*)::bigint AS count
        FROM scalp_pipeline_symbols
        WHERE active = TRUE
          AND load_status IN ('pending', 'retry_wait')
          AND COALESCE(load_next_run_at, NOW()) <= NOW();
    `);
  return Math.max(0, Math.floor(Number(rows[0]?.count || 0)));
}

async function countPendingPrepareSymbols(): Promise<number> {
  const db = scalpPrisma();
  const rows = await db.$queryRaw<
    Array<{ count: bigint | number | string }>
  >(Prisma.sql`
        SELECT COUNT(*)::bigint AS count
        FROM scalp_pipeline_symbols
        WHERE active = TRUE
          AND load_status = 'succeeded'
          AND prepare_status IN ('pending', 'retry_wait')
          AND COALESCE(prepare_next_run_at, NOW()) <= NOW();
    `);
  return Math.max(0, Math.floor(Number(rows[0]?.count || 0)));
}

async function countPendingWorkerRows(): Promise<number> {
  const db = scalpPrisma();
  const rows = await db.$queryRaw<
    Array<{ count: bigint | number | string }>
  >(Prisma.sql`
        SELECT COUNT(*)::bigint AS count
        FROM scalp_deployment_weekly_metrics
        WHERE status IN ('pending', 'retry_wait')
          AND next_run_at <= NOW();
    `);
  return Math.max(0, Math.floor(Number(rows[0]?.count || 0)));
}

async function countPendingPromotionRows(): Promise<number> {
  const db = scalpPrisma();
  const rows = await db.$queryRaw<
    Array<{ count: bigint | number | string }>
  >(Prisma.sql`
        SELECT COUNT(*)::bigint AS count
        FROM scalp_deployments
        WHERE in_universe = TRUE
          AND promotion_dirty = TRUE;
    `);
  return Math.max(0, Math.floor(Number(rows[0]?.count || 0)));
}

function resolveWeeklyPolicyDefaults(): SyncResearchWeeklyPolicy {
  return {
    enabled: envBool("SCALP_WEEKLY_ROBUSTNESS_ENABLED", true),
    topKPerSymbol: Math.max(
      1,
      toPositiveInt(envNumber("SCALP_WEEKLY_ROBUSTNESS_TOPK_PER_SYMBOL", 2), 2),
    ),
    globalMaxSymbols: Math.max(
      1,
      Math.min(
        200,
        toPositiveInt(
          envNumber("SCALP_WEEKLY_ROBUSTNESS_GLOBAL_MAX_SYMBOLS", 6),
          6,
        ),
      ),
    ),
    globalMaxDeployments: Math.max(
      1,
      Math.min(
        1_000,
        toPositiveInt(
          envNumber("SCALP_WEEKLY_ROBUSTNESS_GLOBAL_MAX_DEPLOYMENTS", 12),
          12,
        ),
      ),
    ),
    lookbackDays: Math.max(
      28,
      toPositiveInt(envNumber("SCALP_WEEKLY_ROBUSTNESS_LOOKBACK_DAYS", 91), 91),
    ),
    minCandlesPerSlice: Math.max(
      120,
      toPositiveInt(
        envNumber("SCALP_WEEKLY_ROBUSTNESS_MIN_CANDLES_PER_SLICE", 180),
        180,
      ),
    ),
    requireWinnerShortlist: envBool(
      "SCALP_WEEKLY_ROBUSTNESS_REQUIRE_WINNER_SHORTLIST",
      true,
    ),
    minSlices: Math.max(
      2,
      toPositiveInt(envNumber("SCALP_WEEKLY_ROBUSTNESS_MIN_SLICES", 8), 8),
    ),
    minProfitablePct: toBoundedPercent(
      envNumber("SCALP_WEEKLY_ROBUSTNESS_MIN_PROFITABLE_PCT", 55),
      55,
    ),
    minMedianExpectancyR: toFiniteNumber(
      envNumber("SCALP_WEEKLY_ROBUSTNESS_MIN_MEDIAN_EXPECTANCY_R", 0.02),
      0.02,
    ),
    minP25ExpectancyR: toFiniteNumber(
      envNumber("SCALP_WEEKLY_ROBUSTNESS_MIN_P25_EXPECTANCY_R", -0.02),
      -0.02,
    ),
    minWorstNetR: toFiniteNumber(
      envNumber("SCALP_WEEKLY_ROBUSTNESS_MIN_WORST_NET_R", -1.5),
      -1.5,
    ),
    maxTopWeekPnlConcentrationPct: toBoundedPercent(
      envNumber(
        "SCALP_WEEKLY_ROBUSTNESS_MAX_TOP_WEEK_PNL_CONCENTRATION_PCT",
        55,
      ),
      55,
    ),
  };
}

function median(values: number[]): number {
  if (!values.length) return 0;
  const sorted = values.slice().sort((a, b) => a - b);
  const mid = Math.floor(sorted.length / 2);
  if (sorted.length % 2 === 1) return sorted[mid] || 0;
  const left = sorted[mid - 1] || 0;
  const right = sorted[mid] || 0;
  return (left + right) / 2;
}

function mean(values: number[]): number {
  if (!values.length) return 0;
  return values.reduce((acc, row) => acc + row, 0) / values.length;
}

function quantile(values: number[], p: number): number {
  if (!values.length) return 0;
  const sorted = values.slice().sort((a, b) => a - b);
  const clampedP = Math.max(0, Math.min(1, p));
  const index = (sorted.length - 1) * clampedP;
  const low = Math.floor(index);
  const high = Math.ceil(index);
  const lowValue = sorted[low] || 0;
  const highValue = sorted[high] || 0;
  if (low === high) return lowValue;
  const weight = index - low;
  return lowValue + (highValue - lowValue) * weight;
}

function trimmedMean(values: number[], trimRatio: number): number {
  if (!values.length) return 0;
  const sorted = values.slice().sort((a, b) => a - b);
  const clampedTrim = Math.max(0, Math.min(0.49, trimRatio));
  const trimCount = Math.floor(sorted.length * clampedTrim);
  if (trimCount * 2 >= sorted.length) return mean(sorted);
  const trimmed = sorted.slice(trimCount, sorted.length - trimCount);
  return mean(trimmed.length ? trimmed : sorted);
}

function resolveWeeklySelectionTrimRatio(): number {
  return Math.max(
    0,
    Math.min(
      0.4,
      toFiniteNumber(envNumber("SCALP_WEEKLY_SELECTION_TRIM_RATIO", 0.15), 0.15),
    ),
  );
}

function topPositiveNetConcentrationPct(values: number[]): number {
  const positiveNet = values.map((value) => Math.max(0, value));
  const totalPositive = positiveNet.reduce((acc, value) => acc + value, 0);
  if (totalPositive <= 0) return 100;
  const topPositive = positiveNet.length ? Math.max(...positiveNet) : 0;
  return (topPositive / totalPositive) * 100;
}

function computeWeeklyRobustnessFromTasks(params: {
  tasks: Array<{ netR: number; expectancyR: number; maxDrawdownR: number }>;
  nowMs: number;
}): ScalpWeeklyRobustnessMetrics | null {
  if (!params.tasks.length) return null;
  const profitableSlices = params.tasks.filter((row) => row.netR > 0).length;
  const expectancyRows = params.tasks.map((row) => row.expectancyR);
  const netRows = params.tasks.map((row) => row.netR);
  const maxDrawdownRows = params.tasks.map((row) => row.maxDrawdownR);
  const slices = params.tasks.length;
  const totalNetR = netRows.reduce((acc, row) => acc + row, 0);
  const meanExpectancyR =
    expectancyRows.reduce((acc, row) => acc + row, 0) / slices;
  const trimmedMeanExpectancyR = trimmedMean(
    expectancyRows,
    resolveWeeklySelectionTrimRatio(),
  );
  const p25ExpectancyR = quantile(expectancyRows, 0.25);
  const medianExpectancyR = median(expectancyRows);
  const worstNetR = netRows.reduce(
    (acc, row) => Math.min(acc, row),
    Number.POSITIVE_INFINITY,
  );
  const worstMaxDrawdownR = maxDrawdownRows.reduce(
    (acc, row) => Math.max(acc, row),
    0,
  );
  return {
    slices,
    profitableSlices,
    profitablePct: (profitableSlices / slices) * 100,
    meanExpectancyR,
    trimmedMeanExpectancyR,
    p25ExpectancyR,
    medianExpectancyR,
    worstNetR: Number.isFinite(worstNetR) ? worstNetR : 0,
    worstMaxDrawdownR,
    topWeekPnlConcentrationPct: topPositiveNetConcentrationPct(netRows),
    totalNetR,
    evaluatedAtMs: params.nowMs,
  };
}

export async function runDiscoverPipelineJob(
  params: {
    dryRun?: boolean;
    includeLiveQuotes?: boolean;
    maxCandidates?: number;
  } = {},
): Promise<ScalpPipelineJobExecutionResult> {
  return runWithPipelineJobLock("discover", async ({ lockToken, lockMs }) => {
    const nowMs = Date.now();
    const bitgetOnly = isScalpPipelineBitgetOnlyEnabled();
    const snapshot = await runScalpSymbolDiscoveryCycle({
      dryRun: false,
      includeLiveQuotes: params.includeLiveQuotes ?? true,
      nowMs,
      maxCandidatesOverride: params.maxCandidates,
      seedTopSymbols: 12,
      seedTargetHistoryDays: 95,
      seedMaxHistoryDays: 110,
      seedChunkDays: 5,
      seedMaxRequestsPerSymbol: 30,
      seedMaxSymbolsPerRun: 12,
      seedTimeframe: "1m",
      seedOnDryRun: false,
      seedAllowBootstrapSymbols: true,
      sourceOverrides: bitgetOnly
        ? {
            includeCapitalMarketsApi: false,
            includeBitgetMarketsApi: true,
            includeDeploymentSymbols: false,
            includeHistorySymbols: false,
            requireHistoryPresence: false,
          }
        : undefined,
    });
    const discoveredSymbols = Array.from(
      new Set(
        (snapshot.selectedSymbols || [])
          .map((row) => normalizeSymbol(row))
          .filter(Boolean),
      ),
    );
    const selectedSymbols = bitgetOnly
      ? discoveredSymbols.filter((symbol) => isBitgetPipelineSymbol(symbol))
      : discoveredSymbols;
    const droppedNonBitgetSymbols = bitgetOnly
      ? discoveredSymbols.filter((symbol) => !selectedSymbols.includes(symbol))
      : [];
    const db = scalpPrisma();

    const existingRows = await db.$queryRaw<
      Array<{ symbol: string; active: boolean }>
    >(Prisma.sql`
            SELECT symbol, active
            FROM scalp_pipeline_symbols;
        `);
    const existingActive = new Set(
      existingRows
        .filter((row) => row.active)
        .map((row) => normalizeSymbol(row.symbol))
        .filter(Boolean),
    );
    const selectedSet = new Set(selectedSymbols);
    const addedSymbols = selectedSymbols.filter(
      (symbol) => !existingActive.has(symbol),
    );
    const wouldRemoveSymbols = Array.from(existingActive).filter(
      (symbol) => !selectedSet.has(symbol),
    );

    for (const symbol of selectedSymbols) {
      const previouslyActive = existingActive.has(symbol);
      await db.$executeRaw(Prisma.sql`
                INSERT INTO scalp_pipeline_symbols(
                    symbol,
                    active,
                    discover_status,
                    discover_attempts,
                    discover_next_run_at,
                    discover_error,
                    last_discovered_at,
                    load_status,
                    load_next_run_at,
                    prepare_status,
                    prepare_next_run_at,
                    updated_at
                )
                VALUES(
                    ${symbol},
                    TRUE,
                    'succeeded',
                    0,
                    NULL,
                    NULL,
                    NOW(),
                    ${previouslyActive ? "succeeded" : "pending"},
                    ${previouslyActive ? null : new Date(nowMs)},
                    ${previouslyActive ? "succeeded" : "pending"},
                    ${previouslyActive ? null : new Date(nowMs)},
                    NOW()
                )
                ON CONFLICT(symbol)
                DO UPDATE SET
                    active = TRUE,
                    discover_status = 'succeeded',
                    discover_attempts = 0,
                    discover_next_run_at = NULL,
                    discover_error = NULL,
                    last_discovered_at = NOW(),
                    load_status = CASE
                        WHEN scalp_pipeline_symbols.load_status IN ('pending', 'running', 'retry_wait') THEN scalp_pipeline_symbols.load_status
                        ELSE ${previouslyActive ? "succeeded" : "pending"}
                    END,
                    load_next_run_at = CASE
                        WHEN scalp_pipeline_symbols.load_status IN ('pending', 'running', 'retry_wait') THEN scalp_pipeline_symbols.load_next_run_at
                        ELSE ${previouslyActive ? null : new Date(nowMs)}
                    END,
                    prepare_status = CASE
                        WHEN scalp_pipeline_symbols.prepare_status IN ('pending', 'running', 'retry_wait') THEN scalp_pipeline_symbols.prepare_status
                        ELSE ${previouslyActive ? "succeeded" : "pending"}
                    END,
                    prepare_next_run_at = CASE
                        WHEN scalp_pipeline_symbols.prepare_status IN ('pending', 'running', 'retry_wait') THEN scalp_pipeline_symbols.prepare_next_run_at
                        ELSE ${previouslyActive ? null : new Date(nowMs)}
                    END,
                    updated_at = NOW();
            `);
    }

    if (selectedSymbols.length > 0) {
      await db.$executeRaw(Prisma.sql`
                UPDATE scalp_deployments
                SET
                    in_universe = TRUE,
                    retired_at = NULL,
                    updated_by = 'pipeline:discover',
                    updated_at = NOW()
                WHERE symbol IN (${Prisma.join(selectedSymbols)});
            `);
    }

    const pendingAfter = await countPendingLoadSymbols();
    await pulsePipelineJobProgress({
      jobKind: "discover",
      lockToken,
      lockMs,
      progressLabel: `selected ${selectedSymbols.length}`,
      progress: {
        selected: selectedSymbols.length,
        added: addedSymbols.length,
        removed: 0,
        wouldRemove: wouldRemoveSymbols.length,
        droppedNonBitget: droppedNonBitgetSymbols.length,
        pendingLoad: pendingAfter,
      },
    });

    return {
      ok: true,
      processed: selectedSymbols.length,
      succeeded: selectedSymbols.length,
      retried: 0,
      failed: 0,
      pendingAfter,
      downstreamRequested: pendingAfter > 0 || selectedSymbols.length > 0,
      progressLabel: `selected ${selectedSymbols.length}`,
      details: {
        generatedAtIso: snapshot.generatedAtIso,
        selected: selectedSymbols.length,
        addedSymbols,
        removedSymbols: [],
        wouldRemoveSymbols,
        droppedNonBitgetSymbols,
        candidatesEvaluated: snapshot.candidatesEvaluated,
      },
    };
  });
}

async function claimLoadSymbols(
  limit: number,
): Promise<Array<{ symbol: string; attempts: number }>> {
  const db = scalpPrisma();
  const rows = await db.$queryRaw<
    Array<{ symbol: string; attempts: number }>
  >(Prisma.sql`
        WITH candidate AS (
            SELECT symbol
            FROM scalp_pipeline_symbols
            WHERE active = TRUE
              AND load_status IN ('pending', 'retry_wait')
              AND COALESCE(load_next_run_at, NOW()) <= NOW()
            ORDER BY COALESCE(load_next_run_at, NOW()) ASC, symbol ASC
            FOR UPDATE SKIP LOCKED
            LIMIT ${limit}
        )
        UPDATE scalp_pipeline_symbols s
        SET
            load_status = 'running',
            load_attempts = s.load_attempts + 1,
            load_error = NULL,
            updated_at = NOW()
        FROM candidate c
        WHERE s.symbol = c.symbol
        RETURNING s.symbol, s.load_attempts AS attempts;
    `);
  return rows;
}

async function updateLoadSymbolStatus(params: {
  symbol: string;
  status: ScalpPipelineQueueStatus;
  attempts: number;
  error?: string | null;
  retryAfterMs?: number;
  weeksCovered?: number;
  latestWeekStartMs?: number | null;
  markPreparePending?: boolean;
}): Promise<void> {
  const db = scalpPrisma();
  const nextRunAt =
    typeof params.retryAfterMs === "number" && params.retryAfterMs > 0
      ? new Date(Date.now() + params.retryAfterMs)
      : null;
  await db.$executeRaw(Prisma.sql`
        UPDATE scalp_pipeline_symbols
        SET
            load_status = ${params.status},
            load_attempts = ${params.attempts},
            load_next_run_at = ${nextRunAt},
            load_error = ${params.error || null},
            weeks_covered = ${typeof params.weeksCovered === "number" ? params.weeksCovered : 0},
            latest_week_start = ${params.latestWeekStartMs ? new Date(params.latestWeekStartMs) : null},
            last_loaded_at = CASE WHEN ${params.status === "succeeded"} THEN NOW() ELSE last_loaded_at END,
            prepare_status = CASE
                WHEN ${params.markPreparePending === true} THEN 'pending'
                ELSE prepare_status
            END,
            prepare_next_run_at = CASE
                WHEN ${params.markPreparePending === true} THEN NOW()
                ELSE prepare_next_run_at
            END,
            prepare_error = CASE
                WHEN ${params.markPreparePending === true} THEN NULL
                ELSE prepare_error
            END,
            updated_at = NOW()
        WHERE symbol = ${params.symbol};
    `);
}

async function ensureSymbolWeeklyCoverage(params: {
  symbol: string;
  nowMs: number;
  requiredWeeks: number;
  maxRequestsPerSymbol: number;
}): Promise<{
  ok: boolean;
  weeksCovered: number;
  latestWeekStartMs: number | null;
  existingCount: number;
  fetchedCount: number;
  addedCount: number;
  error: string | null;
}> {
  const history = await loadScalpCandleHistory(params.symbol, "1m");
  const existing = history.record?.candles || [];
  const bitgetOnly = isScalpPipelineBitgetOnlyEnabled();
  if (bitgetOnly && !isBitgetPipelineSymbol(params.symbol)) {
    const coverage = countCoveredCompletedWeeks(
      existing,
      params.nowMs,
      params.requiredWeeks,
    );
    return {
      ok: false,
      weeksCovered: coverage.covered,
      latestWeekStartMs: coverage.latestWeekStartMs,
      existingCount: existing.length,
      fetchedCount: 0,
      addedCount: 0,
      error: "bitget_only_symbol_unsupported",
    };
  }
  const earliestMissingWeekStartMs = findEarliestMissingCompletedWeekStartMs(
    existing,
    params.nowMs,
    params.requiredWeeks,
  );
  const seedTfMs = Math.max(ONE_MINUTE_MS, timeframeToMs("1m"));
  const requiredCoverageStartMs = resolveCompletedWeekCoverageStartMs(
    params.nowMs,
    params.requiredWeeks,
  );
  const coverage = countCoveredCompletedWeeks(
    existing,
    params.nowMs,
    params.requiredWeeks,
  );
  const latestExistingTs = existing.length
    ? Number(existing[existing.length - 1]?.[0] || 0)
    : 0;
  const hasCoverage =
    earliestMissingWeekStartMs === null && coverage.covered >= params.requiredWeeks;

  if (hasCoverage) {
    return {
      ok: true,
      weeksCovered: coverage.covered,
      latestWeekStartMs: coverage.latestWeekStartMs,
      existingCount: existing.length,
      fetchedCount: 0,
      addedCount: 0,
      error: null,
    };
  }

  const marketMetadata = bitgetOnly
    ? null
    : await ensureScalpSymbolMarketMetadata(params.symbol, {
        fetchIfMissing: true,
      });
  const marketSource: "capital" | "bitget" = bitgetOnly
    ? "bitget"
    : marketMetadata?.source === "bitget"
      ? "bitget"
      : "capital";
  const epic = bitgetOnly
    ? params.symbol
    : marketMetadata?.epic ||
      (marketSource === "bitget"
        ? params.symbol
        : (await resolveCapitalEpicRuntime(params.symbol)).epic);

  const incrementalFetchFromMs = (() => {
    if (earliestMissingWeekStartMs !== null) {
      return Math.max(earliestMissingWeekStartMs, requiredCoverageStartMs);
    }
    if (
      existing.length > 0 &&
      Number.isFinite(latestExistingTs) &&
      latestExistingTs > 0
    ) {
      return Math.max(
        requiredCoverageStartMs,
        Math.floor(latestExistingTs - seedTfMs * 2),
      );
    }
    return requiredCoverageStartMs;
  })();
  const fetchToMs = params.nowMs;

  const fetchedRaw =
    marketSource === "bitget"
      ? await fetchBitgetCandlesByEpicDateRange(
          epic,
          "1m",
          incrementalFetchFromMs,
          fetchToMs,
          {
            maxPerRequest: BITGET_HISTORY_CANDLES_MAX_LIMIT,
            maxRequests: params.maxRequestsPerSymbol,
          },
        )
      : await fetchCapitalCandlesByEpicDateRange(
          epic,
          "1m",
          incrementalFetchFromMs,
          fetchToMs,
          {
            maxPerRequest: 1000,
            maxRequests: params.maxRequestsPerSymbol,
            debug: false,
            debugLabel: `pipeline-load:${params.symbol}:1m`,
          },
        );
  const fetched = normalizeFetchedCandles(fetchedRaw);
  const merged = mergeScalpCandleHistory(existing, fetched);

  if (merged.length > existing.length) {
    await saveScalpCandleHistory({
      symbol: params.symbol,
      timeframe: "1m",
      epic,
      source: marketSource,
      candles: merged,
    });
  }

  const mergedMissing = findEarliestMissingCompletedWeekStartMs(
    merged,
    params.nowMs,
    params.requiredWeeks,
  );
  const mergedCoverage = countCoveredCompletedWeeks(
    merged,
    params.nowMs,
    params.requiredWeeks,
  );
  const mergedHasCoverage =
    mergedMissing === null && mergedCoverage.covered >= params.requiredWeeks;

  return {
    ok: mergedHasCoverage,
    weeksCovered: mergedCoverage.covered,
    latestWeekStartMs: mergedCoverage.latestWeekStartMs,
    existingCount: existing.length,
    fetchedCount: fetched.length,
    addedCount: Math.max(0, merged.length - existing.length),
    error: mergedHasCoverage ? null : "insufficient_completed_week_coverage",
  };
}

export async function runLoadCandlesPipelineJob(
  params: {
    batchSize?: number;
    maxAttempts?: number;
  } = {},
): Promise<ScalpPipelineJobExecutionResult> {
  return runWithPipelineJobLock(
    "load_candles",
    async ({ lockToken, lockMs }) => {
      const requiredWeeks = resolveRequiredSuccessiveWeeks();
      const batchSize = Math.max(
        1,
        Math.min(40, toPositiveInt(params.batchSize, 6)),
      );
      const maxAttempts = Math.max(
        1,
        Math.min(20, toPositiveInt(params.maxAttempts, 5)),
      );
      const retryAfterMs = Math.max(
        5 * 60_000,
        Math.min(
          3 * 60 * 60_000,
          toPositiveInt(process.env.SCALP_PIPELINE_LOAD_RETRY_MS, 15 * 60_000),
        ),
      );
      const maxRequestsPerSymbol = Math.max(
        40,
        Math.min(
          2500,
          toPositiveInt(
            process.env.SCALP_PIPELINE_LOAD_MAX_REQUESTS_PER_SYMBOL,
            600,
          ),
        ),
      );

      const claimed = await claimLoadSymbols(batchSize);
      let succeeded = 0;
      let retried = 0;
      let failed = 0;

      for (let idx = 0; idx < claimed.length; idx += 1) {
        const row = claimed[idx]!;
        const symbol = normalizeSymbol(row.symbol);
        if (!symbol) continue;
        try {
          const coverage = await ensureSymbolWeeklyCoverage({
            symbol,
            nowMs: Date.now(),
            requiredWeeks,
            maxRequestsPerSymbol,
          });
          if (coverage.ok) {
            await updateLoadSymbolStatus({
              symbol,
              status: "succeeded",
              attempts: row.attempts,
              error: null,
              weeksCovered: coverage.weeksCovered,
              latestWeekStartMs: coverage.latestWeekStartMs,
              markPreparePending: true,
            });
            succeeded += 1;
          } else if (row.attempts >= maxAttempts) {
            await updateLoadSymbolStatus({
              symbol,
              status: "failed",
              attempts: row.attempts,
              error: coverage.error,
              weeksCovered: coverage.weeksCovered,
              latestWeekStartMs: coverage.latestWeekStartMs,
            });
            failed += 1;
          } else {
            await updateLoadSymbolStatus({
              symbol,
              status: "retry_wait",
              attempts: row.attempts,
              error: coverage.error,
              retryAfterMs,
              weeksCovered: coverage.weeksCovered,
              latestWeekStartMs: coverage.latestWeekStartMs,
            });
            retried += 1;
          }
        } catch (err: any) {
          const message = String(
            err?.message || err || "load_symbol_failed",
          ).slice(0, 500);
          if (row.attempts >= maxAttempts) {
            await updateLoadSymbolStatus({
              symbol,
              status: "failed",
              attempts: row.attempts,
              error: message,
            });
            failed += 1;
          } else {
            await updateLoadSymbolStatus({
              symbol,
              status: "retry_wait",
              attempts: row.attempts,
              error: message,
              retryAfterMs,
            });
            retried += 1;
          }
        }
        await pulsePipelineJobProgress({
          jobKind: "load_candles",
          lockToken,
          lockMs,
          progressLabel: `processed ${idx + 1}/${claimed.length}`,
          progress: {
            processed: idx + 1,
            total: claimed.length,
            succeeded,
            retried,
            failed,
          },
        });
      }

      const pendingAfter = await countPendingLoadSymbols();
      return {
        ok: true,
        processed: claimed.length,
        succeeded,
        retried,
        failed,
        pendingAfter,
        downstreamRequested: succeeded > 0,
        progressLabel:
          claimed.length > 0 ? `processed ${claimed.length}` : "idle",
        details: {
          requiredWeeks,
          claimed: claimed.length,
          succeeded,
          retried,
          failed,
        },
      };
    },
  );
}

async function claimPrepareSymbols(
  limit: number,
): Promise<Array<{ symbol: string; attempts: number }>> {
  const db = scalpPrisma();
  const rows = await db.$queryRaw<
    Array<{ symbol: string; attempts: number }>
  >(Prisma.sql`
        WITH candidate AS (
            SELECT symbol
            FROM scalp_pipeline_symbols
            WHERE active = TRUE
              AND load_status = 'succeeded'
              AND prepare_status IN ('pending', 'retry_wait')
              AND COALESCE(prepare_next_run_at, NOW()) <= NOW()
            ORDER BY COALESCE(prepare_next_run_at, NOW()) ASC, symbol ASC
            FOR UPDATE SKIP LOCKED
            LIMIT ${limit}
        )
        UPDATE scalp_pipeline_symbols s
        SET
            prepare_status = 'running',
            prepare_attempts = s.prepare_attempts + 1,
            prepare_error = NULL,
            updated_at = NOW()
        FROM candidate c
        WHERE s.symbol = c.symbol
        RETURNING s.symbol, s.prepare_attempts AS attempts;
    `);
  return rows;
}

async function upsertWeeklyQueueRowsForDeployment(params: {
  deploymentId: string;
  symbol: string;
  strategyId: string;
  tuneId: string;
  nowMs: number;
  requiredWeeks: number;
  refreshRecentWeeks: number;
}): Promise<number> {
  const currentWeekStart = startOfWeekMondayUtc(params.nowMs);
  const firstWeekStart = currentWeekStart - params.requiredWeeks * ONE_WEEK_MS;
  const refreshStart =
    currentWeekStart - Math.max(1, params.refreshRecentWeeks) * ONE_WEEK_MS;
  const db = scalpPrisma();
  let upserted = 0;
  for (
    let weekStartMs = firstWeekStart;
    weekStartMs < currentWeekStart;
    weekStartMs += ONE_WEEK_MS
  ) {
    const weekEndMs = weekStartMs + ONE_WEEK_MS;
    await db.$executeRaw(Prisma.sql`
            INSERT INTO scalp_deployment_weekly_metrics(
                deployment_id,
                symbol,
                strategy_id,
                tune_id,
                week_start,
                week_end,
                status,
                attempts,
                next_run_at,
                created_at,
                updated_at
            )
            VALUES(
                ${params.deploymentId},
                ${params.symbol},
                ${params.strategyId},
                ${params.tuneId},
                ${new Date(weekStartMs)},
                ${new Date(weekEndMs)},
                'pending',
                0,
                NOW(),
                NOW(),
                NOW()
            )
            ON CONFLICT(deployment_id, week_start)
            DO UPDATE SET
                symbol = EXCLUDED.symbol,
                strategy_id = EXCLUDED.strategy_id,
                tune_id = EXCLUDED.tune_id,
                week_end = EXCLUDED.week_end,
                status = CASE
                    WHEN scalp_deployment_weekly_metrics.status IN ('pending', 'running', 'retry_wait') THEN scalp_deployment_weekly_metrics.status
                    WHEN EXCLUDED.week_start >= ${new Date(refreshStart)} THEN 'pending'
                    ELSE scalp_deployment_weekly_metrics.status
                END,
                attempts = CASE
                    WHEN EXCLUDED.week_start >= ${new Date(refreshStart)}
                        AND scalp_deployment_weekly_metrics.status NOT IN ('running')
                    THEN 0
                    ELSE scalp_deployment_weekly_metrics.attempts
                END,
                next_run_at = CASE
                    WHEN EXCLUDED.week_start >= ${new Date(refreshStart)}
                        AND scalp_deployment_weekly_metrics.status NOT IN ('running')
                    THEN NOW()
                    ELSE scalp_deployment_weekly_metrics.next_run_at
                END,
                error_code = CASE
                    WHEN EXCLUDED.week_start >= ${new Date(refreshStart)}
                        AND scalp_deployment_weekly_metrics.status NOT IN ('running')
                    THEN NULL
                    ELSE scalp_deployment_weekly_metrics.error_code
                END,
                error_message = CASE
                    WHEN EXCLUDED.week_start >= ${new Date(refreshStart)}
                        AND scalp_deployment_weekly_metrics.status NOT IN ('running')
                    THEN NULL
                    ELSE scalp_deployment_weekly_metrics.error_message
                END,
                updated_at = NOW();
        `);
    upserted += 1;
  }
  return upserted;
}

async function updatePrepareSymbolStatus(params: {
  symbol: string;
  status: ScalpPipelineQueueStatus;
  attempts: number;
  preparedDeployments: number;
  error?: string | null;
  retryAfterMs?: number;
}): Promise<void> {
  const db = scalpPrisma();
  const nextRunAt =
    typeof params.retryAfterMs === "number" && params.retryAfterMs > 0
      ? new Date(Date.now() + params.retryAfterMs)
      : null;
  await db.$executeRaw(Prisma.sql`
        UPDATE scalp_pipeline_symbols
        SET
            prepare_status = ${params.status},
            prepare_attempts = ${params.attempts},
            prepare_next_run_at = ${nextRunAt},
            prepare_error = ${params.error || null},
            prepared_deployments = ${Math.max(0, Math.floor(params.preparedDeployments))},
            last_prepared_at = CASE WHEN ${params.status === "succeeded"} THEN NOW() ELSE last_prepared_at END,
            updated_at = NOW()
        WHERE symbol = ${params.symbol};
    `);
}

export async function runPreparePipelineJob(
  params: {
    batchSize?: number;
    maxAttempts?: number;
  } = {},
): Promise<ScalpPipelineJobExecutionResult> {
  return runWithPipelineJobLock("prepare", async ({ lockToken, lockMs }) => {
    const bitgetOnly = isScalpPipelineBitgetOnlyEnabled();
    const batchSize = Math.max(
      1,
      Math.min(30, toPositiveInt(params.batchSize, 4)),
    );
    const maxAttempts = Math.max(
      1,
      Math.min(20, toPositiveInt(params.maxAttempts, 5)),
    );
    const retryAfterMs = Math.max(
      5 * 60_000,
      Math.min(
        3 * 60 * 60_000,
        toPositiveInt(process.env.SCALP_PIPELINE_PREPARE_RETRY_MS, 10 * 60_000),
      ),
    );
    const requiredWeeks = resolveRequiredSuccessiveWeeks();
    const refreshRecentWeeks = Math.max(
      1,
      Math.min(
        8,
        toPositiveInt(process.env.SCALP_PIPELINE_WORKER_REFRESH_WEEKS, 2),
      ),
    );

    const policy = await loadScalpSymbolDiscoveryPolicy();
    const strategies = new Set(listScalpStrategies().map((row) => row.id));
    const claimed = await claimPrepareSymbols(batchSize);
    const db = scalpPrisma();

    let succeeded = 0;
    let retried = 0;
    let failed = 0;
    let queuedWeeklyRows = 0;

    for (let idx = 0; idx < claimed.length; idx += 1) {
      const row = claimed[idx]!;
      const symbol = normalizeSymbol(row.symbol);
      if (!symbol) continue;
      if (bitgetOnly && !isBitgetPipelineSymbol(symbol)) {
        await db.$executeRaw(Prisma.sql`
                    UPDATE scalp_deployments
                    SET
                        in_universe = FALSE,
                        retired_at = NOW(),
                        enabled = FALSE,
                        worker_dirty = FALSE,
                        promotion_dirty = FALSE,
                        updated_by = 'pipeline:prepare',
                        updated_at = NOW()
                    WHERE symbol = ${symbol};
                `);
        await updatePrepareSymbolStatus({
          symbol,
          status: "failed",
          attempts: row.attempts,
          preparedDeployments: 0,
          error: "bitget_only_symbol_unsupported",
        });
        failed += 1;
        await pulsePipelineJobProgress({
          jobKind: "prepare",
          lockToken,
          lockMs,
          progressLabel: `processed ${idx + 1}/${claimed.length}`,
          progress: {
            processed: idx + 1,
            total: claimed.length,
            succeeded,
            retried,
            failed,
            queuedWeeklyRows,
          },
        });
        continue;
      }
      try {
        const strategyIds = resolveRecommendedStrategiesForSymbol(
          symbol,
          policy.strategyAllowlist,
        ).filter((id) => strategies.has(id));
        const selectedStrategies =
          strategyIds.length > 0
            ? strategyIds
            : Array.from(strategies).slice(0, 1);
        const symbolVenue = await resolvePipelineDeploymentVenue(symbol);
        const existingDeployments = await db.$queryRaw<
          Array<{
            deploymentId: string;
            strategyId: string;
            tuneId: string;
          }>
        >(Prisma.sql`
                    SELECT
                        deployment_id AS "deploymentId",
                        strategy_id AS "strategyId",
                        tune_id AS "tuneId"
                    FROM scalp_deployments
                    WHERE symbol = ${symbol};
                `);
        const existingByKey = new Map<string, string>();
        for (const dep of existingDeployments) {
          const key = `${dep.strategyId}::${dep.tuneId}`;
          if (existingByKey.has(key)) continue;
          const depVenue = resolveScalpDeploymentVenueFromId(dep.deploymentId);
          if (depVenue !== symbolVenue) continue;
          existingByKey.set(key, dep.deploymentId);
        }
        const preparedIds: string[] = [];

        for (const strategyId of selectedStrategies) {
          const variants = buildScalpResearchTuneVariants({
            symbol,
            strategyId,
            includeBaseline: true,
            maxVariantsPerStrategy: 4,
          }).slice(0, 4);
          const rows = variants.map((variant) => ({
            deploymentId:
              existingByKey.get(`${strategyId}::${variant.tuneId}`) ||
              resolveScalpDeployment({
                venue: symbolVenue,
                symbol,
                strategyId,
                tuneId: variant.tuneId,
              }).deploymentId,
            symbol,
            strategyId,
            tuneId: variant.tuneId,
            source: "matrix" as const,
            enabled: false,
            configOverride: variant.configOverride || null,
            updatedBy: "pipeline:prepare",
          }));
          const upserted = await upsertScalpDeploymentRegistryEntriesBulk(rows);
          for (const entry of upserted.entries) {
            preparedIds.push(entry.deploymentId);
          }
        }

        const uniqPreparedIds = Array.from(new Set(preparedIds));
        if (uniqPreparedIds.length > 0) {
          await db.$executeRaw(Prisma.sql`
                        UPDATE scalp_deployments
                        SET
                            in_universe = TRUE,
                            retired_at = NULL,
                            worker_dirty = TRUE,
                            updated_by = 'pipeline:prepare',
                            last_prepared_at = NOW(),
                            updated_at = NOW()
                        WHERE deployment_id IN (${Prisma.join(uniqPreparedIds)});
                    `);
          await db.$executeRaw(Prisma.sql`
                        UPDATE scalp_deployments
                        SET
                            in_universe = FALSE,
                            retired_at = NOW(),
                            enabled = FALSE,
                            worker_dirty = FALSE,
                            promotion_dirty = FALSE,
                            updated_by = 'pipeline:prepare',
                            updated_at = NOW()
                        WHERE symbol = ${symbol}
                          AND deployment_id NOT IN (${Prisma.join(uniqPreparedIds)});
                    `);
        }

        for (const deploymentId of uniqPreparedIds) {
          const depRows = await db.$queryRaw<
            Array<{
              deploymentId: string;
              symbol: string;
              strategyId: string;
              tuneId: string;
            }>
          >(Prisma.sql`
                        SELECT
                            deployment_id AS "deploymentId",
                            symbol,
                            strategy_id AS "strategyId",
                            tune_id AS "tuneId"
                        FROM scalp_deployments
                        WHERE deployment_id = ${deploymentId}
                        LIMIT 1;
                    `);
          const dep = depRows[0];
          if (!dep) continue;
          queuedWeeklyRows += await upsertWeeklyQueueRowsForDeployment({
            deploymentId: dep.deploymentId,
            symbol: dep.symbol,
            strategyId: dep.strategyId,
            tuneId: dep.tuneId,
            nowMs: Date.now(),
            requiredWeeks,
            refreshRecentWeeks,
          });
        }

        await updatePrepareSymbolStatus({
          symbol,
          status: "succeeded",
          attempts: row.attempts,
          preparedDeployments: uniqPreparedIds.length,
          error: null,
        });
        succeeded += 1;
      } catch (err: any) {
        const message = String(
          err?.message || err || "prepare_symbol_failed",
        ).slice(0, 500);
        if (row.attempts >= maxAttempts) {
          await updatePrepareSymbolStatus({
            symbol,
            status: "failed",
            attempts: row.attempts,
            preparedDeployments: 0,
            error: message,
          });
          failed += 1;
        } else {
          await updatePrepareSymbolStatus({
            symbol,
            status: "retry_wait",
            attempts: row.attempts,
            preparedDeployments: 0,
            error: message,
            retryAfterMs,
          });
          retried += 1;
        }
      }
      await pulsePipelineJobProgress({
        jobKind: "prepare",
        lockToken,
        lockMs,
        progressLabel: `processed ${idx + 1}/${claimed.length}`,
        progress: {
          processed: idx + 1,
          total: claimed.length,
          succeeded,
          retried,
          failed,
          queuedWeeklyRows,
        },
      });
    }

    const pendingAfter = await countPendingPrepareSymbols();
    return {
      ok: true,
      processed: claimed.length,
      succeeded,
      retried,
      failed,
      pendingAfter,
      downstreamRequested: queuedWeeklyRows > 0,
      progressLabel:
        claimed.length > 0 ? `processed ${claimed.length}` : "idle",
      details: {
        claimed: claimed.length,
        succeeded,
        retried,
        failed,
        queuedWeeklyRows,
      },
    };
  });
}

async function claimWorkerRows(
  limit: number,
  workerId: string,
): Promise<
  Array<{
    id: bigint;
    deploymentId: string;
    symbol: string;
    strategyId: string;
    tuneId: string;
    weekStart: Date;
    weekEnd: Date;
    attempts: number;
  }>
> {
  const db = scalpPrisma();
  const rows = await db.$queryRaw<
    Array<{
      id: bigint;
      deploymentId: string;
      symbol: string;
      strategyId: string;
      tuneId: string;
      weekStart: Date;
      weekEnd: Date;
      attempts: number;
    }>
  >(Prisma.sql`
        WITH candidate AS (
            SELECT id
            FROM scalp_deployment_weekly_metrics
            WHERE status IN ('pending', 'retry_wait')
              AND next_run_at <= NOW()
            ORDER BY next_run_at ASC, week_start DESC, id ASC
            FOR UPDATE SKIP LOCKED
            LIMIT ${limit}
        )
        UPDATE scalp_deployment_weekly_metrics m
        SET
            status = 'running',
            attempts = m.attempts + 1,
            worker_id = ${workerId},
            started_at = NOW(),
            updated_at = NOW()
        FROM candidate c
        WHERE m.id = c.id
        RETURNING
            m.id,
            m.deployment_id AS "deploymentId",
            m.symbol,
            m.strategy_id AS "strategyId",
            m.tune_id AS "tuneId",
            m.week_start AS "weekStart",
            m.week_end AS "weekEnd",
            m.attempts;
    `);
  return rows;
}

async function completeWorkerRow(params: {
  id: bigint;
  workerId: string;
  success: boolean;
  retry: boolean;
  retryAfterMs?: number;
  errorCode?: string | null;
  errorMessage?: string | null;
  metrics?: {
    trades: number;
    winRatePct: number;
    netR: number;
    expectancyR: number;
    profitFactor: number | null;
    maxDrawdownR: number;
    avgHoldMinutes: number;
    netPnlUsd: number;
    grossProfitR: number;
    grossLossR: number;
  };
}): Promise<void> {
  const db = scalpPrisma();
  const status = params.success
    ? "succeeded"
    : params.retry
      ? "retry_wait"
      : "failed";
  const nextRunAt =
    params.retry && params.retryAfterMs
      ? new Date(Date.now() + params.retryAfterMs)
      : null;
  const metricsJson = params.metrics ? JSON.stringify(params.metrics) : null;
  await db.$executeRaw(Prisma.sql`
        UPDATE scalp_deployment_weekly_metrics
        SET
            status = ${status},
            next_run_at = COALESCE(${nextRunAt}, next_run_at),
            worker_id = NULL,
            finished_at = NOW(),
            error_code = ${params.errorCode || null},
            error_message = ${params.errorMessage || null},
            trades = ${params.metrics?.trades ?? null},
            win_rate_pct = ${params.metrics?.winRatePct ?? null},
            net_r = ${params.metrics?.netR ?? null},
            expectancy_r = ${params.metrics?.expectancyR ?? null},
            profit_factor = ${params.metrics?.profitFactor ?? null},
            max_drawdown_r = ${params.metrics?.maxDrawdownR ?? null},
            avg_hold_minutes = ${params.metrics?.avgHoldMinutes ?? null},
            net_pnl_usd = ${params.metrics?.netPnlUsd ?? null},
            gross_profit_r = ${params.metrics?.grossProfitR ?? null},
            gross_loss_r = ${params.metrics?.grossLossR ?? null},
            metrics_json = ${metricsJson}::jsonb,
            updated_at = NOW()
        WHERE id = ${params.id}
          AND (worker_id = ${params.workerId} OR worker_id IS NULL);
    `);
}

export async function runWorkerPipelineJob(
  params: {
    batchSize?: number;
    maxAttempts?: number;
    minCandlesPerWeek?: number;
  } = {},
): Promise<ScalpPipelineJobExecutionResult> {
  return runWithPipelineJobLock("worker", async ({ lockToken, lockMs }) => {
    const workerId = lockToken;
    const batchSize = Math.max(
      1,
      Math.min(400, toPositiveInt(params.batchSize, 80)),
    );
    const maxAttempts = Math.max(
      1,
      Math.min(20, toPositiveInt(params.maxAttempts, 5)),
    );
    const retryAfterMs = Math.max(
      5 * 60_000,
      Math.min(
        3 * 60 * 60_000,
        toPositiveInt(process.env.SCALP_PIPELINE_WORKER_RETRY_MS, 15 * 60_000),
      ),
    );
    const minCandlesPerWeek = Math.max(
      60,
      Math.min(20_000, toPositiveInt(params.minCandlesPerWeek, 180)),
    );

    const claimed = await claimWorkerRows(batchSize, workerId);
    let succeeded = 0;
    let retried = 0;
    let failed = 0;
    const db = scalpPrisma();

    for (let idx = 0; idx < claimed.length; idx += 1) {
      const row = claimed[idx]!;
      try {
        const depRows = await db.$queryRaw<
          Array<{
            deploymentId: string;
            symbol: string;
            strategyId: string;
            tuneId: string;
            configOverride: unknown;
          }>
        >(Prisma.sql`
                    SELECT
                        deployment_id AS "deploymentId",
                        symbol,
                        strategy_id AS "strategyId",
                        tune_id AS "tuneId",
                        config_override AS "configOverride"
                    FROM scalp_deployments
                    WHERE deployment_id = ${row.deploymentId}
                    LIMIT 1;
                `);
        const dep = depRows[0];
        if (!dep) {
          await completeWorkerRow({
            id: row.id,
            workerId,
            success: false,
            retry: false,
            errorCode: "deployment_missing",
            errorMessage: "deployment_missing",
          });
          failed += 1;
          continue;
        }
        const history = await loadScalpCandleHistory(dep.symbol, "1m");
        const candles = (history.record?.candles || []).filter((candle) => {
          const ts = Number(candle?.[0] || 0);
          return ts >= row.weekStart.getTime() && ts < row.weekEnd.getTime();
        });
        if (candles.length < minCandlesPerWeek) {
          const retry = row.attempts < maxAttempts;
          await completeWorkerRow({
            id: row.id,
            workerId,
            success: false,
            retry,
            retryAfterMs,
            errorCode: "insufficient_weekly_candles",
            errorMessage: `insufficient_weekly_candles:${candles.length}`,
          });
          if (retry) retried += 1;
          else failed += 1;
          continue;
        }

        const meta = await ensureScalpSymbolMarketMetadata(dep.symbol, {
          fetchIfMissing: true,
        });
        const deploymentRef = resolveScalpDeployment({
          symbol: dep.symbol,
          strategyId: dep.strategyId,
          tuneId: dep.tuneId,
          deploymentId: dep.deploymentId,
        });
        const runtime = buildScalpReplayRuntimeFromDeployment({
          deployment: deploymentRef,
          configOverride: asJsonObject(dep.configOverride) as any,
        });
        const replay = await runScalpReplay({
          candles: toReplayCandles(candles, runtime.defaultSpreadPips),
          pipSize: pipSizeForScalpSymbol(dep.symbol, meta),
          config: runtime,
          captureTimeline: false,
          symbolMeta: meta,
        });

        const metrics = {
          trades: replay.summary.trades,
          winRatePct: replay.summary.winRatePct,
          netR: replay.summary.netR,
          expectancyR: replay.summary.expectancyR,
          profitFactor: replay.summary.profitFactor,
          maxDrawdownR: replay.summary.maxDrawdownR,
          avgHoldMinutes: replay.summary.avgHoldMinutes,
          netPnlUsd: replay.summary.netPnlUsd,
          grossProfitR: replay.summary.grossProfitR,
          grossLossR: replay.summary.grossLossR,
        };
        await completeWorkerRow({
          id: row.id,
          workerId,
          success: true,
          retry: false,
          metrics,
        });
        await db.$executeRaw(Prisma.sql`
                    UPDATE scalp_deployments
                    SET
                        promotion_dirty = TRUE,
                        worker_dirty = FALSE,
                        updated_by = 'pipeline:worker',
                        updated_at = NOW()
                    WHERE deployment_id = ${dep.deploymentId};
                `);
        succeeded += 1;
      } catch (err: any) {
        const message = String(
          err?.message || err || "worker_row_failed",
        ).slice(0, 500);
        const retry = row.attempts < maxAttempts;
        await completeWorkerRow({
          id: row.id,
          workerId,
          success: false,
          retry,
          retryAfterMs,
          errorCode: "worker_replay_failed",
          errorMessage: message,
        });
        if (retry) retried += 1;
        else failed += 1;
      }

      await pulsePipelineJobProgress({
        jobKind: "worker",
        lockToken,
        lockMs,
        progressLabel: `processed ${idx + 1}/${claimed.length}`,
        progress: {
          processed: idx + 1,
          total: claimed.length,
          succeeded,
          retried,
          failed,
        },
      });
    }

    const pendingAfter = await countPendingWorkerRows();
    return {
      ok: true,
      processed: claimed.length,
      succeeded,
      retried,
      failed,
      pendingAfter,
      downstreamRequested: succeeded > 0,
      progressLabel:
        claimed.length > 0 ? `processed ${claimed.length}` : "idle",
      details: {
        claimed: claimed.length,
        succeeded,
        retried,
        failed,
      },
    };
  });
}

export async function runPromotionPipelineJob(
  params: {
    batchSize?: number;
  } = {},
): Promise<ScalpPipelineJobExecutionResult> {
  return runWithPipelineJobLock("promotion", async ({ lockToken, lockMs }) => {
    const batchSize = Math.max(
      1,
      Math.min(600, toPositiveInt(params.batchSize, 200)),
    );
    const policy = resolveWeeklyPolicyDefaults();
    const nowMs = Date.now();
    const requiredWeeks = resolvePromotionFreshWeeks();
    const windowToTs = startOfWeekMondayUtc(nowMs);
    const windowFromTs = windowToTs - policy.lookbackDays * ONE_DAY_MS;

    const db = scalpPrisma();
    const dirtyRows = await db.$queryRaw<
      Array<{ deploymentId: string }>
    >(Prisma.sql`
            SELECT deployment_id AS "deploymentId"
            FROM scalp_deployments
            WHERE in_universe = TRUE
              AND promotion_dirty = TRUE
            ORDER BY updated_at ASC, deployment_id ASC
            LIMIT ${batchSize};
        `);
    if (!dirtyRows.length) {
      return {
        ok: true,
        processed: 0,
        succeeded: 0,
        retried: 0,
        failed: 0,
        pendingAfter: 0,
        downstreamRequested: false,
        progressLabel: "idle",
        details: {
          reason: "no_promotion_dirty_deployments",
        },
      };
    }

    const dirtySet = new Set(dirtyRows.map((row) => row.deploymentId));
    const allDeployments = await listScalpDeploymentRegistryEntries();
    const inUniverseRows = await db.$queryRaw<
      Array<{
        deploymentId: string;
        inUniverse: boolean;
        enabled: boolean;
        promotionGate: unknown;
      }>
    >(Prisma.sql`
            SELECT
                deployment_id AS "deploymentId",
                in_universe AS "inUniverse",
                enabled,
                promotion_gate AS "promotionGate"
            FROM scalp_deployments;
        `);
    const inUniverseByDeploymentId = new Map(
      inUniverseRows.map((row) => [row.deploymentId, row]),
    );

    const consideredDeployments = allDeployments.filter(
      (row) =>
        inUniverseByDeploymentId.get(row.deploymentId)?.inUniverse === true,
    );
    const consideredIds = consideredDeployments.map((row) => row.deploymentId);
    if (!consideredIds.length) {
      return {
        ok: true,
        processed: dirtyRows.length,
        succeeded: dirtyRows.length,
        retried: 0,
        failed: 0,
        pendingAfter: 0,
        downstreamRequested: false,
        progressLabel: "idle",
        details: {
          reason: "no_in_universe_deployments",
        },
      };
    }

    const metricsRows = await db.$queryRaw<
      Array<{
        deploymentId: string;
        symbol: string;
        strategyId: string;
        tuneId: string;
        weekStart: Date;
        weekEnd: Date;
        trades: number | null;
        winRatePct: number | null;
        netR: number | null;
        expectancyR: number | null;
        profitFactor: number | null;
        maxDrawdownR: number | null;
        avgHoldMinutes: number | null;
        netPnlUsd: number | null;
        grossProfitR: number | null;
        grossLossR: number | null;
      }>
    >(Prisma.sql`
            SELECT
                deployment_id AS "deploymentId",
                symbol,
                strategy_id AS "strategyId",
                tune_id AS "tuneId",
                week_start AS "weekStart",
                week_end AS "weekEnd",
                trades,
                win_rate_pct AS "winRatePct",
                net_r AS "netR",
                expectancy_r AS "expectancyR",
                profit_factor AS "profitFactor",
                max_drawdown_r AS "maxDrawdownR",
                avg_hold_minutes AS "avgHoldMinutes",
                net_pnl_usd AS "netPnlUsd",
                gross_profit_r AS "grossProfitR",
                gross_loss_r AS "grossLossR"
            FROM scalp_deployment_weekly_metrics
            WHERE deployment_id IN (${Prisma.join(consideredIds)})
              AND status = 'succeeded'
              AND week_start >= ${new Date(windowFromTs)}
              AND week_start < ${new Date(windowToTs)}
            ORDER BY week_start ASC;
        `);

    const tasksByDeploymentId = new Map<string, Array<any>>();
    for (const row of metricsRows) {
      const bucket = tasksByDeploymentId.get(row.deploymentId) || [];
      bucket.push({
        version: 1,
        cycleId: "pipeline",
        taskId: `${row.deploymentId}:${row.weekStart.getTime()}`,
        symbol: row.symbol,
        strategyId: row.strategyId,
        tuneId: row.tuneId,
        deploymentId: row.deploymentId,
        windowFromTs: row.weekStart.getTime(),
        windowToTs: row.weekEnd.getTime(),
        status: "completed",
        attempts: 1,
        createdAtMs: row.weekStart.getTime(),
        updatedAtMs: nowMs,
        workerId: null,
        startedAtMs: null,
        finishedAtMs: null,
        errorCode: null,
        errorMessage: null,
        result: {
          symbol: row.symbol,
          strategyId: row.strategyId,
          tuneId: row.tuneId,
          deploymentId: row.deploymentId,
          windowFromTs: row.weekStart.getTime(),
          windowToTs: row.weekEnd.getTime(),
          trades: Math.max(0, Math.floor(Number(row.trades || 0))),
          winRatePct: toFiniteNumber(row.winRatePct, 0),
          netR: toFiniteNumber(row.netR, 0),
          expectancyR: toFiniteNumber(row.expectancyR, 0),
          profitFactor:
            row.profitFactor === null || row.profitFactor === undefined
              ? null
              : toFiniteNumber(row.profitFactor, Number.NaN),
          maxDrawdownR: Math.max(0, toFiniteNumber(row.maxDrawdownR, 0)),
          avgHoldMinutes: toFiniteNumber(row.avgHoldMinutes, 0),
          netPnlUsd: toFiniteNumber(row.netPnlUsd, 0),
          grossProfitR: toFiniteNumber(row.grossProfitR, 0),
          grossLossR: toFiniteNumber(row.grossLossR, 0),
        },
      });
      tasksByDeploymentId.set(row.deploymentId, bucket);
    }

    const freshReadyTasks: any[] = [];
    const freshnessByDeploymentId = new Map<
      string,
      ReturnType<typeof evaluateFreshCompletedDeploymentWeeks>
    >();
    const gapSymbols = new Set<string>();
    const gapWindowsByDeploymentId = new Map<
      string,
      {
        windowFromTs: number;
        windowToTs: number;
        missingWeekStarts: number[];
        missingWeeks: number;
      }
    >();
    for (const deployment of consideredDeployments) {
      const deploymentTasks =
        tasksByDeploymentId.get(deployment.deploymentId) || [];
      const freshness = evaluateFreshCompletedDeploymentWeeks({
        tasks: deploymentTasks,
        nowMs,
        requiredWeeks,
      });
      freshnessByDeploymentId.set(deployment.deploymentId, freshness);
      if (freshness.ready) {
        for (const task of freshness.readyTasks) {
          freshReadyTasks.push(task);
        }
      } else if (freshness.missingWeeks > 0) {
        gapSymbols.add(deployment.symbol);
        gapWindowsByDeploymentId.set(deployment.deploymentId, {
          windowFromTs: freshness.windowFromTs,
          windowToTs: freshness.windowToTs,
          missingWeekStarts: freshness.missingWeekStarts || [],
          missingWeeks: freshness.missingWeeks,
        });
      }
    }

    let nudgedLoadSymbols = 0;
    let nudgedWorkerRows = 0;
    if (gapSymbols.size > 0) {
      nudgedLoadSymbols = Number(
        await db.$executeRaw(Prisma.sql`
                UPDATE scalp_pipeline_symbols
                SET
                    load_status = CASE
                        WHEN load_status = 'running' THEN load_status
                        ELSE 'pending'
                    END,
                    load_next_run_at = CASE
                        WHEN load_status = 'running' THEN load_next_run_at
                        ELSE NOW()
                    END,
                    load_error = CASE
                        WHEN load_status = 'running' THEN load_error
                        ELSE NULL
                    END,
                    updated_at = NOW()
                WHERE active = TRUE
                  AND symbol IN (${Prisma.join(Array.from(gapSymbols))});
            `),
      );
    }
    if (gapWindowsByDeploymentId.size > 0) {
      for (const [deploymentId, gapWindow] of gapWindowsByDeploymentId) {
        nudgedWorkerRows += Number(
          await db.$executeRaw(Prisma.sql`
                    UPDATE scalp_deployment_weekly_metrics
                    SET
                        status = 'pending',
                        next_run_at = NOW(),
                        error_code = NULL,
                        error_message = NULL,
                        updated_at = NOW()
                    WHERE deployment_id = ${deploymentId}
                      AND week_start >= ${new Date(gapWindow.windowFromTs)}
                      AND week_start < ${new Date(gapWindow.windowToTs)}
                      AND status IN ('pending', 'retry_wait', 'failed');
                `),
        );
      }
      await db.$executeRaw(Prisma.sql`
                UPDATE scalp_deployments
                SET
                    worker_dirty = TRUE,
                    updated_by = 'pipeline:promotion',
                    updated_at = NOW()
                WHERE deployment_id IN (${Prisma.join(Array.from(gapWindowsByDeploymentId.keys()))});
            `);
    }

    const candidates = buildForwardValidationByCandidateFromTasks({
      tasks: freshReadyTasks,
      selectionWindowDays: policy.lookbackDays,
      forwardWindowDays: 7,
    });
    const candidateByKey = new Map(
      candidates.map((row) => [
        `${row.symbol}::${row.strategyId}::${row.tuneId}`,
        row,
      ]),
    );

    const weeklyByKey = new Map<string, ScalpWeeklyRobustnessMetrics | null>();
    const weeklyGateReasonByKey = new Map<string, string | null>();

    for (const candidate of candidates) {
      const key = `${candidate.symbol}::${candidate.strategyId}::${candidate.tuneId}`;
      const candidateTasks = (freshReadyTasks || []).filter(
        (task) =>
          task.deploymentId === candidate.deploymentId &&
          task.symbol === candidate.symbol &&
          task.strategyId === candidate.strategyId &&
          task.tuneId === candidate.tuneId,
      );
      const weeklyMetrics = computeWeeklyRobustnessFromTasks({
        tasks: candidateTasks.map((task) => ({
          netR: toFiniteNumber(task.result?.netR, 0),
          expectancyR: toFiniteNumber(task.result?.expectancyR, 0),
          maxDrawdownR: toFiniteNumber(task.result?.maxDrawdownR, 0),
        })),
        nowMs,
      });
      weeklyByKey.set(key, weeklyMetrics);
      const weeklyGate = evaluateWeeklyRobustnessGate(weeklyMetrics, policy);
      weeklyGateReasonByKey.set(
        key,
        weeklyGate.passed
          ? null
          : weeklyGate.reason || "weekly_robustness_failed",
      );

      candidate.forwardValidation.weeklySlices = weeklyMetrics?.slices ?? null;
      candidate.forwardValidation.weeklyProfitablePct =
        weeklyMetrics?.profitablePct ?? null;
      candidate.forwardValidation.weeklyMeanExpectancyR =
        weeklyMetrics?.meanExpectancyR ?? null;
      candidate.forwardValidation.weeklyTrimmedMeanExpectancyR =
        weeklyMetrics?.trimmedMeanExpectancyR ?? null;
      candidate.forwardValidation.weeklyP25ExpectancyR =
        weeklyMetrics?.p25ExpectancyR ?? null;
      candidate.forwardValidation.weeklyMedianExpectancyR =
        weeklyMetrics?.medianExpectancyR ?? null;
      candidate.forwardValidation.weeklyWorstNetR =
        weeklyMetrics?.worstNetR ?? null;
      candidate.forwardValidation.weeklyTopWeekPnlConcentrationPct =
        weeklyMetrics?.topWeekPnlConcentrationPct ?? null;
      candidate.forwardValidation.weeklyEvaluatedAtMs = nowMs;
    }

    const tempDeploymentsForWinners = consideredDeployments.map(
      (deployment) => {
        const key = `${deployment.symbol}::${deployment.strategyId}::${deployment.tuneId}`;
        const candidate = candidateByKey.get(key) || null;
        const freshness = freshnessByDeploymentId.get(deployment.deploymentId);
        const freshnessState = freshness
          ? {
              requiredWeeks: freshness.requiredWeeks,
              completedWeeks: freshness.completedWeeks,
              missingWeeks: freshness.missingWeeks,
              windowFromTs: freshness.windowFromTs,
              windowToTs: freshness.windowToTs,
              missingWeekStarts: freshness.missingWeekStarts || [],
            }
          : null;
        const weeklyFailReason = weeklyGateReasonByKey.get(key) || null;
        const eligible = Boolean(
          candidate && freshness?.ready && !weeklyFailReason,
        );
        return {
          deploymentId: deployment.deploymentId,
          symbol: deployment.symbol,
          strategyId: deployment.strategyId,
          tuneId: deployment.tuneId,
          enabled: Boolean(
            inUniverseByDeploymentId.get(deployment.deploymentId)?.enabled,
          ),
          promotionGate: {
            eligible,
            reason: eligible
              ? "weekly_robustness_passed"
              : weeklyFailReason ||
                (!freshness?.ready
                  ? "fresh_weeks_incomplete"
                  : "candidate_missing"),
            source: "walk_forward",
            evaluatedAtMs: nowMs,
            forwardValidation: candidate?.forwardValidation || null,
            thresholds:
              (asJsonObject(
                inUniverseByDeploymentId.get(deployment.deploymentId)
                  ?.promotionGate || null,
              )?.thresholds as any) || null,
            freshness: freshnessState,
          } as ScalpDeploymentPromotionGate,
        };
      },
    );

    const strategyWinnerIds = buildBestEligibleTuneDeploymentIdSet({
      deployments: tempDeploymentsForWinners,
      candidates,
    });
    const strategyWinnerDeployments = tempDeploymentsForWinners.filter((row) =>
      strategyWinnerIds.has(row.deploymentId),
    );
    const globalWinnerIds = buildGlobalSymbolRankedDeploymentIdSet({
      deployments: strategyWinnerDeployments,
      candidates,
      maxSymbols: policy.globalMaxSymbols,
      maxPerSymbol: policy.topKPerSymbol,
      maxDeployments: policy.globalMaxDeployments,
    });
    const winnerIds = policy.requireWinnerShortlist
      ? globalWinnerIds
      : strategyWinnerIds;

    const updates = consideredDeployments.map(
      (deployment): ScalpDeploymentRegistryEntry => {
        const key = `${deployment.symbol}::${deployment.strategyId}::${deployment.tuneId}`;
        const candidate = candidateByKey.get(key) || null;
        const freshness = freshnessByDeploymentId.get(deployment.deploymentId);
        const freshnessState = freshness
          ? {
              requiredWeeks: freshness.requiredWeeks,
              completedWeeks: freshness.completedWeeks,
              missingWeeks: freshness.missingWeeks,
              windowFromTs: freshness.windowFromTs,
              windowToTs: freshness.windowToTs,
              missingWeekStarts: freshness.missingWeekStarts || [],
            }
          : null;
        const weeklyFailReason = weeklyGateReasonByKey.get(key) || null;
        const eligible = Boolean(
          candidate && freshness?.ready && !weeklyFailReason,
        );
        const reason = eligible
          ? "weekly_robustness_passed"
          : weeklyFailReason ||
            (!freshness?.ready
              ? "fresh_weeks_incomplete"
              : "candidate_missing");
        const promotionGate: ScalpDeploymentPromotionGate = {
          eligible,
          reason,
          source: "walk_forward",
          evaluatedAtMs: nowMs,
          forwardValidation: candidate?.forwardValidation || null,
          thresholds: deployment.promotionGate?.thresholds || null,
          freshness: freshnessState,
        };
        const shouldEnable = eligible && winnerIds.has(deployment.deploymentId);
        return {
          ...deployment,
          enabled: shouldEnable,
          promotionGate,
          updatedAtMs: nowMs,
          updatedBy: "pipeline:promotion",
        };
      },
    );

    if (updates.length > 0) {
      await upsertScalpDeploymentRegistryEntriesBulk(
        updates.map((row) => ({
          deploymentId: row.deploymentId,
          symbol: row.symbol,
          strategyId: row.strategyId,
          tuneId: row.tuneId,
          source: row.source,
          enabled: row.enabled,
          configOverride: row.configOverride,
          promotionGate: row.promotionGate,
          updatedBy: "pipeline:promotion",
        })),
      );
    }

    if (dirtySet.size > 0) {
      await db.$executeRaw(Prisma.sql`
                UPDATE scalp_deployments
                SET
                    promotion_dirty = FALSE,
                    updated_by = 'pipeline:promotion',
                    updated_at = NOW()
                WHERE deployment_id IN (${Prisma.join(Array.from(dirtySet))});
            `);
    }

    const pendingAfter = await countPendingPromotionRows();
    await pulsePipelineJobProgress({
      jobKind: "promotion",
      lockToken,
      lockMs,
      progressLabel: `updated ${updates.length}`,
      progress: {
        processedDirtyDeployments: dirtyRows.length,
        consideredDeployments: consideredDeployments.length,
        candidateCount: candidates.length,
        strategyWinnerCount: strategyWinnerIds.size,
        winnerCount: winnerIds.size,
        nudgedLoadSymbols,
        nudgedWorkerRows,
        pendingAfter,
      },
    });

    return {
      ok: true,
      processed: dirtyRows.length,
      succeeded: dirtyRows.length,
      retried: 0,
      failed: 0,
      pendingAfter,
      downstreamRequested: nudgedLoadSymbols > 0 || nudgedWorkerRows > 0,
      progressLabel: `updated ${updates.length}`,
      details: {
        policy,
        dirtyDeployments: dirtyRows.length,
        consideredDeployments: consideredDeployments.length,
        candidateCount: candidates.length,
        strategyWinnerCount: strategyWinnerIds.size,
        winnerCount: winnerIds.size,
        nudgedLoadSymbols,
        nudgedWorkerRows,
      },
    };
  });
}

export async function loadScalpPipelineJobsHealth(): Promise<
  ScalpPipelineJobHealth[]
> {
  if (!isScalpPgConfigured()) return [];
  const db = scalpPrisma();
  for (const jobKind of SCALP_PIPELINE_JOB_KINDS) {
    await ensurePipelineJobRow(jobKind);
  }

  const [jobRows, symbolRows, workerRows, deploymentRows] = await Promise.all([
    db.$queryRaw<
      Array<{
        jobKind: string;
        status: string;
        lockToken: string | null;
        lockExpiresAtMs: bigint | number | null;
        lastRunAtMs: bigint | number | null;
        lastSuccessAtMs: bigint | number | null;
        nextRunAtMs: bigint | number | null;
        lastError: string | null;
        progressLabel: string | null;
        progressJson: unknown;
      }>
    >(Prisma.sql`
            SELECT
                job_kind AS "jobKind",
                status,
                lock_token AS "lockToken",
                (EXTRACT(EPOCH FROM lock_expires_at) * 1000)::bigint AS "lockExpiresAtMs",
                (EXTRACT(EPOCH FROM last_run_at) * 1000)::bigint AS "lastRunAtMs",
                (EXTRACT(EPOCH FROM last_success_at) * 1000)::bigint AS "lastSuccessAtMs",
                (EXTRACT(EPOCH FROM next_run_at) * 1000)::bigint AS "nextRunAtMs",
                last_error AS "lastError",
                progress_label AS "progressLabel",
                progress_json AS "progressJson"
            FROM scalp_pipeline_jobs;
        `),
    db.$queryRaw<
      Array<{
        pendingLoad: bigint | number | null;
        runningLoad: bigint | number | null;
        retryLoad: bigint | number | null;
        failedLoad: bigint | number | null;
        succeededLoad: bigint | number | null;
        pendingPrepare: bigint | number | null;
        runningPrepare: bigint | number | null;
        retryPrepare: bigint | number | null;
        failedPrepare: bigint | number | null;
        succeededPrepare: bigint | number | null;
      }>
    >(Prisma.sql`
            SELECT
                COUNT(*) FILTER (WHERE active = TRUE AND load_status = 'pending')::bigint AS "pendingLoad",
                COUNT(*) FILTER (WHERE active = TRUE AND load_status = 'running')::bigint AS "runningLoad",
                COUNT(*) FILTER (WHERE active = TRUE AND load_status = 'retry_wait')::bigint AS "retryLoad",
                COUNT(*) FILTER (WHERE active = TRUE AND load_status = 'failed')::bigint AS "failedLoad",
                COUNT(*) FILTER (WHERE active = TRUE AND load_status = 'succeeded')::bigint AS "succeededLoad",
                COUNT(*) FILTER (WHERE active = TRUE AND prepare_status = 'pending')::bigint AS "pendingPrepare",
                COUNT(*) FILTER (WHERE active = TRUE AND prepare_status = 'running')::bigint AS "runningPrepare",
                COUNT(*) FILTER (WHERE active = TRUE AND prepare_status = 'retry_wait')::bigint AS "retryPrepare",
                COUNT(*) FILTER (WHERE active = TRUE AND prepare_status = 'failed')::bigint AS "failedPrepare",
                COUNT(*) FILTER (WHERE active = TRUE AND prepare_status = 'succeeded')::bigint AS "succeededPrepare"
            FROM scalp_pipeline_symbols;
        `),
    db.$queryRaw<
      Array<{
        pendingWorker: bigint | number | null;
        runningWorker: bigint | number | null;
        retryWorker: bigint | number | null;
        failedWorker: bigint | number | null;
        succeededWorker: bigint | number | null;
      }>
    >(Prisma.sql`
            SELECT
                COUNT(*) FILTER (WHERE status = 'pending')::bigint AS "pendingWorker",
                COUNT(*) FILTER (WHERE status = 'running')::bigint AS "runningWorker",
                COUNT(*) FILTER (WHERE status = 'retry_wait')::bigint AS "retryWorker",
                COUNT(*) FILTER (WHERE status = 'failed')::bigint AS "failedWorker",
                COUNT(*) FILTER (WHERE status = 'succeeded')::bigint AS "succeededWorker"
            FROM scalp_deployment_weekly_metrics;
        `),
    db.$queryRaw<
      Array<{
        pendingPromotion: bigint | number | null;
        runningPromotion: bigint | number | null;
        retryPromotion: bigint | number | null;
        failedPromotion: bigint | number | null;
        succeededPromotion: bigint | number | null;
      }>
    >(Prisma.sql`
            SELECT
                COUNT(*) FILTER (WHERE in_universe = TRUE AND promotion_dirty = TRUE)::bigint AS "pendingPromotion",
                0::bigint AS "runningPromotion",
                0::bigint AS "retryPromotion",
                0::bigint AS "failedPromotion",
                COUNT(*) FILTER (WHERE in_universe = TRUE AND promotion_dirty = FALSE)::bigint AS "succeededPromotion"
            FROM scalp_deployments;
        `),
  ]);

  const symbolAgg = symbolRows[0] || ({} as any);
  const workerAgg = workerRows[0] || ({} as any);
  const promotionAgg = deploymentRows[0] || ({} as any);
  const nowMs = Date.now();

  const byJobKind = new Map(jobRows.map((row) => [String(row.jobKind), row]));

  const queueByJobKind: Record<
    ScalpPipelineJobKind,
    ScalpPipelineJobHealth["queue"]
  > = {
    discover: {
      pending: Math.max(0, Math.floor(Number(symbolAgg.pendingLoad || 0))),
      running: 0,
      retryWait: 0,
      failed: 0,
      succeeded: Math.max(0, Math.floor(Number(symbolAgg.succeededLoad || 0))),
    },
    load_candles: {
      pending: Math.max(0, Math.floor(Number(symbolAgg.pendingLoad || 0))),
      running: Math.max(0, Math.floor(Number(symbolAgg.runningLoad || 0))),
      retryWait: Math.max(0, Math.floor(Number(symbolAgg.retryLoad || 0))),
      failed: Math.max(0, Math.floor(Number(symbolAgg.failedLoad || 0))),
      succeeded: Math.max(0, Math.floor(Number(symbolAgg.succeededLoad || 0))),
    },
    prepare: {
      pending: Math.max(0, Math.floor(Number(symbolAgg.pendingPrepare || 0))),
      running: Math.max(0, Math.floor(Number(symbolAgg.runningPrepare || 0))),
      retryWait: Math.max(0, Math.floor(Number(symbolAgg.retryPrepare || 0))),
      failed: Math.max(0, Math.floor(Number(symbolAgg.failedPrepare || 0))),
      succeeded: Math.max(
        0,
        Math.floor(Number(symbolAgg.succeededPrepare || 0)),
      ),
    },
    worker: {
      pending: Math.max(0, Math.floor(Number(workerAgg.pendingWorker || 0))),
      running: Math.max(0, Math.floor(Number(workerAgg.runningWorker || 0))),
      retryWait: Math.max(0, Math.floor(Number(workerAgg.retryWorker || 0))),
      failed: Math.max(0, Math.floor(Number(workerAgg.failedWorker || 0))),
      succeeded: Math.max(
        0,
        Math.floor(Number(workerAgg.succeededWorker || 0)),
      ),
    },
    promotion: {
      pending: Math.max(
        0,
        Math.floor(Number(promotionAgg.pendingPromotion || 0)),
      ),
      running: Math.max(
        0,
        Math.floor(Number(promotionAgg.runningPromotion || 0)),
      ),
      retryWait: Math.max(
        0,
        Math.floor(Number(promotionAgg.retryPromotion || 0)),
      ),
      failed: Math.max(
        0,
        Math.floor(Number(promotionAgg.failedPromotion || 0)),
      ),
      succeeded: Math.max(
        0,
        Math.floor(Number(promotionAgg.succeededPromotion || 0)),
      ),
    },
  };

  return SCALP_PIPELINE_JOB_KINDS.map((jobKind) => {
    const row = byJobKind.get(jobKind) || null;
    const lockExpiresAtMs = asTsMs(row?.lockExpiresAtMs);
    return {
      jobKind,
      status: String(row?.status || "idle"),
      locked: typeof lockExpiresAtMs === "number" && lockExpiresAtMs > nowMs,
      lastRunAtMs: asTsMs(row?.lastRunAtMs),
      lastSuccessAtMs: asTsMs(row?.lastSuccessAtMs),
      nextRunAtMs: asTsMs(row?.nextRunAtMs),
      lastError: String(row?.lastError || "").trim() || null,
      progressLabel: String(row?.progressLabel || "").trim() || null,
      progress: asJsonObject(row?.progressJson),
      queue: queueByJobKind[jobKind],
    };
  });
}

export async function listScalpDeploymentWeeklyMetricRows(
  params: { limit?: number } = {},
): Promise<ScalpDeploymentWeeklyMetricSnapshotRow[]> {
  if (!isScalpPgConfigured()) return [];
  const db = scalpPrisma();
  const limit = Math.max(1, Math.min(20_000, toPositiveInt(params.limit, 8_000)));
  const rows = await db.$queryRaw<
    Array<{
      deploymentId: string;
      symbol: string;
      strategyId: string;
      tuneId: string;
      weekStartMs: bigint | number | null;
      weekEndMs: bigint | number | null;
      status: string;
      attempts: number | null;
      startedAtMs: bigint | number | null;
      finishedAtMs: bigint | number | null;
      errorCode: string | null;
      errorMessage: string | null;
      trades: number | null;
      netR: unknown;
      expectancyR: unknown;
      profitFactor: unknown;
      maxDrawdownR: unknown;
    }>
  >(Prisma.sql`
        SELECT
            m.deployment_id AS "deploymentId",
            m.symbol,
            m.strategy_id AS "strategyId",
            m.tune_id AS "tuneId",
            (EXTRACT(EPOCH FROM m.week_start) * 1000)::bigint AS "weekStartMs",
            (EXTRACT(EPOCH FROM m.week_end) * 1000)::bigint AS "weekEndMs",
            m.status,
            m.attempts,
            (EXTRACT(EPOCH FROM m.started_at) * 1000)::bigint AS "startedAtMs",
            (EXTRACT(EPOCH FROM m.finished_at) * 1000)::bigint AS "finishedAtMs",
            m.error_code AS "errorCode",
            m.error_message AS "errorMessage",
            m.trades,
            m.net_r AS "netR",
            m.expectancy_r AS "expectancyR",
            m.profit_factor AS "profitFactor",
            m.max_drawdown_r AS "maxDrawdownR"
        FROM scalp_deployment_weekly_metrics m
        JOIN scalp_deployments d
          ON d.deployment_id = m.deployment_id
        ORDER BY m.week_start DESC, m.deployment_id ASC
        LIMIT ${limit};
    `);

  return rows
    .map((row) => {
      const deploymentId = String(row.deploymentId || "").trim();
      const symbol = String(row.symbol || "")
        .trim()
        .toUpperCase();
      const strategyId = String(row.strategyId || "").trim();
      const tuneId = String(row.tuneId || "").trim() || "default";
      const weekStartMs = asTsMs(row.weekStartMs);
      const weekEndMs = asTsMs(row.weekEndMs);
      if (!deploymentId || !symbol || !strategyId) return null;
      if (weekStartMs === null || weekEndMs === null) return null;
      return {
        deploymentId,
        symbol,
        strategyId,
        tuneId,
        weekStartMs,
        weekEndMs,
        status: String(row.status || "pending")
          .trim()
          .toLowerCase(),
        attempts: Math.max(0, Math.floor(Number(row.attempts || 0))),
        startedAtMs: asTsMs(row.startedAtMs),
        finishedAtMs: asTsMs(row.finishedAtMs),
        errorCode: String(row.errorCode || "").trim() || null,
        errorMessage: String(row.errorMessage || "").trim() || null,
        trades:
          row.trades === null || row.trades === undefined
            ? null
            : Math.max(0, Math.floor(Number(row.trades))),
        netR: asNullableFiniteNumber(row.netR),
        expectancyR: asNullableFiniteNumber(row.expectancyR),
        profitFactor: asNullableFiniteNumber(row.profitFactor),
        maxDrawdownR: asNullableFiniteNumber(row.maxDrawdownR),
      } satisfies ScalpDeploymentWeeklyMetricSnapshotRow;
    })
    .filter((row): row is ScalpDeploymentWeeklyMetricSnapshotRow => row !== null);
}
