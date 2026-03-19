import { Prisma } from "@prisma/client";

import {
  listScalpCandleHistorySymbols,
  loadScalpCandleHistory,
  saveScalpCandleHistory,
  type ScalpCandleHistoryRecord,
} from "./candleHistory";
import { getScalpStrategyConfig } from "./config";
import { isScalpPgConfigured, scalpPrisma } from "./pg/client";
import { deleteDeploymentsByIdFromPg } from "./pg/deployments";
import type { ScalpCandle } from "./types";

function toPositiveInt(value: unknown, fallback: number): number {
  const n = Math.floor(Number(value));
  if (!Number.isFinite(n) || n <= 0) return fallback;
  return n;
}

function toOptionalPositiveInt(value: unknown): number | null {
  const n = Math.floor(Number(value));
  if (!Number.isFinite(n) || n <= 0) return null;
  return n;
}

function toBool(value: unknown, fallback: boolean): boolean {
  if (typeof value === "boolean") return value;
  const normalized = String(value || "")
    .trim()
    .toLowerCase();
  if (!normalized) return fallback;
  if (["1", "true", "yes", "on"].includes(normalized)) return true;
  if (["0", "false", "no", "off"].includes(normalized)) return false;
  return fallback;
}

function toFinite(value: unknown, fallback = 0): number {
  const n = Number(value);
  if (!Number.isFinite(n)) return fallback;
  return n;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return Boolean(value) && typeof value === "object" && !Array.isArray(value);
}

function normalizeSymbol(value: unknown): string {
  return String(value || "")
    .trim()
    .toUpperCase()
    .replace(/[^A-Z0-9._-]/g, "");
}

const ONE_DAY_MS = 24 * 60 * 60_000;
const ONE_WEEK_MS = 7 * ONE_DAY_MS;
const DEFAULT_PIPELINE_HISTORY_KEEP_WEEKS = 14;
const DEFAULT_WEEKLY_METRICS_RETENTION_DAYS = 180;
const DEFAULT_PIPELINE_INACTIVE_SYMBOL_RETENTION_DAYS = 90;
const DEFAULT_RESEARCH_PRIMARY_REFERENCE_WEEKS = 12;
const DEFAULT_RESEARCH_CONFIRMATION_KEEP_WEEKS = 52;
const DEFAULT_RESEARCH_CONFIRMATION_BUFFER_WEEKS = 2;
const MAX_RESEARCH_CONFIRMATION_KEEP_WEEKS = 104;

function startOfWeekMondayUtc(tsMs: number): number {
  const dayStartMs = Math.floor(tsMs / ONE_DAY_MS) * ONE_DAY_MS;
  const dayOfWeek = new Date(dayStartMs).getUTCDay(); // 0=Sunday ... 6=Saturday
  const daysSinceMonday = (dayOfWeek + 6) % 7;
  return dayStartMs - daysSinceMonday * ONE_DAY_MS;
}

export function pruneScalpCandlesToRollingWeeks(params: {
  candles: ScalpCandle[];
  nowMs: number;
  keepWeeks: number;
}): {
  candles: ScalpCandle[];
  removedCount: number;
  cutoffWeekStartMs: number;
} {
  const keepWeeks = Math.max(1, Math.floor(Number(params.keepWeeks) || 1));
  const currentWeekStartMs = startOfWeekMondayUtc(params.nowMs);
  const cutoffWeekStartMs = currentWeekStartMs - (keepWeeks - 1) * ONE_WEEK_MS;
  const rows = Array.isArray(params.candles) ? params.candles : [];
  const kept = rows.filter((row) => Number(row?.[0]) >= cutoffWeekStartMs);
  return {
    candles: kept,
    removedCount: Math.max(0, rows.length - kept.length),
    cutoffWeekStartMs,
  };
}

export function shouldPruneResearchCycle(params: {
  cycle: unknown;
  nowMs: number;
  activeCycleId: string | null;
  retentionMs: number;
  cycleIdFromKey: string;
}): boolean {
  if (!isRecord(params.cycle)) return false;
  const cycleId = String(params.cycle.cycleId || params.cycleIdFromKey).trim();
  if (!cycleId) return false;
  if (params.activeCycleId && cycleId === params.activeCycleId) return false;

  const status = String(params.cycle.status || "")
    .trim()
    .toLowerCase();
  const createdAtMs = toFinite(params.cycle.createdAtMs, 0);
  const updatedAtMs = toFinite(params.cycle.updatedAtMs, createdAtMs);
  const refMs = Math.max(createdAtMs, updatedAtMs);
  if (refMs <= 0) return false;

  const ageMs = params.nowMs - refMs;
  if (ageMs < params.retentionMs) return false;

  if (!status) return false;
  if (status === "running") {
    return ageMs >= params.retentionMs * 2;
  }
  return status === "completed" || status === "failed" || status === "stalled";
}

export function shouldPruneOrphanedDeployment(params: {
  source: unknown;
  enabled: unknown;
  researchTaskCount: unknown;
}): boolean {
  const source = String(params.source || "")
    .trim()
    .toLowerCase();
  const enabled = params.enabled === true;
  const researchTaskCount = Math.max(
    0,
    Math.floor(Number(params.researchTaskCount) || 0),
  );
  if (enabled) return false;
  if (source !== "backtest" && source !== "matrix") return false;
  return researchTaskCount <= 0;
}

export interface ScalpResearchHistoryRetentionPolicy {
  primaryReferenceWeeks: number;
  confirmationKeepWeeks: number;
  confirmationBufferWeeks: number;
  minimumRetainedWeeks: number;
  minimumCycleRetentionDays: number;
}

export function resolveScalpResearchHistoryRetentionPolicy(
  params: {
    primaryReferenceWeeks?: unknown;
    confirmationKeepWeeks?: unknown;
    confirmationBufferWeeks?: unknown;
  } = {},
): ScalpResearchHistoryRetentionPolicy {
  const primaryReferenceWeeks = Math.max(
    1,
    Math.min(
      MAX_RESEARCH_CONFIRMATION_KEEP_WEEKS,
      toPositiveInt(
        params.primaryReferenceWeeks ??
          process.env.SCALP_RESEARCH_PRIMARY_REFERENCE_WEEKS,
        DEFAULT_RESEARCH_PRIMARY_REFERENCE_WEEKS,
      ),
    ),
  );
  const confirmationKeepWeeks = Math.max(
    primaryReferenceWeeks,
    Math.min(
      MAX_RESEARCH_CONFIRMATION_KEEP_WEEKS,
      toPositiveInt(
        params.confirmationKeepWeeks ??
          process.env.SCALP_RESEARCH_CONFIRMATION_KEEP_WEEKS,
        DEFAULT_RESEARCH_CONFIRMATION_KEEP_WEEKS,
      ),
    ),
  );
  const confirmationBufferWeeks = Math.max(
    0,
    Math.min(
      8,
      Math.floor(
        Number(
          params.confirmationBufferWeeks ??
            process.env.SCALP_RESEARCH_CONFIRMATION_BUFFER_WEEKS,
        ) || DEFAULT_RESEARCH_CONFIRMATION_BUFFER_WEEKS,
      ),
    ),
  );
  const minimumRetainedWeeks = Math.max(
    primaryReferenceWeeks,
    confirmationKeepWeeks + confirmationBufferWeeks,
  );
  const minimumCycleRetentionDays = Math.max(14, minimumRetainedWeeks * 7 + 7);
  return {
    primaryReferenceWeeks,
    confirmationKeepWeeks,
    confirmationBufferWeeks,
    minimumRetainedWeeks,
    minimumCycleRetentionDays,
  };
}

export interface RunScalpHousekeepingParams {
  dryRun?: boolean;
  nowMs?: number;
  cycleRetentionDays?: number;
  inactiveSymbolRetentionDays?: number;
  lockMaxAgeMinutes?: number;
  maxScanKeys?: number;
  refreshReport?: boolean;
  cleanupOrphanDeployments?: boolean;
  candleHistoryKeepWeeks?: number;
  candleHistoryTimeframe?: string;
}

export interface RunScalpHousekeepingResult {
  ok: boolean;
  dryRun: boolean;
  generatedAtMs: number;
  generatedAtIso: string;
  config: {
    cycleRetentionDays: number;
    requestedCycleRetentionDays: number | null;
    inactiveSymbolRetentionDays: number;
    requestedInactiveSymbolRetentionDays: number | null;
    lockMaxAgeMinutes: number;
    maxScanKeys: number;
    refreshReport: boolean;
    cleanupOrphanDeployments: boolean;
    journalMax: number;
    tradeLedgerMax: number;
    candleHistoryKeepWeeks: number;
    requestedCandleHistoryKeepWeeks: number | null;
    candleHistoryTimeframe: string;
    researchPrimaryReferenceWeeks: number;
    researchConfirmationKeepWeeks: number;
    researchConfirmationBufferWeeks: number;
  };
  summary: {
    cyclesPruned: number;
    cycleKeysDeleted: number;
    taskKeysDeleted: number;
    aggregateKeysDeleted: number;
    claimCursorKeysDeleted: number;
    researchLocksDeleted: number;
    runLocksDeleted: number;
    orphanedDeploymentsPruned: number;
    listCompactions: number;
    candleHistorySymbolsScanned: number;
    candleHistorySymbolsPruned: number;
    candleHistoryCandlesDeleted: number;
    candleHistorySymbolsSkipped: number;
    reportRefreshed: boolean;
    stalePipelineJobLocksCleared: number;
    stalePipelineLoadRowsRecovered: number;
    stalePipelinePrepareRowsRecovered: number;
    stalePipelineWorkerRowsRecovered: number;
    weeklyMetricsRowsPruned: number;
    inactivePipelineSymbolsPruned: number;
  };
  details: {
    prunedCycleIds: string[];
    deletedResearchLockKeys: string[];
    deletedRunLockKeys: string[];
    orphanedDeploymentIds: string[];
    activePipelineRetentionGuard: {
      minKeepWeeks: number;
      protectedSymbols: string[];
      pendingSymbols: string[];
    };
    prunedInactivePipelineSymbols: string[];
  };
}

interface CandleRetentionResult {
  symbolsScanned: number;
  symbolsPruned: number;
  candlesDeleted: number;
}

interface PgOrphanedDeploymentPruneResult {
  prunedDeploymentIds: string[];
}

function toRecordSummary(record: ScalpCandleHistoryRecord | null): {
  epic: string | null;
  source: "capital";
} {
  return {
    epic: record?.epic ?? null,
    source: "capital",
  };
}

async function pruneCandleHistoryRollingWeeks(params: {
  nowMs: number;
  dryRun: boolean;
  keepWeeks: number;
  timeframe: string;
  skipSymbols?: Set<string>;
}): Promise<CandleRetentionResult> {
  const symbols = await listScalpCandleHistorySymbols(params.timeframe);
  if (!symbols.length) {
    return {
      symbolsScanned: 0,
      symbolsPruned: 0,
      candlesDeleted: 0,
    };
  }

  let symbolsPruned = 0;
  let candlesDeleted = 0;
  for (const symbol of symbols) {
    if (params.skipSymbols?.has(symbol)) continue;
    const loaded = await loadScalpCandleHistory(symbol, params.timeframe);
    const record = loaded.record;
    if (!record || !record.candles.length) continue;

    const pruned = pruneScalpCandlesToRollingWeeks({
      candles: record.candles,
      nowMs: params.nowMs,
      keepWeeks: params.keepWeeks,
    });
    if (pruned.removedCount <= 0) continue;

    symbolsPruned += 1;
    candlesDeleted += pruned.removedCount;
    if (!params.dryRun) {
      const recordSummary = toRecordSummary(record);
      await saveScalpCandleHistory({
        symbol: record.symbol,
        timeframe: record.timeframe,
        epic: recordSummary.epic,
        source: recordSummary.source,
        candles: pruned.candles,
      });
    }
  }

  return {
    symbolsScanned: symbols.length,
    symbolsPruned,
    candlesDeleted,
  };
}

function resolveRequiredSuccessiveWeeksForHousekeeping(): number {
  return Math.max(
    13,
    Math.min(
      52,
      toPositiveInt(process.env.SCALP_PIPELINE_REQUIRED_SUCCESSIVE_WEEKS, 13),
    ),
  );
}

type ActivePipelineCandleRetentionGuard = {
  minKeepWeeks: number;
  protectedSymbols: Set<string>;
  pendingSymbols: string[];
};

async function loadActivePipelineCandleRetentionGuard(
  fallbackKeepWeeks: number,
): Promise<ActivePipelineCandleRetentionGuard> {
  const minKeepWeeks = Math.max(
    fallbackKeepWeeks,
    resolveRequiredSuccessiveWeeksForHousekeeping() + 1,
  );
  if (!isScalpPgConfigured()) {
    return {
      minKeepWeeks,
      protectedSymbols: new Set<string>(),
      pendingSymbols: [],
    };
  }
  const db = scalpPrisma();
  const rows = await db.$queryRaw<
    Array<{
      symbol: string;
    }>
  >(Prisma.sql`
        SELECT
            symbol
        FROM scalp_pipeline_symbols
        WHERE active = TRUE
          AND (
            load_status IN ('pending', 'running', 'retry_wait')
            OR prepare_status IN ('pending', 'running', 'retry_wait')
          )
        ORDER BY symbol ASC
        LIMIT 5000;
    `);

  const protectedSymbols = new Set<string>();
  for (const row of rows) {
    const symbol = normalizeSymbol(row.symbol);
    if (!symbol) continue;
    protectedSymbols.add(symbol);
  }

  return {
    minKeepWeeks,
    protectedSymbols,
    pendingSymbols: Array.from(protectedSymbols).sort(),
  };
}

interface StalePipelineRecoveryResult {
  staleJobLocksCleared: number;
  staleLoadRowsRecovered: number;
  stalePrepareRowsRecovered: number;
  staleWorkerRowsRecovered: number;
}

async function recoverStalePipelineRowsFromPg(params: {
  lockMaxAgeMinutes: number;
  dryRun: boolean;
}): Promise<StalePipelineRecoveryResult> {
  if (!isScalpPgConfigured()) {
    return {
      staleJobLocksCleared: 0,
      staleLoadRowsRecovered: 0,
      stalePrepareRowsRecovered: 0,
      staleWorkerRowsRecovered: 0,
    };
  }

  const db = scalpPrisma();
  const staleCutoff = new Date(
    Date.now() - Math.max(1, params.lockMaxAgeMinutes) * 60_000,
  );

  const staleJobRows = await db.$queryRaw<Array<{ count: bigint | number }>>(
    Prisma.sql`
          SELECT COUNT(*)::bigint AS count
          FROM scalp_pipeline_jobs
          WHERE status = 'running'
            AND lock_expires_at IS NOT NULL
            AND lock_expires_at < NOW();
      `,
  );
  const staleLoadRows = await db.$queryRaw<Array<{ count: bigint | number }>>(
    Prisma.sql`
          SELECT COUNT(*)::bigint AS count
          FROM scalp_pipeline_symbols
          WHERE active = TRUE
            AND load_status = 'running'
            AND updated_at < ${staleCutoff};
      `,
  );
  const stalePrepareRows = await db.$queryRaw<
    Array<{ count: bigint | number }>
  >(Prisma.sql`
          SELECT COUNT(*)::bigint AS count
          FROM scalp_pipeline_symbols
          WHERE active = TRUE
            AND prepare_status = 'running'
            AND updated_at < ${staleCutoff};
      `);
  const staleWorkerRows = await db.$queryRaw<
    Array<{ count: bigint | number }>
  >(Prisma.sql`
          SELECT COUNT(*)::bigint AS count
          FROM scalp_deployment_weekly_metrics
          WHERE status = 'running'
            AND COALESCE(started_at, updated_at) < ${staleCutoff};
      `);

  const staleJobLocksCleared = Math.max(
    0,
    Math.floor(Number(staleJobRows[0]?.count || 0)),
  );
  const staleLoadRowsRecovered = Math.max(
    0,
    Math.floor(Number(staleLoadRows[0]?.count || 0)),
  );
  const stalePrepareRowsRecovered = Math.max(
    0,
    Math.floor(Number(stalePrepareRows[0]?.count || 0)),
  );
  const staleWorkerRowsRecovered = Math.max(
    0,
    Math.floor(Number(staleWorkerRows[0]?.count || 0)),
  );

  if (!params.dryRun) {
    if (staleJobLocksCleared > 0) {
      await db.$executeRaw(Prisma.sql`
                UPDATE scalp_pipeline_jobs
                SET
                    status = 'idle',
                    lock_token = NULL,
                    lock_expires_at = NULL,
                    running_since = NULL,
                    last_error = COALESCE(last_error, 'housekeeping_recovered_stale_lock'),
                    updated_at = NOW()
                WHERE status = 'running'
                  AND lock_expires_at IS NOT NULL
                  AND lock_expires_at < NOW();
            `);
    }
    if (staleLoadRowsRecovered > 0) {
      await db.$executeRaw(Prisma.sql`
                UPDATE scalp_pipeline_symbols
                SET
                    load_status = 'retry_wait',
                    load_next_run_at = NOW(),
                    load_error = COALESCE(load_error, 'housekeeping_recovered_stale_running_row'),
                    updated_at = NOW()
                WHERE active = TRUE
                  AND load_status = 'running'
                  AND updated_at < ${staleCutoff};
            `);
    }
    if (stalePrepareRowsRecovered > 0) {
      await db.$executeRaw(Prisma.sql`
                UPDATE scalp_pipeline_symbols
                SET
                    prepare_status = 'retry_wait',
                    prepare_next_run_at = NOW(),
                    prepare_error = COALESCE(prepare_error, 'housekeeping_recovered_stale_running_row'),
                    updated_at = NOW()
                WHERE active = TRUE
                  AND prepare_status = 'running'
                  AND updated_at < ${staleCutoff};
            `);
    }
    if (staleWorkerRowsRecovered > 0) {
      await db.$executeRaw(Prisma.sql`
                UPDATE scalp_deployment_weekly_metrics
                SET
                    status = 'retry_wait',
                    next_run_at = NOW(),
                    worker_id = NULL,
                    started_at = NULL,
                    finished_at = NOW(),
                    error_code = COALESCE(error_code, 'housekeeping_stale_worker_recovered'),
                    error_message = COALESCE(error_message, 'housekeeping recovered stale running worker row'),
                    updated_at = NOW()
                WHERE status = 'running'
                  AND COALESCE(started_at, updated_at) < ${staleCutoff};
            `);
    }
  }

  return {
    staleJobLocksCleared,
    staleLoadRowsRecovered,
    stalePrepareRowsRecovered,
    staleWorkerRowsRecovered,
  };
}

async function pruneWeeklyMetricsFromPg(params: {
  nowMs: number;
  retentionMs: number;
  dryRun: boolean;
}): Promise<number> {
  if (!isScalpPgConfigured()) return 0;

  const db = scalpPrisma();
  const cutoff = new Date(params.nowMs - params.retentionMs);
  const rows = await db.$queryRaw<Array<{ count: bigint | number }>>(Prisma.sql`
        SELECT COUNT(*)::bigint AS count
        FROM scalp_deployment_weekly_metrics
        WHERE week_end < ${cutoff}
          AND status IN ('succeeded', 'failed');
    `);
  const rowsToDelete = Math.max(0, Math.floor(Number(rows[0]?.count || 0)));
  if (!params.dryRun && rowsToDelete > 0) {
    await db.$executeRaw(Prisma.sql`
            DELETE FROM scalp_deployment_weekly_metrics
            WHERE week_end < ${cutoff}
              AND status IN ('succeeded', 'failed');
        `);
  }
  return rowsToDelete;
}

async function pruneOrphanedDeploymentsFromPg(params: {
  dryRun: boolean;
  enabled: boolean;
}): Promise<PgOrphanedDeploymentPruneResult> {
  if (!params.enabled || !isScalpPgConfigured()) {
    return {
      prunedDeploymentIds: [],
    };
  }

  const db = scalpPrisma();
  const rows = await db.$queryRaw<
    Array<{
      deploymentId: string;
      source: string;
      enabled: boolean;
      inUniverse: boolean;
      researchTaskCount: bigint | number;
    }>
  >(Prisma.sql`
        SELECT
            d.deployment_id AS "deploymentId",
            d.source,
            d.enabled,
            d.in_universe AS "inUniverse",
            COUNT(m.id)::bigint AS "researchTaskCount"
        FROM scalp_deployments d
        LEFT JOIN scalp_deployment_weekly_metrics m
          ON m.deployment_id = d.deployment_id
        GROUP BY d.deployment_id, d.source, d.enabled, d.in_universe
        ORDER BY d.created_at ASC, d.deployment_id ASC
        LIMIT 10000;
    `);

  const deploymentIds = rows
    .filter(
      (row) =>
        row.inUniverse === false &&
        shouldPruneOrphanedDeployment({
          source: row.source,
          enabled: row.enabled,
          researchTaskCount: row.researchTaskCount,
        }),
    )
    .map((row) => String(row.deploymentId || "").trim())
    .filter((row) => Boolean(row));

  if (!deploymentIds.length) {
    return {
      prunedDeploymentIds: [],
    };
  }

  if (!params.dryRun) {
    await deleteDeploymentsByIdFromPg(deploymentIds);
  }

  return {
    prunedDeploymentIds: deploymentIds,
  };
}

async function pruneInactivePipelineSymbolsFromPg(params: {
  dryRun: boolean;
  nowMs: number;
  retentionDays: number;
}): Promise<string[]> {
  if (!isScalpPgConfigured()) return [];
  const cutoff = new Date(params.nowMs - params.retentionDays * ONE_DAY_MS);
  const db = scalpPrisma();
  const rows = await db.$queryRaw<Array<{ symbol: string }>>(Prisma.sql`
        SELECT symbol
        FROM scalp_pipeline_symbols
        WHERE active = FALSE
          AND updated_at < ${cutoff}
          AND load_status NOT IN ('pending', 'running', 'retry_wait')
          AND prepare_status NOT IN ('pending', 'running', 'retry_wait')
        ORDER BY updated_at ASC, symbol ASC
        LIMIT 5000;
    `);
  const symbols = rows
    .map((row) => normalizeSymbol(row.symbol))
    .filter(Boolean);
  if (!params.dryRun && symbols.length > 0) {
    await db.$executeRaw(Prisma.sql`
            DELETE FROM scalp_pipeline_symbols
            WHERE active = FALSE
              AND symbol IN (${Prisma.join(symbols)});
        `);
  }
  return symbols;
}

async function compactScalpTables(params: {
  dryRun: boolean;
  journalMax: number;
  tradeLedgerMax: number;
}): Promise<number> {
  if (params.dryRun || !isScalpPgConfigured()) return 0;

  const db = scalpPrisma();
  await db.$executeRaw(Prisma.sql`
        WITH doomed AS (
            SELECT id
            FROM scalp_journal
            ORDER BY ts DESC
            OFFSET ${params.journalMax}
        )
        DELETE FROM scalp_journal j
        USING doomed d
        WHERE j.id = d.id;
    `);

  await db.$executeRaw(Prisma.sql`
        WITH doomed AS (
            SELECT id
            FROM scalp_trade_ledger
            ORDER BY exit_at DESC
            OFFSET ${params.tradeLedgerMax}
        )
        DELETE FROM scalp_trade_ledger l
        USING doomed d
        WHERE l.id = d.id;
    `);

  return 2;
}

export async function runScalpHousekeeping(
  params: RunScalpHousekeepingParams = {},
): Promise<RunScalpHousekeepingResult> {
  const nowMs = Number.isFinite(Number(params.nowMs))
    ? Math.floor(Number(params.nowMs))
    : Date.now();
  const dryRun = Boolean(params.dryRun);

  const requestedCycleRetentionDays = toOptionalPositiveInt(
    params.cycleRetentionDays ??
      process.env.SCALP_HOUSEKEEPING_CYCLE_RETENTION_DAYS,
  );
  const requestedInactiveSymbolRetentionDays = toOptionalPositiveInt(
    params.inactiveSymbolRetentionDays ??
      process.env.SCALP_PIPELINE_INACTIVE_SYMBOL_RETENTION_DAYS,
  );
  const inactiveSymbolRetentionDays = Math.max(
    30,
    Math.min(
      3650,
      requestedInactiveSymbolRetentionDays ??
        DEFAULT_PIPELINE_INACTIVE_SYMBOL_RETENTION_DAYS,
    ),
  );
  const cycleRetentionDays = Math.max(
    14,
    requestedCycleRetentionDays ?? DEFAULT_WEEKLY_METRICS_RETENTION_DAYS,
  );
  const lockMaxAgeMinutes = toPositiveInt(
    params.lockMaxAgeMinutes ??
      process.env.SCALP_HOUSEKEEPING_LOCK_MAX_AGE_MINUTES,
    45,
  );
  const maxScanKeys = toPositiveInt(
    params.maxScanKeys ?? process.env.SCALP_HOUSEKEEPING_MAX_SCAN_KEYS,
    4000,
  );
  const refreshReport = toBool(
    params.refreshReport ?? process.env.SCALP_HOUSEKEEPING_REFRESH_REPORT,
    false,
  );
  const cleanupOrphanDeployments = toBool(
    params.cleanupOrphanDeployments ??
      process.env.SCALP_HOUSEKEEPING_CLEANUP_ORPHAN_DEPLOYMENTS,
    true,
  );
  const requestedCandleHistoryKeepWeeks = toOptionalPositiveInt(
    params.candleHistoryKeepWeeks ??
      process.env.SCALP_HOUSEKEEPING_CANDLE_HISTORY_KEEP_WEEKS,
  );
  const requiredSuccessiveWeeks =
    resolveRequiredSuccessiveWeeksForHousekeeping();
  const candleHistoryRequestedKeepWeeks = Math.max(
    requiredSuccessiveWeeks + 1,
    Math.min(
      208,
      requestedCandleHistoryKeepWeeks ?? DEFAULT_PIPELINE_HISTORY_KEEP_WEEKS,
    ),
  );
  const candleHistoryTimeframe = String(
    params.candleHistoryTimeframe ??
      process.env.SCALP_HOUSEKEEPING_CANDLE_HISTORY_TIMEFRAME ??
      "1m",
  )
    .trim()
    .toLowerCase();

  const cfg = getScalpStrategyConfig();
  const journalMax = Math.max(
    10,
    Math.min(
      2_000,
      toPositiveInt(
        process.env.SCALP_HOUSEKEEPING_JOURNAL_MAX,
        cfg.storage.journalMax,
      ),
    ),
  );
  const tradeLedgerMax = Math.max(
    200,
    Math.min(
      50_000,
      toPositiveInt(process.env.SCALP_HOUSEKEEPING_TRADE_LEDGER_MAX, 10_000),
    ),
  );

  const retentionMs = cycleRetentionDays * 24 * 60 * 60_000;

  const staleRecovery = await recoverStalePipelineRowsFromPg({
    lockMaxAgeMinutes,
    dryRun,
  });
  const weeklyMetricsRowsPruned = await pruneWeeklyMetricsFromPg({
    nowMs,
    retentionMs,
    dryRun,
  });
  const orphanedDeploymentPrune = await pruneOrphanedDeploymentsFromPg({
    dryRun,
    enabled: cleanupOrphanDeployments,
  });
  const prunedInactivePipelineSymbols = await pruneInactivePipelineSymbolsFromPg({
    dryRun,
    nowMs,
    retentionDays: inactiveSymbolRetentionDays,
  });

  const listCompactions = await compactScalpTables({
    dryRun,
    journalMax,
    tradeLedgerMax,
  });
  const activePipelineCandleGuard =
    await loadActivePipelineCandleRetentionGuard(
      candleHistoryRequestedKeepWeeks,
    );
  const candleHistoryKeepWeeks = Math.max(
    candleHistoryRequestedKeepWeeks,
    activePipelineCandleGuard.minKeepWeeks,
  );
  const candleRetention = await pruneCandleHistoryRollingWeeks({
    nowMs,
    dryRun,
    keepWeeks: candleHistoryKeepWeeks,
    timeframe: candleHistoryTimeframe,
    skipSymbols: activePipelineCandleGuard.protectedSymbols,
  });

  const reportRefreshed = false;

  return {
    ok: true,
    dryRun,
    generatedAtMs: nowMs,
    generatedAtIso: new Date(nowMs).toISOString(),
    config: {
      cycleRetentionDays,
      requestedCycleRetentionDays,
      inactiveSymbolRetentionDays,
      requestedInactiveSymbolRetentionDays,
      lockMaxAgeMinutes,
      maxScanKeys,
      refreshReport,
      cleanupOrphanDeployments,
      journalMax,
      tradeLedgerMax,
      candleHistoryKeepWeeks,
      requestedCandleHistoryKeepWeeks,
      candleHistoryTimeframe,
      researchPrimaryReferenceWeeks: DEFAULT_RESEARCH_PRIMARY_REFERENCE_WEEKS,
      researchConfirmationKeepWeeks: DEFAULT_RESEARCH_CONFIRMATION_KEEP_WEEKS,
      researchConfirmationBufferWeeks:
        DEFAULT_RESEARCH_CONFIRMATION_BUFFER_WEEKS,
    },
    summary: {
      cyclesPruned: 0,
      cycleKeysDeleted: 0,
      taskKeysDeleted: 0,
      aggregateKeysDeleted: 0,
      claimCursorKeysDeleted: 0,
      researchLocksDeleted: 0,
      runLocksDeleted: 0,
      orphanedDeploymentsPruned:
        orphanedDeploymentPrune.prunedDeploymentIds.length,
      listCompactions,
      candleHistorySymbolsScanned: candleRetention.symbolsScanned,
      candleHistorySymbolsPruned: candleRetention.symbolsPruned,
      candleHistoryCandlesDeleted: candleRetention.candlesDeleted,
      candleHistorySymbolsSkipped:
        activePipelineCandleGuard.protectedSymbols.size,
      reportRefreshed,
      stalePipelineJobLocksCleared: staleRecovery.staleJobLocksCleared,
      stalePipelineLoadRowsRecovered: staleRecovery.staleLoadRowsRecovered,
      stalePipelinePrepareRowsRecovered:
        staleRecovery.stalePrepareRowsRecovered,
      stalePipelineWorkerRowsRecovered: staleRecovery.staleWorkerRowsRecovered,
      weeklyMetricsRowsPruned,
      inactivePipelineSymbolsPruned: prunedInactivePipelineSymbols.length,
    },
    details: {
      prunedCycleIds: [],
      deletedResearchLockKeys: [],
      deletedRunLockKeys: [],
      orphanedDeploymentIds: orphanedDeploymentPrune.prunedDeploymentIds,
      activePipelineRetentionGuard: {
        minKeepWeeks: activePipelineCandleGuard.minKeepWeeks,
        protectedSymbols: Array.from(
          activePipelineCandleGuard.protectedSymbols,
        ).sort(),
        pendingSymbols: activePipelineCandleGuard.pendingSymbols,
      },
      prunedInactivePipelineSymbols,
    },
  };
}
