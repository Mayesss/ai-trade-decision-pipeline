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
import { refreshScalpResearchPortfolioReport } from "./researchReporting";
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
  };
  details: {
    prunedCycleIds: string[];
    deletedResearchLockKeys: string[];
    deletedRunLockKeys: string[];
    orphanedDeploymentIds: string[];
    activeCycleRetentionGuard: {
      minKeepWeeks: number;
      protectedSymbols: string[];
      runningCycles: Array<{
        cycleId: string;
        lookbackDays: number;
        derivedLookbackWeeks: number;
        recommendedKeepWeeks: number;
        symbolCount: number;
      }>;
    };
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

interface PgCyclePruneResult {
  prunedCycleIds: string[];
  taskRowsDeleted: number;
}

type ActiveCycleCandleRetentionGuard = {
  minKeepWeeks: number;
  protectedSymbols: Set<string>;
  runningCycles: Array<{
    cycleId: string;
    lookbackDays: number;
    derivedLookbackWeeks: number;
    recommendedKeepWeeks: number;
    symbolCount: number;
  }>;
};

async function loadActiveCycleCandleRetentionGuard(
  fallbackKeepWeeks: number,
): Promise<ActiveCycleCandleRetentionGuard> {
  if (!isScalpPgConfigured()) {
    return {
      minKeepWeeks: fallbackKeepWeeks,
      protectedSymbols: new Set<string>(),
      runningCycles: [],
    };
  }
  const db = scalpPrisma();
  const rows = await db.$queryRaw<
    Array<{
      cycleId: string;
      lookbackDaysText: string | null;
      symbolsJson: unknown;
    }>
  >(Prisma.sql`
        SELECT
            cycle_id AS "cycleId",
            NULLIF(TRIM(params_json->>'lookbackDays'), '') AS "lookbackDaysText",
            params_json->'symbols' AS "symbolsJson"
        FROM scalp_research_cycles
        WHERE status = 'running'::scalp_cycle_status
        ORDER BY created_at DESC
        LIMIT 200;
    `);

  const protectedSymbols = new Set<string>();
  const runningCycles: ActiveCycleCandleRetentionGuard["runningCycles"] = [];
  let minKeepWeeks = Math.max(1, fallbackKeepWeeks);

  for (const row of rows) {
    const cycleId = String(row.cycleId || "").trim();
    if (!cycleId) continue;
    const lookbackDays = Math.max(1, toPositiveInt(row.lookbackDaysText, 90));
    const derivedLookbackWeeks = Math.max(1, Math.ceil(lookbackDays / 7));
    const recommendedKeepWeeks = Math.max(1, derivedLookbackWeeks + 1);
    minKeepWeeks = Math.max(minKeepWeeks, recommendedKeepWeeks);

    const symbols = Array.isArray(row.symbolsJson)
      ? row.symbolsJson
          .map((value) => normalizeSymbol(value))
          .filter((value) => Boolean(value))
      : [];
    for (const symbol of symbols) protectedSymbols.add(symbol);
    runningCycles.push({
      cycleId,
      lookbackDays,
      derivedLookbackWeeks,
      recommendedKeepWeeks,
      symbolCount: symbols.length,
    });
  }

  return {
    minKeepWeeks,
    protectedSymbols,
    runningCycles,
  };
}

async function pruneResearchCyclesFromPg(params: {
  nowMs: number;
  retentionMs: number;
  dryRun: boolean;
}): Promise<PgCyclePruneResult> {
  if (!isScalpPgConfigured()) {
    return {
      prunedCycleIds: [],
      taskRowsDeleted: 0,
    };
  }

  const db = scalpPrisma();
  const completedCutoff = new Date(params.nowMs - params.retentionMs);
  const runningCutoff = new Date(params.nowMs - params.retentionMs * 2);
  const staleRows = await db.$queryRaw<Array<{ cycleId: string }>>(Prisma.sql`
        SELECT cycle_id AS "cycleId"
        FROM scalp_research_cycles
        WHERE (
            status IN ('completed'::scalp_cycle_status, 'failed'::scalp_cycle_status, 'stalled'::scalp_cycle_status)
            AND updated_at < ${completedCutoff}
        )
        OR (
            status = 'running'::scalp_cycle_status
            AND updated_at < ${runningCutoff}
        )
        ORDER BY updated_at ASC
        LIMIT 5000;
    `);

  const cycleIds = staleRows
    .map((row) => String(row.cycleId || "").trim())
    .filter((row) => Boolean(row));

  if (!cycleIds.length) {
    return {
      prunedCycleIds: [],
      taskRowsDeleted: 0,
    };
  }

  const taskRows = await db.$queryRaw<
    Array<{ count: bigint | number }>
  >(Prisma.sql`
        SELECT COUNT(*)::bigint AS count
        FROM scalp_research_tasks
        WHERE cycle_id IN (${Prisma.join(cycleIds)});
    `);
  const taskRowsDeleted = Number(taskRows[0]?.count || 0);

  if (!params.dryRun) {
    await db.$executeRaw(Prisma.sql`
            DELETE FROM scalp_research_cycles
            WHERE cycle_id IN (${Prisma.join(cycleIds)});
        `);
  }

  return {
    prunedCycleIds: cycleIds,
    taskRowsDeleted,
  };
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
      researchTaskCount: bigint | number;
    }>
  >(Prisma.sql`
        SELECT
            d.deployment_id AS "deploymentId",
            d.source,
            d.enabled,
            COUNT(t.task_id)::bigint AS "researchTaskCount"
        FROM scalp_deployments d
        LEFT JOIN scalp_research_tasks t
          ON t.deployment_id = d.deployment_id
        GROUP BY d.deployment_id, d.source, d.enabled
        ORDER BY d.created_at ASC, d.deployment_id ASC
        LIMIT 10000;
    `);

  const deploymentIds = rows
    .filter((row) =>
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

  const retentionPolicy = resolveScalpResearchHistoryRetentionPolicy();

  const requestedCycleRetentionDays = toOptionalPositiveInt(
    params.cycleRetentionDays ??
      process.env.SCALP_HOUSEKEEPING_CYCLE_RETENTION_DAYS,
  );
  const cycleRetentionDays = Math.max(
    retentionPolicy.minimumCycleRetentionDays,
    requestedCycleRetentionDays ?? retentionPolicy.minimumCycleRetentionDays,
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
    true,
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
  const candleHistoryRequestedKeepWeeks = Math.max(
    retentionPolicy.minimumRetainedWeeks,
    Math.min(
      MAX_RESEARCH_CONFIRMATION_KEEP_WEEKS,
      requestedCandleHistoryKeepWeeks ?? retentionPolicy.minimumRetainedWeeks,
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

  const cyclePrune = await pruneResearchCyclesFromPg({
    nowMs,
    retentionMs,
    dryRun,
  });
  const orphanedDeploymentPrune = await pruneOrphanedDeploymentsFromPg({
    dryRun,
    enabled: cleanupOrphanDeployments,
  });

  const listCompactions = await compactScalpTables({
    dryRun,
    journalMax,
    tradeLedgerMax,
  });
  const activeCycleCandleGuard = await loadActiveCycleCandleRetentionGuard(
    candleHistoryRequestedKeepWeeks,
  );
  const candleHistoryKeepWeeks = Math.max(
    candleHistoryRequestedKeepWeeks,
    activeCycleCandleGuard.minKeepWeeks,
  );
  const candleRetention = await pruneCandleHistoryRollingWeeks({
    nowMs,
    dryRun,
    keepWeeks: candleHistoryKeepWeeks,
    timeframe: candleHistoryTimeframe,
    skipSymbols: activeCycleCandleGuard.protectedSymbols,
  });

  let reportRefreshed = false;
  if (refreshReport) {
    await refreshScalpResearchPortfolioReport({ nowMs, persist: false });
    reportRefreshed = true;
  }

  return {
    ok: true,
    dryRun,
    generatedAtMs: nowMs,
    generatedAtIso: new Date(nowMs).toISOString(),
    config: {
      cycleRetentionDays,
      requestedCycleRetentionDays,
      lockMaxAgeMinutes,
      maxScanKeys,
      refreshReport,
      cleanupOrphanDeployments,
      journalMax,
      tradeLedgerMax,
      candleHistoryKeepWeeks,
      requestedCandleHistoryKeepWeeks,
      candleHistoryTimeframe,
      researchPrimaryReferenceWeeks: retentionPolicy.primaryReferenceWeeks,
      researchConfirmationKeepWeeks: retentionPolicy.confirmationKeepWeeks,
      researchConfirmationBufferWeeks: retentionPolicy.confirmationBufferWeeks,
    },
    summary: {
      cyclesPruned: cyclePrune.prunedCycleIds.length,
      cycleKeysDeleted: dryRun ? 0 : cyclePrune.prunedCycleIds.length,
      taskKeysDeleted: dryRun ? 0 : cyclePrune.taskRowsDeleted,
      aggregateKeysDeleted: 0,
      claimCursorKeysDeleted: 0,
      researchLocksDeleted: 0,
      runLocksDeleted: 0,
      orphanedDeploymentsPruned: orphanedDeploymentPrune.prunedDeploymentIds.length,
      listCompactions,
      candleHistorySymbolsScanned: candleRetention.symbolsScanned,
      candleHistorySymbolsPruned: candleRetention.symbolsPruned,
      candleHistoryCandlesDeleted: candleRetention.candlesDeleted,
      candleHistorySymbolsSkipped: activeCycleCandleGuard.protectedSymbols.size,
      reportRefreshed,
    },
    details: {
      prunedCycleIds: cyclePrune.prunedCycleIds,
      deletedResearchLockKeys: [],
      deletedRunLockKeys: [],
      orphanedDeploymentIds: orphanedDeploymentPrune.prunedDeploymentIds,
      activeCycleRetentionGuard: {
        minKeepWeeks: activeCycleCandleGuard.minKeepWeeks,
        protectedSymbols: Array.from(
          activeCycleCandleGuard.protectedSymbols,
        ).sort(),
        runningCycles: activeCycleCandleGuard.runningCycles,
      },
    },
  };
}
