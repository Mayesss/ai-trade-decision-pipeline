import { Prisma } from '@prisma/client';

import { listScalpCandleHistorySymbols, loadScalpCandleHistory, saveScalpCandleHistory, type ScalpCandleHistoryRecord } from './candleHistory';
import { getScalpStrategyConfig } from './config';
import { isScalpPgConfigured, scalpPrisma } from './pg/client';
import { refreshScalpResearchPortfolioReport } from './researchReporting';
import type { ScalpCandle } from './types';

function toPositiveInt(value: unknown, fallback: number): number {
    const n = Math.floor(Number(value));
    if (!Number.isFinite(n) || n <= 0) return fallback;
    return n;
}

function toBool(value: unknown, fallback: boolean): boolean {
    if (typeof value === 'boolean') return value;
    const normalized = String(value || '')
        .trim()
        .toLowerCase();
    if (!normalized) return fallback;
    if (['1', 'true', 'yes', 'on'].includes(normalized)) return true;
    if (['0', 'false', 'no', 'off'].includes(normalized)) return false;
    return fallback;
}

function toFinite(value: unknown, fallback = 0): number {
    const n = Number(value);
    if (!Number.isFinite(n)) return fallback;
    return n;
}

function isRecord(value: unknown): value is Record<string, unknown> {
    return Boolean(value) && typeof value === 'object' && !Array.isArray(value);
}

const ONE_DAY_MS = 24 * 60 * 60_000;
const ONE_WEEK_MS = 7 * ONE_DAY_MS;

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
}): { candles: ScalpCandle[]; removedCount: number; cutoffWeekStartMs: number } {
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

    const status = String(params.cycle.status || '').trim().toLowerCase();
    const createdAtMs = toFinite(params.cycle.createdAtMs, 0);
    const updatedAtMs = toFinite(params.cycle.updatedAtMs, createdAtMs);
    const refMs = Math.max(createdAtMs, updatedAtMs);
    if (refMs <= 0) return false;

    const ageMs = params.nowMs - refMs;
    if (ageMs < params.retentionMs) return false;

    if (!status) return false;
    if (status === 'running') {
        return ageMs >= params.retentionMs * 2;
    }
    return status === 'completed' || status === 'failed' || status === 'stalled';
}

export interface RunScalpHousekeepingParams {
    dryRun?: boolean;
    nowMs?: number;
    cycleRetentionDays?: number;
    lockMaxAgeMinutes?: number;
    maxScanKeys?: number;
    refreshReport?: boolean;
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
        lockMaxAgeMinutes: number;
        maxScanKeys: number;
        refreshReport: boolean;
        journalMax: number;
        tradeLedgerMax: number;
        candleHistoryKeepWeeks: number;
        candleHistoryTimeframe: string;
    };
    summary: {
        cyclesPruned: number;
        cycleKeysDeleted: number;
        taskKeysDeleted: number;
        aggregateKeysDeleted: number;
        claimCursorKeysDeleted: number;
        researchLocksDeleted: number;
        runLocksDeleted: number;
        listCompactions: number;
        candleHistorySymbolsScanned: number;
        candleHistorySymbolsPruned: number;
        candleHistoryCandlesDeleted: number;
        reportRefreshed: boolean;
    };
    details: {
        prunedCycleIds: string[];
        deletedResearchLockKeys: string[];
        deletedRunLockKeys: string[];
    };
}

interface CandleRetentionResult {
    symbolsScanned: number;
    symbolsPruned: number;
    candlesDeleted: number;
}

function toRecordSummary(record: ScalpCandleHistoryRecord | null): { epic: string | null; source: 'capital' } {
    return {
        epic: record?.epic ?? null,
        source: 'capital',
    };
}

async function pruneCandleHistoryRollingWeeks(params: {
    nowMs: number;
    dryRun: boolean;
    keepWeeks: number;
    timeframe: string;
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
        .map((row) => String(row.cycleId || '').trim())
        .filter((row) => Boolean(row));

    if (!cycleIds.length) {
        return {
            prunedCycleIds: [],
            taskRowsDeleted: 0,
        };
    }

    const taskRows = await db.$queryRaw<Array<{ count: bigint | number }>>(Prisma.sql`
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
    const nowMs = Number.isFinite(Number(params.nowMs)) ? Math.floor(Number(params.nowMs)) : Date.now();
    const dryRun = Boolean(params.dryRun);

    const cycleRetentionDays = toPositiveInt(
        params.cycleRetentionDays ?? process.env.SCALP_HOUSEKEEPING_CYCLE_RETENTION_DAYS,
        14,
    );
    const lockMaxAgeMinutes = toPositiveInt(
        params.lockMaxAgeMinutes ?? process.env.SCALP_HOUSEKEEPING_LOCK_MAX_AGE_MINUTES,
        45,
    );
    const maxScanKeys = toPositiveInt(params.maxScanKeys ?? process.env.SCALP_HOUSEKEEPING_MAX_SCAN_KEYS, 4000);
    const refreshReport = toBool(params.refreshReport ?? process.env.SCALP_HOUSEKEEPING_REFRESH_REPORT, true);
    const candleHistoryKeepWeeks = Math.max(
        1,
        Math.min(
            52,
            toPositiveInt(
                params.candleHistoryKeepWeeks ?? process.env.SCALP_HOUSEKEEPING_CANDLE_HISTORY_KEEP_WEEKS,
                12,
            ),
        ),
    );
    const candleHistoryTimeframe = String(
        params.candleHistoryTimeframe ?? process.env.SCALP_HOUSEKEEPING_CANDLE_HISTORY_TIMEFRAME ?? '1m',
    )
        .trim()
        .toLowerCase();

    const cfg = getScalpStrategyConfig();
    const journalMax = Math.max(
        10,
        Math.min(2_000, toPositiveInt(process.env.SCALP_HOUSEKEEPING_JOURNAL_MAX, cfg.storage.journalMax)),
    );
    const tradeLedgerMax = Math.max(
        200,
        Math.min(50_000, toPositiveInt(process.env.SCALP_HOUSEKEEPING_TRADE_LEDGER_MAX, 10_000)),
    );

    const retentionMs = cycleRetentionDays * 24 * 60 * 60_000;

    const cyclePrune = await pruneResearchCyclesFromPg({
        nowMs,
        retentionMs,
        dryRun,
    });

    const listCompactions = await compactScalpTables({
        dryRun,
        journalMax,
        tradeLedgerMax,
    });
    const candleRetention = await pruneCandleHistoryRollingWeeks({
        nowMs,
        dryRun,
        keepWeeks: candleHistoryKeepWeeks,
        timeframe: candleHistoryTimeframe,
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
            lockMaxAgeMinutes,
            maxScanKeys,
            refreshReport,
            journalMax,
            tradeLedgerMax,
            candleHistoryKeepWeeks,
            candleHistoryTimeframe,
        },
        summary: {
            cyclesPruned: cyclePrune.prunedCycleIds.length,
            cycleKeysDeleted: dryRun ? 0 : cyclePrune.prunedCycleIds.length,
            taskKeysDeleted: dryRun ? 0 : cyclePrune.taskRowsDeleted,
            aggregateKeysDeleted: 0,
            claimCursorKeysDeleted: 0,
            researchLocksDeleted: 0,
            runLocksDeleted: 0,
            listCompactions,
            candleHistorySymbolsScanned: candleRetention.symbolsScanned,
            candleHistorySymbolsPruned: candleRetention.symbolsPruned,
            candleHistoryCandlesDeleted: candleRetention.candlesDeleted,
            reportRefreshed,
        },
        details: {
            prunedCycleIds: cyclePrune.prunedCycleIds,
            deletedResearchLockKeys: [],
            deletedRunLockKeys: [],
        },
    };
}
