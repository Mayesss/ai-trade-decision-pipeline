import { getScalpStrategyConfig } from './config';
import { refreshScalpResearchPortfolioReport } from './researchReporting';

const KV_REST_API_URL = (process.env.KV_REST_API_URL || '').replace(/\/$/, '');
const KV_REST_API_TOKEN = process.env.KV_REST_API_TOKEN || '';

const RESEARCH_ACTIVE_CYCLE_KEY = 'scalp:research:active-cycle:v1';
const RESEARCH_CYCLE_KEY_PREFIX = 'scalp:research:cycle:v1';
const RESEARCH_TASK_KEY_PREFIX = 'scalp:research:task:v1';
const RESEARCH_AGG_KEY_PREFIX = 'scalp:research:aggregate:v1';
const RESEARCH_CLAIM_CURSOR_KEY_PREFIX = 'scalp:research:claim-cursor:v1';
const RESEARCH_LOCK_KEY_PREFIX = 'scalp:research:lock:v1';
const RUN_LOCK_KEY_PREFIX = 'scalp:runlock:v2';
const JOURNAL_LIST_KEY = 'scalp:journal:list:v1';
const TRADE_LEDGER_LIST_KEY = 'scalp:trade-ledger:list:v1';

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

async function kvRawCommand(command: string, ...args: Array<string | number>): Promise<unknown> {
    if (!KV_REST_API_URL || !KV_REST_API_TOKEN) return null;
    const encodedArgs = args
        .map((arg) => encodeURIComponent(typeof arg === 'string' ? arg : String(arg)))
        .join('/');
    const url = `${KV_REST_API_URL}/${command}${encodedArgs ? `/${encodedArgs}` : ''}`;
    const res = await fetch(url, {
        method: 'POST',
        headers: {
            Authorization: `Bearer ${KV_REST_API_TOKEN}`,
        },
    });
    if (!res.ok) return null;
    const data = await res.json().catch(() => null);
    if (!data || typeof data !== 'object') return null;
    return (data as any).result ?? null;
}

async function kvDel(key: string): Promise<boolean> {
    const out = await kvRawCommand('DEL', key);
    return toFinite(out, 0) > 0;
}

async function kvGetJson(key: string): Promise<unknown> {
    const out = await kvRawCommand('GET', key);
    if (typeof out !== 'string') return out;
    const text = out.trim();
    if (!text) return null;
    try {
        return JSON.parse(text);
    } catch {
        return null;
    }
}

async function kvTtl(key: string): Promise<number | null> {
    const out = await kvRawCommand('TTL', key);
    if (out === null || out === undefined) return null;
    const n = Number(out);
    if (!Number.isFinite(n)) return null;
    return Math.floor(n);
}

async function kvLTrim(key: string, start: number, stop: number): Promise<void> {
    await kvRawCommand('LTRIM', key, start, stop);
}

async function scanKeysByPrefix(prefix: string, maxKeys: number): Promise<string[]> {
    if (!KV_REST_API_URL || !KV_REST_API_TOKEN) return [];
    const out: string[] = [];
    let cursor = '0';
    const hardCap = Math.max(1, Math.min(20_000, Math.floor(maxKeys)));

    for (let i = 0; i < 200; i += 1) {
        const res = await kvRawCommand('SCAN', cursor, 'MATCH', `${prefix}*`, 'COUNT', 250);
        if (!Array.isArray(res) || res.length < 2) break;
        const nextCursor = String(res[0] ?? '0');
        const keysRaw = Array.isArray(res[1]) ? res[1] : [];
        for (const key of keysRaw) {
            const normalized = String(key || '').trim();
            if (!normalized) continue;
            out.push(normalized);
            if (out.length >= hardCap) return Array.from(new Set(out));
        }
        if (nextCursor === '0') break;
        cursor = nextCursor;
    }

    return Array.from(new Set(out));
}

function parseResearchLockTimestampMs(token: unknown): number | null {
    const value = String(token || '').trim();
    if (!value) return null;
    const parts = value.split(':');
    if (parts.length < 3) return null;
    const ts = Number(parts[1]);
    if (!Number.isFinite(ts) || ts <= 0) return null;
    return Math.floor(ts);
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
        // prune stale orphaned running cycles only if very old and not active
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
        reportRefreshed: boolean;
    };
    details: {
        prunedCycleIds: string[];
        deletedResearchLockKeys: string[];
        deletedRunLockKeys: string[];
    };
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
    const lockMaxAgeMs = lockMaxAgeMinutes * 60_000;

    let cyclesPruned = 0;
    let cycleKeysDeleted = 0;
    let taskKeysDeleted = 0;
    let aggregateKeysDeleted = 0;
    let claimCursorKeysDeleted = 0;
    let researchLocksDeleted = 0;
    let runLocksDeleted = 0;
    let listCompactions = 0;
    let reportRefreshed = false;

    const prunedCycleIds: string[] = [];
    const deletedResearchLockKeys: string[] = [];
    const deletedRunLockKeys: string[] = [];

    const activeRaw = await kvGetJson(RESEARCH_ACTIVE_CYCLE_KEY);
    const activeCycleId = isRecord(activeRaw) ? String(activeRaw.cycleId || '').trim() || null : null;

    const cycleKeys = await scanKeysByPrefix(`${RESEARCH_CYCLE_KEY_PREFIX}:`, maxScanKeys);
    for (const cycleKey of cycleKeys) {
        const cycle = await kvGetJson(cycleKey);
        const cycleIdFromKey = cycleKey.slice(`${RESEARCH_CYCLE_KEY_PREFIX}:`.length);
        const shouldPrune = shouldPruneResearchCycle({
            cycle,
            nowMs,
            activeCycleId,
            retentionMs,
            cycleIdFromKey,
        });
        if (!shouldPrune) continue;

        const cycleId = String((isRecord(cycle) ? cycle.cycleId : '') || cycleIdFromKey).trim();
        const taskIds = isRecord(cycle) && Array.isArray(cycle.taskIds) ? cycle.taskIds : [];

        cyclesPruned += 1;
        prunedCycleIds.push(cycleId);

        if (dryRun) continue;

        if (await kvDel(cycleKey)) cycleKeysDeleted += 1;
        const aggKey = `${RESEARCH_AGG_KEY_PREFIX}:${cycleId}`;
        if (await kvDel(aggKey)) aggregateKeysDeleted += 1;
        const claimCursorKey = `${RESEARCH_CLAIM_CURSOR_KEY_PREFIX}:${cycleId}`;
        if (await kvDel(claimCursorKey)) claimCursorKeysDeleted += 1;

        for (const taskIdRaw of taskIds) {
            const taskId = String(taskIdRaw || '').trim();
            if (!taskId) continue;
            const taskKey = `${RESEARCH_TASK_KEY_PREFIX}:${cycleId}:${taskId}`;
            if (await kvDel(taskKey)) taskKeysDeleted += 1;
        }
    }

    const researchLockKeys = await scanKeysByPrefix(`${RESEARCH_LOCK_KEY_PREFIX}:`, maxScanKeys);
    for (const key of researchLockKeys) {
        const ttl = await kvTtl(key);
        const token = await kvRawCommand('GET', key);
        const tokenTs = parseResearchLockTimestampMs(token);
        const staleByAge = tokenTs !== null && nowMs - tokenTs > lockMaxAgeMs;
        const staleByTtl = ttl === -1; // lock without expiry should not persist
        if (!staleByAge && !staleByTtl) continue;
        deletedResearchLockKeys.push(key);
        if (!dryRun && (await kvDel(key))) {
            researchLocksDeleted += 1;
        }
    }

    const runLockKeys = await scanKeysByPrefix(`${RUN_LOCK_KEY_PREFIX}:`, maxScanKeys);
    for (const key of runLockKeys) {
        const ttl = await kvTtl(key);
        const stale = ttl === -1;
        if (!stale) continue;
        deletedRunLockKeys.push(key);
        if (!dryRun && (await kvDel(key))) {
            runLocksDeleted += 1;
        }
    }

    if (!dryRun) {
        await kvLTrim(JOURNAL_LIST_KEY, 0, journalMax - 1);
        await kvLTrim(TRADE_LEDGER_LIST_KEY, 0, tradeLedgerMax - 1);
        listCompactions = 2;
    }

    if (refreshReport) {
        await refreshScalpResearchPortfolioReport({ nowMs, persist: !dryRun });
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
        },
        summary: {
            cyclesPruned,
            cycleKeysDeleted,
            taskKeysDeleted,
            aggregateKeysDeleted,
            claimCursorKeysDeleted,
            researchLocksDeleted,
            runLocksDeleted,
            listCompactions,
            reportRefreshed,
        },
        details: {
            prunedCycleIds,
            deletedResearchLockKeys,
            deletedRunLockKeys,
        },
    };
}
