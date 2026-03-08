import crypto from 'node:crypto';

import { kvGetJson, kvSetJson } from '../kv';
import { loadScalpCandleHistory } from './candleHistory';
import { resolveScalpDeployment } from './deployments';
import { pipSizeForScalpSymbol } from './marketData';
import { defaultScalpReplayConfig, runScalpReplay } from './replay/harness';
import { listScalpStrategies } from './strategies/registry';
import {
    loadScalpSymbolDiscoveryPolicy,
    loadScalpSymbolUniverseSnapshot,
    resolveRecommendedStrategiesForSymbol,
} from './symbolDiscovery';

const RESEARCH_CYCLE_VERSION = 1 as const;
const RESEARCH_ACTIVE_CYCLE_KEY = 'scalp:research:active-cycle:v1';
const RESEARCH_CYCLE_KEY_PREFIX = 'scalp:research:cycle:v1';
const RESEARCH_TASK_KEY_PREFIX = 'scalp:research:task:v1';
const RESEARCH_AGG_KEY_PREFIX = 'scalp:research:aggregate:v1';
const RESEARCH_LOCK_KEY_PREFIX = 'scalp:research:lock:v1';

const DEFAULT_LOOKBACK_DAYS = 90;
const DEFAULT_CHUNK_DAYS = 14;
const DEFAULT_MIN_CANDLES_PER_TASK = 180;
const DEFAULT_MAX_TASKS = 240;
const DEFAULT_MAX_ATTEMPTS = 2;
const DEFAULT_RUNNING_STALE_AFTER_MS = 20 * 60 * 1000;
const DEFAULT_LOCK_TTL_SECONDS = 120;

const KV_REST_API_URL = (process.env.KV_REST_API_URL || '').replace(/\/$/, '');
const KV_REST_API_TOKEN = process.env.KV_REST_API_TOKEN || '';

export type ScalpResearchCycleStatus = 'running' | 'completed' | 'failed' | 'stalled';
export type ScalpResearchTaskStatus = 'pending' | 'running' | 'completed' | 'failed';

export interface ScalpResearchCycleParams {
    symbols: string[];
    lookbackDays: number;
    chunkDays: number;
    minCandlesPerTask: number;
    maxTasks: number;
    maxAttempts: number;
    runningStaleAfterMs: number;
}

export interface ScalpResearchTask {
    version: 1;
    cycleId: string;
    taskId: string;
    symbol: string;
    strategyId: string;
    tuneId: string;
    deploymentId: string;
    windowFromTs: number;
    windowToTs: number;
    status: ScalpResearchTaskStatus;
    attempts: number;
    createdAtMs: number;
    updatedAtMs: number;
    workerId: string | null;
    startedAtMs: number | null;
    finishedAtMs: number | null;
    errorCode: string | null;
    errorMessage: string | null;
    result: ScalpResearchTaskResult | null;
}

export interface ScalpResearchTaskResult {
    symbol: string;
    strategyId: string;
    tuneId: string;
    deploymentId: string;
    windowFromTs: number;
    windowToTs: number;
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
}

export interface ScalpResearchCandidateAggregate {
    symbol: string;
    strategyId: string;
    tuneId: string;
    deploymentId: string;
    completedTasks: number;
    failedTasks: number;
    trades: number;
    winRatePct: number;
    netR: number;
    expectancyR: number;
    profitFactor: number | null;
    maxDrawdownR: number;
    avgHoldMinutes: number;
    netPnlUsd: number;
    grossProfitR: number;
    grossLossAbsR: number;
}

export interface ScalpResearchCycleSummary {
    cycleId: string;
    status: ScalpResearchCycleStatus;
    totals: {
        tasks: number;
        pending: number;
        running: number;
        completed: number;
        failed: number;
    };
    progressPct: number;
    candidateAggregates: ScalpResearchCandidateAggregate[];
    generatedAtMs: number;
}

export interface ScalpResearchCycleSnapshot {
    version: 1;
    cycleId: string;
    status: ScalpResearchCycleStatus;
    createdAtMs: number;
    updatedAtMs: number;
    startedBy: string | null;
    dryRun: boolean;
    sourceUniverseGeneratedAt: string | null;
    params: ScalpResearchCycleParams;
    symbols: string[];
    taskIds: string[];
    latestSummary: ScalpResearchCycleSummary | null;
}

export interface StartResearchCycleParams {
    dryRun?: boolean;
    force?: boolean;
    symbols?: string[];
    lookbackDays?: number;
    chunkDays?: number;
    minCandlesPerTask?: number;
    maxTasks?: number;
    maxAttempts?: number;
    runningStaleAfterMs?: number;
    startedBy?: string | null;
}

export interface WorkerRunParams {
    cycleId?: string;
    workerId?: string;
    maxRuns?: number;
}

export interface WorkerRunOutcome {
    cycleId: string | null;
    workerId: string;
    attemptedRuns: number;
    completedRuns: number;
    failedRuns: number;
    claimedTasks: Array<{
        taskId: string;
        symbol: string;
        strategyId: string;
        status: ScalpResearchTaskStatus;
        errorCode: string | null;
        errorMessage: string | null;
        trades: number | null;
        netR: number | null;
    }>;
}

export interface AggregateResearchCycleParams {
    cycleId?: string;
    finalizeWhenDone?: boolean;
}

function isRecord(value: unknown): value is Record<string, unknown> {
    return Boolean(value) && typeof value === 'object' && !Array.isArray(value);
}

function normalizeSymbol(value: unknown): string {
    return String(value || '')
        .trim()
        .toUpperCase()
        .replace(/[^A-Z0-9._-]/g, '');
}

function toPositiveInt(value: unknown, fallback: number): number {
    const n = Math.floor(Number(value));
    if (!Number.isFinite(n) || n <= 0) return fallback;
    return n;
}

function toNonNegativeInt(value: unknown, fallback: number): number {
    const n = Math.floor(Number(value));
    if (!Number.isFinite(n) || n < 0) return fallback;
    return n;
}

function toFinite(value: unknown, fallback = 0): number {
    const n = Number(value);
    if (!Number.isFinite(n)) return fallback;
    return n;
}

function dedupe<T>(rows: T[]): T[] {
    return Array.from(new Set(rows));
}

function cycleKey(cycleId: string): string {
    return `${RESEARCH_CYCLE_KEY_PREFIX}:${cycleId}`;
}

function taskKey(cycleId: string, taskId: string): string {
    return `${RESEARCH_TASK_KEY_PREFIX}:${cycleId}:${taskId}`;
}

function aggregateKey(cycleId: string): string {
    return `${RESEARCH_AGG_KEY_PREFIX}:${cycleId}`;
}

function lockKey(name: string): string {
    return `${RESEARCH_LOCK_KEY_PREFIX}:${name}`;
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

async function scanKeysByPrefix(prefix: string, maxKeys: number): Promise<string[]> {
    if (!KV_REST_API_URL || !KV_REST_API_TOKEN) return [];
    const hardCap = Math.max(1, Math.min(20_000, Math.floor(maxKeys)));
    let cursor = '0';
    const keys: string[] = [];

    for (let i = 0; i < 200; i += 1) {
        const res = await kvRawCommand('SCAN', cursor, 'MATCH', `${prefix}*`, 'COUNT', 250);
        if (!Array.isArray(res) || res.length < 2) break;
        const nextCursor = String(res[0] ?? '0');
        const rows = Array.isArray(res[1]) ? res[1] : [];
        for (const row of rows) {
            const key = String(row || '').trim();
            if (!key) continue;
            keys.push(key);
            if (keys.length >= hardCap) {
                return Array.from(new Set(keys));
            }
        }
        if (nextCursor === '0') break;
        cursor = nextCursor;
    }
    return Array.from(new Set(keys));
}

export async function listResearchCycleIds(maxKeys = 200): Promise<string[]> {
    const prefix = `${RESEARCH_CYCLE_KEY_PREFIX}:`;
    const keys = await scanKeysByPrefix(prefix, maxKeys);
    return keys
        .map((key) => key.slice(prefix.length).trim())
        .filter((cycleId) => Boolean(cycleId));
}

export async function loadLatestCompletedResearchCycleId(maxKeys = 200): Promise<string | null> {
    const cycleIds = await listResearchCycleIds(maxKeys);
    let best: { cycleId: string; updatedAtMs: number } | null = null;

    for (const cycleId of cycleIds) {
        const cycle = await loadResearchCycle(cycleId);
        if (!cycle || cycle.status !== 'completed') continue;
        const updatedAtMs = Math.max(0, Math.floor(Number(cycle.updatedAtMs) || Number(cycle.createdAtMs) || 0));
        if (!best || updatedAtMs > best.updatedAtMs) {
            best = { cycleId: cycle.cycleId, updatedAtMs };
        }
    }

    return best?.cycleId || null;
}

async function tryAcquireLock(name: string, token: string, ttlSeconds: number): Promise<boolean> {
    if (!KV_REST_API_URL || !KV_REST_API_TOKEN) return true;
    const key = lockKey(name);
    const ttl = Math.max(10, Math.floor(ttlSeconds));
    const out = await kvRawCommand('SET', key, token, 'NX', 'EX', ttl);
    return String(out || '').toUpperCase() === 'OK';
}

async function releaseLock(name: string, token: string): Promise<void> {
    if (!KV_REST_API_URL || !KV_REST_API_TOKEN) return;
    const key = lockKey(name);
    const current = await kvRawCommand('GET', key);
    if (String(current || '') !== token) return;
    await kvRawCommand('DEL', key);
}

function newToken(prefix: string): string {
    return `${prefix}:${Date.now()}:${crypto.randomUUID()}`;
}

export function buildResearchCycleId(nowMs = Date.now()): string {
    const iso = new Date(nowMs).toISOString().replace(/[-:]/g, '').replace(/\.\d{3}Z$/, 'Z');
    const suffix = crypto.randomBytes(3).toString('hex');
    return `rc_${iso}_${suffix}`;
}

function sanitizeTuneId(value: string): string {
    return String(value)
        .trim()
        .toLowerCase()
        .replace(/[^a-z0-9._-]/g, '_')
        .slice(0, 80);
}

export function buildResearchCycleTasks(params: {
    cycleId: string;
    nowMs: number;
    symbols: string[];
    lookbackDays: number;
    chunkDays: number;
    maxTasks: number;
    strategyAllowlist: string[];
}): ScalpResearchTask[] {
    const symbols = dedupe(params.symbols.map((row) => normalizeSymbol(row)).filter((row) => Boolean(row)));
    const fromTs = params.nowMs - params.lookbackDays * 24 * 60 * 60_000;
    const chunkMs = Math.max(1, params.chunkDays) * 24 * 60 * 60_000;
    const knownStrategies = new Set(listScalpStrategies().map((row) => row.id));
    const tasks: ScalpResearchTask[] = [];

    for (const symbol of symbols) {
        const strategyIds = resolveRecommendedStrategiesForSymbol(symbol, params.strategyAllowlist).filter((id) => knownStrategies.has(id));
        if (!strategyIds.length) continue;
        for (const strategyId of strategyIds) {
            let chunkFrom = fromTs;
            let chunkIdx = 0;
            while (chunkFrom < params.nowMs) {
                const chunkTo = Math.min(params.nowMs, chunkFrom + chunkMs);
                const tuneId = sanitizeTuneId(`research_${symbol}_${strategyId}_${chunkIdx + 1}`);
                const deployment = resolveScalpDeployment({
                    symbol,
                    strategyId,
                    tuneId,
                });
                const taskId = sanitizeTuneId(`${symbol}_${strategyId}_${chunkIdx + 1}`);
                tasks.push({
                    version: RESEARCH_CYCLE_VERSION,
                    cycleId: params.cycleId,
                    taskId,
                    symbol,
                    strategyId,
                    tuneId: deployment.tuneId,
                    deploymentId: deployment.deploymentId,
                    windowFromTs: chunkFrom,
                    windowToTs: chunkTo,
                    status: 'pending',
                    attempts: 0,
                    createdAtMs: params.nowMs,
                    updatedAtMs: params.nowMs,
                    workerId: null,
                    startedAtMs: null,
                    finishedAtMs: null,
                    errorCode: null,
                    errorMessage: null,
                    result: null,
                });
                chunkFrom = chunkTo;
                chunkIdx += 1;
                if (tasks.length >= params.maxTasks) {
                    return tasks;
                }
            }
            if (tasks.length >= params.maxTasks) {
                return tasks;
            }
        }
    }
    return tasks;
}

async function saveCycle(cycle: ScalpResearchCycleSnapshot): Promise<void> {
    await kvSetJson(cycleKey(cycle.cycleId), cycle);
}

async function saveTask(task: ScalpResearchTask): Promise<void> {
    await kvSetJson(taskKey(task.cycleId, task.taskId), task);
}

export async function loadResearchCycle(cycleId: string): Promise<ScalpResearchCycleSnapshot | null> {
    const raw = await kvGetJson<unknown>(cycleKey(cycleId));
    if (!isRecord(raw)) return null;
    return raw as unknown as ScalpResearchCycleSnapshot;
}

export async function loadResearchTask(cycleId: string, taskId: string): Promise<ScalpResearchTask | null> {
    const raw = await kvGetJson<unknown>(taskKey(cycleId, taskId));
    if (!isRecord(raw)) return null;
    return raw as unknown as ScalpResearchTask;
}

export async function loadActiveResearchCycleId(): Promise<string | null> {
    const raw = await kvGetJson<{ cycleId?: string }>(RESEARCH_ACTIVE_CYCLE_KEY);
    const cycleId = String(raw?.cycleId || '').trim();
    return cycleId || null;
}

async function setActiveResearchCycleId(cycleId: string | null): Promise<void> {
    await kvSetJson(RESEARCH_ACTIVE_CYCLE_KEY, {
        cycleId: cycleId || null,
        updatedAtMs: Date.now(),
    });
}

export async function startScalpResearchCycle(params: StartResearchCycleParams = {}): Promise<{
    started: boolean;
    cycle: ScalpResearchCycleSnapshot;
}> {
    const nowMs = Date.now();
    const lockToken = newToken('start');
    const gotLock = await tryAcquireLock('start', lockToken, DEFAULT_LOCK_TTL_SECONDS);
    if (!gotLock) {
        throw new Error('research_cycle_start_locked');
    }

    try {
        const dryRun = Boolean(params.dryRun);
        const force = Boolean(params.force);
        const activeCycleId = await loadActiveResearchCycleId();
        if (!force && activeCycleId) {
            const existing = await loadResearchCycle(activeCycleId);
            if (existing && existing.status === 'running') {
                return { started: false, cycle: existing };
            }
        }

        const policy = await loadScalpSymbolDiscoveryPolicy();
        const universe = await loadScalpSymbolUniverseSnapshot();
        const symbols = dedupe(
            (params.symbols?.length ? params.symbols : universe?.selectedSymbols || policy.pinnedSymbols)
                .map((row) => normalizeSymbol(row))
                .filter((row) => Boolean(row)),
        );

        const cycleId = buildResearchCycleId(nowMs);
        const cycleParams: ScalpResearchCycleParams = {
            symbols,
            lookbackDays: toPositiveInt(params.lookbackDays, DEFAULT_LOOKBACK_DAYS),
            chunkDays: toPositiveInt(params.chunkDays, DEFAULT_CHUNK_DAYS),
            minCandlesPerTask: toPositiveInt(params.minCandlesPerTask, DEFAULT_MIN_CANDLES_PER_TASK),
            maxTasks: toPositiveInt(params.maxTasks, DEFAULT_MAX_TASKS),
            maxAttempts: toPositiveInt(params.maxAttempts, DEFAULT_MAX_ATTEMPTS),
            runningStaleAfterMs: toPositiveInt(params.runningStaleAfterMs, DEFAULT_RUNNING_STALE_AFTER_MS),
        };

        const tasks = buildResearchCycleTasks({
            cycleId,
            nowMs,
            symbols,
            lookbackDays: cycleParams.lookbackDays,
            chunkDays: cycleParams.chunkDays,
            maxTasks: cycleParams.maxTasks,
            strategyAllowlist: policy.strategyAllowlist,
        });

        const cycle: ScalpResearchCycleSnapshot = {
            version: RESEARCH_CYCLE_VERSION,
            cycleId,
            status: 'running',
            createdAtMs: nowMs,
            updatedAtMs: nowMs,
            startedBy: params.startedBy || null,
            dryRun,
            sourceUniverseGeneratedAt: universe?.generatedAtIso || null,
            params: cycleParams,
            symbols,
            taskIds: tasks.map((row) => row.taskId),
            latestSummary: null,
        };

        if (!dryRun) {
            await saveCycle(cycle);
            for (const task of tasks) {
                await saveTask(task);
            }
            await setActiveResearchCycleId(cycleId);
        }

        return {
            started: true,
            cycle,
        };
    } finally {
        await releaseLock('start', lockToken);
    }
}

async function loadAllTasks(cycle: ScalpResearchCycleSnapshot): Promise<ScalpResearchTask[]> {
    const tasks: ScalpResearchTask[] = [];
    for (const taskId of cycle.taskIds) {
        const task = await loadResearchTask(cycle.cycleId, taskId);
        if (task) tasks.push(task);
    }
    return tasks;
}

function toReplayCandles(rows: Array<[number, number, number, number, number, number]>, spreadPips: number) {
    return rows.map((row) => ({
        ts: Number(row[0]),
        open: Number(row[1]),
        high: Number(row[2]),
        low: Number(row[3]),
        close: Number(row[4]),
        volume: Number(row[5] ?? 0),
        spreadPips,
    }));
}

async function runResearchTask(task: ScalpResearchTask, minCandlesPerTask: number): Promise<ScalpResearchTaskResult> {
    const history = await loadScalpCandleHistory(task.symbol, '1m');
    const all = (history.record?.candles || []) as Array<[number, number, number, number, number, number]>;
    const rows = all.filter((row) => row[0] >= task.windowFromTs && row[0] < task.windowToTs);
    if (rows.length < minCandlesPerTask) {
        throw Object.assign(new Error(`insufficient_candles:${rows.length}`), { code: 'insufficient_candles' });
    }

    const base = defaultScalpReplayConfig(task.symbol);
    const deployment = resolveScalpDeployment({
        symbol: task.symbol,
        strategyId: task.strategyId,
        tuneId: task.tuneId,
        deploymentId: task.deploymentId,
    });
    const runtime = {
        ...base,
        symbol: deployment.symbol,
        strategyId: deployment.strategyId,
        tuneId: deployment.tuneId,
        deploymentId: deployment.deploymentId,
        tuneLabel: deployment.tuneLabel,
    };
    const replay = await runScalpReplay({
        candles: toReplayCandles(rows, runtime.defaultSpreadPips),
        pipSize: pipSizeForScalpSymbol(task.symbol),
        config: runtime,
        captureTimeline: false,
    });

    return {
        symbol: replay.summary.symbol,
        strategyId: replay.summary.strategyId,
        tuneId: replay.summary.tuneId,
        deploymentId: replay.summary.deploymentId,
        windowFromTs: task.windowFromTs,
        windowToTs: task.windowToTs,
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
}

async function claimNextTask(cycle: ScalpResearchCycleSnapshot, workerId: string): Promise<{
    task: ScalpResearchTask;
    lockName: string;
    lockToken: string;
} | null> {
    const nowMs = Date.now();
    for (const taskId of cycle.taskIds) {
        const lockName = `${cycle.cycleId}:${taskId}`;
        const lockToken = newToken(`claim:${workerId}`);
        const gotTaskLock = await tryAcquireLock(lockName, lockToken, DEFAULT_LOCK_TTL_SECONDS);
        if (!gotTaskLock) continue;

        const task = await loadResearchTask(cycle.cycleId, taskId);
        if (!task) {
            await releaseLock(lockName, lockToken);
            continue;
        }

        const staleRunning =
            task.status === 'running' &&
            task.startedAtMs !== null &&
            nowMs - task.startedAtMs >= cycle.params.runningStaleAfterMs;

        const retryableFailed = task.status === 'failed' && task.attempts < cycle.params.maxAttempts;
        const claimable = task.status === 'pending' || staleRunning || retryableFailed;

        if (!claimable) {
            await releaseLock(lockName, lockToken);
            continue;
        }

        const nextTask: ScalpResearchTask = {
            ...task,
            status: 'running',
            attempts: task.attempts + 1,
            workerId,
            startedAtMs: nowMs,
            updatedAtMs: nowMs,
            finishedAtMs: null,
            errorCode: null,
            errorMessage: null,
        };
        await saveTask(nextTask);
        return {
            task: nextTask,
            lockName,
            lockToken,
        };
    }
    return null;
}

export async function runResearchWorker(params: WorkerRunParams = {}): Promise<WorkerRunOutcome> {
    const workerId = String(params.workerId || '').trim() || `worker_${crypto.randomUUID().slice(0, 8)}`;
    const cycleId = params.cycleId || (await loadActiveResearchCycleId());
    if (!cycleId) {
        return {
            cycleId: null,
            workerId,
            attemptedRuns: 0,
            completedRuns: 0,
            failedRuns: 0,
            claimedTasks: [],
        };
    }

    const cycle = await loadResearchCycle(cycleId);
    if (!cycle) {
        return {
            cycleId,
            workerId,
            attemptedRuns: 0,
            completedRuns: 0,
            failedRuns: 0,
            claimedTasks: [],
        };
    }

    const maxRuns = Math.max(1, Math.min(10, toPositiveInt(params.maxRuns, 1)));
    const out: WorkerRunOutcome = {
        cycleId,
        workerId,
        attemptedRuns: 0,
        completedRuns: 0,
        failedRuns: 0,
        claimedTasks: [],
    };

    for (let i = 0; i < maxRuns; i += 1) {
        const claim = await claimNextTask(cycle, workerId);
        if (!claim) break;
        out.attemptedRuns += 1;

        try {
            const result = await runResearchTask(claim.task, cycle.params.minCandlesPerTask);
            const completedTask: ScalpResearchTask = {
                ...claim.task,
                status: 'completed',
                result,
                updatedAtMs: Date.now(),
                finishedAtMs: Date.now(),
                errorCode: null,
                errorMessage: null,
            };
            await saveTask(completedTask);
            out.completedRuns += 1;
            out.claimedTasks.push({
                taskId: completedTask.taskId,
                symbol: completedTask.symbol,
                strategyId: completedTask.strategyId,
                status: completedTask.status,
                errorCode: null,
                errorMessage: null,
                trades: result.trades,
                netR: result.netR,
            });
        } catch (err: any) {
            const failedTask: ScalpResearchTask = {
                ...claim.task,
                status: 'failed',
                updatedAtMs: Date.now(),
                finishedAtMs: Date.now(),
                errorCode: String(err?.code || 'task_failed'),
                errorMessage: String(err?.message || err || 'task_failed').slice(0, 300),
            };
            await saveTask(failedTask);
            out.failedRuns += 1;
            out.claimedTasks.push({
                taskId: failedTask.taskId,
                symbol: failedTask.symbol,
                strategyId: failedTask.strategyId,
                status: failedTask.status,
                errorCode: failedTask.errorCode,
                errorMessage: failedTask.errorMessage,
                trades: failedTask.result?.trades || null,
                netR: failedTask.result?.netR || null,
            });
        } finally {
            await releaseLock(claim.lockName, claim.lockToken);
        }
    }

    return out;
}

export function summarizeResearchTasks(cycle: ScalpResearchCycleSnapshot, tasks: ScalpResearchTask[]): ScalpResearchCycleSummary {
    const totals = {
        tasks: tasks.length,
        pending: 0,
        running: 0,
        completed: 0,
        failed: 0,
    };

    const byCandidate = new Map<string, ScalpResearchCandidateAggregate>();

    for (const task of tasks) {
        if (task.status === 'pending') totals.pending += 1;
        else if (task.status === 'running') totals.running += 1;
        else if (task.status === 'completed') totals.completed += 1;
        else if (task.status === 'failed') totals.failed += 1;

        const key = `${task.symbol}::${task.strategyId}`;
        if (!byCandidate.has(key)) {
            byCandidate.set(key, {
                symbol: task.symbol,
                strategyId: task.strategyId,
                tuneId: task.tuneId,
                deploymentId: task.deploymentId,
                completedTasks: 0,
                failedTasks: 0,
                trades: 0,
                winRatePct: 0,
                netR: 0,
                expectancyR: 0,
                profitFactor: null,
                maxDrawdownR: 0,
                avgHoldMinutes: 0,
                netPnlUsd: 0,
                grossProfitR: 0,
                grossLossAbsR: 0,
            });
        }
        const agg = byCandidate.get(key)!;

        if (task.status === 'completed' && task.result) {
            agg.completedTasks += 1;
            agg.trades += task.result.trades;
            agg.netR += task.result.netR;
            agg.netPnlUsd += task.result.netPnlUsd;
            agg.grossProfitR += Math.max(0, task.result.grossProfitR);
            agg.grossLossAbsR += Math.abs(Math.min(0, task.result.grossLossR));
            agg.maxDrawdownR = Math.max(agg.maxDrawdownR, Math.max(0, task.result.maxDrawdownR));
            agg.winRatePct += task.result.winRatePct * task.result.trades;
            agg.avgHoldMinutes += task.result.avgHoldMinutes * task.result.trades;
        }
        if (task.status === 'failed') {
            agg.failedTasks += 1;
        }
    }

    const candidateAggregates = Array.from(byCandidate.values())
        .map((row) => {
            const winRatePct = row.trades > 0 ? row.winRatePct / row.trades : 0;
            const avgHoldMinutes = row.trades > 0 ? row.avgHoldMinutes / row.trades : 0;
            const expectancyR = row.trades > 0 ? row.netR / row.trades : 0;
            const profitFactor = row.grossLossAbsR > 0 ? row.grossProfitR / row.grossLossAbsR : null;
            return {
                ...row,
                winRatePct,
                avgHoldMinutes,
                expectancyR,
                profitFactor,
            };
        })
        .sort((a, b) => {
            if (b.netR !== a.netR) return b.netR - a.netR;
            const pfA = a.profitFactor ?? -1;
            const pfB = b.profitFactor ?? -1;
            if (pfB !== pfA) return pfB - pfA;
            if (a.maxDrawdownR !== b.maxDrawdownR) return a.maxDrawdownR - b.maxDrawdownR;
            return b.trades - a.trades;
        });

    const progressPct = totals.tasks > 0 ? ((totals.completed + totals.failed) / totals.tasks) * 100 : 0;
    const status: ScalpResearchCycleStatus =
        totals.tasks > 0 && totals.completed + totals.failed === totals.tasks
            ? totals.failed > 0 && totals.completed === 0
                ? 'failed'
                : 'completed'
            : totals.running > 0
              ? 'running'
              : totals.pending > 0
                ? 'running'
                : 'stalled';

    return {
        cycleId: cycle.cycleId,
        status,
        totals,
        progressPct,
        candidateAggregates,
        generatedAtMs: Date.now(),
    };
}

export async function aggregateScalpResearchCycle(
    params: AggregateResearchCycleParams = {},
): Promise<{ cycle: ScalpResearchCycleSnapshot; summary: ScalpResearchCycleSummary } | null> {
    const cycleId = params.cycleId || (await loadActiveResearchCycleId());
    if (!cycleId) return null;

    const cycle = await loadResearchCycle(cycleId);
    if (!cycle) return null;

    const lockToken = newToken('aggregate');
    const gotLock = await tryAcquireLock(`aggregate:${cycleId}`, lockToken, DEFAULT_LOCK_TTL_SECONDS);
    if (!gotLock) {
        const tasks = await loadAllTasks(cycle);
        const summary = summarizeResearchTasks(cycle, tasks);
        return { cycle, summary };
    }

    try {
        const tasks = await loadAllTasks(cycle);
        const summary = summarizeResearchTasks(cycle, tasks);
        const nextCycle: ScalpResearchCycleSnapshot = {
            ...cycle,
            status: summary.status,
            updatedAtMs: Date.now(),
            latestSummary: summary,
        };
        await saveCycle(nextCycle);
        await kvSetJson(aggregateKey(cycleId), summary);

        if (params.finalizeWhenDone !== false && summary.status === 'completed') {
            const activeCycleId = await loadActiveResearchCycleId();
            if (activeCycleId === cycleId) {
                await setActiveResearchCycleId(null);
            }
        }

        return { cycle: nextCycle, summary };
    } finally {
        await releaseLock(`aggregate:${cycleId}`, lockToken);
    }
}

export async function loadResearchCycleSummary(cycleId: string): Promise<ScalpResearchCycleSummary | null> {
    return kvGetJson<ScalpResearchCycleSummary>(aggregateKey(cycleId));
}

export async function listResearchCycleTasks(cycleId: string, limit = 1000): Promise<ScalpResearchTask[]> {
    const cycle = await loadResearchCycle(cycleId);
    if (!cycle) return [];
    const max = Math.max(1, Math.min(5000, Math.floor(limit)));
    const out: ScalpResearchTask[] = [];
    for (const taskId of cycle.taskIds.slice(0, max)) {
        const task = await loadResearchTask(cycle.cycleId, taskId);
        if (task) out.push(task);
    }
    return out;
}
