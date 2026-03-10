import crypto from 'node:crypto';

import { kvGetJson, kvMGetJson, kvSetJson } from '../kv';
import type { ScalpStrategyConfigOverride } from './config';
import { loadScalpCandleHistory } from './candleHistory';
import { resolveScalpDeployment } from './deployments';
import { pipSizeForScalpSymbol } from './marketData';
import { defaultScalpReplayConfig, runScalpReplay } from './replay/harness';
import { buildScalpReplayRuntimeFromDeployment } from './replay/runtimeConfig';
import { buildScalpResearchTuneVariants, resolveScalpResearchTunerPolicy } from './researchTuner';
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
const RESEARCH_CLAIM_CURSOR_KEY_PREFIX = 'scalp:research:claim-cursor:v1';
const RESEARCH_LOCK_KEY_PREFIX = 'scalp:research:lock:v1';
const RESEARCH_WORKER_HEARTBEAT_KEY = 'scalp:research:worker-heartbeat:v1';
const RESEARCH_SYMBOL_COOLDOWN_KEY = 'scalp:research:symbol-cooldown:v1';

const DEFAULT_LOOKBACK_DAYS = 90;
const DEFAULT_CHUNK_DAYS = 14;
const DEFAULT_MIN_CANDLES_PER_TASK = 180;
const DEFAULT_MAX_TASKS = 4_000;
const DEFAULT_MAX_ATTEMPTS = 2;
const DEFAULT_RUNNING_STALE_AFTER_MS = 20 * 60 * 1000;
const DEFAULT_SYMBOL_COOLDOWN_FAILURE_THRESHOLD = 3;
const DEFAULT_SYMBOL_COOLDOWN_WINDOW_MS = 30 * 60 * 1000;
const DEFAULT_SYMBOL_COOLDOWN_DURATION_MS = 3 * 60 * 60 * 1000;
const DEFAULT_SYMBOL_COOLDOWN_MAX_TRACKED_SYMBOLS = 400;
const DEFAULT_WORKER_MAX_RUNS = 40;
const DEFAULT_WORKER_MAX_RUNS_CAP = 200;
const DEFAULT_WORKER_CONCURRENCY = 4;
const DEFAULT_WORKER_MAX_CONCURRENCY = 16;
const DEFAULT_WORKER_MAX_DURATION_MS = 105_000;
const DEFAULT_WORKER_MAX_DURATION_CAP_MS = 15 * 60_000;
const DEFAULT_CLAIM_SCAN_BATCH_SIZE = 64;
const DEFAULT_LOCK_TTL_SECONDS = 120;
const DEFAULT_TASK_TIMEOUT_MS = 60_000;
const RESEARCH_TASK_TIMEOUT_MS = Math.max(
    1_000,
    Math.min(10 * 60_000, Math.floor(Number(process.env.SCALP_RESEARCH_TASK_TIMEOUT_MS) || DEFAULT_TASK_TIMEOUT_MS)),
);
const RESEARCH_KV_HTTP_TIMEOUT_MS = Math.max(
    1000,
    Math.min(60_000, Math.floor(Number(process.env.SCALP_RESEARCH_KV_HTTP_TIMEOUT_MS) || 10_000)),
);
const RESEARCH_KV_MAX_RETRIES = Math.max(
    0,
    Math.min(8, Math.floor(Number(process.env.SCALP_RESEARCH_KV_MAX_RETRIES) || 3)),
);
const RESEARCH_KV_RETRY_BASE_MS = Math.max(
    25,
    Math.min(5_000, Math.floor(Number(process.env.SCALP_RESEARCH_KV_RETRY_BASE_MS) || 200)),
);
const RESEARCH_KV_RETRY_MAX_DELAY_MS = Math.max(
    50,
    Math.min(15_000, Math.floor(Number(process.env.SCALP_RESEARCH_KV_RETRY_MAX_DELAY_MS) || 2_000)),
);
const RESEARCH_CLAIM_SCAN_BATCH_SIZE = Math.max(
    8,
    Math.min(512, Math.floor(Number(process.env.SCALP_RESEARCH_CLAIM_SCAN_BATCH_SIZE) || DEFAULT_CLAIM_SCAN_BATCH_SIZE)),
);

const upstash_payasyougo_KV_REST_API_URL = (process.env.upstash_payasyougo_KV_REST_API_URL || '').replace(/\/$/, '');
const upstash_payasyougo_KV_REST_API_TOKEN = process.env.upstash_payasyougo_KV_REST_API_TOKEN || '';

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
    tunerEnabled?: boolean;
    maxTuneVariantsPerStrategy?: number;
}

export interface ScalpResearchTask {
    version: 1;
    cycleId: string;
    taskId: string;
    symbol: string;
    strategyId: string;
    tuneId: string;
    deploymentId: string;
    configOverride?: ScalpStrategyConfigOverride | null;
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
    configOverride?: ScalpStrategyConfigOverride | null;
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
    tunerEnabled?: boolean;
    maxTuneVariantsPerStrategy?: number;
    startedBy?: string | null;
}

export interface WorkerRunParams {
    cycleId?: string;
    workerId?: string;
    maxRuns?: number;
    concurrency?: number;
    maxDurationMs?: number;
    debug?: boolean;
}

export type ResearchWorkerHeartbeatStatus =
    | 'started'
    | 'completed'
    | 'failed'
    | 'no_cycle'
    | 'cycle_not_found'
    | 'cycle_not_running';

export interface ScalpResearchWorkerHeartbeatSnapshot {
    version: 1;
    updatedAtMs: number;
    status: ResearchWorkerHeartbeatStatus;
    cycleId: string | null;
    workerId: string;
    maxRuns: number;
    concurrency: number;
    maxDurationMs: number;
    startedAtMs: number;
    finishedAtMs: number | null;
    durationMs: number | null;
    attemptedRuns: number;
    completedRuns: number;
    failedRuns: number;
    stoppedByDurationBudget: boolean;
    noClaimScanSummary: WorkerNoClaimScanSummary | null;
    error: string | null;
}

export interface WorkerRunOutcome {
    cycleId: string | null;
    workerId: string;
    maxRuns: number;
    concurrency: number;
    maxDurationMs: number;
    attemptedRuns: number;
    completedRuns: number;
    failedRuns: number;
    stoppedByDurationBudget: boolean;
    noClaimScanSummary: WorkerNoClaimScanSummary | null;
    diagnostics: {
        durationMs: number;
        cycleTaskCount: number;
        scanTasksVisited: number;
        taskLockAttempts: number;
        taskLockAcquired: number;
        historyCacheHits: number;
        historyCacheMisses: number;
        historySymbolsLoaded: number;
        historyCandlesLoaded: number;
        windowCandlesProcessed: number;
    };
    claimedTasks: Array<{
        taskId: string;
        symbol: string;
        strategyId: string;
        tuneId: string;
        status: ScalpResearchTaskStatus;
        errorCode: string | null;
        errorMessage: string | null;
        trades: number | null;
        netR: number | null;
        durationMs: number | null;
        scanIndex: number | null;
    }>;
}

export type WorkerNoClaimScanSummary = {
    scannedTasks: number;
    lockAttempts: number;
    locksAcquired: number;
    lockMisses: number;
    missingTask: number;
    pending: number;
    runningFresh: number;
    runningStale: number;
    runningMissingStartedAt: number;
    failedRetryable: number;
    failedMaxed: number;
    completed: number;
    symbolCooldownBlocked: number;
};

type ClaimScanSummary = WorkerNoClaimScanSummary;

export interface AggregateResearchCycleParams {
    cycleId?: string;
    finalizeWhenDone?: boolean;
}

export interface ResearchWorkerRuntimeConfig {
    maxRuns: number;
    concurrency: number;
    maxDurationMs: number;
    maxRunsCap: number;
    maxConcurrency: number;
    maxDurationCapMs: number;
}

export interface ResearchSymbolCooldownEntry {
    failureCount: number;
    windowStartedAtMs: number;
    blockedUntilMs: number;
    lastFailureCode: string | null;
    lastFailureMessage: string | null;
    updatedAtMs: number;
    cycleId: string | null;
}

export interface ResearchSymbolCooldownSnapshot {
    version: 1;
    updatedAtMs: number;
    symbols: Record<string, ResearchSymbolCooldownEntry>;
}

export interface ResearchSymbolCooldownConfig {
    enabled: boolean;
    failureThreshold: number;
    failureWindowMs: number;
    cooldownMs: number;
    maxTrackedSymbols: number;
}

export interface ResearchTaskClaimability {
    claimable: boolean;
    runningStale: boolean;
    runningMissingStartedAt: boolean;
    maxAttemptsReached: boolean;
    shouldMarkFailedForAttempts: boolean;
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

function toOptionalText(value: unknown, max = 200): string | null {
    const normalized = String(value || '').trim();
    if (!normalized) return null;
    return normalized.slice(0, Math.max(1, Math.floor(max)));
}

function dedupe<T>(rows: T[]): T[] {
    return Array.from(new Set(rows));
}

export function resolveResearchSymbolCooldownConfig(): ResearchSymbolCooldownConfig {
    return {
        enabled: toBool(process.env.SCALP_RESEARCH_SYMBOL_COOLDOWN_ENABLED, true),
        failureThreshold: Math.max(
            1,
            toPositiveInt(process.env.SCALP_RESEARCH_SYMBOL_COOLDOWN_FAILURE_THRESHOLD, DEFAULT_SYMBOL_COOLDOWN_FAILURE_THRESHOLD),
        ),
        failureWindowMs: Math.max(
            60_000,
            toPositiveInt(process.env.SCALP_RESEARCH_SYMBOL_COOLDOWN_WINDOW_MS, DEFAULT_SYMBOL_COOLDOWN_WINDOW_MS),
        ),
        cooldownMs: Math.max(
            60_000,
            toPositiveInt(process.env.SCALP_RESEARCH_SYMBOL_COOLDOWN_DURATION_MS, DEFAULT_SYMBOL_COOLDOWN_DURATION_MS),
        ),
        maxTrackedSymbols: Math.max(
            10,
            Math.min(
                5000,
                toPositiveInt(
                    process.env.SCALP_RESEARCH_SYMBOL_COOLDOWN_MAX_TRACKED_SYMBOLS,
                    DEFAULT_SYMBOL_COOLDOWN_MAX_TRACKED_SYMBOLS,
                ),
            ),
        ),
    };
}

export function resolveResearchWorkerRuntimeConfig(
    params: { maxRuns?: number; concurrency?: number; maxDurationMs?: number } = {},
    env: NodeJS.ProcessEnv = process.env,
): ResearchWorkerRuntimeConfig {
    const maxRunsCap = Math.max(
        1,
        Math.min(2_000, toPositiveInt(env.SCALP_RESEARCH_WORKER_MAX_RUNS_CAP, DEFAULT_WORKER_MAX_RUNS_CAP)),
    );
    const maxConcurrency = Math.max(
        1,
        Math.min(128, toPositiveInt(env.SCALP_RESEARCH_WORKER_MAX_CONCURRENCY, DEFAULT_WORKER_MAX_CONCURRENCY)),
    );
    const requestedMaxRuns = toPositiveInt(params.maxRuns, DEFAULT_WORKER_MAX_RUNS);
    const maxRuns = Math.max(1, Math.min(maxRunsCap, requestedMaxRuns));
    const defaultConcurrency = Math.max(
        1,
        Math.min(maxConcurrency, toPositiveInt(env.SCALP_RESEARCH_WORKER_CONCURRENCY, DEFAULT_WORKER_CONCURRENCY)),
    );
    const requestedConcurrency = toPositiveInt(params.concurrency, defaultConcurrency);
    const concurrency = Math.max(1, Math.min(maxRuns, maxConcurrency, requestedConcurrency));
    const maxDurationCapMs = Math.max(
        10_000,
        Math.min(
            30 * 60_000,
            toPositiveInt(env.SCALP_RESEARCH_WORKER_MAX_DURATION_CAP_MS, DEFAULT_WORKER_MAX_DURATION_CAP_MS),
        ),
    );
    const defaultMaxDurationMs = Math.max(
        5_000,
        Math.min(
            maxDurationCapMs,
            toPositiveInt(env.SCALP_RESEARCH_WORKER_MAX_DURATION_MS, DEFAULT_WORKER_MAX_DURATION_MS),
        ),
    );
    const requestedMaxDurationMs = toPositiveInt(params.maxDurationMs, defaultMaxDurationMs);
    const maxDurationMs = Math.max(5_000, Math.min(maxDurationCapMs, requestedMaxDurationMs));
    return {
        maxRuns,
        concurrency,
        maxDurationMs,
        maxRunsCap,
        maxConcurrency,
        maxDurationCapMs,
    };
}

function normalizeResearchWorkerHeartbeatStatus(value: unknown): ResearchWorkerHeartbeatStatus {
    const normalized = String(value || '')
        .trim()
        .toLowerCase();
    if (
        normalized === 'started' ||
        normalized === 'completed' ||
        normalized === 'failed' ||
        normalized === 'no_cycle' ||
        normalized === 'cycle_not_found' ||
        normalized === 'cycle_not_running'
    ) {
        return normalized;
    }
    return 'failed';
}

function normalizeWorkerNoClaimScanSummary(raw: unknown): WorkerNoClaimScanSummary | null {
    if (!isRecord(raw)) return null;
    return {
        scannedTasks: toNonNegativeInt(raw.scannedTasks, 0),
        lockAttempts: toNonNegativeInt(raw.lockAttempts, 0),
        locksAcquired: toNonNegativeInt(raw.locksAcquired, 0),
        lockMisses: toNonNegativeInt(raw.lockMisses, 0),
        missingTask: toNonNegativeInt(raw.missingTask, 0),
        pending: toNonNegativeInt(raw.pending, 0),
        runningFresh: toNonNegativeInt(raw.runningFresh, 0),
        runningStale: toNonNegativeInt(raw.runningStale, 0),
        runningMissingStartedAt: toNonNegativeInt(raw.runningMissingStartedAt, 0),
        failedRetryable: toNonNegativeInt(raw.failedRetryable, 0),
        failedMaxed: toNonNegativeInt(raw.failedMaxed, 0),
        completed: toNonNegativeInt(raw.completed, 0),
        symbolCooldownBlocked: toNonNegativeInt(raw.symbolCooldownBlocked, 0),
    };
}

function normalizeResearchWorkerHeartbeat(raw: unknown): ScalpResearchWorkerHeartbeatSnapshot | null {
    if (!isRecord(raw)) return null;
    return {
        version: 1,
        updatedAtMs: toNonNegativeInt(raw.updatedAtMs, Date.now()),
        status: normalizeResearchWorkerHeartbeatStatus(raw.status),
        cycleId: toOptionalText(raw.cycleId, 140),
        workerId: toOptionalText(raw.workerId, 140) || 'worker_unknown',
        maxRuns: Math.max(1, toPositiveInt(raw.maxRuns, DEFAULT_WORKER_MAX_RUNS)),
        concurrency: Math.max(1, toPositiveInt(raw.concurrency, DEFAULT_WORKER_CONCURRENCY)),
        maxDurationMs: Math.max(1_000, toPositiveInt(raw.maxDurationMs, DEFAULT_WORKER_MAX_DURATION_MS)),
        startedAtMs: toNonNegativeInt(raw.startedAtMs, 0),
        finishedAtMs: Number.isFinite(Number(raw.finishedAtMs)) ? toNonNegativeInt(raw.finishedAtMs, 0) : null,
        durationMs: Number.isFinite(Number(raw.durationMs)) ? toNonNegativeInt(raw.durationMs, 0) : null,
        attemptedRuns: toNonNegativeInt(raw.attemptedRuns, 0),
        completedRuns: toNonNegativeInt(raw.completedRuns, 0),
        failedRuns: toNonNegativeInt(raw.failedRuns, 0),
        stoppedByDurationBudget: toBool(raw.stoppedByDurationBudget, false),
        noClaimScanSummary: normalizeWorkerNoClaimScanSummary(raw.noClaimScanSummary),
        error: toOptionalText(raw.error, 300),
    };
}

function normalizeResearchSymbolCooldownEntry(raw: unknown): ResearchSymbolCooldownEntry {
    if (!isRecord(raw)) {
        return {
            failureCount: 0,
            windowStartedAtMs: 0,
            blockedUntilMs: 0,
            lastFailureCode: null,
            lastFailureMessage: null,
            updatedAtMs: 0,
            cycleId: null,
        };
    }
    return {
        failureCount: toNonNegativeInt(raw.failureCount, 0),
        windowStartedAtMs: toNonNegativeInt(raw.windowStartedAtMs, 0),
        blockedUntilMs: toNonNegativeInt(raw.blockedUntilMs, 0),
        lastFailureCode: toOptionalText(raw.lastFailureCode, 80),
        lastFailureMessage: toOptionalText(raw.lastFailureMessage, 220),
        updatedAtMs: toNonNegativeInt(raw.updatedAtMs, 0),
        cycleId: toOptionalText(raw.cycleId, 120),
    };
}

function normalizeResearchSymbolCooldownSnapshot(raw: unknown): ResearchSymbolCooldownSnapshot {
    const out: ResearchSymbolCooldownSnapshot = {
        version: 1,
        updatedAtMs: Date.now(),
        symbols: {},
    };
    if (!isRecord(raw)) return out;
    out.updatedAtMs = toNonNegativeInt(raw.updatedAtMs, Date.now());
    const symbolsRaw = isRecord(raw.symbols) ? raw.symbols : {};
    for (const [rawSymbol, rawEntry] of Object.entries(symbolsRaw)) {
        const symbol = normalizeSymbol(rawSymbol);
        if (!symbol) continue;
        out.symbols[symbol] = normalizeResearchSymbolCooldownEntry(rawEntry);
    }
    return out;
}

export function createEmptyResearchSymbolCooldownSnapshot(nowMs = Date.now()): ResearchSymbolCooldownSnapshot {
    return {
        version: 1,
        updatedAtMs: Math.max(0, Math.floor(nowMs)),
        symbols: {},
    };
}

export function isResearchTaskFailureEligibleForSymbolCooldown(errorCode: unknown, errorMessage: unknown): boolean {
    const code = String(errorCode || '')
        .trim()
        .toLowerCase();
    if (!code) return false;
    if (code === 'insufficient_candles' || code === 'task_timeout') return true;
    const msg = String(errorMessage || '')
        .trim()
        .toLowerCase();
    if (code === 'task_failed') {
        if (!msg) return false;
        return (
            msg.includes('fetch failed') ||
            msg.includes('network') ||
            msg.includes('timeout') ||
            msg.includes('timed out') ||
            msg.includes('econnreset') ||
            msg.includes('enotfound') ||
            msg.includes('eai_again') ||
            msg.includes('socket') ||
            msg.includes('tls')
        );
    }
    return false;
}

export function evaluateResearchTaskClaimability(params: {
    status: ScalpResearchTaskStatus;
    attempts: number;
    maxAttempts: number;
    startedAtMs: number | null;
    runningStaleAfterMs: number;
    nowMs: number;
}): ResearchTaskClaimability {
    const maxAttempts = Math.max(1, Math.floor(Number(params.maxAttempts) || DEFAULT_MAX_ATTEMPTS));
    const attempts = Math.max(0, Math.floor(Number(params.attempts) || 0));
    const startedAtMs = Number(params.startedAtMs);
    const runningMissingStartedAt =
        params.status === 'running' && (!Number.isFinite(startedAtMs) || startedAtMs <= 0 || params.startedAtMs === null);
    const runningStale =
        params.status === 'running' &&
        !runningMissingStartedAt &&
        Number.isFinite(startedAtMs) &&
        Math.max(0, Math.floor(Number(params.nowMs) || 0)) - startedAtMs >= Math.max(1, params.runningStaleAfterMs);
    const maxAttemptsReached = attempts >= maxAttempts;
    const shouldMarkFailedForAttempts =
        (params.status === 'pending' || params.status === 'running') &&
        maxAttemptsReached &&
        (params.status === 'pending' || runningStale || runningMissingStartedAt);
    const claimable =
        !maxAttemptsReached &&
        (params.status === 'pending' || (params.status === 'running' && (runningStale || runningMissingStartedAt)));
    return {
        claimable,
        runningStale,
        runningMissingStartedAt,
        maxAttemptsReached,
        shouldMarkFailedForAttempts,
    };
}

export function isResearchSymbolCooldownActive(
    snapshot: ResearchSymbolCooldownSnapshot,
    symbolRaw: string,
    nowMs = Date.now(),
): boolean {
    const symbol = normalizeSymbol(symbolRaw);
    if (!symbol) return false;
    const row = snapshot.symbols[symbol];
    if (!row) return false;
    return toNonNegativeInt(row.blockedUntilMs, 0) > Math.max(0, Math.floor(nowMs));
}

export function registerResearchSymbolFailure(
    snapshot: ResearchSymbolCooldownSnapshot,
    params: {
        symbol: string;
        errorCode: string | null;
        errorMessage: string | null;
        cycleId?: string | null;
        nowMs?: number;
        config?: ResearchSymbolCooldownConfig;
    },
): { changed: boolean; blockedNow: boolean; blockedUntilMs: number } {
    const symbol = normalizeSymbol(params.symbol);
    if (!symbol) return { changed: false, blockedNow: false, blockedUntilMs: 0 };
    const nowMs = Math.max(0, Math.floor(Number(params.nowMs) || Date.now()));
    const cfg = params.config || resolveResearchSymbolCooldownConfig();
    if (!cfg.enabled) return { changed: false, blockedNow: false, blockedUntilMs: 0 };

    const existing = normalizeResearchSymbolCooldownEntry(snapshot.symbols[symbol]);
    let next = { ...existing };
    const windowStart = toNonNegativeInt(next.windowStartedAtMs, 0);
    if (!windowStart || nowMs - windowStart > cfg.failureWindowMs) {
        next.windowStartedAtMs = nowMs;
        next.failureCount = 0;
    }
    next.failureCount += 1;
    next.lastFailureCode = toOptionalText(params.errorCode, 80);
    next.lastFailureMessage = toOptionalText(params.errorMessage, 220);
    next.updatedAtMs = nowMs;
    next.cycleId = toOptionalText(params.cycleId, 120);

    let blockedNow = false;
    if (next.failureCount >= cfg.failureThreshold) {
        const newBlockedUntilMs = nowMs + cfg.cooldownMs;
        blockedNow = toNonNegativeInt(next.blockedUntilMs, 0) <= nowMs;
        next.blockedUntilMs = Math.max(toNonNegativeInt(next.blockedUntilMs, 0), newBlockedUntilMs);
        next.failureCount = 0;
        next.windowStartedAtMs = nowMs;
    }

    snapshot.symbols[symbol] = next;
    snapshot.updatedAtMs = nowMs;

    const symbols = Object.entries(snapshot.symbols)
        .map(([rowSymbol, row]) => ({
            symbol: rowSymbol,
            updatedAtMs: toNonNegativeInt(row.updatedAtMs, 0),
            blockedUntilMs: toNonNegativeInt(row.blockedUntilMs, 0),
            failureCount: toNonNegativeInt(row.failureCount, 0),
        }))
        .sort((a, b) => {
            const aPriority = a.blockedUntilMs > nowMs || a.failureCount > 0 ? 1 : 0;
            const bPriority = b.blockedUntilMs > nowMs || b.failureCount > 0 ? 1 : 0;
            if (aPriority !== bPriority) return bPriority - aPriority;
            return b.updatedAtMs - a.updatedAtMs;
        });
    for (const row of symbols.slice(cfg.maxTrackedSymbols)) {
        delete snapshot.symbols[row.symbol];
    }

    return { changed: true, blockedNow, blockedUntilMs: toNonNegativeInt(next.blockedUntilMs, 0) };
}

function clearResearchSymbolFailureCounters(snapshot: ResearchSymbolCooldownSnapshot, symbolRaw: string, nowMs = Date.now()): boolean {
    const symbol = normalizeSymbol(symbolRaw);
    if (!symbol) return false;
    const row = snapshot.symbols[symbol];
    if (!row) return false;
    const blockedUntilMs = toNonNegativeInt(row.blockedUntilMs, 0);
    if (blockedUntilMs > nowMs) return false;
    if (toNonNegativeInt(row.failureCount, 0) === 0 && toNonNegativeInt(row.windowStartedAtMs, 0) === 0) return false;
    snapshot.symbols[symbol] = {
        ...row,
        failureCount: 0,
        windowStartedAtMs: 0,
        updatedAtMs: Math.max(0, Math.floor(nowMs)),
    };
    snapshot.updatedAtMs = Math.max(0, Math.floor(nowMs));
    return true;
}

function compactResearchSymbolCooldownSnapshot(snapshot: ResearchSymbolCooldownSnapshot, nowMs = Date.now()): void {
    const now = Math.max(0, Math.floor(nowMs));
    for (const [symbol, row] of Object.entries(snapshot.symbols)) {
        const blockedUntilMs = toNonNegativeInt(row.blockedUntilMs, 0);
        const failureCount = toNonNegativeInt(row.failureCount, 0);
        const updatedAtMs = toNonNegativeInt(row.updatedAtMs, 0);
        if (blockedUntilMs > now) continue;
        if (failureCount > 0) continue;
        if (now - updatedAtMs <= DEFAULT_SYMBOL_COOLDOWN_WINDOW_MS) continue;
        delete snapshot.symbols[symbol];
    }
}

async function loadResearchSymbolCooldownSnapshot(): Promise<ResearchSymbolCooldownSnapshot> {
    const raw = await kvGetJson<unknown>(RESEARCH_SYMBOL_COOLDOWN_KEY);
    return normalizeResearchSymbolCooldownSnapshot(raw);
}

async function saveResearchSymbolCooldownSnapshot(snapshot: ResearchSymbolCooldownSnapshot): Promise<void> {
    const nowMs = Date.now();
    const normalized = normalizeResearchSymbolCooldownSnapshot(snapshot);
    normalized.updatedAtMs = nowMs;
    compactResearchSymbolCooldownSnapshot(normalized, nowMs);
    await kvSetJson(RESEARCH_SYMBOL_COOLDOWN_KEY, normalized);
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

function claimCursorKey(cycleId: string): string {
    return `${RESEARCH_CLAIM_CURSOR_KEY_PREFIX}:${cycleId}`;
}

function lockKey(name: string): string {
    return `${RESEARCH_LOCK_KEY_PREFIX}:${name}`;
}

function sleepMs(ms: number): Promise<void> {
    return new Promise((resolve) => {
        setTimeout(resolve, Math.max(0, Math.floor(ms)));
    });
}

function retryDelayMs(attempt: number): number {
    const exp = Math.min(8, Math.max(0, attempt));
    const base = Math.min(RESEARCH_KV_RETRY_MAX_DELAY_MS, RESEARCH_KV_RETRY_BASE_MS * 2 ** exp);
    const jitter = Math.floor(Math.random() * Math.max(1, Math.floor(base / 4)));
    return Math.min(RESEARCH_KV_RETRY_MAX_DELAY_MS, base + jitter);
}

function isRetryableHttpStatus(status: number): boolean {
    return status === 408 || status === 425 || status === 429 || (status >= 500 && status <= 599);
}

function isRetryableNetworkError(err: unknown): boolean {
    const text = String((err as any)?.message || err || '')
        .trim()
        .toLowerCase();
    const name = String((err as any)?.name || '')
        .trim()
        .toLowerCase();
    if (!text && !name) return false;
    if (name === 'aborterror') return true;
    return (
        text.includes('fetch failed') ||
        text.includes('network') ||
        text.includes('timeout') ||
        text.includes('timed out') ||
        text.includes('econnreset') ||
        text.includes('enotfound') ||
        text.includes('eai_again') ||
        text.includes('socket') ||
        text.includes('tls')
    );
}

async function kvRawCommand(command: string, ...args: Array<string | number>): Promise<unknown> {
    if (!upstash_payasyougo_KV_REST_API_URL || !upstash_payasyougo_KV_REST_API_TOKEN) return null;
    const encodedArgs = args
        .map((arg) => encodeURIComponent(typeof arg === 'string' ? arg : String(arg)))
        .join('/');
    const url = `${upstash_payasyougo_KV_REST_API_URL}/${command}${encodedArgs ? `/${encodedArgs}` : ''}`;
    for (let attempt = 0; attempt <= RESEARCH_KV_MAX_RETRIES; attempt += 1) {
        const controller = new AbortController();
        const timeoutId = setTimeout(() => controller.abort(), RESEARCH_KV_HTTP_TIMEOUT_MS);
        try {
            const res = await fetch(url, {
                method: 'POST',
                headers: {
                    Authorization: `Bearer ${upstash_payasyougo_KV_REST_API_TOKEN}`,
                },
                signal: controller.signal,
            });
            if (!res.ok) {
                if (attempt < RESEARCH_KV_MAX_RETRIES && isRetryableHttpStatus(res.status)) {
                    await sleepMs(retryDelayMs(attempt));
                    continue;
                }
                return null;
            }
            const data = await res.json().catch(() => null);
            if (!data || typeof data !== 'object') return null;
            return (data as any).result ?? null;
        } catch (err) {
            if (attempt < RESEARCH_KV_MAX_RETRIES && isRetryableNetworkError(err)) {
                await sleepMs(retryDelayMs(attempt));
                continue;
            }
            throw err;
        } finally {
            clearTimeout(timeoutId);
        }
    }
    return null;
}

async function scanKeysByPrefix(prefix: string, maxKeys: number): Promise<string[]> {
    if (!upstash_payasyougo_KV_REST_API_URL || !upstash_payasyougo_KV_REST_API_TOKEN) return [];
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
    if (!upstash_payasyougo_KV_REST_API_URL || !upstash_payasyougo_KV_REST_API_TOKEN) return true;
    const key = lockKey(name);
    const ttl = Math.max(10, Math.floor(ttlSeconds));
    const out = await kvRawCommand('SET', key, token, 'NX', 'EX', ttl);
    return String(out || '').toUpperCase() === 'OK';
}

async function releaseLock(name: string, token: string): Promise<void> {
    if (!upstash_payasyougo_KV_REST_API_URL || !upstash_payasyougo_KV_REST_API_TOKEN) return;
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
    tunerEnabled: boolean;
    maxTuneVariantsPerStrategy: number;
}): ScalpResearchTask[] {
    const symbols = dedupe(params.symbols.map((row) => normalizeSymbol(row)).filter((row) => Boolean(row)));
    const fromTs = params.nowMs - params.lookbackDays * 24 * 60 * 60_000;
    const chunkMs = Math.max(1, params.chunkDays) * 24 * 60 * 60_000;
    const knownStrategies = new Set(listScalpStrategies().map((row) => row.id));
    const tasks: ScalpResearchTask[] = [];
    const usedTaskIds = new Set<string>();

    const nextTaskId = (raw: string): string => {
        let candidate = sanitizeTuneId(raw) || 'task';
        if (!usedTaskIds.has(candidate)) {
            usedTaskIds.add(candidate);
            return candidate;
        }
        const hash = crypto
            .createHash('sha1')
            .update(raw)
            .digest('hex')
            .slice(0, 10);
        candidate = sanitizeTuneId(`${candidate.slice(0, 64)}_${hash}`) || `task_${hash}`;
        let attempt = 2;
        while (usedTaskIds.has(candidate)) {
            candidate = sanitizeTuneId(`${candidate.slice(0, 60)}_${attempt}`) || `task_${hash}_${attempt}`;
            attempt += 1;
        }
        usedTaskIds.add(candidate);
        return candidate;
    };

    for (const symbol of symbols) {
        const strategyIds = resolveRecommendedStrategiesForSymbol(symbol, params.strategyAllowlist).filter((id) => knownStrategies.has(id));
        if (!strategyIds.length) continue;
        for (const strategyId of strategyIds) {
            const tuneVariants = params.tunerEnabled
                ? buildScalpResearchTuneVariants({
                      symbol,
                      strategyId,
                      maxVariantsPerStrategy: params.maxTuneVariantsPerStrategy,
                      includeBaseline: true,
                  })
                : [{ tuneId: 'default', configOverride: null }];
            let chunkFrom = fromTs;
            let chunkIdx = 0;
            while (chunkFrom < params.nowMs) {
                const chunkTo = Math.min(params.nowMs, chunkFrom + chunkMs);
                for (const tune of tuneVariants) {
                    const deployment = resolveScalpDeployment({
                        symbol,
                        strategyId,
                        tuneId: tune.tuneId,
                    });
                    const taskId = nextTaskId(`${symbol}_${strategyId}_${deployment.tuneId}_${chunkIdx + 1}`);
                    tasks.push({
                        version: RESEARCH_CYCLE_VERSION,
                        cycleId: params.cycleId,
                        taskId,
                        symbol,
                        strategyId,
                        tuneId: deployment.tuneId,
                        deploymentId: deployment.deploymentId,
                        configOverride: tune.configOverride || null,
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
                    if (tasks.length >= params.maxTasks) {
                        return tasks;
                    }
                }
                chunkFrom = chunkTo;
                chunkIdx += 1;
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

async function saveResearchWorkerHeartbeat(snapshot: ScalpResearchWorkerHeartbeatSnapshot): Promise<void> {
    await kvSetJson(RESEARCH_WORKER_HEARTBEAT_KEY, snapshot);
}

async function loadResearchTasksBatch(cycleId: string, taskIds: string[]): Promise<ScalpResearchTask[]> {
    if (!taskIds.length) return [];
    const keys = taskIds.map((taskId) => taskKey(cycleId, taskId));
    const rows = await kvMGetJson<unknown>(keys);
    const out: ScalpResearchTask[] = [];
    for (const row of rows) {
        if (!isRecord(row)) continue;
        out.push(row as unknown as ScalpResearchTask);
    }
    return out;
}

async function loadResearchTasksBatchWithNulls(cycleId: string, taskIds: string[]): Promise<Array<ScalpResearchTask | null>> {
    if (!taskIds.length) return [];
    const keys = taskIds.map((taskId) => taskKey(cycleId, taskId));
    const rows = await kvMGetJson<unknown>(keys);
    const out: Array<ScalpResearchTask | null> = [];
    for (let i = 0; i < taskIds.length; i += 1) {
        const row = rows[i];
        out.push(isRecord(row) ? (row as unknown as ScalpResearchTask) : null);
    }
    return out;
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

export async function retryResearchTask(params: {
    cycleId?: string;
    taskId: string;
    resetAttempts?: boolean;
}): Promise<{
    cycle: ScalpResearchCycleSnapshot;
    task: ScalpResearchTask;
}> {
    const requestedCycleId = String(params.cycleId || '').trim();
    const taskId = String(params.taskId || '').trim();
    if (!taskId) {
        throw Object.assign(new Error('task_id_required'), { code: 'task_id_required' });
    }

    const activeCycleId = await loadActiveResearchCycleId();
    const cycleId = requestedCycleId || activeCycleId || '';
    if (!cycleId) {
        throw Object.assign(new Error('research_cycle_not_found'), { code: 'research_cycle_not_found' });
    }
    if (activeCycleId && cycleId !== activeCycleId) {
        throw Object.assign(new Error('research_cycle_not_active'), { code: 'research_cycle_not_active' });
    }

    const cycle = await loadResearchCycle(cycleId);
    if (!cycle) {
        throw Object.assign(new Error('research_cycle_not_found'), { code: 'research_cycle_not_found' });
    }
    if (cycle.status !== 'running') {
        throw Object.assign(new Error('research_cycle_not_running'), { code: 'research_cycle_not_running' });
    }
    if (!cycle.taskIds.includes(taskId)) {
        throw Object.assign(new Error('task_not_found'), { code: 'task_not_found' });
    }

    const lockName = `${cycleId}:${taskId}`;
    const lockToken = newToken('retry');
    const gotTaskLock = await tryAcquireLock(lockName, lockToken, DEFAULT_LOCK_TTL_SECONDS);
    if (!gotTaskLock) {
        throw Object.assign(new Error('task_locked'), { code: 'task_locked' });
    }

    try {
        const task = await loadResearchTask(cycleId, taskId);
        if (!task) {
            throw Object.assign(new Error('task_not_found'), { code: 'task_not_found' });
        }
        if (task.status !== 'failed') {
            throw Object.assign(new Error('task_not_failed'), { code: 'task_not_failed' });
        }

        const nowMs = Date.now();
        const nextTask: ScalpResearchTask = {
            ...task,
            status: 'pending',
            attempts: params.resetAttempts === false ? task.attempts : 0,
            updatedAtMs: nowMs,
            workerId: null,
            startedAtMs: null,
            finishedAtMs: null,
            errorCode: null,
            errorMessage: null,
            result: null,
        };
        await saveTask(nextTask);
        return {
            cycle,
            task: nextTask,
        };
    } finally {
        await releaseLock(lockName, lockToken);
    }
}

export async function loadActiveResearchCycleId(): Promise<string | null> {
    const raw = await kvGetJson<{ cycleId?: string }>(RESEARCH_ACTIVE_CYCLE_KEY);
    const cycleId = String(raw?.cycleId || '').trim();
    return cycleId || null;
}

export async function loadResearchWorkerHeartbeat(): Promise<ScalpResearchWorkerHeartbeatSnapshot | null> {
    const raw = await kvGetJson<unknown>(RESEARCH_WORKER_HEARTBEAT_KEY);
    return normalizeResearchWorkerHeartbeat(raw);
}

async function setActiveResearchCycleId(cycleId: string | null): Promise<void> {
    await kvSetJson(RESEARCH_ACTIVE_CYCLE_KEY, {
        cycleId: cycleId || null,
        updatedAtMs: Date.now(),
    });
}

async function loadClaimCursorIndex(cycleId: string): Promise<number> {
    const raw = await kvGetJson<{ nextIndex?: unknown }>(claimCursorKey(cycleId));
    return toNonNegativeInt(raw?.nextIndex, 0);
}

async function saveClaimCursorIndex(cycleId: string, nextIndex: number): Promise<void> {
    await kvSetJson(claimCursorKey(cycleId), {
        cycleId,
        nextIndex: toNonNegativeInt(nextIndex, 0),
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
        const tunerPolicy = resolveScalpResearchTunerPolicy();
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
            tunerEnabled: params.tunerEnabled ?? tunerPolicy.enabled,
            maxTuneVariantsPerStrategy: toPositiveInt(
                params.maxTuneVariantsPerStrategy,
                tunerPolicy.maxVariantsPerStrategy,
            ),
        };

        const tasks = buildResearchCycleTasks({
            cycleId,
            nowMs,
            symbols,
            lookbackDays: cycleParams.lookbackDays,
            chunkDays: cycleParams.chunkDays,
            maxTasks: cycleParams.maxTasks,
            strategyAllowlist: policy.strategyAllowlist,
            tunerEnabled: cycleParams.tunerEnabled !== false,
            maxTuneVariantsPerStrategy: Math.max(1, cycleParams.maxTuneVariantsPerStrategy || 1),
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
            await saveClaimCursorIndex(cycleId, 0);
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
    return loadResearchTasksBatch(cycle.cycleId, cycle.taskIds);
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

type HistoryRow = [number, number, number, number, number, number];

type WorkerDiagnostics = WorkerRunOutcome['diagnostics'];

function lowerBoundByTs(rows: HistoryRow[], targetTs: number): number {
    let lo = 0;
    let hi = rows.length;
    while (lo < hi) {
        const mid = (lo + hi) >>> 1;
        if (Number(rows[mid]?.[0] || 0) < targetTs) lo = mid + 1;
        else hi = mid;
    }
    return lo;
}

function sliceRowsByWindow(rows: HistoryRow[], fromTs: number, toTs: number): HistoryRow[] {
    if (!rows.length || fromTs >= toTs) return [];
    const start = lowerBoundByTs(rows, fromTs);
    const end = lowerBoundByTs(rows, toTs);
    if (start >= end) return [];
    return rows.slice(start, end);
}

async function loadHistoryRowsForTask(
    symbol: string,
    historyCache: Map<string, HistoryRow[]>,
    diagnostics: WorkerDiagnostics,
): Promise<HistoryRow[]> {
    const cached = historyCache.get(symbol);
    if (cached) {
        diagnostics.historyCacheHits += 1;
        return cached;
    }
    diagnostics.historyCacheMisses += 1;
    const history = await loadScalpCandleHistory(symbol, '1m');
    const all = (history.record?.candles || []) as HistoryRow[];
    historyCache.set(symbol, all);
    diagnostics.historySymbolsLoaded += 1;
    diagnostics.historyCandlesLoaded += all.length;
    return all;
}

async function runResearchTask(
    task: ScalpResearchTask,
    minCandlesPerTask: number,
    historyCache: Map<string, HistoryRow[]>,
    diagnostics: WorkerDiagnostics,
): Promise<ScalpResearchTaskResult> {
    const all = await loadHistoryRowsForTask(task.symbol, historyCache, diagnostics);
    const rows = sliceRowsByWindow(all, task.windowFromTs, task.windowToTs);
    diagnostics.windowCandlesProcessed += rows.length;
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
    const runtime = buildScalpReplayRuntimeFromDeployment({
        deployment,
        configOverride: task.configOverride,
        baseRuntime: base,
    });
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

async function runResearchTaskWithTimeout(
    task: ScalpResearchTask,
    minCandlesPerTask: number,
    historyCache: Map<string, HistoryRow[]>,
    diagnostics: WorkerDiagnostics,
    timeoutMs: number,
): Promise<ScalpResearchTaskResult> {
    const safeTimeoutMs = Math.max(1_000, Math.floor(timeoutMs));
    return await new Promise<ScalpResearchTaskResult>((resolve, reject) => {
        let settled = false;
        const timeoutId = setTimeout(() => {
            if (settled) return;
            settled = true;
            reject(
                Object.assign(new Error(`task_timeout:${safeTimeoutMs}`), {
                    code: 'task_timeout',
                    timeoutMs: safeTimeoutMs,
                }),
            );
        }, safeTimeoutMs);

        runResearchTask(task, minCandlesPerTask, historyCache, diagnostics)
            .then((result) => {
                if (settled) return;
                settled = true;
                clearTimeout(timeoutId);
                resolve(result);
            })
            .catch((err) => {
                if (settled) return;
                settled = true;
                clearTimeout(timeoutId);
                reject(err);
            });
    });
}

type ClaimNextTaskOptions = {
    symbolCooldowns: ResearchSymbolCooldownSnapshot;
    symbolCooldownConfig: ResearchSymbolCooldownConfig;
};

async function claimNextTask(
    cycle: ScalpResearchCycleSnapshot,
    workerId: string,
    startIndex = 0,
    opts: ClaimNextTaskOptions,
): Promise<{
    claim: {
        task: ScalpResearchTask;
        lockName: string;
        lockToken: string;
        scanIndex: number;
    } | null;
    scanSummary: ClaimScanSummary;
}> {
    const newScanSummary = (): ClaimScanSummary => ({
        scannedTasks: 0,
        lockAttempts: 0,
        locksAcquired: 0,
        lockMisses: 0,
        missingTask: 0,
        pending: 0,
        runningFresh: 0,
        runningStale: 0,
        runningMissingStartedAt: 0,
        failedRetryable: 0,
        failedMaxed: 0,
        completed: 0,
        symbolCooldownBlocked: 0,
    });
    if (!cycle.taskIds.length) {
        return {
            claim: null,
            scanSummary: newScanSummary(),
        };
    }
    const taskCount = cycle.taskIds.length;
    const firstIndex = Math.max(0, Math.min(taskCount - 1, Math.floor(startIndex) || 0));
    const scanSummary: ClaimScanSummary = newScanSummary();
    let visited = 0;
    while (visited < taskCount) {
        const remaining = taskCount - visited;
        const batchSize = Math.max(1, Math.min(RESEARCH_CLAIM_SCAN_BATCH_SIZE, remaining));
        const batchRows: Array<{ scanIndex: number; taskId: string }> = [];
        for (let i = 0; i < batchSize; i += 1) {
            const scanIndex = (firstIndex + visited + i) % taskCount;
            const taskId = cycle.taskIds[scanIndex];
            batchRows.push({ scanIndex, taskId });
        }
        visited += batchSize;
        const batchTasks = await loadResearchTasksBatchWithNulls(
            cycle.cycleId,
            batchRows.map((row) => row.taskId),
        );

        for (let i = 0; i < batchRows.length; i += 1) {
            const row = batchRows[i];
            const task = batchTasks[i];
            scanSummary.scannedTasks += 1;
            if (!task) {
                scanSummary.missingTask += 1;
                continue;
            }

            const nowMs = Date.now();
            if (task.status === 'pending') scanSummary.pending += 1;
            if (task.status === 'completed') scanSummary.completed += 1;

            const claimability = evaluateResearchTaskClaimability({
                status: task.status,
                attempts: task.attempts,
                maxAttempts: cycle.params.maxAttempts,
                startedAtMs: task.startedAtMs,
                runningStaleAfterMs: cycle.params.runningStaleAfterMs,
                nowMs,
            });
            if (task.status === 'running') {
                if (claimability.runningMissingStartedAt) scanSummary.runningMissingStartedAt += 1;
                else if (claimability.runningStale) scanSummary.runningStale += 1;
                else scanSummary.runningFresh += 1;
            }

            const retryableFailed = task.status === 'failed' && task.attempts < cycle.params.maxAttempts;
            if (task.status === 'failed') {
                if (retryableFailed) scanSummary.failedRetryable += 1;
                else scanSummary.failedMaxed += 1;
            }

            const symbolCooldownActive = opts.symbolCooldownConfig.enabled
                ? isResearchSymbolCooldownActive(opts.symbolCooldowns, task.symbol, nowMs)
                : false;
            const statusCanTransition = task.status === 'pending' || task.status === 'running';
            const shouldTryClaimLock =
                statusCanTransition &&
                (symbolCooldownActive || claimability.shouldMarkFailedForAttempts || claimability.claimable);
            if (!shouldTryClaimLock) continue;

            const lockName = `${cycle.cycleId}:${row.taskId}`;
            const lockToken = newToken(`claim:${workerId}`);
            scanSummary.lockAttempts += 1;
            const gotTaskLock = await tryAcquireLock(lockName, lockToken, DEFAULT_LOCK_TTL_SECONDS);
            if (!gotTaskLock) {
                scanSummary.lockMisses += 1;
                continue;
            }
            scanSummary.locksAcquired += 1;

            const latestTask = await loadResearchTask(cycle.cycleId, row.taskId);
            if (!latestTask) {
                scanSummary.missingTask += 1;
                await releaseLock(lockName, lockToken);
                continue;
            }
            const latestNowMs = Date.now();
            const latestClaimability = evaluateResearchTaskClaimability({
                status: latestTask.status,
                attempts: latestTask.attempts,
                maxAttempts: cycle.params.maxAttempts,
                startedAtMs: latestTask.startedAtMs,
                runningStaleAfterMs: cycle.params.runningStaleAfterMs,
                nowMs: latestNowMs,
            });
            const latestSymbolCooldownActive = opts.symbolCooldownConfig.enabled
                ? isResearchSymbolCooldownActive(opts.symbolCooldowns, latestTask.symbol, latestNowMs)
                : false;
            if (latestSymbolCooldownActive && (latestTask.status === 'pending' || latestTask.status === 'running')) {
                const blockedUntilMs = toNonNegativeInt(
                    opts.symbolCooldowns.symbols[normalizeSymbol(latestTask.symbol)]?.blockedUntilMs,
                    0,
                );
                const blockedTask: ScalpResearchTask = {
                    ...latestTask,
                    status: 'failed',
                    attempts: Math.max(latestTask.attempts, cycle.params.maxAttempts),
                    updatedAtMs: latestNowMs,
                    finishedAtMs: latestNowMs,
                    errorCode: 'symbol_cooldown_active',
                    errorMessage: `symbol_cooldown_active_until:${new Date(blockedUntilMs).toISOString()}`.slice(0, 300),
                };
                await saveTask(blockedTask);
                scanSummary.symbolCooldownBlocked += 1;
                scanSummary.failedMaxed += 1;
                await releaseLock(lockName, lockToken);
                continue;
            }

            if (latestClaimability.shouldMarkFailedForAttempts) {
                const maxedTask: ScalpResearchTask = {
                    ...latestTask,
                    status: 'failed',
                    attempts: Math.max(latestTask.attempts, cycle.params.maxAttempts),
                    updatedAtMs: latestNowMs,
                    finishedAtMs: latestNowMs,
                    errorCode: 'max_attempts_exhausted',
                    errorMessage: `max_attempts_exhausted:${latestTask.status}:${latestTask.attempts}/${cycle.params.maxAttempts}`.slice(
                        0,
                        300,
                    ),
                };
                await saveTask(maxedTask);
                scanSummary.failedMaxed += 1;
                await releaseLock(lockName, lockToken);
                continue;
            }
            if (!latestClaimability.claimable) {
                await releaseLock(lockName, lockToken);
                continue;
            }

            const nextTask: ScalpResearchTask = {
                ...latestTask,
                status: 'running',
                attempts: latestTask.attempts + 1,
                workerId,
                startedAtMs: latestNowMs,
                updatedAtMs: latestNowMs,
                finishedAtMs: null,
                errorCode: null,
                errorMessage: null,
            };
            await saveTask(nextTask);
            return {
                claim: {
                    task: nextTask,
                    lockName,
                    lockToken,
                    scanIndex: row.scanIndex,
                },
                scanSummary,
            };
        }
    }
    return { claim: null, scanSummary };
}

export async function runResearchWorker(params: WorkerRunParams = {}): Promise<WorkerRunOutcome> {
    const startedAtMs = Date.now();
    const workerId = String(params.workerId || '').trim() || `worker_${crypto.randomUUID().slice(0, 8)}`;
    const debug = params.debug === true;
    const workerRuntime = resolveResearchWorkerRuntimeConfig({
        maxRuns: params.maxRuns,
        concurrency: params.concurrency,
        maxDurationMs: params.maxDurationMs,
    });
    const maxRuns = workerRuntime.maxRuns;
    const concurrency = workerRuntime.concurrency;
    const maxDurationMs = workerRuntime.maxDurationMs;
    const buildDiagnostics = (cycleTaskCount = 0): WorkerRunOutcome['diagnostics'] => ({
        durationMs: Date.now() - startedAtMs,
        cycleTaskCount,
        scanTasksVisited: 0,
        taskLockAttempts: 0,
        taskLockAcquired: 0,
        historyCacheHits: 0,
        historyCacheMisses: 0,
        historySymbolsLoaded: 0,
        historyCandlesLoaded: 0,
        windowCandlesProcessed: 0,
    });
    const buildEarlyOutcome = (cycleId: string | null, cycleTaskCount = 0): WorkerRunOutcome => ({
        cycleId,
        workerId,
        maxRuns,
        concurrency,
        maxDurationMs,
        attemptedRuns: 0,
        completedRuns: 0,
        failedRuns: 0,
        stoppedByDurationBudget: false,
        noClaimScanSummary: null,
        diagnostics: buildDiagnostics(cycleTaskCount),
        claimedTasks: [],
    });
    const emitWorkerLog = (
        event: string,
        payload: Record<string, unknown>,
        level: 'info' | 'warn' = 'info',
        force = false,
    ): void => {
        if (!force && !debug) return;
        const line = JSON.stringify({
            scope: 'scalp_research_worker',
            event,
            ...payload,
        });
        if (level === 'warn') console.warn(line);
        else console.info(line);
    };
    const persistHeartbeat = async (params: {
        status: ResearchWorkerHeartbeatStatus;
        cycleId: string | null;
        attemptedRuns?: number;
        completedRuns?: number;
        failedRuns?: number;
        stoppedByDurationBudget?: boolean;
        noClaimScanSummary?: WorkerNoClaimScanSummary | null;
        finishedAtMs?: number | null;
        durationMs?: number | null;
        error?: string | null;
    }): Promise<void> => {
        const nowMs = Date.now();
        try {
            await saveResearchWorkerHeartbeat({
                version: 1,
                updatedAtMs: nowMs,
                status: params.status,
                cycleId: params.cycleId,
                workerId,
                maxRuns,
                concurrency,
                maxDurationMs,
                startedAtMs,
                finishedAtMs:
                    params.finishedAtMs === undefined
                        ? params.status === 'started'
                            ? null
                            : nowMs
                        : params.finishedAtMs,
                durationMs: params.durationMs === undefined ? (params.status === 'started' ? null : nowMs - startedAtMs) : params.durationMs,
                attemptedRuns: toNonNegativeInt(params.attemptedRuns, 0),
                completedRuns: toNonNegativeInt(params.completedRuns, 0),
                failedRuns: toNonNegativeInt(params.failedRuns, 0),
                stoppedByDurationBudget: params.stoppedByDurationBudget === true,
                noClaimScanSummary: params.noClaimScanSummary || null,
                error: toOptionalText(params.error, 300),
            });
        } catch (err: any) {
            emitWorkerLog(
                'worker_heartbeat_save_failed',
                {
                    workerId,
                    cycleId: params.cycleId,
                    status: params.status,
                    error: String(err?.message || err),
                },
                'warn',
                true,
            );
        }
    };

    let heartbeatCycleId: string | null = String(params.cycleId || '').trim() || null;
    try {
        await persistHeartbeat({
            status: 'started',
            cycleId: heartbeatCycleId,
            finishedAtMs: null,
            durationMs: null,
        });

        const cycleId = params.cycleId || (await loadActiveResearchCycleId());
        heartbeatCycleId = cycleId || null;
        if (!cycleId) {
            emitWorkerLog(
                'early_exit_no_cycle',
                {
                    workerId,
                },
                'info',
            );
            const out = buildEarlyOutcome(null, 0);
            await persistHeartbeat({
                status: 'no_cycle',
                cycleId: null,
                attemptedRuns: out.attemptedRuns,
                completedRuns: out.completedRuns,
                failedRuns: out.failedRuns,
                stoppedByDurationBudget: out.stoppedByDurationBudget,
                noClaimScanSummary: out.noClaimScanSummary,
                finishedAtMs: Date.now(),
                durationMs: out.diagnostics.durationMs,
            });
            return out;
        }

        const cycle = await loadResearchCycle(cycleId);
        if (!cycle) {
            emitWorkerLog(
                'early_exit_cycle_not_found',
                {
                    cycleId,
                    workerId,
                },
                'warn',
            );
            const out = buildEarlyOutcome(cycleId, 0);
            await persistHeartbeat({
                status: 'cycle_not_found',
                cycleId,
                attemptedRuns: out.attemptedRuns,
                completedRuns: out.completedRuns,
                failedRuns: out.failedRuns,
                stoppedByDurationBudget: out.stoppedByDurationBudget,
                noClaimScanSummary: out.noClaimScanSummary,
                finishedAtMs: Date.now(),
                durationMs: out.diagnostics.durationMs,
            });
            return out;
        }
        if (cycle.status !== 'running') {
            emitWorkerLog(
                'early_exit_cycle_not_running',
                {
                    cycleId,
                    workerId,
                    cycleStatus: cycle.status,
                    cycleTaskCount: cycle.taskIds.length,
                },
                'warn',
            );
            const out = buildEarlyOutcome(cycleId, cycle.taskIds.length);
            await persistHeartbeat({
                status: 'cycle_not_running',
                cycleId,
                attemptedRuns: out.attemptedRuns,
                completedRuns: out.completedRuns,
                failedRuns: out.failedRuns,
                stoppedByDurationBudget: out.stoppedByDurationBudget,
                noClaimScanSummary: out.noClaimScanSummary,
                finishedAtMs: Date.now(),
                durationMs: out.diagnostics.durationMs,
            });
            return out;
        }
        const diagnostics: WorkerDiagnostics = {
            durationMs: 0,
            cycleTaskCount: cycle.taskIds.length,
            scanTasksVisited: 0,
            taskLockAttempts: 0,
            taskLockAcquired: 0,
            historyCacheHits: 0,
            historyCacheMisses: 0,
            historySymbolsLoaded: 0,
            historyCandlesLoaded: 0,
            windowCandlesProcessed: 0,
        };
        const out: WorkerRunOutcome = {
            cycleId,
            workerId,
            maxRuns,
            concurrency,
            maxDurationMs,
            attemptedRuns: 0,
            completedRuns: 0,
            failedRuns: 0,
            stoppedByDurationBudget: false,
            noClaimScanSummary: null,
            diagnostics,
            claimedTasks: [],
        };
        const symbolCooldownConfig = resolveResearchSymbolCooldownConfig();
        const symbolCooldowns = await loadResearchSymbolCooldownSnapshot();
        let symbolCooldownsDirty = false;
        const historyCache = new Map<string, HistoryRow[]>();
        let nextScanIndex = await loadClaimCursorIndex(cycleId);
        let lastNoClaimScanSummary: ClaimScanSummary | null = null;
        let stopAfterCurrentBatch = false;
        let claimIterations = 0;
        const shouldWarnNoClaim = (scanSummary: ClaimScanSummary): boolean =>
            scanSummary.pending > 0 ||
            scanSummary.runningStale > 0 ||
            scanSummary.runningMissingStartedAt > 0 ||
            scanSummary.failedRetryable > 0 ||
            scanSummary.lockMisses > 0;
        type ClaimedTaskRunOutcome = {
            ok: boolean;
            symbolCooldownsDirty: boolean;
            row: WorkerRunOutcome['claimedTasks'][number];
        };
        const runClaimedTask = async (params: {
            claim: {
                task: ScalpResearchTask;
                lockName: string;
                lockToken: string;
                scanIndex: number;
            };
            taskStartedAtMs: number;
        }): Promise<ClaimedTaskRunOutcome> => {
            const { claim, taskStartedAtMs } = params;
            try {
                const result = await runResearchTaskWithTimeout(
                    claim.task,
                    cycle.params.minCandlesPerTask,
                    historyCache,
                    diagnostics,
                    RESEARCH_TASK_TIMEOUT_MS,
                );
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
                let cooldownChanged = false;
                if (symbolCooldownConfig.enabled) {
                    const cleared = clearResearchSymbolFailureCounters(symbolCooldowns, completedTask.symbol, Date.now());
                    if (cleared) cooldownChanged = true;
                }
                return {
                    ok: true,
                    symbolCooldownsDirty: cooldownChanged,
                    row: {
                        taskId: completedTask.taskId,
                        symbol: completedTask.symbol,
                        strategyId: completedTask.strategyId,
                        tuneId: completedTask.tuneId,
                        status: completedTask.status,
                        errorCode: null,
                        errorMessage: null,
                        trades: result.trades,
                        netR: result.netR,
                        durationMs: Date.now() - taskStartedAtMs,
                        scanIndex: claim.scanIndex,
                    },
                };
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
                let cooldownChanged = false;
                if (
                    symbolCooldownConfig.enabled &&
                    isResearchTaskFailureEligibleForSymbolCooldown(failedTask.errorCode, failedTask.errorMessage)
                ) {
                    const failureUpdate = registerResearchSymbolFailure(symbolCooldowns, {
                        symbol: failedTask.symbol,
                        errorCode: failedTask.errorCode,
                        errorMessage: failedTask.errorMessage,
                        cycleId,
                        nowMs: Date.now(),
                        config: symbolCooldownConfig,
                    });
                    if (failureUpdate.changed) cooldownChanged = true;
                    if (failureUpdate.blockedNow) {
                        emitWorkerLog(
                            'symbol_cooldown_activated',
                            {
                                cycleId,
                                workerId,
                                symbol: failedTask.symbol,
                                blockedUntilMs: failureUpdate.blockedUntilMs,
                                blockedUntilIso: new Date(failureUpdate.blockedUntilMs).toISOString(),
                                errorCode: failedTask.errorCode,
                                errorMessage: failedTask.errorMessage,
                                failureThreshold: symbolCooldownConfig.failureThreshold,
                                failureWindowMs: symbolCooldownConfig.failureWindowMs,
                                cooldownMs: symbolCooldownConfig.cooldownMs,
                            },
                            'warn',
                            true,
                        );
                    }
                }
                emitWorkerLog(
                    'task_failed',
                    {
                        cycleId,
                        workerId,
                        taskId: failedTask.taskId,
                        symbol: failedTask.symbol,
                        strategyId: failedTask.strategyId,
                        tuneId: failedTask.tuneId,
                        attempts: failedTask.attempts,
                        maxAttemptsCfg: cycle.params.maxAttempts,
                        errorCode: failedTask.errorCode,
                        errorMessage: failedTask.errorMessage,
                        scanIndex: claim.scanIndex,
                    },
                    'warn',
                    true,
                );
                return {
                    ok: false,
                    symbolCooldownsDirty: cooldownChanged,
                    row: {
                        taskId: failedTask.taskId,
                        symbol: failedTask.symbol,
                        strategyId: failedTask.strategyId,
                        tuneId: failedTask.tuneId,
                        status: failedTask.status,
                        errorCode: failedTask.errorCode,
                        errorMessage: failedTask.errorMessage,
                        trades: failedTask.result?.trades || null,
                        netR: failedTask.result?.netR || null,
                        durationMs: Date.now() - taskStartedAtMs,
                        scanIndex: claim.scanIndex,
                    },
                };
            } finally {
                await releaseLock(claim.lockName, claim.lockToken);
            }
        };

        while (out.attemptedRuns < maxRuns) {
            if (Date.now() - startedAtMs >= maxDurationMs) {
                out.stoppedByDurationBudget = true;
                break;
            }
            const remainingRuns = maxRuns - out.attemptedRuns;
            const batchSize = Math.max(1, Math.min(concurrency, remainingRuns));
            const batchClaims: Array<{
                claim: {
                    task: ScalpResearchTask;
                    lockName: string;
                    lockToken: string;
                    scanIndex: number;
                };
                taskStartedAtMs: number;
            }> = [];

            for (let i = 0; i < batchSize; i += 1) {
                if (Date.now() - startedAtMs >= maxDurationMs) {
                    out.stoppedByDurationBudget = true;
                    stopAfterCurrentBatch = true;
                    break;
                }
                claimIterations += 1;
                const claimOutcome = await claimNextTask(cycle, workerId, nextScanIndex, {
                    symbolCooldowns,
                    symbolCooldownConfig,
                });
                diagnostics.scanTasksVisited += claimOutcome.scanSummary.scannedTasks;
                diagnostics.taskLockAttempts += claimOutcome.scanSummary.lockAttempts;
                diagnostics.taskLockAcquired += claimOutcome.scanSummary.locksAcquired;
                if (!claimOutcome.claim) {
                    lastNoClaimScanSummary = claimOutcome.scanSummary;
                    out.noClaimScanSummary = claimOutcome.scanSummary;
                    const warnNoClaim = shouldWarnNoClaim(claimOutcome.scanSummary);
                    emitWorkerLog(
                        'no_claimable_tasks',
                        {
                            cycleId,
                            workerId,
                            iteration: claimIterations,
                            maxRuns,
                            concurrency,
                            maxDurationMs,
                            nextScanIndex,
                            cycleTaskCount: cycle.taskIds.length,
                            runningStaleAfterMs: cycle.params.runningStaleAfterMs,
                            maxAttemptsCfg: cycle.params.maxAttempts,
                            plannedBatchSize: batchSize,
                            claimedInBatch: batchClaims.length,
                            scanSummary: claimOutcome.scanSummary,
                        },
                        warnNoClaim ? 'warn' : 'info',
                        warnNoClaim,
                    );
                    stopAfterCurrentBatch = true;
                    break;
                }
                const claim = claimOutcome.claim;
                nextScanIndex = cycle.taskIds.length > 0 ? (claim.scanIndex + 1) % cycle.taskIds.length : 0;
                out.attemptedRuns += 1;
                batchClaims.push({
                    claim,
                    taskStartedAtMs: Date.now(),
                });
            }

            if (batchClaims.length > 0) {
                const batchResults = await Promise.all(batchClaims.map((row) => runClaimedTask(row)));
                for (const result of batchResults) {
                    if (result.ok) out.completedRuns += 1;
                    else out.failedRuns += 1;
                    if (result.symbolCooldownsDirty) symbolCooldownsDirty = true;
                    out.claimedTasks.push(result.row);
                }
            }

            if (Date.now() - startedAtMs >= maxDurationMs) {
                out.stoppedByDurationBudget = true;
            }
            if (stopAfterCurrentBatch || batchClaims.length === 0 || out.stoppedByDurationBudget) {
                break;
            }
        }

        if (out.attemptedRuns > 0) {
            await saveClaimCursorIndex(cycleId, nextScanIndex);
        }
        if (symbolCooldownsDirty) {
            try {
                await saveResearchSymbolCooldownSnapshot(symbolCooldowns);
            } catch (err: any) {
                emitWorkerLog(
                    'symbol_cooldown_save_failed',
                    {
                        cycleId,
                        workerId,
                        error: String(err?.message || err),
                    },
                    'warn',
                    true,
                );
            }
        }
        out.diagnostics.durationMs = Date.now() - startedAtMs;
        const shouldWarnCompletion =
            out.failedRuns > 0 ||
            (out.attemptedRuns === 0 &&
                lastNoClaimScanSummary !== null &&
                (lastNoClaimScanSummary.pending > 0 ||
                    lastNoClaimScanSummary.runningFresh > 0 ||
                    lastNoClaimScanSummary.runningStale > 0 ||
                    lastNoClaimScanSummary.runningMissingStartedAt > 0 ||
                    lastNoClaimScanSummary.failedRetryable > 0 ||
                    lastNoClaimScanSummary.lockMisses > 0));
        emitWorkerLog(
            'run_completed',
            {
                cycleId,
                workerId,
                maxRuns,
                concurrency,
                maxDurationMs,
                stoppedByDurationBudget: out.stoppedByDurationBudget,
                attemptedRuns: out.attemptedRuns,
                completedRuns: out.completedRuns,
                failedRuns: out.failedRuns,
                nextScanIndex,
                claimIterations,
                diagnostics: out.diagnostics,
                lastNoClaimScanSummary,
                claimedTaskIds: out.claimedTasks.slice(0, 10).map((row) => row.taskId),
            },
            shouldWarnCompletion ? 'warn' : 'info',
            shouldWarnCompletion,
        );
        await persistHeartbeat({
            status: 'completed',
            cycleId,
            attemptedRuns: out.attemptedRuns,
            completedRuns: out.completedRuns,
            failedRuns: out.failedRuns,
            stoppedByDurationBudget: out.stoppedByDurationBudget,
            noClaimScanSummary: out.noClaimScanSummary,
            finishedAtMs: Date.now(),
            durationMs: out.diagnostics.durationMs,
            error: null,
        });
        return out;
    } catch (err: any) {
        await persistHeartbeat({
            status: 'failed',
            cycleId: heartbeatCycleId,
            finishedAtMs: Date.now(),
            durationMs: Date.now() - startedAtMs,
            error: String(err?.message || err || 'worker_failed').slice(0, 300),
        });
        throw err;
    }
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

        const key = `${task.symbol}::${task.strategyId}::${task.tuneId}`;
        if (!byCandidate.has(key)) {
            byCandidate.set(key, {
                symbol: task.symbol,
                strategyId: task.strategyId,
                tuneId: task.tuneId,
                deploymentId: task.deploymentId,
                configOverride: task.configOverride || null,
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
    return loadResearchTasksBatch(cycle.cycleId, cycle.taskIds.slice(0, max));
}
