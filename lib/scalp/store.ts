import { kvGetJson, kvListPushJson, kvListRangeJson, kvSetJson } from '../kv';
import { DEFAULT_SCALP_TUNE_ID, normalizeScalpTuneId, resolveScalpDeployment } from './deployments';
import {
    getDefaultScalpStrategy,
    getScalpStrategyById,
    listScalpStrategies,
    normalizeScalpStrategyId,
} from './strategies/registry';
import type { ScalpJournalEntry, ScalpSessionState, ScalpTradeLedgerEntry } from './types';

const SCALP_STATE_KEY_PREFIX = 'scalp:state:v2';
const SCALP_STATE_KEY_PREFIX_V1 = 'scalp:state:v1';
const SCALP_RUN_LOCK_KEY_PREFIX = 'scalp:runlock:v2';
const SCALP_JOURNAL_LIST_KEY = 'scalp:journal:list:v1';
const SCALP_TRADE_LEDGER_LIST_KEY = 'scalp:trade-ledger:list:v1';
const SCALP_RUNTIME_SETTINGS_KEY = 'scalp:runtime:settings:v1';
const SCALP_DEFAULT_STATE_TTL_SECONDS = 3 * 24 * 60 * 60;
const SCALP_DEFAULT_JOURNAL_MAX = 500;
const SCALP_DEFAULT_TRADE_LEDGER_MAX = 10_000;

const defaultScalpStrategy = getDefaultScalpStrategy();
export const SCALP_STRATEGY_SHORT_NAME = defaultScalpStrategy.shortName;
export const SCALP_STRATEGY_LONG_NAME = defaultScalpStrategy.longName;

interface ScalpRuntimeStrategySettings {
    enabled: boolean | null;
    updatedAtMs: number | null;
    updatedBy: string | null;
}

interface ScalpRuntimeSettings {
    defaultStrategyId: string;
    strategies: Record<string, ScalpRuntimeStrategySettings>;
}

export interface ScalpStrategyControlSnapshot {
    strategyId: string;
    shortName: string;
    longName: string;
    enabled: boolean;
    envEnabled: boolean;
    kvEnabled: boolean | null;
    updatedAtMs: number | null;
    updatedBy: string | null;
}

export interface ScalpStrategyRuntimeSnapshot {
    defaultStrategyId: string;
    strategyId: string;
    strategy: ScalpStrategyControlSnapshot;
    strategies: ScalpStrategyControlSnapshot[];
}

const upstash_payasyougo_KV_REST_API_URL = (process.env.upstash_payasyougo_KV_REST_API_URL || '').replace(/\/$/, '');
const upstash_payasyougo_KV_REST_API_TOKEN = process.env.upstash_payasyougo_KV_REST_API_TOKEN || '';

function resolveStrategyId(value: unknown, fallback = defaultScalpStrategy.id): string {
    const normalized = normalizeScalpStrategyId(value);
    if (!normalized) return fallback;
    const strategy = getScalpStrategyById(normalized);
    return strategy?.id || fallback;
}

type ScalpDeploymentKeyOptions = {
    tuneId?: string;
    deploymentId?: string;
};

function stateKey(deploymentId: string, dayKey: string): string {
    return `${SCALP_STATE_KEY_PREFIX}:${deploymentId}:${dayKey}`;
}

function legacyStrategyStateKey(strategyId: string, symbol: string, dayKey: string): string {
    return `${SCALP_STATE_KEY_PREFIX_V1}:${strategyId}:${String(symbol || '').toUpperCase()}:${dayKey}`;
}

function legacyStateKey(symbol: string, dayKey: string): string {
    return `${SCALP_STATE_KEY_PREFIX_V1}:${String(symbol || '').toUpperCase()}:${dayKey}`;
}

function runLockKey(deploymentId: string): string {
    return `${SCALP_RUN_LOCK_KEY_PREFIX}:${deploymentId}`;
}

function resolveDeploymentKey(params: {
    symbol: string;
    strategyId?: string;
    tuneId?: string;
    deploymentId?: string;
}) {
    return resolveScalpDeployment({
        symbol: params.symbol,
        strategyId: params.strategyId,
        tuneId: params.tuneId,
        deploymentId: params.deploymentId,
        fallbackStrategyId: defaultScalpStrategy.id,
        fallbackTuneId: DEFAULT_SCALP_TUNE_ID,
    });
}

function hydrateSessionStateDeployment(state: ScalpSessionState, params: {
    symbol: string;
    strategyId?: string;
    tuneId?: string;
    deploymentId?: string;
}): ScalpSessionState {
    const deployment = resolveDeploymentKey({
        symbol: state.symbol || params.symbol,
        strategyId: state.strategyId || params.strategyId,
        tuneId: state.tuneId || params.tuneId,
        deploymentId: state.deploymentId || params.deploymentId,
    });
    return {
        ...state,
        version: 2,
        symbol: deployment.symbol,
        strategyId: deployment.strategyId,
        tuneId: deployment.tuneId,
        deploymentId: deployment.deploymentId,
    };
}

function safeRecord(value: unknown): Record<string, unknown> {
    if (!value || typeof value !== 'object' || Array.isArray(value)) return {};
    return value as Record<string, unknown>;
}

function compactReasonCodes(value: unknown): string[] {
    if (!Array.isArray(value)) return [];
    const out: string[] = [];
    for (const item of value) {
        const code = String(item || '')
            .trim()
            .toUpperCase();
        if (!code) continue;
        out.push(code.slice(0, 80));
        if (out.length >= 16) break;
    }
    return out;
}

function parseBool(value: unknown): boolean | null {
    if (typeof value === 'boolean') return value;
    if (typeof value === 'number') {
        if (value === 1) return true;
        if (value === 0) return false;
        return null;
    }
    const normalized = String(value || '')
        .trim()
        .toLowerCase();
    if (['1', 'true', 'yes', 'on'].includes(normalized)) return true;
    if (['0', 'false', 'no', 'off'].includes(normalized)) return false;
    return null;
}

function parseOptionalTime(value: unknown): number | null {
    const n = Number(value);
    if (!Number.isFinite(n) || n <= 0) return null;
    return Math.floor(n);
}

function parseUpdatedBy(value: unknown): string | null {
    const normalized = String(value || '').trim();
    if (!normalized) return null;
    return normalized.slice(0, 120);
}

function parseScalpRuntimeSettings(raw: unknown): ScalpRuntimeSettings {
    const row = safeRecord(raw);
    const strategiesRaw = safeRecord(row.strategies);
    const defaultStrategyId = resolveStrategyId(row.defaultStrategyId, defaultScalpStrategy.id);
    const legacyStrategyEnabled = parseBool(row.strategyEnabled);
    const legacyUpdatedAtMs = parseOptionalTime(row.updatedAtMs);
    const legacyUpdatedBy = parseUpdatedBy(row.updatedBy);
    const strategies: Record<string, ScalpRuntimeStrategySettings> = {};

    for (const strategy of listScalpStrategies()) {
        const rawSettings = safeRecord(strategiesRaw[strategy.id]);
        let enabled = parseBool(rawSettings.enabled);
        let updatedAtMs = parseOptionalTime(rawSettings.updatedAtMs);
        let updatedBy = parseUpdatedBy(rawSettings.updatedBy);

        if (enabled === null && strategy.id === defaultStrategyId && legacyStrategyEnabled !== null) {
            enabled = legacyStrategyEnabled;
            if (!updatedAtMs) updatedAtMs = legacyUpdatedAtMs;
            if (!updatedBy) updatedBy = legacyUpdatedBy;
        }

        strategies[strategy.id] = {
            enabled,
            updatedAtMs,
            updatedBy,
        };
    }

    return {
        defaultStrategyId,
        strategies,
    };
}

function toControlSnapshot(
    settings: ScalpRuntimeSettings,
    strategyId: string,
    envEnabled: boolean,
): ScalpStrategyControlSnapshot {
    const strategy = getScalpStrategyById(strategyId);
    if (!strategy) {
        throw new Error(`Unknown scalp strategy: ${strategyId}`);
    }
    const runtime = settings.strategies[strategy.id] || { enabled: null, updatedAtMs: null, updatedBy: null };
    const kvEnabled = runtime.enabled;
    return {
        strategyId: strategy.id,
        shortName: strategy.shortName,
        longName: strategy.longName,
        enabled: Boolean(envEnabled) && (kvEnabled ?? true),
        envEnabled: Boolean(envEnabled),
        kvEnabled,
        updatedAtMs: runtime.updatedAtMs,
        updatedBy: runtime.updatedBy,
    };
}

function serializeScalpRuntimeSettings(settings: ScalpRuntimeSettings): Record<string, unknown> {
    const strategies: Record<string, unknown> = {};
    for (const strategy of listScalpStrategies()) {
        const row = settings.strategies[strategy.id];
        if (!row) continue;
        const entry: Record<string, unknown> = {};
        if (typeof row.enabled === 'boolean') entry.enabled = row.enabled;
        if (Number.isFinite(Number(row.updatedAtMs)) && Number(row.updatedAtMs) > 0) entry.updatedAtMs = Number(row.updatedAtMs);
        if (row.updatedBy) entry.updatedBy = row.updatedBy;
        if (Object.keys(entry).length > 0) {
            strategies[strategy.id] = entry;
        }
    }
    return {
        defaultStrategyId: resolveStrategyId(settings.defaultStrategyId, defaultScalpStrategy.id),
        strategies,
    };
}

export async function loadScalpStrategyRuntimeSnapshot(
    envEnabled: boolean,
    preferredStrategyId?: string,
): Promise<ScalpStrategyRuntimeSnapshot> {
    const settings = parseScalpRuntimeSettings(await kvGetJson<ScalpRuntimeSettings>(SCALP_RUNTIME_SETTINGS_KEY));
    const defaultStrategyId = resolveStrategyId(settings.defaultStrategyId, defaultScalpStrategy.id);
    const selectedStrategyId = resolveStrategyId(preferredStrategyId, defaultStrategyId);
    const strategies = listScalpStrategies().map((strategy) => toControlSnapshot(settings, strategy.id, envEnabled));
    const strategy = strategies.find((row) => row.strategyId === selectedStrategyId) || toControlSnapshot(settings, defaultStrategyId, envEnabled);

    return {
        defaultStrategyId,
        strategyId: strategy.strategyId,
        strategy,
        strategies,
    };
}

export async function loadScalpStrategyControlSnapshot(
    envEnabled: boolean,
    preferredStrategyId?: string,
): Promise<ScalpStrategyControlSnapshot> {
    const runtime = await loadScalpStrategyRuntimeSnapshot(envEnabled, preferredStrategyId);
    return runtime.strategy;
}

export async function setScalpStrategyKvEnabled(params: {
    strategyId?: string;
    enabled: boolean;
    envEnabled: boolean;
    updatedBy?: string | null;
}): Promise<ScalpStrategyRuntimeSnapshot> {
    const current = parseScalpRuntimeSettings(await kvGetJson<ScalpRuntimeSettings>(SCALP_RUNTIME_SETTINGS_KEY));
    const strategyId = resolveStrategyId(params.strategyId, current.defaultStrategyId);
    const nextSettings: ScalpRuntimeSettings = {
        defaultStrategyId: current.defaultStrategyId,
        strategies: { ...current.strategies },
    };
    nextSettings.strategies[strategyId] = {
        enabled: Boolean(params.enabled),
        updatedAtMs: Date.now(),
        updatedBy: parseUpdatedBy(params.updatedBy),
    };
    await kvSetJson(SCALP_RUNTIME_SETTINGS_KEY, serializeScalpRuntimeSettings(nextSettings));
    return loadScalpStrategyRuntimeSnapshot(params.envEnabled, strategyId);
}

export async function setScalpDefaultStrategy(params: {
    strategyId: string;
    envEnabled: boolean;
}): Promise<ScalpStrategyRuntimeSnapshot> {
    const current = parseScalpRuntimeSettings(await kvGetJson<ScalpRuntimeSettings>(SCALP_RUNTIME_SETTINGS_KEY));
    const strategyId = resolveStrategyId(params.strategyId, current.defaultStrategyId);
    const nextSettings: ScalpRuntimeSettings = {
        defaultStrategyId: strategyId,
        strategies: { ...current.strategies },
    };
    await kvSetJson(SCALP_RUNTIME_SETTINGS_KEY, serializeScalpRuntimeSettings(nextSettings));
    return loadScalpStrategyRuntimeSnapshot(params.envEnabled, strategyId);
}

function parseSessionState(raw: unknown): ScalpSessionState | null {
    const row = safeRecord(raw);
    const symbol = String(row.symbol || '')
        .trim()
        .toUpperCase();
    const dayKey = String(row.dayKey || '').trim();
    const state = String(row.state || '').trim().toUpperCase();
    if (!symbol || !dayKey || !state) return null;

    const allowedStates = new Set([
        'IDLE',
        'ASIA_RANGE_READY',
        'SWEEP_DETECTED',
        'CONFIRMING',
        'WAITING_RETRACE',
        'IN_TRADE',
        'DONE',
        'COOLDOWN',
    ]);
    if (!allowedStates.has(state)) return null;

    const createdAtMs = Number(row.createdAtMs);
    const updatedAtMs = Number(row.updatedAtMs);

    const run = safeRecord(row.run);
    const stats = safeRecord(row.stats);
    const lastProcessed = safeRecord(row.lastProcessed);

    return {
        version: Number(row.version) === 2 ? 2 : 1,
        symbol,
        strategyId: normalizeScalpStrategyId(row.strategyId) || '',
        tuneId: normalizeScalpTuneId(row.tuneId, ''),
        deploymentId: String(row.deploymentId || '').trim(),
        dayKey,
        state: state as ScalpSessionState['state'],
        createdAtMs: Number.isFinite(createdAtMs) && createdAtMs > 0 ? createdAtMs : Date.now(),
        updatedAtMs: Number.isFinite(updatedAtMs) && updatedAtMs > 0 ? updatedAtMs : Date.now(),
        cooldownUntilMs: Number.isFinite(Number(row.cooldownUntilMs)) ? Number(row.cooldownUntilMs) : null,
        killSwitchActive: Boolean(row.killSwitchActive),
        asiaRange: row.asiaRange ? (row.asiaRange as ScalpSessionState['asiaRange']) : null,
        sweep: row.sweep ? (row.sweep as ScalpSessionState['sweep']) : null,
        confirmation: row.confirmation ? (row.confirmation as ScalpSessionState['confirmation']) : null,
        ifvg: row.ifvg ? (row.ifvg as ScalpSessionState['ifvg']) : null,
        trade: row.trade ? (row.trade as ScalpSessionState['trade']) : null,
        lastProcessed: {
            m1ClosedTsMs: Number.isFinite(Number(lastProcessed.m1ClosedTsMs)) ? Number(lastProcessed.m1ClosedTsMs) : null,
            m3ClosedTsMs: Number.isFinite(Number(lastProcessed.m3ClosedTsMs)) ? Number(lastProcessed.m3ClosedTsMs) : null,
            m5ClosedTsMs: Number.isFinite(Number(lastProcessed.m5ClosedTsMs)) ? Number(lastProcessed.m5ClosedTsMs) : null,
            m15ClosedTsMs: Number.isFinite(Number(lastProcessed.m15ClosedTsMs)) ? Number(lastProcessed.m15ClosedTsMs) : null,
        },
        stats: {
            tradesPlaced: Math.max(0, Math.floor(Number(stats.tradesPlaced) || 0)),
            wins: Math.max(0, Math.floor(Number(stats.wins) || 0)),
            losses: Math.max(0, Math.floor(Number(stats.losses) || 0)),
            realizedR: Number.isFinite(Number(stats.realizedR)) ? Number(stats.realizedR) : 0,
            consecutiveLosses: Math.max(0, Math.floor(Number(stats.consecutiveLosses) || 0)),
            lastExitAtMs: Number.isFinite(Number(stats.lastExitAtMs)) ? Number(stats.lastExitAtMs) : null,
            lastTradeAtMs: Number.isFinite(Number(stats.lastTradeAtMs)) ? Number(stats.lastTradeAtMs) : null,
        },
        run: {
            lastRunAtMs: Number.isFinite(Number(run.lastRunAtMs)) ? Number(run.lastRunAtMs) : null,
            lastRunId: String(run.lastRunId || '').trim() || null,
            dryRunLast: Boolean(run.dryRunLast),
            lastReasonCodes: compactReasonCodes(run.lastReasonCodes),
        },
    };
}

export async function loadScalpSessionState(
    symbol: string,
    dayKey: string,
    strategyId = defaultScalpStrategy.id,
    opts: ScalpDeploymentKeyOptions = {},
): Promise<ScalpSessionState | null> {
    const deployment = resolveDeploymentKey({
        symbol,
        strategyId,
        tuneId: opts.tuneId,
        deploymentId: opts.deploymentId,
    });
    const raw = await kvGetJson<ScalpSessionState>(stateKey(deployment.deploymentId, dayKey));
    const parsed = parseSessionState(raw);
    if (parsed) return hydrateSessionStateDeployment(parsed, deployment);

    if (deployment.tuneId === DEFAULT_SCALP_TUNE_ID) {
        const legacyStrategyRaw = await kvGetJson<ScalpSessionState>(
            legacyStrategyStateKey(deployment.strategyId, deployment.symbol, dayKey),
        );
        const legacyStrategyParsed = parseSessionState(legacyStrategyRaw);
        if (legacyStrategyParsed) {
            return hydrateSessionStateDeployment(legacyStrategyParsed, deployment);
        }
    }

    if (deployment.strategyId === defaultScalpStrategy.id && deployment.tuneId === DEFAULT_SCALP_TUNE_ID) {
        const legacyRaw = await kvGetJson<ScalpSessionState>(legacyStateKey(symbol, dayKey));
        const legacyParsed = parseSessionState(legacyRaw);
        return legacyParsed ? hydrateSessionStateDeployment(legacyParsed, deployment) : null;
    }
    return null;
}

export async function saveScalpSessionState(
    state: ScalpSessionState,
    ttlSeconds = SCALP_DEFAULT_STATE_TTL_SECONDS,
    strategyId = defaultScalpStrategy.id,
    opts: ScalpDeploymentKeyOptions = {},
): Promise<void> {
    const deployment = resolveDeploymentKey({
        symbol: state.symbol,
        strategyId: state.strategyId || strategyId,
        tuneId: opts.tuneId || state.tuneId,
        deploymentId: opts.deploymentId || state.deploymentId,
    });
    const nextState = hydrateSessionStateDeployment(state, deployment);
    await kvSetJson(stateKey(deployment.deploymentId, nextState.dayKey), nextState, Math.max(30, Math.floor(ttlSeconds)));
}

function sanitizeJournalEntry(entry: ScalpJournalEntry): ScalpJournalEntry {
    return {
        id: String(entry?.id || `${Date.now()}`),
        timestampMs: Number.isFinite(Number(entry?.timestampMs)) ? Number(entry.timestampMs) : Date.now(),
        type: entry?.type || 'execution',
        symbol: entry?.symbol ? String(entry.symbol).toUpperCase() : null,
        dayKey: entry?.dayKey ? String(entry.dayKey) : null,
        level: entry?.level === 'warn' || entry?.level === 'error' ? entry.level : 'info',
        reasonCodes: compactReasonCodes(entry?.reasonCodes),
        payload: safeRecord(entry?.payload),
    };
}

export async function appendScalpJournal(entry: ScalpJournalEntry, _maxRows = SCALP_DEFAULT_JOURNAL_MAX): Promise<void> {
    const sanitized = sanitizeJournalEntry(entry);
    await kvListPushJson(SCALP_JOURNAL_LIST_KEY, sanitized);
}

export async function loadScalpJournal(limit = 200): Promise<ScalpJournalEntry[]> {
  const safeLimit = Math.max(1, Math.min(1_000, Math.floor(limit)));
  const rows = await kvListRangeJson<ScalpJournalEntry>(SCALP_JOURNAL_LIST_KEY, 0, safeLimit - 1);
    const out: ScalpJournalEntry[] = [];
    for (const row of rows) {
        out.push(sanitizeJournalEntry(row));
    }
  return out;
}

function sanitizeTradeLedgerEntry(entry: ScalpTradeLedgerEntry): ScalpTradeLedgerEntry {
    const symbol = String(entry?.symbol || '')
        .trim()
        .toUpperCase();
    const strategyId = normalizeScalpStrategyId(entry?.strategyId) || defaultScalpStrategy.id;
    const tuneId = normalizeScalpTuneId(entry?.tuneId, DEFAULT_SCALP_TUNE_ID);
    const deploymentId = String(entry?.deploymentId || '')
        .trim();
    const sideRaw = String(entry?.side || '')
        .trim()
        .toUpperCase();
    const side = sideRaw === 'BUY' || sideRaw === 'SELL' ? sideRaw : null;
    const rMultiple = Number(entry?.rMultiple);
    return {
        id: String(entry?.id || `${Date.now()}`),
        timestampMs: Number.isFinite(Number(entry?.timestampMs)) ? Number(entry.timestampMs) : Date.now(),
        exitAtMs: Number.isFinite(Number(entry?.exitAtMs)) ? Number(entry.exitAtMs) : Date.now(),
        symbol: symbol || 'UNKNOWN',
        strategyId,
        tuneId,
        deploymentId,
        side,
        dryRun: Boolean(entry?.dryRun),
        rMultiple: Number.isFinite(rMultiple) ? rMultiple : 0,
        reasonCodes: compactReasonCodes(entry?.reasonCodes),
    };
}

export async function appendScalpTradeLedgerEntry(
    entry: ScalpTradeLedgerEntry,
    _maxRows = SCALP_DEFAULT_TRADE_LEDGER_MAX,
): Promise<void> {
    const sanitized = sanitizeTradeLedgerEntry(entry);
    await kvListPushJson(SCALP_TRADE_LEDGER_LIST_KEY, sanitized);
}

export async function loadScalpTradeLedger(limit = 2_000): Promise<ScalpTradeLedgerEntry[]> {
    const safeLimit = Math.max(1, Math.min(50_000, Math.floor(limit)));
    const rows = await kvListRangeJson<ScalpTradeLedgerEntry>(SCALP_TRADE_LEDGER_LIST_KEY, 0, safeLimit - 1);
    const out: ScalpTradeLedgerEntry[] = [];
    for (const row of rows) {
        out.push(sanitizeTradeLedgerEntry(row));
    }
    return out;
}

async function kvRawCommand(command: string, ...args: Array<string | number>): Promise<unknown> {
    if (!upstash_payasyougo_KV_REST_API_URL || !upstash_payasyougo_KV_REST_API_TOKEN) return null;
    const encodedArgs = args
        .map((arg) => encodeURIComponent(typeof arg === 'string' ? arg : String(arg)))
        .join('/');
    const url = `${upstash_payasyougo_KV_REST_API_URL}/${command}${encodedArgs ? `/${encodedArgs}` : ''}`;
    const res = await fetch(url, {
        method: 'POST',
        headers: {
            Authorization: `Bearer ${upstash_payasyougo_KV_REST_API_TOKEN}`,
        },
    });
    if (!res.ok) return null;
    const data = await res.json().catch(() => null);
    if (!data || typeof data !== 'object') return null;
    return (data as any).result ?? null;
}

export async function tryAcquireScalpRunLock(
    symbol: string,
    token: string,
    ttlSeconds: number,
    strategyId = defaultScalpStrategy.id,
    opts: ScalpDeploymentKeyOptions = {},
): Promise<boolean> {
    if (!upstash_payasyougo_KV_REST_API_URL || !upstash_payasyougo_KV_REST_API_TOKEN) {
        return true;
    }
    const deployment = resolveDeploymentKey({
        symbol,
        strategyId,
        tuneId: opts.tuneId,
        deploymentId: opts.deploymentId,
    });
    const key = runLockKey(deployment.deploymentId);
    const ttl = Math.max(15, Math.floor(ttlSeconds));
    const result = await kvRawCommand('SET', key, token, 'NX', 'EX', ttl);
    return String(result || '').toUpperCase() === 'OK';
}

export async function releaseScalpRunLock(
    symbol: string,
    token: string,
    strategyId = defaultScalpStrategy.id,
    opts: ScalpDeploymentKeyOptions = {},
): Promise<void> {
    if (!upstash_payasyougo_KV_REST_API_URL || !upstash_payasyougo_KV_REST_API_TOKEN) return;
    const deployment = resolveDeploymentKey({
        symbol,
        strategyId,
        tuneId: opts.tuneId,
        deploymentId: opts.deploymentId,
    });
    const key = runLockKey(deployment.deploymentId);
    const current = await kvRawCommand('GET', key);
    if (String(current || '') !== token) return;
    await kvRawCommand('DEL', key);
}
