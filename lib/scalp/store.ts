import { kvGetJson, kvListPushJson, kvListRangeJson, kvListTrim, kvSetJson } from '../kv';
import {
    getDefaultScalpStrategy,
    getScalpStrategyById,
    listScalpStrategies,
    normalizeScalpStrategyId,
} from './strategies/registry';
import type { ScalpJournalEntry, ScalpSessionState } from './types';

const SCALP_STATE_KEY_PREFIX = 'scalp:state:v1';
const SCALP_RUN_LOCK_KEY_PREFIX = 'scalp:runlock:v1';
const SCALP_JOURNAL_LIST_KEY = 'scalp:journal:list:v1';
const SCALP_RUNTIME_SETTINGS_KEY = 'scalp:runtime:settings:v1';
const SCALP_DEFAULT_STATE_TTL_SECONDS = 3 * 24 * 60 * 60;
const SCALP_DEFAULT_JOURNAL_MAX = 500;

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

const KV_REST_API_URL = (process.env.KV_REST_API_URL || '').replace(/\/$/, '');
const KV_REST_API_TOKEN = process.env.KV_REST_API_TOKEN || '';

function resolveStrategyId(value: unknown, fallback = defaultScalpStrategy.id): string {
    const normalized = normalizeScalpStrategyId(value);
    if (!normalized) return fallback;
    const strategy = getScalpStrategyById(normalized);
    return strategy?.id || fallback;
}

function stateKey(strategyId: string, symbol: string, dayKey: string): string {
    return `${SCALP_STATE_KEY_PREFIX}:${strategyId}:${String(symbol || '').toUpperCase()}:${dayKey}`;
}

function legacyStateKey(symbol: string, dayKey: string): string {
    return `${SCALP_STATE_KEY_PREFIX}:${String(symbol || '').toUpperCase()}:${dayKey}`;
}

function runLockKey(strategyId: string, symbol: string): string {
    return `${SCALP_RUN_LOCK_KEY_PREFIX}:${strategyId}:${String(symbol || '').toUpperCase()}`;
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
        version: 1,
        symbol,
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
): Promise<ScalpSessionState | null> {
    const resolvedStrategyId = resolveStrategyId(strategyId, defaultScalpStrategy.id);
    const raw = await kvGetJson<ScalpSessionState>(stateKey(resolvedStrategyId, symbol, dayKey));
    const parsed = parseSessionState(raw);
    if (parsed) return parsed;

    if (resolvedStrategyId === defaultScalpStrategy.id) {
        const legacyRaw = await kvGetJson<ScalpSessionState>(legacyStateKey(symbol, dayKey));
        return parseSessionState(legacyRaw);
    }
    return null;
}

export async function saveScalpSessionState(
    state: ScalpSessionState,
    ttlSeconds = SCALP_DEFAULT_STATE_TTL_SECONDS,
    strategyId = defaultScalpStrategy.id,
): Promise<void> {
    const resolvedStrategyId = resolveStrategyId(strategyId, defaultScalpStrategy.id);
    await kvSetJson(stateKey(resolvedStrategyId, state.symbol, state.dayKey), state, Math.max(30, Math.floor(ttlSeconds)));
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

export async function appendScalpJournal(entry: ScalpJournalEntry, maxRows = SCALP_DEFAULT_JOURNAL_MAX): Promise<void> {
    const sanitized = sanitizeJournalEntry(entry);
    const max = Math.max(10, Math.min(2_000, Math.floor(maxRows)));
    await kvListPushJson(SCALP_JOURNAL_LIST_KEY, sanitized);
    await kvListTrim(SCALP_JOURNAL_LIST_KEY, 0, max - 1);
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

export async function tryAcquireScalpRunLock(
    symbol: string,
    token: string,
    ttlSeconds: number,
    strategyId = defaultScalpStrategy.id,
): Promise<boolean> {
    if (!KV_REST_API_URL || !KV_REST_API_TOKEN) {
        return true;
    }
    const key = runLockKey(resolveStrategyId(strategyId, defaultScalpStrategy.id), symbol);
    const ttl = Math.max(15, Math.floor(ttlSeconds));
    const result = await kvRawCommand('SET', key, token, 'NX', 'EX', ttl);
    return String(result || '').toUpperCase() === 'OK';
}

export async function releaseScalpRunLock(symbol: string, token: string, strategyId = defaultScalpStrategy.id): Promise<void> {
    if (!KV_REST_API_URL || !KV_REST_API_TOKEN) return;
    const key = runLockKey(resolveStrategyId(strategyId, defaultScalpStrategy.id), symbol);
    const current = await kvRawCommand('GET', key);
    if (String(current || '') !== token) return;
    await kvRawCommand('DEL', key);
}
