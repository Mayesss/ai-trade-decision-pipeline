import { kvGetJson, kvListPushJson, kvListRangeJson, kvListTrim, kvSetJson } from '../kv';
import type { ScalpJournalEntry, ScalpSessionState } from './types';

const SCALP_STATE_KEY_PREFIX = 'scalp:state:v1';
const SCALP_RUN_LOCK_KEY_PREFIX = 'scalp:runlock:v1';
const SCALP_JOURNAL_LIST_KEY = 'scalp:journal:list:v1';
const SCALP_RUNTIME_SETTINGS_KEY = 'scalp:runtime:settings:v1';
const SCALP_DEFAULT_STATE_TTL_SECONDS = 3 * 24 * 60 * 60;
const SCALP_DEFAULT_JOURNAL_MAX = 500;

export const SCALP_STRATEGY_SHORT_NAME = 'HSS-ICT M15/M3';
export const SCALP_STRATEGY_LONG_NAME = 'Hybrid Session-Scoped ICT Scalp (M15/M3)';

interface ScalpRuntimeSettings {
    strategyEnabled: boolean;
    updatedAtMs: number;
    updatedBy: string | null;
}

export interface ScalpStrategyControlSnapshot {
    shortName: string;
    longName: string;
    enabled: boolean;
    envEnabled: boolean;
    kvEnabled: boolean | null;
    updatedAtMs: number | null;
    updatedBy: string | null;
}

const KV_REST_API_URL = (process.env.KV_REST_API_URL || '').replace(/\/$/, '');
const KV_REST_API_TOKEN = process.env.KV_REST_API_TOKEN || '';

function stateKey(symbol: string, dayKey: string): string {
    return `${SCALP_STATE_KEY_PREFIX}:${String(symbol || '').toUpperCase()}:${dayKey}`;
}

function runLockKey(symbol: string): string {
    return `${SCALP_RUN_LOCK_KEY_PREFIX}:${String(symbol || '').toUpperCase()}`;
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

function parseScalpRuntimeSettings(raw: unknown): ScalpRuntimeSettings | null {
    const row = safeRecord(raw);
    const strategyEnabled = parseBool(row.strategyEnabled);
    if (strategyEnabled === null) return null;
    const updatedAtMs = Number(row.updatedAtMs);
    const updatedByRaw = String(row.updatedBy || '').trim();
    return {
        strategyEnabled,
        updatedAtMs: Number.isFinite(updatedAtMs) && updatedAtMs > 0 ? Math.floor(updatedAtMs) : Date.now(),
        updatedBy: updatedByRaw ? updatedByRaw.slice(0, 120) : null,
    };
}

export async function loadScalpStrategyControlSnapshot(envEnabled: boolean): Promise<ScalpStrategyControlSnapshot> {
    const settings = parseScalpRuntimeSettings(await kvGetJson<ScalpRuntimeSettings>(SCALP_RUNTIME_SETTINGS_KEY));
    const kvEnabled = settings?.strategyEnabled ?? null;
    const effectiveEnabled = Boolean(envEnabled) && (kvEnabled ?? true);
    return {
        shortName: SCALP_STRATEGY_SHORT_NAME,
        longName: SCALP_STRATEGY_LONG_NAME,
        enabled: effectiveEnabled,
        envEnabled: Boolean(envEnabled),
        kvEnabled,
        updatedAtMs: settings?.updatedAtMs ?? null,
        updatedBy: settings?.updatedBy ?? null,
    };
}

export async function setScalpStrategyKvEnabled(params: {
    enabled: boolean;
    envEnabled: boolean;
    updatedBy?: string | null;
}): Promise<ScalpStrategyControlSnapshot> {
    const nextSettings: ScalpRuntimeSettings = {
        strategyEnabled: Boolean(params.enabled),
        updatedAtMs: Date.now(),
        updatedBy: String(params.updatedBy || '').trim() || null,
    };
    await kvSetJson(SCALP_RUNTIME_SETTINGS_KEY, nextSettings);
    return {
        shortName: SCALP_STRATEGY_SHORT_NAME,
        longName: SCALP_STRATEGY_LONG_NAME,
        enabled: Boolean(params.envEnabled) && nextSettings.strategyEnabled,
        envEnabled: Boolean(params.envEnabled),
        kvEnabled: nextSettings.strategyEnabled,
        updatedAtMs: nextSettings.updatedAtMs,
        updatedBy: nextSettings.updatedBy,
    };
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

export async function loadScalpSessionState(symbol: string, dayKey: string): Promise<ScalpSessionState | null> {
    const raw = await kvGetJson<ScalpSessionState>(stateKey(symbol, dayKey));
    return parseSessionState(raw);
}

export async function saveScalpSessionState(
    state: ScalpSessionState,
    ttlSeconds = SCALP_DEFAULT_STATE_TTL_SECONDS,
): Promise<void> {
    await kvSetJson(stateKey(state.symbol, state.dayKey), state, Math.max(30, Math.floor(ttlSeconds)));
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

export async function tryAcquireScalpRunLock(symbol: string, token: string, ttlSeconds: number): Promise<boolean> {
    if (!KV_REST_API_URL || !KV_REST_API_TOKEN) {
        return true;
    }
    const key = runLockKey(symbol);
    const ttl = Math.max(15, Math.floor(ttlSeconds));
    const result = await kvRawCommand('SET', key, token, 'NX', 'EX', ttl);
    return String(result || '').toUpperCase() === 'OK';
}

export async function releaseScalpRunLock(symbol: string, token: string): Promise<void> {
    if (!KV_REST_API_URL || !KV_REST_API_TOKEN) return;
    const key = runLockKey(symbol);
    const current = await kvRawCommand('GET', key);
    if (String(current || '') !== token) return;
    await kvRawCommand('DEL', key);
}
