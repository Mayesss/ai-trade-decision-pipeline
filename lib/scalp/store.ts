import { empty, join, raw, sql } from './pg/sql';

import { DEFAULT_SCALP_TUNE_ID, normalizeScalpTuneId, resolveScalpDeployment } from './deployments';
import { isScalpPgConfigured, scalpPrisma } from './pg/client';
import {
    insertJournalEntryToPg,
    insertTradeLedgerEntryToPg,
    upsertRuntimeDefaultToPg,
    upsertSessionStateToPg,
    upsertStrategyOverrideToPg,
} from './pg/storeMirror';
import {
    getDefaultScalpStrategy,
    getScalpStrategyById,
    listScalpStrategies,
    normalizeScalpStrategyId,
} from './strategies/registry';
import type { ScalpJournalEntry, ScalpSessionState, ScalpTradeLedgerEntry } from './types';
import { DEFAULT_SCALP_VENUE, normalizeScalpVenue, type ScalpVenue } from './venue';

const SCALP_DEFAULT_STATE_TTL_SECONDS = 3 * 24 * 60 * 60;
const SCALP_DEFAULT_JOURNAL_MAX = 500;
const SCALP_DEFAULT_TRADE_LEDGER_MAX = 10_000;
const SCALP_PERSIST_ACTOR = 'phase_g_pg_primary';

const defaultScalpStrategy = getDefaultScalpStrategy();
export const SCALP_STRATEGY_SHORT_NAME = defaultScalpStrategy.shortName;
export const SCALP_STRATEGY_LONG_NAME = defaultScalpStrategy.longName;

const pgStoreWarnings = new Set<string>();

function warnPgStoreOnce(key: string, message: string, err?: unknown): void {
    if (pgStoreWarnings.has(key)) return;
    pgStoreWarnings.add(key);
    if (process.env.NODE_ENV === 'test') return;
    if (err) {
        console.warn(message, err);
        return;
    }
    console.warn(message);
}

function isRecord(value: unknown): value is Record<string, unknown> {
    return Boolean(value) && typeof value === 'object' && !Array.isArray(value);
}

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

type ScalpDeploymentKeyOptions = {
    venue?: ScalpVenue;
    tuneId?: string;
    deploymentId?: string;
};

function resolveStrategyId(value: unknown, fallback = defaultScalpStrategy.id): string {
    const normalized = normalizeScalpStrategyId(value);
    if (!normalized) return fallback;
    const strategy = getScalpStrategyById(normalized);
    return strategy?.id || fallback;
}

function resolveDeploymentKey(params: {
    venue?: ScalpVenue;
    symbol: string;
    strategyId?: string;
    tuneId?: string;
    deploymentId?: string;
}) {
    return resolveScalpDeployment({
        venue: params.venue,
        symbol: params.symbol,
        strategyId: params.strategyId,
        tuneId: params.tuneId,
        deploymentId: params.deploymentId,
        fallbackVenue: DEFAULT_SCALP_VENUE,
        fallbackStrategyId: defaultScalpStrategy.id,
        fallbackTuneId: DEFAULT_SCALP_TUNE_ID,
    });
}

function hydrateSessionStateDeployment(
    state: ScalpSessionState,
    params: {
        venue?: ScalpVenue;
        symbol: string;
        strategyId?: string;
        tuneId?: string;
        deploymentId?: string;
    },
): ScalpSessionState {
    // Prefer caller-provided deployment identity so legacy state payloads
    // cannot overwrite canonical session-scoped ids.
    const deployment = resolveDeploymentKey({
        venue: params.venue || state.venue,
        symbol: params.symbol || state.symbol,
        strategyId: params.strategyId || state.strategyId,
        tuneId: params.tuneId || state.tuneId,
        deploymentId: params.deploymentId || state.deploymentId,
    });
    return {
        ...state,
        version: 2,
        venue: deployment.venue,
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

function toMs(value: unknown): number | null {
    if (value instanceof Date) return value.getTime();
    const n = Number(value);
    if (!Number.isFinite(n) || n <= 0) return null;
    return Math.floor(n);
}

async function loadRuntimeSettingsFromPg(): Promise<ScalpRuntimeSettings> {
    const fallback: ScalpRuntimeSettings = parseScalpRuntimeSettings({
        defaultStrategyId: defaultScalpStrategy.id,
        strategies: {},
    });
    if (!isScalpPgConfigured()) return fallback;

    try {
        const db = scalpPrisma();
        const [runtimeRows, overrideRows] = await Promise.all([
            db.$queryRaw<Array<{ defaultStrategyId: string | null }>>(sql`
                SELECT default_strategy_id AS "defaultStrategyId"
                FROM scalp_runtime_settings
                WHERE singleton = TRUE
                LIMIT 1;
            `),
            db.$queryRaw<
                Array<{
                    strategyId: string;
                    kvEnabled: boolean | null;
                    updatedAt: Date | null;
                    updatedBy: string | null;
                }>
            >(sql`
                SELECT
                    strategy_id AS "strategyId",
                    kv_enabled AS "kvEnabled",
                    updated_at AS "updatedAt",
                    updated_by AS "updatedBy"
                FROM scalp_strategy_overrides;
            `),
        ]);

        const runtimeRow = runtimeRows[0];
        const parsed = parseScalpRuntimeSettings({
            defaultStrategyId: runtimeRow?.defaultStrategyId || defaultScalpStrategy.id,
            strategies: Object.fromEntries(
                overrideRows.map((row) => [
                    row.strategyId,
                    {
                        enabled: typeof row.kvEnabled === 'boolean' ? row.kvEnabled : null,
                        updatedAtMs: toMs(row.updatedAt),
                        updatedBy: row.updatedBy || null,
                    },
                ]),
            ),
        });
        return parsed;
    } catch (err) {
        warnPgStoreOnce('load_runtime_settings_failed', 'Failed to load scalp runtime settings from PG.', err);
        return fallback;
    }
}

export async function loadScalpStrategyRuntimeSnapshot(
    envEnabled: boolean,
    preferredStrategyId?: string,
): Promise<ScalpStrategyRuntimeSnapshot> {
    const settings = await loadRuntimeSettingsFromPg();
    const defaultStrategyId = resolveStrategyId(settings.defaultStrategyId, defaultScalpStrategy.id);
    const selectedStrategyId = resolveStrategyId(preferredStrategyId, defaultStrategyId);
    const strategies = listScalpStrategies().map((strategy) => toControlSnapshot(settings, strategy.id, envEnabled));
    const strategy =
        strategies.find((row) => row.strategyId === selectedStrategyId) ||
        toControlSnapshot(settings, defaultStrategyId, envEnabled);

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
    const current = await loadRuntimeSettingsFromPg();
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

    if (!isScalpPgConfigured()) {
        warnPgStoreOnce(
            'set_strategy_enabled_missing_pg',
            'Scalp strategy toggle skipped because PG is not configured (set DATABASE_URL or SCALP_PG_CONNECTION_STRING).',
        );
        return loadScalpStrategyRuntimeSnapshot(params.envEnabled, strategyId);
    }

    try {
        await upsertRuntimeDefaultToPg({
            defaultStrategyId: nextSettings.defaultStrategyId,
            envEnabled: params.envEnabled,
            updatedBy: parseUpdatedBy(params.updatedBy) || SCALP_PERSIST_ACTOR,
        });
        await upsertStrategyOverrideToPg({
            strategyId,
            kvEnabled: nextSettings.strategies[strategyId]?.enabled ?? null,
            updatedAtMs: nextSettings.strategies[strategyId]?.updatedAtMs ?? Date.now(),
            updatedBy: nextSettings.strategies[strategyId]?.updatedBy || SCALP_PERSIST_ACTOR,
        });
    } catch (err) {
        warnPgStoreOnce('set_strategy_enabled_failed', 'Failed to persist scalp strategy toggle to PG.', err);
    }

    return loadScalpStrategyRuntimeSnapshot(params.envEnabled, strategyId);
}

export async function setScalpDefaultStrategy(params: {
    strategyId: string;
    envEnabled: boolean;
}): Promise<ScalpStrategyRuntimeSnapshot> {
    const current = await loadRuntimeSettingsFromPg();
    const strategyId = resolveStrategyId(params.strategyId, current.defaultStrategyId);
    if (isScalpPgConfigured()) {
        try {
            await upsertRuntimeDefaultToPg({
                defaultStrategyId: strategyId,
                envEnabled: params.envEnabled,
                updatedBy: SCALP_PERSIST_ACTOR,
            });
        } catch (err) {
            warnPgStoreOnce('set_default_strategy_failed', 'Failed to persist scalp default strategy to PG.', err);
        }
    } else {
        warnPgStoreOnce(
            'set_default_strategy_missing_pg',
            'Scalp default strategy update skipped because PG is not configured (set DATABASE_URL or SCALP_PG_CONNECTION_STRING).',
        );
    }
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
        venue: normalizeScalpVenue(row.venue, DEFAULT_SCALP_VENUE),
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
    if (!isScalpPgConfigured()) return null;
    const deployment = resolveDeploymentKey({
        venue: opts.venue,
        symbol,
        strategyId,
        tuneId: opts.tuneId,
        deploymentId: opts.deploymentId,
    });
    try {
        const db = scalpPrisma();
        const rows = await db.$queryRaw<Array<{ stateJson: unknown }>>(sql`
            SELECT state_json AS "stateJson"
            FROM scalp_sessions
            WHERE deployment_id = ${deployment.deploymentId}
              AND day_key = ${dayKey}::date
            LIMIT 1;
        `);
        const parsed = parseSessionState(rows[0]?.stateJson);
        return parsed ? hydrateSessionStateDeployment(parsed, deployment) : null;
    } catch (err) {
        warnPgStoreOnce('load_session_state_failed', 'Failed to load scalp session state from PG.', err);
        return null;
    }
}

export async function saveScalpSessionState(
    state: ScalpSessionState,
    _ttlSeconds = SCALP_DEFAULT_STATE_TTL_SECONDS,
    strategyId = defaultScalpStrategy.id,
    opts: ScalpDeploymentKeyOptions = {},
): Promise<void> {
    if (!isScalpPgConfigured()) return;
    const deployment = resolveDeploymentKey({
        venue: opts.venue || state.venue,
        symbol: state.symbol,
        strategyId: state.strategyId || strategyId,
        tuneId: opts.tuneId || state.tuneId,
        deploymentId: opts.deploymentId || state.deploymentId,
    });
    const nextState = hydrateSessionStateDeployment(state, deployment);
    try {
        await upsertSessionStateToPg(nextState);
    } catch (err) {
        warnPgStoreOnce('save_session_state_failed', 'Failed to persist scalp session state to PG.', err);
    }
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

function normalizeJournalType(value: unknown): ScalpJournalEntry['type'] {
    const normalized = String(value || '').trim().toLowerCase();
    if (normalized === 'state') return 'state';
    if (normalized === 'risk') return 'risk';
    if (normalized === 'error') return 'error';
    return 'execution';
}

export async function appendScalpJournal(entry: ScalpJournalEntry, _maxRows = SCALP_DEFAULT_JOURNAL_MAX): Promise<void> {
    if (!isScalpPgConfigured()) return;
    const sanitized = sanitizeJournalEntry(entry);
    try {
        await insertJournalEntryToPg(sanitized);
    } catch (err) {
        warnPgStoreOnce('append_journal_failed', 'Failed to append scalp journal entry in PG.', err);
    }
}

export async function loadScalpJournal(limit = 200): Promise<ScalpJournalEntry[]> {
    if (!isScalpPgConfigured()) return [];
    const safeLimit = Math.max(1, Math.min(1_000, Math.floor(limit)));
    try {
        const db = scalpPrisma();
        const rows = await db.$queryRaw<
            Array<{
                id: string;
                timestampMs: bigint | number | string;
                type: string;
                symbol: string | null;
                dayKey: string | null;
                level: string;
                reasonCodes: string[];
                payload: unknown;
            }>
        >(sql`
            SELECT
                id::text AS id,
                (EXTRACT(EPOCH FROM ts) * 1000.0)::bigint AS "timestampMs",
                type,
                symbol,
                TO_CHAR(day_key, 'YYYY-MM-DD') AS "dayKey",
                level,
                reason_codes AS "reasonCodes",
                payload
            FROM scalp_journal
            ORDER BY ts DESC
            LIMIT ${safeLimit};
        `);
        return rows.map((row) =>
            sanitizeJournalEntry({
                id: row.id,
                timestampMs: Number(row.timestampMs),
                type: normalizeJournalType(row.type),
                symbol: row.symbol,
                dayKey: row.dayKey,
                level: row.level === 'warn' || row.level === 'error' ? row.level : 'info',
                reasonCodes: Array.isArray(row.reasonCodes) ? row.reasonCodes : [],
                payload: isRecord(row.payload) ? row.payload : {},
            }),
        );
    } catch (err) {
        warnPgStoreOnce('load_journal_failed', 'Failed to load scalp journal entries from PG.', err);
        return [];
    }
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

function normalizeTradeSide(value: unknown): 'BUY' | 'SELL' | null {
    const sideRaw = String(value || '')
        .trim()
        .toUpperCase();
    return sideRaw === 'BUY' || sideRaw === 'SELL' ? sideRaw : null;
}

export async function appendScalpTradeLedgerEntry(
    entry: ScalpTradeLedgerEntry,
    _maxRows = SCALP_DEFAULT_TRADE_LEDGER_MAX,
): Promise<void> {
    if (!isScalpPgConfigured()) return;
    const sanitized = sanitizeTradeLedgerEntry(entry);
    try {
        await insertTradeLedgerEntryToPg(sanitized);
    } catch (err) {
        warnPgStoreOnce('append_trade_ledger_failed', 'Failed to append scalp trade ledger row in PG.', err);
    }
}

export async function loadScalpTradeLedger(limit = 2_000): Promise<ScalpTradeLedgerEntry[]> {
    if (!isScalpPgConfigured()) return [];
    const safeLimit = Math.max(1, Math.min(50_000, Math.floor(limit)));
    try {
        const db = scalpPrisma();
        const rows = await db.$queryRaw<
            Array<{
                id: string;
                timestampMs: bigint | number | string;
                exitAtMs: bigint | number | string;
                symbol: string;
                strategyId: string;
                tuneId: string;
                deploymentId: string;
                side: string | null;
                dryRun: boolean;
                rMultiple: number | string;
                reasonCodes: string[];
            }>
        >(sql`
            SELECT
                id::text AS id,
                (EXTRACT(EPOCH FROM created_at) * 1000.0)::bigint AS "timestampMs",
                (EXTRACT(EPOCH FROM exit_at) * 1000.0)::bigint AS "exitAtMs",
                symbol,
                strategy_id AS "strategyId",
                tune_id AS "tuneId",
                deployment_id AS "deploymentId",
                side,
                dry_run AS "dryRun",
                r_multiple::double precision AS "rMultiple",
                reason_codes AS "reasonCodes"
            FROM scalp_trade_ledger
            ORDER BY exit_at DESC
            LIMIT ${safeLimit};
        `);
        return rows.map((row) =>
            sanitizeTradeLedgerEntry({
                id: row.id,
                timestampMs: Number(row.timestampMs),
                exitAtMs: Number(row.exitAtMs),
                symbol: row.symbol,
                strategyId: row.strategyId,
                tuneId: row.tuneId,
                deploymentId: row.deploymentId,
                side: normalizeTradeSide(row.side),
                dryRun: Boolean(row.dryRun),
                rMultiple: Number(row.rMultiple),
                reasonCodes: Array.isArray(row.reasonCodes) ? row.reasonCodes : [],
            }),
        );
    } catch (err) {
        warnPgStoreOnce('load_trade_ledger_failed', 'Failed to load scalp trade ledger rows from PG.', err);
        return [];
    }
}

export async function tryAcquireScalpRunLock(
    _symbol: string,
    _token: string,
    _ttlSeconds: number,
    _strategyId = defaultScalpStrategy.id,
    _opts: ScalpDeploymentKeyOptions = {},
): Promise<boolean> {
    return true;
}

export async function releaseScalpRunLock(
    _symbol: string,
    _token: string,
    _strategyId = defaultScalpStrategy.id,
    _opts: ScalpDeploymentKeyOptions = {},
): Promise<void> {
}

export function serializeScalpRuntimeSnapshotForDebug(
    snapshot: ScalpStrategyRuntimeSnapshot,
): Record<string, unknown> {
    const settings: ScalpRuntimeSettings = {
        defaultStrategyId: snapshot.defaultStrategyId,
        strategies: Object.fromEntries(
            snapshot.strategies.map((row) => [
                row.strategyId,
                {
                    enabled: row.kvEnabled,
                    updatedAtMs: row.updatedAtMs,
                    updatedBy: row.updatedBy,
                },
            ]),
        ),
    };
    return serializeScalpRuntimeSettings(settings);
}
