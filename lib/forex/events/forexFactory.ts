import crypto from 'crypto';

import { kvGetJson, kvSetJson } from '../../kv';
import type {
    ForexEventSnapshot,
    ForexEventState,
    ForexEventStoreMeta,
    NormalizedForexEconomicEvent,
} from '../types';
import { currentUtcDayKey, formatUtcDate, getForexEventConfig, normalizeImpact } from './config';

const EVENTS_SNAPSHOT_KEY = 'forex:events:snapshot:v1';
const EVENTS_META_KEY = 'forex:events:meta:v1';
const EVENTS_CALL_COUNTER_PREFIX = 'forex:events:forexfactory:calls';
const EVENTS_STORE_TTL_SECONDS = 14 * 24 * 60 * 60;
const EVENTS_CALL_COUNTER_TTL_SECONDS = 8 * 24 * 60 * 60;

const COUNTRY_TO_CURRENCY: Record<string, string> = {
    US: 'USD',
    USA: 'USD',
    UNITEDSTATES: 'USD',
    EUR: 'EUR',
    EU: 'EUR',
    EMU: 'EUR',
    EUROAREA: 'EUR',
    EUROZONE: 'EUR',
    GB: 'GBP',
    GBR: 'GBP',
    UNITEDKINGDOM: 'GBP',
    UK: 'GBP',
    JP: 'JPY',
    JPN: 'JPY',
    JAPAN: 'JPY',
    CH: 'CHF',
    CHE: 'CHF',
    SWITZERLAND: 'CHF',
    CA: 'CAD',
    CAN: 'CAD',
    CANADA: 'CAD',
    AU: 'AUD',
    AUS: 'AUD',
    AUSTRALIA: 'AUD',
    NZ: 'NZD',
    NZL: 'NZD',
    NEWZEALAND: 'NZD',
};

function safeRecord(value: unknown): Record<string, unknown> {
    if (!value || typeof value !== 'object' || Array.isArray(value)) return {};
    return value as Record<string, unknown>;
}

function normalizeCountryKey(value: string): string {
    return value.replace(/[^A-Z]/g, '');
}

export function resolveCountryCurrency(value: unknown): string | null {
    const raw = String(value ?? '')
        .trim()
        .toUpperCase();
    if (!raw) return null;
    const cleaned = normalizeCountryKey(raw);
    return COUNTRY_TO_CURRENCY[cleaned] ?? null;
}

function resolveCurrency(row: Record<string, unknown>): string | null {
    const direct = String(row.currency ?? row.ccy ?? row.country ?? '')
        .trim()
        .toUpperCase();
    if (/^[A-Z]{3}$/.test(direct)) return direct;

    const fromCountry = resolveCountryCurrency(row.country ?? row.countryCode);
    if (fromCountry) return fromCountry;

    return null;
}

function toIsoUtcString(dateMs: number): string {
    return new Date(dateMs).toISOString();
}

export function parseEventTimestampUtc(rawValue: unknown): string | null {
    const raw = String(rawValue ?? '').trim();
    if (!raw) return null;

    const withT = raw.includes('T') ? raw : raw.replace(' ', 'T');
    const withZone = /[zZ]$|[+\-]\d{2}:?\d{2}$/.test(withT) ? withT : `${withT}Z`;
    const ts = Date.parse(withZone);
    if (!Number.isFinite(ts)) return null;
    return toIsoUtcString(ts);
}

function normalizeMetricValue(value: unknown): string | number | null {
    if (value === null || value === undefined) return null;
    if (typeof value === 'number') return Number.isFinite(value) ? value : null;

    const str = String(value).trim();
    if (!str) return null;
    const lowered = str.toLowerCase();
    if (lowered === 'null' || lowered === 'n/a' || lowered === 'na' || lowered === '--') return null;

    const normalized = str.replace(/,/g, '');
    if (/^[+-]?\d*\.?\d+$/.test(normalized)) {
        const numeric = Number(normalized);
        if (Number.isFinite(numeric)) return numeric;
    }

    return str;
}

function normalizeEventName(value: unknown): string | null {
    const name = String(value ?? '').trim();
    return name ? name : null;
}

function buildEventId(currency: string, timestampUtc: string, eventName: string): string {
    const base = `${currency}|${timestampUtc}|${eventName.toUpperCase()}`;
    return crypto.createHash('sha1').update(base).digest('hex').slice(0, 20);
}

export function normalizeForexFactoryEventRow(value: unknown): NormalizedForexEconomicEvent | null {
    const row = safeRecord(value);
    const currency = resolveCurrency(row);
    if (!currency) return null;

    const timestampUtc = parseEventTimestampUtc(row.date ?? row.dateUtc ?? row.timestamp ?? row.time);
    if (!timestampUtc) return null;

    const eventName = normalizeEventName(row.event ?? row.name ?? row.title);
    if (!eventName) return null;

    return {
        id: buildEventId(currency, timestampUtc, eventName),
        timestamp_utc: timestampUtc,
        currency,
        impact: normalizeImpact(row.impact ?? row.importance ?? row.priority),
        event_name: eventName,
        actual: normalizeMetricValue(row.actual),
        forecast: normalizeMetricValue(row.forecast ?? row.estimate),
        previous: normalizeMetricValue(row.previous),
        source: 'forexfactory',
    };
}

function callCounterKey(dayKey: string): string {
    return `${EVENTS_CALL_COUNTER_PREFIX}:${dayKey}`;
}

function defaultMeta(dayKey: string, counter = 0): ForexEventStoreMeta {
    return {
        lastFetchAttemptAtMs: null,
        lastSuccessAtMs: null,
        lastFailureAtMs: null,
        lastError: null,
        callCounterDay: dayKey,
        callCounter: counter,
    };
}

async function readCallCounter(dayKey: string): Promise<number> {
    const current = await kvGetJson<number>(callCounterKey(dayKey));
    return Number.isFinite(current as number) ? Number(current) : 0;
}

async function bumpCallCounter(dayKey: string): Promise<number> {
    const current = await readCallCounter(dayKey);
    const next = current + 1;
    await kvSetJson(callCounterKey(dayKey), next, EVENTS_CALL_COUNTER_TTL_SECONDS);
    return next;
}

async function loadSnapshot(): Promise<ForexEventSnapshot | null> {
    return kvGetJson<ForexEventSnapshot>(EVENTS_SNAPSHOT_KEY);
}

async function loadMeta(dayKey: string): Promise<ForexEventStoreMeta> {
    const todayCounter = await readCallCounter(dayKey);
    const rawMeta = await kvGetJson<ForexEventStoreMeta>(EVENTS_META_KEY);
    if (!rawMeta) return defaultMeta(dayKey, todayCounter);

    return {
        ...rawMeta,
        callCounterDay: dayKey,
        callCounter: todayCounter,
    };
}

function isSnapshotStale(lastSuccessAtMs: number | null, staleMinutes: number, nowMs: number): boolean {
    if (!Number.isFinite(lastSuccessAtMs as number) || (lastSuccessAtMs as number) <= 0) return true;
    return nowMs - Number(lastSuccessAtMs) > staleMinutes * 60_000;
}

function timestampMs(isoTimestamp: string): number {
    const ts = Date.parse(isoTimestamp);
    return Number.isFinite(ts) ? ts : NaN;
}

function normalizeCalendarUrl(raw: string): string {
    const value = String(raw || '').trim();
    if (!value) return 'https://nfs.faireconomy.media/ff_calendar_thisweek.json';
    if (value.startsWith('http://') || value.startsWith('https://')) return value;
    return `https://${value.replace(/^\/+/, '')}`;
}

async function fetchForexFactoryCalendar(fromDate: string, toDate: string): Promise<NormalizedForexEconomicEvent[]> {
    const cfg = getForexEventConfig();
    const url = normalizeCalendarUrl(cfg.forexFactoryCalendarUrl);
    const res = await fetch(url, { method: 'GET' });

    if (!res.ok) {
        const message = await res.text().catch(() => '');
        throw new Error(`ForexFactory calendar error ${res.status}: ${message || res.statusText}`);
    }

    const payload: unknown = await res.json();
    if (!Array.isArray(payload)) {
        throw new Error('ForexFactory calendar returned invalid payload');
    }

    const fromMs = Date.parse(`${fromDate}T00:00:00.000Z`);
    const toMs = Date.parse(`${toDate}T23:59:59.999Z`);

    const deduped = new Map<string, NormalizedForexEconomicEvent>();
    for (const row of payload) {
        const normalized = normalizeForexFactoryEventRow(row);
        if (!normalized) continue;
        const ts = timestampMs(normalized.timestamp_utc);
        if (!Number.isFinite(ts)) continue;
        if (ts < fromMs || ts > toMs) continue;
        deduped.set(normalized.id, normalized);
    }

    return Array.from(deduped.values()).sort((a, b) => Date.parse(a.timestamp_utc) - Date.parse(b.timestamp_utc));
}

export async function getForexEventsState(nowMs = Date.now()): Promise<ForexEventState> {
    const cfg = getForexEventConfig();
    const dayKey = currentUtcDayKey(nowMs);

    const [snapshot, meta] = await Promise.all([loadSnapshot(), loadMeta(dayKey)]);

    return {
        snapshot,
        meta,
        stale: isSnapshotStale(meta.lastSuccessAtMs, cfg.staleMinutes, nowMs),
        staleMinutes: cfg.staleMinutes,
        refreshMinutes: cfg.refreshMinutes,
    };
}

export function shouldWarnCallBudget(callCounter: number, warnThreshold: number): boolean {
    if (!Number.isFinite(callCounter) || !Number.isFinite(warnThreshold)) return false;
    return callCounter > warnThreshold;
}

export type RefreshForexEventsResult = {
    ok: boolean;
    refreshed: boolean;
    skipped: boolean;
    reason: string | null;
    state: ForexEventState;
    fromDate: string;
    toDate: string;
    requestedAtMs: number;
};

export async function refreshForexEvents(opts: { force?: boolean; nowMs?: number } = {}): Promise<RefreshForexEventsResult> {
    const nowMs = Number.isFinite(opts.nowMs as number) ? Number(opts.nowMs) : Date.now();
    const force = Boolean(opts.force);
    const cfg = getForexEventConfig();
    const dayKey = currentUtcDayKey(nowMs);
    const fromDate = formatUtcDate(new Date(nowMs - 24 * 60 * 60 * 1000));
    const toDate = formatUtcDate(new Date(nowMs + 7 * 24 * 60 * 60 * 1000));

    const stateBefore = await getForexEventsState(nowMs);
    const recentlyRefreshed =
        Number.isFinite(stateBefore.meta.lastSuccessAtMs as number) &&
        nowMs - Number(stateBefore.meta.lastSuccessAtMs) < cfg.refreshMinutes * 60_000;

    if (!force && recentlyRefreshed) {
        return {
            ok: true,
            refreshed: false,
            skipped: true,
            reason: 'within_refresh_interval',
            state: stateBefore,
            fromDate,
            toDate,
            requestedAtMs: nowMs,
        };
    }

    const callCounter = await bumpCallCounter(dayKey);
    if (shouldWarnCallBudget(callCounter, cfg.callWarnThreshold)) {
        console.warn(
            `Forex events ForexFactory call counter warning: ${callCounter} calls today (threshold=${cfg.callWarnThreshold}).`,
        );
    }

    const attemptMeta: ForexEventStoreMeta = {
        ...stateBefore.meta,
        callCounterDay: dayKey,
        callCounter,
        lastFetchAttemptAtMs: nowMs,
    };
    await kvSetJson(EVENTS_META_KEY, attemptMeta, EVENTS_STORE_TTL_SECONDS);

    try {
        const events = await fetchForexFactoryCalendar(fromDate, toDate);
        const snapshot: ForexEventSnapshot = {
            source: 'forexfactory',
            fetchedAtMs: nowMs,
            fromDate,
            toDate,
            events,
        };

        const successMeta: ForexEventStoreMeta = {
            ...attemptMeta,
            lastSuccessAtMs: nowMs,
            lastFailureAtMs: null,
            lastError: null,
        };

        await Promise.all([
            kvSetJson(EVENTS_SNAPSHOT_KEY, snapshot, EVENTS_STORE_TTL_SECONDS),
            kvSetJson(EVENTS_META_KEY, successMeta, EVENTS_STORE_TTL_SECONDS),
        ]);

        return {
            ok: true,
            refreshed: true,
            skipped: false,
            reason: null,
            state: await getForexEventsState(nowMs),
            fromDate,
            toDate,
            requestedAtMs: nowMs,
        };
    } catch (err) {
        const failureMeta: ForexEventStoreMeta = {
            ...attemptMeta,
            lastFailureAtMs: nowMs,
            lastError: err instanceof Error ? err.message : String(err),
        };
        await kvSetJson(EVENTS_META_KEY, failureMeta, EVENTS_STORE_TTL_SECONDS);

        return {
            ok: false,
            refreshed: false,
            skipped: false,
            reason: failureMeta.lastError,
            state: await getForexEventsState(nowMs),
            fromDate,
            toDate,
            requestedAtMs: nowMs,
        };
    }
}
