import { kvGetJson, kvListPushJson, kvListRangeJson, kvListTrim, kvSetJson } from '../kv';
import type {
    ForexJournalEntry,
    ForexPacketSnapshot,
    ForexPositionContext,
    ForexScanSnapshot,
} from './types';

const FOREX_SCAN_KEY = 'forex:scan:latest:v1';
const FOREX_PACKETS_KEY = 'forex:packets:latest:v1';
const FOREX_JOURNAL_LEGACY_KEY = 'forex:journal:latest:v1';
const FOREX_JOURNAL_LIST_KEY = 'forex:journal:list:v2';
const FOREX_COOLDOWN_KEY_PREFIX = 'forex:risk:cooldown';
const FOREX_RANGE_FADE_COOLDOWN_KEY_PREFIX = 'forex:module:range_fade:cooldown';
const FOREX_POSITION_CONTEXT_KEY_PREFIX = 'forex:position:context';
const FOREX_REENTRY_LOCK_KEY_PREFIX = 'forex:reentry:lock';
const FOREX_STORE_TTL_SECONDS = 14 * 24 * 60 * 60;
const FOREX_JOURNAL_MAX = 500;
const FOREX_JOURNAL_ENTRY_MAX_BYTES = 1400;
const FOREX_JOURNAL_STRING_MAX = 220;
const FOREX_JOURNAL_REASON_CODES_MAX = 16;

function safeRecord(value: unknown): Record<string, any> {
    if (!value || typeof value !== 'object' || Array.isArray(value)) return {};
    return value as Record<string, any>;
}

function toFiniteNumber(value: unknown, digits = 6): number | null {
    const n = Number(value);
    if (!Number.isFinite(n)) return null;
    return Number(n.toFixed(digits));
}

function compactString(value: unknown, max = FOREX_JOURNAL_STRING_MAX): string | null {
    if (value === null || value === undefined) return null;
    const raw = String(value).trim();
    if (!raw) return null;
    if (raw.length <= max) return raw;
    return `${raw.slice(0, max)}…`;
}

function compactReasonCodes(value: unknown): string[] {
    if (!Array.isArray(value)) return [];
    const out: string[] = [];
    for (const item of value) {
        const code = compactString(item, 80);
        if (!code) continue;
        out.push(code.toUpperCase());
        if (out.length >= FOREX_JOURNAL_REASON_CODES_MAX) break;
    }
    return out;
}

function compactPacket(packetRaw: unknown): Record<string, any> | undefined {
    const packet = safeRecord(packetRaw);
    if (!Object.keys(packet).length) return undefined;

    const out: Record<string, any> = {};
    const pair = compactString(packet.pair, 16);
    if (pair) out.pair = pair.toUpperCase();
    const generatedAtMs = toFiniteNumber(packet.generatedAtMs, 0);
    if (generatedAtMs !== null) out.generatedAtMs = generatedAtMs;

    const regime = compactString(packet.regime, 24);
    if (regime) out.regime = regime;
    const permission = compactString(packet.permission, 24);
    if (permission) out.permission = permission;
    const riskState = compactString(packet.risk_state, 24);
    if (riskState) out.risk_state = riskState;

    const confidence = toFiniteNumber(packet.confidence, 4);
    if (confidence !== null) out.confidence = confidence;

    if (Array.isArray(packet.allowed_modules)) {
        out.allowed_modules = packet.allowed_modules
            .map((v: unknown) => compactString(v, 24))
            .filter((v: string | null): v is string => Boolean(v))
            .slice(0, 4);
    }
    if (Array.isArray(packet.notes_codes)) {
        out.notes_codes = packet.notes_codes
            .map((v: unknown) => compactString(v, 60))
            .filter((v: string | null): v is string => Boolean(v))
            .slice(0, 8);
    }

    return Object.keys(out).length ? out : undefined;
}

function compactRow(rowRaw: unknown): Record<string, any> | undefined {
    const row = safeRecord(rowRaw);
    if (!Object.keys(row).length) return undefined;
    const out: Record<string, any> = {};
    const pair = compactString(row.pair, 16);
    if (pair) out.pair = pair.toUpperCase();
    if (typeof row.eligible === 'boolean') out.eligible = row.eligible;
    const rank = toFiniteNumber(row.rank, 0);
    if (rank !== null) out.rank = rank;
    const score = toFiniteNumber(row.score, 4);
    if (score !== null) out.score = score;

    const metrics = safeRecord(row.metrics);
    if (Object.keys(metrics).length) {
        out.metrics = {
            sessionTag: compactString(metrics.sessionTag, 20),
            spreadPips: toFiniteNumber(metrics.spreadPips, 4),
            spreadToAtr1h: toFiniteNumber(metrics.spreadToAtr1h, 5),
            atr1hPercent: toFiniteNumber(metrics.atr1hPercent, 6),
            trendStrength: toFiniteNumber(metrics.trendStrength, 4),
            chopScore: toFiniteNumber(metrics.chopScore, 4),
            shockFlag: Boolean(metrics.shockFlag),
        };
    }
    return Object.keys(out).length ? out : undefined;
}

function compactObject(value: unknown, depth = 0): any {
    if (value === null || value === undefined) return null;
    if (typeof value === 'string') return compactString(value);
    if (typeof value === 'number') return Number.isFinite(value) ? value : null;
    if (typeof value === 'boolean') return value;
    if (Array.isArray(value)) {
        return value.slice(0, depth === 0 ? 12 : 8).map((item) => compactObject(item, depth + 1));
    }
    if (typeof value !== 'object') return compactString(value);
    if (depth >= 3) return '[truncated]';

    const out: Record<string, any> = {};
    const input = value as Record<string, unknown>;
    const keys = Object.keys(input).slice(0, 16);
    for (const key of keys) {
        if (key === 'matchedEvents') continue;
        if (key === 'reasonCodes') {
            out.reasonCodes = compactReasonCodes(input[key]);
            continue;
        }
        const next = compactObject(input[key], depth + 1);
        if (next === null || next === undefined || next === '') continue;
        out[key] = next;
    }
    return out;
}

function compactPayload(type: ForexJournalEntry['type'], payloadRaw: unknown): Record<string, any> {
    const payload = safeRecord(payloadRaw);
    if (!Object.keys(payload).length) return {};

    const out: Record<string, any> = {};
    const includeKeys = [
        'generatedAtMs',
        'dryRun',
        'phase',
        'packetAgeMinutes',
        'notionalUsd',
        'stale',
        'refreshed',
        'skipped',
        'reentryLockUntil',
        'staleThresholdMinutes',
        'orderNotionalUsd',
        'attemptedPairs',
        'placedPairs',
    ];

    for (const key of includeKeys) {
        if (!(key in payload)) continue;
        const next = compactObject(payload[key], 0);
        if (next === null || next === undefined || next === '') continue;
        out[key] = next;
    }

    if (type === 'execution') {
        if ('signal' in payload) out.signal = compactObject(payload.signal, 0);
        if ('decision' in payload) out.decision = compactObject(payload.decision, 0);
        if ('execution' in payload) out.execution = compactObject(payload.execution, 0);
        if ('risk' in payload) out.risk = compactObject(payload.risk, 0);
        if ('gate' in payload) out.gate = compactObject(payload.gate, 0);
        if ('eventTier' in payload) out.eventTier = compactObject(payload.eventTier, 0);
        if ('progress' in payload) out.progress = compactObject(payload.progress, 0);
        if ('openPosition' in payload) out.openPosition = compactObject(payload.openPosition, 0);
        if ('positionContext' in payload) out.positionContext = compactObject(payload.positionContext, 0);
        if ('sizing' in payload) out.sizing = compactObject(payload.sizing, 0);
        if ('riskCap' in payload) out.riskCap = compactObject(payload.riskCap, 0);
        if ('packet' in payload) out.packet = compactPacket(payload.packet);
        if ('row' in payload) out.row = compactRow(payload.row);
    } else {
        const maybePacket = compactPacket(payload.packet);
        if (maybePacket) out.packet = maybePacket;
        const maybeRow = compactRow(payload.row);
        if (maybeRow) out.row = maybeRow;
    }

    if ('reason' in payload && !('reason' in out)) {
        const reason = compactString(payload.reason, 260);
        if (reason) out.reason = reason;
    }

    return out;
}

function sanitizeJournalEntry(entry: ForexJournalEntry): ForexJournalEntry {
    const type = (entry?.type || 'execution') as ForexJournalEntry['type'];
    const sanitized: ForexJournalEntry = {
        id: compactString(entry?.id, 80) || `${Date.now()}`,
        timestampMs: Number.isFinite(Number(entry?.timestampMs)) ? Number(entry.timestampMs) : Date.now(),
        type,
        pair: compactString(entry?.pair, 16)?.toUpperCase() ?? null,
        level: entry?.level === 'warn' || entry?.level === 'error' ? entry.level : 'info',
        reasonCodes: compactReasonCodes(entry?.reasonCodes),
        payload: compactPayload(type, entry?.payload),
    };

    const payload = { ...safeRecord(sanitized.payload) };
    const withPayload = () => ({ ...sanitized, payload });
    const withinLimit = () => Buffer.byteLength(JSON.stringify(withPayload()), 'utf8') <= FOREX_JOURNAL_ENTRY_MAX_BYTES;
    if (withinLimit()) return withPayload();

    const dropOrder = [
        'positionContext',
        'openPosition',
        'row',
        'packet',
        'sizing',
        'riskCap',
        'progress',
        'eventTier',
        'risk',
        'gate',
        'execution',
        'decision',
        'signal',
        'attemptedPairs',
        'placedPairs',
    ];

    for (const key of dropOrder) {
        if (!(key in payload)) continue;
        delete payload[key];
        if (withinLimit()) return withPayload();
    }

    const minimalPayload: Record<string, any> = {};
    for (const key of ['generatedAtMs', 'phase', 'dryRun', 'packetAgeMinutes', 'notionalUsd']) {
        if (payload[key] !== undefined) {
            minimalPayload[key] = payload[key];
        }
    }
    minimalPayload.truncated = true;
    sanitized.payload = minimalPayload;
    if (Buffer.byteLength(JSON.stringify(sanitized), 'utf8') <= FOREX_JOURNAL_ENTRY_MAX_BYTES) return sanitized;

    return {
        ...sanitized,
        reasonCodes: sanitized.reasonCodes.slice(0, 4),
        payload: { truncated: true },
    };
}

function parseJournalRows(rows: unknown[]): ForexJournalEntry[] {
    const out: ForexJournalEntry[] = [];
    for (const row of rows) {
        if (!row || typeof row !== 'object' || Array.isArray(row)) continue;
        const raw = row as ForexJournalEntry;
        out.push({
            id: compactString(raw.id, 80) || `${Date.now()}:${out.length}`,
            timestampMs: Number.isFinite(Number(raw.timestampMs)) ? Number(raw.timestampMs) : Date.now(),
            type: raw.type,
            pair: compactString(raw.pair, 16)?.toUpperCase() ?? null,
            level: raw.level === 'warn' || raw.level === 'error' ? raw.level : 'info',
            reasonCodes: compactReasonCodes(raw.reasonCodes),
            payload: safeRecord(raw.payload),
        });
    }
    return out;
}

function cooldownKey(pair: string): string {
    return `${FOREX_COOLDOWN_KEY_PREFIX}:${String(pair || '').toUpperCase()}`;
}

function rangeFadeCooldownKey(pair: string): string {
    return `${FOREX_RANGE_FADE_COOLDOWN_KEY_PREFIX}:${String(pair || '').toUpperCase()}`;
}

function positionContextKey(pair: string): string {
    return `${FOREX_POSITION_CONTEXT_KEY_PREFIX}:${String(pair || '').toUpperCase()}`;
}

function reentryLockKey(pair: string): string {
    return `${FOREX_REENTRY_LOCK_KEY_PREFIX}:${String(pair || '').toUpperCase()}`;
}

export async function saveForexScanSnapshot(snapshot: ForexScanSnapshot) {
    await kvSetJson(FOREX_SCAN_KEY, snapshot, FOREX_STORE_TTL_SECONDS);
}

export async function loadForexScanSnapshot(): Promise<ForexScanSnapshot | null> {
    return kvGetJson<ForexScanSnapshot>(FOREX_SCAN_KEY);
}

export async function saveForexPacketSnapshot(snapshot: ForexPacketSnapshot) {
    await kvSetJson(FOREX_PACKETS_KEY, snapshot, FOREX_STORE_TTL_SECONDS);
}

export async function loadForexPacketSnapshot(): Promise<ForexPacketSnapshot | null> {
    return kvGetJson<ForexPacketSnapshot>(FOREX_PACKETS_KEY);
}

export async function loadForexJournal(limit = 200): Promise<ForexJournalEntry[]> {
    const safeLimit = Math.max(1, Math.min(1000, Math.floor(limit)));
    const listRows = await kvListRangeJson<ForexJournalEntry>(FOREX_JOURNAL_LIST_KEY, 0, safeLimit - 1);
    const parsedList = parseJournalRows(listRows).slice(0, safeLimit);
    if (parsedList.length > 0) return parsedList;

    const legacyRows = await kvGetJson<ForexJournalEntry[]>(FOREX_JOURNAL_LEGACY_KEY);
    if (!Array.isArray(legacyRows)) return [];
    return parseJournalRows(legacyRows).slice(0, safeLimit);
}

export async function appendForexJournal(entry: ForexJournalEntry) {
    const sanitized = sanitizeJournalEntry(entry);
    await kvListPushJson(FOREX_JOURNAL_LIST_KEY, sanitized);
    await kvListTrim(FOREX_JOURNAL_LIST_KEY, 0, FOREX_JOURNAL_MAX - 1);
}

export async function setForexPairCooldown(pair: string, untilMs: number) {
    await kvSetJson(cooldownKey(pair), { untilMs }, FOREX_STORE_TTL_SECONDS);
}

export async function getForexPairCooldownUntil(pair: string): Promise<number | null> {
    const raw = await kvGetJson<{ untilMs?: number }>(cooldownKey(pair));
    const value = Number(raw?.untilMs);
    return Number.isFinite(value) && value > 0 ? value : null;
}

export async function setForexRangeFadeCooldown(pair: string, untilMs: number) {
    await kvSetJson(rangeFadeCooldownKey(pair), { untilMs }, FOREX_STORE_TTL_SECONDS);
}

export async function getForexRangeFadeCooldownUntil(pair: string): Promise<number | null> {
    const raw = await kvGetJson<{ untilMs?: number }>(rangeFadeCooldownKey(pair));
    const value = Number(raw?.untilMs);
    return Number.isFinite(value) && value > 0 ? value : null;
}

export async function saveForexPositionContext(context: ForexPositionContext) {
    await kvSetJson(positionContextKey(context.pair), context, FOREX_STORE_TTL_SECONDS);
}

export async function loadForexPositionContext(pair: string): Promise<ForexPositionContext | null> {
    const raw = await kvGetJson<ForexPositionContext | null>(positionContextKey(pair));
    if (!raw || typeof raw !== 'object') return null;
    if (raw.side !== 'BUY' && raw.side !== 'SELL') return null;

    const entryModuleRaw = String((raw as any).entryModule || (raw as any).module || '')
        .trim()
        .toLowerCase();
    if (!['pullback', 'breakout_retest', 'range_fade'].includes(entryModuleRaw)) return null;

    const entryPrice = Number((raw as any).entryPrice);
    const initialStopPrice = Number((raw as any).initialStopPrice ?? (raw as any).stopPrice);
    const currentStopPrice = Number((raw as any).currentStopPrice ?? (raw as any).stopPrice ?? initialStopPrice);
    if (!(Number.isFinite(entryPrice) && entryPrice > 0)) return null;
    if (!(Number.isFinite(initialStopPrice) && initialStopPrice > 0)) return null;
    if (!(Number.isFinite(currentStopPrice) && currentStopPrice > 0)) return null;

    const initialRiskRaw = Number((raw as any).initialRiskPrice);
    const inferredRisk = Math.abs(entryPrice - initialStopPrice);
    const initialRiskPrice = Number.isFinite(initialRiskRaw) && initialRiskRaw > 0 ? initialRiskRaw : inferredRisk;
    if (!(Number.isFinite(initialRiskPrice) && initialRiskPrice > 0)) return null;

    const openedAtMs = Number((raw as any).openedAtMs);
    if (!(Number.isFinite(openedAtMs) && openedAtMs > 0)) return null;
    const lastManagedAtMs = Number((raw as any).lastManagedAtMs ?? (raw as any).updatedAtMs ?? openedAtMs);
    const lastCloseAtMsRaw = Number((raw as any).lastCloseAtMs);

    const trailingModeRaw = String((raw as any).trailingMode || '').trim().toLowerCase();
    const trailingMode =
        trailingModeRaw === 'structure' ||
        trailingModeRaw === 'atr' ||
        trailingModeRaw === 'range_protective' ||
        trailingModeRaw === 'none'
            ? trailingModeRaw
            : 'none';

    const partialTaken = Number((raw as any).partialTakenPct);
    const partialTakenPct = Number.isFinite(partialTaken) ? Math.max(0, Math.min(100, partialTaken)) : 0;
    const tp1 = Number((raw as any).tp1Price);
    const tp2 = Number((raw as any).tp2Price);
    const rangeLower = Number((raw as any).rangeLowerBoundary);
    const rangeUpper = Number((raw as any).rangeUpperBoundary);
    const entryNotionalUsd = Number((raw as any).entryNotionalUsd);
    const entryLeverage = Number((raw as any).entryLeverage);

    return {
        ...raw,
        pair: String((raw as any).pair || pair).toUpperCase(),
        side: raw.side,
        entryModule: entryModuleRaw as ForexPositionContext['entryModule'],
        entryPrice,
        initialStopPrice,
        currentStopPrice,
        initialRiskPrice,
        partialTakenPct,
        trailingActive: Boolean((raw as any).trailingActive),
        trailingMode,
        tp1Price: Number.isFinite(tp1) ? tp1 : null,
        tp2Price: Number.isFinite(tp2) ? tp2 : null,
        rangeLowerBoundary: Number.isFinite(rangeLower) ? rangeLower : null,
        rangeUpperBoundary: Number.isFinite(rangeUpper) ? rangeUpper : null,
        openedAtMs,
        lastManagedAtMs: Number.isFinite(lastManagedAtMs) ? lastManagedAtMs : openedAtMs,
        lastCloseAtMs: Number.isFinite(lastCloseAtMsRaw) && lastCloseAtMsRaw > 0 ? lastCloseAtMsRaw : null,
        module: entryModuleRaw as ForexPositionContext['entryModule'],
        stopPrice: currentStopPrice,
        updatedAtMs: Number.isFinite(lastManagedAtMs) ? lastManagedAtMs : openedAtMs,
        entryNotionalUsd: Number.isFinite(entryNotionalUsd) && entryNotionalUsd > 0 ? entryNotionalUsd : null,
        entryLeverage: Number.isFinite(entryLeverage) && entryLeverage > 0 ? entryLeverage : null,
    };
}

export async function deleteForexPositionContext(pair: string) {
    await kvSetJson(positionContextKey(pair), null, 5);
}

export async function setForexReentryLock(pair: string, untilMs: number) {
    await kvSetJson(reentryLockKey(pair), { untilMs }, FOREX_STORE_TTL_SECONDS);
}

export async function getForexReentryLockUntil(pair: string): Promise<number | null> {
    const raw = await kvGetJson<{ untilMs?: number }>(reentryLockKey(pair));
    const value = Number(raw?.untilMs);
    return Number.isFinite(value) && value > 0 ? value : null;
}

export async function clearForexReentryLock(pair: string) {
    await kvSetJson(reentryLockKey(pair), null, 5);
}
