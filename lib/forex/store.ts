import { kvGetJson, kvSetJson } from '../kv';
import type {
    ForexJournalEntry,
    ForexPacketSnapshot,
    ForexPositionContext,
    ForexScanSnapshot,
} from './types';

const FOREX_SCAN_KEY = 'forex:scan:latest:v1';
const FOREX_PACKETS_KEY = 'forex:packets:latest:v1';
const FOREX_JOURNAL_KEY = 'forex:journal:latest:v1';
const FOREX_COOLDOWN_KEY_PREFIX = 'forex:risk:cooldown';
const FOREX_RANGE_FADE_COOLDOWN_KEY_PREFIX = 'forex:module:range_fade:cooldown';
const FOREX_POSITION_CONTEXT_KEY_PREFIX = 'forex:position:context';
const FOREX_REENTRY_LOCK_KEY_PREFIX = 'forex:reentry:lock';
const FOREX_STORE_TTL_SECONDS = 14 * 24 * 60 * 60;
const FOREX_JOURNAL_MAX = 1200;

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
    const raw = await kvGetJson<ForexJournalEntry[]>(FOREX_JOURNAL_KEY);
    if (!Array.isArray(raw)) return [];
    return raw.slice(0, Math.max(1, limit));
}

export async function appendForexJournal(entry: ForexJournalEntry) {
    const existing = await kvGetJson<ForexJournalEntry[]>(FOREX_JOURNAL_KEY);
    const rows = Array.isArray(existing) ? existing : [];
    rows.unshift(entry);
    if (rows.length > FOREX_JOURNAL_MAX) {
        rows.splice(FOREX_JOURNAL_MAX);
    }
    await kvSetJson(FOREX_JOURNAL_KEY, rows, FOREX_STORE_TTL_SECONDS);
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
