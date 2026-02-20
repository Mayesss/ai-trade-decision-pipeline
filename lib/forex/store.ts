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
    if (!(Number.isFinite(Number(raw.stopPrice)) && Number(raw.stopPrice) > 0)) return null;
    return raw;
}

export async function deleteForexPositionContext(pair: string) {
    await kvSetJson(positionContextKey(pair), null, 5);
}
