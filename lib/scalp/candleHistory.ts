import { mkdir, readFile, writeFile } from 'node:fs/promises';
import path from 'node:path';

import { kvGetJson, kvSetJson } from '../kv';
import type { ScalpCandle } from './types';

export type CandleHistoryBackend = 'file' | 'kv';

export interface ScalpCandleHistoryRecord {
    version: 1;
    symbol: string;
    timeframe: string;
    epic: string | null;
    source: 'capital';
    updatedAtMs: number;
    candles: ScalpCandle[];
}

export interface ScalpCandleHistoryLoadResult {
    backend: CandleHistoryBackend;
    storageRef: string;
    record: ScalpCandleHistoryRecord | null;
}

export interface ScalpCandleHistorySaveResult {
    backend: CandleHistoryBackend;
    storageRef: string;
    saved: boolean;
}

const CANDLE_HISTORY_VERSION = 1 as const;
const CANDLE_HISTORY_KEY_PREFIX = 'scalp:candles-history:v1';
const DEFAULT_CANDLE_HISTORY_DIR = 'data/candles-history';

function normalizeSymbol(value: string): string {
    return String(value || '')
        .trim()
        .toUpperCase()
        .replace(/[^A-Z0-9._-]/g, '');
}

function normalizeTimeframe(value: string): string {
    const normalized = String(value || '')
        .trim()
        .toLowerCase();
    const match = normalized.match(/^(\d+)([mhdw])$/);
    if (!match) return '15m';
    const amount = Number(match[1]);
    const unit = match[2];
    if (!Number.isFinite(amount) || amount <= 0) return '15m';
    return `${Math.floor(amount)}${unit}`;
}

function normalizeCandleRow(row: unknown): ScalpCandle | null {
    const value = row as unknown[];
    if (!Array.isArray(value)) return null;
    const ts = Number(value[0]);
    const open = Number(value[1]);
    const high = Number(value[2]);
    const low = Number(value[3]);
    const close = Number(value[4]);
    const volume = Number(value[5] ?? 0);
    if (![ts, open, high, low, close].every((n) => Number.isFinite(n) && n > 0)) return null;
    return [Math.floor(ts), open, high, low, close, Number.isFinite(volume) ? volume : 0];
}

function dedupeSortCandles(rows: ScalpCandle[]): ScalpCandle[] {
    const byTs = new Map<number, ScalpCandle>();
    for (const row of rows) {
        const normalized = normalizeCandleRow(row);
        if (!normalized) continue;
        byTs.set(normalized[0], normalized);
    }
    return Array.from(byTs.values()).sort((a, b) => a[0] - b[0]);
}

function normalizeRecord(
    raw: unknown,
    fallback: { symbol: string; timeframe: string; epic: string | null },
): ScalpCandleHistoryRecord | null {
    if (!raw || typeof raw !== 'object') return null;
    const row = raw as Record<string, unknown>;
    const symbol = normalizeSymbol(String(row.symbol || fallback.symbol));
    const timeframe = normalizeTimeframe(String(row.timeframe || fallback.timeframe));
    if (!symbol) return null;
    const candlesRaw = Array.isArray(row.candles) ? row.candles : [];
    const candles = dedupeSortCandles(candlesRaw.map((item) => normalizeCandleRow(item)).filter((c): c is ScalpCandle => Boolean(c)));
    return {
        version: CANDLE_HISTORY_VERSION,
        symbol,
        timeframe,
        epic: row.epic ? String(row.epic).trim().toUpperCase() : fallback.epic,
        source: 'capital',
        updatedAtMs: Number.isFinite(Number(row.updatedAtMs)) ? Number(row.updatedAtMs) : Date.now(),
        candles,
    };
}

function getLocalHistoryRoot(): string {
    const configured = String(process.env.CANDLE_HISTORY_DIR || DEFAULT_CANDLE_HISTORY_DIR).trim();
    const root = path.isAbsolute(configured) ? configured : path.resolve(process.cwd(), configured);
    return root;
}

function resolveBackend(preferred?: CandleHistoryBackend): CandleHistoryBackend {
    if (preferred) return preferred;
    const mode = String(process.env.CANDLE_HISTORY_STORE || 'auto')
        .trim()
        .toLowerCase();
    if (mode === 'kv') return 'kv';
    if (mode === 'file') return 'file';
    const hasKv = Boolean(process.env.KV_REST_API_URL && process.env.KV_REST_API_TOKEN);
    return hasKv ? 'kv' : 'file';
}

function historyKvKey(symbol: string, timeframe: string): string {
    return `${CANDLE_HISTORY_KEY_PREFIX}:${symbol}:${timeframe}`;
}

function historyFilePath(symbol: string, timeframe: string): string {
    return path.join(getLocalHistoryRoot(), symbol, `${timeframe}.json`);
}

async function loadFromFile(symbol: string, timeframe: string): Promise<ScalpCandleHistoryLoadResult> {
    const filePath = historyFilePath(symbol, timeframe);
    try {
        const raw = await readFile(filePath, 'utf8');
        const parsed = JSON.parse(raw);
        return {
            backend: 'file',
            storageRef: filePath,
            record: normalizeRecord(parsed, { symbol, timeframe, epic: null }),
        };
    } catch {
        return {
            backend: 'file',
            storageRef: filePath,
            record: null,
        };
    }
}

async function saveToFile(record: ScalpCandleHistoryRecord): Promise<ScalpCandleHistorySaveResult> {
    const filePath = historyFilePath(record.symbol, record.timeframe);
    await mkdir(path.dirname(filePath), { recursive: true });
    await writeFile(filePath, JSON.stringify(record, null, 2), 'utf8');
    return {
        backend: 'file',
        storageRef: filePath,
        saved: true,
    };
}

async function loadFromKv(symbol: string, timeframe: string): Promise<ScalpCandleHistoryLoadResult> {
    const key = historyKvKey(symbol, timeframe);
    const raw = await kvGetJson<unknown>(key);
    return {
        backend: 'kv',
        storageRef: key,
        record: normalizeRecord(raw, { symbol, timeframe, epic: null }),
    };
}

async function saveToKv(record: ScalpCandleHistoryRecord): Promise<ScalpCandleHistorySaveResult> {
    const key = historyKvKey(record.symbol, record.timeframe);
    await kvSetJson(key, record);
    return {
        backend: 'kv',
        storageRef: key,
        saved: true,
    };
}

export async function loadScalpCandleHistory(
    symbolRaw: string,
    timeframeRaw: string,
    opts: { backend?: CandleHistoryBackend } = {},
): Promise<ScalpCandleHistoryLoadResult> {
    const symbol = normalizeSymbol(symbolRaw);
    const timeframe = normalizeTimeframe(timeframeRaw);
    if (!symbol) {
        throw new Error('Invalid candle-history symbol');
    }
    const backend = resolveBackend(opts.backend);
    if (backend === 'kv') return loadFromKv(symbol, timeframe);
    return loadFromFile(symbol, timeframe);
}

export async function saveScalpCandleHistory(
    recordRaw: Omit<ScalpCandleHistoryRecord, 'version' | 'updatedAtMs' | 'candles'> & {
        candles: ScalpCandle[];
    },
    opts: { backend?: CandleHistoryBackend } = {},
): Promise<ScalpCandleHistorySaveResult> {
    const symbol = normalizeSymbol(recordRaw.symbol);
    const timeframe = normalizeTimeframe(recordRaw.timeframe);
    if (!symbol) {
        throw new Error('Invalid candle-history symbol');
    }
    const record: ScalpCandleHistoryRecord = {
        version: CANDLE_HISTORY_VERSION,
        symbol,
        timeframe,
        epic: recordRaw.epic ? String(recordRaw.epic).trim().toUpperCase() : null,
        source: 'capital',
        updatedAtMs: Date.now(),
        candles: dedupeSortCandles(recordRaw.candles || []),
    };
    const backend = resolveBackend(opts.backend);
    if (backend === 'kv') return saveToKv(record);
    return saveToFile(record);
}

export function mergeScalpCandleHistory(existing: ScalpCandle[], incoming: ScalpCandle[]): ScalpCandle[] {
    return dedupeSortCandles([...(existing || []), ...(incoming || [])]);
}

export function timeframeToMs(timeframeRaw: string): number {
    const timeframe = normalizeTimeframe(timeframeRaw);
    const match = timeframe.match(/^(\d+)([mhdw])$/);
    if (!match) return 15 * 60_000;
    const amount = Number(match[1]);
    const unit = match[2];
    if (!Number.isFinite(amount) || amount <= 0) return 15 * 60_000;
    if (unit === 'm') return amount * 60_000;
    if (unit === 'h') return amount * 60 * 60_000;
    if (unit === 'd') return amount * 24 * 60 * 60_000;
    return amount * 7 * 24 * 60 * 60_000;
}

export function normalizeHistoryTimeframe(value: string): string {
    return normalizeTimeframe(value);
}

