import { empty, join, raw, sql } from './pg/sql';

import {
    fetchCapitalCandlesByEpicDateRange,
    resolveCapitalEpicRuntime,
} from '../capital';
import { kvGetJson, kvMGetJson, kvSetJson } from '../kv';
import { fetchBitgetCandlesByEpicDateRange } from './bitgetHistory';
import { isScalpPgConfigured, scalpPrisma } from './pg/client';
import type { ScalpCandle } from './types';

export type CandleHistoryBackend = 'pg';
export type ScalpCandleHistorySource = 'broker' | 'kv' | 'pg';
export type ScalpCandleHistoryVenue = 'bitget' | 'capital';
export type ScalpCandleHistoryReadSource = ScalpCandleHistorySource;

export interface ScalpCandleHistoryDiagnostics {
    source: ScalpCandleHistorySource;
    fallbacksTried: string[];
    coverageRatio?: number;
    storageRef: string;
}

export interface ScalpCandleHistoryReadOptions {
    backend?: CandleHistoryBackend;
    venue?: ScalpCandleHistoryVenue;
    readOrder?: ScalpCandleHistorySource[];
    maxBrokerRangeDays?: number;
    requireCoverageRatio?: number;
    auditSource?: string;
}

export interface ScalpCandleHistoryRecord {
    version: 1;
    symbol: string;
    timeframe: string;
    epic: string | null;
    source: ScalpCandleHistoryVenue;
    updatedAtMs: number;
    candles: ScalpCandle[];
}

export interface ScalpCandleHistoryLoadResult {
    backend: CandleHistoryBackend;
    storageRef: string;
    record: ScalpCandleHistoryRecord | null;
    diagnostics?: ScalpCandleHistoryDiagnostics;
}

export interface ScalpCandleHistorySaveResult {
    backend: CandleHistoryBackend;
    storageRef: string;
    saved: boolean;
}

export interface ScalpCandleHistoryBulkSaveResult {
    backend: CandleHistoryBackend;
    saved: number;
    storageRef: string;
}

export interface ScalpCandleHistoryStatsLoadResult {
    backend: CandleHistoryBackend;
    storageRef: string;
    symbol: string;
    timeframe: string;
    epic: string | null;
    updatedAtMs: number | null;
    candleCount: number;
    fromTsMs: number | null;
    toTsMs: number | null;
}

const CANDLE_HISTORY_VERSION = 1 as const;
const ONE_DAY_MS = 24 * 60 * 60 * 1000;
const ONE_WEEK_MS = 7 * ONE_DAY_MS;
const DEFAULT_READ_ORDER: ScalpCandleHistorySource[] = ['broker', 'kv', 'pg'];
const DEFAULT_REQUIRE_COVERAGE_RATIO = 0.8;
const DEFAULT_MAX_BROKER_RANGE_DAYS = 14;
const DEFAULT_KV_TTL_SECONDS = 400 * 24 * 60 * 60;

export interface ScalpCandleHistoryFreshnessStats {
    symbol: string;
    timeframe: string;
    fromTsMs: number;
    toTsMs: number;
    latestCandleTsMs: number | null;
    candlesInWindow: number;
    storageRef: string;
    diagnostics: ScalpCandleHistoryDiagnostics;
}

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

function normalizeHistorySource(value: unknown, fallback: ScalpCandleHistoryRecord['source'] = 'bitget'): ScalpCandleHistoryRecord['source'] {
    const normalized = String(value || '')
        .trim()
        .toLowerCase();
    if (normalized === 'bitget') return 'bitget';
    if (normalized === 'capital') return 'capital';
    return fallback;
}

function normalizeVenue(value: unknown): ScalpCandleHistoryVenue {
    return String(value || '').trim().toLowerCase() === 'capital' ? 'capital' : 'bitget';
}

function inferVenueForSymbol(symbol: string): ScalpCandleHistoryVenue {
    const normalized = normalizeSymbol(symbol);
    if (/^[A-Z]{6}$/.test(normalized) && !normalized.endsWith('USDT')) return 'capital';
    return 'bitget';
}

function candleHistoryKvTtlSeconds(): number {
    const raw = Math.floor(Number(process.env.SCALP_CANDLE_HISTORY_KV_TTL_SECONDS || DEFAULT_KV_TTL_SECONDS));
    return Number.isFinite(raw) && raw > 0 ? raw : DEFAULT_KV_TTL_SECONDS;
}

function normalizeReadOrder(value: unknown): ScalpCandleHistorySource[] {
    const raw = Array.isArray(value) && value.length
        ? value
        : String(process.env.SCALP_CANDLE_HISTORY_READ_ORDER || '')
            .split(',')
            .map((row) => row.trim())
            .filter(Boolean);
    const normalized = (raw.length ? raw : DEFAULT_READ_ORDER)
        .map((row) => String(row || '').trim().toLowerCase())
        .filter((row): row is ScalpCandleHistorySource => row === 'broker' || row === 'kv' || row === 'pg');
    return normalized.length ? Array.from(new Set(normalized)) : DEFAULT_READ_ORDER;
}

function historyKvKey(symbol: string, timeframe: string, weekStartMs: number): string {
    return `scalp:candles:v1:${symbol}:${timeframe}:${weekStartMs}`;
}

function weekStartsForRange(fromTsMs: number, toTsMs: number): number[] {
    const fromMs = Math.max(0, Math.floor(Number(fromTsMs) || 0));
    const toMs = Math.max(fromMs + 1, Math.floor(Number(toTsMs) || 0));
    const starts: number[] = [];
    let cursor = weekStartMondayUtcMs(fromMs);
    const end = weekStartMondayUtcMs(Math.max(fromMs, toMs - 1));
    while (cursor <= end) {
        starts.push(cursor);
        cursor += ONE_WEEK_MS;
    }
    return starts;
}

function expectedCandleCount(fromTsMs: number, toTsMs: number, timeframe: string): number {
    const tfMs = Math.max(1, timeframeToMs(timeframe));
    const span = Math.max(0, Math.floor(Number(toTsMs) || 0) - Math.floor(Number(fromTsMs) || 0));
    return Math.max(1, Math.floor(span / tfMs));
}

function filterCandlesInRange(candles: ScalpCandle[], fromTsMs: number, toTsMs: number): ScalpCandle[] {
    const fromMs = Math.max(0, Math.floor(Number(fromTsMs) || 0));
    const toMs = Math.max(fromMs + 1, Math.floor(Number(toTsMs) || 0));
    return dedupeSortCandles((candles || []).filter((row) => {
        const ts = Number(row?.[0]);
        return Number.isFinite(ts) && ts >= fromMs && ts < toMs;
    }));
}

function coverageRatioFor(candles: ScalpCandle[], fromTsMs: number, toTsMs: number, timeframe: string): number {
    return Math.min(1, candles.length / Math.max(1, expectedCandleCount(fromTsMs, toTsMs, timeframe)));
}

function shouldLogCandleHistory(): boolean {
    const raw = String(process.env.SCALP_CANDLE_HISTORY_AUDIT_LOG || '').trim().toLowerCase();
    return ['1', 'true', 'yes', 'on'].includes(raw);
}

function logCandleHistory(event: string, payload: Record<string, unknown>): void {
    if (!shouldLogCandleHistory() && event !== 'full_history_read_warning') return;
    try {
        console.info(`[scalp-candle-history] ${JSON.stringify({ event, ...payload })}`);
    } catch {
        console.info('[scalp-candle-history]', event, payload);
    }
}

function maybeWarnFullHistoryRead(symbol: string, timeframe: string, auditSource?: string): void {
    const env = String(process.env.NODE_ENV || '').trim().toLowerCase();
    const warnEnabled = env === 'production' || ['1', 'true', 'yes', 'on'].includes(
        String(process.env.SCALP_CANDLE_HISTORY_WARN_FULL_READS || '').trim().toLowerCase(),
    );
    if (!warnEnabled) return;
    logCandleHistory('full_history_read_warning', {
        symbol,
        timeframe,
        auditSource: auditSource || null,
        message: 'Full candle-history read requested; prefer range/tail/stats helpers.',
    });
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
    fallback: { symbol: string; timeframe: string; epic: string | null; source: ScalpCandleHistoryRecord['source'] },
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
        source: normalizeHistorySource(row.source, fallback.source),
        updatedAtMs: Number.isFinite(Number(row.updatedAtMs)) ? Number(row.updatedAtMs) : Date.now(),
        candles,
    };
}

function resolveBackend(preferred?: CandleHistoryBackend): CandleHistoryBackend {
    if (preferred === 'pg') return 'pg';
    const mode = String(process.env.CANDLE_HISTORY_STORE || 'auto')
        .trim()
        .toLowerCase();
    if (mode === 'pg') return 'pg';
    if (isScalpPgConfigured()) return 'pg';
    return 'pg';
}

function startOfUtcDay(tsMs: number): number {
    const date = new Date(tsMs);
    return Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), date.getUTCDate());
}

function weekStartMondayUtcMs(tsMs: number): number {
    const dayStartMs = startOfUtcDay(tsMs);
    const dayOfWeek = new Date(dayStartMs).getUTCDay();
    const daysSinceMonday = (dayOfWeek + 6) % 7;
    return dayStartMs - daysSinceMonday * ONE_DAY_MS;
}

function candlesToWeeklyRows(record: ScalpCandleHistoryRecord): Array<{
    symbol: string;
    timeframe: string;
    weekStartMs: number;
    epic: string | null;
    source: ScalpCandleHistoryRecord['source'];
    candles: ScalpCandle[];
    candleCount: number;
    firstTsMs: number | null;
    lastTsMs: number | null;
}> {
    const byWeek = new Map<number, ScalpCandle[]>();
    for (const candle of record.candles) {
        const ts = Number(candle?.[0] || 0);
        if (!Number.isFinite(ts) || ts <= 0) continue;
        const weekStartMs = weekStartMondayUtcMs(Math.floor(ts));
        const bucket = byWeek.get(weekStartMs) || [];
        bucket.push(candle);
        byWeek.set(weekStartMs, bucket);
    }
    return Array.from(byWeek.entries())
        .sort((a, b) => a[0] - b[0])
        .map(([weekStartMs, candles]) => {
            const weeklyCandles = dedupeSortCandles(candles);
            const candleCount = weeklyCandles.length;
            const firstTsMs = candleCount > 0 ? Number(weeklyCandles[0]?.[0]) : null;
            const lastTsMs = candleCount > 0 ? Number(weeklyCandles[candleCount - 1]?.[0]) : null;
            return {
                symbol: record.symbol,
                timeframe: record.timeframe,
                weekStartMs,
                epic: record.epic,
                source: record.source,
                candles: weeklyCandles,
                candleCount,
                firstTsMs: Number.isFinite(Number(firstTsMs)) ? Math.floor(Number(firstTsMs)) : null,
                lastTsMs: Number.isFinite(Number(lastTsMs)) ? Math.floor(Number(lastTsMs)) : null,
            };
        });
}

function rowsToRecord(params: {
    symbol: string;
    timeframe: string;
    rows: Array<{ epic: string | null; source: string | null; updatedAtMs: number | null; candles: unknown }>;
}): ScalpCandleHistoryRecord | null {
    if (!params.rows.length) return null;
    let latestUpdatedAtMs = 0;
    let epic: string | null = null;
    let source: ScalpCandleHistoryRecord['source'] = 'bitget';
    const merged: ScalpCandle[] = [];
    for (const row of params.rows) {
        const candles = Array.isArray(row.candles) ? row.candles : [];
        for (const candle of candles) {
            const normalized = normalizeCandleRow(candle);
            if (normalized) merged.push(normalized);
        }
        if (row.epic && !epic) {
            epic = String(row.epic).trim().toUpperCase();
        }
        if (row.source) {
            source = normalizeHistorySource(row.source, source);
        }
        const updatedAtMs = Number(row.updatedAtMs || 0);
        if (Number.isFinite(updatedAtMs) && updatedAtMs > latestUpdatedAtMs) {
            latestUpdatedAtMs = Math.floor(updatedAtMs);
        }
    }
    return {
        version: CANDLE_HISTORY_VERSION,
        symbol: params.symbol,
        timeframe: params.timeframe,
        epic,
        source,
        updatedAtMs: latestUpdatedAtMs > 0 ? latestUpdatedAtMs : Date.now(),
        candles: dedupeSortCandles(merged),
    };
}

function recordFromCandles(params: {
    symbol: string;
    timeframe: string;
    epic: string | null;
    source: ScalpCandleHistoryRecord['source'];
    candles: ScalpCandle[];
    updatedAtMs?: number;
}): ScalpCandleHistoryRecord | null {
    const candles = dedupeSortCandles(params.candles || []);
    if (!candles.length) return null;
    return {
        version: CANDLE_HISTORY_VERSION,
        symbol: params.symbol,
        timeframe: params.timeframe,
        epic: params.epic,
        source: params.source,
        updatedAtMs: Number.isFinite(Number(params.updatedAtMs)) ? Math.floor(Number(params.updatedAtMs)) : Date.now(),
        candles,
    };
}

function buildLoadResult(params: {
    symbol: string;
    timeframe: string;
    storageRef: string;
    record: ScalpCandleHistoryRecord | null;
    source: ScalpCandleHistorySource;
    fallbacksTried: string[];
    coverageRatio?: number;
}): ScalpCandleHistoryLoadResult {
    return {
        backend: 'pg',
        storageRef: params.storageRef,
        record: params.record,
        diagnostics: {
            source: params.source,
            fallbacksTried: params.fallbacksTried,
            coverageRatio: params.coverageRatio,
            storageRef: params.storageRef,
        },
    };
}

async function saveRecordToKv(record: ScalpCandleHistoryRecord): Promise<void> {
    const weekRows = candlesToWeeklyRows(record);
    await Promise.all(
        weekRows.map(async (row) => {
            const key = historyKvKey(row.symbol, row.timeframe, row.weekStartMs);
            const existing = await kvGetJson<ScalpCandleHistoryRecord>(key).catch(() => null);
            const existingCandles = Array.isArray(existing?.candles)
                ? existing.candles.map((entry) => normalizeCandleRow(entry)).filter((entry): entry is ScalpCandle => Boolean(entry))
                : [];
            const candles = dedupeSortCandles([...existingCandles, ...row.candles]);
            return kvSetJson(
                key,
                {
                    version: CANDLE_HISTORY_VERSION,
                    symbol: row.symbol,
                    timeframe: row.timeframe,
                    epic: row.epic || existing?.epic || null,
                    source: row.source,
                    updatedAtMs: Date.now(),
                    candles,
                } satisfies ScalpCandleHistoryRecord,
                candleHistoryKvTtlSeconds(),
            ).catch(() => null);
        }),
    );
}

async function loadFromPg(symbol: string, timeframe: string): Promise<ScalpCandleHistoryLoadResult> {
    const db = scalpPrisma();
    const rows = await db.$queryRaw<
        Array<{
            epic: string | null;
            source: string | null;
            updatedAtMs: bigint | number | null;
            candles: unknown;
        }>
    >(sql`
        SELECT
            epic,
            source,
            (EXTRACT(EPOCH FROM updated_at) * 1000)::bigint AS "updatedAtMs",
            candles_json AS candles
        FROM scalp_candle_history_weeks
        WHERE symbol = ${symbol}
          AND timeframe = ${timeframe}
        ORDER BY week_start ASC;
    `);
    const record = rowsToRecord({
        symbol,
        timeframe,
        rows: rows.map((row) => ({
            epic: row.epic,
            source: row.source,
            updatedAtMs: Number(row.updatedAtMs || 0),
            candles: row.candles,
        })),
    });
    return {
        backend: 'pg',
        storageRef: `scalp_candle_history_weeks:${symbol}:${timeframe}`,
        record,
    };
}

async function loadFromPgRange(
    symbol: string,
    timeframe: string,
    fromTsMs: number,
    toTsMs: number,
): Promise<ScalpCandleHistoryLoadResult> {
    const fromMs = Math.max(0, Math.floor(Number(fromTsMs) || 0));
    const toMsRaw = Math.max(0, Math.floor(Number(toTsMs) || 0));
    const toMs = Math.max(fromMs + 1, toMsRaw);
    const db = scalpPrisma();
    const rows = await db.$queryRaw<
        Array<{
            epic: string | null;
            source: string | null;
            updatedAtMs: bigint | number | null;
            candles: unknown;
        }>
    >(sql`
        SELECT
            epic,
            source,
            (EXTRACT(EPOCH FROM updated_at) * 1000)::bigint AS "updatedAtMs",
            candles_json AS candles
        FROM scalp_candle_history_weeks
        WHERE symbol = ${symbol}
          AND timeframe = ${timeframe}
          AND week_start < to_timestamp(${toMs} / 1000.0)
          AND (week_start + interval '7 day') > to_timestamp(${fromMs} / 1000.0)
        ORDER BY week_start ASC;
    `);
    const record = rowsToRecord({
        symbol,
        timeframe,
        rows: rows.map((row) => ({
            epic: row.epic,
            source: row.source,
            updatedAtMs: Number(row.updatedAtMs || 0),
            candles: row.candles,
        })),
    });
    return {
        backend: 'pg',
        storageRef: `scalp_candle_history_weeks:${symbol}:${timeframe}:${fromMs}-${toMs}`,
        record,
    };
}

async function loadFromPgTail(
    symbol: string,
    timeframe: string,
    candleLimit: number,
): Promise<ScalpCandleHistoryLoadResult> {
    const limit = Math.max(1, Math.floor(Number(candleLimit) || 1));
    const tfMs = Math.max(1, timeframeToMs(timeframe));
    const weekLimit = Math.max(1, Math.min(260, Math.ceil((limit * tfMs) / ONE_WEEK_MS) + 1));
    const db = scalpPrisma();
    const rows = await db.$queryRaw<
        Array<{
            epic: string | null;
            source: string | null;
            updatedAtMs: bigint | number | null;
            weekStartMs: bigint | number | null;
            candles: unknown;
        }>
    >(sql`
        SELECT *
        FROM (
            SELECT
                epic,
                source,
                (EXTRACT(EPOCH FROM updated_at) * 1000)::bigint AS "updatedAtMs",
                (EXTRACT(EPOCH FROM week_start) * 1000)::bigint AS "weekStartMs",
                candles_json AS candles
            FROM scalp_candle_history_weeks
            WHERE symbol = ${symbol}
              AND timeframe = ${timeframe}
            ORDER BY week_start DESC
            LIMIT ${weekLimit}
        ) recent
        ORDER BY "weekStartMs" ASC;
    `);
    const record = rowsToRecord({
        symbol,
        timeframe,
        rows: rows.map((row) => ({
            epic: row.epic,
            source: row.source,
            updatedAtMs: Number(row.updatedAtMs || 0),
            candles: row.candles,
        })),
    });
    return {
        backend: 'pg',
        storageRef: `scalp_candle_history_weeks:${symbol}:${timeframe}:tail:${limit}`,
        record: record ? { ...record, candles: record.candles.slice(-limit) } : null,
    };
}

async function loadFromKvRange(
    symbol: string,
    timeframe: string,
    fromTsMs: number,
    toTsMs: number,
): Promise<ScalpCandleHistoryLoadResult> {
    const weekStarts = weekStartsForRange(fromTsMs, toTsMs);
    const keys = weekStarts.map((weekStartMs) => historyKvKey(symbol, timeframe, weekStartMs));
    const rows = keys.length === 1
        ? [await kvGetJson<ScalpCandleHistoryRecord>(keys[0]!)]
        : await kvMGetJson<ScalpCandleHistoryRecord>(keys);
    const record = rowsToRecord({
        symbol,
        timeframe,
        rows: rows
            .map((row) => ({
                epic: row?.epic || null,
                source: row?.source || null,
                updatedAtMs: row?.updatedAtMs || null,
                candles: row?.candles || [],
            })),
    });
    return {
        backend: 'pg',
        storageRef: `kv:${symbol}:${timeframe}:${weekStarts[0] ?? 0}-${weekStarts.at(-1) ?? 0}`,
        record: record ? { ...record, candles: filterCandlesInRange(record.candles, fromTsMs, toTsMs) } : null,
    };
}

async function loadFromBrokerRange(
    symbol: string,
    timeframe: string,
    fromTsMs: number,
    toTsMs: number,
    opts: ScalpCandleHistoryReadOptions,
): Promise<ScalpCandleHistoryLoadResult> {
    const venue = opts.venue ? normalizeVenue(opts.venue) : inferVenueForSymbol(symbol);
    const fromMs = Math.max(0, Math.floor(Number(fromTsMs) || 0));
    const toMs = Math.max(fromMs + 1, Math.floor(Number(toTsMs) || 0));
    const maxDays = Math.max(1, Math.floor(Number(opts.maxBrokerRangeDays || DEFAULT_MAX_BROKER_RANGE_DAYS)));
    if (toMs - fromMs > maxDays * ONE_DAY_MS) {
        throw new Error(`broker_range_too_wide:${maxDays}d`);
    }
    if (venue === 'capital') {
        const resolved = await resolveCapitalEpicRuntime(symbol);
        const rows = await fetchCapitalCandlesByEpicDateRange(resolved.epic, timeframe, fromMs, toMs, {
            maxPerRequest: 1000,
            maxRequests: Math.max(20, Math.ceil((toMs - fromMs) / Math.max(1, timeframeToMs(timeframe)) / 900)),
        });
        const candles = filterCandlesInRange(
            dedupeSortCandles(rows.map((row) => normalizeCandleRow(row)).filter((row): row is ScalpCandle => Boolean(row))),
            fromMs,
            toMs,
        );
        return {
            backend: 'pg',
            storageRef: `broker:capital:${symbol}:${timeframe}:${fromMs}-${toMs}`,
            record: recordFromCandles({
                symbol,
                timeframe,
                epic: resolved.epic,
                source: 'capital',
                candles,
            }),
        };
    }
    const candles = filterCandlesInRange(
        await fetchBitgetCandlesByEpicDateRange(symbol, timeframe, fromMs, toMs, {
            maxPerRequest: 200,
            maxRequests: Math.max(40, Math.ceil((toMs - fromMs) / Math.max(1, timeframeToMs(timeframe)) / 160)),
        }),
        fromMs,
        toMs,
    );
    return {
        backend: 'pg',
        storageRef: `broker:bitget:${symbol}:${timeframe}:${fromMs}-${toMs}`,
        record: recordFromCandles({
            symbol,
            timeframe,
            epic: symbol,
            source: 'bitget',
            candles,
        }),
    };
}

async function loadFromPgBulk(symbols: string[], timeframe: string): Promise<ScalpCandleHistoryLoadResult[]> {
    if (!symbols.length) return [];
    const db = scalpPrisma();
    const chunkSize = Math.max(
        1,
        Math.min(50, Math.floor(Number(process.env.SCALP_CANDLE_HISTORY_BULK_QUERY_SYMBOL_CHUNK) || 8)),
    );
    const grouped = new Map<string, Array<{ epic: string | null; source: string | null; updatedAtMs: number | null; candles: unknown }>>();
    for (let offset = 0; offset < symbols.length; offset += chunkSize) {
        const slice = symbols.slice(offset, offset + chunkSize);
        if (!slice.length) continue;
        const rows = await db.$queryRaw<
            Array<{
                symbol: string;
                epic: string | null;
                source: string | null;
                updatedAtMs: bigint | number | null;
                candles: unknown;
            }>
        >(sql`
            SELECT
                symbol,
                epic,
                source,
                (EXTRACT(EPOCH FROM updated_at) * 1000)::bigint AS "updatedAtMs",
                candles_json AS candles
            FROM scalp_candle_history_weeks
            WHERE timeframe = ${timeframe}
              AND symbol IN (${join(slice)})
            ORDER BY symbol ASC, week_start ASC;
        `);
        for (const row of rows) {
            const symbol = normalizeSymbol(row.symbol);
            if (!symbol) continue;
            const bucket = grouped.get(symbol) || [];
            bucket.push({
                epic: row.epic,
                source: row.source,
                updatedAtMs: Number(row.updatedAtMs || 0),
                candles: row.candles,
            });
            grouped.set(symbol, bucket);
        }
    }

    return symbols.map((symbol) => {
        const record = rowsToRecord({
            symbol,
            timeframe,
            rows: grouped.get(symbol) || [],
        });
        return {
            backend: 'pg' as const,
            storageRef: `scalp_candle_history_weeks:${symbol}:${timeframe}`,
            record,
        };
    });
}

async function loadFromPgStatsBulk(symbols: string[], timeframe: string): Promise<ScalpCandleHistoryStatsLoadResult[]> {
    if (!symbols.length) return [];
    const db = scalpPrisma();
    const chunkSize = Math.max(
        1,
        Math.min(400, Math.floor(Number(process.env.SCALP_CANDLE_HISTORY_STATS_BULK_QUERY_SYMBOL_CHUNK) || 200)),
    );
    const grouped = new Map<
        string,
        {
            epic: string | null;
            updatedAtMs: number | null;
            candleCount: number;
            fromTsMs: number | null;
            toTsMs: number | null;
        }
    >();

    for (let offset = 0; offset < symbols.length; offset += chunkSize) {
        const slice = symbols.slice(offset, offset + chunkSize);
        if (!slice.length) continue;
        const rows = await db.$queryRaw<
            Array<{
                symbol: string;
                epic: string | null;
                updatedAtMs: bigint | number | null;
                candleCount: bigint | number | null;
                fromTsMs: bigint | number | null;
                toTsMs: bigint | number | null;
            }>
        >(sql`
            SELECT
                symbol,
                NULLIF(MAX(TRIM(COALESCE(epic, ''))), '') AS epic,
                MAX((EXTRACT(EPOCH FROM updated_at) * 1000)::bigint) AS "updatedAtMs",
                COALESCE(SUM(candle_count), 0)::bigint AS "candleCount",
                MIN(first_ts_ms) AS "fromTsMs",
                MAX(last_ts_ms) AS "toTsMs"
            FROM scalp_candle_history_weeks
            WHERE timeframe = ${timeframe}
              AND symbol IN (${join(slice)})
            GROUP BY symbol;
        `);
        for (const row of rows) {
            const symbol = normalizeSymbol(row.symbol);
            if (!symbol) continue;
            grouped.set(symbol, {
                epic: row.epic ? String(row.epic).trim().toUpperCase() : null,
                updatedAtMs: Number.isFinite(Number(row.updatedAtMs)) ? Math.floor(Number(row.updatedAtMs)) : null,
                candleCount: Number.isFinite(Number(row.candleCount)) ? Math.max(0, Math.floor(Number(row.candleCount))) : 0,
                fromTsMs: Number.isFinite(Number(row.fromTsMs)) ? Math.floor(Number(row.fromTsMs)) : null,
                toTsMs: Number.isFinite(Number(row.toTsMs)) ? Math.floor(Number(row.toTsMs)) : null,
            });
        }
    }

    return symbols.map((symbol) => {
        const stats = grouped.get(symbol);
        return {
            backend: 'pg' as const,
            storageRef: `scalp_candle_history_weeks:stats:${symbol}:${timeframe}`,
            symbol,
            timeframe,
            epic: stats?.epic || null,
            updatedAtMs: stats?.updatedAtMs ?? null,
            candleCount: stats?.candleCount ?? 0,
            fromTsMs: stats?.fromTsMs ?? null,
            toTsMs: stats?.toTsMs ?? null,
        };
    });
}

async function loadFromPgFreshnessStats(
    symbol: string,
    timeframe: string,
    fromTsMs: number,
    toTsMs: number,
): Promise<ScalpCandleHistoryFreshnessStats> {
    const fromMs = Math.max(0, Math.floor(Number(fromTsMs) || 0));
    const toMs = Math.max(fromMs + 1, Math.floor(Number(toTsMs) || 0));
    const db = scalpPrisma();
    const rows = await db.$queryRaw<
        Array<{
            weekStartMs: bigint | number | null;
            candleCount: bigint | number | null;
            firstTsMs: bigint | number | null;
            lastTsMs: bigint | number | null;
        }>
    >(sql`
        SELECT
            (EXTRACT(EPOCH FROM week_start) * 1000)::bigint AS "weekStartMs",
            candle_count::bigint AS "candleCount",
            first_ts_ms::bigint AS "firstTsMs",
            last_ts_ms::bigint AS "lastTsMs"
        FROM scalp_candle_history_weeks
        WHERE symbol = ${symbol}
          AND timeframe = ${timeframe}
          AND week_start < to_timestamp(${toMs} / 1000.0)
          AND (week_start + interval '7 day') > to_timestamp(${fromMs} / 1000.0)
          AND candle_count > 0
        ORDER BY week_start ASC;
    `);
    const tfMs = Math.max(1, timeframeToMs(timeframe));
    let latestCandleTsMs: number | null = null;
    let candlesInWindow = 0;
    for (const row of rows) {
        const weekStartMs = Number(row.weekStartMs || 0);
        const firstTsMs = Number(row.firstTsMs || 0);
        const lastTsMs = Number(row.lastTsMs || 0);
        const candleCount = Math.max(0, Math.floor(Number(row.candleCount || 0)));
        if (!Number.isFinite(weekStartMs) || !Number.isFinite(firstTsMs) || !Number.isFinite(lastTsMs) || candleCount <= 0) {
            continue;
        }
        const rowFromMs = Math.max(fromMs, firstTsMs);
        const rowToInclusiveMs = Math.min(toMs - 1, lastTsMs);
        if (rowToInclusiveMs < rowFromMs) continue;
        const startIndex = Math.max(0, Math.ceil((rowFromMs - firstTsMs) / tfMs));
        const endIndex = Math.min(candleCount - 1, Math.floor((rowToInclusiveMs - firstTsMs) / tfMs));
        if (endIndex < startIndex) continue;
        const latestInRowMs = rowToInclusiveMs;
        latestCandleTsMs =
            latestCandleTsMs === null
                ? latestInRowMs
                : Math.max(latestCandleTsMs, latestInRowMs);
        const coversWholeStoredWeek = fromMs <= firstTsMs && toMs > lastTsMs;
        candlesInWindow += coversWholeStoredWeek
            ? candleCount
            : endIndex - startIndex + 1;
    }
    return {
        symbol,
        timeframe,
        fromTsMs: fromMs,
        toTsMs: toMs,
        latestCandleTsMs,
        candlesInWindow,
        storageRef: `scalp_candle_history_weeks:freshness:${symbol}:${timeframe}:${fromMs}-${toMs}`,
        diagnostics: {
            source: 'pg',
            fallbacksTried: ['pg'],
            storageRef: `scalp_candle_history_weeks:freshness:${symbol}:${timeframe}:${fromMs}-${toMs}`,
        },
    };
}

async function saveToPg(record: ScalpCandleHistoryRecord): Promise<ScalpCandleHistorySaveResult> {
    await saveToPgBulk([record]);
    return {
        backend: 'pg',
        storageRef: `scalp_candle_history_weeks:${record.symbol}:${record.timeframe}`,
        saved: true,
    };
}

async function saveToPgBulk(records: ScalpCandleHistoryRecord[]): Promise<ScalpCandleHistoryBulkSaveResult> {
    if (!records.length) {
        return { backend: 'pg', saved: 0, storageRef: 'scalp_candle_history_weeks:bulk' };
    }
    const db = scalpPrisma();
    const fullReplaceEnabled = ['1', 'true', 'yes', 'on'].includes(
        String(process.env.SCALP_CANDLE_HISTORY_FULL_REPLACE || '')
            .trim()
            .toLowerCase(),
    );
    const chunkSize = Math.max(
        1,
        Math.min(10, Math.floor(Number(process.env.SCALP_CANDLE_HISTORY_BULK_SAVE_RECORD_CHUNK) || 1)),
    );
    for (let offset = 0; offset < records.length; offset += chunkSize) {
        const slice = records.slice(offset, offset + chunkSize);
        if (!slice.length) continue;
        for (const record of slice) {
            const weekRows = candlesToWeeklyRows(record);
            if (fullReplaceEnabled) {
                await db.$executeRaw(sql`
                    DELETE FROM scalp_candle_history_weeks
                    WHERE symbol = ${record.symbol}
                      AND timeframe = ${record.timeframe};
                `);
            }
            for (const row of weekRows) {
                let candlesForWrite = row.candles;
                if (!fullReplaceEnabled) {
                    const existingWeekRows = await db.$queryRaw<
                        Array<{ candles: unknown }>
                    >(sql`
                        SELECT candles_json AS candles
                        FROM scalp_candle_history_weeks
                        WHERE symbol = ${row.symbol}
                          AND timeframe = ${row.timeframe}
                          AND week_start = to_timestamp(${row.weekStartMs} / 1000.0)
                        LIMIT 1;
                    `);
                    const existingWeekCandlesRaw = Array.isArray(existingWeekRows?.[0]?.candles)
                        ? (existingWeekRows[0].candles as unknown[])
                        : [];
                    if (existingWeekCandlesRaw.length > 0) {
                        const existingWeekCandles = existingWeekCandlesRaw
                            .map((entry) => normalizeCandleRow(entry))
                            .filter((entry): entry is ScalpCandle => Boolean(entry));
                        candlesForWrite = dedupeSortCandles([
                            ...existingWeekCandles,
                            ...row.candles,
                        ]);
                    }
                }
                const candleCountForWrite = candlesForWrite.length;
                const firstTsMsForWrite = candleCountForWrite > 0 ? Number(candlesForWrite[0]?.[0]) : null;
                const lastTsMsForWrite =
                    candleCountForWrite > 0 ? Number(candlesForWrite[candleCountForWrite - 1]?.[0]) : null;
                await db.$executeRaw(
                    sql`
                        INSERT INTO scalp_candle_history_weeks(
                            symbol,
                            timeframe,
                            week_start,
                            epic,
                            source,
                            candles_json,
                            candle_count,
                            first_ts_ms,
                            last_ts_ms,
                            updated_at
                        )
                        VALUES(
                            ${row.symbol},
                            ${row.timeframe},
                            to_timestamp(${row.weekStartMs} / 1000.0),
                            ${row.epic},
                            ${row.source},
                            ${JSON.stringify(candlesForWrite)}::jsonb,
                            ${candleCountForWrite},
                            ${Number.isFinite(Number(firstTsMsForWrite)) ? Math.floor(Number(firstTsMsForWrite)) : null},
                            ${Number.isFinite(Number(lastTsMsForWrite)) ? Math.floor(Number(lastTsMsForWrite)) : null},
                            NOW()
                        )
                        ON CONFLICT(symbol, timeframe, week_start)
                        DO UPDATE SET
                            epic = EXCLUDED.epic,
                            source = EXCLUDED.source,
                            candles_json = EXCLUDED.candles_json,
                            candle_count = EXCLUDED.candle_count,
                            first_ts_ms = EXCLUDED.first_ts_ms,
                            last_ts_ms = EXCLUDED.last_ts_ms,
                            updated_at = NOW();
                    `,
                );
                await kvSetJson(
                    historyKvKey(row.symbol, row.timeframe, row.weekStartMs),
                    {
                        version: CANDLE_HISTORY_VERSION,
                        symbol: row.symbol,
                        timeframe: row.timeframe,
                        epic: row.epic,
                        source: row.source,
                        updatedAtMs: Date.now(),
                        candles: candlesForWrite,
                    } satisfies ScalpCandleHistoryRecord,
                ).catch(() => null);
            }
        }
    }

    return {
        backend: 'pg',
        saved: records.length,
        storageRef: 'scalp_candle_history_weeks:bulk',
    };
}

async function listHistorySymbolsFromPg(timeframe: string): Promise<string[]> {
    const db = scalpPrisma();
    const rows = await db.$queryRaw<Array<{ symbol: string }>>(sql`
        SELECT DISTINCT symbol
        FROM scalp_candle_history_weeks
        WHERE timeframe = ${timeframe}
        ORDER BY symbol ASC;
    `);
    return Array.from(
        new Set(
            rows
                .map((row) => normalizeSymbol(row.symbol))
                .filter((row) => Boolean(row)),
        ),
    ).sort();
}

export async function loadScalpCandleHistory(
    symbolRaw: string,
    timeframeRaw: string,
    opts: ScalpCandleHistoryReadOptions = {},
): Promise<ScalpCandleHistoryLoadResult> {
    const symbol = normalizeSymbol(symbolRaw);
    const timeframe = normalizeTimeframe(timeframeRaw);
    if (!symbol) {
        throw new Error('Invalid candle-history symbol');
    }
    const backend = resolveBackend(opts.backend);
    if (!isScalpPgConfigured()) {
        throw new Error('scalp_pg_not_configured_for_candle_history');
    }
    void backend;
    maybeWarnFullHistoryRead(symbol, timeframe, opts.auditSource);
    const result = await loadFromPg(symbol, timeframe);
    return buildLoadResult({
        symbol,
        timeframe,
        storageRef: result.storageRef,
        record: result.record,
        source: 'pg',
        fallbacksTried: ['pg'],
    });
}

export async function loadScalpCandleHistoryInRange(
    symbolRaw: string,
    timeframeRaw: string,
    fromTsMs: number,
    toTsMs: number,
    opts: ScalpCandleHistoryReadOptions = {},
): Promise<ScalpCandleHistoryLoadResult> {
    return loadScalpCandleHistoryRange(symbolRaw, timeframeRaw, fromTsMs, toTsMs, opts);
}

export async function loadScalpCandleHistoryRange(
    symbolRaw: string,
    timeframeRaw: string,
    fromTsMs: number,
    toTsMs: number,
    opts: ScalpCandleHistoryReadOptions = {},
): Promise<ScalpCandleHistoryLoadResult> {
    const symbol = normalizeSymbol(symbolRaw);
    const timeframe = normalizeTimeframe(timeframeRaw);
    if (!symbol) {
        throw new Error('Invalid candle-history symbol');
    }
    const fromMs = Math.max(0, Math.floor(Number(fromTsMs) || 0));
    const toMs = Math.max(fromMs + 1, Math.floor(Number(toTsMs) || 0));
    const readOrder = normalizeReadOrder(opts.readOrder);
    const requireCoverageRatio = Math.max(
        0,
        Math.min(1, Number(opts.requireCoverageRatio ?? DEFAULT_REQUIRE_COVERAGE_RATIO)),
    );
    const fallbacksTried: string[] = [];
    let lastResult: ScalpCandleHistoryLoadResult | null = null;
    let bestResult: ScalpCandleHistoryLoadResult | null = null;
    let bestCoverageRatio = -1;
    for (const source of readOrder) {
        if (source === 'pg' && !isScalpPgConfigured()) continue;
        try {
            fallbacksTried.push(source);
            const result =
                source === 'broker'
                    ? await loadFromBrokerRange(symbol, timeframe, fromMs, toMs, opts)
                    : source === 'kv'
                      ? await loadFromKvRange(symbol, timeframe, fromMs, toMs)
                      : await loadFromPgRange(symbol, timeframe, fromMs, toMs);
            const candles = filterCandlesInRange(result.record?.candles || [], fromMs, toMs);
            const coverageRatio = coverageRatioFor(candles, fromMs, toMs, timeframe);
            const normalizedRecord = result.record ? { ...result.record, candles } : null;
            lastResult = buildLoadResult({
                symbol,
                timeframe,
                storageRef: result.storageRef,
                record: normalizedRecord,
                source,
                fallbacksTried: [...fallbacksTried],
                coverageRatio,
            });
            if (coverageRatio > bestCoverageRatio) {
                bestCoverageRatio = coverageRatio;
                bestResult = lastResult;
            }
            if (normalizedRecord && candles.length > 0 && coverageRatio >= requireCoverageRatio) {
                if (source === 'broker') void saveRecordToKv(normalizedRecord).catch(() => null);
                logCandleHistory('range_loaded', {
                    auditSource: opts.auditSource || null,
                    source,
                    symbol,
                    timeframe,
                    fromTsMs: fromMs,
                    toTsMs: toMs,
                    candles: candles.length,
                    coverageRatio,
                    storageRef: result.storageRef,
                });
                return lastResult;
            }
        } catch (err) {
            logCandleHistory('range_source_failed', {
                auditSource: opts.auditSource || null,
                source,
                symbol,
                timeframe,
                fromTsMs: fromMs,
                toTsMs: toMs,
                message: err instanceof Error ? err.message : String(err),
            });
        }
    }
    return bestResult || lastResult || buildLoadResult({
        symbol,
        timeframe,
        storageRef: `missing:${symbol}:${timeframe}:${fromMs}-${toMs}`,
        record: null,
        source: 'pg',
        fallbacksTried,
        coverageRatio: 0,
    });
}

export async function loadScalpCandleHistoryTail(
    symbolRaw: string,
    timeframeRaw: string,
    candleLimit: number,
    opts: ScalpCandleHistoryReadOptions = {},
): Promise<ScalpCandleHistoryLoadResult> {
    const symbol = normalizeSymbol(symbolRaw);
    const timeframe = normalizeTimeframe(timeframeRaw);
    if (!symbol) throw new Error('Invalid candle-history symbol');
    const limit = Math.max(1, Math.floor(Number(candleLimit) || 1));
    const tfMs = Math.max(1, timeframeToMs(timeframe));
    const toMs = Date.now();
    const fromMs = Math.max(0, toMs - (limit + 20) * tfMs);
    const range = await loadScalpCandleHistoryRange(symbol, timeframe, fromMs, toMs, {
        ...opts,
        requireCoverageRatio: opts.requireCoverageRatio ?? 0.2,
    });
    if (!(range.record?.candles?.length) && isScalpPgConfigured()) {
        const pgTail = await loadFromPgTail(symbol, timeframe, limit);
        return buildLoadResult({
            symbol,
            timeframe,
            storageRef: `${pgTail.storageRef}:tail:${limit}`,
            record: pgTail.record,
            source: 'pg',
            fallbacksTried: [...(range.diagnostics?.fallbacksTried || []), 'pg_tail'],
            coverageRatio: pgTail.record?.candles?.length ? 1 : 0,
        });
    }
    const candles = (range.record?.candles || []).slice(-limit);
    return {
        ...range,
        storageRef: `${range.storageRef}:tail:${limit}`,
        record: range.record ? { ...range.record, candles } : null,
        diagnostics: range.diagnostics
            ? { ...range.diagnostics, storageRef: `${range.storageRef}:tail:${limit}` }
            : undefined,
    };
}

export async function loadScalpCandleHistoryFreshnessStats(
    symbolRaw: string,
    timeframeRaw: string,
    fromTsMs: number,
    toTsMs: number,
    opts: ScalpCandleHistoryReadOptions = {},
): Promise<ScalpCandleHistoryFreshnessStats> {
    const symbol = normalizeSymbol(symbolRaw);
    const timeframe = normalizeTimeframe(timeframeRaw);
    if (!symbol) throw new Error('Invalid candle-history symbol');
    if (!isScalpPgConfigured()) throw new Error('scalp_pg_not_configured_for_candle_history');
    void opts;
    return loadFromPgFreshnessStats(symbol, timeframe, fromTsMs, toTsMs);
}

export async function loadScalpCandleHistoryWeeklyBars(
    symbolRaw: string,
    timeframeRaw: string,
    fromTsMs: number,
    toTsMs: number,
    opts: ScalpCandleHistoryReadOptions = {},
): Promise<ScalpCandleHistoryLoadResult> {
    return loadScalpCandleHistoryRange(symbolRaw, timeframeRaw, fromTsMs, toTsMs, {
        ...opts,
        requireCoverageRatio: opts.requireCoverageRatio ?? 0.2,
    });
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
        source: normalizeHistorySource(recordRaw.source, 'bitget'),
        updatedAtMs: Date.now(),
        candles: dedupeSortCandles(recordRaw.candles || []),
    };
    const backend = resolveBackend(opts.backend);
    if (!isScalpPgConfigured()) {
        throw new Error('scalp_pg_not_configured_for_candle_history');
    }
    void backend;
    return saveToPg(record);
}

export async function loadScalpCandleHistoryBulk(
    symbolsRaw: string[],
    timeframeRaw: string,
    opts: { backend?: CandleHistoryBackend } = {},
): Promise<ScalpCandleHistoryLoadResult[]> {
    const symbols = Array.from(
        new Set(
            (symbolsRaw || [])
                .map((symbol) => normalizeSymbol(symbol))
                .filter((symbol) => Boolean(symbol)),
        ),
    );
    const timeframe = normalizeTimeframe(timeframeRaw);
    const backend = resolveBackend(opts.backend);
    if (!isScalpPgConfigured()) {
        throw new Error('scalp_pg_not_configured_for_candle_history');
    }
    void backend;
    return loadFromPgBulk(symbols, timeframe);
}

export async function loadScalpCandleHistoryStatsBulk(
    symbolsRaw: string[],
    timeframeRaw: string,
    opts: { backend?: CandleHistoryBackend } = {},
): Promise<ScalpCandleHistoryStatsLoadResult[]> {
    const symbols = Array.from(
        new Set(
            (symbolsRaw || [])
                .map((symbol) => normalizeSymbol(symbol))
                .filter((symbol) => Boolean(symbol)),
        ),
    );
    const timeframe = normalizeTimeframe(timeframeRaw);
    const backend = resolveBackend(opts.backend);
    if (!isScalpPgConfigured()) {
        throw new Error('scalp_pg_not_configured_for_candle_history');
    }
    void backend;
    return loadFromPgStatsBulk(symbols, timeframe);
}

export async function saveScalpCandleHistoryBulk(
    recordsRaw: Array<
        Omit<ScalpCandleHistoryRecord, 'version' | 'updatedAtMs' | 'candles'> & {
            candles: ScalpCandle[];
        }
    >,
    opts: { backend?: CandleHistoryBackend } = {},
): Promise<ScalpCandleHistoryBulkSaveResult> {
    const records = (recordsRaw || [])
        .map((recordRaw) => {
            const symbol = normalizeSymbol(recordRaw.symbol);
            const timeframe = normalizeTimeframe(recordRaw.timeframe);
            if (!symbol) return null;
            const normalized = normalizeRecord(
                {
                    symbol,
                    timeframe,
                    epic: recordRaw.epic,
                    source: normalizeHistorySource(recordRaw.source, 'bitget'),
                    updatedAtMs: Date.now(),
                    candles: recordRaw.candles || [],
                },
                { symbol, timeframe, epic: null, source: 'bitget' },
            );
            return normalized;
        })
        .filter((row): row is ScalpCandleHistoryRecord => Boolean(row));

    const backend = resolveBackend(opts.backend);
    if (!isScalpPgConfigured()) {
        throw new Error('scalp_pg_not_configured_for_candle_history');
    }
    void backend;
    return saveToPgBulk(records);
}

export async function listScalpCandleHistorySymbols(
    timeframeRaw = '1m',
    opts: { backend?: CandleHistoryBackend } = {},
): Promise<string[]> {
    const timeframe = normalizeTimeframe(timeframeRaw);
    const backend = resolveBackend(opts.backend);
    if (!isScalpPgConfigured()) {
        throw new Error('scalp_pg_not_configured_for_candle_history');
    }
    void backend;
    return listHistorySymbolsFromPg(timeframe);
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
    return amount * ONE_WEEK_MS;
}

export function normalizeHistoryTimeframe(value: string): string {
    return normalizeTimeframe(value);
}
