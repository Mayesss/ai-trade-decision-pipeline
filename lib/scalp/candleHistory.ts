import { Prisma } from '@prisma/client';

import { isScalpPgConfigured, scalpPrisma } from './pg/client';
import type { ScalpCandle } from './types';

export type CandleHistoryBackend = 'pg';

export interface ScalpCandleHistoryRecord {
    version: 1;
    symbol: string;
    timeframe: string;
    epic: string | null;
    source: 'capital' | 'bitget';
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

function normalizeHistorySource(value: unknown, fallback: ScalpCandleHistoryRecord['source'] = 'capital'): ScalpCandleHistoryRecord['source'] {
    const normalized = String(value || '')
        .trim()
        .toLowerCase();
    if (normalized === 'bitget') return 'bitget';
    if (normalized === 'capital') return 'capital';
    return fallback;
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
        .map(([weekStartMs, candles]) => ({
            symbol: record.symbol,
            timeframe: record.timeframe,
            weekStartMs,
            epic: record.epic,
            source: record.source,
            candles: dedupeSortCandles(candles),
        }));
}

function rowsToRecord(params: {
    symbol: string;
    timeframe: string;
    rows: Array<{ epic: string | null; source: string | null; updatedAtMs: number | null; candles: unknown }>;
}): ScalpCandleHistoryRecord | null {
    if (!params.rows.length) return null;
    let latestUpdatedAtMs = 0;
    let epic: string | null = null;
    let source: ScalpCandleHistoryRecord['source'] = 'capital';
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

async function loadFromPg(symbol: string, timeframe: string): Promise<ScalpCandleHistoryLoadResult> {
    const db = scalpPrisma();
    const rows = await db.$queryRaw<
        Array<{
            epic: string | null;
            source: string | null;
            updatedAtMs: bigint | number | null;
            candles: unknown;
        }>
    >(Prisma.sql`
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
        >(Prisma.sql`
            SELECT
                symbol,
                epic,
                source,
                (EXTRACT(EPOCH FROM updated_at) * 1000)::bigint AS "updatedAtMs",
                candles_json AS candles
            FROM scalp_candle_history_weeks
            WHERE timeframe = ${timeframe}
              AND symbol IN (${Prisma.join(slice)})
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
        >(Prisma.sql`
            SELECT
                symbol,
                NULLIF(MAX(TRIM(COALESCE(epic, ''))), '') AS epic,
                MAX((EXTRACT(EPOCH FROM updated_at) * 1000)::bigint) AS "updatedAtMs",
                COALESCE(SUM(jsonb_array_length(candles_json)), 0)::bigint AS "candleCount",
                MIN(
                    CASE
                        WHEN jsonb_array_length(candles_json) > 0
                             AND (candles_json -> 0 ->> 0) ~ '^[0-9]+$'
                        THEN (candles_json -> 0 ->> 0)::bigint
                        ELSE NULL
                    END
                ) AS "fromTsMs",
                MAX(
                    CASE
                        WHEN jsonb_array_length(candles_json) > 0
                             AND (candles_json -> (jsonb_array_length(candles_json) - 1) ->> 0) ~ '^[0-9]+$'
                        THEN (candles_json -> (jsonb_array_length(candles_json) - 1) ->> 0)::bigint
                        ELSE NULL
                    END
                ) AS "toTsMs"
            FROM scalp_candle_history_weeks
            WHERE timeframe = ${timeframe}
              AND symbol IN (${Prisma.join(slice)})
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
    const chunkSize = Math.max(
        1,
        Math.min(10, Math.floor(Number(process.env.SCALP_CANDLE_HISTORY_BULK_SAVE_RECORD_CHUNK) || 1)),
    );
    for (let offset = 0; offset < records.length; offset += chunkSize) {
        const slice = records.slice(offset, offset + chunkSize);
        if (!slice.length) continue;
        for (const record of slice) {
            const weekRows = candlesToWeeklyRows(record);
            await db.$executeRaw(Prisma.sql`
                DELETE FROM scalp_candle_history_weeks
                WHERE symbol = ${record.symbol}
                  AND timeframe = ${record.timeframe};
            `);
            for (const row of weekRows) {
                await db.$executeRaw(
                    Prisma.sql`
                        INSERT INTO scalp_candle_history_weeks(
                            symbol,
                            timeframe,
                            week_start,
                            epic,
                            source,
                            candles_json,
                            updated_at
                        )
                        VALUES(
                            ${row.symbol},
                            ${row.timeframe},
                            to_timestamp(${row.weekStartMs} / 1000.0),
                            ${row.epic},
                            ${row.source},
                            ${JSON.stringify(row.candles)}::jsonb,
                            NOW()
                        )
                        ON CONFLICT(symbol, timeframe, week_start)
                        DO UPDATE SET
                            epic = EXCLUDED.epic,
                            source = EXCLUDED.source,
                            candles_json = EXCLUDED.candles_json,
                            updated_at = NOW();
                    `,
                );
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
    const rows = await db.$queryRaw<Array<{ symbol: string }>>(Prisma.sql`
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
    opts: { backend?: CandleHistoryBackend } = {},
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
    return loadFromPg(symbol, timeframe);
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
        source: normalizeHistorySource(recordRaw.source, 'capital'),
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
                    source: normalizeHistorySource(recordRaw.source, 'capital'),
                    updatedAtMs: Date.now(),
                    candles: recordRaw.candles || [],
                },
                { symbol, timeframe, epic: null, source: 'capital' },
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
