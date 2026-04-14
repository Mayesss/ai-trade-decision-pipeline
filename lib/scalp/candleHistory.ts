import { empty, join, raw, sql } from './pg/sql';

import { isScalpPgConfigured, scalpPrisma } from './pg/client';
import type { ScalpCandle } from './types';

export type CandleHistoryBackend = 'pg';

export interface ScalpCandleHistoryRecord {
    version: 1;
    symbol: string;
    timeframe: string;
    epic: string | null;
    source: 'bitget';
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

function normalizeHistorySource(value: unknown, fallback: ScalpCandleHistoryRecord['source'] = 'bitget'): ScalpCandleHistoryRecord['source'] {
    const normalized = String(value || '')
        .trim()
        .toLowerCase();
    if (normalized === 'bitget') return 'bitget';
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

export async function loadScalpCandleHistoryInRange(
    symbolRaw: string,
    timeframeRaw: string,
    fromTsMs: number,
    toTsMs: number,
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
    return loadFromPgRange(symbol, timeframe, fromTsMs, toTsMs);
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
