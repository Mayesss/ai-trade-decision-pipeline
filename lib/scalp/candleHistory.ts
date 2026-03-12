import { Prisma } from '@prisma/client';

import { isScalpPgConfigured, scalpPrisma } from './pg/client';
import type { ScalpCandle } from './types';

export type CandleHistoryBackend = 'pg';

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
    source: 'capital';
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
        source: 'capital',
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
    const weekRows = records.flatMap((record) => candlesToWeeklyRows(record));
    const pairRows = records.map((record) => ({ symbol: record.symbol, timeframe: record.timeframe }));
    const db = scalpPrisma();
    const pairJson = JSON.stringify(pairRows);
    const weekJson = JSON.stringify(
        weekRows.map((row) => ({
            symbol: row.symbol,
            timeframe: row.timeframe,
            week_start_ms: row.weekStartMs,
            epic: row.epic,
            source: row.source,
            candles: row.candles,
        })),
    );

    await db.$executeRaw(
        Prisma.sql`
            WITH pairs AS (
                SELECT DISTINCT
                    UPPER(TRIM(x.symbol)) AS symbol,
                    LOWER(TRIM(x.timeframe)) AS timeframe
                FROM jsonb_to_recordset(${pairJson}::jsonb) AS x(symbol text, timeframe text)
            ),
            input AS (
                SELECT
                    UPPER(TRIM(x.symbol)) AS symbol,
                    LOWER(TRIM(x.timeframe)) AS timeframe,
                    x.week_start_ms::bigint AS week_start_ms,
                    NULLIF(UPPER(TRIM(COALESCE(x.epic, ''))), '') AS epic,
                    COALESCE(NULLIF(TRIM(x.source), ''), 'capital') AS source,
                    COALESCE(x.candles, '[]'::jsonb) AS candles
                FROM jsonb_to_recordset(${weekJson}::jsonb) AS x(
                    symbol text,
                    timeframe text,
                    week_start_ms bigint,
                    epic text,
                    source text,
                    candles jsonb
                )
            ),
            upserted AS (
                INSERT INTO scalp_candle_history_weeks(symbol, timeframe, week_start, epic, source, candles_json, updated_at)
                SELECT
                    i.symbol,
                    i.timeframe,
                    to_timestamp(i.week_start_ms / 1000.0),
                    i.epic,
                    i.source,
                    i.candles,
                    NOW()
                FROM input i
                ON CONFLICT(symbol, timeframe, week_start)
                DO UPDATE SET
                    epic = EXCLUDED.epic,
                    source = EXCLUDED.source,
                    candles_json = EXCLUDED.candles_json,
                    updated_at = NOW()
                RETURNING 1
            )
            DELETE FROM scalp_candle_history_weeks w
            USING pairs p
            WHERE w.symbol = p.symbol
              AND w.timeframe = p.timeframe
              AND NOT EXISTS (
                  SELECT 1
                  FROM input i
                  WHERE i.symbol = w.symbol
                    AND i.timeframe = w.timeframe
                    AND i.week_start_ms = (EXTRACT(EPOCH FROM w.week_start) * 1000)::bigint
              );
        `,
    );

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
        source: 'capital',
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
                    source: 'capital',
                    updatedAtMs: Date.now(),
                    candles: recordRaw.candles || [],
                },
                { symbol, timeframe, epic: null },
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
