import { Prisma } from '@prisma/client';

import { bitgetFetch, resolveProductType } from '../lib/bitget';
import { scalpPrisma } from '../lib/scalp/pg/client';
import type { ScalpCandle } from '../lib/scalp/types';

type ScriptOptions = {
    apply: boolean;
    timeframe: string;
    defaultLookbackDays: number;
    limitPerRequest: number;
    requestSpanMinutes: number;
    sleepMs: number;
    maxRequestsPerSymbol: number;
    onlySymbols: string[];
};

type CryptoWeekSymbolRow = {
    symbol: string;
    weekRows: number;
    candles: number;
    fromTsMs: number | null;
    toTsMs: number | null;
    sourcePreview: string[];
    assetCategory: string | null;
    instrumentType: string | null;
};

type ReplacementTarget = {
    targetSymbol: string;
    sourceSymbols: string[];
    mapReasons: string[];
    weekRows: number;
    candles: number;
    fromMs: number;
    toMs: number;
};

type WeeklyBucket = {
    weekStartMs: number;
    candles: ScalpCandle[];
};

const ONE_MINUTE_MS = 60_000;
const ONE_DAY_MS = 24 * 60 * 60 * 1000;

function parseArgs(argv: string[]): ScriptOptions {
    const opts: ScriptOptions = {
        apply: false,
        timeframe: '1m',
        defaultLookbackDays: 120,
        limitPerRequest: 1000,
        requestSpanMinutes: 2500,
        sleepMs: 25,
        maxRequestsPerSymbol: 2500,
        onlySymbols: [],
    };

    for (const arg of argv) {
        if (arg === '--apply') {
            opts.apply = true;
            continue;
        }
        if (!arg.startsWith('--')) continue;
        const [rawKey, rawValue = ''] = arg.split('=');
        const key = rawKey.trim();
        const value = rawValue.trim();
        if (!key) continue;

        if (key === '--timeframe' && value) {
            opts.timeframe = value.toLowerCase();
        } else if (key === '--defaultLookbackDays' && value) {
            const n = Number(value);
            if (Number.isFinite(n) && n > 0) opts.defaultLookbackDays = Math.max(1, Math.floor(n));
        } else if (key === '--limitPerRequest' && value) {
            const n = Number(value);
            if (Number.isFinite(n) && n > 0) opts.limitPerRequest = Math.max(20, Math.min(1000, Math.floor(n)));
        } else if (key === '--requestSpanMinutes' && value) {
            const n = Number(value);
            if (Number.isFinite(n) && n > 0) opts.requestSpanMinutes = Math.max(120, Math.floor(n));
        } else if (key === '--sleepMs' && value) {
            const n = Number(value);
            if (Number.isFinite(n) && n >= 0) opts.sleepMs = Math.max(0, Math.floor(n));
        } else if (key === '--maxRequestsPerSymbol' && value) {
            const n = Number(value);
            if (Number.isFinite(n) && n > 0) opts.maxRequestsPerSymbol = Math.max(100, Math.floor(n));
        } else if (key === '--onlySymbols' && value) {
            opts.onlySymbols = Array.from(
                new Set(
                    value
                        .split(',')
                        .map((row) => normalizeSymbol(row))
                        .filter((row) => Boolean(row)),
                ),
            );
        }
    }
    return opts;
}

function normalizeSymbol(value: unknown): string {
    return String(value || '')
        .trim()
        .toUpperCase()
        .replace(/[^A-Z0-9._-]/g, '');
}

function parseBitgetCandle(row: unknown): ScalpCandle | null {
    if (!Array.isArray(row)) return null;
    const ts = Number(row[0]);
    const open = Number(row[1]);
    const high = Number(row[2]);
    const low = Number(row[3]);
    const close = Number(row[4]);
    const volume = Number(row[5] ?? 0);
    if (![ts, open, high, low, close].every((n) => Number.isFinite(n) && n > 0)) return null;
    return [Math.floor(ts), open, high, low, close, Number.isFinite(volume) ? volume : 0];
}

function dedupeSortCandles(candles: ScalpCandle[]): ScalpCandle[] {
    const byTs = new Map<number, ScalpCandle>();
    for (const row of candles) {
        const parsed = parseBitgetCandle(row);
        if (!parsed) continue;
        byTs.set(parsed[0], parsed);
    }
    return Array.from(byTs.values()).sort((lhs, rhs) => lhs[0] - rhs[0]);
}

function weekStartMondayUtcMs(tsMs: number): number {
    const date = new Date(tsMs);
    const dayStart = Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), date.getUTCDate());
    const day = new Date(dayStart).getUTCDay();
    const sinceMonday = (day + 6) % 7;
    return dayStart - sinceMonday * ONE_DAY_MS;
}

function toWeeklyBuckets(candles: ScalpCandle[]): WeeklyBucket[] {
    const byWeek = new Map<number, ScalpCandle[]>();
    for (const candle of candles) {
        const ts = Number(candle?.[0] || 0);
        if (!Number.isFinite(ts) || ts <= 0) continue;
        const key = weekStartMondayUtcMs(ts);
        const bucket = byWeek.get(key) || [];
        bucket.push(candle);
        byWeek.set(key, bucket);
    }

    return Array.from(byWeek.entries())
        .sort((lhs, rhs) => lhs[0] - rhs[0])
        .map(([weekStartMs, rows]) => ({
            weekStartMs,
            candles: dedupeSortCandles(rows),
        }));
}

function delay(ms: number): Promise<void> {
    if (!(ms > 0)) return Promise.resolve();
    return new Promise((resolve) => setTimeout(resolve, ms));
}

function resolveBitgetSymbol(symbol: string, bitgetContracts: Set<string>): { symbol: string | null; reason: string } {
    const normalized = normalizeSymbol(symbol);
    if (!normalized) return { symbol: null, reason: 'invalid_symbol' };
    if (bitgetContracts.has(normalized)) return { symbol: normalized, reason: 'exact_contract' };

    const candidates: string[] = [];
    if (normalized.endsWith('USD') && !normalized.endsWith('USDT')) {
        const base = normalized.slice(0, -3);
        if (base) candidates.push(`${base}USDT`);
    }
    if (!normalized.endsWith('USDT')) candidates.push(`${normalized}USDT`);

    for (const candidate of candidates) {
        if (bitgetContracts.has(candidate)) {
            return { symbol: candidate, reason: `mapped_${normalized}_to_${candidate}` };
        }
    }
    return { symbol: null, reason: 'bitget_contract_not_found' };
}

async function fetchBitgetContractSet(): Promise<Set<string>> {
    const productType = String(resolveProductType() || 'usdt-futures')
        .trim()
        .toUpperCase();
    const contracts = await bitgetFetch('GET', '/api/v2/mix/market/contracts', { productType });
    const out = new Set<string>();
    if (!Array.isArray(contracts)) return out;
    for (const row of contracts) {
        const symbol = normalizeSymbol((row as Record<string, unknown>)?.symbol);
        if (symbol) out.add(symbol);
    }
    return out;
}

async function fetchBitget1mCandles(params: {
    symbol: string;
    fromMs: number;
    toMs: number;
    limitPerRequest: number;
    requestSpanMinutes: number;
    sleepMs: number;
    maxRequestsPerSymbol: number;
}): Promise<{ candles: ScalpCandle[]; requests: number }> {
    const symbol = normalizeSymbol(params.symbol);
    const fromMs = Math.floor(params.fromMs);
    const toMs = Math.floor(params.toMs);
    if (!symbol) throw new Error('invalid_symbol_for_bitget_backfill');
    if (!(Number.isFinite(fromMs) && Number.isFinite(toMs) && fromMs > 0 && toMs > fromMs)) {
        throw new Error(`invalid_backfill_window_for_${symbol}`);
    }

    const productType = String(resolveProductType() || 'usdt-futures')
        .trim()
        .toUpperCase();
    const candlesByTs = new Map<number, ScalpCandle>();
    let cursorEnd = toMs;
    let requests = 0;
    const spanMs = Math.max(120, Math.floor(params.requestSpanMinutes)) * ONE_MINUTE_MS;
    const requestLimit = Math.max(20, Math.min(1000, Math.floor(params.limitPerRequest)));
    const maxRequests = Math.max(100, Math.floor(params.maxRequestsPerSymbol));

    while (cursorEnd >= fromMs) {
        if (requests >= maxRequests) {
            throw new Error(`backfill_max_requests_reached_for_${symbol}`);
        }
        const startTime = Math.max(fromMs, cursorEnd - spanMs + ONE_MINUTE_MS);
        const rows = await bitgetFetch('GET', '/api/v2/mix/market/candles', {
            symbol,
            productType,
            granularity: '1m',
            limit: requestLimit,
            startTime,
            endTime: cursorEnd,
        });
        requests += 1;

        const parsedRows = Array.isArray(rows)
            ? rows
                  .map((row) => parseBitgetCandle(row))
                  .filter((row): row is ScalpCandle => Boolean(row))
                  .filter((row) => row[0] >= fromMs && row[0] <= toMs)
            : [];

        if (!parsedRows.length) {
            if (startTime <= fromMs) break;
            cursorEnd = startTime - ONE_MINUTE_MS;
            if (params.sleepMs > 0) await delay(params.sleepMs);
            continue;
        }

        let oldestTs = Number.POSITIVE_INFINITY;
        for (const candle of parsedRows) {
            candlesByTs.set(candle[0], candle);
            if (candle[0] < oldestTs) oldestTs = candle[0];
        }

        if (!Number.isFinite(oldestTs)) break;
        if (oldestTs >= cursorEnd) {
            cursorEnd -= spanMs;
        } else {
            cursorEnd = oldestTs - 1;
        }
        if (params.sleepMs > 0) await delay(params.sleepMs);
    }

    return {
        candles: Array.from(candlesByTs.values()).sort((lhs, rhs) => lhs[0] - rhs[0]),
        requests,
    };
}

async function loadCryptoWeekSymbols(timeframe: string): Promise<CryptoWeekSymbolRow[]> {
    const db = scalpPrisma();
    const rows = await db.$queryRaw<
        Array<{
            symbol: string;
            weekRows: number | bigint;
            candles: number | bigint;
            fromTsMs: number | bigint | null;
            toTsMs: number | bigint | null;
            sourcePreview: string[] | null;
            assetCategory: string | null;
            instrumentType: string | null;
        }>
    >(Prisma.sql`
        SELECT
            w.symbol,
            COUNT(*)::bigint AS "weekRows",
            COALESCE(SUM(jsonb_array_length(w.candles_json)), 0)::bigint AS candles,
            MIN(
                CASE
                    WHEN jsonb_array_length(w.candles_json) > 0
                         AND (w.candles_json -> 0 ->> 0) ~ '^[0-9]+$'
                    THEN (w.candles_json -> 0 ->> 0)::bigint
                    ELSE NULL
                END
            ) AS "fromTsMs",
            MAX(
                CASE
                    WHEN jsonb_array_length(w.candles_json) > 0
                         AND (w.candles_json -> (jsonb_array_length(w.candles_json) - 1) ->> 0) ~ '^[0-9]+$'
                    THEN (w.candles_json -> (jsonb_array_length(w.candles_json) - 1) ->> 0)::bigint
                    ELSE NULL
                END
            ) AS "toTsMs",
            ARRAY_REMOVE(ARRAY_AGG(DISTINCT NULLIF(TRIM(COALESCE(w.source, '')), '')), NULL) AS "sourcePreview",
            NULLIF(LOWER(TRIM(COALESCE(m.asset_category, ''))), '') AS "assetCategory",
            NULLIF(UPPER(TRIM(COALESCE(m.instrument_type, ''))), '') AS "instrumentType"
        FROM scalp_candle_history_weeks w
        LEFT JOIN scalp_symbol_market_metadata m
          ON m.symbol = w.symbol
        WHERE w.timeframe = ${timeframe}
        GROUP BY
            w.symbol,
            m.asset_category,
            m.instrument_type
        HAVING
            LOWER(TRIM(COALESCE(m.asset_category, ''))) = 'crypto'
            OR UPPER(TRIM(COALESCE(m.instrument_type, ''))) = 'CRYPTOCURRENCIES'
        ORDER BY w.symbol ASC;
    `);

    return rows.map((row) => ({
        symbol: normalizeSymbol(row.symbol),
        weekRows: Number(row.weekRows || 0),
        candles: Number(row.candles || 0),
        fromTsMs: row.fromTsMs === null ? null : Number(row.fromTsMs),
        toTsMs: row.toTsMs === null ? null : Number(row.toTsMs),
        sourcePreview: Array.isArray(row.sourcePreview) ? row.sourcePreview.map((v) => String(v || '').trim()).filter(Boolean) : [],
        assetCategory: row.assetCategory ? String(row.assetCategory) : null,
        instrumentType: row.instrumentType ? String(row.instrumentType) : null,
    }));
}

async function replaceWeeklyCandles(params: {
    symbol: string;
    timeframe: string;
    candles: ScalpCandle[];
    source: string;
    epic: string;
}): Promise<number> {
    const db = scalpPrisma();
    const weekly = toWeeklyBuckets(params.candles);
    for (const bucket of weekly) {
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
                    ${params.symbol},
                    ${params.timeframe},
                    to_timestamp(${bucket.weekStartMs} / 1000.0),
                    ${params.epic},
                    ${params.source},
                    ${JSON.stringify(bucket.candles)}::jsonb,
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
    return weekly.length;
}

async function main(): Promise<void> {
    const opts = parseArgs(process.argv.slice(2));
    const nowMs = Date.now();
    const db = scalpPrisma();
    const contracts = await fetchBitgetContractSet();
    const cryptoRowsAll = await loadCryptoWeekSymbols(opts.timeframe);
    const onlySet = new Set(opts.onlySymbols);
    const cryptoRows = onlySet.size
        ? cryptoRowsAll.filter((row) => onlySet.has(row.symbol))
        : cryptoRowsAll;

    const skipped: Array<{ sourceSymbol: string; reason: string }> = [];
    const grouped = new Map<string, ReplacementTarget>();

    for (const row of cryptoRows) {
        const mapping = resolveBitgetSymbol(row.symbol, contracts);
        if (!mapping.symbol) {
            skipped.push({
                sourceSymbol: row.symbol,
                reason: mapping.reason,
            });
            continue;
        }
        const fallbackFromMs = nowMs - opts.defaultLookbackDays * ONE_DAY_MS;
        const fromMs = row.fromTsMs ?? fallbackFromMs;
        const toMs = Math.max(row.toTsMs ?? 0, nowMs);
        const current = grouped.get(mapping.symbol);
        if (!current) {
            grouped.set(mapping.symbol, {
                targetSymbol: mapping.symbol,
                sourceSymbols: [row.symbol],
                mapReasons: [mapping.reason],
                weekRows: row.weekRows,
                candles: row.candles,
                fromMs,
                toMs,
            });
            continue;
        }
        if (!current.sourceSymbols.includes(row.symbol)) current.sourceSymbols.push(row.symbol);
        if (!current.mapReasons.includes(mapping.reason)) current.mapReasons.push(mapping.reason);
        current.weekRows += row.weekRows;
        current.candles += row.candles;
        current.fromMs = Math.min(current.fromMs, fromMs);
        current.toMs = Math.max(current.toMs, toMs);
    }

    const targets = Array.from(grouped.values()).sort((lhs, rhs) => lhs.targetSymbol.localeCompare(rhs.targetSymbol));
    const symbolsToDelete = Array.from(
        new Set(targets.flatMap((target) => [...target.sourceSymbols, target.targetSymbol])),
    ).sort();

    const report = {
        apply: opts.apply,
        options: opts,
        nowIso: new Date(nowMs).toISOString(),
        counts: {
            cryptoSymbolsInWeeks: cryptoRows.length,
            replacementTargets: targets.length,
            skipped: skipped.length,
        },
        skipped,
        targets,
        symbolsToDelete,
    };

    if (!opts.apply) {
        console.log(JSON.stringify(report, null, 2));
        return;
    }

    if (symbolsToDelete.length) {
        await db.$executeRaw(
            Prisma.sql`
                DELETE FROM scalp_candle_history_weeks
                WHERE timeframe = ${opts.timeframe}
                  AND symbol IN (${Prisma.join(symbolsToDelete)});
            `,
        );
    }

    const backfills: Array<{
        symbol: string;
        sourceSymbols: string[];
        requests: number;
        fetchedCandles: number;
        weekRows: number;
        fromMs: number;
        toMs: number;
    }> = [];

    for (const target of targets) {
        const fetchRes = await fetchBitget1mCandles({
            symbol: target.targetSymbol,
            fromMs: target.fromMs,
            toMs: target.toMs,
            limitPerRequest: opts.limitPerRequest,
            requestSpanMinutes: opts.requestSpanMinutes,
            sleepMs: opts.sleepMs,
            maxRequestsPerSymbol: opts.maxRequestsPerSymbol,
        });
        const weekRows = await replaceWeeklyCandles({
            symbol: target.targetSymbol,
            timeframe: opts.timeframe,
            candles: fetchRes.candles,
            source: 'bitget',
            epic: target.targetSymbol,
        });
        backfills.push({
            symbol: target.targetSymbol,
            sourceSymbols: target.sourceSymbols,
            requests: fetchRes.requests,
            fetchedCandles: fetchRes.candles.length,
            weekRows,
            fromMs: target.fromMs,
            toMs: target.toMs,
        });
    }

    const postRows = await db.$queryRaw<
        Array<{
            symbol: string;
            weekRows: number | bigint;
            candles: number | bigint;
            sourcePreview: string[] | null;
        }>
    >(Prisma.sql`
        SELECT
            symbol,
            COUNT(*)::bigint AS "weekRows",
            COALESCE(SUM(jsonb_array_length(candles_json)), 0)::bigint AS candles,
            ARRAY_REMOVE(ARRAY_AGG(DISTINCT NULLIF(TRIM(COALESCE(source, '')), '')), NULL) AS "sourcePreview"
        FROM scalp_candle_history_weeks
        WHERE timeframe = ${opts.timeframe}
          AND symbol IN (${Prisma.join(targets.map((row) => row.targetSymbol))})
        GROUP BY symbol
        ORDER BY symbol ASC;
    `);

    console.log(
        JSON.stringify(
            {
                ...report,
                applied: true,
                deletedRowsForSymbols: symbolsToDelete.length,
                backfills,
                postRows: postRows.map((row) => ({
                    symbol: normalizeSymbol(row.symbol),
                    weekRows: Number(row.weekRows || 0),
                    candles: Number(row.candles || 0),
                    sourcePreview: Array.isArray(row.sourcePreview)
                        ? row.sourcePreview.map((v) => String(v || '').trim()).filter(Boolean)
                        : [],
                })),
            },
            null,
            2,
        ),
    );
}

main().catch((err) => {
    const message = String(err?.message || err || 'unknown_error');
    const stack = err instanceof Error ? err.stack : undefined;
    console.error(
        JSON.stringify(
            {
                ok: false,
                error: message,
                stack,
            },
            null,
            2,
        ),
    );
    process.exit(1);
});

