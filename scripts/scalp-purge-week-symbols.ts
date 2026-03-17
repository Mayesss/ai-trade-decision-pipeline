import { Prisma } from '@prisma/client';

import { scalpPrisma } from '../lib/scalp/pg/client';

type ScriptOptions = {
    symbols: string[];
};

function normalizeSymbol(value: unknown): string {
    return String(value || '')
        .trim()
        .toUpperCase()
        .replace(/[^A-Z0-9._-]/g, '');
}

function parseArgs(argv: string[]): ScriptOptions {
    const out: ScriptOptions = {
        symbols: [],
    };
    for (const arg of argv) {
        if (!arg.startsWith('--')) continue;
        const [rawKey, rawValue = ''] = arg.split('=');
        const key = rawKey.trim();
        const value = rawValue.trim();
        if (key === '--symbols' && value) {
            out.symbols = Array.from(
                new Set(
                    value
                        .split(',')
                        .map((row) => normalizeSymbol(row))
                        .filter((row) => Boolean(row)),
                ),
            );
        }
    }
    return out;
}

async function loadSummary(symbols: string[]): Promise<Array<{ symbol: string; timeframe: string; weekRows: number; candles: number }>> {
    if (!symbols.length) return [];
    const db = scalpPrisma();
    const rows = await db.$queryRaw<
        Array<{
            symbol: string;
            timeframe: string;
            weekRows: number | bigint;
            candles: number | bigint;
        }>
    >(Prisma.sql`
        SELECT
            symbol,
            timeframe,
            COUNT(*)::bigint AS "weekRows",
            COALESCE(SUM(jsonb_array_length(candles_json)), 0)::bigint AS candles
        FROM scalp_candle_history_weeks
        WHERE symbol IN (${Prisma.join(symbols)})
        GROUP BY symbol, timeframe
        ORDER BY symbol ASC, timeframe ASC;
    `);
    return rows.map((row) => ({
        symbol: normalizeSymbol(row.symbol),
        timeframe: String(row.timeframe || '').trim().toLowerCase(),
        weekRows: Number(row.weekRows || 0),
        candles: Number(row.candles || 0),
    }));
}

async function main(): Promise<void> {
    const opts = parseArgs(process.argv.slice(2));
    if (!opts.symbols.length) {
        throw new Error('missing_symbols_arg');
    }

    const db = scalpPrisma();
    const before = await loadSummary(opts.symbols);
    const deleted = await db.$executeRaw(
        Prisma.sql`
            DELETE FROM scalp_candle_history_weeks
            WHERE symbol IN (${Prisma.join(opts.symbols)});
        `,
    );
    const after = await loadSummary(opts.symbols);

    console.log(
        JSON.stringify(
            {
                symbols: opts.symbols,
                before,
                deletedRows: Number(deleted || 0),
                after,
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

