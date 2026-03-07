import { loadScalpCandleHistory, saveScalpCandleHistory } from '../lib/scalp/candleHistory';

type CliArgs = {
    symbols: string[];
    timeframes: string[];
    maxDays: number | null;
    dryRun: boolean;
};

const ONE_DAY_MS = 24 * 60 * 60 * 1000;

function normalizeSymbol(value: string): string {
    return String(value || '')
        .trim()
        .toUpperCase()
        .replace(/[^A-Z0-9._-]/g, '');
}

function normalizeTimeframe(value: string): string {
    const raw = String(value || '')
        .trim()
        .toLowerCase();
    const match = raw.match(/^(\d+)([mhdw])$/);
    if (!match) return '';
    return `${Number(match[1])}${match[2]}`;
}

function parseBool(value: string | undefined, fallback: boolean): boolean {
    if (value === undefined) return fallback;
    const normalized = String(value).trim().toLowerCase();
    if (['1', 'true', 'yes', 'on'].includes(normalized)) return true;
    if (['0', 'false', 'no', 'off'].includes(normalized)) return false;
    return fallback;
}

function parsePositiveInt(value: string | undefined): number | null {
    if (value === undefined) return null;
    const n = Number(value);
    if (!Number.isFinite(n) || n <= 0) return null;
    return Math.floor(n);
}

function parseCsv(value: string | undefined): string[] {
    if (!value) return [];
    return value
        .split(',')
        .map((row) => row.trim())
        .filter((row) => row.length > 0);
}

function parseArgs(argv: string[]): CliArgs {
    const out: Record<string, string> = {};
    for (let i = 0; i < argv.length; i += 1) {
        const row = argv[i];
        if (!row.startsWith('--')) continue;
        const key = row.slice(2);
        const next = argv[i + 1];
        if (!next || next.startsWith('--')) {
            out[key] = 'true';
            continue;
        }
        out[key] = next;
        i += 1;
    }

    const symbols = parseCsv(out.symbols)
        .map((row) => normalizeSymbol(row))
        .filter((row) => Boolean(row));
    const timeframes = parseCsv(out.timeframes)
        .map((row) => normalizeTimeframe(row))
        .filter((row) => Boolean(row));

    return {
        symbols: symbols.length ? Array.from(new Set(symbols)) : ['BTCUSDT', 'XAUUSDT'],
        timeframes: timeframes.length ? Array.from(new Set(timeframes)) : ['1m', '15m'],
        maxDays: parsePositiveInt(out.maxDays),
        dryRun: parseBool(out.dryRun, false),
    };
}

function tsToIso(ts: number | null): string | null {
    if (!Number.isFinite(Number(ts))) return null;
    return new Date(Number(ts)).toISOString();
}

async function main(): Promise<void> {
    const args = parseArgs(process.argv.slice(2));
    const nowMs = Date.now();
    const minTs = args.maxDays ? nowMs - args.maxDays * ONE_DAY_MS : null;

    const rows: Array<Record<string, unknown>> = [];
    for (const symbol of args.symbols) {
        for (const timeframe of args.timeframes) {
            const source = await loadScalpCandleHistory(symbol, timeframe, { backend: 'file' });
            const sourceCandles = source.record?.candles || [];
            const filteredCandles =
                minTs === null ? sourceCandles : sourceCandles.filter((row) => Number(row[0]) >= Number(minTs));

            if (!source.record || filteredCandles.length === 0) {
                rows.push({
                    symbol,
                    timeframe,
                    ok: false,
                    reason: 'missing_or_empty_source',
                    sourceCount: sourceCandles.length,
                });
                continue;
            }

            if (!args.dryRun) {
                await saveScalpCandleHistory(
                    {
                        symbol,
                        timeframe,
                        epic: source.record.epic,
                        source: 'capital',
                        candles: filteredCandles,
                    },
                    { backend: 'kv' },
                );
            }

            const target = args.dryRun ? null : await loadScalpCandleHistory(symbol, timeframe, { backend: 'kv' });
            const targetCount = target?.record?.candles?.length ?? null;
            rows.push({
                symbol,
                timeframe,
                ok: true,
                dryRun: args.dryRun,
                sourceCount: sourceCandles.length,
                syncedCount: filteredCandles.length,
                targetCount,
                fromTs: filteredCandles[0]?.[0] ?? null,
                toTs: filteredCandles[filteredCandles.length - 1]?.[0] ?? null,
                fromIso: tsToIso(filteredCandles[0]?.[0] ?? null),
                toIso: tsToIso(filteredCandles[filteredCandles.length - 1]?.[0] ?? null),
            });
        }
    }

    const summary = {
        generatedAtIso: new Date(nowMs).toISOString(),
        dryRun: args.dryRun,
        symbols: args.symbols,
        timeframes: args.timeframes,
        maxDays: args.maxDays,
        rows,
    };
    console.log(JSON.stringify(summary, null, 2));
}

main().catch((err) => {
    console.error(
        JSON.stringify(
            {
                error: 'scalp_sync_candles_kv_failed',
                message: err?.message || String(err),
            },
            null,
            2,
        ),
    );
    process.exitCode = 1;
});
