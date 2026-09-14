// Deeper candle history for the veto test's REFERENCE set.
// Needs bars well BEFORE the decision window so early decisions have a
// look-back that is not almost empty.
import nextEnv from '@next/env';
import { readFileSync, writeFileSync } from 'node:fs';
import { bitgetFetch, resolveProductType } from '../lib/bitget';
import { fetchCapitalCandlesByEpicDateRange } from '../lib/capital';
const { loadEnvConfig } = nextEnv;
loadEnvConfig(process.cwd());

const BAR_MS = 4 * 3600_000;
const LOOKBACK_MS = 550 * 24 * 3600_000; // ~18 months before the earliest decision

async function bitgetDeep(symbol: string, fromMs: number): Promise<number[][]> {
    const productType = resolveProductType();
    const rows: string[][] = await bitgetFetch('GET', '/api/v2/mix/market/candles', {
        symbol, productType, granularity: '4H', limit: 1000,
    });
    let candles = rows.map((c) => c.slice(0, 6).map(Number)).sort((a, b) => a[0] - b[0]);
    let guard = 0;
    while (candles.length && candles[0][0] > fromMs && guard < 30) {
        guard += 1;
        const older: string[][] = await bitgetFetch('GET', '/api/v2/mix/market/history-candles', {
            symbol, productType, granularity: '4H', endTime: candles[0][0], limit: 200,
        });
        const mapped = older.map((c) => c.slice(0, 6).map(Number))
            .filter((c) => c[0] < candles[0][0]).sort((a, b) => a[0] - b[0]);
        if (!mapped.length) break;
        candles = [...mapped, ...candles];
    }
    return candles;
}

async function main() {
    const ds = JSON.parse(readFileSync('.study/decisions.json', 'utf8')) as Array<{
        symbol: string; platform: string; decided_at_ms: string; epic: string | null }>;
    const groups = new Map<string, typeof ds>();
    for (const d of ds) {
        const key = `${d.platform}:${d.symbol}`;
        const g = groups.get(key);
        if (g) g.push(d); else groups.set(key, [d]);
    }
    const out: Record<string, number[][]> = {};
    let total = 0;
    for (const [key, rows] of groups) {
        const [platform, symbol] = key.split(':');
        const minTs = Math.min(...rows.map((r) => Number(r.decided_at_ms)));
        const from = minTs - LOOKBACK_MS;
        const to = Math.min(Date.now(), Math.max(...rows.map((r) => Number(r.decided_at_ms))) + 30 * BAR_MS);
        try {
            const candles = platform === 'bitget'
                ? await bitgetDeep(symbol, from)
                : await fetchCapitalCandlesByEpicDateRange(rows.find((r) => r.epic)?.epic || symbol, '4H', from, to);
            out[key] = candles;
            total += candles.length;
            const first = candles.length ? new Date(candles[0][0]).toISOString().slice(0, 10) : '-';
            console.log(`${key.padEnd(22)} ${String(candles.length).padStart(5)} bars from ${first}`);
        } catch (e) { console.log(`${key.padEnd(22)} FAILED ${(e as Error).message}`); }
    }
    writeFileSync('.study/candles-deep.json', JSON.stringify(out));
    console.log(`\ntotal reference bars: ${total.toLocaleString()}`);
}
main().catch((e) => { console.error(e); process.exit(1); });
