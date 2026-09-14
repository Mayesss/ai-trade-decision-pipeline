// Fetch 4H candles covering the decision sample, per symbol, from the venues.
import nextEnv from '@next/env';
import { readFileSync, writeFileSync } from 'node:fs';

import { bitgetFetch, resolveProductType } from '../lib/bitget';
import { fetchCapitalCandlesByEpicDateRange } from '../lib/capital';

const { loadEnvConfig } = nextEnv;
loadEnvConfig(process.cwd());

const BAR_MS = 4 * 3600_000;
const PAD_AFTER = 30 * BAR_MS; // room for the longest forward horizon

type D = { symbol: string; platform: string; decided_at_ms: string | number; epic: string | null };

async function bitget4h(symbol: string, fromMs: number): Promise<number[][]> {
    const productType = resolveProductType();
    const rows: string[][] = await bitgetFetch('GET', '/api/v2/mix/market/candles', {
        symbol, productType, granularity: '4H', limit: 1000,
    });
    let candles = rows.map((c) => c.slice(0, 6).map(Number)).sort((a, b) => a[0] - b[0]);
    let guard = 0;
    while (candles.length && candles[0][0] > fromMs && guard < 12) {
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
    const ds: D[] = JSON.parse(readFileSync('.study/decisions.json', 'utf8'));
    const groups = new Map<string, D[]>();
    for (const d of ds) {
        const key = `${d.platform}:${d.symbol}`;
        const group = groups.get(key);
        if (group) group.push(d);
        else groups.set(key, [d]);
    }
    const out: Record<string, number[][]> = {};
    for (const [key, rows] of groups) {
        const [platform, symbol] = key.split(':');
        const tss = rows.map((r) => Number(r.decided_at_ms));
        const from = Math.min(...tss) - 5 * BAR_MS;
        const to = Math.min(Date.now(), Math.max(...tss) + PAD_AFTER);
        try {
            const candles = platform === 'bitget'
                ? await bitget4h(symbol, from)
                : await fetchCapitalCandlesByEpicDateRange(rows.find((r) => r.epic)?.epic || symbol, '4H', from, to);
            out[key] = candles;
            const cov = candles.filter((c) => c[0] >= from && c[0] <= to).length;
            console.log(`${key.padEnd(24)} bars=${String(candles.length).padStart(5)} in-range=${cov} n=${rows.length}`);
        } catch (err) {
            console.log(`${key.padEnd(24)} FAILED ${(err as Error).message}`);
        }
    }
    writeFileSync('.study/candles.json', JSON.stringify(out));
}
main().catch((e) => { console.error(e); process.exit(1); });
