// How much 4H history can we actually get? This bounds any backtest.
import nextEnv from '@next/env';
import { bitgetFetch, resolveProductType } from '../lib/bitget';
import { fetchCapitalCandlesByEpicDateRange } from '../lib/capital';
const { loadEnvConfig } = nextEnv;
loadEnvConfig(process.cwd());
const DAY = 86400_000;
const iso = (ms: number) => new Date(ms).toISOString().slice(0, 10);

async function capitalDepth(epic: string) {
    for (const days of [90, 180, 365, 730]) {
        try {
            const c = await fetchCapitalCandlesByEpicDateRange(epic, '4H', Date.now() - days * DAY, Date.now());
            if (!c.length) { console.log(`  capital ${epic.padEnd(11)} ${String(days).padStart(4)}d -> 0 bars`); continue; }
            console.log(`  capital ${epic.padEnd(11)} ${String(days).padStart(4)}d -> ${String(c.length).padStart(5)} bars, oldest ${iso(c[0][0])}`);
        } catch (e) { console.log(`  capital ${epic.padEnd(11)} ${String(days).padStart(4)}d -> FAIL ${(e as Error).message}`); }
    }
}

async function bitgetDepth(symbol: string) {
    // walk history-candles backwards to find the true start of 4H data
    let oldest = Date.now(); let bars = 0; let guard = 0;
    while (guard < 40) {
        guard++;
        const rows: string[][] = await bitgetFetch('GET', '/api/v2/mix/market/history-candles', {
            symbol, productType: resolveProductType(), granularity: '4H', endTime: oldest, limit: 200,
        });
        if (!rows.length) break;
        const ts = rows.map((r) => Number(r[0])).sort((a, b) => a - b);
        if (ts[0] >= oldest) break;
        bars += rows.length; oldest = ts[0];
    }
    console.log(`  bitget  ${symbol.padEnd(11)} reachable back to ${iso(oldest)} (~${bars} bars, ${(((Date.now() - oldest) / DAY) / 365).toFixed(1)}y)`);
}

async function main() {
    console.log('CAPITAL (CFD) 4H depth:');
    await capitalDepth('GOLD');
    await capitalDepth('US100');
    console.log('BITGET (perp) 4H depth:');
    await bitgetDepth('BTCUSDT');
    await bitgetDepth('SOLUSDT');
}
main().catch((e) => console.error(e));
