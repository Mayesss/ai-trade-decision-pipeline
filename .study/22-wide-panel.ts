// Widen the panel: top-N Bitget USDT perps by 24h volume, 4H bars, ~2.5y.
// Writes one JSON per symbol to .study/panel-wide/ (Parquet + R2 comes later).
import nextEnv from '@next/env';
import { mkdirSync, writeFileSync, existsSync } from 'node:fs';
import { bitgetFetch, resolveProductType } from '../lib/bitget';
const { loadEnvConfig } = nextEnv;
loadEnvConfig(process.cwd());

const TOP_N = Number(process.env.TOP_N || 120);
const LOOKBACK_MS = 900 * 24 * 3600_000;   // ~2.5y
const MIN_BARS = 1500;                      // ~10 months; drops freshly listed
const OUT = '.study/panel-wide';

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

async function deep(symbol: string, fromMs: number): Promise<number[][]> {
    const productType = resolveProductType();
    const rows: string[][] = await bitgetFetch('GET', '/api/v2/mix/market/candles', {
        symbol, productType, granularity: '4H', limit: 1000,
    });
    let candles = rows.map((c) => c.slice(0, 6).map(Number)).sort((a, b) => a[0] - b[0]);
    let guard = 0;
    while (candles.length && candles[0][0] > fromMs && guard < 25) {
        guard += 1;
        await sleep(90);
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
    if (!existsSync(OUT)) mkdirSync(OUT, { recursive: true });
    const tickers: Array<Record<string, unknown>> = await bitgetFetch('GET', '/api/v2/mix/market/tickers', {
        productType: resolveProductType(),
    });
    const universe = tickers
        .filter((t) => String(t.symbol).endsWith('USDT'))
        .map((t) => ({ symbol: String(t.symbol), vol: Number(t.usdtVolume ?? t.quoteVolume ?? 0) }))
        .sort((a, b) => b.vol - a.vol)
        .slice(0, TOP_N);

    const from = Date.now() - LOOKBACK_MS;
    const manifest: Array<Record<string, unknown>> = [];
    let ok = 0, thin = 0, failed = 0;
    for (let i = 0; i < universe.length; i++) {
        const { symbol, vol } = universe[i];
        try {
            const c = await deep(symbol, from);
            if (c.length < MIN_BARS) { thin++; console.log(`  skip ${symbol} (${c.length} bars)`); continue; }
            writeFileSync(`${OUT}/${symbol}.json`, JSON.stringify(c));
            manifest.push({ symbol, vol24hUsd: vol, bars: c.length, firstMs: c[0][0], lastMs: c[c.length - 1][0] });
            ok++;
            if (ok % 20 === 0) console.log(`  ...${ok} saved (${i + 1}/${universe.length} scanned)`);
        } catch (e) { failed++; console.log(`  FAIL ${symbol}: ${(e as Error).message}`); }
        await sleep(120);
    }
    writeFileSync(`${OUT}/_manifest.json`, JSON.stringify(manifest, null, 1));
    const totalBars = manifest.reduce((s, m) => s + Number(m.bars), 0);
    console.log(`\nsaved ${ok} symbols, skipped ${thin} thin, ${failed} failed`);
    console.log(`total bars: ${totalBars.toLocaleString()}`);
    console.log(`oldest bar: ${new Date(Math.min(...manifest.map((m) => Number(m.firstMs)))).toISOString().slice(0, 10)}`);
}
main().catch((e) => { console.error(e); process.exit(1); });
