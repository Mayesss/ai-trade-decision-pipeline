// HOLDOUT SET: bars BEFORE 2024-03-05, which nothing in this project has seen.
// The 4-signal test used the whole current panel, so no clean holdout exists
// inside it. Bitget has ~3.7y; we only pulled 2.5y. The remainder is untouched.
// Saved to a SEPARATE directory so the discovery panel cannot be contaminated.
import nextEnv from '@next/env';
import { mkdirSync, writeFileSync, existsSync, readFileSync } from 'node:fs';
import { bitgetFetch, resolveProductType } from '../lib/bitget';
const { loadEnvConfig } = nextEnv;
loadEnvConfig(process.cwd());

const OUT = '.study/panel-holdout';
const CUTOFF = Date.parse('2024-03-05T00:00:00Z'); // discovery panel starts here
const MIN_BARS = 500;
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

async function olderThan(symbol: string, endMs: number): Promise<number[][]> {
    const productType = resolveProductType();
    let out: number[][] = [];
    let cursor = endMs;
    for (let g = 0; g < 30; g++) {
        const rows: string[][] = await bitgetFetch('GET', '/api/v2/mix/market/history-candles', {
            symbol, productType, granularity: '4H', endTime: cursor, limit: 200,
        });
        const mapped = rows.map((c) => c.slice(0, 6).map(Number))
            .filter((c) => c[0] < cursor).sort((a, b) => a[0] - b[0]);
        if (!mapped.length) break;
        out = [...mapped, ...out];
        cursor = mapped[0][0];
        await sleep(90);
    }
    return out;
}

async function main() {
    if (!existsSync(OUT)) mkdirSync(OUT, { recursive: true });
    const manifest = JSON.parse(readFileSync('.study/panel-wide/_manifest.json', 'utf8'));
    const kept: Array<Record<string, unknown>> = [];
    let thin = 0;
    for (let i = 0; i < manifest.length; i++) {
        const { symbol } = manifest[i];
        try {
            const c = await olderThan(symbol, CUTOFF);
            if (c.length < MIN_BARS) { thin++; continue; }
            writeFileSync(`${OUT}/${symbol}.json`, JSON.stringify(c));
            kept.push({ symbol, bars: c.length, firstMs: c[0][0], lastMs: c[c.length - 1][0] });
            if (kept.length % 20 === 0) console.log(`  ...${kept.length} saved (${i + 1}/${manifest.length})`);
        } catch (e) { console.log(`  FAIL ${symbol}: ${(e as Error).message}`); }
        await sleep(110);
    }
    writeFileSync(`${OUT}/_manifest.json`, JSON.stringify(kept, null, 1));
    const bars = kept.reduce((s, k) => s + Number(k.bars), 0);
    console.log(`\nHOLDOUT: ${kept.length} symbols, ${bars.toLocaleString()} bars, ${thin} too thin`);
    if (kept.length) {
        const f = Math.min(...kept.map((k) => Number(k.firstMs)));
        const l = Math.max(...kept.map((k) => Number(k.lastMs)));
        console.log(`range: ${new Date(f).toISOString().slice(0,10)} .. ${new Date(l).toISOString().slice(0,10)}`);
    }
}
main().catch((e) => { console.error(e); process.exit(1); });
