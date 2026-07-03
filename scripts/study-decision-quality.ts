// One-off study: quality of swing AI decisions and of the pre-AI gates, plus a
// comparison of candidate decision drivers (pivot trendlines, regression channel,
// structure biases) against realized forward moves.
//
// For EVERY decision tick in swing.decisions (called AND skipped), refetches 4H
// candles and computes: ATR14, regression-channel signals, pivot-trendline signals,
// and forward outcomes (signed close returns and max excursions in ATR units).
// STATE fields are parsed from the stored prompt when the AI was actually called.
//
// Usage: node --import tsx scripts/study-decision-quality.ts <dumpDir>
//   reads  <dumpDir>/all-decisions.jsonl
//   writes <dumpDir>/study-rows.json
import nextEnv from '@next/env';
import { readFileSync, writeFileSync } from 'node:fs';
import path from 'node:path';

import { bitgetFetch, resolveProductType } from '../lib/bitget';
import { fetchCapitalCandlesByEpicDateRange } from '../lib/capital';
import { computeATR } from '../lib/indicators';

const { loadEnvConfig } = nextEnv;
loadEnvConfig(process.cwd());

const BAR_MS = 4 * 60 * 60 * 1000;
const SR_WINDOW = 150;
const PIVOT_K = 2; // fractal half-width for swing pivots
const PIVOT_LOOKBACK = 60; // bars scanned for trendline pivots

type Tick = {
    id: number;
    ts: number;
    symbol: string;
    platform: string;
    category: string | null;
    epic: string | null;
    action: string | null;
    skipStage: string | null;
    skipReason: string | null;
    exitPct: string | null;
    user: string | null;
};

function parseStateBlocks(user: string): { state: any; market: any } | null {
    const stateMatch = user.match(/STATE \(derived signals[^:]*:\n(\{.*?\})\n\nMARKET/s);
    const marketMatch = user.match(/MARKET \(raw inputs\):\n(\{.*?\})\n\nTASKS/s);
    if (!stateMatch) return null;
    try {
        return {
            state: JSON.parse(stateMatch[1]!),
            market: marketMatch ? JSON.parse(marketMatch[1]!) : null,
        };
    } catch {
        return null;
    }
}

function fitChannel(closes: number[], price: number, atr: number, n: number) {
    const ys = closes.slice(-n);
    if (ys.length < 30 || !(atr > 0)) return null;
    const len = ys.length;
    const xMean = (len - 1) / 2;
    const yMean = ys.reduce((a, b) => a + b, 0) / len;
    let sxx = 0;
    let sxy = 0;
    for (let i = 0; i < len; i++) {
        sxx += (i - xMean) * (i - xMean);
        sxy += (i - xMean) * (ys[i]! - yMean);
    }
    const slope = sxx > 0 ? sxy / sxx : 0;
    const intercept = yMean - slope * xMean;
    let ssr = 0;
    for (let i = 0; i < len; i++) {
        const r = ys[i]! - (intercept + slope * i);
        ssr += r * r;
    }
    const sd = Math.sqrt(ssr / Math.max(1, len - 2));
    const mid = intercept + slope * (len - 1);
    return {
        slopePctPerBar: price > 0 ? (slope / price) * 100 : 0,
        slopeAtrPerBar: slope / atr,
        distUpAtr: Math.max(0, (mid + 2 * sd - price) / atr),
        distDownAtr: Math.max(0, (price - (mid - 2 * sd)) / atr),
        posInChannel: sd > 0 ? (price - (mid - 2 * sd)) / (4 * sd) : 0.5, // 0=lower band, 1=upper band
    };
}

// Simple pivot trendlines: line through the last two swing highs (resistance) and
// the last two swing lows (support), evaluated at the current bar.
function pivotTrendlines(window: number[][], price: number, atr: number) {
    const n = window.length;
    const from = Math.max(PIVOT_K, n - PIVOT_LOOKBACK);
    const highs: Array<{ i: number; v: number }> = [];
    const lows: Array<{ i: number; v: number }> = [];
    for (let i = from; i < n - PIVOT_K; i++) {
        let isH = true;
        let isL = true;
        for (let j = 1; j <= PIVOT_K; j++) {
            if (window[i]![2]! <= window[i - j]![2]! || window[i]![2]! < window[i + j]![2]!) isH = false;
            if (window[i]![3]! >= window[i - j]![3]! || window[i]![3]! > window[i + j]![3]!) isL = false;
            if (!isH && !isL) break;
        }
        if (isH) highs.push({ i, v: window[i]![2]! });
        if (isL) lows.push({ i, v: window[i]![3]! });
    }
    const line = (pts: Array<{ i: number; v: number }>) => {
        if (pts.length < 2) return null;
        const [a, b] = [pts[pts.length - 2]!, pts[pts.length - 1]!];
        if (b.i === a.i) return null;
        const slope = (b.v - a.v) / (b.i - a.i);
        const atNow = b.v + slope * (n - 1 - b.i);
        const atPrev = b.v + slope * (n - 2 - b.i);
        return { slope, atNow, atPrev };
    };
    const res = line(highs);
    const sup = line(lows);
    const lastClose = window[n - 1]![4]!;
    const prevClose = window[n - 2]![4]!;
    return {
        resDistAtr: res ? (res.atNow - price) / atr : null, // + = line above price
        supDistAtr: sup ? (price - sup.atNow) / atr : null, // + = line below price
        resSlopeAtr: res ? res.slope / atr : null,
        supSlopeAtr: sup ? sup.slope / atr : null,
        resBrokenUp: res ? lastClose > res.atNow && prevClose <= res.atPrev : false,
        supBrokenDown: sup ? lastClose < sup.atNow && prevClose >= sup.atPrev : false,
    };
}

async function fetchBitget4h(symbol: string, needFromMs: number): Promise<number[][]> {
    const productType = resolveProductType();
    const rows: any[] = await bitgetFetch('GET', '/api/v2/mix/market/candles', {
        symbol,
        productType,
        granularity: '4H',
        limit: 1000,
    });
    let candles = rows
        .map((c) => [Number(c[0]), Number(c[1]), Number(c[2]), Number(c[3]), Number(c[4]), Number(c[5])])
        .sort((a, b) => a[0]! - b[0]!);
    let guard = 0;
    while (candles.length && candles[0]![0]! > needFromMs && guard < 10) {
        guard += 1;
        const older: any[] = await bitgetFetch('GET', '/api/v2/mix/market/history-candles', {
            symbol,
            productType,
            granularity: '4H',
            endTime: candles[0]![0]!,
            limit: 200,
        });
        const mapped = older
            .map((c) => [Number(c[0]), Number(c[1]), Number(c[2]), Number(c[3]), Number(c[4]), Number(c[5])])
            .filter((c) => c[0]! < candles[0]![0]!)
            .sort((a, b) => a[0]! - b[0]!);
        if (!mapped.length) break;
        candles = [...mapped, ...candles];
    }
    return candles as number[][];
}

async function main() {
    const dumpDir = process.argv[2];
    if (!dumpDir) throw new Error('usage: study-decision-quality.ts <dumpDir>');

    const ticks: Tick[] = readFileSync(path.join(dumpDir, 'all-decisions.jsonl'), 'utf8')
        .split('\n')
        .filter(Boolean)
        .map((l) => JSON.parse(l));

    const bySymbol = new Map<string, Tick[]>();
    for (const t of ticks) {
        const key = `${t.platform}:${t.symbol}`;
        if (!bySymbol.has(key)) bySymbol.set(key, []);
        bySymbol.get(key)!.push(t);
    }

    const candlesBySymbol = new Map<string, number[][]>();
    for (const [key, ds] of bySymbol) {
        const [platform, symbol] = key.split(':') as [string, string];
        const minTs = Math.min(...ds.map((d) => d.ts));
        const needFromMs = minTs - (SR_WINDOW + 20) * BAR_MS * (platform === 'capital' ? 1.6 : 1);
        try {
            const candles =
                platform === 'bitget'
                    ? await fetchBitget4h(symbol, needFromMs)
                    : await fetchCapitalCandlesByEpicDateRange(ds[0]!.epic || symbol, '4H', needFromMs, Date.now());
            candlesBySymbol.set(key, candles);
            console.error(`[candles] ${key}: ${candles.length} bars`);
        } catch (err) {
            console.error(`[candles] ${key} FAILED: ${(err as Error).message}`);
        }
    }

    const rows: any[] = [];
    for (const t of ticks) {
        const candles = candlesBySymbol.get(`${t.platform}:${t.symbol}`);
        if (!candles?.length) continue;
        const closedBefore = candles.filter((c) => c[0]! + BAR_MS <= t.ts);
        const window = closedBefore.slice(-SR_WINDOW);
        if (window.length < 60) continue;

        const parsed = t.user ? parseStateBlocks(t.user) : null;
        const price = Number(parsed?.market?.price?.last) || window.at(-1)![4]!;
        const atr = computeATR(window as any[], 14);
        if (!(atr > 0)) continue;

        const closes = window.map((c) => c[4]!);
        const ch = fitChannel(closes, price, atr, 50);
        const tl = pivotTrendlines(window, price, atr);

        const fwd = candles.filter((c) => c[0]! >= t.ts);
        const at = (k: number) => (fwd.length >= k ? (fwd[k - 1]![4]! - price) / atr : null);
        const mfe = (k: number, dir: 'up' | 'down') => {
            const seg = fwd.slice(0, k);
            if (seg.length < Math.min(k, 4)) return null;
            return dir === 'up'
                ? (Math.max(...seg.map((c) => c[2]!)) - price) / atr
                : (price - Math.min(...seg.map((c) => c[3]!))) / atr;
        };

        const st = parsed?.state;
        rows.push({
            id: t.id,
            ts: t.ts,
            symbol: t.symbol,
            platform: t.platform,
            category: t.category,
            action: t.action,
            skipStage: t.skipStage,
            skipReason: t.skipReason,
            exitPct: t.exitPct != null ? Number(t.exitPct) : null,
            called: !!t.user,
            price,
            atr,
            atrPct: (atr / price) * 100,
            // channel + trendline signals (available for every tick)
            chSlope: ch?.slopePctPerBar ?? null,
            chPos: ch?.posInChannel ?? null,
            chUp: ch?.distUpAtr ?? null,
            chDown: ch?.distDownAtr ?? null,
            tlResDist: tl.resDistAtr,
            tlSupDist: tl.supDistAtr,
            tlResSlope: tl.resSlopeAtr,
            tlSupSlope: tl.supSlopeAtr,
            tlBreakUp: tl.resBrokenUp,
            tlBreakDown: tl.supBrokenDown,
            // STATE extracts (called ticks only)
            posOpen: st?.position?.open ?? null,
            posSide: st?.position?.side ?? null,
            posPnlPct: st?.position?.unrealized_pnl_pct ?? null,
            holdMin: st?.position?.hold_minutes ?? null,
            microBias: st?.biases?.micro ?? null,
            primaryBias: st?.biases?.primary ?? null,
            macroBias: st?.biases?.macro ?? null,
            contextBias: st?.biases?.context ?? null,
            structPrimary: st?.structure?.primary?.state ?? null,
            breakStatePrimary: st?.structure?.primary?.break_state ?? null,
            bosPrimary: st?.structure?.primary?.bos ?? null,
            bosDirPrimary: st?.structure?.primary?.bos_dir ?? null,
            supDistAtr: st?.levels?.primary?.support?.dist_atr ?? null,
            resDistAtr: st?.levels?.primary?.resistance?.dist_atr ?? null,
            extMicro: st?.extension_atr?.micro ?? null,
            extPrimary: st?.extension_atr?.primary ?? null,
            // forward outcomes (ATR units)
            fwdBars: fwd.length,
            ret6: at(6),
            ret12: at(12),
            ret30: at(30),
            mfeUp30: mfe(30, 'up'),
            mfeDown30: mfe(30, 'down'),
        });
    }

    writeFileSync(path.join(dumpDir, 'study-rows.json'), JSON.stringify(rows));
    console.error(`[done] ${rows.length}/${ticks.length} ticks → study-rows.json`);
}

main().catch((err) => {
    console.error(err);
    process.exit(1);
});
