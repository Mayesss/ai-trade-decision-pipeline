// Build the decision-level panel: volatility-normalised forward returns.
//   y_h = dir * (close_{t+h bars} - P_t) / ATR_t      [dir = +1 BUY, -1 SELL]
// y is a risk-adjusted forward return: profit per unit of the instrument's own
// 4H volatility, so crypto and index ticks are on one scale.
import { readFileSync, writeFileSync } from 'node:fs';

const BAR_MS = 4 * 3600_000;
const HORIZONS = [1, 2, 6, 12];

type Dec = {
    id: string; decided_at_ms: string; symbol: string; platform: string; category: string;
    action: string; price: number; atr: number; signal_strength: string | null;
    strategy: string | null; ai_model: string | null; risk_usd: number | null;
    sl: number | null; tp: number | null; placed: boolean | null;
};

const decisions: Dec[] = JSON.parse(readFileSync('.study/decisions.json', 'utf8'));
const candles: Record<string, number[][]> = JSON.parse(readFileSync('.study/candles.json', 'utf8'));

const rows: Array<Record<string, unknown>> = [];
let noSeries = 0, noBar = 0;

for (const d of decisions) {
    const series = candles[`${d.platform}:${d.symbol}`];
    if (!series?.length) { noSeries += 1; continue; }
    const ts = Number(d.decided_at_ms);
    const price = Number(d.price), atr = Number(d.atr);
    if (!(price > 0) || !(atr > 0)) continue;

    // first bar that CLOSES strictly after the decision
    const j = series.findIndex((c) => c[0] + BAR_MS > ts);
    if (j < 0) { noBar += 1; continue; }
    // decision must sit inside/adjacent to the series, not before it starts
    if (series[j][0] > ts + BAR_MS) { noBar += 1; continue; }

    const dir = d.action === 'BUY' ? 1 : -1;
    const row: Record<string, unknown> = {
        id: d.id, ts, day: new Date(ts).toISOString().slice(0, 10),
        symbol: d.symbol, platform: d.platform, category: d.category,
        action: d.action, dir, price, atr,
        atrPct: (atr / price) * 100,
        signal: d.signal_strength, strategy: d.strategy, model: d.ai_model,
        placed: d.placed === true, riskUsd: d.risk_usd,
        slAtr: d.sl != null && d.sl > 0 ? Math.abs(price - d.sl) / atr : null,
        tpAtr: d.tp != null && d.tp > 0 ? Math.abs(d.tp - price) / atr : null,
    };
    for (const h of HORIZONS) {
        const end = j + h - 1;
        if (end >= series.length) { row[`y${h}`] = null; row[`u${h}`] = null; continue; }
        const seg = series.slice(j, end + 1);
        const close = series[end][4];
        const u = (close - price) / atr;          // unconditional (long-side) move
        row[`u${h}`] = u;
        row[`y${h}`] = dir * u;                   // signed by the engine's call
        const hi = Math.max(...seg.map((c) => c[2]));
        const lo = Math.min(...seg.map((c) => c[3]));
        row[`mfe${h}`] = dir === 1 ? (hi - price) / atr : (price - lo) / atr;
        row[`mae${h}`] = dir === 1 ? (price - lo) / atr : (hi - price) / atr;
    }
    rows.push(row);
}

writeFileSync('.study/panel.json', JSON.stringify(rows));
const cov = (h: number) => rows.filter((r) => r[`y${h}`] != null).length;
console.log(`panel rows ${rows.length} / ${decisions.length}  (noSeries=${noSeries} noBar=${noBar})`);
console.log('coverage by horizon:', HORIZONS.map((h) => `h${h}=${cov(h)}`).join('  '));
console.log('span:', rows[0]?.day, '->', rows[rows.length - 1]?.day);
