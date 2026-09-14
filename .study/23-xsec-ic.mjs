// CROSS-SECTIONAL FACTOR TEST on the widened panel.
//
// This is what widening breadth actually buys. The veto test was limited by 453
// AI decisions and more reference bars cannot fix that. A cross-sectional test
// is limited by (symbols x dates), which is exactly what just grew.
//
// At each 4H timestamp: rank all available symbols by a signal, correlate with
// forward 24h return (Spearman). Rank IC is inherently market-neutral, so a
// market-wide move cannot create it.
//
// PRE-SPECIFIED: 4 signals, horizon 6 bars. Bonferroni for 4 tests => p<0.0125.
import { readFileSync } from 'node:fs';

const DIR = '.study/panel-wide';
const H = 6, MIN_XSEC = 20;
const manifest = JSON.parse(readFileSync(`${DIR}/_manifest.json`, 'utf8'));

const ema = (v, n) => { const k = 2 / (n + 1); const o = []; let p = v[0];
    for (let i = 0; i < v.length; i++) { p = i ? v[i] * k + p * (1 - k) : v[0]; o.push(p); } return o; };
function atr14(c) { const o = new Array(c.length).fill(null); let prev = null, a = null;
    for (let i = 0; i < c.length; i++) { const tr = prev === null ? c[i][2] - c[i][3]
        : Math.max(c[i][2] - c[i][3], Math.abs(c[i][2] - prev), Math.abs(c[i][3] - prev));
        a = i === 0 ? tr : (a * 13 + tr) / 14; prev = c[i][4]; if (i >= 14) o[i] = a; } return o; }

// per-symbol feature series keyed by bar open time
const series = new Map();
for (const m of manifest) {
    const c = JSON.parse(readFileSync(`${DIR}/${m.symbol}.json`, 'utf8'));
    if (c.length < 200) continue;
    const cl = c.map((x) => x[4]);
    const A = atr14(c), E20 = ema(cl, 20), E50 = ema(cl, 50);
    const byTs = new Map();
    for (let i = 60; i < c.length - H; i++) {
        const a = A[i]; if (!(a > 0) || !(cl[i] > 0)) continue;
        byTs.set(c[i][0], {
            mom30: (cl[i] - cl[i - 30]) / a,          // 5-day momentum
            rev6:  (cl[i] - cl[i - 6]) / a,           // 1-day move (reversal candidate)
            trend: (E20[i] - E50[i]) / a,
            vol:   (a / cl[i]) * 100,                 // ATR%
            fwd:   (cl[i + H] - cl[i]) / a,           // forward 24h, ATR units
        });
    }
    series.set(m.symbol, byTs);
}
const allTs = new Set();
for (const byTs of series.values()) for (const ts of byTs.keys()) allTs.add(ts);
const timestamps = [...allTs].sort((a, b) => a - b);
console.log(`symbols: ${series.size}   timestamps: ${timestamps.length.toLocaleString()}`);

const rank = (xs) => {
    const idx = xs.map((v, i) => [v, i]).sort((a, b) => a[0] - b[0]);
    const r = new Array(xs.length);
    for (let i = 0; i < idx.length;) {
        let j = i; while (j + 1 < idx.length && idx[j + 1][0] === idx[i][0]) j++;
        const avg = (i + j) / 2 + 1;
        for (let k = i; k <= j; k++) r[idx[k][1]] = avg;
        i = j + 1;
    } return r;
};
const pearson = (a, b) => {
    const n = a.length, ma = a.reduce((x, y) => x + y, 0) / n, mb = b.reduce((x, y) => x + y, 0) / n;
    let sab = 0, sa = 0, sb = 0;
    for (let i = 0; i < n; i++) { const da = a[i] - ma, db = b[i] - mb; sab += da * db; sa += da * da; sb += db * db; }
    return sa > 0 && sb > 0 ? sab / Math.sqrt(sa * sb) : 0;
};

const SIGNALS = [
    ['mom30 (5d momentum)', (f) => f.mom30],
    ['rev6  (1d reversal)', (f) => -f.rev6],
    ['trend (EMA20-EMA50)', (f) => f.trend],
    ['lowvol (-ATR%)',      (f) => -f.vol],
];

const mean = (a) => a.reduce((x, y) => x + y, 0) / a.length;
const sd = (a) => { const m = mean(a); return Math.sqrt(a.reduce((s, x) => s + (x - m) ** 2, 0) / (a.length - 1)); };

console.log('\n  signal                  n_dates  breadth  mean_IC     ICIR    t_all   t_nonoverlap');
for (const [label, fn] of SIGNALS) {
    const ics = [];
    for (const ts of timestamps) {
        const sv = [], fv = [];
        for (const byTs of series.values()) {
            const f = byTs.get(ts); if (!f) continue;
            const s = fn(f); if (!Number.isFinite(s) || !Number.isFinite(f.fwd)) continue;
            sv.push(s); fv.push(f.fwd);
        }
        if (sv.length < MIN_XSEC) continue;
        ics.push({ ts, n: sv.length, ic: pearson(rank(sv), rank(fv)) });
    }
    if (ics.length < 50) { console.log(`  ${label.padEnd(22)} too few dates`); continue; }
    const v = ics.map((x) => x.ic);
    const m = mean(v), s = sd(v);
    const tAll = m / (s / Math.sqrt(v.length));
    // non-overlapping: every Hth date, so forward windows do not share bars
    const nov = ics.filter((_, i) => i % H === 0).map((x) => x.ic);
    const tNov = mean(nov) / (sd(nov) / Math.sqrt(nov.length));
    const breadth = Math.round(mean(ics.map((x) => x.n)));
    console.log(`  ${label.padEnd(22)} ${String(ics.length).padStart(6)}  ${String(breadth).padStart(7)}  ${m.toFixed(4).padStart(7)}  ${(m / s).toFixed(3).padStart(6)}  ${tAll.toFixed(2).padStart(6)}  ${tNov.toFixed(2).padStart(9)}`);
}
console.log('\n  t_nonoverlap is the honest one (no shared forward windows).');
console.log('  Bonferroni for 4 pre-specified tests: |t| > 2.50 (p<0.0125).');
