import { readFileSync } from 'node:fs';
const rows = JSON.parse(readFileSync('.study/panel.json', 'utf8'));
const candles = JSON.parse(readFileSync('.study/candles.json', 'utf8'));
const BAR_MS = 4 * 3600_000;
const mean = (a) => a.reduce((x, y) => x + y, 0) / a.length;
const med = (a) => { if (!a.length) return NaN; const s = [...a].sort((x, y) => x - y); return s[Math.floor(s.length / 2)]; };

console.log('1. PRICE / CANDLE ALIGNMENT  (snapshot price vs the OHLC of the bar containing it)');
console.log('   a systematic bid/mid/ask mismatch or a wrong series shows up as price outside [low,high]');
const byPlat = {};
for (const r of rows) {
    const s = candles[`${r.platform}:${r.symbol}`];
    const j = s.findIndex((c) => c[0] + BAR_MS > r.ts);
    if (j < 0) continue;
    const [, , hi, lo] = s[j];
    const inside = r.price >= lo && r.price <= hi;
    // signed distance outside the bar, in ATR
    const off = r.price > hi ? (r.price - hi) / r.atr : r.price < lo ? (r.price - lo) / r.atr : 0;
    (byPlat[r.platform] ??= []).push({ inside, off, rel: (r.price - (hi + lo) / 2) / r.atr });
}
for (const [p, a] of Object.entries(byPlat)) {
    console.log(`   ${p.padEnd(9)} n=${a.length}  inside bar: ${(100 * a.filter((x) => x.inside).length / a.length).toFixed(1)}%` +
        `  median |offset| when outside: ${med(a.filter((x) => !x.inside).map((x) => Math.abs(x.off))).toFixed(3)} ATR` +
        `  mean signed offset: ${mean(a.map((x) => x.off)).toFixed(4)} ATR`);
}

console.log('');
console.log('2. IS THE PERMUTATION NULL DEGENERATE?  (it only has bite on days holding both longs and shorts)');
const byDay = new Map();
for (const r of rows) (byDay.get(r.day) ?? byDay.set(r.day, []).get(r.day)).push(r);
let mixedDays = 0, mixedObs = 0;
for (const [, g] of byDay) {
    const hasL = g.some((r) => r.dir === 1), hasS = g.some((r) => r.dir === -1);
    if (hasL && hasS) { mixedDays++; mixedObs += g.length; }
}
console.log(`   days=${byDay.size}  days with BOTH directions=${mixedDays}  obs on those days=${mixedObs}/${rows.length}` +
    ` (${(100 * mixedObs / rows.length).toFixed(0)}%)`);

console.log('');
console.log('3. MEAN y BY HORIZON AND DIRECTION  (a constant price offset would push longs and shorts OPPOSITE ways)');
console.log('   h   n_long  mean_long   n_short  mean_short   mean_u (tape drift, long-side)');
for (const h of [1, 2, 6, 12]) {
    const u = rows.filter((r) => r[`y${h}`] != null);
    const L = u.filter((r) => r.dir === 1).map((r) => r[`y${h}`]);
    const S = u.filter((r) => r.dir === -1).map((r) => r[`y${h}`]);
    console.log(`   ${String(h).padStart(2)}  ${String(L.length).padStart(6)}  ${mean(L).toFixed(4).padStart(9)}   ` +
        `${String(S.length).padStart(7)}  ${mean(S).toFixed(4).padStart(10)}   ${mean(u.map((r) => r[`u${h}`])).toFixed(4).padStart(7)}`);
}

console.log('');
console.log('4. OUTLIER / CONCENTRATION CHECK at h=12 (the strongest result)');
const h12 = rows.filter((r) => r.y12 != null);
const vals = h12.map((r) => r.y12);
console.log(`   mean=${mean(vals).toFixed(3)}  median=${med(vals).toFixed(3)}  ` +
    `winsorised 5/95=${mean(((a) => { const s = [...a].sort((x, y) => x - y); const lo = s[Math.floor(0.05 * s.length)], hi = s[Math.floor(0.95 * s.length)]; return a.map((v) => Math.min(hi, Math.max(lo, v))); })(vals)).toFixed(3)}`);
const bySym = new Map();
for (const r of h12) (bySym.get(r.symbol) ?? bySym.set(r.symbol, []).get(r.symbol)).push(r.y12);
const symStats = [...bySym.entries()].map(([s, v]) => ({ s, n: v.length, m: mean(v) })).sort((a, b) => a.m - b.m);
console.log('   worst 5 symbols:', symStats.slice(0, 5).map((x) => `${x.s}(n=${x.n},${x.m.toFixed(2)})`).join(' '));
console.log('   best  5 symbols:', symStats.slice(-5).map((x) => `${x.s}(n=${x.n},${x.m.toFixed(2)})`).join(' '));
console.log(`   symbols with mean<0: ${symStats.filter((x) => x.m < 0).length}/${symStats.length}`);
const exBtc = h12.filter((r) => r.symbol !== 'BTCUSDT').map((r) => r.y12);
console.log(`   excluding BTCUSDT (n=${exBtc.length}): mean=${mean(exBtc).toFixed(3)}`);
console.log(`   median per-symbol mean: ${med(symStats.map((x) => x.m)).toFixed(3)}`);
