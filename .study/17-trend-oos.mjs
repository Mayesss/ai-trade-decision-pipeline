// The veto diagnostic threw up a candidate: counter-trend decisions did far
// worse. That was found POST-HOC on 435 decisions, so it is a hypothesis, not
// a finding. Test the underlying mechanism on data it was NOT discovered in:
// all 73k reference bars, independent of any AI decision.
//
// H: forward return is positively related to trend state (EMA20 vs EMA50).
import { readFileSync } from 'node:fs';
const H = 6;
const candles = JSON.parse(readFileSync('.study/candles-deep.json', 'utf8'));
const ema = (v, n) => { const k = 2 / (n + 1); const o = []; let p = v[0];
    for (let i = 0; i < v.length; i++) { p = i ? v[i] * k + p * (1 - k) : v[0]; o.push(p); } return o; };
function atr14(c) { const o = new Array(c.length).fill(null); let prev = null, a = null;
    for (let i = 0; i < c.length; i++) { const tr = prev === null ? c[i][2] - c[i][3]
        : Math.max(c[i][2] - c[i][3], Math.abs(c[i][2] - prev), Math.abs(c[i][3] - prev));
        a = i === 0 ? tr : (a * 13 + tr) / 14; prev = c[i][4]; if (i >= 14) o[i] = a; } return o; }

const obs = [];
for (const [key, c] of Object.entries(candles)) {
    if (!c || c.length < 80) continue;
    const closes = c.map((x) => x[4]);
    const A = atr14(c), E20 = ema(closes, 20), E50 = ema(closes, 50);
    for (let i = 60; i < c.length - H; i++) {
        const a = A[i]; if (!(a > 0)) continue;
        obs.push({
            key, day: new Date(c[i][0]).toISOString().slice(0, 10),
            up: E20[i] > E50[i],
            sep: (E20[i] - E50[i]) / a,
            fwd: (closes[i + H] - closes[i]) / a,
        });
    }
}
const mean = (a) => a.reduce((x, y) => x + y, 0) / a.length;
function clusterT(vals, keys) {
    const n = vals.length, m = mean(vals), g = new Map();
    for (let i = 0; i < n; i++) g.set(keys[i], (g.get(keys[i]) || 0) + (vals[i] - m));
    const G = g.size; let meat = 0; for (const s of g.values()) meat += s * s;
    const se = Math.sqrt((meat / (n * n)) * (G / Math.max(1, G - 1)));
    return { n, G, m, se, t: m / se };
}
console.log(`reference observations: ${obs.length.toLocaleString()} bars, ${new Set(obs.map(o=>o.day)).size} distinct days\n`);

// A trend-following position: long when EMA20>EMA50, short otherwise.
const tf = obs.map((o) => ({ ...o, y: (o.up ? 1 : -1) * o.fwd }));
const c = clusterT(tf.map((o) => o.y), tf.map((o) => o.day));
console.log('TREND-FOLLOWING on the full reference panel (long if EMA20>EMA50, else short):');
console.log(`  n=${c.n.toLocaleString()}  days=${c.G}  mean=${c.m.toFixed(4)} ATR  t_day=${c.t.toFixed(2)}  95%CI [${(c.m-1.96*c.se).toFixed(3)}, ${(c.m+1.96*c.se).toFixed(3)}]`);
console.log(`  win rate=${(100 * tf.filter((o) => o.y > 0).length / tf.length).toFixed(1)}%`);

// split by venue and by period, to see whether it is one regime
console.log('\n  by platform:');
for (const p of ['bitget', 'capital']) {
    const s = tf.filter((o) => o.key.startsWith(p));
    const cc = clusterT(s.map((o) => o.y), s.map((o) => o.day));
    console.log(`    ${p.padEnd(8)} n=${String(cc.n).padStart(6)}  mean=${cc.m.toFixed(4)}  t=${cc.t.toFixed(2)}`);
}
console.log('\n  by half (is it one regime?):');
const sorted = [...tf].sort((a, b) => a.day.localeCompare(b.day));
const mid = Math.floor(sorted.length / 2);
for (const [lab, s] of [['first half', sorted.slice(0, mid)], ['second half', sorted.slice(mid)]]) {
    const cc = clusterT(s.map((o) => o.y), s.map((o) => o.day));
    console.log(`    ${lab.padEnd(12)} ${s[0].day}..${s[s.length-1].day}  n=${String(cc.n).padStart(6)}  mean=${cc.m.toFixed(4)}  t=${cc.t.toFixed(2)}`);
}
console.log('\n  the AI decision window only (2026-06-27 onward):');
const win = tf.filter((o) => o.day >= '2026-06-27');
const cw = clusterT(win.map((o) => o.y), win.map((o) => o.day));
console.log(`    n=${cw.n.toLocaleString()}  mean=${cw.m.toFixed(4)}  t=${cw.t.toFixed(2)}`);
