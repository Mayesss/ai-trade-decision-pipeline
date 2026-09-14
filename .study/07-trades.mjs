// Realized risk-adjusted returns: R = pnl_net / risk_usd budgeted at entry.
import { readFileSync } from 'node:fs';
const trades = JSON.parse(readFileSync('.study/trades.json', 'utf8'));
const REPS = 20000;
let seed = 424242; const rnd = () => ((seed = (seed * 1103515245 + 12345) & 0x7fffffff) / 0x7fffffff);
const mean = (a) => a.reduce((x, y) => x + y, 0) / a.length;
const sd = (a) => { const m = mean(a); return Math.sqrt(a.reduce((s, x) => s + (x - m) ** 2, 0) / (a.length - 1)); };
const day = (ms) => new Date(Number(ms)).toISOString().slice(0, 10);

function clusterT(vals, keys) {
    const n = vals.length, m = mean(vals);
    const g = new Map();
    for (let i = 0; i < n; i++) g.set(keys[i], (g.get(keys[i]) || 0) + (vals[i] - m));
    const G = g.size; let meat = 0; for (const s of g.values()) meat += s * s;
    const se = Math.sqrt((meat / (n * n)) * (G / Math.max(1, G - 1)));
    return { n, G, m, se, t: m / se };
}
function bootCI(vals, keys) {
    const g = new Map();
    for (let i = 0; i < vals.length; i++) (g.get(keys[i]) ?? g.set(keys[i], []).get(keys[i])).push(vals[i]);
    const blocks = [...g.values()];
    const ms = [];
    for (let b = 0; b < REPS; b++) {
        const pool = [];
        for (let k = 0; k < blocks.length; k++) pool.push(...blocks[Math.floor(rnd() * blocks.length)]);
        ms.push(mean(pool));
    }
    ms.sort((a, b) => a - b);
    return [ms[Math.floor(0.025 * REPS)], ms[Math.floor(0.975 * REPS)]];
}

const measured = trades.filter((t) => t.risk_usd > 0 && t.pnl_net != null)
    .map((t) => ({ ...t, R: t.pnl_net / t.risk_usd, day: day(t.exit_ts_ms) }));

console.log(`closed trades total: ${trades.length}`);
console.log(`with a risk budget on the placing decision (R-measurable): ${measured.length}`);
if (measured.length) {
    console.log(`R sample spans ${measured[0].day} -> ${measured[measured.length - 1].day}`);
}

const T_FREEZE = Date.parse('2026-09-11T00:00:00Z');
for (const [label, set] of [
    ['ALL R-measurable trades', measured],
    ['since 09-11 freeze', measured.filter((t) => Number(t.exit_ts_ms) >= T_FREEZE)],
    ['overhaul week 09-02..09-11', measured.filter((t) => Number(t.exit_ts_ms) >= Date.parse('2026-09-02T12:00:00Z') && Number(t.exit_ts_ms) < T_FREEZE)],
]) {
    if (set.length < 5) { console.log(`\n${label}: n=${set.length} -- not testable`); continue; }
    const v = set.map((t) => t.R);
    const c = clusterT(v, set.map((t) => t.day));
    const ci = bootCI(v, set.map((t) => t.day));
    const wins = v.filter((x) => x > 0).length;
    console.log(`\n${label}`);
    console.log(`  n=${c.n} (days=${c.G})  mean R=${c.m.toFixed(3)}  sd=${sd(v).toFixed(2)}  t_day=${c.t.toFixed(2)}` +
        `  95% CI [${ci[0].toFixed(3)}, ${ci[1].toFixed(3)}]`);
    console.log(`  win rate=${(100 * wins / v.length).toFixed(1)}%  sum R=${v.reduce((a, b) => a + b, 0).toFixed(1)}` +
        `  worst=${Math.min(...v).toFixed(2)}R  best=${Math.max(...v).toFixed(2)}R`);
}

// how much of the closed book is even measurable
const byWeek = new Map();
for (const t of trades) {
    const k = day(t.exit_ts_ms).slice(0, 7);
    const e = byWeek.get(k) ?? byWeek.set(k, { n: 0, m: 0 }).get(k);
    e.n++; if (t.risk_usd > 0) e.m++;
}
console.log('\nR-measurability by month (risk_usd present on the placing decision):');
for (const [k, e] of [...byWeek].sort()) console.log(`  ${k}  closed=${String(e.n).padStart(3)}  measurable=${String(e.m).padStart(3)}  (${(100 * e.m / e.n).toFixed(0)}%)`);
