// Overlapping-window-safe inference.
// At h=12 a position window spans 48h while decisions arrive every 4h, so
// adjacent DAYS are correlated too. Day clusters are not enough: use a moving
// BLOCK bootstrap over the day axis with block length matched to the horizon,
// plus week clustering as a second opinion.
import { readFileSync } from 'node:fs';
const rows = JSON.parse(readFileSync('.study/panel.json', 'utf8'));
const REPS = 20000;
let seed = 7717; const rnd = () => ((seed = (seed * 1103515245 + 12345) & 0x7fffffff) / 0x7fffffff);
const mean = (a) => a.reduce((x, y) => x + y, 0) / a.length;
const sd = (a) => { const m = mean(a); return Math.sqrt(a.reduce((s, x) => s + (x - m) ** 2, 0) / (a.length - 1)); };

function clusterT(vals, keys) {
    const n = vals.length, m = mean(vals);
    const g = new Map();
    for (let i = 0; i < n; i++) g.set(keys[i], (g.get(keys[i]) || 0) + (vals[i] - m));
    const G = g.size; let meat = 0; for (const s of g.values()) meat += s * s;
    const se = Math.sqrt((meat / (n * n)) * (G / Math.max(1, G - 1)));
    return { n, G, m, se, t: m / se };
}

// moving-block bootstrap over the ordered day axis
function movingBlockP(sample, key, blockDays) {
    const usable = sample.filter((r) => r[key] != null);
    const days = [...new Set(usable.map((r) => r.day))].sort();
    const idx = new Map(days.map((d, i) => [d, i]));
    const byDay = days.map(() => []);
    for (const r of usable) byDay[idx.get(r.day)].push(r[key]);
    const D = days.length, obs = mean(usable.map((r) => r[key]));
    const nBlocks = Math.ceil(D / blockDays);
    let ge = 0;
    for (let b = 0; b < REPS; b++) {
        const pool = [];
        for (let k = 0; k < nBlocks; k++) {
            const start = Math.floor(rnd() * D);
            for (let o = 0; o < blockDays; o++) pool.push(...byDay[(start + o) % D]);
        }
        if (pool.length && Math.abs(mean(pool) - obs) >= Math.abs(obs)) ge++;
    }
    return (ge + 1) / (REPS + 1);
}

const weekOf = (day) => { const d = new Date(day + 'T00:00:00Z'); d.setUTCDate(d.getUTCDate() - d.getUTCDay()); return d.toISOString().slice(0, 10); };

console.log('A. HORIZON CURVE under progressively stricter dependence assumptions');
console.log('   h    n   mean     t_day    t_week   p_block(matched)   blockDays');
for (const h of [1, 2, 6, 12]) {
    const u = rows.filter((r) => r[`y${h}`] != null);
    const v = u.map((r) => r[`y${h}`]);
    const cd = clusterT(v, u.map((r) => r.day));
    const cw = clusterT(v, u.map((r) => weekOf(r.day)));
    const blockDays = Math.max(2, Math.ceil((h * 4) / 24) + 1);
    const pb = movingBlockP(rows, `y${h}`, blockDays);
    console.log(`  ${String(h).padStart(2)} ${String(cd.n).padStart(4)}  ${cd.m.toFixed(3).padStart(6)}  ` +
        `${cd.t.toFixed(2).padStart(6)}  ${cw.t.toFixed(2).padStart(7)} (G=${cw.G})   ${pb.toFixed(4).padStart(6)}` +
        `            ${blockDays}`);
}

console.log('');
console.log('B. REGIME STABILITY -- is the sign stable, or is it one period?');
const T_OVER = Date.parse('2026-09-02T12:00:00Z');
console.log('   period                      h=1      h=2      h=6      h=12     n(h=6)');
const periods = [
    ['v1 pre-overhaul', (r) => r.ts < T_OVER],
    ['  ... Jun 27 - Jul 31', (r) => r.ts < Date.parse('2026-08-01T00:00:00Z')],
    ['  ... Aug 01 - Sep 02', (r) => r.ts >= Date.parse('2026-08-01T00:00:00Z') && r.ts < T_OVER],
    ['v2 overhaul week', (r) => r.ts >= T_OVER],
];
for (const [label, f] of periods) {
    const s = rows.filter(f);
    const cells = [1, 2, 6, 12].map((h) => { const v = s.filter((r) => r[`y${h}`] != null).map((r) => r[`y${h}`]); return v.length ? mean(v).toFixed(3).padStart(7) : '     --'; });
    console.log(`   ${label.padEnd(26)} ${cells.join('  ')}   ${s.filter((r) => r.y6 != null).length}`);
}

console.log('');
console.log('C. MONTHLY mean y6 (sign stability, equal-weight by month)');
const byMonth = new Map();
for (const r of rows.filter((x) => x.y6 != null)) { const k = r.day.slice(0, 7); (byMonth.get(k) ?? byMonth.set(k, []).get(k)).push(r.y6); }
for (const [k, v] of [...byMonth].sort()) console.log(`   ${k}  n=${String(v.length).padStart(3)}  mean=${mean(v).toFixed(3).padStart(7)}  sd=${sd(v).toFixed(2)}`);
