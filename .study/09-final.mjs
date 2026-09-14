import { readFileSync } from 'node:fs';
const rows = JSON.parse(readFileSync('.study/panel.json', 'utf8'));
const REPS = 20000;
let seed = 99001; const rnd = () => ((seed = (seed * 1103515245 + 12345) & 0x7fffffff) / 0x7fffffff);
const mean = (a) => a.reduce((x, y) => x + y, 0) / a.length;

function blockCI(sample, key, blockDays) {
    const usable = sample.filter((r) => r[key] != null);
    const days = [...new Set(usable.map((r) => r.day))].sort();
    const idx = new Map(days.map((d, i) => [d, i]));
    const byDay = days.map(() => []);
    for (const r of usable) byDay[idx.get(r.day)].push(r[key]);
    const D = days.length, nB = Math.ceil(D / blockDays), ms = [];
    for (let b = 0; b < REPS; b++) {
        const pool = [];
        for (let k = 0; k < nB; k++) { const s = Math.floor(rnd() * D); for (let o = 0; o < blockDays; o++) pool.push(...byDay[(s + o) % D]); }
        ms.push(mean(pool));
    }
    ms.sort((a, b) => a - b);
    return { m: mean(usable.map((r) => r[key])), lo: ms[Math.floor(0.025 * REPS)], hi: ms[Math.floor(0.975 * REPS)], n: usable.length };
}

console.log('CONFIDENCE BANDS (moving-block bootstrap over days, block matched to horizon)');
for (const [h, bd] of [[1, 2], [2, 2], [6, 2], [12, 3]]) {
    const c = blockCI(rows, `y${h}`, bd);
    console.log(`  h=${String(h).padStart(2)} (${String(h * 4).padStart(2)}h)  n=${c.n}  mean=${c.m.toFixed(3)} ATR  95% CI [${c.lo.toFixed(3)}, ${c.hi.toFixed(3)}]` +
        `   ${c.lo > 0 ? 'POSITIVE' : c.hi < 0 ? 'NEGATIVE (excludes 0)' : 'includes 0'}`);
}

console.log('\nCurrent engine only (post-overhaul, 09-02 12:00Z ->):');
const v2 = rows.filter((r) => r.ts >= Date.parse('2026-09-02T12:00:00Z'));
for (const [h, bd] of [[6, 2], [12, 3]]) {
    const c = blockCI(v2, `y${h}`, bd);
    console.log(`  h=${String(h).padStart(2)}  n=${c.n}  mean=${c.m.toFixed(3)} ATR  95% CI [${c.lo.toFixed(3)}, ${c.hi.toFixed(3)}]  -> includes 0: ${c.lo <= 0 && c.hi >= 0}`);
}

console.log('\nMULTIPLICITY ACCOUNTING');
const tests = ['4 horizons', '2 direction splits', '5 subsets (platform x2, signal x2, placed)', '3 regimes + 2 sub-periods'];
console.log(`  tests run in this session: ${4 + 2 + 5 + 5} across ${tests.join(', ')}`);
console.log('  Bonferroni-adjusted 5% threshold for 16 tests: p < 0.0031');
console.log('  strongest single result: h=12 all decisions, p_block = 0.0079  -> does NOT clear it');
console.log('  Harvey-Liu (2014) bar for a claimed new effect: |t| > 3.0');
console.log('  h=12 t_day = -3.19 clears that; h=12 t_week = -2.22 (G=11 clusters) does not.');
console.log('  With only 11 week-clusters the week-clustered SE is itself unreliable, so the');
console.log('  honest read is: the h=12 result sits right ON the bar, not clearly past it.');
