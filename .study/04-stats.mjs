// Inference on the decision panel.
//
// H0: the engine's directional calls carry no information about forward
//     volatility-normalised returns, i.e. E[y_h] = 0.
//
// Dependence: decisions overlap in time and are correlated across symbols on
// the same day (one macro tape drives 25 instruments). All inference clusters
// on calendar day; the naive iid t-stat is reported only to show the inflation.
import { readFileSync } from 'node:fs';

const rows = JSON.parse(readFileSync('.study/panel.json', 'utf8'));
const REPS = 20000;

// deterministic RNG so the numbers reproduce
let seed = 20260911;
const rnd = () => ((seed = (seed * 1103515245 + 12345) & 0x7fffffff) / 0x7fffffff);

const mean = (a) => a.reduce((x, y) => x + y, 0) / a.length;
const sd = (a) => { const m = mean(a); return Math.sqrt(a.reduce((s, x) => s + (x - m) ** 2, 0) / (a.length - 1)); };
const groupBy = (a, k) => { const m = new Map(); for (const r of a) { const g = k(r); (m.get(g) ?? m.set(g, []).get(g)).push(r); } return m; };

// Cluster-robust SE of a mean, clustering on day.
function clusteredT(vals, days) {
    const n = vals.length, m = mean(vals);
    const byDay = new Map();
    for (let i = 0; i < n; i++) byDay.set(days[i], (byDay.get(days[i]) || 0) + (vals[i] - m));
    const G = byDay.size;
    let meat = 0; for (const s of byDay.values()) meat += s * s;
    const varMean = (meat / (n * n)) * (G / Math.max(1, G - 1));
    const se = Math.sqrt(varMean);
    return { n, G, mean: m, sd: sd(vals), se, t: m / se };
}

// Block bootstrap: resample DAYS with replacement, recentre for the null.
function bootP(vals, days) {
    const byDay = groupBy(vals.map((v, i) => ({ v, d: days[i] })), (r) => r.d);
    const blocks = [...byDay.values()].map((g) => g.map((r) => r.v));
    const obs = mean(vals);
    let ge = 0;
    for (let b = 0; b < REPS; b++) {
        const pool = [];
        for (let k = 0; k < blocks.length; k++) pool.push(...blocks[Math.floor(rnd() * blocks.length)]);
        if (Math.abs(mean(pool) - obs) >= Math.abs(obs)) ge++;
    }
    return (ge + 1) / (REPS + 1);
}

// Permutation: shuffle the engine's direction labels WITHIN each day.
// Holds the day's long/short mix and the cross-section of available moves
// fixed, so this isolates skill at picking WHICH instrument to trade WHICH way
// -- market drift and a persistent long bias cannot produce it.
function permP(sample, hKey) {
    const usable = sample.filter((r) => r[hKey] != null);
    const obs = mean(usable.map((r) => r[hKey]));
    const byDay = [...groupBy(usable, (r) => r.day).values()];
    let ge = 0;
    for (let b = 0; b < REPS; b++) {
        let sum = 0, cnt = 0;
        for (const g of byDay) {
            const dirs = g.map((r) => r.dir);
            for (let i = dirs.length - 1; i > 0; i--) { const j = Math.floor(rnd() * (i + 1)); [dirs[i], dirs[j]] = [dirs[j], dirs[i]]; }
            for (let i = 0; i < g.length; i++) { sum += dirs[i] * g[i][`u${hKey.slice(1)}`]; cnt++; }
        }
        if (Math.abs(sum / cnt) >= Math.abs(obs)) ge++;
    }
    return (ge + 1) / (REPS + 1);
}

function report(label, sample, h, { perm = true } = {}) {
    const key = `y${h}`;
    const usable = sample.filter((r) => r[key] != null);
    if (usable.length < 20) { console.log(`${label.padEnd(34)} n=${usable.length}  -- too few to test`); return null; }
    const vals = usable.map((r) => r[key]);
    const days = usable.map((r) => r.day);
    const c = clusteredT(vals, days);
    const naiveT = c.mean / (c.sd / Math.sqrt(c.n));
    const pb = bootP(vals, days);
    const pp = perm ? permP(usable, key) : null;
    const hit = usable.filter((r) => r[key] > 0).length / usable.length;
    console.log(
        `${label.padEnd(34)} n=${String(c.n).padStart(4)} days=${String(c.G).padStart(3)}` +
        ` mean=${c.mean.toFixed(4)} sd=${c.sd.toFixed(3)}` +
        ` t_iid=${naiveT.toFixed(2)} t_clu=${c.t.toFixed(2)}` +
        ` p_boot=${pb.toFixed(3)}${pp != null ? ` p_perm=${pp.toFixed(3)}` : ''}` +
        ` hit=${(hit * 100).toFixed(1)}%`,
    );
    return { ...c, naiveT, pb, pp, hit };
}

console.log('='.repeat(132));
console.log('A. PRIMARY TEST -- all directional decisions, by horizon (h in 4H bars). Primary horizon h=6 (24h).');
console.log('='.repeat(132));
for (const h of [1, 2, 6, 12]) report(`h=${h} (${h * 4}h) all decisions`, rows, h);

console.log('');
console.log('B. WHERE COULD A POSITIVE MEAN COME FROM? Decomposition at h=6');
console.log('='.repeat(132));
const u6 = rows.filter((r) => r.u6 != null);
const longs = u6.filter((r) => r.dir === 1), shorts = u6.filter((r) => r.dir === -1);
console.log(`unconditional drift of the traded tape: mean u6 = ${mean(u6.map((r) => r.u6)).toFixed(4)} ATR (long-side move, all ticks)`);
console.log(`direction mix: ${longs.length} long / ${shorts.length} short = ${(100 * longs.length / u6.length).toFixed(1)}% long`);
console.log(`drift-riding component: mean(dir) * mean(u6) = ${(mean(u6.map((r) => r.dir)) * mean(u6.map((r) => r.u6))).toFixed(4)}`);
report('  longs only (h=6)', longs, 6, { perm: false });
report('  shorts only (h=6)', shorts, 6, { perm: false });

console.log('');
console.log('C. SUBSETS at h=6 (each is an extra test -- see multiplicity note)');
console.log('='.repeat(132));
for (const [label, f] of [
    ['crypto (bitget)', (r) => r.platform === 'bitget'],
    ['capital (idx/cmdty/fx)', (r) => r.platform === 'capital'],
    ['signal HIGH', (r) => r.signal === 'HIGH'],
    ['signal MEDIUM', (r) => r.signal === 'MEDIUM'],
    ['actually placed', (r) => r.placed === true],
]) report(label, rows.filter(f), 6, { perm: false });

console.log('');
console.log('D. REGIMES -- the engine is not one fixed model');
console.log('='.repeat(132));
const T_OVERHAUL = Date.parse('2026-09-02T12:00:00Z');
const T_FREEZE = Date.parse('2026-09-11T00:00:00Z');
for (const [label, f] of [
    ['v1 pre-overhaul (-> 09-02)', (r) => r.ts < T_OVERHAUL],
    ['v2 overhaul week (09-02->09-11)', (r) => r.ts >= T_OVERHAUL && r.ts < T_FREEZE],
    ['v3 since freeze (09-11 ->)', (r) => r.ts >= T_FREEZE],
]) report(label, rows.filter(f), 6, { perm: false });

console.log('');
console.log('E. POWER -- what edge could this sample even detect at h=6?');
console.log('='.repeat(132));
const all6 = rows.filter((r) => r.y6 != null);
const c6 = clusteredT(all6.map((r) => r.y6), all6.map((r) => r.day));
const nEff = (c6.sd / c6.se) ** 2; // effective independent obs implied by clustering
console.log(`nominal n=${c6.n}, distinct days=${c6.G}, effective n implied by day-clustering = ${nEff.toFixed(0)}`);
for (const [lab, crit] of [['t>1.96 (nominal 5%)', 1.96], ['t>3.0 (Harvey-Liu multiple-testing bar)', 3.0]]) {
    const mde80 = (crit + 0.84) * c6.sd / Math.sqrt(nEff);
    console.log(`  ${lab.padEnd(42)} detectable mean at 80% power: ${mde80.toFixed(3)} ATR/trade` +
                `  -> n needed for a true 0.10 ATR edge: ${Math.ceil(((crit + 0.84) * c6.sd / 0.10) ** 2)}`);
}
