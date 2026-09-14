// Was the AI trading momentum into a mean-reverting tape?
// One pre-specified question: does the AI's chosen direction line up with the
// recent 24h move (momentum-chasing) or against it (reversal)?
import { readFileSync } from 'node:fs';
const BAR_MS = 4 * 3600_000;
const candles = JSON.parse(readFileSync('.study/candles-deep.json', 'utf8'));
const panel = JSON.parse(readFileSync('.study/panel.json', 'utf8'));
function atr14(c) { const o = new Array(c.length).fill(null); let prev = null, a = null;
    for (let i = 0; i < c.length; i++) { const tr = prev === null ? c[i][2] - c[i][3]
        : Math.max(c[i][2] - c[i][3], Math.abs(c[i][2] - prev), Math.abs(c[i][3] - prev));
        a = i === 0 ? tr : (a * 13 + tr) / 14; prev = c[i][4]; if (i >= 14) o[i] = a; } return o; }

const rows = [];
for (const p of panel) {
    if (p.y6 == null) continue;
    const c = candles[`${p.platform}:${p.symbol}`]; if (!c) continue;
    let i = -1; for (let j = 0; j < c.length; j++) if (c[j][0] + BAR_MS > p.ts) { i = j; break; }
    if (i < 40) continue;
    const A = atr14(c), a = A[i]; if (!(a > 0)) continue;
    const cl = c.map((x) => x[4]);
    rows.push({ ...p, rev6: (cl[i] - cl[i - 6]) / a, mom30: (cl[i] - cl[i - 30]) / a });
}
const mean = (a) => a.reduce((x, y) => x + y, 0) / a.length;
function clusterT(v, k) { const n = v.length, m = mean(v), g = new Map();
    for (let i = 0; i < n; i++) g.set(k[i], (g.get(k[i]) || 0) + (v[i] - m));
    const G = g.size; let meat = 0; for (const s of g.values()) meat += s*s;
    const se = Math.sqrt((meat/(n*n))*(G/Math.max(1,G-1))); return { n, G, m, se, t: m/se }; }

console.log(`decisions with a recent-move reading: ${rows.length}\n`);
// alignment: +1 if the AI traded WITH the last 24h move (momentum-chasing)
const align = rows.map((r) => ({ ...r, chase: r.dir * Math.sign(r.rev6) }));
const chasing = align.filter((r) => r.chase > 0), fading = align.filter((r) => r.chase < 0);
console.log(`AI traded WITH the last 24h move (momentum-chasing): ${chasing.length} / ${rows.length} (${(100*chasing.length/rows.length).toFixed(0)}%)`);
console.log(`AI traded AGAINST it (fading):                       ${fading.length} / ${rows.length} (${(100*fading.length/rows.length).toFixed(0)}%)`);

const cc = clusterT(chasing.map(r=>r.y6), chasing.map(r=>r.day));
const cf = clusterT(fading.map(r=>r.y6),  fading.map(r=>r.day));
console.log(`\n  chasing decisions: mean y6 = ${cc.m.toFixed(3)} ATR  (n=${cc.n}, t=${cc.t.toFixed(2)})`);
console.log(`  fading  decisions: mean y6 = ${cf.m.toFixed(3)} ATR  (n=${cf.n}, t=${cf.t.toFixed(2)})`);
const d = cf.m - cc.m, se = Math.sqrt(cc.se**2 + cf.se**2);
console.log(`  fading - chasing = ${d.toFixed(3)} ATR   t = ${(d/se).toFixed(2)}`);

// correlation between the AI's direction and the recent move
const dirs = rows.map(r=>r.dir), revs = rows.map(r=>r.rev6);
const md = mean(dirs), mr = mean(revs);
let sab=0,sa=0,sb=0; for (let i=0;i<dirs.length;i++){const x=dirs[i]-md,y=revs[i]-mr;sab+=x*y;sa+=x*x;sb+=y*y;}
console.log(`\n  corr(AI direction, last-24h move) = ${(sab/Math.sqrt(sa*sb)).toFixed(3)}`);
console.log(`  (positive = the AI systematically bought strength / sold weakness)`);
