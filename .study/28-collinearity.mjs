// Are the 9 survivors 9 findings or 1 finding measured 9 ways?
// Correlate their per-date IC series: highly correlated ICs = the same bet.
import { readFileSync } from 'node:fs';
import { SIGNALS, indicators, WARMUP } from './signals.mjs';
const DIR = '.study/panel-wide', H = 6, MIN_XSEC = 20;
const SURV = ['px_vs_ema20','resid_ret6','rsi14','ret_12','vol_per_atr','ret_6','ret_1','ret_2','hl_range'];

const manifest = JSON.parse(readFileSync(`${DIR}/_manifest.json`, 'utf8'));
const data = new Map();
for (const m of manifest) {
    const c = JSON.parse(readFileSync(`${DIR}/${m.symbol}.json`, 'utf8'));
    if (c.length < WARMUP + H + 10) continue;
    data.set(m.symbol, { c, ind: indicators(c) });
}
const btc = data.get('BTCUSDT'); const btcIdx = new Map();
if (btc) for (let i = 0; i < btc.c.length; i++) btcIdx.set(btc.c[i][0], i);
const FN = new Map(SIGNALS);
FN.set('resid_ret6', (d, i, ts) => { const bi = btcIdx.get(ts); if (bi == null || bi < 6) return null;
    return (d.cl[i]-d.cl[i-6])/d.cl[i-6] - (btc.ind.cl[bi]-btc.ind.cl[bi-6])/btc.ind.cl[bi-6]; });

const byTs = new Map();
for (const { c, ind } of data.values()) for (let i = WARMUP; i < c.length - H; i++) {
    if (!(ind.atr[i] > 0)) continue;
    const fwd = (ind.cl[i+H] - ind.cl[i]) / ind.atr[i]; if (!Number.isFinite(fwd)) continue;
    const ts = c[i][0]; if (!byTs.has(ts)) byTs.set(ts, []); byTs.get(ts).push({ i, ind, fwd, ts });
}
const dates = [...byTs.keys()].sort((a,b)=>a-b).filter((ts)=>byTs.get(ts).length >= MIN_XSEC);
const rank = (xs) => { const idx = xs.map((v,i)=>[v,i]).sort((a,b)=>a[0]-b[0]); const r = new Array(xs.length);
    for (let i=0;i<idx.length;){let j=i;while(j+1<idx.length&&idx[j+1][0]===idx[i][0])j++;const a=(i+j)/2+1;
    for(let k=i;k<=j;k++)r[idx[k][1]]=a;i=j+1;} return r; };
const pear = (a,b)=>{const n=a.length,ma=a.reduce((x,y)=>x+y,0)/n,mb=b.reduce((x,y)=>x+y,0)/n;
    let sab=0,sa=0,sb=0;for(let i=0;i<n;i++){const x=a[i]-ma,y=b[i]-mb;sab+=x*y;sa+=x*x;sb+=y*y;}
    return sa>0&&sb>0?sab/Math.sqrt(sa*sb):0;};

const icSeries = {};
for (const name of SURV) {
    const fn = FN.get(name); const out = [];
    for (const ts of dates) {
        const sv = [], fv = [];
        for (const r of byTs.get(ts)) { let v; try { v = fn(r.ind, r.i, ts); } catch { v = null; }
            if (v == null || !Number.isFinite(v)) continue; sv.push(v); fv.push(r.fwd); }
        out.push(sv.length >= MIN_XSEC ? pear(rank(sv), rank(fv)) : null);
    }
    icSeries[name] = out;
}
console.log('Correlation between the survivors\' per-date IC series:\n');
process.stdout.write('                 ' + SURV.map((s)=>s.slice(0,7).padStart(8)).join('') + '\n');
const pairs = [];
for (const a of SURV) {
    process.stdout.write(a.padEnd(17));
    for (const b of SURV) {
        const xs = [], ys = [];
        for (let i = 0; i < dates.length; i++) { const x = icSeries[a][i], y = icSeries[b][i];
            if (x != null && y != null) { xs.push(x); ys.push(y); } }
        const c = pear(xs, ys);
        if (a < b) pairs.push({ a, b, c });
        process.stdout.write((a === b ? '    1.00' : c.toFixed(2).padStart(8)));
    }
    process.stdout.write('\n');
}
const hi = pairs.filter((p) => Math.abs(p.c) > 0.7).length;
console.log(`\npairs correlated above |0.7|: ${hi} of ${pairs.length}`);
console.log(`mean absolute pairwise correlation: ${(pairs.reduce((s,p)=>s+Math.abs(p.c),0)/pairs.length).toFixed(2)}`);
