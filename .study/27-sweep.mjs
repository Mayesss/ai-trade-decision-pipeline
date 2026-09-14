// Signal sweep. Usage: node .study/27-sweep.mjs <panelDir> <label>
// Cross-sectional rank IC vs forward 24h return, one pre-specified family,
// Bonferroni-corrected, inference on NON-OVERLAPPING dates only.
import { readFileSync, writeFileSync } from 'node:fs';
import { SIGNALS, indicators, WARMUP } from './signals.mjs';

const DIR = process.argv[2] || '.study/panel-wide';
const LABEL = process.argv[3] || 'discovery';
const H = 6, MIN_XSEC = 20;

const manifest = JSON.parse(readFileSync(`${DIR}/_manifest.json`, 'utf8'));
const data = new Map();
for (const m of manifest) {
    const c = JSON.parse(readFileSync(`${DIR}/${m.symbol}.json`, 'utf8'));
    if (c.length < WARMUP + H + 10) continue;
    data.set(m.symbol, { c, ind: indicators(c) });
}
// BTC reference for relative signals
const btc = data.get('BTCUSDT');
const btcRet = new Map();
if (btc) for (let i = 0; i < btc.c.length; i++) btcRet.set(btc.c[i][0], i);

const EXTRA = [
    ['resid_ret6',  (d, i, ts) => { if (!btc) return null; const bi = btcRet.get(ts); if (bi == null || bi < 6) return null;
        return (d.cl[i] - d.cl[i-6]) / d.cl[i-6] - (btc.ind.cl[bi] - btc.ind.cl[bi-6]) / btc.ind.cl[bi-6]; }],
    ['resid_ret30', (d, i, ts) => { if (!btc) return null; const bi = btcRet.get(ts); if (bi == null || bi < 30) return null;
        return (d.cl[i] - d.cl[i-30]) / d.cl[i-30] - (btc.ind.cl[bi] - btc.ind.cl[bi-30]) / btc.ind.cl[bi-30]; }],
    ['beta60_btc',  (d, i, ts) => { if (!btc) return null; const bi = btcRet.get(ts); if (bi == null || bi < 60) return null;
        let sxy = 0, sxx = 0; for (let k = 0; k < 60; k++) { const x = btc.ind.ret[bi-k], y = d.ret[i-k]; sxy += x*y; sxx += x*x; }
        return sxx > 0 ? sxy / sxx : null; }],
    ['corr60_btc',  (d, i, ts) => { if (!btc) return null; const bi = btcRet.get(ts); if (bi == null || bi < 60) return null;
        let sxy=0,sxx=0,syy=0; for (let k = 0; k < 60; k++) { const x = btc.ind.ret[bi-k], y = d.ret[i-k]; sxy+=x*y; sxx+=x*x; syy+=y*y; }
        return sxx>0&&syy>0 ? sxy/Math.sqrt(sxx*syy) : null; }],
];
const ALL = [...SIGNALS.map(([n, f]) => [n, (d, i) => f(d, i)]), ...EXTRA];
const FAMILY = ALL.length;

// index every symbol by timestamp
const byTs = new Map();
for (const [sym, { c, ind }] of data) {
    for (let i = WARMUP; i < c.length - H; i++) {
        const ts = c[i][0];
        if (!(ind.atr[i] > 0)) continue;
        const fwd = (ind.cl[i + H] - ind.cl[i]) / ind.atr[i];
        if (!Number.isFinite(fwd)) continue;
        if (!byTs.has(ts)) byTs.set(ts, []);
        byTs.get(ts).push({ sym, i, ind, fwd, ts });
    }
}
const dates = [...byTs.keys()].sort((a, b) => a - b).filter((ts) => byTs.get(ts).length >= MIN_XSEC);
console.log(`[${LABEL}] symbols=${data.size}  dates=${dates.length.toLocaleString()}  family=${FAMILY}`);
if (!dates.length) { console.log('no usable dates'); process.exit(0); }

const rank = (xs) => { const idx = xs.map((v, i) => [v, i]).sort((a, b) => a[0] - b[0]); const r = new Array(xs.length);
    for (let i = 0; i < idx.length;) { let j = i; while (j + 1 < idx.length && idx[j+1][0] === idx[i][0]) j++;
        const avg = (i + j) / 2 + 1; for (let k = i; k <= j; k++) r[idx[k][1]] = avg; i = j + 1; } return r; };
const pearson = (a, b) => { const n = a.length, ma = a.reduce((x,y)=>x+y,0)/n, mb = b.reduce((x,y)=>x+y,0)/n;
    let sab=0,sa=0,sb=0; for (let i=0;i<n;i++){const x=a[i]-ma,y=b[i]-mb;sab+=x*y;sa+=x*x;sb+=y*y;}
    return sa>0&&sb>0 ? sab/Math.sqrt(sa*sb) : 0; };
const mean = (a) => a.reduce((x,y)=>x+y,0)/a.length;
const sd = (a) => { const m = mean(a); return Math.sqrt(a.reduce((s,x)=>s+(x-m)**2,0)/(a.length-1)); };

const results = [];
// Rank persistence: if a signal's cross-sectional ranking barely changes between
// rebalances, date-level ICs are the SAME BET re-tested and the t-stat below is
// not usable. Learned the hard way on vol_per_atr (persistence 0.997, t=-4.51,
// and an entirely absent effect once tested as a portfolio). 2026-09-14.
const persistenceOf = (fn) => {
    let prev = null; const ps = [];
    for (let di = 0; di < dates.length; di += H) {
        const rowsAt = byTs.get(dates[di]);
        const keep = [];
        for (const r of rowsAt) { let v; try { v = fn(r.ind, r.i, dates[di]); } catch { v = null; }
            if (v != null && Number.isFinite(v)) keep.push({ sym: r.sym, v }); }
        if (keep.length < MIN_XSEC) continue;
        const rk = rank(keep.map((x) => x.v));
        const m = new Map(keep.map((x, j) => [x.sym, rk[j]]));
        if (prev) { const common = [...m.keys()].filter((x) => prev.has(x));
            if (common.length >= MIN_XSEC) ps.push(pearson(common.map((x) => m.get(x)), common.map((x) => prev.get(x)))); }
        prev = m;
    }
    return ps.length >= 20 ? ps.reduce((a, b) => a + b, 0) / ps.length : null;
};
for (const [name, fn] of ALL) {
    const ics = [];
    for (let di = 0; di < dates.length; di++) {
        const ts = dates[di], rowsAt = byTs.get(ts);
        const sv = [], fv = [];
        for (const r of rowsAt) {
            let v; try { v = fn(r.ind, r.i, ts); } catch { v = null; }
            if (v == null || !Number.isFinite(v)) continue;
            sv.push(v); fv.push(r.fwd);
        }
        if (sv.length < MIN_XSEC) continue;
        ics.push({ di, ic: pearson(rank(sv), rank(fv)), n: sv.length });
    }
    if (ics.length < 100) { results.push({ name, skipped: true, dates: ics.length }); continue; }
    const nov = ics.filter((x) => x.di % H === 0).map((x) => x.ic);
    const v = ics.map((x) => x.ic);
    const m = mean(nov), s = sd(nov), t = m / (s / Math.sqrt(nov.length));
    const pers = persistenceOf(fn);
    results.push({ name, meanIC: m, icir: m / s, t, nDates: ics.length, nNonOverlap: nov.length,
        breadth: Math.round(mean(ics.map((x) => x.n))), meanIC_all: mean(v),
        persistence: pers, inferenceValid: pers == null ? null : pers <= 0.9 });
}

// Bonferroni threshold for this family, two-sided 5%
const zFor = (p) => { // Acklam
    const a=[-3.969683028665376e+01,2.209460984245205e+02,-2.759285104469687e+02,1.383577518672690e+02,-3.066479806614716e+01,2.506628277459239e+00];
    const b=[-5.447609879822406e+01,1.615858368580409e+02,-1.556989798598866e+02,6.680131188771972e+01,-1.328068155288572e+01];
    const c=[-7.784894002430293e-03,-3.223964580411365e-01,-2.400758277161838e+00,-2.549732539343734e+00,4.374664141464968e+00,2.938163982698783e+00];
    const d=[7.784695709041462e-03,3.224671290700398e-01,2.445134137142996e+00,3.754408661907416e+00]; const pl=0.02425;
    if (p<pl){const q=Math.sqrt(-2*Math.log(p));return (((((c[0]*q+c[1])*q+c[2])*q+c[3])*q+c[4])*q+c[5])/((((d[0]*q+d[1])*q+d[2])*q+d[3])*q+1);}
    if (p<=1-pl){const q=p-0.5,r=q*q;return (((((a[0]*r+a[1])*r+a[2])*r+a[3])*r+a[4])*r+a[5])*q/(((((b[0]*r+b[1])*r+b[2])*r+b[3])*r+b[4])*r+1);}
    const q=Math.sqrt(-2*Math.log(1-p));return -(((((c[0]*q+c[1])*q+c[2])*q+c[3])*q+c[4])*q+c[5])/((((d[0]*q+d[1])*q+d[2])*q+d[3])*q+1); };
const TCRIT = zFor(1 - 0.05 / (2 * FAMILY));

const ok = results.filter((r) => !r.skipped).sort((a, b) => Math.abs(b.t) - Math.abs(a.t));
console.log(`Bonferroni threshold for family of ${FAMILY}: |t| > ${TCRIT.toFixed(2)}\n`);
console.log('  signal            breadth   meanIC     ICIR      t   persist   survives');
for (const r of ok) {
    const invalid = r.persistence != null && r.persistence > 0.9;
    const s = invalid ? '  [t INVALID: static]' : (Math.abs(r.t) > TCRIT ? '  YES' : '');
    const pp = r.persistence == null ? '  --' : r.persistence.toFixed(3);
    console.log(`  ${r.name.padEnd(17)} ${String(r.breadth).padStart(6)}  ${r.meanIC.toFixed(4).padStart(8)}  ${r.icir.toFixed(3).padStart(6)}  ${r.t.toFixed(2).padStart(6)}  ${pp.padStart(7)}${s}`);
}
const survivors = ok.filter((r) => Math.abs(r.t) > TCRIT && !(r.persistence != null && r.persistence > 0.9));
console.log(`\n[${LABEL}] ${survivors.length} of ${FAMILY} survive correction: ${survivors.map((s) => s.name).join(', ') || '(none)'}`);
writeFileSync(`.study/sweep-${LABEL}.json`, JSON.stringify({ label: LABEL, family: FAMILY, tcrit: TCRIT, results: ok }, null, 1));
