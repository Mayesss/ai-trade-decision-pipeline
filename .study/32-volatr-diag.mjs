// What is vol_per_atr actually ranking? Its construction mixes a token COUNT
// with a PRICE-unit ATR, so it may be a proxy for price scale.
import { readFileSync } from 'node:fs';
import { indicators, WARMUP } from './signals.mjs';
const H = 6, MIN_XSEC = 20;
function load(DIR) {
    const manifest = JSON.parse(readFileSync(`${DIR}/_manifest.json`, 'utf8'));
    const data = new Map();
    for (const m of manifest) {
        const c = JSON.parse(readFileSync(`${DIR}/${m.symbol}.json`, 'utf8'));
        if (c.length < WARMUP + H + 10) continue;
        data.set(m.symbol, { c, ind: indicators(c) });
    }
    return data;
}
const pear = (a,b)=>{const n=a.length,ma=a.reduce((x,y)=>x+y,0)/n,mb=b.reduce((x,y)=>x+y,0)/n;
    let sab=0,sa=0,sb=0;for(let i=0;i<n;i++){const x=a[i]-ma,y=b[i]-mb;sab+=x*y;sa+=x*x;sb+=y*y;}
    return sa>0&&sb>0?sab/Math.sqrt(sa*sb):0;};
const rank=(xs)=>{const idx=xs.map((v,i)=>[v,i]).sort((a,b)=>a[0]-b[0]);const r=new Array(xs.length);
    for(let i=0;i<idx.length;){let j=i;while(j+1<idx.length&&idx[j+1][0]===idx[i][0])j++;const a=(i+j)/2+1;
    for(let k=i;k<=j;k++)r[idx[k][1]]=a;i=j+1;}return r;};

for (const [DIR, label] of [['.study/panel-wide','DISCOVERY'], ['.study/panel-holdout','HOLDOUT']]) {
    const data = load(DIR);
    const byTs = new Map();
    for (const [sym,{c,ind}] of data) for (let i = WARMUP; i < c.length - H; i++) {
        if (!(ind.atr[i] > 0) || !(ind.vol[i] > 0)) continue;
        const ts = c[i][0]; if (!byTs.has(ts)) byTs.set(ts,[]);
        byTs.get(ts).push({ sym, i, ind });
    }
    const dates = [...byTs.keys()].sort((a,b)=>a-b).filter(ts=>byTs.get(ts).length>=MIN_XSEC);
    const corrs = { price: [], logvol: [], dollarvol: [], atrpct: [] };
    const persist = [];
    let prevRank = null;
    for (let k = 0; k < dates.length; k += H) {
        const rows = byTs.get(dates[k]);
        const v = rows.map(r => Math.log(1 + r.ind.vol[r.i]) / r.ind.atr[r.i]);
        const price = rows.map(r => Math.log(r.ind.cl[r.i]));
        const lv = rows.map(r => Math.log(1 + r.ind.vol[r.i]));
        const dv = rows.map(r => Math.log(1 + r.ind.vol[r.i] * r.ind.cl[r.i]));
        const ap = rows.map(r => (r.ind.atr[r.i] / r.ind.cl[r.i]) * 100);
        const rv = rank(v);
        corrs.price.push(pear(rv, rank(price)));
        corrs.logvol.push(pear(rv, rank(lv)));
        corrs.dollarvol.push(pear(rv, rank(dv)));
        corrs.atrpct.push(pear(rv, rank(ap)));
        // persistence: how stable is the ranking from one rebalance to the next?
        const m = new Map(rows.map((r, j) => [r.sym, rv[j]]));
        if (prevRank) {
            const common = [...m.keys()].filter(s => prevRank.has(s));
            if (common.length >= MIN_XSEC) persist.push(pear(common.map(s=>m.get(s)), common.map(s=>prevRank.get(s))));
        }
        prevRank = m;
    }
    const mean=(a)=>a.reduce((x,y)=>x+y,0)/a.length;
    console.log(`\n[${label}] rank-correlation of vol_per_atr with:`);
    console.log(`  log(price)        ${mean(corrs.price).toFixed(3)}   <- if strongly negative, it is a price-scale proxy`);
    console.log(`  log(volume)       ${mean(corrs.logvol).toFixed(3)}`);
    console.log(`  log(dollar vol)   ${mean(corrs.dollarvol).toFixed(3)}`);
    console.log(`  ATR%              ${mean(corrs.atrpct).toFixed(3)}`);
    console.log(`  rank persistence across rebalances: ${mean(persist).toFixed(3)}  (1.0 = static characteristic)`);
}
