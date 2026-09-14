// Audit: which signals are persistent enough that their IC t-stat is inflated?
// A signal whose cross-sectional ranking barely changes between rebalances is
// being re-tested on the same bet, so date-level observations are not
// independent and the sweep's t-stat overstates the evidence.
import { readFileSync } from 'node:fs';
import { SIGNALS, indicators, WARMUP } from './signals.mjs';
const H = 6, MIN_XSEC = 20, DIR = '.study/panel-wide';
const manifest = JSON.parse(readFileSync(`${DIR}/_manifest.json`, 'utf8'));
const data = new Map();
for (const m of manifest) {
    const c = JSON.parse(readFileSync(`${DIR}/${m.symbol}.json`, 'utf8'));
    if (c.length < WARMUP + H + 10) continue;
    data.set(m.symbol, { c, ind: indicators(c) });
}
const btc = data.get('BTCUSDT'); const bIdx = new Map();
if (btc) for (let i = 0; i < btc.c.length; i++) bIdx.set(btc.c[i][0], i);
const FN = new Map(SIGNALS);
FN.set('resid_ret6',(d,i,ts)=>{const bi=bIdx.get(ts);if(bi==null||bi<6)return null;
    return (d.cl[i]-d.cl[i-6])/d.cl[i-6]-(btc.ind.cl[bi]-btc.ind.cl[bi-6])/btc.ind.cl[bi-6];});
FN.set('corr60_btc',(d,i,ts)=>{const bi=bIdx.get(ts);if(bi==null||bi<60)return null;
    let sxy=0,sxx=0,syy=0;for(let k=0;k<60;k++){const x=btc.ind.ret[bi-k],y=d.ret[i-k];sxy+=x*y;sxx+=x*x;syy+=y*y;}
    return sxx>0&&syy>0?sxy/Math.sqrt(sxx*syy):null;});

const byTs = new Map();
for (const [sym,{c,ind}] of data) for (let i = WARMUP; i < c.length - H; i++) {
    if (!(ind.atr[i] > 0)) continue;
    const ts = c[i][0]; if (!byTs.has(ts)) byTs.set(ts,[]); byTs.get(ts).push({ sym, i, ind, ts });
}
const dates = [...byTs.keys()].sort((a,b)=>a-b).filter(ts=>byTs.get(ts).length>=MIN_XSEC).filter((_,k)=>k%H===0);
const rank=(xs)=>{const idx=xs.map((v,i)=>[v,i]).sort((a,b)=>a[0]-b[0]);const r=new Array(xs.length);
    for(let i=0;i<idx.length;){let j=i;while(j+1<idx.length&&idx[j+1][0]===idx[i][0])j++;const a=(i+j)/2+1;
    for(let k=i;k<=j;k++)r[idx[k][1]]=a;i=j+1;}return r;};
const pear=(a,b)=>{const n=a.length,ma=a.reduce((x,y)=>x+y,0)/n,mb=b.reduce((x,y)=>x+y,0)/n;
    let sab=0,sa=0,sb=0;for(let i=0;i<n;i++){const x=a[i]-ma,y=b[i]-mb;sab+=x*y;sa+=x*x;sb+=y*y;}
    return sa>0&&sb>0?sab/Math.sqrt(sa*sb):0;};

const sweep = JSON.parse(readFileSync('.study/sweep-discovery.json','utf8'));
const tByName = new Map(sweep.results.map(r=>[r.name,r.t]));
const out = [];
for (const [name] of [...SIGNALS, ['resid_ret6'], ['corr60_btc']]) {
    const fn = FN.get(name); if (!fn) continue;
    let prev = null; const ps = [];
    for (const ts of dates) {
        const rows = byTs.get(ts);
        const vals = rows.map(r=>{let v;try{v=fn(r.ind,r.i,ts);}catch{v=null;}return (v==null||!Number.isFinite(v))?null:v;});
        const keep = rows.map((r,j)=>({sym:r.sym,v:vals[j]})).filter(x=>x.v!=null);
        if (keep.length < MIN_XSEC) continue;
        const rk = rank(keep.map(x=>x.v));
        const m = new Map(keep.map((x,j)=>[x.sym, rk[j]]));
        if (prev) { const common=[...m.keys()].filter(s=>prev.has(s));
            if (common.length>=MIN_XSEC) ps.push(pear(common.map(s=>m.get(s)),common.map(s=>prev.get(s)))); }
        prev = m;
    }
    if (ps.length < 20) continue;
    const p = ps.reduce((a,b)=>a+b,0)/ps.length;
    out.push({ name, persistence: p, t: tByName.get(name) ?? NaN });
}
out.sort((a,b)=>b.persistence-a.persistence);
console.log('  signal            persistence    sweep_t   inference');
for (const o of out) {
    const bad = o.persistence > 0.9;
    const warn = o.persistence > 0.7 && !bad;
    console.log(`  ${o.name.padEnd(17)} ${o.persistence.toFixed(3).padStart(10)} ${(Number.isFinite(o.t)?o.t.toFixed(2):'--').padStart(10)}   ${bad?'INVALID - same bet re-tested':warn?'inflated':''}`);
}
console.log('\npersistence > 0.9 => the ranking barely changes, so date-level ICs are not');
console.log('independent and the sweep t-stat is not usable for that signal.');
