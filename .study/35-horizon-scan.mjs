// Does a LONGER horizon rescue the effect? Longer holds mean fewer rebalances,
// so costs fall -- but the signal has to survive out there too.
// Runs the same composite at several horizons, gross and net.
import { readFileSync } from 'node:fs';
import { SIGNALS, indicators, WARMUP } from './signals.mjs';
const MIN_XSEC = 20, Q = 5;
const COMPOSITE = ['px_vs_ema20','rsi14','ret_12','ret_1','ret_2','resid_ret6']; // vol_per_atr dropped (static proxy)
const HORIZONS = [[6,'24h'],[12,'48h'],[42,'7d'],[84,'14d'],[180,'30d']];

function load(DIR) {
    const manifest = JSON.parse(readFileSync(`${DIR}/_manifest.json`, 'utf8'));
    const data = new Map();
    for (const m of manifest) {
        const c = JSON.parse(readFileSync(`${DIR}/${m.symbol}.json`, 'utf8'));
        if (c.length < WARMUP + 200) continue;
        data.set(m.symbol, { c, ind: indicators(c) });
    }
    return data;
}
const rank=(xs)=>{const idx=xs.map((v,i)=>[v,i]).sort((a,b)=>a[0]-b[0]);const r=new Array(xs.length);
    for(let i=0;i<idx.length;){let j=i;while(j+1<idx.length&&idx[j+1][0]===idx[i][0])j++;const a=(i+j)/2+1;
    for(let k=i;k<=j;k++)r[idx[k][1]]=a;i=j+1;}return r;};
const pear=(a,b)=>{const n=a.length,ma=a.reduce((x,y)=>x+y,0)/n,mb=b.reduce((x,y)=>x+y,0)/n;
    let sab=0,sa=0,sb=0;for(let i=0;i<n;i++){const x=a[i]-ma,y=b[i]-mb;sab+=x*y;sa+=x*x;sb+=y*y;}
    return sa>0&&sb>0?sab/Math.sqrt(sa*sb):0;};
const mean=(a)=>a.reduce((x,y)=>x+y,0)/a.length;
const sd=(a)=>{const m=mean(a);return Math.sqrt(a.reduce((s,x)=>s+(x-m)**2,0)/(a.length-1));};

for (const [DIR, label] of [['.study/panel-wide','DISCOVERY'],['.study/panel-holdout','HOLDOUT']]) {
    const data = load(DIR);
    const btc = data.get('BTCUSDT'); const bIdx = new Map();
    if (btc) for (let i = 0; i < btc.c.length; i++) bIdx.set(btc.c[i][0], i);
    const FN = new Map(SIGNALS);
    FN.set('resid_ret6',(d,i,ts)=>{const bi=bIdx.get(ts);if(bi==null||bi<6)return null;
        return (d.cl[i]-d.cl[i-6])/d.cl[i-6]-(btc.ind.cl[bi]-btc.ind.cl[bi-6])/btc.ind.cl[bi-6];});
    console.log(`\n[${label}]`);
    console.log('   horizon  rebal   IC      t     gross/period   gross ann   turnover   net ann @6bp');
    for (const [H, name] of HORIZONS) {
        const byTs = new Map();
        for (const [sym,{c,ind}] of data) for (let i = WARMUP; i < c.length - H; i++) {
            if (!(ind.atr[i] > 0)) continue;
            const fwdPct = (ind.cl[i+H]-ind.cl[i])/ind.cl[i];
            const fwdAtr = (ind.cl[i+H]-ind.cl[i])/ind.atr[i];
            if (!Number.isFinite(fwdPct)) continue;
            const ts = c[i][0]; if (!byTs.has(ts)) byTs.set(ts,[]);
            byTs.get(ts).push({ sym, i, ind, fwdPct, fwdAtr, ts });
        }
        const all = [...byTs.keys()].sort((a,b)=>a-b).filter(ts=>byTs.get(ts).length>=MIN_XSEC);
        const rebal = all.filter((_,k)=>k%H===0);
        if (rebal.length < 30) { console.log(`   ${name.padStart(5)}  too few rebalances`); continue; }
        let prevW = new Map(); const rets=[], turns=[], ics=[];
        for (const ts of rebal) {
            const rows = byTs.get(ts);
            const cols = [];
            for (const nm of COMPOSITE) {
                const fn = FN.get(nm);
                const vals = rows.map(r=>{let v;try{v=fn(r.ind,r.i,ts);}catch{v=null;}return (v==null||!Number.isFinite(v))?null:v;});
                const ok = vals.filter(v=>v!=null); if (ok.length < MIN_XSEC) continue;
                const m = mean(ok), s = sd(ok)||1;
                cols.push(vals.map(v=>v==null?null:(v-m)/s));
            }
            if (!cols.length) continue;
            const scored = rows.map((r,j)=>{const xs=cols.map(c=>c[j]).filter(v=>v!=null);
                return xs.length?{r,s:mean(xs)}:null;}).filter(Boolean);
            if (scored.length < MIN_XSEC) continue;
            ics.push(pear(rank(scored.map(x=>x.s)), rank(scored.map(x=>x.r.fwdAtr))));
            scored.sort((a,b)=>a.s-b.s);
            const nQ = Math.max(2, Math.floor(scored.length/Q));
            const longs = scored.slice(0,nQ), shorts = scored.slice(-nQ);
            const w = new Map();
            for (const x of longs) w.set(x.r.sym, 0.5/nQ);
            for (const x of shorts) w.set(x.r.sym, -0.5/nQ);
            rets.push(mean(longs.map(x=>x.r.fwdPct)) - mean(shorts.map(x=>x.r.fwdPct)));
            let t=0; const keys=new Set([...w.keys(),...prevW.keys()]);
            for (const k of keys) t += Math.abs((w.get(k)||0)-(prevW.get(k)||0));
            turns.push(t); prevW = w;
        }
        const g = mean(rets), tn = mean(turns), icm = mean(ics);
        const icT = icm / (sd(ics)/Math.sqrt(ics.length));
        const perYear = 365 / (H * 4 / 24);
        const net = g - tn * 0.0006;
        console.log(`   ${name.padStart(5)}  ${String(rets.length).padStart(5)}  ${icm.toFixed(4).padStart(7)} ${icT.toFixed(2).padStart(6)}   ${(g*100).toFixed(4).padStart(9)}%   ${(g*perYear*100).toFixed(1).padStart(7)}%   ${(tn*100).toFixed(0).padStart(6)}%   ${(net*perYear*100).toFixed(1).padStart(9)}%`);
    }
}
console.log('\n(6bp = Bitget taker per side; perYear assumes non-overlapping holds)');
