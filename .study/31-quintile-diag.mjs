// Direct diagnostic: mean forward return by composite quintile, in BOTH units.
// If rank IC is negative, quintile 1 (lowest score) should have the HIGHEST
// forward return. If it does not, the IC and the portfolio disagree and one of
// them is measuring something other than what I think.
import { readFileSync } from 'node:fs';
import { SIGNALS, indicators, WARMUP } from './signals.mjs';
const H = 6, MIN_XSEC = 20, Q = 5;
const REPLICATED = ['px_vs_ema20','rsi14','ret_12','ret_1','ret_2','vol_per_atr','resid_ret6'];

function run(DIR, label) {
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
    FN.set('resid_ret6', (d,i,ts) => { const bi = bIdx.get(ts); if (bi==null||bi<6) return null;
        return (d.cl[i]-d.cl[i-6])/d.cl[i-6] - (btc.ind.cl[bi]-btc.ind.cl[bi-6])/btc.ind.cl[bi-6]; });

    const byTs = new Map();
    for (const [sym,{c,ind}] of data) for (let i = WARMUP; i < c.length - H; i++) {
        if (!(ind.atr[i] > 0)) continue;
        const fwdPct = (ind.cl[i+H]-ind.cl[i])/ind.cl[i];
        const fwdAtr = (ind.cl[i+H]-ind.cl[i])/ind.atr[i];
        if (!Number.isFinite(fwdPct)) continue;
        const ts = c[i][0]; if (!byTs.has(ts)) byTs.set(ts,[]);
        byTs.get(ts).push({ sym, i, ind, fwdPct, fwdAtr, ts });
    }
    const all = [...byTs.keys()].sort((a,b)=>a-b).filter((ts)=>byTs.get(ts).length>=MIN_XSEC);
    const rebal = all.filter((_,k)=>k%H===0);
    const buckets = Array.from({length:Q},()=>({pct:[],atr:[]}));
    for (const ts of rebal) {
        const rows = byTs.get(ts);
        const cols = [];
        for (const name of REPLICATED) {
            const fn = FN.get(name);
            const vals = rows.map((r)=>{let v;try{v=fn(r.ind,r.i,ts);}catch{v=null;}return (v==null||!Number.isFinite(v))?null:v;});
            const ok = vals.filter((v)=>v!=null); if (ok.length < MIN_XSEC) continue;
            const m = ok.reduce((a,b)=>a+b,0)/ok.length;
            const s = Math.sqrt(ok.reduce((a,b)=>a+(b-m)**2,0)/(ok.length-1))||1;
            cols.push(vals.map((v)=>v==null?null:(v-m)/s));
        }
        if (!cols.length) continue;
        const scored = rows.map((r,j)=>{const xs=cols.map(c=>c[j]).filter(v=>v!=null);
            return xs.length?{r,s:xs.reduce((a,b)=>a+b,0)/xs.length}:null;}).filter(Boolean);
        if (scored.length < MIN_XSEC) continue;
        scored.sort((a,b)=>a.s-b.s);
        const n = scored.length;
        for (let q = 0; q < Q; q++) {
            const lo = Math.floor(q*n/Q), hi = Math.floor((q+1)*n/Q);
            for (let k = lo; k < hi; k++) { buckets[q].pct.push(scored[k].r.fwdPct); buckets[q].atr.push(scored[k].r.fwdAtr); }
        }
    }
    const mean=(a)=>a.reduce((x,y)=>x+y,0)/a.length;
    const med=(a)=>{const s=[...a].sort((x,y)=>x-y);return s[Math.floor(s.length/2)];};
    console.log(`\n[${label}]  rebalances=${rebal.length}`);
    console.log('  quintile (1=lowest composite)   n     mean fwd %   median fwd %   mean fwd ATR');
    for (let q = 0; q < Q; q++) {
        const b = buckets[q];
        console.log(`    Q${q+1}${q===0?' (we go LONG) ':q===Q-1?' (we go SHORT)':'              '}  ${String(b.pct.length).padStart(6)}   ${(mean(b.pct)*100).toFixed(3).padStart(9)}%   ${(med(b.pct)*100).toFixed(3).padStart(11)}%   ${mean(b.atr).toFixed(4).padStart(11)}`);
    }
    const sp = mean(buckets[0].pct) - mean(buckets[Q-1].pct);
    const sa = mean(buckets[0].atr) - mean(buckets[Q-1].atr);
    console.log(`  Q1 - Q5 spread:  ${(sp*100).toFixed(4)}% (pct)   ${sa.toFixed(4)} (ATR units)`);
}
run('.study/panel-wide','DISCOVERY');
run('.study/panel-holdout','HOLDOUT');
