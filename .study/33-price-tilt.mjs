// vol_per_atr is a price-level tilt (corr -0.99 with log price, persistence 0.998).
// For a near-static characteristic the IC t-stat is meaningless (observations are
// the same bet repeated). The honest test: is the tilt's return CONSISTENT across
// sub-periods, and does it survive costs given its low turnover?
import { readFileSync } from 'node:fs';
import { indicators, WARMUP } from './signals.mjs';
const H = 6, MIN_XSEC = 20, Q = 5;

function run(DIR, label) {
    const manifest = JSON.parse(readFileSync(`${DIR}/_manifest.json`, 'utf8'));
    const data = new Map();
    for (const m of manifest) {
        const c = JSON.parse(readFileSync(`${DIR}/${m.symbol}.json`, 'utf8'));
        if (c.length < WARMUP + H + 10) continue;
        data.set(m.symbol, { c, ind: indicators(c) });
    }
    const byTs = new Map();
    for (const [sym,{c,ind}] of data) for (let i = WARMUP; i < c.length - H; i++) {
        if (!(ind.atr[i] > 0) || !(ind.vol[i] > 0)) continue;
        const fwdPct = (ind.cl[i+H]-ind.cl[i])/ind.cl[i];
        if (!Number.isFinite(fwdPct)) continue;
        const ts = c[i][0]; if (!byTs.has(ts)) byTs.set(ts,[]);
        byTs.get(ts).push({ sym, i, ind, fwdPct, ts });
    }
    const all = [...byTs.keys()].sort((a,b)=>a-b).filter(ts=>byTs.get(ts).length>=MIN_XSEC);
    const rebal = all.filter((_,k)=>k%H===0);

    let prevW = new Map();
    const recs = [];
    for (const ts of rebal) {
        const rows = byTs.get(ts);
        // the signal, as actually constructed
        const scored = rows.map(r => ({ r, s: Math.log(1 + r.ind.vol[r.i]) / r.ind.atr[r.i] }))
                           .filter(x => Number.isFinite(x.s));
        if (scored.length < MIN_XSEC) continue;
        scored.sort((a,b)=>a.s-b.s);
        const n = scored.length, nQ = Math.max(2, Math.floor(n / Q));
        // IC negative => LOW signal (= HIGH price) predicts HIGH forward return
        const longs = scored.slice(0, nQ), shorts = scored.slice(-nQ);
        const w = new Map();
        for (const x of longs) w.set(x.r.sym, 0.5/nQ);
        for (const x of shorts) w.set(x.r.sym, -0.5/nQ);
        const gross = longs.reduce((a,x)=>a+x.r.fwdPct,0)/nQ - shorts.reduce((a,x)=>a+x.r.fwdPct,0)/nQ;
        let turn = 0; const keys = new Set([...w.keys(), ...prevW.keys()]);
        for (const k of keys) turn += Math.abs((w.get(k)||0) - (prevW.get(k)||0));
        prevW = w;
        recs.push({ ts, gross, turn,
            longPx: Math.exp(longs.reduce((a,x)=>a+Math.log(x.r.ind.cl[x.r.i]),0)/nQ),
            shortPx: Math.exp(shorts.reduce((a,x)=>a+Math.log(x.r.ind.cl[x.r.i]),0)/nQ) });
    }
    const mean=(a)=>a.reduce((x,y)=>x+y,0)/a.length;
    const sd=(a)=>{const m=mean(a);return Math.sqrt(a.reduce((s,x)=>s+(x-m)**2,0)/(a.length-1));};
    const g = mean(recs.map(r=>r.gross)), gsd = sd(recs.map(r=>r.gross)), tn = mean(recs.map(r=>r.turn));
    console.log(`\n[${label}] rebalances=${recs.length}`);
    console.log(`  long leg avg price $${mean(recs.map(r=>r.longPx)).toFixed(2)}  |  short leg avg price $${mean(recs.map(r=>r.shortPx)).toFixed(4)}`);
    console.log(`  gross/24h ${(g*100).toFixed(4)}%   sd ${(gsd*100).toFixed(3)}%   turnover ${(tn*100).toFixed(1)}%`);
    for (const bp of [0, 2, 6]) {
        const net = g - tn*(bp/10000);
        console.log(`   ${String(bp).padStart(2)}bp net/24h ${(net*100).toFixed(4).padStart(8)}%  ann ${(net*365*100).toFixed(1).padStart(7)}%  Sharpe ${(net/gsd*Math.sqrt(365)).toFixed(2)}`);
    }
    // consistency: split into 6 equal sub-periods
    console.log('  consistency across sub-periods (gross/24h):');
    const k = Math.floor(recs.length / 6);
    const parts = [];
    for (let p = 0; p < 6; p++) {
        const seg = recs.slice(p*k, p === 5 ? recs.length : (p+1)*k);
        if (!seg.length) continue;
        const m = mean(seg.map(r=>r.gross));
        parts.push(m);
        const d = new Date(seg[0].ts).toISOString().slice(0,7);
        console.log(`    ${d}  n=${String(seg.length).padStart(3)}  ${(m*100).toFixed(4).padStart(8)}%  ${m>0?'+':'-'}`);
    }
    const pos = parts.filter(x=>x>0).length;
    console.log(`  sub-periods positive: ${pos}/${parts.length}`);
    return { g, gsd, tn, parts };
}
console.log('PRICE-LEVEL TILT (long high-priced / short low-priced), 24h rebalance');
run('.study/panel-wide','DISCOVERY 2024-03..2026-09');
run('.study/panel-holdout','HOLDOUT 2021-06..2024-03');
