// THE GATE: does a tradeable long/short book survive fees?
// Composite of the 7 REPLICATED signals, z-scored per date, equal-weight.
// Dollar-neutral quintile book, rebalanced every 24h (H=6 bars).
// Returns in PERCENT (not ATR) because this is P&L, not a ranking test.
import { readFileSync } from 'node:fs';
import { SIGNALS, indicators, WARMUP } from './signals.mjs';

const H = 6, MIN_XSEC = 20, QUANTILE = 5;
const REPLICATED = ['px_vs_ema20','rsi14','ret_12','ret_1','ret_2','vol_per_atr','resid_ret6'];

function run(DIR, label, WEIGHTING) {
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
    FN.set('resid_ret6', (d, i, ts) => { if (!btc) return null; const bi = bIdx.get(ts); if (bi == null || bi < 6) return null;
        return (d.cl[i]-d.cl[i-6])/d.cl[i-6] - (btc.ind.cl[bi]-btc.ind.cl[bi-6])/btc.ind.cl[bi-6]; });

    const byTs = new Map();
    for (const [sym, { c, ind }] of data) for (let i = WARMUP; i < c.length - H; i++) {
        if (!(ind.atr[i] > 0)) continue;
        const fwdPct = (ind.cl[i + H] - ind.cl[i]) / ind.cl[i];
        if (!Number.isFinite(fwdPct)) continue;
        const ts = c[i][0]; if (!byTs.has(ts)) byTs.set(ts, []);
        byTs.get(ts).push({ sym, i, ind, fwdPct, ts });
    }
    // rebalance dates: every H bars, non-overlapping holding periods
    const all = [...byTs.keys()].sort((a,b)=>a-b).filter((ts)=>byTs.get(ts).length >= MIN_XSEC);
    const rebal = all.filter((_, k) => k % H === 0);

    let prevW = new Map();
    const rets = [], turns = [];
    for (const ts of rebal) {
        const rows = byTs.get(ts);
        // z-score each signal across the cross-section, then average
        const cols = [];
        for (const name of REPLICATED) {
            const fn = FN.get(name);
            const vals = rows.map((r) => { let v; try { v = fn(r.ind, r.i, ts); } catch { v = null; }
                return (v == null || !Number.isFinite(v)) ? null : v; });
            const ok = vals.filter((v) => v != null);
            if (ok.length < MIN_XSEC) continue;
            const m = ok.reduce((a,b)=>a+b,0)/ok.length;
            const s = Math.sqrt(ok.reduce((a,b)=>a+(b-m)**2,0)/(ok.length-1)) || 1;
            cols.push(vals.map((v) => v == null ? null : (v - m) / s));
        }
        if (!cols.length) continue;
        const score = rows.map((_, j) => {
            const xs = cols.map((c) => c[j]).filter((v) => v != null);
            return xs.length ? xs.reduce((a,b)=>a+b,0) / xs.length : null;
        });
        const valid = rows.map((r, j) => ({ r, s: score[j] })).filter((x) => x.s != null);
        if (valid.length < MIN_XSEC) continue;
        valid.sort((a, b) => a.s - b.s);
        const nQ = Math.max(2, Math.floor(valid.length / QUANTILE));
        // IC is NEGATIVE => low composite score predicts HIGH forward return => go long the low end
        const longs = valid.slice(0, nQ), shorts = valid.slice(-nQ);
        // RISK-PARITY: the IC was measured on ATR-normalised returns, so position
        // size must be inversely proportional to ATR or the portfolio is a
        // different bet from the one that was tested.
        const invVol = (x) => { const a = x.r.ind.atr[x.r.i] / x.r.ind.cl[x.r.i]; return a > 0 ? 1 / a : 0; };
        const w = new Map();
        let gross;
        if (WEIGHTING === 'riskparity') {
            const lw = longs.map(invVol), sw = shorts.map(invVol);
            const lsum = lw.reduce((a,b)=>a+b,0) || 1, ssum = sw.reduce((a,b)=>a+b,0) || 1;
            longs.forEach((x, j) => w.set(x.r.sym, 0.5 * lw[j] / lsum));
            shorts.forEach((x, j) => w.set(x.r.sym, -0.5 * sw[j] / ssum));
            gross = longs.reduce((a,x,j)=>a + (lw[j]/lsum) * x.r.fwdPct, 0)
                  - shorts.reduce((a,x,j)=>a + (sw[j]/ssum) * x.r.fwdPct, 0);
        } else {
            for (const x of longs) w.set(x.r.sym, 0.5 / nQ);
            for (const x of shorts) w.set(x.r.sym, -0.5 / nQ);
            gross = longs.reduce((a,x)=>a+x.r.fwdPct,0)/nQ - shorts.reduce((a,x)=>a+x.r.fwdPct,0)/nQ;
        }
        // turnover = sum |w_new - w_old| (both legs), in units of gross book
        let turn = 0;
        const keys = new Set([...w.keys(), ...prevW.keys()]);
        for (const k of keys) turn += Math.abs((w.get(k) || 0) - (prevW.get(k) || 0));
        rets.push(gross); turns.push(turn); prevW = w;
    }
    const mean = (a) => a.reduce((x,y)=>x+y,0)/a.length;
    const sd = (a) => { const m = mean(a); return Math.sqrt(a.reduce((s,x)=>s+(x-m)**2,0)/(a.length-1)); };
    const g = mean(rets), gsd = sd(rets), tn = mean(turns);
    const periodsPerYear = 365; // 24h holding
    console.log(`\n[${label}]  rebalances=${rets.length}  avg breadth=${Math.round(mean([...byTs.values()].map(v=>v.length)))}`);
    console.log(`  gross return / 24h : ${(g*100).toFixed(4)}%   sd ${(gsd*100).toFixed(3)}%   Sharpe(ann) ${(g/gsd*Math.sqrt(periodsPerYear)).toFixed(2)}`);
    console.log(`  avg turnover / rebal: ${(tn*100).toFixed(1)}% of gross book`);
    console.log(`  fee   net/24h      ann.return   ann.Sharpe`);
    for (const feeBps of [0, 2, 4, 6, 10]) {
        const cost = tn * (feeBps / 10000);
        const net = g - cost;
        const ann = net * periodsPerYear;
        console.log(`  ${String(feeBps).padStart(2)}bp  ${(net*100).toFixed(4).padStart(8)}%   ${(ann*100).toFixed(1).padStart(8)}%   ${(net/gsd*Math.sqrt(periodsPerYear)).toFixed(2).padStart(9)}`);
    }
    return { g, gsd, tn };
}
console.log('Composite of 7 replicated signals. Quintile long/short book, 24h holding.\n');
for (const wmode of ['equaldollar', 'riskparity']) {
  console.log(`================ WEIGHTING: ${wmode} ================`);
  run('.study/panel-wide', 'DISCOVERY 2024-03..2026-09', wmode);
  run('.study/panel-holdout', 'HOLDOUT 2021-06..2024-03', wmode);
  console.log('');
}
console.log('\nBitget perp fees: taker ~6bp, maker ~2bp per side. Funding not modelled');
console.log('(a dollar-neutral book pays and receives funding roughly symmetrically).');
