// RETROSPECTIVE VETO TEST
// Question: if a base-rate veto had been running, would it have helped?
//
// For each of the 453 real directional decisions: find the most similar market
// states in history, see what happened next in those states, and compare the
// AI's chosen direction against that base rate. Then check whether the
// decisions the base rate DISAGREED with actually did worse.
//
// No-lookahead rules, both enforced:
//   - a neighbour bar must CLOSE before the decision time
//   - the neighbour's own OUTCOME (6 bars later) must also be known before it
//   - feature scaling is fitted only on bars before the decision window opens
import { readFileSync } from 'node:fs';

const BAR_MS = 4 * 3600_000;
const K = 200;            // pre-specified
const H = 6;              // pre-specified primary horizon (24h)
const SCALE_CUTOFF = Date.parse('2026-06-01T00:00:00Z'); // decisions start 06-27

const candles = JSON.parse(readFileSync('.study/candles-deep.json', 'utf8'));
const panel = JSON.parse(readFileSync('.study/panel.json', 'utf8'));

// ---- indicators -----------------------------------------------------------
const ema = (v, n) => { const k = 2 / (n + 1); const o = []; let p = v[0];
    for (let i = 0; i < v.length; i++) { p = i ? v[i] * k + p * (1 - k) : v[0]; o.push(p); } return o; };
function atr14(c) {
    const o = new Array(c.length).fill(null); let prev = null, a = null;
    for (let i = 0; i < c.length; i++) {
        const tr = prev === null ? c[i][2] - c[i][3]
            : Math.max(c[i][2] - c[i][3], Math.abs(c[i][2] - prev), Math.abs(c[i][3] - prev));
        a = i === 0 ? tr : (a * 13 + tr) / 14; prev = c[i][4]; if (i >= 14) o[i] = a;
    } return o;
}
function rsi14(c) {
    const o = new Array(c.length).fill(null); let ag = 0, al = 0;
    for (let i = 1; i < c.length; i++) {
        const d = c[i][4] - c[i - 1][4], g = Math.max(0, d), l = Math.max(0, -d);
        if (i <= 14) { ag += g / 14; al += l / 14; if (i === 14) o[i] = al === 0 ? 100 : 100 - 100 / (1 + ag / al); }
        else { ag = (ag * 13 + g) / 14; al = (al * 13 + l) / 14; o[i] = al === 0 ? 100 : 100 - 100 / (1 + ag / al); }
    } return o;
}

// ---- reference panel: features at every bar -------------------------------
const REF = [];
for (const [key, c] of Object.entries(candles)) {
    if (!c || c.length < 80) continue;
    const closes = c.map((x) => x[4]);
    const A = atr14(c), R = rsi14(c), E20 = ema(closes, 20), E50 = ema(closes, 50);
    for (let i = 60; i < c.length - H; i++) {
        const a = A[i], r = R[i];
        if (!(a > 0) || r == null) continue;
        const px = closes[i];
        REF.push({
            key, ts: c[i][0], closeTs: c[i][0] + BAR_MS,
            outcomeTs: c[i + H][0] + BAR_MS,
            f: [r, (a / px) * 100, (px - E20[i]) / a, (E20[i] - E50[i]) / a, (px - closes[i - 6]) / a],
            fwd: (closes[i + H] - px) / a,   // raw forward move in ATR, unsigned
        });
    }
}
// scaling fitted ONLY on bars before the decision window
const scaleSet = REF.filter((r) => r.closeTs < SCALE_CUTOFF);
const D = 5, mu = [], sg = [];
for (let d = 0; d < D; d++) {
    const v = scaleSet.map((r) => r.f[d]).filter(Number.isFinite);
    const m = v.reduce((a, b) => a + b, 0) / v.length;
    mu.push(m);
    sg.push(Math.sqrt(v.reduce((s, x) => s + (x - m) ** 2, 0) / (v.length - 1)) || 1);
}
for (const r of REF) r.z = r.f.map((x, d) => (x - mu[d]) / sg[d]);
console.log(`reference bars: ${REF.length.toLocaleString()}  (scaling fitted on ${scaleSet.length.toLocaleString()} pre-window bars)`);

// ---- decisions: feature vector at the decision bar -------------------------
const featAt = new Map();
for (const [key, c] of Object.entries(candles)) {
    if (!c || c.length < 80) continue;
    const closes = c.map((x) => x[4]);
    const A = atr14(c), R = rsi14(c), E20 = ema(closes, 20), E50 = ema(closes, 50);
    featAt.set(key, { c, closes, A, R, E20, E50 });
}
const rows = [];
for (const p of panel) {
    if (p.y6 == null) continue;
    const st = featAt.get(`${p.platform}:${p.symbol}`);
    if (!st) continue;
    let i = -1;
    for (let j = 0; j < st.c.length; j++) if (st.c[j][0] + BAR_MS > p.ts) { i = j; break; }
    if (i < 60) continue;
    const a = st.A[i], r = st.R[i];
    if (!(a > 0) || r == null) continue;
    const px = st.closes[i];
    const f = [r, (a / px) * 100, (px - st.E20[i]) / a, (st.E20[i] - st.E50[i]) / a, (px - st.closes[i - 6]) / a];
    rows.push({ ...p, z: f.map((x, d) => (x - mu[d]) / sg[d]) });
}
console.log(`decisions with a usable state vector: ${rows.length} / ${panel.length}`);

// ---- base rate by nearest neighbours --------------------------------------
for (const q of rows) {
    const cand = [];
    for (const r of REF) {
        if (r.outcomeTs >= q.ts) continue;               // outcome must be known before the decision
        let d2 = 0;
        for (let k = 0; k < D; k++) { const dd = q.z[k] - r.z[k]; d2 += dd * dd; if (d2 > 1e9) break; }
        cand.push({ d2, fwd: r.fwd });
    }
    if (cand.length < 500) { q.base = null; continue; }
    cand.sort((a, b) => a.d2 - b.d2);
    q.top = cand.slice(0, 500).map((x) => x.fwd);
    const at = (k) => q.top.slice(0, k).reduce((s, x) => s + x, 0) / k;
    q.baseAtK = { 50: at(50), 100: at(100), 200: at(200), 500: at(500) };
    q.baseRaw = q.baseAtK[K];
    q.base = q.dir * q.baseRaw;
    q.neighbours = cand.length;
}
const usable = rows.filter((r) => r.base != null);
console.log(`decisions with >= 500 eligible historical neighbours: ${usable.length}\n`);

// ---- the test --------------------------------------------------------------
const mean = (a) => a.reduce((x, y) => x + y, 0) / a.length;
const sd = (a) => { const m = mean(a); return Math.sqrt(a.reduce((s, x) => s + (x - m) ** 2, 0) / (a.length - 1)); };
function clusterT(vals, keys) {
    const n = vals.length, m = mean(vals), g = new Map();
    for (let i = 0; i < n; i++) g.set(keys[i], (g.get(keys[i]) || 0) + (vals[i] - m));
    const G = g.size; let meat = 0; for (const s of g.values()) meat += s * s;
    const se = Math.sqrt((meat / (n * n)) * (G / Math.max(1, G - 1)));
    return { n, G, m, se, t: m / se };
}
const pass = usable.filter((r) => r.base >= 0);
const veto = usable.filter((r) => r.base < 0);
const show = (label, set) => {
    if (!set.length) { console.log(`${label.padEnd(30)} n=0`); return null; }
    const c = clusterT(set.map((r) => r.y6), set.map((r) => r.day));
    console.log(`${label.padEnd(30)} n=${String(c.n).padStart(3)}  mean y6=${c.m.toFixed(3).padStart(7)} ATR  t_day=${c.t.toFixed(2).padStart(5)}  win=${(100 * set.filter((r) => r.y6 > 0).length / set.length).toFixed(0)}%`);
    return c;
};
console.log('=== PRIMARY: does the base rate separate good decisions from bad? ===');
const all = show('all decisions', usable);
const p1 = show('base rate AGREED (keep)', pass);
const v1 = show('base rate DISAGREED (veto)', veto);

if (p1 && v1) {
    const diff = p1.m - v1.m, se = Math.sqrt(p1.se ** 2 + v1.se ** 2), t = diff / se;
    console.log(`\nseparation (keep - veto) = ${diff.toFixed(3)} ATR   t = ${t.toFixed(2)}`);
    console.log(`veto rate = ${(100 * veto.length / usable.length).toFixed(1)}% of decisions`);
    console.log(`mean y6 if you had traded only the KEEPs: ${p1.m.toFixed(3)}  vs trading everything: ${all.m.toFixed(3)}`);
    console.log(`\n=> ${t > 1.96 ? 'VETO SEPARATES (worth building)' : t < -1.96 ? 'VETO IS BACKWARDS' : 'NO SEPARATION — the veto is decoration'}`);
}

// ---- robustness grid (secondary, labelled) ---------------------------------
console.log('\n=== SECONDARY: robustness across k and horizon (NOT the primary test) ===');
console.log('   h    k     n   keep_n  keep_mean  veto_mean    diff      t');
for (const h of [1, 2, 6, 12]) {
    for (const k of [50, 100, 200, 500]) {
        const sub = usable.filter((r) => r[`y${h}`] != null);
        if (sub.length < 30) continue;
        const P = [], V = [], PK = [], VK = [];
        for (const q of sub) {
            const sgn = q.dir * q.baseAtK[k];
            (sgn >= 0 ? P : V).push(q[`y${h}`]);
            (sgn >= 0 ? PK : VK).push(q.day);
        }
        if (P.length < 10 || V.length < 10) continue;
        const cp = clusterT(P, PK), cv = clusterT(V, VK);
        const d = cp.m - cv.m, se = Math.sqrt(cp.se ** 2 + cv.se ** 2);
        console.log(`  ${String(h).padStart(2)}  ${String(k).padStart(3)}  ${String(sub.length).padStart(4)}  ${String(P.length).padStart(6)}  ${cp.m.toFixed(3).padStart(8)}  ${cv.m.toFixed(3).padStart(9)}  ${d.toFixed(3).padStart(7)}  ${(d / se).toFixed(2).padStart(6)}`);
    }
}

// ---- is the veto just a trend filter? --------------------------------------
console.log('\n=== DIAGNOSTIC: is the veto just "do not fight the trend"? ===');
const L = usable.filter((r) => r.dir === 1), S = usable.filter((r) => r.dir === -1);
console.log(`  longs  vetoed: ${L.filter((r) => r.base < 0).length}/${L.length} (${(100 * L.filter((r) => r.base < 0).length / L.length).toFixed(0)}%)`);
console.log(`  shorts vetoed: ${S.filter((r) => r.base < 0).length}/${S.length} (${(100 * S.filter((r) => r.base < 0).length / S.length).toFixed(0)}%)`);
// simple trend-agreement baseline: EMA20>EMA50 (z[3] > 0) agreeing with direction
const trendAgree = usable.filter((r) => (r.z[3] > 0 ? 1 : -1) === r.dir);
const trendFight = usable.filter((r) => (r.z[3] > 0 ? 1 : -1) !== r.dir);
if (trendAgree.length > 10 && trendFight.length > 10) {
    const ca = clusterT(trendAgree.map((r) => r.y6), trendAgree.map((r) => r.day));
    const cf = clusterT(trendFight.map((r) => r.y6), trendFight.map((r) => r.day));
    const d = ca.m - cf.m, se = Math.sqrt(ca.se ** 2 + cf.se ** 2);
    console.log(`  plain trend filter:  agree n=${ca.n} mean=${ca.m.toFixed(3)} | fight n=${cf.n} mean=${cf.m.toFixed(3)} | diff=${d.toFixed(3)} t=${(d / se).toFixed(2)}`);
    const agreeBoth = usable.filter((r) => ((r.z[3] > 0 ? 1 : -1) === r.dir) === (r.base >= 0)).length;
    console.log(`  veto agrees with the plain trend filter on ${(100 * agreeBoth / usable.length).toFixed(0)}% of decisions`);
}

// ---- how big a separation could this sample even detect? -------------------
console.log('\n=== POWER ===');
const sdAll = sd(usable.map((r) => r.y6));
const nEffP = (sd(pass.map((r) => r.y6)) / p1.se) ** 2, nEffV = (sd(veto.map((r) => r.y6)) / v1.se) ** 2;
console.log(`  effective n: keep ~${nEffP.toFixed(0)}, veto ~${nEffV.toFixed(0)} (nominal ${pass.length}/${veto.length})`);
console.log(`  detectable separation at 80% power: ${(2.8 * sdAll * Math.sqrt(1 / nEffP + 1 / nEffV)).toFixed(3)} ATR`);
console.log(`  observed separation: ${(p1.m - v1.m).toFixed(3)} ATR`);
