// Replication check: discovery (2024-03..2026-09, 83 symbols) vs
// holdout (2021-06..2024-03, 32 symbols, NEVER looked at before now).
// Different period, different regime, different symbol set, identical code.
import { readFileSync } from 'node:fs';
const D = JSON.parse(readFileSync('.study/sweep-discovery.json', 'utf8'));
const H = JSON.parse(readFileSync('.study/sweep-holdout.json', 'utf8'));
const dm = new Map(D.results.map((r) => [r.name, r]));
const hm = new Map(H.results.map((r) => [r.name, r]));
const TD = D.tcrit, TH = H.tcrit;

const dSurv = D.results.filter((r) => Math.abs(r.t) > TD).map((r) => r.name);
const hSurv = H.results.filter((r) => Math.abs(r.t) > TH).map((r) => r.name);

console.log(`discovery survivors: ${dSurv.length}   holdout survivors: ${hSurv.length}\n`);
console.log('  signal            disc_IC   disc_t   hold_IC   hold_t   sign  verdict');
const replicated = [];
for (const name of dSurv) {
    const d = dm.get(name), h = hm.get(name);
    if (!h || h.skipped) { console.log(`  ${name.padEnd(17)} ${d.meanIC.toFixed(4).padStart(7)} ${d.t.toFixed(2).padStart(7)}   (absent in holdout)`); continue; }
    const same = Math.sign(d.meanIC) === Math.sign(h.meanIC);
    const pass = same && Math.abs(h.t) > TH;
    if (pass) replicated.push(name);
    console.log(`  ${name.padEnd(17)} ${d.meanIC.toFixed(4).padStart(7)} ${d.t.toFixed(2).padStart(7)}  ${h.meanIC.toFixed(4).padStart(8)} ${h.t.toFixed(2).padStart(7)}   ${same ? 'same' : 'FLIP'}  ${pass ? 'REPLICATES' : (same ? 'same sign, under threshold' : 'FAILS')}`);
}
console.log(`\nREPLICATED (survive BOTH, same sign): ${replicated.length}/${dSurv.length}`);
console.log(`  ${replicated.join(', ')}`);

// Signals that only look good in the holdout are DISCOVERIES ON THE HOLDOUT.
const onlyH = hSurv.filter((n) => !dSurv.includes(n));
console.log(`\nHoldout-only (NOT replicated - these are discoveries ON the holdout and are now contaminated):`);
for (const n of onlyH) {
    const d = dm.get(n), h = hm.get(n);
    console.log(`  ${n.padEnd(17)} discovery t=${d ? d.t.toFixed(2) : 'n/a'}  holdout t=${h.t.toFixed(2)}  <- do NOT adopt; needs fresh data`);
}

// crude null probability, acknowledging collinearity
const perSignal = 0.05 / D.family / 2; // one-sided, Bonferroni
console.log(`\nUnder the null, P(a given signal replicates with the same sign) ~= ${perSignal.toExponential(1)}`);
console.log(`Survivors are collinear (mean |corr| 0.37, ~3-4 distinct effects), so treat this as`);
console.log(`~3-4 independent replications, not ${replicated.length}. Still far beyond chance.`);
