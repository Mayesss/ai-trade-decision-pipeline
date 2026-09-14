// How large a t-stat does the BEST of N strategy variants show under a pure
// null? Monte Carlo, no formula recalled: draw N iid standard-normal t-stats,
// take the max, average over many draws.
// mulberry32: a weak LCG cycles here and saturates E[max] at large N.
let seed = 13371 >>> 0;
const rnd = () => {
    seed = (seed + 0x6D2B79F5) >>> 0;
    let t = seed;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
};
const normal = () => { // Box-Muller
    let u = 0, v = 0;
    while (u === 0) u = rnd();
    while (v === 0) v = rnd();
    return Math.sqrt(-2 * Math.log(u)) * Math.cos(2 * Math.PI * v);
};
const DRAWS = 4000;
console.log('Expected best t-stat from N INDEPENDENT strategy variants, under H0 (no edge at all):');
console.log('  N trials      E[max t]   P(best t > 3.0)');
for (const N of [1, 10, 100, 1000, 10000, 100000]) {
    let sum = 0, over = 0;
    for (let d = 0; d < DRAWS; d++) {
        let mx = -Infinity;
        for (let i = 0; i < N; i++) { const z = normal(); if (z > mx) mx = z; }
        sum += mx; if (mx > 3.0) over++;
    }
    console.log(`  ${String(N).padStart(7)}      ${(sum / DRAWS).toFixed(2).padStart(6)}      ${(100 * over / DRAWS).toFixed(1).padStart(5)}%`);
}
console.log('\nNote: strategy variants are highly CORRELATED, so effective N < nominal N');
console.log('and the real figures sit below these. The direction is what matters.');

// How much effective sample does the available history actually carry?
console.log('\nHistorical sample available (measured 2026-09-11):');
const venues = [
    { name: 'bitget perp', symbols: 10, bars: 8000, barsPerDay: 6 },
    { name: 'capital CFD', symbols: 15, bars: 3200, barsPerDay: 6 },
];
let totalBars = 0, totalDayUnits = 0;
for (const v of venues) {
    const bars = v.symbols * v.bars;
    const dayUnits = v.symbols * (v.bars / v.barsPerDay);
    totalBars += bars; totalDayUnits += dayUnits;
    console.log(`  ${v.name.padEnd(12)} ${v.symbols} symbols x ${v.bars} bars = ${bars.toLocaleString()} bar-obs (${Math.round(dayUnits).toLocaleString()} symbol-days)`);
}
console.log(`  TOTAL        ${totalBars.toLocaleString()} bar-obs, ${Math.round(totalDayUnits).toLocaleString()} symbol-days`);
console.log(`  vs 453 live directional decisions (effective n = 94) from 3 months of trading.`);
// cross-sectional correlation haircut: symbols within a class move together
for (const share of [0.2, 0.1, 0.05]) {
    console.log(`  if only ${(share * 100).toFixed(0)}% of that is independent -> ~${Math.round(totalDayUnits * share).toLocaleString()} effective obs`);
}
console.log('\nPower target from the audit: ~4,900 obs to resolve a 0.10 ATR/trade edge at |t|>3.');
