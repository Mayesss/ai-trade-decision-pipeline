// XAlpha's stated cadence -> trial count -> expected best t under H0.
// 15s/factor, 16min/generation, ~3h/cycle (arXiv 2607.08332).
let seed = 90210 >>> 0;
const rnd = () => { seed = (seed + 0x6D2B79F5) >>> 0; let t = seed;
    t = Math.imul(t ^ (t >>> 15), t | 1); t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296; };
const normal = () => { let u = 0, v = 0; while (u === 0) u = rnd(); while (v === 0) v = rnd();
    return Math.sqrt(-2 * Math.log(u)) * Math.cos(2 * Math.PI * v); };

const perGen = Math.round((16 * 60) / 15);
const gens = Math.round((3 * 60) / 16);
console.log(`stated cadence -> ${perGen} factors/generation x ${gens} generations = ${perGen * gens} factors/cycle`);

const DRAWS = 20000;
for (const N of [perGen, perGen * gens]) {
    let sum = 0, over3 = 0, over2 = 0;
    for (let d = 0; d < DRAWS; d++) {
        let mx = -Infinity;
        for (let i = 0; i < N; i++) { const z = normal(); if (z > mx) mx = z; }
        sum += mx; if (mx > 3.0) over3++; if (mx > 2.0) over2++;
    }
    console.log(`  N=${String(N).padStart(4)} independent trials under H0: E[max t]=${(sum / DRAWS).toFixed(2)}  P(best>3)=${(100 * over3 / DRAWS).toFixed(1)}%  P(best>2)=${(100 * over2 / DRAWS).toFixed(1)}%`);
}
