/**
 * Cell-stability empirical test for scalp-v4.
 *
 * Question: Does the regime-cell label have predictive power across time?
 * I.e., if a (strategy, cell) pair had positive expectancy in OLDER windows,
 * does it still have positive expectancy in NEWER windows of the same cell?
 *
 * If yes → the v4 cell layer is load-bearing; per-cell history predicts
 * future per-cell performance.
 * If no → "cell label" is just a colored sticker that doesn't carry across
 * time; the 2-year walk-forward is mostly expensive theatre and recency-
 * based selection would be a better default.
 *
 * Method: For each (deployment, cell) with >= MIN_WINDOWS entries in
 * incremental_state_json.cells[cellId].windowExpectancyR (chronological),
 * split the array at the midpoint. Compute mean expectancy in old half vs
 * new half, then aggregate sign agreement + Pearson correlation + the
 * (+/+, +/-, -/+, -/-) quadrant distribution.
 *
 * Run: npx tsx scripts/scalp-regimes-cell-stability.ts
 *   MIN_WINDOWS=12 to require ≥12 chronological windows per pair
 *   CELL=vol=high|trend=trending_up|risk=risk_on to drill into one cell
 */
import nextEnv from "@next/env";
nextEnv.loadEnvConfig(process.cwd());
process.env.SCALP_PG_USE_HTTP = "1";

import { scalpPrisma } from "../lib/scalp/pg/client";
import { sql } from "../lib/scalp/pg/sql";
import { SCALP_REGIME_CLASSIFIER_VERSION } from "../lib/scalp/regimes/classifier";

interface CellSplit {
  deploymentId: string;
  cellId: string;
  nWindows: number;
  oldHalfMean: number;
  newHalfMean: number;
}

const MIN_WINDOWS = Math.max(6, Math.floor(Number(process.env.MIN_WINDOWS) || 12));
const CELL_FILTER = String(process.env.CELL || "").trim();

function mean(arr: number[]): number {
  if (!arr.length) return Number.NaN;
  return arr.reduce((acc, v) => acc + v, 0) / arr.length;
}

function pearson(xs: number[], ys: number[]): number {
  const n = xs.length;
  if (n < 3) return Number.NaN;
  const mx = mean(xs);
  const my = mean(ys);
  let num = 0;
  let dx2 = 0;
  let dy2 = 0;
  for (let i = 0; i < n; i += 1) {
    const dx = xs[i]! - mx;
    const dy = ys[i]! - my;
    num += dx * dy;
    dx2 += dx * dx;
    dy2 += dy * dy;
  }
  const denom = Math.sqrt(dx2 * dy2);
  if (denom === 0) return Number.NaN;
  return num / denom;
}

function fmtPct(v: number): string {
  if (!Number.isFinite(v)) return "—";
  return `${(v * 100).toFixed(1)}%`;
}

function fmtNum(v: number, digits = 4): string {
  if (!Number.isFinite(v)) return "—";
  return v.toFixed(digits);
}

(async () => {
  const db = scalpPrisma();
  const classifierVersion = SCALP_REGIME_CLASSIFIER_VERSION;

  // One row per (deployment_id, cell_id) with the per-window expectancy array.
  // Filter out rows synthesized from envelope (those don't have real
  // per-window history — they're a single fabricated point).
  type Row = {
    deploymentId: string;
    cellId: string;
    arr: number[] | null;
    synthesizedAt: number | null;
  };
  const rows = await db.$queryRaw<Row[]>(sql`
    SELECT
      r.deployment_id AS "deploymentId",
      cell.key AS "cellId",
      (cell.value->'windowExpectancyR') AS arr,
      NULLIF(r.incremental_state_json->>'synthesizedAt', '')::bigint AS "synthesizedAt"
    FROM scalp_regime_walkforward_results r,
         LATERAL jsonb_each(r.incremental_state_json->'cells') AS cell
    WHERE r.classifier_version = ${classifierVersion}
      AND r.incremental_state_json IS NOT NULL
      AND r.incremental_state_json ? 'cells'
      AND cell.value ? 'windowExpectancyR'
      AND jsonb_array_length(cell.value->'windowExpectancyR') >= ${MIN_WINDOWS};
  `);

  const splits: CellSplit[] = [];
  for (const row of rows) {
    if (row.synthesizedAt) continue; // skip backfilled state
    if (CELL_FILTER && row.cellId !== CELL_FILTER) continue;
    const arr = Array.isArray(row.arr)
      ? (row.arr as unknown[]).map((v) => Number(v)).filter(Number.isFinite)
      : [];
    if (arr.length < MIN_WINDOWS) continue;
    const mid = Math.floor(arr.length / 2);
    const oldHalf = arr.slice(0, mid);
    const newHalf = arr.slice(mid);
    splits.push({
      deploymentId: row.deploymentId,
      cellId: row.cellId,
      nWindows: arr.length,
      oldHalfMean: mean(oldHalf),
      newHalfMean: mean(newHalf),
    });
  }

  console.log(`\nclassifier: ${classifierVersion}`);
  console.log(`min windows per pair: ${MIN_WINDOWS}`);
  if (CELL_FILTER) console.log(`cell filter: ${CELL_FILTER}`);
  console.log(`eligible (deployment, cell) pairs: ${splits.length}`);
  if (splits.length === 0) {
    console.log("\nno pairs meet the threshold — nothing to test.");
    await db.$disconnect();
    return;
  }

  const oldArr = splits.map((s) => s.oldHalfMean);
  const newArr = splits.map((s) => s.newHalfMean);
  const corr = pearson(oldArr, newArr);
  const signAgree = splits.filter((s) => Math.sign(s.oldHalfMean) === Math.sign(s.newHalfMean)).length;
  const pp = splits.filter((s) => s.oldHalfMean > 0 && s.newHalfMean > 0).length;
  const pn = splits.filter((s) => s.oldHalfMean > 0 && s.newHalfMean <= 0).length;
  const np = splits.filter((s) => s.oldHalfMean <= 0 && s.newHalfMean > 0).length;
  const nn = splits.filter((s) => s.oldHalfMean <= 0 && s.newHalfMean <= 0).length;

  console.log("\n=== AGGREGATE ===");
  console.log(`  sign agreement:    ${signAgree}/${splits.length} (${fmtPct(signAgree / splits.length)})`);
  console.log(`  pearson r:         ${fmtNum(corr, 3)}    (1.0 = perfect, 0 = noise, -1 = inverted)`);
  console.log(`  avg old-half mean: ${fmtNum(mean(oldArr))}`);
  console.log(`  avg new-half mean: ${fmtNum(mean(newArr))}`);
  console.log("\n=== QUADRANTS (old → new) ===");
  console.log(`  (+,+)  worked then & now:  ${pp.toString().padStart(5)}  ${fmtPct(pp / splits.length).padStart(7)}`);
  console.log(`  (+,-)  decayed:            ${pn.toString().padStart(5)}  ${fmtPct(pn / splits.length).padStart(7)}`);
  console.log(`  (-,+)  reversed (lucky?):  ${np.toString().padStart(5)}  ${fmtPct(np / splits.length).padStart(7)}`);
  console.log(`  (-,-)  consistently bad:   ${nn.toString().padStart(5)}  ${fmtPct(nn / splits.length).padStart(7)}`);
  console.log(`  random baseline:           25% per quadrant if regime label is meaningless`);

  // Per-cell breakdown
  const byCell = new Map<string, CellSplit[]>();
  for (const s of splits) {
    const list = byCell.get(s.cellId) || [];
    list.push(s);
    byCell.set(s.cellId, list);
  }
  console.log("\n=== PER-CELL (cells with >= 20 pairs) ===");
  console.log("  cell                                              n     sign%    pearson  avg_old   avg_new");
  const sortedCells = Array.from(byCell.entries())
    .filter(([, list]) => list.length >= 20)
    .sort((a, b) => b[1].length - a[1].length);
  for (const [cellId, list] of sortedCells) {
    const cOld = list.map((x) => x.oldHalfMean);
    const cNew = list.map((x) => x.newHalfMean);
    const cAgree = list.filter((x) => Math.sign(x.oldHalfMean) === Math.sign(x.newHalfMean)).length;
    console.log(
      `  ${cellId.padEnd(50)} ${list.length.toString().padStart(4)}  ${fmtPct(cAgree / list.length).padStart(6)}  ${fmtNum(pearson(cOld, cNew), 3).padStart(7)}  ${fmtNum(mean(cOld)).padStart(8)}  ${fmtNum(mean(cNew)).padStart(8)}`,
    );
  }

  console.log("\n=== HOW TO READ ===");
  console.log("  • sign agreement >70%  AND  pearson > 0.3  → cell label has predictive power; v4 is load-bearing");
  console.log("  • sign agreement ~50%  AND  pearson near 0 → cell label is theatre; recency-only would do at least as well");
  console.log("  • (+,-) >> (+,+)              → regime classifier names today's regime but past 'same-regime' edge has decayed");
  console.log("  • (-,-) high                  → strong negative-screen value even if positive prediction is weak");

  await db.$disconnect();
})().catch((err) => {
  console.error("FAILED:", err);
  process.exit(1);
});
