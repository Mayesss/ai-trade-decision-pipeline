/**
 * Cross-symbol pooled significance for composer promotion.
 *
 * At higher timeframes (M30/H1) a single symbol rarely accumulates enough
 * trades for the promotion significance gate (40 trades / lowerBoundR), even
 * over a 48-week stage-C window. But the edge being tested lives in the
 * STRATEGY (arm + blocks), not the symbol — so we pool the same strategy's
 * stage-C trades across its sibling symbols (same venue + session + arm +
 * model version) and gate on the POOLED sample.
 *
 * We only need each sibling's per-symbol sufficient statistics — trade count
 * n, mean per-trade R, and sample std of R — which are already persisted in
 * `metadata.v3Ranking.stageC.stats`. From those we reconstruct the EXACT mean
 * and sample variance of the concatenated per-trade population (no per-trade
 * arrays required), then recompute lowerBoundR = meanR - 1.64*stderrR over the
 * pool — identical in form to the per-symbol gate, just over more samples.
 *
 * Statistical effect: a strategy with a consistent cross-symbol edge gets a
 * TIGHTER (higher) pooled lowerBoundR and clears the gate; a one-symbol fluke
 * gets diluted by its weaker siblings and is correctly rejected. This is the
 * anti-overfitting property we want.
 */

export interface StageStatGroup {
  /** trade count for this symbol's stage-C window */
  n: number;
  /** mean per-trade R */
  meanR: number;
  /** sample standard deviation of per-trade R */
  stdR: number;
}

export interface PooledStageStats {
  n: number;
  meanR: number;
  stdR: number;
  stderrR: number;
  lowerBoundR: number;
  /** number of sibling symbols contributing to the pool */
  symbols: number;
}

const Z_95_ONE_SIDED = 1.64;

function isFiniteNumber(value: unknown): value is number {
  return typeof value === "number" && Number.isFinite(value);
}

/**
 * Combine per-symbol stage-C sufficient statistics into the exact statistics
 * of the pooled per-trade population. Returns null when no group has trades.
 *
 * Uses the standard combination of group means/variances:
 *   pooledMean = Σ(n_i·mean_i) / Σn_i
 *   totalSS    = Σ[ (n_i-1)·sd_i²  +  n_i·(mean_i - pooledMean)² ]
 *   pooledVar  = totalSS / (N-1)            (sample variance of the union)
 *   stderr     = sqrt(pooledVar) / sqrt(N)
 *   lowerBound = pooledMean - 1.64·stderr
 */
export function combineStageStats(groups: StageStatGroup[]): PooledStageStats | null {
  const valid = groups.filter(
    (g) => isFiniteNumber(g.n) && g.n >= 1 && isFiniteNumber(g.meanR),
  );
  if (!valid.length) return null;

  const N = valid.reduce((acc, g) => acc + g.n, 0);
  if (N < 1) return null;

  const pooledMean = valid.reduce((acc, g) => acc + g.n * g.meanR, 0) / N;

  let totalSS = 0;
  for (const g of valid) {
    const sd = isFiniteNumber(g.stdR) && g.stdR > 0 ? g.stdR : 0;
    const within = g.n > 1 ? (g.n - 1) * sd * sd : 0;
    const between = g.n * (g.meanR - pooledMean) * (g.meanR - pooledMean);
    totalSS += within + between;
  }
  const pooledVar = N > 1 ? totalSS / (N - 1) : 0;
  const pooledSd = Math.sqrt(Math.max(0, pooledVar));
  const stderrR = N > 0 ? pooledSd / Math.sqrt(N) : 0;
  const lowerBoundR = pooledMean - Z_95_ONE_SIDED * stderrR;

  return {
    n: N,
    meanR: pooledMean,
    stdR: pooledSd,
    stderrR,
    lowerBoundR,
    symbols: valid.length,
  };
}

/**
 * Symbol-less strategy identity — the cohort over which trades are pooled.
 * Mirrors the discovery behavior fingerprint MINUS the symbol.
 */
export function composerCohortKey(parts: {
  venue: string;
  session: string;
  armId: string;
  modelVersion: string;
}): string {
  return [
    String(parts.venue || "").trim().toLowerCase(),
    String(parts.session || "").trim().toLowerCase(),
    String(parts.armId || "").trim().toLowerCase(),
    String(parts.modelVersion || "").trim().toLowerCase(),
  ].join(":");
}
