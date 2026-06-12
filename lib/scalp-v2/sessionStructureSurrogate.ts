import type { SessionStructureStageAPrediction } from "./sessionStructureAdaptiveSearch";

export interface SessionStructureSurrogateConfig {
  enabled: boolean;
  prioritize: boolean;
  skipProb: number;
  skipMinSamples: number;
  maxSkipPct: number;
}

function envBool(name: string, fallback: boolean): boolean {
  const raw = String(process.env[name] || "").trim().toLowerCase();
  if (!raw) return fallback;
  return raw === "1" || raw === "true" || raw === "yes" || raw === "on";
}

function envFiniteOr(name: string, fallback: number): number {
  const raw = String(process.env[name] || "").trim();
  if (!raw) return fallback;
  const value = Number(raw);
  return Number.isFinite(value) ? value : fallback;
}

export function resolveSessionStructureSurrogateConfig(): SessionStructureSurrogateConfig {
  return {
    // DEFAULT OFF: live validation (2026-06-07) showed stage-A pass is
    // non-stationary — history-averaged priors mis-predict (even invert) current
    // pass rates, so skip/prioritise would act on bad signal. Re-enable only
    // once the prior is recency-weighted and skip decisions are backtested
    // against held-out windows. See [[project_surrogate_nonstationary]].
    enabled: envBool("SCALP_V2_SURROGATE_PRESCREEN_ENABLED", false),
    prioritize: envBool("SCALP_V2_SURROGATE_PRIORITIZE", true),
    skipProb: Math.max(0, Math.min(1, envFiniteOr("SCALP_V2_SURROGATE_SKIP_PROB", 0.02))),
    skipMinSamples: Math.max(
      1,
      Math.floor(envFiniteOr("SCALP_V2_SURROGATE_SKIP_MIN_SAMPLES", 40)),
    ),
    maxSkipPct: Math.max(0, Math.min(1, envFiniteOr("SCALP_V2_SURROGATE_MAX_SKIP_PCT", 0.5))),
  };
}

export interface SurrogateSkip<T> {
  candidate: T;
  prediction: SessionStructureStageAPrediction;
}

export interface SurrogatePrescreenResult<T> {
  /** Survivors, reordered by P(pass) desc when prioritising. */
  ordered: T[];
  /** Evidence-gated, capped hopeless candidates to reject this window. */
  skipped: Array<SurrogateSkip<T>>;
}

/**
 * Pure surrogate pre-screen: predicts P(stage-A pass) per candidate, then
 *  (a) evidence-gated hard-skips the most hopeless (high samples + very low P),
 *      capped at maxSkipPct of the input, never touching exploration candidates,
 *  (b) reorders the survivors by P(pass) desc so promising ones backtest first.
 * Disabled ⇒ identity (original order, no skips).
 */
export function applySurrogatePrescreen<T>(params: {
  candidates: T[];
  predict: (candidate: T) => SessionStructureStageAPrediction;
  config: SessionStructureSurrogateConfig;
  isExploration?: (candidate: T) => boolean;
  tieBreak?: (a: T, b: T) => number;
}): SurrogatePrescreenResult<T> {
  const { candidates, predict, config } = params;
  if (!config.enabled || candidates.length === 0) {
    return { ordered: candidates.slice(), skipped: [] };
  }
  const isExploration = params.isExploration || (() => false);
  const tieBreak = params.tieBreak || (() => 0);

  const scored = candidates.map((candidate, index) => ({
    candidate,
    index,
    prediction: predict(candidate),
    exploration: isExploration(candidate),
  }));

  // Skip candidates: enough evidence, very low P, not exploration. Skip the
  // most hopeless first, capped so we never drop more than maxSkipPct.
  const skipCap = Math.floor(candidates.length * config.maxSkipPct);
  const skipEligible = scored
    .filter(
      (row) =>
        !row.exploration &&
        row.prediction.samples >= config.skipMinSamples &&
        row.prediction.probability <= config.skipProb,
    )
    .sort(
      (a, b) =>
        a.prediction.probability - b.prediction.probability ||
        tieBreak(a.candidate, b.candidate) ||
        a.index - b.index,
    )
    .slice(0, Math.max(0, skipCap));

  const skipSet = new Set(skipEligible.map((row) => row.index));
  const survivors = scored.filter((row) => !skipSet.has(row.index));

  const ordered = config.prioritize
    ? survivors
        .slice()
        .sort(
          (a, b) =>
            b.prediction.probability - a.prediction.probability ||
            tieBreak(a.candidate, b.candidate) ||
            a.index - b.index,
        )
        .map((row) => row.candidate)
    : survivors.map((row) => row.candidate);

  return {
    ordered,
    skipped: skipEligible.map((row) => ({
      candidate: row.candidate,
      prediction: row.prediction,
    })),
  };
}
