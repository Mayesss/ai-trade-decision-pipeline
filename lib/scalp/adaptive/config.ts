export interface ScalpAdaptiveRuntimeConfig {
  enabled: boolean;
  retrainCadenceWeeks: number;
  lockDays: number;
  minExpectancyDeltaR: number;
  minSupport: number;
  pilotSize: number;
  pilotRotateWeeks: number;
  minConfidence: number;
  edgeScoreThreshold: number;
  maxPatternArms: number;
  lookaheadBars: number;
}

function toBool(value: unknown, fallback: boolean): boolean {
  const normalized = String(value || "").trim().toLowerCase();
  if (!normalized) return fallback;
  if (["1", "true", "yes", "on"].includes(normalized)) return true;
  if (["0", "false", "no", "off"].includes(normalized)) return false;
  return fallback;
}

function toPositiveInt(value: unknown, fallback: number, max = Number.POSITIVE_INFINITY): number {
  const n = Math.floor(Number(value));
  if (!Number.isFinite(n) || n <= 0) return fallback;
  return Math.max(1, Math.min(max, n));
}

function toFinite(value: unknown, fallback: number): number {
  const n = Number(value);
  if (!Number.isFinite(n)) return fallback;
  return n;
}

export function resolveScalpAdaptiveRuntimeConfig(): ScalpAdaptiveRuntimeConfig {
  return {
    enabled: toBool(process.env.SCALP_ADAPTIVE_ENABLED, false),
    retrainCadenceWeeks: toPositiveInt(process.env.SCALP_ADAPTIVE_RETRAIN_CADENCE_WEEKS, 1, 12),
    lockDays: toPositiveInt(process.env.SCALP_ADAPTIVE_LOCK_DAYS, 14, 90),
    minExpectancyDeltaR: toFinite(process.env.SCALP_ADAPTIVE_MIN_EXPECTANCY_DELTA_R, 0.02),
    minSupport: toPositiveInt(process.env.SCALP_ADAPTIVE_MIN_SUPPORT, 30, 10000),
    pilotSize: toPositiveInt(process.env.SCALP_ADAPTIVE_PILOT_SIZE, 6, 200),
    pilotRotateWeeks: toPositiveInt(process.env.SCALP_ADAPTIVE_PILOT_ROTATE_WEEKS, 4, 26),
    minConfidence: Math.max(0, Math.min(1, toFinite(process.env.SCALP_ADAPTIVE_MIN_CONFIDENCE, 0.6))),
    edgeScoreThreshold: toFinite(process.env.SCALP_ADAPTIVE_EDGE_SCORE_THRESHOLD, 0.08),
    maxPatternArms: toPositiveInt(process.env.SCALP_ADAPTIVE_MAX_PATTERN_ARMS, 24, 256),
    lookaheadBars: toPositiveInt(process.env.SCALP_ADAPTIVE_LOOKAHEAD_BARS, 4, 48),
  };
}
