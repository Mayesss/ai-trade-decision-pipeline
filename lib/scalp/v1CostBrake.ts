function envInt(name: string, fallback: number, min: number, max: number): number {
  const n = Math.floor(Number(process.env[name]));
  if (!Number.isFinite(n)) return fallback;
  return Math.max(min, Math.min(max, n));
}

export type ScalpV1ResearchHardCaps = {
  maxCandidates: number;
  maxBatchSizeDiscover: number;
  maxBatchSizeLoad: number;
  maxBatchSizePrepare: number;
  maxBatchSizeWorker: number;
  maxBatchSizePromotion: number;
  maxAttempts: number;
  maxSelfHops: number;
  maxMinCandlesPerWeek: number;
};

export function resolveScalpV1ResearchHardCaps(): ScalpV1ResearchHardCaps {
  return {
    maxCandidates: envInt("SCALP_V1_RESEARCH_MAX_CANDIDATES_CAP", 80, 10, 2_000),
    maxBatchSizeDiscover: envInt("SCALP_V1_RESEARCH_DISCOVER_BATCH_CAP", 40, 1, 400),
    maxBatchSizeLoad: envInt("SCALP_V1_RESEARCH_LOAD_BATCH_CAP", 12, 1, 200),
    maxBatchSizePrepare: envInt("SCALP_V1_RESEARCH_PREPARE_BATCH_CAP", 12, 1, 200),
    maxBatchSizeWorker: envInt("SCALP_V1_RESEARCH_WORKER_BATCH_CAP", 60, 1, 600),
    maxBatchSizePromotion: envInt("SCALP_V1_RESEARCH_PROMOTION_BATCH_CAP", 120, 1, 1_500),
    maxAttempts: envInt("SCALP_V1_RESEARCH_MAX_ATTEMPTS_CAP", 3, 1, 10),
    maxSelfHops: envInt("SCALP_V1_RESEARCH_MAX_SELF_HOPS_CAP", 3, 0, 20),
    maxMinCandlesPerWeek: envInt(
      "SCALP_V1_RESEARCH_WORKER_MIN_CANDLES_CAP",
      2_000,
      20,
      20_000,
    ),
  };
}

export function clampScalpV1HardCap(value: number, hardCap: number): number {
  return Math.max(1, Math.min(Math.floor(value), hardCap));
}
