import type { NextApiRequest, NextApiResponse } from "next";

function parseBoolLike(value: unknown, fallback: boolean): boolean {
  const normalized = String(value ?? "")
    .trim()
    .toLowerCase();
  if (!normalized) return fallback;
  if (["1", "true", "yes", "on"].includes(normalized)) return true;
  if (["0", "false", "no", "off"].includes(normalized)) return false;
  return fallback;
}

function envBool(name: string, fallback: boolean): boolean {
  return parseBoolLike(process.env[name], fallback);
}

function envInt(name: string, fallback: number, min: number, max: number): number {
  const n = Math.floor(Number(process.env[name]));
  if (!Number.isFinite(n)) return fallback;
  return Math.max(min, Math.min(max, n));
}

function firstQueryValue(value: string | string[] | undefined): string | undefined {
  if (typeof value === "string") return value.trim() || undefined;
  if (Array.isArray(value) && value.length > 0) {
    return String(value[0] || "").trim() || undefined;
  }
  return undefined;
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

export function maybeRespondScalpV1ResearchPaused(params: {
  req: NextApiRequest;
  res: NextApiResponse;
  routeId: string;
}): boolean {
  const paused = envBool("SCALP_V1_RESEARCH_PAUSED", true);
  if (!paused) return false;

  const forceRun = parseBoolLike(firstQueryValue(params.req.query.forceRun), false);
  const forceAllowed = envBool("SCALP_V1_RESEARCH_ALLOW_FORCE_RUN", false);
  if (forceRun && forceAllowed) return false;

  params.res.status(200).json({
    ok: true,
    paused: true,
    routeId: params.routeId,
    reason: "SCALP_V1_RESEARCH_PAUSED",
    message:
      "Legacy scalp-v1 research loop is paused by cost brake. Execution, guardrail, and housekeeping remain active.",
    forceRunAllowed: forceAllowed,
  });
  return true;
}
