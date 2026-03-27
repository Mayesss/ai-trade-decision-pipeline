import type { ScalpV2PrimitiveBlockMap } from "./types";

export const MODEL_GUIDED_COMPOSER_V2_STRATEGY_ID =
  "model_guided_composer_v2";

export type ModelGuidedComposerArmId =
  | "regime"
  | "compress"
  | "orb"
  | "auction"
  | "trend"
  | "vwap"
  | "basis"
  | "relative"
  | "season"
  | "reclaim"
  | "hss"
  | "adaptive";

export interface ModelGuidedComposerExecutionPlan {
  armId: ModelGuidedComposerArmId;
  strategyId: string;
  source: "pattern_block" | "tune_prefix" | "fallback";
  patternBlockId: string | null;
}

const DEFAULT_ARM: ModelGuidedComposerArmId = "regime";

const ARM_TO_STRATEGY_ID: Record<ModelGuidedComposerArmId, string> =
  Object.freeze({
    regime: "regime_pullback_m15_m3",
    compress: "compression_breakout_pullback_m15_m3",
    orb: "opening_range_breakout_retest_m5_m1",
    auction: "failed_auction_extreme_reversal_m15_m1",
    trend: "trend_day_reacceleration_m15_m3",
    vwap: "anchored_vwap_reversion_m15_m3",
    basis: "basis_dislocation_reversion_proxy_m15_m3",
    relative: "relative_value_spread_proxy_m15_m3",
    season: "session_seasonality_bias_m15_m3",
    reclaim: "pdh_pdl_reclaim_m15_m3",
    hss: "hss_ict_m15_m3_guarded",
    adaptive: "adaptive_meta_selector_m15_m3",
  });

const PATTERN_BLOCK_TO_ARM: Record<string, ModelGuidedComposerArmId> =
  Object.freeze({
    pattern_regime_bias: "regime",
    pattern_pullback_structure: "regime",
    pattern_ifvg_displacement: "regime",
    pattern_compression_breakout: "compress",
    pattern_opening_range_breakout_retest: "orb",
    pattern_failed_auction_extreme: "auction",
    pattern_trend_day_reacceleration: "trend",
    pattern_anchored_vwap_reversion: "vwap",
    pattern_basis_dislocation_reversion: "basis",
    pattern_relative_value_spread_reversion: "relative",
    pattern_session_seasonality_bias: "season",
    pattern_pdh_pdl_reclaim: "reclaim",
    pattern_hss_ict_structure: "hss",
    pattern_adaptive_pattern_router: "adaptive",
  });

function normalizeArm(value: unknown): ModelGuidedComposerArmId | null {
  const normalized = String(value || "")
    .trim()
    .toLowerCase();
  if (
    normalized === "regime" ||
    normalized === "compress" ||
    normalized === "orb" ||
    normalized === "auction" ||
    normalized === "trend" ||
    normalized === "vwap" ||
    normalized === "basis" ||
    normalized === "relative" ||
    normalized === "season" ||
    normalized === "reclaim" ||
    normalized === "hss" ||
    normalized === "adaptive"
  ) {
    return normalized;
  }
  return null;
}

function fallbackPlan(
  patternBlockId: string | null = null,
): ModelGuidedComposerExecutionPlan {
  return {
    armId: DEFAULT_ARM,
    strategyId: ARM_TO_STRATEGY_ID[DEFAULT_ARM],
    source: "fallback",
    patternBlockId,
  };
}

export function isModelGuidedComposerStrategyId(value: unknown): boolean {
  return (
    String(value || "").trim().toLowerCase() ===
    MODEL_GUIDED_COMPOSER_V2_STRATEGY_ID
  );
}

export function parseModelGuidedComposerArmFromTuneId(
  tuneId: unknown,
): ModelGuidedComposerArmId | null {
  const normalized = String(tuneId || "")
    .trim()
    .toLowerCase();
  if (!normalized) return null;
  const match = normalized.match(/^mdl_([a-z0-9]+)(?:_|$)/);
  if (!match) return null;
  return normalizeArm(match[1]);
}

export function buildModelGuidedComposerTuneId(params: {
  armId: ModelGuidedComposerArmId;
  digest: string;
}): string {
  const armId = normalizeArm(params.armId) || DEFAULT_ARM;
  const digest = String(params.digest || "")
    .trim()
    .toLowerCase()
    .replace(/[^a-f0-9]/g, "");
  const token = (digest || "seed00000000").slice(0, 10);
  return `mdl_${armId}_${token}`;
}

export function resolveModelGuidedComposerExecutionPlanFromTuneId(
  tuneId: unknown,
): ModelGuidedComposerExecutionPlan {
  const armId = parseModelGuidedComposerArmFromTuneId(tuneId);
  if (!armId) return fallbackPlan();
  return {
    armId,
    strategyId: ARM_TO_STRATEGY_ID[armId],
    source: "tune_prefix",
    patternBlockId: null,
  };
}

export function resolveModelGuidedComposerExecutionPlanFromBlocks(
  blocksByFamily: Partial<ScalpV2PrimitiveBlockMap> | null | undefined,
): ModelGuidedComposerExecutionPlan {
  const patterns = Array.isArray(blocksByFamily?.pattern)
    ? blocksByFamily!.pattern.map((row) => String(row || "").trim()).filter(Boolean)
    : [];
  for (const patternBlockId of patterns) {
    const armId = PATTERN_BLOCK_TO_ARM[patternBlockId];
    if (!armId) continue;
    return {
      armId,
      strategyId: ARM_TO_STRATEGY_ID[armId],
      source: "pattern_block",
      patternBlockId,
    };
  }
  return fallbackPlan(patterns[0] || null);
}

export function resolveModelGuidedComposerExecutionPlan(params: {
  tuneId?: unknown;
  blocksByFamily?: Partial<ScalpV2PrimitiveBlockMap> | null;
}): ModelGuidedComposerExecutionPlan {
  const fromBlocks = resolveModelGuidedComposerExecutionPlanFromBlocks(
    params.blocksByFamily,
  );
  if (fromBlocks.source === "pattern_block") return fromBlocks;
  const fromTune = resolveModelGuidedComposerExecutionPlanFromTuneId(
    params.tuneId,
  );
  if (fromTune.source === "tune_prefix") return fromTune;
  return fromBlocks.source === "fallback" ? fromBlocks : fallbackPlan();
}

export function resolveScalpExecutionStrategyId(params: {
  strategyId: unknown;
  tuneId?: unknown;
}): string {
  const strategyId = String(params.strategyId || "").trim().toLowerCase();
  if (!isModelGuidedComposerStrategyId(strategyId)) return strategyId;
  return resolveModelGuidedComposerExecutionPlanFromTuneId(params.tuneId)
    .strategyId;
}
