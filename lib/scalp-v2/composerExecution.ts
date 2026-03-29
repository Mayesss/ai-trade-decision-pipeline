import type { ScalpV2PrimitiveBlockMap } from "./types";

export const MODEL_GUIDED_COMPOSER_V2_STRATEGY_ID =
  "model_guided_composer_v2";

export type ModelGuidedComposerBaseArm =
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

export type ModelGuidedComposerTimeframeVariant = "m15_m3" | "m5_m1" | "m5_m3";

export type ModelGuidedComposerArmId =
  `${ModelGuidedComposerBaseArm}_${ModelGuidedComposerTimeframeVariant}`;

export const COMPOSER_TIMEFRAME_VARIANTS: readonly {
  readonly label: ModelGuidedComposerTimeframeVariant;
  readonly baseTf: "M15" | "M5";
  readonly confirmTf: "M3" | "M1";
}[] = Object.freeze([
  { label: "m15_m3" as const, baseTf: "M15" as const, confirmTf: "M3" as const },
  { label: "m5_m1" as const, baseTf: "M5" as const, confirmTf: "M1" as const },
  { label: "m5_m3" as const, baseTf: "M5" as const, confirmTf: "M3" as const },
]);

export interface ModelGuidedComposerExecutionPlan {
  armId: ModelGuidedComposerArmId;
  baseArm: ModelGuidedComposerBaseArm;
  tfVariant: ModelGuidedComposerTimeframeVariant;
  strategyId: string;
  source: "pattern_block" | "tune_prefix" | "fallback";
  patternBlockId: string | null;
}

const DEFAULT_BASE_ARM: ModelGuidedComposerBaseArm = "regime";
const DEFAULT_TF_VARIANT: ModelGuidedComposerTimeframeVariant = "m15_m3";
const DEFAULT_ARM: ModelGuidedComposerArmId = "regime_m15_m3";

// --- Base arm default timeframe (used for backward-compatible bare arm resolution) ---

const BASE_ARM_DEFAULT_TF: Record<ModelGuidedComposerBaseArm, ModelGuidedComposerTimeframeVariant> =
  Object.freeze({
    regime: "m15_m3",
    compress: "m15_m3",
    orb: "m5_m1",
    auction: "m15_m3",
    trend: "m15_m3",
    vwap: "m15_m3",
    basis: "m15_m3",
    relative: "m15_m3",
    season: "m15_m3",
    reclaim: "m15_m3",
    hss: "m15_m3",
    adaptive: "m15_m3",
  });

// --- Strategy ID resolution ---

// Strategy ID templates: use {TF} as a placeholder for the timeframe suffix.
// Most strategies have the TF at the end, but hss has "_guarded" after it.
const STRATEGY_ID_TEMPLATES: Partial<Record<ModelGuidedComposerBaseArm, string>> =
  Object.freeze({
    hss: "hss_ict_{TF}_guarded",
  });

function strategyIdForArm(baseArm: ModelGuidedComposerBaseArm, tfVariant: ModelGuidedComposerTimeframeVariant): string {
  const [baseTfNum, confirmTfNum] = tfVariantToNums(tfVariant);
  const tfSuffix = `m${baseTfNum}_m${confirmTfNum}`;

  // Use explicit template if defined
  const template = STRATEGY_ID_TEMPLATES[baseArm];
  if (template) return template.replace("{TF}", tfSuffix);

  // Default: strip original TF suffix and append the new one
  const base = BASE_STRATEGY_IDS[baseArm];
  if (!base) return BASE_STRATEGY_IDS[DEFAULT_BASE_ARM];
  const stripped = base.replace(/_m\d+_m\d+/, "");
  return `${stripped}_${tfSuffix}`;
}

function tfVariantToNums(v: ModelGuidedComposerTimeframeVariant): [number, number] {
  if (v === "m5_m1") return [5, 1];
  if (v === "m5_m3") return [5, 3];
  return [15, 3];
}

const BASE_STRATEGY_IDS: Record<ModelGuidedComposerBaseArm, string> =
  Object.freeze({
    regime: "regime_pullback_m15_m3",
    compress: "compression_breakout_pullback_m15_m3",
    orb: "opening_range_breakout_retest_m5_m1",
    auction: "failed_auction_extreme_reversal_m15_m3",
    trend: "trend_day_reacceleration_m15_m3",
    vwap: "anchored_vwap_reversion_m15_m3",
    basis: "basis_dislocation_reversion_proxy_m15_m3",
    relative: "relative_value_spread_proxy_m15_m3",
    season: "session_seasonality_bias_m15_m3",
    reclaim: "pdh_pdl_reclaim_m15_m3",
    hss: "hss_ict_m15_m3_guarded",
    adaptive: "adaptive_meta_selector_m15_m3",
  });

// Build the full ARM_TO_STRATEGY_ID map: 12 base arms × 3 TF variants = 36 entries
const ARM_TO_STRATEGY_ID: Record<ModelGuidedComposerArmId, string> =
  Object.freeze(
    Object.fromEntries(
      (Object.keys(BASE_STRATEGY_IDS) as ModelGuidedComposerBaseArm[]).flatMap(
        (baseArm) =>
          COMPOSER_TIMEFRAME_VARIANTS.map((v) => [
            `${baseArm}_${v.label}` as ModelGuidedComposerArmId,
            strategyIdForArm(baseArm, v.label),
          ]),
      ),
    ) as Record<ModelGuidedComposerArmId, string>,
  );

const PATTERN_BLOCK_TO_BASE_ARM: Record<string, ModelGuidedComposerBaseArm> =
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

// Keep old PATTERN_BLOCK_TO_ARM as a convenience (maps to default TF variant)
const PATTERN_BLOCK_TO_ARM: Record<string, ModelGuidedComposerArmId> =
  Object.freeze(
    Object.fromEntries(
      Object.entries(PATTERN_BLOCK_TO_BASE_ARM).map(([block, baseArm]) => [
        block,
        `${baseArm}_${BASE_ARM_DEFAULT_TF[baseArm]}` as ModelGuidedComposerArmId,
      ]),
    ),
  );

const ALL_BASE_ARMS: readonly ModelGuidedComposerBaseArm[] = Object.freeze(
  Object.keys(BASE_STRATEGY_IDS) as ModelGuidedComposerBaseArm[],
);

const ALL_TF_VARIANTS: readonly ModelGuidedComposerTimeframeVariant[] =
  Object.freeze(["m15_m3", "m5_m1", "m5_m3"] as const);

const VALID_ARM_IDS: ReadonlySet<string> = new Set(
  ALL_BASE_ARMS.flatMap((b) => ALL_TF_VARIANTS.map((t) => `${b}_${t}`)),
);

function normalizeBaseArm(value: unknown): ModelGuidedComposerBaseArm | null {
  const normalized = String(value || "").trim().toLowerCase();
  if (ALL_BASE_ARMS.includes(normalized as ModelGuidedComposerBaseArm)) {
    return normalized as ModelGuidedComposerBaseArm;
  }
  return null;
}

function normalizeTfVariant(value: unknown): ModelGuidedComposerTimeframeVariant | null {
  const normalized = String(value || "").trim().toLowerCase();
  if (ALL_TF_VARIANTS.includes(normalized as ModelGuidedComposerTimeframeVariant)) {
    return normalized as ModelGuidedComposerTimeframeVariant;
  }
  return null;
}

function normalizeArm(value: unknown): ModelGuidedComposerArmId | null {
  const normalized = String(value || "").trim().toLowerCase();
  if (VALID_ARM_IDS.has(normalized)) return normalized as ModelGuidedComposerArmId;
  // Backward compat: bare base arm → append default TF variant
  const baseArm = normalizeBaseArm(normalized);
  if (baseArm) return `${baseArm}_${BASE_ARM_DEFAULT_TF[baseArm]}` as ModelGuidedComposerArmId;
  return null;
}

function splitArm(armId: ModelGuidedComposerArmId): {
  baseArm: ModelGuidedComposerBaseArm;
  tfVariant: ModelGuidedComposerTimeframeVariant;
} {
  for (const base of ALL_BASE_ARMS) {
    if (armId.startsWith(`${base}_`)) {
      const suffix = armId.slice(base.length + 1);
      const tfVar = normalizeTfVariant(suffix);
      if (tfVar) return { baseArm: base, tfVariant: tfVar };
    }
  }
  return { baseArm: DEFAULT_BASE_ARM, tfVariant: DEFAULT_TF_VARIANT };
}

function buildPlan(
  armId: ModelGuidedComposerArmId,
  source: ModelGuidedComposerExecutionPlan["source"],
  patternBlockId: string | null,
): ModelGuidedComposerExecutionPlan {
  const { baseArm, tfVariant } = splitArm(armId);
  return {
    armId,
    baseArm,
    tfVariant,
    strategyId: ARM_TO_STRATEGY_ID[armId] || strategyIdForArm(baseArm, tfVariant),
    source,
    patternBlockId,
  };
}

function fallbackPlan(
  patternBlockId: string | null = null,
): ModelGuidedComposerExecutionPlan {
  return buildPlan(DEFAULT_ARM, "fallback", patternBlockId);
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
  const normalized = String(tuneId || "").trim().toLowerCase();
  if (!normalized) return null;
  // New format: mdl_{baseArm}_{tfVariant}_{digest} e.g. mdl_regime_m15_m3_a1b2c3d4e5
  const fullMatch = normalized.match(/^mdl_([a-z]+)_(m\d+_m\d+)(?:_|$)/);
  if (fullMatch) {
    const baseArm = normalizeBaseArm(fullMatch[1]);
    const tfVar = normalizeTfVariant(fullMatch[2]);
    if (baseArm && tfVar) return `${baseArm}_${tfVar}`;
  }
  // Backward compat: mdl_{baseArm}_{digest} e.g. mdl_regime_abc123
  const legacyMatch = normalized.match(/^mdl_([a-z]+)(?:_|$)/);
  if (legacyMatch) {
    const baseArm = normalizeBaseArm(legacyMatch[1]);
    if (baseArm) return `${baseArm}_${BASE_ARM_DEFAULT_TF[baseArm]}`;
  }
  return null;
}

export function buildModelGuidedComposerTuneId(params: {
  armId: ModelGuidedComposerArmId;
  digest: string;
}): string {
  const armId = normalizeArm(params.armId) || DEFAULT_ARM;
  const { baseArm, tfVariant } = splitArm(armId);
  const digest = String(params.digest || "")
    .trim()
    .toLowerCase()
    .replace(/[^a-f0-9]/g, "");
  const token = (digest || "seed00000000").slice(0, 10);
  return `mdl_${baseArm}_${tfVariant}_${token}`;
}

export function resolveModelGuidedComposerExecutionPlanFromTuneId(
  tuneId: unknown,
): ModelGuidedComposerExecutionPlan {
  const armId = parseModelGuidedComposerArmFromTuneId(tuneId);
  if (!armId) return fallbackPlan();
  return buildPlan(armId, "tune_prefix", null);
}

export function resolveModelGuidedComposerExecutionPlanFromBlocks(
  blocksByFamily: Partial<ScalpV2PrimitiveBlockMap> | null | undefined,
  tfVariant?: ModelGuidedComposerTimeframeVariant,
): ModelGuidedComposerExecutionPlan {
  const patterns = Array.isArray(blocksByFamily?.pattern)
    ? blocksByFamily!.pattern.map((row) => String(row || "").trim()).filter(Boolean)
    : [];
  for (const patternBlockId of patterns) {
    const baseArm = PATTERN_BLOCK_TO_BASE_ARM[patternBlockId];
    if (!baseArm) continue;
    const variant = tfVariant || BASE_ARM_DEFAULT_TF[baseArm];
    const armId = `${baseArm}_${variant}` as ModelGuidedComposerArmId;
    return buildPlan(armId, "pattern_block", patternBlockId);
  }
  return fallbackPlan(patterns[0] || null);
}

export function resolveBaseArmFromPatternBlock(
  blocksByFamily: Partial<ScalpV2PrimitiveBlockMap> | null | undefined,
): ModelGuidedComposerBaseArm {
  const patterns = Array.isArray(blocksByFamily?.pattern)
    ? blocksByFamily!.pattern.map((row) => String(row || "").trim()).filter(Boolean)
    : [];
  for (const patternBlockId of patterns) {
    const baseArm = PATTERN_BLOCK_TO_BASE_ARM[patternBlockId];
    if (baseArm) return baseArm;
  }
  return DEFAULT_BASE_ARM;
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
