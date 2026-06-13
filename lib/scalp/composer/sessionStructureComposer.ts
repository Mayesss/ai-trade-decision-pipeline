import crypto from "crypto";

import type {
  ScalpComposerModelScore,
  ScalpComposerPrimitiveBlockMap,
  ScalpComposerSession,
  ScalpComposerVenue,
} from "./types";

export const SESSION_STRUCTURE_COMPOSER_V1_STRATEGY_ID =
  "session_structure_composer_v1";

export const SESSION_STRUCTURE_COMPOSER_TUNE_ID_PREFIX = "ssc";

export type SessionStructureContextBlockId =
  | "m30_session_momentum"
  | "h1_directional_bias"
  | "opening_drive"
  | "atr_expansion"
  | "vwap_balance_shift"
  | "london_open_drive"
  | "ny_continuation"
  | "atr_low_chop_avoid";

export type SessionStructureLevelBlockId =
  | "session_vwap"
  | "opening_range_15m"
  | "opening_range_30m"
  | "opening_range_45m"
  | "opening_range_60m"
  | "previous_session_hl"
  | "asia_range_hl"
  | "prior_day_hl"
  | "intraday_swing_hl";

export type SessionStructureTriggerBlockId =
  | "breakout_retest_hold"
  | "breakout_retest_hold_tight"
  | "breakout_retest_hold_loose"
  | "vwap_pullback_continuation"
  | "sweep_reclaim"
  | "failed_breakout_return";

export type SessionStructureConfirmationBlockId =
  | "m15_close_acceptance"
  | "m30_close_acceptance"
  | "body_atr_expansion"
  | "volume_expansion_20"
  | "retest_wick_rejection";

export type SessionStructureManagementBlockId =
  | "fixed_1_5r_time_2h"
  | "fixed_2r_time_4h"
  | "target_next_session_level"
  | "trail_after_0_8r_time_3h";

export interface SessionStructureComposerPlan {
  contextId: SessionStructureContextBlockId;
  levelId: SessionStructureLevelBlockId;
  triggerId: SessionStructureTriggerBlockId;
  confirmationId: SessionStructureConfirmationBlockId;
  managementId: SessionStructureManagementBlockId;
  digest: string;
}

export interface SessionStructureAdaptivePriorEntry {
  score: number;
  samples: number;
  stageAPass: number;
  stageBPass: number;
  stageCPass: number;
}

export interface SessionStructureAdaptivePriorSet {
  version: string;
  generatedAtMs: number;
  windowToTs: number;
  minSamples: number;
  /** Population stage-A pass rate — the prior the surrogate shrinks toward. */
  stageABaseRate: number;
  global: Record<string, SessionStructureAdaptivePriorEntry>;
  scoped: Record<string, Record<string, SessionStructureAdaptivePriorEntry>>;
  diagnostics: {
    rows: number;
    scoredKeys: number;
    stageAPass: number;
    stageBPass: number;
    stageCPass: number;
  };
}

export interface SessionStructureAdaptiveScoreTrace {
  adjustment: number;
  keys: string[];
  matchedKeys: string[];
}

export type SessionStructureNoveltyLane = "exploit" | "adjacent" | "explore";

export interface SessionStructureNoveltyBudget {
  enabled?: boolean;
  exploitPct?: number;
  adjacentPct?: number;
  explorePct?: number;
  maxPerCluster?: number;
  maxPerFamily?: number;
  exploitAdjustmentThreshold?: number;
  /** Fraction of the budget reserved for evolved (offspring) cells. */
  evolutionPct?: number;
  /** Floor on the explore lane so evolution never zeroes global coverage. */
  minExplorePct?: number;
}

/** Provenance for an offspring grid cell (set by the evolution lane). */
export interface SessionStructureEvolutionTrace {
  op: "mutation" | "crossover";
  parentTuneIds: string[];
  parentFingerprints: string[];
  bestParentFitness: number;
  boost: number;
}

/**
 * Structural input the grid builder accepts for offspring (a subset of the
 * evolution module's SessionStructureOffspring — kept local to avoid a circular
 * import; consumed as a ReadonlyMap so the wider value type stays assignable).
 */
export interface SessionStructureOffspringSpec {
  op: "mutation" | "crossover";
  parentTuneIds: string[];
  parentFingerprints: string[];
  bestParentFitness: number;
  rankWeight: number;
}

export interface SessionStructureNoveltyTrace {
  lane: SessionStructureNoveltyLane;
  strategyClusterKey: string;
  strategyFamilyKey: string;
  familyRank: number;
  clusterRank: number;
}

export interface SessionStructureComposerCandidateDslSpec {
  candidateId: string;
  tuneId: string;
  venue: ScalpComposerVenue;
  symbol: string;
  entrySessionProfile: ScalpComposerSession;
  blocksByFamily: ScalpComposerPrimitiveBlockMap;
  sessionBlocksByFamily: {
    context: SessionStructureContextBlockId[];
    level: SessionStructureLevelBlockId[];
    trigger: SessionStructureTriggerBlockId[];
    confirmation: SessionStructureConfirmationBlockId[];
    management: SessionStructureManagementBlockId[];
  };
  referenceStrategyIds: string[];
  supportScore: number;
  generatedAtMs: number;
  behaviorFingerprint: string;
  compatibilityReasonCodes: string[];
  model: ScalpComposerModelScore;
  sessionComposerPlan: SessionStructureComposerPlan;
  adaptivePrior?: SessionStructureAdaptiveScoreTrace | null;
  novelty?: SessionStructureNoveltyTrace | null;
  evolution?: SessionStructureEvolutionTrace | null;
  regimeGateId?: string | null;
}

export const SESSION_STRUCTURE_CONTEXT_BLOCKS: readonly SessionStructureContextBlockId[] =
  Object.freeze([
    "m30_session_momentum",
    "h1_directional_bias",
    "opening_drive",
    "atr_expansion",
    "vwap_balance_shift",
    "london_open_drive",
    "ny_continuation",
    "atr_low_chop_avoid",
  ]);

export const SESSION_STRUCTURE_LEVEL_BLOCKS: readonly SessionStructureLevelBlockId[] =
  Object.freeze([
    "session_vwap",
    "opening_range_15m",
    "opening_range_30m",
    "opening_range_45m",
    "opening_range_60m",
    "previous_session_hl",
    "asia_range_hl",
    "prior_day_hl",
    "intraday_swing_hl",
  ]);

export const SESSION_STRUCTURE_TRIGGER_BLOCKS: readonly SessionStructureTriggerBlockId[] =
  Object.freeze([
    "breakout_retest_hold",
    "breakout_retest_hold_tight",
    "breakout_retest_hold_loose",
    "vwap_pullback_continuation",
    "sweep_reclaim",
    "failed_breakout_return",
  ]);

export const SESSION_STRUCTURE_CONFIRMATION_BLOCKS: readonly SessionStructureConfirmationBlockId[] =
  Object.freeze([
    "m15_close_acceptance",
    "m30_close_acceptance",
    "body_atr_expansion",
    "volume_expansion_20",
    "retest_wick_rejection",
  ]);

export const SESSION_STRUCTURE_MANAGEMENT_BLOCKS: readonly SessionStructureManagementBlockId[] =
  Object.freeze([
    "fixed_1_5r_time_2h",
    "fixed_2r_time_4h",
    "target_next_session_level",
    "trail_after_0_8r_time_3h",
  ]);

const CONTEXT_CODE: Record<SessionStructureContextBlockId, string> = {
  m30_session_momentum: "m30mom",
  h1_directional_bias: "h1bias",
  opening_drive: "opdrv",
  atr_expansion: "atrexp",
  vwap_balance_shift: "vwbal",
  london_open_drive: "londrv",
  ny_continuation: "nycont",
  atr_low_chop_avoid: "atrnochop",
};

const LEVEL_CODE: Record<SessionStructureLevelBlockId, string> = {
  session_vwap: "svwap",
  opening_range_15m: "orb15",
  opening_range_30m: "orb30",
  opening_range_45m: "orb45",
  opening_range_60m: "orb60",
  previous_session_hl: "psesshl",
  asia_range_hl: "asiahl",
  prior_day_hl: "pdhpdl",
  intraday_swing_hl: "swinghl",
};

const TRIGGER_CODE: Record<SessionStructureTriggerBlockId, string> = {
  breakout_retest_hold: "brkret",
  breakout_retest_hold_tight: "brktit",
  breakout_retest_hold_loose: "brkloo",
  vwap_pullback_continuation: "vwpb",
  sweep_reclaim: "sweep",
  failed_breakout_return: "fbr",
};

const CONFIRMATION_CODE: Record<SessionStructureConfirmationBlockId, string> = {
  m15_close_acceptance: "m15acc",
  m30_close_acceptance: "m30acc",
  body_atr_expansion: "bodyatr",
  volume_expansion_20: "vol20",
  retest_wick_rejection: "wickrej",
};

const MANAGEMENT_CODE: Record<SessionStructureManagementBlockId, string> = {
  fixed_1_5r_time_2h: "fix15r2h",
  fixed_2r_time_4h: "fix2r4h",
  target_next_session_level: "nextsess",
  trail_after_0_8r_time_3h: "trl08r3h",
};

const CONTEXT_BY_CODE = invert(CONTEXT_CODE);
const LEVEL_BY_CODE = invert(LEVEL_CODE);
const TRIGGER_BY_CODE = invert(TRIGGER_CODE);
const CONFIRMATION_BY_CODE = invert(CONFIRMATION_CODE);
const MANAGEMENT_BY_CODE = invert(MANAGEMENT_CODE);

function invert<T extends string>(input: Record<T, string>): Record<string, T> {
  return Object.fromEntries(
    Object.entries(input).map(([id, code]) => [String(code), id]),
  ) as Record<string, T>;
}

function emptyLegacyBlockMap(): ScalpComposerPrimitiveBlockMap {
  return {
    pattern: [],
    session_filter: [],
    state_machine: [],
    entry_trigger: [],
    exit_rule: [],
    risk_rule: [],
  };
}

function normalizeDigest(value: unknown): string {
  const raw = String(value || "").trim().toLowerCase();
  const hex = raw.replace(/[^a-f0-9]/g, "");
  if (hex.length >= 10) return hex.slice(0, 10);
  return crypto.createHash("sha1").update(raw || "session-structure").digest("hex").slice(0, 10);
}

export function isSessionStructureComposerStrategyId(value: unknown): boolean {
  return String(value || "").trim().toLowerCase() === SESSION_STRUCTURE_COMPOSER_V1_STRATEGY_ID;
}

export function buildSessionStructureComposerTuneId(params: {
  contextId: SessionStructureContextBlockId;
  levelId: SessionStructureLevelBlockId;
  triggerId: SessionStructureTriggerBlockId;
  confirmationId: SessionStructureConfirmationBlockId;
  managementId: SessionStructureManagementBlockId;
  digest: string;
}): string {
  return [
    SESSION_STRUCTURE_COMPOSER_TUNE_ID_PREFIX,
    CONTEXT_CODE[params.contextId],
    LEVEL_CODE[params.levelId],
    TRIGGER_CODE[params.triggerId],
    CONFIRMATION_CODE[params.confirmationId],
    MANAGEMENT_CODE[params.managementId],
    normalizeDigest(params.digest),
  ].join("_");
}

export function parseSessionStructureComposerTuneId(value: unknown): SessionStructureComposerPlan {
  const raw = String(value || "").trim().toLowerCase();
  const parts = raw.split("_");
  if (parts.length >= 7 && parts[0] === SESSION_STRUCTURE_COMPOSER_TUNE_ID_PREFIX) {
    return {
      contextId: CONTEXT_BY_CODE[parts[1]!] || "m30_session_momentum",
      levelId: LEVEL_BY_CODE[parts[2]!] || "session_vwap",
      triggerId: TRIGGER_BY_CODE[parts[3]!] || "breakout_retest_hold",
      confirmationId: CONFIRMATION_BY_CODE[parts[4]!] || "m15_close_acceptance",
      managementId: MANAGEMENT_BY_CODE[parts[5]!] || "fixed_2r_time_4h",
      digest: normalizeDigest(parts[6]),
    };
  }
  return {
    contextId: "m30_session_momentum",
    levelId: "session_vwap",
    triggerId: "vwap_pullback_continuation",
    confirmationId: "m15_close_acceptance",
    managementId: "fixed_2r_time_4h",
    digest: normalizeDigest(raw || "default"),
  };
}

export function sessionStructureBehaviorFingerprint(
  plan: Omit<SessionStructureComposerPlan, "digest">,
): string {
  return [
    plan.contextId,
    plan.levelId,
    plan.triggerId,
    plan.confirmationId,
    plan.managementId,
  ].join("|");
}

function isSessionStructureBreakoutRetestTrigger(triggerId: SessionStructureTriggerBlockId): boolean {
  return (
    triggerId === "breakout_retest_hold" ||
    triggerId === "breakout_retest_hold_tight" ||
    triggerId === "breakout_retest_hold_loose"
  );
}

function isSessionStructureOpeningRangeLevel(levelId: SessionStructureLevelBlockId): boolean {
  return (
    levelId === "opening_range_15m" ||
    levelId === "opening_range_30m" ||
    levelId === "opening_range_45m" ||
    levelId === "opening_range_60m"
  );
}

export function isSessionStructureHighLowLevel(levelId: SessionStructureLevelBlockId): boolean {
  return (
    isSessionStructureOpeningRangeLevel(levelId) ||
    levelId === "previous_session_hl" ||
    levelId === "asia_range_hl" ||
    levelId === "prior_day_hl" ||
    levelId === "intraday_swing_hl"
  );
}

export function validateSessionStructureCompatibility(params: {
  contextId: SessionStructureContextBlockId;
  levelId: SessionStructureLevelBlockId;
  triggerId: SessionStructureTriggerBlockId;
  confirmationId: SessionStructureConfirmationBlockId;
  managementId: SessionStructureManagementBlockId;
}): { compatible: boolean; reasonCodes: string[] } {
  const reasonCodes: string[] = [];
  if (params.triggerId === "vwap_pullback_continuation" && params.levelId !== "session_vwap") {
    reasonCodes.push("SESSION_COMPOSER_INCOMPAT_VWAP_TRIGGER_REQUIRES_SESSION_VWAP");
  }
  if (
    (params.triggerId === "failed_breakout_return" || params.triggerId === "sweep_reclaim") &&
    !isSessionStructureHighLowLevel(params.levelId)
  ) {
    reasonCodes.push("SESSION_COMPOSER_INCOMPAT_TRIGGER_REQUIRES_HIGH_LOW_LEVEL");
  }
  if (
    isSessionStructureBreakoutRetestTrigger(params.triggerId) &&
    !(
      isSessionStructureOpeningRangeLevel(params.levelId) ||
      params.levelId === "previous_session_hl" ||
      params.levelId === "intraday_swing_hl"
    )
  ) {
    reasonCodes.push("SESSION_COMPOSER_INCOMPAT_BREAKOUT_RETEST_PREFERS_INTRADAY_HL");
  }
  if (
    params.confirmationId === "retest_wick_rejection" &&
    !isSessionStructureHighLowLevel(params.levelId)
  ) {
    reasonCodes.push("SESSION_COMPOSER_INCOMPAT_WICK_REJECTION_REQUIRES_HIGH_LOW_LEVEL");
  }
  if (
    params.triggerId === "failed_breakout_return" &&
    params.managementId === "target_next_session_level"
  ) {
    reasonCodes.push("SESSION_COMPOSER_INCOMPAT_FAILED_BREAKOUT_TARGET_LEVEL_GEOMETRY_WEAK");
  }
  if (
    params.managementId === "target_next_session_level" &&
    !isSessionStructureHighLowLevel(params.levelId)
  ) {
    reasonCodes.push("SESSION_COMPOSER_INCOMPAT_TARGET_LEVEL_REQUIRES_HIGH_LOW_LEVEL");
  }
  return { compatible: reasonCodes.length === 0, reasonCodes };
}

function deterministicScore(seed: string): number {
  const digest = crypto.createHash("sha1").update(seed).digest();
  return digest.readUInt32BE(0) / 0xffffffff;
}

// Generation-time score nudge for higher-timeframe blocks (see scoreCombo).
// Tunable via env; modest relative to the ~0.13–0.22 per-dimension weights so
// it reshapes the ranked top-N without crowding out lower-TF combos entirely.
function envScoreBoost(name: string, fallback: number): number {
  const n = Number(process.env[name]);
  return Number.isFinite(n) && n >= 0 && n <= 0.5 ? n : fallback;
}
const SESSION_STRUCTURE_H1_SCORE_BOOST = envScoreBoost(
  "SCALP_SSC_H1_SCORE_BOOST",
  0.06,
);
const SESSION_STRUCTURE_M30_SCORE_BOOST = envScoreBoost(
  "SCALP_SSC_M30_SCORE_BOOST",
  0.03,
);

function scoreCombo(params: {
  venue: ScalpComposerVenue;
  symbol: string;
  session: ScalpComposerSession;
  contextId: SessionStructureContextBlockId;
  levelId: SessionStructureLevelBlockId;
  triggerId: SessionStructureTriggerBlockId;
  confirmationId: SessionStructureConfirmationBlockId;
  managementId: SessionStructureManagementBlockId;
  adaptivePriors?: SessionStructureAdaptivePriorSet | null;
}): number {
  const contextWeight: Record<SessionStructureContextBlockId, number> = {
    m30_session_momentum: 0.2,
    h1_directional_bias: 0.17,
    opening_drive: 0.18,
    atr_expansion: 0.16,
    vwap_balance_shift: 0.19,
    london_open_drive: 0.22,
    ny_continuation: 0.21,
    atr_low_chop_avoid: 0.2,
  };
  const levelWeight: Record<SessionStructureLevelBlockId, number> = {
    session_vwap: 0.22,
    opening_range_15m: 0.21,
    opening_range_30m: 0.2,
    opening_range_45m: 0.205,
    opening_range_60m: 0.195,
    previous_session_hl: 0.19,
    asia_range_hl: 0.15,
    prior_day_hl: 0.13,
    intraday_swing_hl: 0.18,
  };
  const triggerWeight: Record<SessionStructureTriggerBlockId, number> = {
    breakout_retest_hold: 0.22,
    breakout_retest_hold_tight: 0.215,
    breakout_retest_hold_loose: 0.205,
    vwap_pullback_continuation: 0.21,
    sweep_reclaim: 0.13,
    failed_breakout_return: 0.08,
  };
  const confirmationWeight: Record<SessionStructureConfirmationBlockId, number> = {
    m15_close_acceptance: 0.19,
    m30_close_acceptance: 0.17,
    body_atr_expansion: 0.16,
    volume_expansion_20: 0.14,
    retest_wick_rejection: 0.18,
  };
  const managementWeight: Record<SessionStructureManagementBlockId, number> = {
    fixed_1_5r_time_2h: 0.18,
    fixed_2r_time_4h: 0.19,
    target_next_session_level: 0.15,
    trail_after_0_8r_time_3h: 0.2,
  };
  const preferredPairBonus =
    (params.levelId === "session_vwap" && params.triggerId === "vwap_pullback_continuation") ||
    ((isSessionStructureOpeningRangeLevel(params.levelId) || params.levelId === "previous_session_hl") &&
      isSessionStructureBreakoutRetestTrigger(params.triggerId)) ||
    (params.levelId === "prior_day_hl" && params.triggerId === "sweep_reclaim") ||
    (params.contextId === "vwap_balance_shift" &&
      params.levelId === "session_vwap" &&
      params.triggerId === "vwap_pullback_continuation") ||
    (params.contextId === "atr_low_chop_avoid" &&
      isSessionStructureOpeningRangeLevel(params.levelId) &&
      isSessionStructureBreakoutRetestTrigger(params.triggerId))
      ? 0.08
      : 0;
  const duplicateClusterPenalty =
    params.contextId === "h1_directional_bias" &&
    params.levelId === "opening_range_30m" &&
    params.triggerId === "breakout_retest_hold"
      ? 0.035
      : 0;
  const seed = [
    params.venue,
    params.symbol,
    params.session,
    params.contextId,
    params.levelId,
    params.triggerId,
    params.confirmationId,
    params.managementId,
  ].join(":");
  const adaptive = resolveSessionStructureAdaptiveScore({
    priors: params.adaptivePriors || null,
    venue: params.venue,
    symbol: params.symbol,
    session: params.session,
    contextId: params.contextId,
    levelId: params.levelId,
    triggerId: params.triggerId,
    confirmationId: params.confirmationId,
    managementId: params.managementId,
  });
  // Tilt the score-ranked, capped generation toward higher timeframes. The
  // H1/M30 bands clear stage C at a far higher rate with positive net edge once
  // fee drag is removed (see scripts/scalp-tf-probe.ts + the band funnel), but
  // the static block weights above mildly disfavor them. This is an explicit
  // nudge; the adaptive priors then reinforce it as those bands keep passing.
  const higherTimeframeBoost =
    params.contextId === "h1_directional_bias"
      ? SESSION_STRUCTURE_H1_SCORE_BOOST
      : (isSessionStructureOpeningRangeLevel(params.levelId) &&
          params.levelId !== "opening_range_15m") ||
        params.confirmationId === "m30_close_acceptance" ||
        params.contextId === "m30_session_momentum"
      ? SESSION_STRUCTURE_M30_SCORE_BOOST
      : 0;
  return (
    contextWeight[params.contextId] +
    levelWeight[params.levelId] +
    triggerWeight[params.triggerId] +
    confirmationWeight[params.confirmationId] +
    managementWeight[params.managementId] +
    preferredPairBonus +
    higherTimeframeBoost +
    -duplicateClusterPenalty +
    (adaptive?.adjustment || 0) +
    deterministicScore(seed) * 0.02
  );
}

function sessionStructureLevelFamily(levelId: SessionStructureLevelBlockId): string {
  if (isSessionStructureOpeningRangeLevel(levelId)) return "opening_range";
  return levelId;
}

function sessionStructureTriggerFamily(triggerId: SessionStructureTriggerBlockId): string {
  if (isSessionStructureBreakoutRetestTrigger(triggerId)) return "breakout_retest";
  return triggerId;
}

function sessionStructureAdaptiveScopeKey(params: {
  venue: ScalpComposerVenue;
  symbol: string;
  session: ScalpComposerSession;
}): string {
  return [params.venue, String(params.symbol || "").trim().toUpperCase(), params.session].join(":");
}

export function sessionStructureAdaptiveKeys(
  plan: Omit<SessionStructureComposerPlan, "digest">,
): string[] {
  const levelFamily = sessionStructureLevelFamily(plan.levelId);
  const triggerFamily = sessionStructureTriggerFamily(plan.triggerId);
  const fingerprint = sessionStructureBehaviorFingerprint(plan);
  return [
    `behavior:${fingerprint}`,
    `cluster:${plan.contextId}|${levelFamily}|${triggerFamily}`,
    `context:${plan.contextId}`,
    `level:${plan.levelId}`,
    `level_family:${levelFamily}`,
    `trigger:${plan.triggerId}`,
    `trigger_family:${triggerFamily}`,
    `confirmation:${plan.confirmationId}`,
    `management:${plan.managementId}`,
  ];
}

function clampScore(value: number, min: number, max: number): number {
  const n = Number(value);
  if (!Number.isFinite(n)) return 0;
  return Math.max(min, Math.min(max, n));
}

function resolveSessionStructureAdaptiveScore(params: {
  priors?: SessionStructureAdaptivePriorSet | null;
  venue: ScalpComposerVenue;
  symbol: string;
  session: ScalpComposerSession;
  contextId: SessionStructureContextBlockId;
  levelId: SessionStructureLevelBlockId;
  triggerId: SessionStructureTriggerBlockId;
  confirmationId: SessionStructureConfirmationBlockId;
  managementId: SessionStructureManagementBlockId;
}): SessionStructureAdaptiveScoreTrace | null {
  const priors = params.priors;
  if (!priors) return null;
  const keys = sessionStructureAdaptiveKeys({
    contextId: params.contextId,
    levelId: params.levelId,
    triggerId: params.triggerId,
    confirmationId: params.confirmationId,
    managementId: params.managementId,
  });
  const scoped = priors.scoped[sessionStructureAdaptiveScopeKey(params)] || {};
  const weights: Record<string, number> = {
    behavior: 0.12,
    cluster: 0.08,
    context: 0.03,
    level: 0.04,
    level_family: 0.018,
    trigger: 0.04,
    trigger_family: 0.018,
    confirmation: 0.03,
    management: 0.03,
  };
  let adjustment = 0;
  const matchedKeys: string[] = [];
  for (const key of keys) {
    const prefix = key.split(":")[0] || "";
    const weight = weights[prefix] || 0;
    if (weight <= 0) continue;
    const scopedEntry = scoped[key];
    const globalEntry = priors.global[key];
    if (!scopedEntry && !globalEntry) continue;
    matchedKeys.push(key);
    const scopedScore = scopedEntry ? clampScore(scopedEntry.score, -1, 1) : 0;
    const globalScore = globalEntry ? clampScore(globalEntry.score, -1, 1) : 0;
    const score =
      scopedEntry && globalEntry
        ? scopedScore * 0.65 + globalScore * 0.35
        : scopedEntry
          ? scopedScore
          : globalScore * 0.75;
    adjustment += weight * score;
  }
  return {
    adjustment: clampScore(adjustment, -0.14, 0.2),
    keys,
    matchedKeys,
  };
}

function sessionStructureDiversityClusterKey(row: SessionStructureComposerCandidateDslSpec): string {
  const plan = row.sessionComposerPlan;
  return [
    plan.contextId,
    sessionStructureLevelFamily(plan.levelId),
    sessionStructureTriggerFamily(plan.triggerId),
  ].join("|");
}

function sessionStructureFamilyKey(row: SessionStructureComposerCandidateDslSpec): string {
  const plan = row.sessionComposerPlan;
  return [
    sessionStructureLevelFamily(plan.levelId),
    sessionStructureTriggerFamily(plan.triggerId),
  ].join("|");
}

function sessionStructureNoveltyLane(row: SessionStructureComposerCandidateDslSpec): SessionStructureNoveltyLane {
  const adjustment = Number(row.adaptivePrior?.adjustment || 0);
  const matched = (row.adaptivePrior?.matchedKeys || []).length > 0;
  if (matched && adjustment >= 0.05) return "exploit";
  if (matched || adjustment > 0) return "adjacent";
  return "explore";
}

function normalizeNoveltyBudget(
  budget: SessionStructureNoveltyBudget | null | undefined,
  maxCandidates: number,
  hasAdaptiveSignal: boolean,
): Required<SessionStructureNoveltyBudget> {
  const enabled = budget?.enabled !== false;
  const exploitPct = hasAdaptiveSignal ? clampScore(Number(budget?.exploitPct ?? 0.55), 0, 1) : 0;
  const adjacentPct = hasAdaptiveSignal ? clampScore(Number(budget?.adjacentPct ?? 0.25), 0, 1) : 0;
  const explorePctRaw = Number(budget?.explorePct);
  const explorePct = Number.isFinite(explorePctRaw)
    ? clampScore(explorePctRaw, 0, 1)
    : Math.max(0, 1 - exploitPct - adjacentPct);
  return {
    enabled,
    exploitPct,
    adjacentPct,
    explorePct,
    maxPerCluster: Math.max(1, Math.min(maxCandidates, Math.floor(Number(budget?.maxPerCluster || Math.ceil(maxCandidates * 0.08))))),
    maxPerFamily: Math.max(1, Math.min(maxCandidates, Math.floor(Number(budget?.maxPerFamily || Math.ceil(maxCandidates * 0.18))))),
    exploitAdjustmentThreshold: clampScore(Number(budget?.exploitAdjustmentThreshold ?? 0.05), -1, 1),
    evolutionPct: clampScore(Number(budget?.evolutionPct ?? 0), 0, 1),
    minExplorePct: clampScore(Number(budget?.minExplorePct ?? 0), 0, 1),
  };
}

function selectDiverseSessionStructureCandidates(
  rows: SessionStructureComposerCandidateDslSpec[],
  maxCandidates: number,
  noveltyBudget?: SessionStructureNoveltyBudget | null,
): SessionStructureComposerCandidateDslSpec[] {
  const hasAdaptiveSignal = rows.some((row) => (row.adaptivePrior?.matchedKeys || []).length > 0);
  const budget = normalizeNoveltyBudget(noveltyBudget, maxCandidates, hasAdaptiveSignal);
  if (budget.enabled) {
    return selectNovelSessionStructureCandidates(rows, maxCandidates, budget);
  }
  const clusters = new Map<string, SessionStructureComposerCandidateDslSpec[]>();
  for (const row of rows) {
    const key = sessionStructureDiversityClusterKey(row);
    const bucket = clusters.get(key) || [];
    bucket.push(row);
    clusters.set(key, bucket);
  }
  const orderedClusters = Array.from(clusters.values())
    .map((bucket) =>
      bucket.sort(
        (a, b) =>
          b.model.compositeScore - a.model.compositeScore ||
          a.behaviorFingerprint.localeCompare(b.behaviorFingerprint),
      ),
    )
    .sort(
      (a, b) =>
        (b[0]?.model.compositeScore || 0) - (a[0]?.model.compositeScore || 0) ||
        (a[0]?.behaviorFingerprint || "").localeCompare(b[0]?.behaviorFingerprint || ""),
    );
  const out: SessionStructureComposerCandidateDslSpec[] = [];
  let depth = 0;
  while (out.length < maxCandidates) {
    let added = false;
    for (const bucket of orderedClusters) {
      const row = bucket[depth];
      if (!row) continue;
      out.push(row);
      added = true;
      if (out.length >= maxCandidates) break;
    }
    if (!added) break;
    depth += 1;
  }
  return out.sort(
    (a, b) =>
      b.model.compositeScore - a.model.compositeScore ||
      a.behaviorFingerprint.localeCompare(b.behaviorFingerprint),
  );
}

function selectNovelSessionStructureCandidates(
  rows: SessionStructureComposerCandidateDslSpec[],
  maxCandidates: number,
  budget: Required<SessionStructureNoveltyBudget>,
): SessionStructureComposerCandidateDslSpec[] {
  const sorted = rows.slice().sort(
    (a, b) =>
      b.model.compositeScore - a.model.compositeScore ||
      a.behaviorFingerprint.localeCompare(b.behaviorFingerprint),
  );
  const clusterRank = new Map<string, Map<string, number>>();
  const familyRank = new Map<string, Map<string, number>>();
  for (const row of sorted) {
    const clusterKey = sessionStructureDiversityClusterKey(row);
    const familyKey = sessionStructureFamilyKey(row);
    const cBucket = clusterRank.get(clusterKey) || new Map<string, number>();
    cBucket.set(row.behaviorFingerprint, cBucket.size + 1);
    clusterRank.set(clusterKey, cBucket);
    const fBucket = familyRank.get(familyKey) || new Map<string, number>();
    fBucket.set(row.behaviorFingerprint, fBucket.size + 1);
    familyRank.set(familyKey, fBucket);
  }

  const targetExploit = Math.floor(maxCandidates * budget.exploitPct);
  const targetAdjacent = Math.floor(maxCandidates * budget.adjacentPct);
  const targetExplore = Math.max(0, maxCandidates - targetExploit - targetAdjacent);
  const laneTargets: Record<SessionStructureNoveltyLane, number> = {
    exploit: targetExploit,
    adjacent: targetAdjacent,
    explore: targetExplore,
  };
  const byLane: Record<SessionStructureNoveltyLane, SessionStructureComposerCandidateDslSpec[]> = {
    exploit: [],
    adjacent: [],
    explore: [],
  };
  for (const row of sorted) {
    const lane = sessionStructureNoveltyLane(row);
    const effectiveLane =
      lane === "exploit" && Number(row.adaptivePrior?.adjustment || 0) < budget.exploitAdjustmentThreshold
        ? "adjacent"
        : lane;
    byLane[effectiveLane].push(row);
  }

  const selected = new Map<string, SessionStructureComposerCandidateDslSpec>();
  const familyCounts = new Map<string, number>();
  const clusterCounts = new Map<string, number>();
  const take = (
    row: SessionStructureComposerCandidateDslSpec,
    enforceFamily: boolean,
    enforceCluster: boolean,
  ): boolean => {
    if (selected.has(row.behaviorFingerprint)) return false;
    const familyKey = sessionStructureFamilyKey(row);
    const clusterKey = sessionStructureDiversityClusterKey(row);
    if (enforceFamily && (familyCounts.get(familyKey) || 0) >= budget.maxPerFamily) return false;
    if (enforceCluster && (clusterCounts.get(clusterKey) || 0) >= budget.maxPerCluster) return false;
    const lane = sessionStructureNoveltyLane(row);
    selected.set(row.behaviorFingerprint, {
      ...row,
      novelty: {
        lane,
        strategyClusterKey: clusterKey,
        strategyFamilyKey: familyKey,
        clusterRank: clusterRank.get(clusterKey)?.get(row.behaviorFingerprint) || 0,
        familyRank: familyRank.get(familyKey)?.get(row.behaviorFingerprint) || 0,
      },
    });
    familyCounts.set(familyKey, (familyCounts.get(familyKey) || 0) + 1);
    clusterCounts.set(clusterKey, (clusterCounts.get(clusterKey) || 0) + 1);
    return true;
  };

  const pickFromLane = (lane: SessionStructureNoveltyLane, target: number): void => {
    if (target <= 0) return;
    for (const row of byLane[lane]) {
      if (selected.size >= maxCandidates) return;
      const laneCount = Array.from(selected.values()).filter((selectedRow) => selectedRow.novelty?.lane === lane).length;
      if (laneCount >= target) return;
      take(row, true, true);
    }
  };

  // Reserved evolution slice: prioritise offspring (mutation/crossover) cells
  // regardless of which adaptive lane they fall in, honouring diversity caps.
  // Carved from explore (evolution is a smarter explore), floored so global
  // coverage never drops to zero. Reduce explore by what was ACTUALLY taken so
  // an empty offspring set doesn't starve explore.
  const evolutionTarget = Math.floor(maxCandidates * (budget.evolutionPct || 0));
  const exploreFloor = Math.floor(maxCandidates * (budget.minExplorePct || 0));
  let evolutionTaken = 0;
  if (evolutionTarget > 0) {
    for (const row of sorted) {
      if (selected.size >= maxCandidates || evolutionTaken >= evolutionTarget) break;
      if (!row.evolution) continue;
      if (take(row, true, true)) evolutionTaken += 1;
    }
  }
  const adjustedExplore = Math.max(exploreFloor, laneTargets.explore - evolutionTaken);

  pickFromLane("exploit", laneTargets.exploit);
  pickFromLane("adjacent", laneTargets.adjacent);
  pickFromLane("explore", adjustedExplore);

  for (const row of sorted) {
    if (selected.size >= maxCandidates) break;
    take(row, true, true);
  }
  for (const row of sorted) {
    if (selected.size >= maxCandidates) break;
    take(row, false, true);
  }
  for (const row of sorted) {
    if (selected.size >= maxCandidates) break;
    take(row, false, false);
  }

  return Array.from(selected.values()).sort(
    (a, b) =>
      b.model.compositeScore - a.model.compositeScore ||
      a.behaviorFingerprint.localeCompare(b.behaviorFingerprint),
  );
}

export function buildScalpComposerSessionStructureComposerGrid(params: {
  venue: ScalpComposerVenue;
  symbol: string;
  entrySessionProfile: ScalpComposerSession;
  maxCandidates?: number;
  generatedAtMs?: number;
  adaptivePriors?: SessionStructureAdaptivePriorSet | null;
  noveltyBudget?: SessionStructureNoveltyBudget | null;
  /** Offspring genomes (by behaviour fingerprint) to prioritise this cycle. */
  offspring?: ReadonlyMap<string, SessionStructureOffspringSpec> | null;
  /** Max additive composite-score nudge applied to offspring cells. */
  evolutionScoreBoost?: number;
}): SessionStructureComposerCandidateDslSpec[] {
  const maxCandidates = Math.max(1, Math.min(2_000, Math.floor(Number(params.maxCandidates || 60))));
  const generatedAtMs = Math.floor(Number(params.generatedAtMs || Date.now()));
  const offspring = params.offspring || null;
  const evolutionScoreBoost = Math.max(0, Number(params.evolutionScoreBoost ?? 0));
  const byFingerprint = new Map<string, SessionStructureComposerCandidateDslSpec>();

  for (const contextId of SESSION_STRUCTURE_CONTEXT_BLOCKS) {
    for (const levelId of SESSION_STRUCTURE_LEVEL_BLOCKS) {
      for (const triggerId of SESSION_STRUCTURE_TRIGGER_BLOCKS) {
        for (const confirmationId of SESSION_STRUCTURE_CONFIRMATION_BLOCKS) {
          for (const managementId of SESSION_STRUCTURE_MANAGEMENT_BLOCKS) {
            const compat = validateSessionStructureCompatibility({
              contextId,
              levelId,
              triggerId,
              confirmationId,
              managementId,
            });
            if (!compat.compatible) continue;
            const fingerprint = sessionStructureBehaviorFingerprint({
              contextId,
              levelId,
              triggerId,
              confirmationId,
              managementId,
            });
            if (byFingerprint.has(fingerprint)) continue;
            const digest = crypto
              .createHash("sha1")
              .update(`${params.venue}:${params.symbol}:${params.entrySessionProfile}:${fingerprint}`)
              .digest("hex")
              .slice(0, 10);
            const tuneId = buildSessionStructureComposerTuneId({
              contextId,
              levelId,
              triggerId,
              confirmationId,
              managementId,
              digest,
            });
            const rawScore = scoreCombo({
              venue: params.venue,
              symbol: params.symbol,
              session: params.entrySessionProfile,
              contextId,
              levelId,
              triggerId,
              confirmationId,
              managementId,
              adaptivePriors: params.adaptivePriors || null,
            });
            const offspringSpec = offspring?.get(fingerprint) || null;
            const evolutionBoost = offspringSpec
              ? evolutionScoreBoost * Math.max(0, Math.min(1, offspringSpec.rankWeight))
              : 0;
            const compositeScore = Math.max(0, Math.min(1, rawScore + evolutionBoost));
            const model: ScalpComposerModelScore = {
              family: "interpretable_pattern_blend",
              version: "session_structure_composer_v1",
              interpretableScore: compositeScore,
              treeScore: 0,
              sequenceScore: 0,
              compositeScore,
              confidence: Math.max(0.35, Math.min(0.9, 0.45 + compositeScore * 0.45)),
            };
            const plan: SessionStructureComposerPlan = {
              contextId,
              levelId,
              triggerId,
              confirmationId,
              managementId,
              digest,
            };
            const adaptivePrior = resolveSessionStructureAdaptiveScore({
              priors: params.adaptivePriors || null,
              venue: params.venue,
              symbol: params.symbol,
              session: params.entrySessionProfile,
              contextId,
              levelId,
              triggerId,
              confirmationId,
              managementId,
            });
            byFingerprint.set(fingerprint, {
              candidateId: tuneId,
              tuneId,
              venue: params.venue,
              symbol: params.symbol,
              entrySessionProfile: params.entrySessionProfile,
              blocksByFamily: emptyLegacyBlockMap(),
              sessionBlocksByFamily: {
                context: [contextId],
                level: [levelId],
                trigger: [triggerId],
                confirmation: [confirmationId],
                management: [managementId],
              },
              referenceStrategyIds: [
                "opening_range_breakout_retest_m5_m1",
                "anchored_vwap_reversion_m15_m3",
                "trend_day_reacceleration_m15_m3",
                "pdh_pdl_reclaim_m15_m3",
              ],
              supportScore: compositeScore * 12,
              generatedAtMs,
              behaviorFingerprint: fingerprint,
              compatibilityReasonCodes: compat.reasonCodes,
              model,
              sessionComposerPlan: plan,
              adaptivePrior,
              evolution: offspringSpec
                ? {
                    op: offspringSpec.op,
                    parentTuneIds: offspringSpec.parentTuneIds,
                    parentFingerprints: offspringSpec.parentFingerprints,
                    bestParentFitness: offspringSpec.bestParentFitness,
                    boost: evolutionBoost,
                  }
                : null,
              regimeGateId: null,
            });
          }
        }
      }
    }
  }

  const sorted = Array.from(byFingerprint.values()).sort(
    (a, b) =>
      b.model.compositeScore - a.model.compositeScore ||
      a.behaviorFingerprint.localeCompare(b.behaviorFingerprint),
  );
  return selectDiverseSessionStructureCandidates(sorted, maxCandidates, params.noveltyBudget || null);
}
