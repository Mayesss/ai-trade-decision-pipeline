import crypto from "crypto";

import type {
  ScalpComposerModelScore,
  ScalpComposerPrimitiveBlockMap,
  ScalpComposerSession,
  ScalpComposerVenue,
} from "./types";

export const DAY_MODEL_GUIDED_COMPOSER_V1_STRATEGY_ID =
  "day_model_guided_composer_v1";

export const DAY_COMPOSER_TUNE_ID_PREFIX = "dtc";

export type DayComposerContextBlockId =
  | "h1_trend_d1_bias"
  | "h1_range_bound"
  | "atr_compression_expansion"
  | "inside_day_breakout"
  | "session_momentum"
  | "weekly_open_bias";

export type DayComposerLevelBlockId =
  | "prior_day_hl"
  | "asia_range_hl"
  | "previous_session_hl"
  | "weekly_open"
  | "session_vwap"
  | "opening_range_30m";

export type DayComposerTriggerBlockId =
  | "sweep_reclaim"
  | "breakout_retest_hold"
  | "failed_breakout_return"
  | "vwap_pullback_continuation";

export type DayComposerConfirmationBlockId =
  | "m15_close_acceptance"
  | "h1_close_acceptance"
  | "body_atr_expansion"
  | "volume_expansion_20";

export type DayComposerManagementBlockId =
  | "fixed_2r_time_6h"
  | "target_opposite_session_level"
  | "target_pdh_pdl"
  | "trail_after_1r_time_8h";

export interface DayComposerPlan {
  contextId: DayComposerContextBlockId;
  levelId: DayComposerLevelBlockId;
  triggerId: DayComposerTriggerBlockId;
  confirmationId: DayComposerConfirmationBlockId;
  managementId: DayComposerManagementBlockId;
  digest: string;
}

export interface DayComposerCandidateDslSpec {
  candidateId: string;
  tuneId: string;
  venue: ScalpComposerVenue;
  symbol: string;
  entrySessionProfile: ScalpComposerSession;
  blocksByFamily: ScalpComposerPrimitiveBlockMap;
  dayBlocksByFamily: {
    context: DayComposerContextBlockId[];
    level: DayComposerLevelBlockId[];
    trigger: DayComposerTriggerBlockId[];
    confirmation: DayComposerConfirmationBlockId[];
    management: DayComposerManagementBlockId[];
  };
  referenceStrategyIds: string[];
  supportScore: number;
  generatedAtMs: number;
  behaviorFingerprint: string;
  compatibilityReasonCodes: string[];
  model: ScalpComposerModelScore;
  dayComposerPlan: DayComposerPlan;
  regimeGateId?: string | null;
}

export const DAY_COMPOSER_CONTEXT_BLOCKS: readonly DayComposerContextBlockId[] =
  Object.freeze([
    "h1_trend_d1_bias",
    "h1_range_bound",
    "atr_compression_expansion",
    "inside_day_breakout",
    "session_momentum",
    "weekly_open_bias",
  ]);

export const DAY_COMPOSER_LEVEL_BLOCKS: readonly DayComposerLevelBlockId[] =
  Object.freeze([
    "prior_day_hl",
    "asia_range_hl",
    "previous_session_hl",
    "weekly_open",
    "session_vwap",
    "opening_range_30m",
  ]);

export const DAY_COMPOSER_TRIGGER_BLOCKS: readonly DayComposerTriggerBlockId[] =
  Object.freeze([
    "sweep_reclaim",
    "breakout_retest_hold",
    "failed_breakout_return",
    "vwap_pullback_continuation",
  ]);

export const DAY_COMPOSER_CONFIRMATION_BLOCKS: readonly DayComposerConfirmationBlockId[] =
  Object.freeze([
    "m15_close_acceptance",
    "h1_close_acceptance",
    "body_atr_expansion",
    "volume_expansion_20",
  ]);

export const DAY_COMPOSER_MANAGEMENT_BLOCKS: readonly DayComposerManagementBlockId[] =
  Object.freeze([
    "fixed_2r_time_6h",
    "target_opposite_session_level",
    "target_pdh_pdl",
    "trail_after_1r_time_8h",
  ]);

const CONTEXT_CODE: Record<DayComposerContextBlockId, string> = {
  h1_trend_d1_bias: "h1td1",
  h1_range_bound: "h1rng",
  atr_compression_expansion: "atrexp",
  inside_day_breakout: "idbo",
  session_momentum: "smomo",
  weekly_open_bias: "wob",
};

const LEVEL_CODE: Record<DayComposerLevelBlockId, string> = {
  prior_day_hl: "pdhpdl",
  asia_range_hl: "asiahl",
  previous_session_hl: "psesshl",
  weekly_open: "wopen",
  session_vwap: "svwap",
  opening_range_30m: "orb30",
};

const TRIGGER_CODE: Record<DayComposerTriggerBlockId, string> = {
  sweep_reclaim: "sweep",
  breakout_retest_hold: "brkret",
  failed_breakout_return: "fbr",
  vwap_pullback_continuation: "vwpb",
};

const CONFIRMATION_CODE: Record<DayComposerConfirmationBlockId, string> = {
  m15_close_acceptance: "m15acc",
  h1_close_acceptance: "h1acc",
  body_atr_expansion: "bodyatr",
  volume_expansion_20: "vol20",
};

const MANAGEMENT_CODE: Record<DayComposerManagementBlockId, string> = {
  fixed_2r_time_6h: "fix2r6h",
  target_opposite_session_level: "oppsess",
  target_pdh_pdl: "pdhpdl",
  trail_after_1r_time_8h: "trl1r8h",
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
  return crypto.createHash("sha1").update(raw || "day-composer").digest("hex").slice(0, 10);
}

export function isDayModelGuidedComposerStrategyId(value: unknown): boolean {
  return String(value || "").trim().toLowerCase() === DAY_MODEL_GUIDED_COMPOSER_V1_STRATEGY_ID;
}

export function buildDayComposerTuneId(params: {
  contextId: DayComposerContextBlockId;
  levelId: DayComposerLevelBlockId;
  triggerId: DayComposerTriggerBlockId;
  confirmationId: DayComposerConfirmationBlockId;
  managementId: DayComposerManagementBlockId;
  digest: string;
}): string {
  return [
    DAY_COMPOSER_TUNE_ID_PREFIX,
    CONTEXT_CODE[params.contextId],
    LEVEL_CODE[params.levelId],
    TRIGGER_CODE[params.triggerId],
    CONFIRMATION_CODE[params.confirmationId],
    MANAGEMENT_CODE[params.managementId],
    normalizeDigest(params.digest),
  ].join("_");
}

export function parseDayComposerTuneId(value: unknown): DayComposerPlan {
  const raw = String(value || "").trim().toLowerCase();
  const parts = raw.split("_");
  if (parts.length >= 7 && parts[0] === DAY_COMPOSER_TUNE_ID_PREFIX) {
    return {
      contextId: CONTEXT_BY_CODE[parts[1]!] || "h1_trend_d1_bias",
      levelId: LEVEL_BY_CODE[parts[2]!] || "prior_day_hl",
      triggerId: TRIGGER_BY_CODE[parts[3]!] || "sweep_reclaim",
      confirmationId: CONFIRMATION_BY_CODE[parts[4]!] || "m15_close_acceptance",
      managementId: MANAGEMENT_BY_CODE[parts[5]!] || "fixed_2r_time_6h",
      digest: normalizeDigest(parts[6]),
    };
  }
  return {
    contextId: "h1_trend_d1_bias",
    levelId: "prior_day_hl",
    triggerId: "sweep_reclaim",
    confirmationId: "m15_close_acceptance",
    managementId: "fixed_2r_time_6h",
    digest: normalizeDigest(raw || "default"),
  };
}

export function dayComposerBehaviorFingerprint(plan: Omit<DayComposerPlan, "digest">): string {
  return [
    plan.contextId,
    plan.levelId,
    plan.triggerId,
    plan.confirmationId,
    plan.managementId,
  ].join("|");
}

export function isDayComposerHighLowLevel(levelId: DayComposerLevelBlockId): boolean {
  return (
    levelId === "prior_day_hl" ||
    levelId === "asia_range_hl" ||
    levelId === "previous_session_hl" ||
    levelId === "opening_range_30m"
  );
}

export function validateDayComposerCompatibility(params: {
  contextId: DayComposerContextBlockId;
  levelId: DayComposerLevelBlockId;
  triggerId: DayComposerTriggerBlockId;
  confirmationId: DayComposerConfirmationBlockId;
  managementId: DayComposerManagementBlockId;
}): { compatible: boolean; reasonCodes: string[] } {
  const reasonCodes: string[] = [];
  if (params.triggerId === "vwap_pullback_continuation" && params.levelId !== "session_vwap") {
    reasonCodes.push("DAY_COMPOSER_INCOMPAT_VWAP_TRIGGER_REQUIRES_SESSION_VWAP");
  }
  if (
    (params.triggerId === "failed_breakout_return" || params.triggerId === "sweep_reclaim") &&
    !isDayComposerHighLowLevel(params.levelId)
  ) {
    reasonCodes.push("DAY_COMPOSER_INCOMPAT_TRIGGER_REQUIRES_HIGH_LOW_LEVEL");
  }
  if (
    params.confirmationId === "h1_close_acceptance" &&
    params.managementId === "fixed_2r_time_6h"
  ) {
    reasonCodes.push("DAY_COMPOSER_INCOMPAT_H1_CONFIRMATION_REQUIRES_LONGER_MANAGEMENT");
  }
  return { compatible: reasonCodes.length === 0, reasonCodes };
}

function deterministicScore(seed: string): number {
  const digest = crypto.createHash("sha1").update(seed).digest();
  return digest.readUInt32BE(0) / 0xffffffff;
}

function scoreCombo(params: {
  venue: ScalpComposerVenue;
  symbol: string;
  session: ScalpComposerSession;
  contextId: DayComposerContextBlockId;
  levelId: DayComposerLevelBlockId;
  triggerId: DayComposerTriggerBlockId;
  confirmationId: DayComposerConfirmationBlockId;
  managementId: DayComposerManagementBlockId;
}): number {
  const contextWeight: Record<DayComposerContextBlockId, number> = {
    h1_trend_d1_bias: 0.2,
    h1_range_bound: 0.14,
    atr_compression_expansion: 0.18,
    inside_day_breakout: 0.16,
    session_momentum: 0.17,
    weekly_open_bias: 0.15,
  };
  const levelWeight: Record<DayComposerLevelBlockId, number> = {
    prior_day_hl: 0.2,
    asia_range_hl: 0.18,
    previous_session_hl: 0.17,
    weekly_open: 0.13,
    session_vwap: 0.18,
    opening_range_30m: 0.16,
  };
  const triggerWeight: Record<DayComposerTriggerBlockId, number> = {
    sweep_reclaim: 0.2,
    breakout_retest_hold: 0.18,
    failed_breakout_return: 0.16,
    vwap_pullback_continuation: 0.18,
  };
  const confirmationWeight: Record<DayComposerConfirmationBlockId, number> = {
    m15_close_acceptance: 0.16,
    h1_close_acceptance: 0.19,
    body_atr_expansion: 0.17,
    volume_expansion_20: 0.15,
  };
  const managementWeight: Record<DayComposerManagementBlockId, number> = {
    fixed_2r_time_6h: 0.15,
    target_opposite_session_level: 0.18,
    target_pdh_pdl: 0.18,
    trail_after_1r_time_8h: 0.19,
  };
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
  return (
    contextWeight[params.contextId] +
    levelWeight[params.levelId] +
    triggerWeight[params.triggerId] +
    confirmationWeight[params.confirmationId] +
    managementWeight[params.managementId] +
    deterministicScore(seed) * 0.02
  );
}

export function buildScalpComposerDayModelComposerGrid(params: {
  venue: ScalpComposerVenue;
  symbol: string;
  entrySessionProfile: ScalpComposerSession;
  maxCandidates?: number;
  generatedAtMs?: number;
}): DayComposerCandidateDslSpec[] {
  const maxCandidates = Math.max(1, Math.min(2_000, Math.floor(Number(params.maxCandidates || 120))));
  const generatedAtMs = Math.floor(Number(params.generatedAtMs || Date.now()));
  const byFingerprint = new Map<string, DayComposerCandidateDslSpec>();

  for (const contextId of DAY_COMPOSER_CONTEXT_BLOCKS) {
    for (const levelId of DAY_COMPOSER_LEVEL_BLOCKS) {
      for (const triggerId of DAY_COMPOSER_TRIGGER_BLOCKS) {
        for (const confirmationId of DAY_COMPOSER_CONFIRMATION_BLOCKS) {
          for (const managementId of DAY_COMPOSER_MANAGEMENT_BLOCKS) {
            const compat = validateDayComposerCompatibility({
              contextId,
              levelId,
              triggerId,
              confirmationId,
              managementId,
            });
            if (!compat.compatible) continue;
            const fingerprint = dayComposerBehaviorFingerprint({
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
            const tuneId = buildDayComposerTuneId({
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
            });
            const compositeScore = Math.max(0, Math.min(1, rawScore));
            const model: ScalpComposerModelScore = {
              family: "interpretable_pattern_blend",
              version: "day_model_guided_composer_v1",
              interpretableScore: compositeScore,
              treeScore: 0,
              sequenceScore: 0,
              compositeScore,
              confidence: Math.max(0.35, Math.min(0.9, 0.45 + compositeScore * 0.45)),
            };
            const plan: DayComposerPlan = {
              contextId,
              levelId,
              triggerId,
              confirmationId,
              managementId,
              digest,
            };
            byFingerprint.set(fingerprint, {
              candidateId: tuneId,
              tuneId,
              venue: params.venue,
              symbol: params.symbol,
              entrySessionProfile: params.entrySessionProfile,
              blocksByFamily: emptyLegacyBlockMap(),
              dayBlocksByFamily: {
                context: [contextId],
                level: [levelId],
                trigger: [triggerId],
                confirmation: [confirmationId],
                management: [managementId],
              },
              referenceStrategyIds: [
                "pdh_pdl_reclaim_m15_m3",
                "opening_range_breakout_retest_m5_m1",
                "anchored_vwap_reversion_m15_m3",
                "trend_day_reacceleration_m15_m3",
              ],
              supportScore: compositeScore * 12,
              generatedAtMs,
              behaviorFingerprint: fingerprint,
              compatibilityReasonCodes: compat.reasonCodes,
              model,
              dayComposerPlan: plan,
              regimeGateId: null,
            });
          }
        }
      }
    }
  }

  return Array.from(byFingerprint.values())
    .sort(
      (a, b) =>
        b.model.compositeScore - a.model.compositeScore ||
        a.behaviorFingerprint.localeCompare(b.behaviorFingerprint),
    )
    .slice(0, maxCandidates);
}
