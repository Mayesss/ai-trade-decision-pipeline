import crypto from "crypto";

import type {
  ScalpV2ComposerModelScore,
  ScalpV2PrimitiveBlockMap,
  ScalpV2Session,
  ScalpV2Venue,
} from "./types";

export const SESSION_STRUCTURE_COMPOSER_V1_STRATEGY_ID =
  "session_structure_composer_v1";

export const SESSION_STRUCTURE_COMPOSER_TUNE_ID_PREFIX = "ssc";

export type SessionStructureContextBlockId =
  | "m30_session_momentum"
  | "h1_directional_bias"
  | "opening_drive"
  | "atr_expansion"
  | "vwap_balance_shift";

export type SessionStructureLevelBlockId =
  | "session_vwap"
  | "opening_range_30m"
  | "previous_session_hl"
  | "asia_range_hl"
  | "prior_day_hl"
  | "intraday_swing_hl";

export type SessionStructureTriggerBlockId =
  | "breakout_retest_hold"
  | "vwap_pullback_continuation"
  | "sweep_reclaim"
  | "failed_breakout_return";

export type SessionStructureConfirmationBlockId =
  | "m15_close_acceptance"
  | "m30_close_acceptance"
  | "body_atr_expansion"
  | "volume_expansion_20";

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

export interface SessionStructureComposerCandidateDslSpec {
  candidateId: string;
  tuneId: string;
  venue: ScalpV2Venue;
  symbol: string;
  entrySessionProfile: ScalpV2Session;
  blocksByFamily: ScalpV2PrimitiveBlockMap;
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
  model: ScalpV2ComposerModelScore;
  sessionComposerPlan: SessionStructureComposerPlan;
  regimeGateId?: string | null;
}

export const SESSION_STRUCTURE_CONTEXT_BLOCKS: readonly SessionStructureContextBlockId[] =
  Object.freeze([
    "m30_session_momentum",
    "h1_directional_bias",
    "opening_drive",
    "atr_expansion",
    "vwap_balance_shift",
  ]);

export const SESSION_STRUCTURE_LEVEL_BLOCKS: readonly SessionStructureLevelBlockId[] =
  Object.freeze([
    "session_vwap",
    "opening_range_30m",
    "previous_session_hl",
    "asia_range_hl",
    "prior_day_hl",
    "intraday_swing_hl",
  ]);

export const SESSION_STRUCTURE_TRIGGER_BLOCKS: readonly SessionStructureTriggerBlockId[] =
  Object.freeze([
    "breakout_retest_hold",
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
};

const LEVEL_CODE: Record<SessionStructureLevelBlockId, string> = {
  session_vwap: "svwap",
  opening_range_30m: "orb30",
  previous_session_hl: "psesshl",
  asia_range_hl: "asiahl",
  prior_day_hl: "pdhpdl",
  intraday_swing_hl: "swinghl",
};

const TRIGGER_CODE: Record<SessionStructureTriggerBlockId, string> = {
  breakout_retest_hold: "brkret",
  vwap_pullback_continuation: "vwpb",
  sweep_reclaim: "sweep",
  failed_breakout_return: "fbr",
};

const CONFIRMATION_CODE: Record<SessionStructureConfirmationBlockId, string> = {
  m15_close_acceptance: "m15acc",
  m30_close_acceptance: "m30acc",
  body_atr_expansion: "bodyatr",
  volume_expansion_20: "vol20",
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

function emptyLegacyBlockMap(): ScalpV2PrimitiveBlockMap {
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

export function isSessionStructureHighLowLevel(levelId: SessionStructureLevelBlockId): boolean {
  return (
    levelId === "opening_range_30m" ||
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
    params.triggerId === "breakout_retest_hold" &&
    !["opening_range_30m", "previous_session_hl", "intraday_swing_hl"].includes(params.levelId)
  ) {
    reasonCodes.push("SESSION_COMPOSER_INCOMPAT_BREAKOUT_RETEST_PREFERS_INTRADAY_HL");
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

function scoreCombo(params: {
  venue: ScalpV2Venue;
  symbol: string;
  session: ScalpV2Session;
  contextId: SessionStructureContextBlockId;
  levelId: SessionStructureLevelBlockId;
  triggerId: SessionStructureTriggerBlockId;
  confirmationId: SessionStructureConfirmationBlockId;
  managementId: SessionStructureManagementBlockId;
}): number {
  const contextWeight: Record<SessionStructureContextBlockId, number> = {
    m30_session_momentum: 0.2,
    h1_directional_bias: 0.17,
    opening_drive: 0.18,
    atr_expansion: 0.16,
    vwap_balance_shift: 0.19,
  };
  const levelWeight: Record<SessionStructureLevelBlockId, number> = {
    session_vwap: 0.22,
    opening_range_30m: 0.2,
    previous_session_hl: 0.19,
    asia_range_hl: 0.15,
    prior_day_hl: 0.13,
    intraday_swing_hl: 0.18,
  };
  const triggerWeight: Record<SessionStructureTriggerBlockId, number> = {
    breakout_retest_hold: 0.22,
    vwap_pullback_continuation: 0.21,
    sweep_reclaim: 0.13,
    failed_breakout_return: 0.08,
  };
  const confirmationWeight: Record<SessionStructureConfirmationBlockId, number> = {
    m15_close_acceptance: 0.19,
    m30_close_acceptance: 0.17,
    body_atr_expansion: 0.16,
    volume_expansion_20: 0.14,
  };
  const managementWeight: Record<SessionStructureManagementBlockId, number> = {
    fixed_1_5r_time_2h: 0.18,
    fixed_2r_time_4h: 0.19,
    target_next_session_level: 0.15,
    trail_after_0_8r_time_3h: 0.2,
  };
  const preferredPairBonus =
    (params.levelId === "session_vwap" && params.triggerId === "vwap_pullback_continuation") ||
    ((params.levelId === "opening_range_30m" || params.levelId === "previous_session_hl") &&
      params.triggerId === "breakout_retest_hold")
      ? 0.08
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
  return (
    contextWeight[params.contextId] +
    levelWeight[params.levelId] +
    triggerWeight[params.triggerId] +
    confirmationWeight[params.confirmationId] +
    managementWeight[params.managementId] +
    preferredPairBonus +
    deterministicScore(seed) * 0.02
  );
}

export function buildScalpV2SessionStructureComposerGrid(params: {
  venue: ScalpV2Venue;
  symbol: string;
  entrySessionProfile: ScalpV2Session;
  maxCandidates?: number;
  generatedAtMs?: number;
}): SessionStructureComposerCandidateDslSpec[] {
  const maxCandidates = Math.max(1, Math.min(2_000, Math.floor(Number(params.maxCandidates || 60))));
  const generatedAtMs = Math.floor(Number(params.generatedAtMs || Date.now()));
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
            });
            const compositeScore = Math.max(0, Math.min(1, rawScore));
            const model: ScalpV2ComposerModelScore = {
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
