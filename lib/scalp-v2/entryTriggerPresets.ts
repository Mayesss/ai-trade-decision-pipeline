/**
 * Entry trigger presets — maps DSL entry_trigger block IDs to concrete
 * ScalpReplayRuntimeConfig.strategy overrides for sweep/confirm/ifvg params.
 *
 * Each preset tunes the entry detection sensitivity to produce genuinely
 * different trade entries from the same underlying strategy logic.
 */

export type EntryTriggerBlockId =
  | "entry_sweep_reclaim"
  | "entry_mss_break_trigger"
  | "entry_ifvg_touch"
  | "entry_displacement_confirm"
  | "entry_opening_range_retest"
  | "entry_vwap_snapback"
  | "entry_extreme_reversal"
  | "entry_relative_value_mean_revert"
  | "entry_seasonality_window_bias"
  | "entry_adaptive_arm_selection";

export interface EntryTriggerOverrides {
  // Sweep detection
  sweepBufferPips?: number;
  sweepBufferAtrMult?: number;
  sweepRejectMaxBars?: number;
  sweepMinWickBodyRatio?: number;
  // Confirmation
  displacementBodyAtrMult?: number;
  displacementRangeAtrMult?: number;
  mssLookbackBars?: number;
  confirmTtlMinutes?: number;
  allowPullbackSwingBreakTrigger?: boolean;
  // IFVG entry
  ifvgMinAtrMult?: number;
  ifvgMaxAtrMult?: number;
  ifvgTtlMinutes?: number;
  ifvgEntryMode?: "first_touch" | "midline_touch" | "full_fill";
}

import type { ModelGuidedComposerBaseArm } from "./composerExecution";

const ENTRY_TRIGGER_PRESETS: Record<EntryTriggerBlockId, EntryTriggerOverrides> =
  Object.freeze({
    // Tight sweep detection, strict wick ratio — fewer but higher-quality entries.
    entry_sweep_reclaim: {
      sweepBufferPips: 0.08,
      sweepRejectMaxBars: 12,
      sweepMinWickBodyRatio: 0.6,
      displacementBodyAtrMult: 0.06,
      mssLookbackBars: 1,
      confirmTtlMinutes: 30,
      ifvgEntryMode: "first_touch",
    },

    // Emphasize MSS break — wider lookback, loose sweep, strict displacement.
    entry_mss_break_trigger: {
      sweepBufferPips: 0.15,
      sweepRejectMaxBars: 25,
      sweepMinWickBodyRatio: 0.4,
      displacementBodyAtrMult: 0.04,
      displacementRangeAtrMult: 0.10,
      mssLookbackBars: 2,
      confirmTtlMinutes: 45,
      allowPullbackSwingBreakTrigger: true,
      ifvgEntryMode: "first_touch",
    },

    // IFVG-focused — midline entry for better fill, wider FVG acceptance.
    entry_ifvg_touch: {
      sweepBufferPips: 0.12,
      sweepRejectMaxBars: 20,
      displacementBodyAtrMult: 0.06,
      mssLookbackBars: 1,
      confirmTtlMinutes: 60,
      ifvgMinAtrMult: 0,
      ifvgMaxAtrMult: 4,
      ifvgTtlMinutes: 120,
      ifvgEntryMode: "midline_touch",
    },

    // High displacement thresholds — catches strong momentum moves only.
    entry_displacement_confirm: {
      sweepBufferPips: 0.20,
      sweepRejectMaxBars: 30,
      sweepMinWickBodyRatio: 0.3,
      displacementBodyAtrMult: 0.10,
      displacementRangeAtrMult: 0.20,
      mssLookbackBars: 1,
      confirmTtlMinutes: 25,
      ifvgEntryMode: "first_touch",
    },

    // Opening range — fast confirmation, tight FVG window.
    entry_opening_range_retest: {
      sweepBufferPips: 0.10,
      sweepRejectMaxBars: 15,
      displacementBodyAtrMult: 0.05,
      mssLookbackBars: 1,
      confirmTtlMinutes: 20,
      ifvgMinAtrMult: 0,
      ifvgMaxAtrMult: 3,
      ifvgTtlMinutes: 60,
      ifvgEntryMode: "first_touch",
    },

    // VWAP snap-back — loose sweep, emphasis on mean reversion fill.
    entry_vwap_snapback: {
      sweepBufferPips: 0.25,
      sweepRejectMaxBars: 30,
      sweepMinWickBodyRatio: 0.3,
      displacementBodyAtrMult: 0.04,
      mssLookbackBars: 1,
      confirmTtlMinutes: 50,
      ifvgMinAtrMult: 0,
      ifvgMaxAtrMult: 5,
      ifvgTtlMinutes: 90,
      ifvgEntryMode: "midline_touch",
    },

    // Extreme reversal — very strict displacement, fast TTL.
    entry_extreme_reversal: {
      sweepBufferPips: 0.05,
      sweepRejectMaxBars: 8,
      sweepMinWickBodyRatio: 0.8,
      displacementBodyAtrMult: 0.12,
      displacementRangeAtrMult: 0.25,
      mssLookbackBars: 1,
      confirmTtlMinutes: 15,
      ifvgEntryMode: "first_touch",
    },

    // Relative value mean reversion — wide acceptance, patient fill.
    entry_relative_value_mean_revert: {
      sweepBufferPips: 0.20,
      sweepRejectMaxBars: 30,
      sweepMinWickBodyRatio: 0.3,
      displacementBodyAtrMult: 0.04,
      mssLookbackBars: 1,
      confirmTtlMinutes: 60,
      ifvgMinAtrMult: 0,
      ifvgMaxAtrMult: 5,
      ifvgTtlMinutes: 120,
      ifvgEntryMode: "midline_touch",
    },

    // Seasonality window — moderate sensitivity, session-aware TTL.
    entry_seasonality_window_bias: {
      sweepBufferPips: 0.15,
      sweepRejectMaxBars: 20,
      displacementBodyAtrMult: 0.06,
      mssLookbackBars: 1,
      confirmTtlMinutes: 40,
      ifvgEntryMode: "first_touch",
    },

    // Adaptive — balanced defaults, lets the delegate strategy decide.
    entry_adaptive_arm_selection: {
      sweepBufferPips: 0.12,
      sweepRejectMaxBars: 20,
      sweepMinWickBodyRatio: 0.5,
      displacementBodyAtrMult: 0.06,
      mssLookbackBars: 1,
      confirmTtlMinutes: 45,
      ifvgEntryMode: "first_touch",
    },
  });

/**
 * Compatibility matrix: which entry triggers are valid per base arm.
 * Prevents nonsensical combos (e.g. VWAP snapback on regime pullback).
 */
export const ENTRY_TRIGGER_COMPAT: Record<ModelGuidedComposerBaseArm, readonly EntryTriggerBlockId[]> =
  Object.freeze({
    regime: ["entry_sweep_reclaim", "entry_mss_break_trigger", "entry_ifvg_touch", "entry_displacement_confirm"],
    compress: ["entry_sweep_reclaim", "entry_ifvg_touch", "entry_displacement_confirm"],
    orb: ["entry_opening_range_retest", "entry_displacement_confirm"],
    auction: ["entry_extreme_reversal", "entry_displacement_confirm"],
    trend: ["entry_mss_break_trigger", "entry_displacement_confirm"],
    vwap: ["entry_vwap_snapback", "entry_displacement_confirm"],
    basis: ["entry_relative_value_mean_revert", "entry_displacement_confirm"],
    relative: ["entry_relative_value_mean_revert", "entry_displacement_confirm"],
    season: ["entry_seasonality_window_bias", "entry_displacement_confirm"],
    reclaim: ["entry_sweep_reclaim", "entry_mss_break_trigger"],
    hss: ["entry_mss_break_trigger", "entry_ifvg_touch", "entry_displacement_confirm"],
    adaptive: ["entry_adaptive_arm_selection"],
  });

/** Short codes for tuneId encoding. */
export const ENTRY_TRIGGER_SHORT_CODES: Record<string, string> = Object.freeze({
  entry_sweep_reclaim: "esr",
  entry_mss_break_trigger: "emss",
  entry_ifvg_touch: "eifvg",
  entry_displacement_confirm: "edisp",
  entry_opening_range_retest: "eor",
  entry_vwap_snapback: "evwap",
  entry_extreme_reversal: "erev",
  entry_relative_value_mean_revert: "erv",
  entry_seasonality_window_bias: "eswb",
  entry_adaptive_arm_selection: "eadp",
});

const SHORT_CODE_TO_BLOCK: Record<string, EntryTriggerBlockId> = Object.freeze(
  Object.fromEntries(
    Object.entries(ENTRY_TRIGGER_SHORT_CODES).map(([block, code]) => [
      code,
      block as EntryTriggerBlockId,
    ]),
  ) as Record<string, EntryTriggerBlockId>,
);

/**
 * Resolve an entry short code back to the full block ID.
 */
export function resolveEntryTriggerBlockFromShortCode(
  code: string | null | undefined,
): EntryTriggerBlockId | null {
  if (!code) return null;
  return SHORT_CODE_TO_BLOCK[code.toLowerCase()] || null;
}

/**
 * Resolve entry trigger overrides from DSL entry_trigger blocks.
 * Uses the first recognized block as the primary profile.
 */
export function resolveEntryTriggerOverrides(
  entryBlocks: string[] | null | undefined,
): EntryTriggerOverrides {
  if (!Array.isArray(entryBlocks)) return {};
  for (const block of entryBlocks) {
    const preset = ENTRY_TRIGGER_PRESETS[block as EntryTriggerBlockId];
    if (preset) return { ...preset };
  }
  return {};
}
