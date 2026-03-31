/**
 * Exit rule presets — maps DSL exit_rule block IDs to concrete
 * ScalpReplayRuntimeConfig.strategy overrides.
 *
 * Each preset defines a complete exit behavior profile.  When applied,
 * these values override the defaults from defaultScalpReplayConfig().
 */

export type ExitRuleBlockId =
  | "exit_fixed_r_take_profit"
  | "exit_tp1_then_trail"
  | "exit_trailing_atr"
  | "exit_time_stop"
  | "exit_break_even_shift"
  | "exit_invalidation_stop"
  | "exit_session_cutoff";

export interface ExitRuleOverrides {
  takeProfitR?: number;
  tp1R?: number;
  tp1ClosePct?: number;
  breakEvenOffsetR?: number;
  trailStartR?: number;
  trailAtrMult?: number;
  timeStopBars?: number;
}

// Each preset fully defines the exit behavior. Values set to high numbers
// (999) effectively disable that feature without special-case code.
const EXIT_RULE_PRESETS: Record<ExitRuleBlockId, ExitRuleOverrides> =
  Object.freeze({
    // Simple fixed R:R take profit — no partials, no trail.
    // Time stop at 60 bars prevents runaway holds on missed TPs.
    exit_fixed_r_take_profit: {
      takeProfitR: 1.0,
      tp1R: 999,
      tp1ClosePct: 0,
      breakEvenOffsetR: 0,
      trailStartR: 999,
      trailAtrMult: 0,
      timeStopBars: 60,
    },

    // Partial at TP1, trail the rest (mirrors current defaults).
    exit_tp1_then_trail: {
      takeProfitR: 2.0,
      tp1R: 1.0,
      tp1ClosePct: 50,
      breakEvenOffsetR: 0.1,
      trailStartR: 1.0,
      trailAtrMult: 2.0,
      timeStopBars: 60,
    },

    // Pure ATR trail — no partial close, trail from early R.
    // Time stop at 120 bars as a safety net; trail should exit first.
    exit_trailing_atr: {
      takeProfitR: 3.0,
      tp1R: 999,
      tp1ClosePct: 0,
      breakEvenOffsetR: 0,
      trailStartR: 0.3,
      trailAtrMult: 1.5,
      timeStopBars: 120,
    },

    // Aggressive time-limited exit — quick in, quick out.
    exit_time_stop: {
      takeProfitR: 0.8,
      tp1R: 999,
      tp1ClosePct: 0,
      breakEvenOffsetR: 0,
      trailStartR: 999,
      trailAtrMult: 0,
      timeStopBars: 20,
    },

    // Break-even shift after small profit — protect capital early.
    exit_break_even_shift: {
      takeProfitR: 1.5,
      tp1R: 0.4,
      tp1ClosePct: 0,
      breakEvenOffsetR: 0.15,
      trailStartR: 999,
      trailAtrMult: 0,
      timeStopBars: 80,
    },

    // Invalidation stop — rely on strategy's own invalidation logic.
    // Minimal exit intervention: wide TP, time stop at 90 bars as safety net.
    exit_invalidation_stop: {
      takeProfitR: 2.5,
      tp1R: 999,
      tp1ClosePct: 0,
      breakEvenOffsetR: 0,
      trailStartR: 999,
      trailAtrMult: 0,
      timeStopBars: 90,
    },

    // Session cutoff — moderate time stop aligned with session end.
    exit_session_cutoff: {
      takeProfitR: 1.2,
      tp1R: 999,
      tp1ClosePct: 0,
      breakEvenOffsetR: 0,
      trailStartR: 999,
      trailAtrMult: 0,
      timeStopBars: 40,
    },
  });

/**
 * Curated exit profiles used for research grid expansion.
 * Kept to 4 to avoid combinatorial explosion while covering
 * the most impactful exit behavior differences.
 */
export const EXIT_RULE_RESEARCH_PROFILES: readonly ExitRuleBlockId[] =
  Object.freeze([
    "exit_fixed_r_take_profit",
    "exit_tp1_then_trail",
    "exit_trailing_atr",
    "exit_time_stop",
  ]);

/** Short codes for tuneId encoding. */
export const EXIT_RULE_SHORT_CODES: Record<string, string> = Object.freeze({
  exit_fixed_r_take_profit: "xftp",
  exit_tp1_then_trail: "xtt",
  exit_trailing_atr: "xatr",
  exit_time_stop: "xts",
  exit_break_even_shift: "xbe",
  exit_invalidation_stop: "xinv",
  exit_session_cutoff: "xsc",
});

const SHORT_CODE_TO_BLOCK: Record<string, ExitRuleBlockId> = Object.freeze(
  Object.fromEntries(
    Object.entries(EXIT_RULE_SHORT_CODES).map(([block, code]) => [
      code,
      block as ExitRuleBlockId,
    ]),
  ) as Record<string, ExitRuleBlockId>,
);

/**
 * Resolve an exit short code back to the full block ID.
 * Returns null for unknown codes.
 */
export function resolveExitRuleBlockFromShortCode(
  code: string | null | undefined,
): ExitRuleBlockId | null {
  if (!code) return null;
  return SHORT_CODE_TO_BLOCK[code.toLowerCase()] || null;
}

/**
 * Resolve exit rule overrides from DSL exit_rule blocks.
 * Uses the first recognized block as the primary profile.
 * Returns empty object if no recognized blocks → defaults apply.
 */
export function resolveExitRuleOverrides(
  exitBlocks: string[] | null | undefined,
): ExitRuleOverrides {
  if (!Array.isArray(exitBlocks)) return {};
  for (const block of exitBlocks) {
    const preset = EXIT_RULE_PRESETS[block as ExitRuleBlockId];
    if (preset) return { ...preset };
  }
  return {};
}
