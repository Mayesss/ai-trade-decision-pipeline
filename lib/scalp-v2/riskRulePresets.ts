/**
 * Risk rule presets — maps DSL risk_rule block IDs to overrides
 * applied both at research backtest time and at promotion/execution.
 *
 * Research overrides affect ScalpReplayRuntimeConfig.strategy fields.
 * Deployment overrides affect ScalpV2RiskProfile fields.
 */

import type { ScalpV2RiskProfile } from "./types";

export type RiskRuleBlockId =
  | "risk_pct_equity_sizing"
  | "risk_min_max_notional"
  | "risk_spread_aware_stop_buffer"
  | "risk_atr_stop_distance"
  | "risk_single_position_per_symbol"
  | "risk_leverage_cap"
  | "risk_adaptive_lock_period";

export type RiskRuleOverrides = Partial<ScalpV2RiskProfile>;

/**
 * Backtest-compatible overrides — field names match
 * ScalpReplayRuntimeConfig.strategy (flat keys).
 */
export interface RiskRuleReplayOverrides {
  riskPerTradePct?: number;
  maxTradesPerDay?: number;
  dailyLossLimitR?: number;
}

const RISK_RULE_PRESETS: Record<RiskRuleBlockId, {
  replay: RiskRuleReplayOverrides;
  deployment: RiskRuleOverrides;
}> = Object.freeze({
  // Conservative sizing — lower risk per trade.
  risk_pct_equity_sizing: {
    replay: { riskPerTradePct: 0.3 },
    deployment: { riskPerTradePct: 0.3 },
  },

  // Notional guardrails — tighter position limits.
  risk_min_max_notional: {
    replay: { riskPerTradePct: 0.5 },
    deployment: { riskPerTradePct: 0.5, maxOpenPositionsPerSymbol: 1 },
  },

  // Spread-aware buffer — tighter daily loss limit.
  risk_spread_aware_stop_buffer: {
    replay: { dailyLossLimitR: -1.5 },
    deployment: { autoPauseDailyR: -1.5 },
  },

  // ATR-based stop distance — wider stop tolerance, lower position risk.
  risk_atr_stop_distance: {
    replay: { riskPerTradePct: 0.4, dailyLossLimitR: -2.0 },
    deployment: { riskPerTradePct: 0.4, autoPauseDailyR: -2.0 },
  },

  // Single position per symbol — strict position limit, 1 trade/day.
  risk_single_position_per_symbol: {
    replay: { maxTradesPerDay: 1 },
    deployment: { maxOpenPositionsPerSymbol: 1 },
  },

  // Leverage cap — conservative risk budget across the board.
  risk_leverage_cap: {
    replay: { riskPerTradePct: 0.25, maxTradesPerDay: 1, dailyLossLimitR: -1.0 },
    deployment: { riskPerTradePct: 0.25, maxOpenPositionsPerSymbol: 1, autoPauseDailyR: -1.0 },
  },

  // Adaptive lock period — tighter drawdown limits.
  risk_adaptive_lock_period: {
    replay: { dailyLossLimitR: -1.0 },
    deployment: { autoPauseDailyR: -1.0, autoPause30dR: -3.0 },
  },
});

/**
 * Curated risk profiles for research grid expansion.
 * Just 2 profiles to keep the multiplier manageable:
 * - Default behavior (no overrides, handled implicitly)
 * - Conservative sizing (lower risk, fewer trades)
 */
export const RISK_RULE_RESEARCH_PROFILES: readonly (RiskRuleBlockId | null)[] =
  Object.freeze([
    null,                          // default — no risk override
    "risk_leverage_cap" as const,  // conservative — backtested with tight limits
  ]);

/** Short codes for tuneId encoding. */
export const RISK_RULE_SHORT_CODES: Record<string, string> = Object.freeze({
  risk_pct_equity_sizing: "rpeq",
  risk_min_max_notional: "rmmn",
  risk_spread_aware_stop_buffer: "rsab",
  risk_atr_stop_distance: "ratr",
  risk_single_position_per_symbol: "rsps",
  risk_leverage_cap: "rlev",
  risk_adaptive_lock_period: "ralp",
});

const SHORT_CODE_TO_BLOCK: Record<string, RiskRuleBlockId> = Object.freeze(
  Object.fromEntries(
    Object.entries(RISK_RULE_SHORT_CODES).map(([block, code]) => [
      code,
      block as RiskRuleBlockId,
    ]),
  ) as Record<string, RiskRuleBlockId>,
);

export function resolveRiskRuleBlockFromShortCode(
  code: string | null | undefined,
): RiskRuleBlockId | null {
  if (!code) return null;
  return SHORT_CODE_TO_BLOCK[code.toLowerCase()] || null;
}

/**
 * Resolve replay-compatible risk overrides from DSL risk_rule blocks.
 * Used during research backtesting.
 */
export function resolveRiskRuleReplayOverrides(
  riskBlocks: string[] | null | undefined,
): RiskRuleReplayOverrides {
  if (!Array.isArray(riskBlocks)) return {};
  for (const block of riskBlocks) {
    const preset = RISK_RULE_PRESETS[block as RiskRuleBlockId];
    if (preset) return { ...preset.replay };
  }
  return {};
}

/**
 * Resolve deployment-level risk overrides from DSL risk_rule blocks.
 * Used at promotion/execution time.
 */
export function resolveRiskRuleOverrides(
  riskBlocks: string[] | null | undefined,
): RiskRuleOverrides {
  if (!Array.isArray(riskBlocks)) return {};
  for (const block of riskBlocks) {
    const preset = RISK_RULE_PRESETS[block as RiskRuleBlockId];
    if (preset) return { ...preset.deployment };
  }
  return {};
}

/**
 * Merge risk rule overrides into an existing risk profile.
 */
export function mergeRiskProfileWithOverrides(
  base: ScalpV2RiskProfile,
  overrides: RiskRuleOverrides,
): ScalpV2RiskProfile {
  return {
    riskPerTradePct: overrides.riskPerTradePct ?? base.riskPerTradePct,
    maxOpenPositionsPerSymbol: overrides.maxOpenPositionsPerSymbol ?? base.maxOpenPositionsPerSymbol,
    autoPauseDailyR: overrides.autoPauseDailyR ?? base.autoPauseDailyR,
    autoPause30dR: overrides.autoPause30dR ?? base.autoPause30dR,
  };
}
