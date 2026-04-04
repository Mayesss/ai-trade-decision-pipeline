import crypto from "crypto";

import {
  buildModelGuidedComposerTuneId,
  COMPOSER_TIMEFRAME_VARIANTS,
  resolveBaseArmFromPatternBlock,
  resolveModelGuidedComposerExecutionPlanFromBlocks,
  type ModelGuidedComposerArmId,
  type ModelGuidedComposerBaseArm,
} from "./composerExecution";
import { EXIT_RULE_RESEARCH_PROFILES } from "./exitRulePresets";
import { listScalpV2CatalogStrategies } from "./strategyCatalog";

import type {
  ScalpV2CandidateDslSpec,
  ScalpV2ComposerModelFamily,
  ScalpV2ComposerModelScore,
  ScalpV2ModelGuidedCandidateDslSpec,
  ScalpV2PrimitiveBlock,
  ScalpV2PrimitiveBlockMap,
  ScalpV2PrimitiveFamily,
  ScalpV2Session,
  ScalpV2StrategyPrimitiveReference,
  ScalpV2Venue,
} from "./types";

const PRIMITIVE_FAMILIES: ScalpV2PrimitiveFamily[] = [
  "pattern",
  "session_filter",
  "state_machine",
  "entry_trigger",
  "exit_rule",
  "risk_rule",
];

function emptyBlockMap(): ScalpV2PrimitiveBlockMap {
  return {
    pattern: [],
    session_filter: [],
    state_machine: [],
    entry_trigger: [],
    exit_rule: [],
    risk_rule: [],
  };
}

function toBlockMap(
  patch: Partial<ScalpV2PrimitiveBlockMap>,
): ScalpV2PrimitiveBlockMap {
  return {
    pattern: uniqueStrings(patch.pattern || []),
    session_filter: uniqueStrings(patch.session_filter || []),
    state_machine: uniqueStrings(patch.state_machine || []),
    entry_trigger: uniqueStrings(patch.entry_trigger || []),
    exit_rule: uniqueStrings(patch.exit_rule || []),
    risk_rule: uniqueStrings(patch.risk_rule || []),
  };
}

function uniqueStrings(values: string[]): string[] {
  return Array.from(
    new Set(
      (values || [])
        .map((row) => String(row || "").trim())
        .filter(Boolean),
    ),
  );
}

function normalizeSymbol(value: string): string {
  return String(value || "")
    .trim()
    .toUpperCase()
    .replace(/[^A-Z0-9._-]/g, "");
}

const PRIMITIVE_BLOCKS: ScalpV2PrimitiveBlock[] = [
  {
    id: "pattern_regime_bias",
    family: "pattern",
    label: "Regime Bias",
    description:
      "Trend/regime context from higher timeframe structure before entry.",
    tags: ["trend", "context", "m15"],
    sourceStrategyIds: ["regime_pullback_m15_m3", "trend_day_reacceleration_m15_m3"],
  },
  {
    id: "pattern_pullback_structure",
    family: "pattern",
    label: "Pullback Structure",
    description:
      "Pullback to contextual levels before confirmation and entry.",
    tags: ["pullback", "structure"],
    sourceStrategyIds: ["regime_pullback_m15_m3", "compression_breakout_pullback_m15_m3"],
  },
  {
    id: "pattern_compression_breakout",
    family: "pattern",
    label: "Compression Breakout",
    description:
      "Range compression and expansion trigger with retest behavior.",
    tags: ["breakout", "volatility", "compression"],
    sourceStrategyIds: ["compression_breakout_pullback_m15_m3"],
  },
  {
    id: "pattern_opening_range_breakout_retest",
    family: "pattern",
    label: "Opening Range Breakout Retest",
    description:
      "Opening range breakout with disciplined retest requirement.",
    tags: ["orb", "opening_range", "m5_m1"],
    sourceStrategyIds: ["opening_range_breakout_retest_m5_m1"],
  },
  {
    id: "pattern_failed_auction_extreme",
    family: "pattern",
    label: "Failed Auction Extreme",
    description: "Extreme rejection and mean reversion after failed auction.",
    tags: ["reversal", "auction", "extreme"],
    sourceStrategyIds: ["failed_auction_extreme_reversal_m15_m1"],
  },
  {
    id: "pattern_trend_day_reacceleration",
    family: "pattern",
    label: "Trend Reacceleration",
    description: "Continuation setup after intraday reset inside trend day.",
    tags: ["continuation", "trend"],
    sourceStrategyIds: ["trend_day_reacceleration_m15_m3"],
  },
  {
    id: "pattern_anchored_vwap_reversion",
    family: "pattern",
    label: "Anchored VWAP Reversion",
    description:
      "Mean reversion around anchored VWAP dislocation and reversion zones.",
    tags: ["vwap", "reversion", "mean_revert"],
    sourceStrategyIds: ["anchored_vwap_reversion_m15_m3"],
  },
  {
    id: "pattern_basis_dislocation_reversion",
    family: "pattern",
    label: "Basis Dislocation Reversion",
    description:
      "Cross-stream dislocation proxy and normalization reversion trigger.",
    tags: ["basis", "proxy", "crypto"],
    sourceStrategyIds: ["basis_dislocation_reversion_proxy_m15_m3"],
  },
  {
    id: "pattern_relative_value_spread_reversion",
    family: "pattern",
    label: "Relative Value Spread Reversion",
    description:
      "Relative spread extension and reversion signal extraction.",
    tags: ["relative_value", "spread", "proxy"],
    sourceStrategyIds: ["relative_value_spread_proxy_m15_m3"],
  },
  {
    id: "pattern_session_seasonality_bias",
    family: "pattern",
    label: "Session Seasonality Bias",
    description:
      "Session-time conditioned directional bias and filter combination.",
    tags: ["seasonality", "session"],
    sourceStrategyIds: ["session_seasonality_bias_m15_m3"],
  },
  {
    id: "pattern_pdh_pdl_reclaim",
    family: "pattern",
    label: "PDH/PDL Reclaim",
    description:
      "Prior-day high/low reclaim and continuation/rejection interpretation.",
    tags: ["pdh", "pdl", "reclaim"],
    sourceStrategyIds: ["pdh_pdl_reclaim_m15_m3"],
  },
  {
    id: "pattern_hss_ict_structure",
    family: "pattern",
    label: "HSS/ICT Structure",
    description:
      "Market structure shift and imbalance interpretation in ICT-style flow.",
    tags: ["hss", "ict", "structure"],
    sourceStrategyIds: ["hss_ict_m15_m3_guarded"],
  },
  {
    id: "pattern_ifvg_displacement",
    family: "pattern",
    label: "IFVG + Displacement",
    description:
      "Imbalance with displacement confirmation before trigger activation.",
    tags: ["ifvg", "displacement", "confirmation"],
    sourceStrategyIds: [
      "regime_pullback_m15_m3",
      "compression_breakout_pullback_m15_m3",
      "hss_ict_m15_m3_guarded",
    ],
  },
  {
    id: "pattern_adaptive_pattern_router",
    family: "pattern",
    label: "Adaptive Pattern Router",
    description:
      "Pattern catalog arm selection with confidence-threshold routing.",
    tags: ["adaptive", "meta_selector"],
    sourceStrategyIds: ["adaptive_meta_selector_m15_m3"],
  },
  {
    id: "session_tokyo_window",
    family: "session_filter",
    label: "Tokyo Window",
    description: "Session-local trading window with Tokyo constraints.",
    tags: ["session", "tokyo"],
    sourceStrategyIds: ["session_seasonality_bias_m15_m3"],
  },
  {
    id: "session_berlin_window",
    family: "session_filter",
    label: "Berlin Window",
    description: "Session-local trading window with Berlin constraints.",
    tags: ["session", "berlin"],
    sourceStrategyIds: [
      "regime_pullback_m15_m3",
      "compression_breakout_pullback_m15_m3",
    ],
  },
  {
    id: "session_newyork_window",
    family: "session_filter",
    label: "New York Window",
    description: "Session-local trading window with New York constraints.",
    tags: ["session", "newyork"],
    sourceStrategyIds: ["opening_range_breakout_retest_m5_m1"],
  },
  {
    id: "session_pacific_window",
    family: "session_filter",
    label: "Pacific Window",
    description: "Session-local trading window with Pacific constraints.",
    tags: ["session", "pacific"],
    sourceStrategyIds: ["session_seasonality_bias_m15_m3"],
  },
  {
    id: "session_sydney_window",
    family: "session_filter",
    label: "Sydney Window",
    description: "Session-local trading window with Sydney constraints.",
    tags: ["session", "sydney"],
    sourceStrategyIds: ["session_seasonality_bias_m15_m3"],
  },
  {
    id: "session_blocked_hours",
    family: "session_filter",
    label: "Blocked Hours",
    description:
      "Explicit blocked-hour exclusions around weak liquidity periods.",
    tags: ["session", "blocked_hours"],
    sourceStrategyIds: [
      "regime_pullback_m15_m3",
      "hss_ict_m15_m3_guarded",
      "session_seasonality_bias_m15_m3",
    ],
  },
  {
    id: "session_raid_window",
    family: "session_filter",
    label: "Raid Window",
    description:
      "Raid/tactical window constraints for trigger activation and entries.",
    tags: ["session", "raid_window"],
    sourceStrategyIds: [
      "regime_pullback_m15_m3",
      "compression_breakout_pullback_m15_m3",
      "hss_ict_m15_m3_guarded",
    ],
  },
  {
    id: "session_seasonality_profile",
    family: "session_filter",
    label: "Seasonality Profile",
    description:
      "Time-of-day profile gating by expected directional/statistical edge.",
    tags: ["session", "seasonality"],
    sourceStrategyIds: ["session_seasonality_bias_m15_m3"],
  },
  {
    id: "state_consecutive_loss_pause",
    family: "state_machine",
    label: "Consecutive Loss Pause",
    description: "Pause and cooldown state after consecutive losses.",
    tags: ["state", "cooldown"],
    sourceStrategyIds: [
      "regime_pullback_m15_m3",
      "compression_breakout_pullback_m15_m3",
      "trend_day_reacceleration_m15_m3",
    ],
  },
  {
    id: "state_daily_loss_limit",
    family: "state_machine",
    label: "Daily Loss Guard",
    description: "Daily loss guardrail state for hard stop behavior.",
    tags: ["state", "risk"],
    sourceStrategyIds: [
      "regime_pullback_m15_m3",
      "compression_breakout_pullback_m15_m3",
      "opening_range_breakout_retest_m5_m1",
    ],
  },
  {
    id: "state_max_trades_per_day",
    family: "state_machine",
    label: "Daily Trade Cap",
    description: "Cap entries per day to limit churn and overtrading loops.",
    tags: ["state", "churn"],
    sourceStrategyIds: [
      "regime_pullback_m15_m3",
      "compression_breakout_pullback_m15_m3",
      "opening_range_breakout_retest_m5_m1",
    ],
  },
  {
    id: "state_confirm_ttl_expiry",
    family: "state_machine",
    label: "Confirmation TTL",
    description:
      "State expiration for stale confirmations before entry commitment.",
    tags: ["state", "ttl"],
    sourceStrategyIds: ["regime_pullback_m15_m3", "hss_ict_m15_m3_guarded"],
  },
  {
    id: "state_sweep_reject_timeout",
    family: "state_machine",
    label: "Sweep Reject Timeout",
    description:
      "State timeout for sweep rejection lifecycle and invalidation reset.",
    tags: ["state", "sweep"],
    sourceStrategyIds: [
      "regime_pullback_m15_m3",
      "compression_breakout_pullback_m15_m3",
      "pdh_pdl_reclaim_m15_m3",
    ],
  },
  {
    id: "state_adaptive_confidence_gate",
    family: "state_machine",
    label: "Adaptive Confidence Gate",
    description:
      "Arm selection state machine with confidence threshold and skip paths.",
    tags: ["state", "adaptive"],
    sourceStrategyIds: ["adaptive_meta_selector_m15_m3"],
  },
  {
    id: "entry_sweep_reclaim",
    family: "entry_trigger",
    label: "Sweep Reclaim Trigger",
    description:
      "Entry trigger around sweep reclaim with context and rejection checks.",
    tags: ["entry", "sweep"],
    sourceStrategyIds: [
      "regime_pullback_m15_m3",
      "compression_breakout_pullback_m15_m3",
      "pdh_pdl_reclaim_m15_m3",
    ],
  },
  {
    id: "entry_mss_break_trigger",
    family: "entry_trigger",
    label: "MSS Break Trigger",
    description:
      "Market structure shift break trigger with directional confirmation.",
    tags: ["entry", "mss"],
    sourceStrategyIds: [
      "regime_pullback_m15_m3",
      "hss_ict_m15_m3_guarded",
      "trend_day_reacceleration_m15_m3",
    ],
  },
  {
    id: "entry_ifvg_touch",
    family: "entry_trigger",
    label: "IFVG Touch Trigger",
    description:
      "Imbalance touch trigger with mode constraints and minimum quality.",
    tags: ["entry", "ifvg"],
    sourceStrategyIds: [
      "regime_pullback_m15_m3",
      "compression_breakout_pullback_m15_m3",
      "hss_ict_m15_m3_guarded",
    ],
  },
  {
    id: "entry_displacement_confirm",
    family: "entry_trigger",
    label: "Displacement Confirm",
    description:
      "Displacement confirmation gate before entry trigger is armed.",
    tags: ["entry", "displacement"],
    sourceStrategyIds: [
      "regime_pullback_m15_m3",
      "compression_breakout_pullback_m15_m3",
      "opening_range_breakout_retest_m5_m1",
    ],
  },
  {
    id: "entry_opening_range_retest",
    family: "entry_trigger",
    label: "Opening Range Retest",
    description:
      "Opening-range retest entry with directional breakout context.",
    tags: ["entry", "opening_range"],
    sourceStrategyIds: ["opening_range_breakout_retest_m5_m1"],
  },
  {
    id: "entry_vwap_snapback",
    family: "entry_trigger",
    label: "VWAP Snapback",
    description: "VWAP snapback trigger from stretched mean-reversion state.",
    tags: ["entry", "vwap", "mean_revert"],
    sourceStrategyIds: ["anchored_vwap_reversion_m15_m3"],
  },
  {
    id: "entry_extreme_reversal",
    family: "entry_trigger",
    label: "Extreme Reversal",
    description: "Extreme rejection reversal entry trigger.",
    tags: ["entry", "reversal"],
    sourceStrategyIds: ["failed_auction_extreme_reversal_m15_m1"],
  },
  {
    id: "entry_relative_value_mean_revert",
    family: "entry_trigger",
    label: "Relative Value Mean Revert",
    description:
      "Relative-value spread normalization trigger back to fair value.",
    tags: ["entry", "relative_value"],
    sourceStrategyIds: [
      "basis_dislocation_reversion_proxy_m15_m3",
      "relative_value_spread_proxy_m15_m3",
      "funding_oi_exhaustion_proxy_m15_m3",
    ],
  },
  {
    id: "entry_seasonality_window_bias",
    family: "entry_trigger",
    label: "Seasonality Bias Trigger",
    description:
      "Entry trigger conditioned on session seasonal bias windows.",
    tags: ["entry", "seasonality"],
    sourceStrategyIds: ["session_seasonality_bias_m15_m3"],
  },
  {
    id: "entry_adaptive_arm_selection",
    family: "entry_trigger",
    label: "Adaptive Arm Selection",
    description:
      "Adaptive pattern/incumbent arm routing into a final entry intent.",
    tags: ["entry", "adaptive"],
    sourceStrategyIds: ["adaptive_meta_selector_m15_m3"],
  },
  {
    id: "exit_fixed_r_take_profit",
    family: "exit_rule",
    label: "Fixed R TP",
    description: "Fixed-R take-profit objective with hard stop pairing.",
    tags: ["exit", "tp"],
    sourceStrategyIds: [
      "regime_pullback_m15_m3",
      "compression_breakout_pullback_m15_m3",
      "opening_range_breakout_retest_m5_m1",
    ],
  },
  {
    id: "exit_tp1_then_trail",
    family: "exit_rule",
    label: "TP1 Then Trail",
    description:
      "Partial TP1 close then trailing management for extended moves.",
    tags: ["exit", "tp1", "trail"],
    sourceStrategyIds: [
      "regime_pullback_m15_m3",
      "compression_breakout_pullback_m15_m3",
      "trend_day_reacceleration_m15_m3",
    ],
  },
  {
    id: "exit_break_even_shift",
    family: "exit_rule",
    label: "Break-even Shift",
    description: "Shift stop to break-even after favorable move threshold.",
    tags: ["exit", "breakeven"],
    sourceStrategyIds: [
      "regime_pullback_m15_m3",
      "compression_breakout_pullback_m15_m3",
      "hss_ict_m15_m3_guarded",
    ],
  },
  {
    id: "exit_trailing_atr",
    family: "exit_rule",
    label: "ATR Trailing Exit",
    description: "ATR-based trailing stop behavior for adaptive exits.",
    tags: ["exit", "trail", "atr"],
    sourceStrategyIds: [
      "regime_pullback_m15_m3",
      "compression_breakout_pullback_m15_m3",
      "trend_day_reacceleration_m15_m3",
    ],
  },
  {
    id: "exit_time_stop",
    family: "exit_rule",
    label: "Time Stop",
    description: "Maximum hold-time exit to reduce churn and stale exposure.",
    tags: ["exit", "time_stop"],
    sourceStrategyIds: [
      "regime_pullback_m15_m3",
      "compression_breakout_pullback_m15_m3",
      "session_seasonality_bias_m15_m3",
    ],
  },
  {
    id: "exit_invalidation_stop",
    family: "exit_rule",
    label: "Invalidation Stop",
    description: "Structure invalidation stop when premise breaks.",
    tags: ["exit", "invalidation"],
    sourceStrategyIds: ["hss_ict_m15_m3_guarded", "pdh_pdl_reclaim_m15_m3"],
  },
  {
    id: "exit_session_cutoff",
    family: "exit_rule",
    label: "Session Cutoff Exit",
    description:
      "Session-time cutoff close policy to avoid off-window carry.",
    tags: ["exit", "session"],
    sourceStrategyIds: ["opening_range_breakout_retest_m5_m1", "session_seasonality_bias_m15_m3"],
  },
  {
    id: "risk_pct_equity_sizing",
    family: "risk_rule",
    label: "Percent Equity Sizing",
    description: "Risk-per-trade percentage sizing baseline.",
    tags: ["risk", "sizing"],
    sourceStrategyIds: [
      "regime_pullback_m15_m3",
      "compression_breakout_pullback_m15_m3",
      "opening_range_breakout_retest_m5_m1",
    ],
  },
  {
    id: "risk_min_max_notional",
    family: "risk_rule",
    label: "Notional Clamp",
    description: "Min/max notional clamps before order submission.",
    tags: ["risk", "notional"],
    sourceStrategyIds: [
      "regime_pullback_m15_m3",
      "compression_breakout_pullback_m15_m3",
    ],
  },
  {
    id: "risk_spread_aware_stop_buffer",
    family: "risk_rule",
    label: "Spread-aware Stop Buffer",
    description: "Spread-aware stop-distance and placement adjustments.",
    tags: ["risk", "spread"],
    sourceStrategyIds: [
      "regime_pullback_m15_m3",
      "compression_breakout_pullback_m15_m3",
      "opening_range_breakout_retest_m5_m1",
    ],
  },
  {
    id: "risk_atr_stop_distance",
    family: "risk_rule",
    label: "ATR Stop Distance",
    description: "ATR-informed stop distance and trailing behavior.",
    tags: ["risk", "atr"],
    sourceStrategyIds: [
      "regime_pullback_m15_m3",
      "trend_day_reacceleration_m15_m3",
      "anchored_vwap_reversion_m15_m3",
    ],
  },
  {
    id: "risk_single_position_per_symbol",
    family: "risk_rule",
    label: "Single Position per Symbol",
    description: "Single active position per symbol exposure guard.",
    tags: ["risk", "exposure"],
    sourceStrategyIds: [
      "regime_pullback_m15_m3",
      "compression_breakout_pullback_m15_m3",
      "adaptive_meta_selector_m15_m3",
    ],
  },
  {
    id: "risk_leverage_cap",
    family: "risk_rule",
    label: "Leverage Cap",
    description: "Leverage cap controls to bound tail-risk amplification.",
    tags: ["risk", "leverage"],
    sourceStrategyIds: [
      "funding_oi_exhaustion_proxy_m15_m3",
      "basis_dislocation_reversion_proxy_m15_m3",
      "relative_value_spread_proxy_m15_m3",
    ],
  },
  {
    id: "risk_adaptive_lock_period",
    family: "risk_rule",
    label: "Adaptive Lock Period",
    description:
      "Lock period and drawdown breach controls for adaptive snapshots.",
    tags: ["risk", "adaptive"],
    sourceStrategyIds: ["adaptive_meta_selector_m15_m3"],
  },
];

const STRATEGY_REFERENCE_MAP: Record<string, ScalpV2PrimitiveBlockMap> = {
  regime_pullback_m15_m3: toBlockMap({
    pattern: [
      "pattern_regime_bias",
      "pattern_pullback_structure",
      "pattern_ifvg_displacement",
    ],
    session_filter: ["session_berlin_window", "session_raid_window", "session_blocked_hours"],
    state_machine: [
      "state_consecutive_loss_pause",
      "state_daily_loss_limit",
      "state_max_trades_per_day",
      "state_confirm_ttl_expiry",
      "state_sweep_reject_timeout",
    ],
    entry_trigger: [
      "entry_sweep_reclaim",
      "entry_mss_break_trigger",
      "entry_ifvg_touch",
      "entry_displacement_confirm",
    ],
    exit_rule: [
      "exit_fixed_r_take_profit",
      "exit_tp1_then_trail",
      "exit_break_even_shift",
      "exit_trailing_atr",
      "exit_time_stop",
    ],
    risk_rule: [
      "risk_pct_equity_sizing",
      "risk_min_max_notional",
      "risk_spread_aware_stop_buffer",
      "risk_atr_stop_distance",
      "risk_single_position_per_symbol",
    ],
  }),
  hss_ict_m15_m3_guarded: toBlockMap({
    pattern: ["pattern_hss_ict_structure", "pattern_ifvg_displacement"],
    session_filter: ["session_berlin_window", "session_raid_window", "session_blocked_hours"],
    state_machine: ["state_confirm_ttl_expiry", "state_sweep_reject_timeout"],
    entry_trigger: ["entry_mss_break_trigger", "entry_ifvg_touch", "entry_displacement_confirm"],
    exit_rule: ["exit_invalidation_stop", "exit_break_even_shift", "exit_tp1_then_trail"],
    risk_rule: ["risk_pct_equity_sizing", "risk_spread_aware_stop_buffer", "risk_single_position_per_symbol"],
  }),
  opening_range_breakout_retest_m5_m1: toBlockMap({
    pattern: ["pattern_opening_range_breakout_retest"],
    session_filter: ["session_newyork_window", "session_raid_window"],
    state_machine: ["state_daily_loss_limit", "state_max_trades_per_day"],
    entry_trigger: ["entry_opening_range_retest", "entry_displacement_confirm"],
    exit_rule: ["exit_fixed_r_take_profit", "exit_session_cutoff", "exit_time_stop"],
    risk_rule: ["risk_pct_equity_sizing", "risk_spread_aware_stop_buffer", "risk_min_max_notional"],
  }),
  pdh_pdl_reclaim_m15_m3: toBlockMap({
    pattern: ["pattern_pdh_pdl_reclaim", "pattern_pullback_structure"],
    session_filter: ["session_berlin_window", "session_raid_window"],
    state_machine: ["state_sweep_reject_timeout", "state_max_trades_per_day"],
    entry_trigger: ["entry_sweep_reclaim", "entry_mss_break_trigger"],
    exit_rule: ["exit_invalidation_stop", "exit_break_even_shift", "exit_time_stop"],
    risk_rule: ["risk_pct_equity_sizing", "risk_spread_aware_stop_buffer"],
  }),
  compression_breakout_pullback_m15_m3: toBlockMap({
    pattern: [
      "pattern_compression_breakout",
      "pattern_pullback_structure",
      "pattern_ifvg_displacement",
    ],
    session_filter: ["session_berlin_window", "session_raid_window"],
    state_machine: [
      "state_consecutive_loss_pause",
      "state_daily_loss_limit",
      "state_max_trades_per_day",
      "state_sweep_reject_timeout",
    ],
    entry_trigger: [
      "entry_sweep_reclaim",
      "entry_ifvg_touch",
      "entry_displacement_confirm",
    ],
    exit_rule: [
      "exit_fixed_r_take_profit",
      "exit_tp1_then_trail",
      "exit_break_even_shift",
      "exit_trailing_atr",
      "exit_time_stop",
    ],
    risk_rule: [
      "risk_pct_equity_sizing",
      "risk_min_max_notional",
      "risk_spread_aware_stop_buffer",
      "risk_single_position_per_symbol",
    ],
  }),
  failed_auction_extreme_reversal_m15_m1: toBlockMap({
    pattern: ["pattern_failed_auction_extreme"],
    session_filter: ["session_berlin_window", "session_newyork_window"],
    state_machine: ["state_max_trades_per_day", "state_consecutive_loss_pause"],
    entry_trigger: ["entry_extreme_reversal"],
    exit_rule: ["exit_break_even_shift", "exit_time_stop", "exit_fixed_r_take_profit"],
    risk_rule: ["risk_pct_equity_sizing", "risk_spread_aware_stop_buffer"],
  }),
  trend_day_reacceleration_m15_m3: toBlockMap({
    pattern: ["pattern_trend_day_reacceleration", "pattern_regime_bias"],
    session_filter: ["session_berlin_window", "session_newyork_window"],
    state_machine: ["state_consecutive_loss_pause", "state_daily_loss_limit", "state_max_trades_per_day"],
    entry_trigger: ["entry_mss_break_trigger", "entry_displacement_confirm"],
    exit_rule: ["exit_tp1_then_trail", "exit_trailing_atr", "exit_time_stop"],
    risk_rule: ["risk_pct_equity_sizing", "risk_atr_stop_distance", "risk_single_position_per_symbol"],
  }),
  anchored_vwap_reversion_m15_m3: toBlockMap({
    pattern: ["pattern_anchored_vwap_reversion"],
    session_filter: ["session_berlin_window", "session_newyork_window"],
    state_machine: ["state_max_trades_per_day"],
    entry_trigger: ["entry_vwap_snapback", "entry_displacement_confirm"],
    exit_rule: ["exit_break_even_shift", "exit_tp1_then_trail", "exit_time_stop"],
    risk_rule: ["risk_pct_equity_sizing", "risk_atr_stop_distance"],
  }),
  funding_oi_exhaustion_proxy_m15_m3: toBlockMap({
    pattern: ["pattern_basis_dislocation_reversion", "pattern_relative_value_spread_reversion"],
    session_filter: ["session_berlin_window", "session_newyork_window"],
    state_machine: ["state_max_trades_per_day"],
    entry_trigger: ["entry_relative_value_mean_revert"],
    exit_rule: ["exit_break_even_shift", "exit_time_stop", "exit_fixed_r_take_profit"],
    risk_rule: ["risk_pct_equity_sizing", "risk_leverage_cap", "risk_single_position_per_symbol"],
  }),
  basis_dislocation_reversion_proxy_m15_m3: toBlockMap({
    pattern: ["pattern_basis_dislocation_reversion"],
    session_filter: ["session_berlin_window", "session_newyork_window"],
    state_machine: ["state_max_trades_per_day"],
    entry_trigger: ["entry_relative_value_mean_revert"],
    exit_rule: ["exit_break_even_shift", "exit_time_stop", "exit_fixed_r_take_profit"],
    risk_rule: ["risk_pct_equity_sizing", "risk_leverage_cap", "risk_single_position_per_symbol"],
  }),
  relative_value_spread_proxy_m15_m3: toBlockMap({
    pattern: ["pattern_relative_value_spread_reversion"],
    session_filter: ["session_berlin_window", "session_newyork_window"],
    state_machine: ["state_max_trades_per_day"],
    entry_trigger: ["entry_relative_value_mean_revert"],
    exit_rule: ["exit_break_even_shift", "exit_time_stop", "exit_fixed_r_take_profit"],
    risk_rule: ["risk_pct_equity_sizing", "risk_leverage_cap", "risk_single_position_per_symbol"],
  }),
  session_seasonality_bias_m15_m3: toBlockMap({
    pattern: ["pattern_session_seasonality_bias"],
    session_filter: [
      "session_tokyo_window",
      "session_berlin_window",
      "session_newyork_window",
      "session_sydney_window",
      "session_seasonality_profile",
      "session_blocked_hours",
    ],
    state_machine: ["state_max_trades_per_day", "state_daily_loss_limit"],
    entry_trigger: ["entry_seasonality_window_bias"],
    exit_rule: ["exit_session_cutoff", "exit_time_stop", "exit_break_even_shift"],
    risk_rule: ["risk_pct_equity_sizing", "risk_spread_aware_stop_buffer"],
  }),
  adaptive_meta_selector_m15_m3: toBlockMap({
    pattern: ["pattern_adaptive_pattern_router"],
    session_filter: [
      "session_tokyo_window",
      "session_berlin_window",
      "session_newyork_window",
      "session_sydney_window",
      "session_blocked_hours",
    ],
    state_machine: ["state_adaptive_confidence_gate", "state_max_trades_per_day"],
    entry_trigger: ["entry_adaptive_arm_selection"],
    exit_rule: ["exit_tp1_then_trail", "exit_break_even_shift", "exit_time_stop"],
    risk_rule: [
      "risk_pct_equity_sizing",
      "risk_single_position_per_symbol",
      "risk_adaptive_lock_period",
    ],
  }),
};

const SESSION_PRIMARY_BLOCK_BY_PROFILE: Record<ScalpV2Session, string> = {
  tokyo: "session_tokyo_window",
  berlin: "session_berlin_window",
  newyork: "session_newyork_window",
  pacific: "session_pacific_window",
  sydney: "session_sydney_window",
};

function fallbackReferenceMap(): ScalpV2PrimitiveBlockMap {
  return toBlockMap({
    pattern: ["pattern_regime_bias", "pattern_pullback_structure", "pattern_ifvg_displacement"],
    session_filter: ["session_berlin_window", "session_raid_window"],
    state_machine: ["state_max_trades_per_day", "state_daily_loss_limit"],
    entry_trigger: ["entry_mss_break_trigger", "entry_displacement_confirm"],
    exit_rule: ["exit_tp1_then_trail", "exit_break_even_shift", "exit_time_stop"],
    risk_rule: ["risk_pct_equity_sizing", "risk_spread_aware_stop_buffer", "risk_single_position_per_symbol"],
  });
}

function strategyNotes(strategyId: string): string[] {
  if (strategyId === "adaptive_meta_selector_m15_m3") {
    return [
      "Routes between incumbent and pattern arms using confidence gating.",
      "Treat as meta-router reference, not a single standalone pattern.",
    ];
  }
  if (strategyId.includes("_proxy_")) {
    return [
      "Uses proxy features that should be paired with stronger risk clamps.",
    ];
  }
  if (strategyId.includes("opening_range")) {
    return [
      "Intraday opening range logic is session-sensitive and should remain tightly windowed.",
    ];
  }
  return [
    "Interpretable detector flow with explicit reason-code boundaries.",
  ];
}

function familyBlockUsageCount(
  references: ScalpV2StrategyPrimitiveReference[],
  family: ScalpV2PrimitiveFamily,
): Map<string, number> {
  const counts = new Map<string, number>();
  for (const ref of references) {
    for (const blockId of ref.blocksByFamily[family]) {
      counts.set(blockId, (counts.get(blockId) || 0) + 1);
    }
  }
  return counts;
}

function strategyIdsForBlock(blockId: string): string[] {
  const strategyIds = new Set<string>();
  for (const block of PRIMITIVE_BLOCKS) {
    if (block.id !== blockId) continue;
    for (const strategyId of block.sourceStrategyIds) strategyIds.add(strategyId);
  }
  return Array.from(strategyIds);
}

function blockByIdMap(): Map<string, ScalpV2PrimitiveBlock> {
  return new Map(PRIMITIVE_BLOCKS.map((row) => [row.id, row]));
}

function scoreBlockForContext(params: {
  blockId: string;
  family: ScalpV2PrimitiveFamily;
  symbol: string;
  venue: ScalpV2Venue;
  entrySessionProfile: ScalpV2Session;
  usageCounts: Map<string, number>;
  blockMap: Map<string, ScalpV2PrimitiveBlock>;
}): number {
  const base = params.usageCounts.get(params.blockId) || 0;
  const block = params.blockMap.get(params.blockId);
  if (!block) return base;
  const symbol = normalizeSymbol(params.symbol);
  let boost = 0;

  if (params.family === "session_filter") {
    if (params.blockId === SESSION_PRIMARY_BLOCK_BY_PROFILE[params.entrySessionProfile]) {
      boost += 3;
    }
    if (block.tags.includes(params.entrySessionProfile)) boost += 1.5;
  }

  if (params.venue === "bitget" && symbol.endsWith("USDT")) {
    if (block.tags.includes("crypto")) boost += 1.5;
    if (block.tags.includes("proxy")) boost += 0.8;
  }
  if (params.venue === "capital") {
    if (block.tags.includes("session")) boost += 0.4;
    if (block.tags.includes("seasonality")) boost += 0.7;
    if (symbol === "EURUSD") {
      if (block.tags.includes("trend")) boost += 0.7;
      if (block.tags.includes("mean_revert")) boost += 0.4;
    }
  }
  if (symbol === "BTCUSDT") {
    if (block.tags.includes("volatility")) boost += 0.8;
    if (block.tags.includes("ifvg")) boost += 0.6;
  }

  return base + boost;
}

function sortedBlockPool(params: {
  family: ScalpV2PrimitiveFamily;
  references: ScalpV2StrategyPrimitiveReference[];
  symbol: string;
  venue: ScalpV2Venue;
  entrySessionProfile: ScalpV2Session;
  maxSize: number;
  blockMap: Map<string, ScalpV2PrimitiveBlock>;
}): string[] {
  const usageCounts = familyBlockUsageCount(params.references, params.family);
  const sorted = Array.from(usageCounts.keys()).sort((a, b) => {
    const scoreA = scoreBlockForContext({
      blockId: a,
      family: params.family,
      symbol: params.symbol,
      venue: params.venue,
      entrySessionProfile: params.entrySessionProfile,
      usageCounts,
      blockMap: params.blockMap,
    });
    const scoreB = scoreBlockForContext({
      blockId: b,
      family: params.family,
      symbol: params.symbol,
      venue: params.venue,
      entrySessionProfile: params.entrySessionProfile,
      usageCounts,
      blockMap: params.blockMap,
    });
    if (scoreB !== scoreA) return scoreB - scoreA;
    return a.localeCompare(b);
  });
  return sorted.slice(0, Math.max(1, Math.floor(params.maxSize)));
}

function candidateHash(input: string): string {
  return crypto.createHash("sha1").update(input).digest("hex");
}

function normalizeCursorOffset(value: unknown): number {
  const n = Math.floor(Number(value));
  if (!Number.isFinite(n) || n < 0) return 0;
  return n;
}

export function resolveScalpV2CandidateEvaluationWindow<T>(params: {
  candidates: T[];
  maxCandidates: number;
  startOffset?: number;
}): {
  selectedCandidates: T[];
  poolSize: number;
  maxCandidates: number;
  startOffset: number;
  evaluatedCount: number;
  nextOffset: number;
} {
  const candidates = Array.isArray(params.candidates) ? params.candidates : [];
  const poolSize = candidates.length;
  const maxCandidates = Math.max(
    1,
    Math.floor(Number(params.maxCandidates) || 1),
  );
  if (poolSize === 0) {
    return {
      selectedCandidates: [],
      poolSize: 0,
      maxCandidates,
      startOffset: 0,
      evaluatedCount: 0,
      nextOffset: 0,
    };
  }

  const evaluatedCount = Math.min(poolSize, maxCandidates);
  const startOffset = normalizeCursorOffset(params.startOffset) % poolSize;
  const selectedCandidates: T[] = [];
  for (let idx = 0; idx < evaluatedCount; idx += 1) {
    selectedCandidates.push(candidates[(startOffset + idx) % poolSize] as T);
  }
  const nextOffset = (startOffset + evaluatedCount) % poolSize;

  return {
    selectedCandidates,
    poolSize,
    maxCandidates,
    startOffset,
    evaluatedCount,
    nextOffset,
  };
}

function clamp01(value: number): number {
  if (!Number.isFinite(value)) return 0;
  return Math.max(0, Math.min(1, value));
}

function hasBlockId(
  blockIds: string[],
  target: string,
): boolean {
  return (blockIds || []).some((row) => row === target);
}

function hasBlockTag(
  blockIds: string[],
  tag: string,
  blockMap: Map<string, ScalpV2PrimitiveBlock>,
): boolean {
  const normalizedTag = String(tag || "").trim().toLowerCase();
  if (!normalizedTag) return false;
  for (const blockId of blockIds || []) {
    const block = blockMap.get(blockId);
    if (!block) continue;
    if (
      block.tags.some(
        (row) => String(row || "").trim().toLowerCase() === normalizedTag,
      )
    ) {
      return true;
    }
  }
  return false;
}

function inferComposerFamily(
  interpretableScore: number,
  treeScore: number,
  sequenceScore: number,
): ScalpV2ComposerModelFamily {
  const best = Math.max(interpretableScore, treeScore, sequenceScore);
  if (best === treeScore) return "tree_split_proxy";
  if (best === sequenceScore) return "sequence_state_proxy";
  return "interpretable_pattern_blend";
}

function buildComposerModelScore(params: {
  dsl: ScalpV2CandidateDslSpec;
  blockMap: Map<string, ScalpV2PrimitiveBlock>;
}): ScalpV2ComposerModelScore {
  const { dsl, blockMap } = params;
  const patternBlocks = dsl.blocksByFamily.pattern;
  const sessionBlocks = dsl.blocksByFamily.session_filter;
  const stateBlocks = dsl.blocksByFamily.state_machine;
  const entryBlocks = dsl.blocksByFamily.entry_trigger;
  const exitBlocks = dsl.blocksByFamily.exit_rule;
  const riskBlocks = dsl.blocksByFamily.risk_rule;

  const sessionPrimary = SESSION_PRIMARY_BLOCK_BY_PROFILE[dsl.entrySessionProfile];
  const sessionPrimaryMatch = hasBlockId(sessionBlocks, sessionPrimary) ? 1 : 0;
  const supportNorm = clamp01(dsl.supportScore / 12);
  const referencesNorm = clamp01(dsl.referenceStrategyIds.length / 12);
  const familiesCoveredNorm =
    PRIMITIVE_FAMILIES.filter((family) => dsl.blocksByFamily[family].length > 0)
      .length / PRIMITIVE_FAMILIES.length;
  const interpretableScore = clamp01(
    0.24 +
      supportNorm * 0.34 +
      referencesNorm * 0.18 +
      sessionPrimaryMatch * 0.12 +
      familiesCoveredNorm * 0.12,
  );

  let treeScore = 0.28;
  if (dsl.venue === "bitget") {
    if (hasBlockTag(patternBlocks, "volatility", blockMap)) treeScore += 0.12;
    if (hasBlockTag(patternBlocks, "crypto", blockMap)) treeScore += 0.08;
    if (hasBlockTag(patternBlocks, "proxy", blockMap)) treeScore += 0.06;
  } else {
    if (hasBlockTag(patternBlocks, "trend", blockMap)) treeScore += 0.1;
    if (hasBlockTag(patternBlocks, "mean_revert", blockMap)) treeScore += 0.08;
    if (hasBlockTag(sessionBlocks, "seasonality", blockMap)) treeScore += 0.06;
  }
  if (dsl.symbol === "BTCUSDT") {
    if (hasBlockTag(patternBlocks, "ifvg", blockMap)) treeScore += 0.07;
    if (hasBlockTag(entryBlocks, "displacement", blockMap)) treeScore += 0.05;
  }
  if (dsl.symbol === "EURUSD") {
    if (hasBlockTag(sessionBlocks, "session", blockMap)) treeScore += 0.05;
    if (hasBlockTag(patternBlocks, "trend", blockMap)) treeScore += 0.05;
  }
  if (hasBlockTag(riskBlocks, "spread", blockMap)) treeScore += 0.04;
  if (hasBlockTag(riskBlocks, "atr", blockMap)) treeScore += 0.04;
  if (
    hasBlockId(patternBlocks, "pattern_adaptive_pattern_router") &&
    !hasBlockId(stateBlocks, "state_adaptive_confidence_gate")
  ) {
    treeScore -= 0.12;
  }
  const treeScoreBounded = clamp01(treeScore);

  let sequenceScore = 0.2;
  sequenceScore += Math.min(0.2, stateBlocks.length * 0.08);
  sequenceScore += Math.min(0.22, entryBlocks.length * 0.1);
  sequenceScore += Math.min(0.2, exitBlocks.length * 0.08);
  if (
    hasBlockId(entryBlocks, "entry_sweep_reclaim") &&
    hasBlockId(stateBlocks, "state_sweep_reject_timeout")
  ) {
    sequenceScore += 0.08;
  }
  if (
    hasBlockId(entryBlocks, "entry_displacement_confirm") &&
    hasBlockId(patternBlocks, "pattern_ifvg_displacement")
  ) {
    sequenceScore += 0.08;
  }
  if (
    hasBlockId(exitBlocks, "exit_tp1_then_trail") &&
    hasBlockId(exitBlocks, "exit_break_even_shift")
  ) {
    sequenceScore += 0.05;
  }
  if (hasBlockId(riskBlocks, "risk_single_position_per_symbol")) {
    sequenceScore += 0.04;
  }
  if (!sessionBlocks.length) sequenceScore -= 0.15;
  const sequenceScoreBounded = clamp01(sequenceScore);

  const compositeScore = clamp01(
    interpretableScore * 0.45 +
      treeScoreBounded * 0.32 +
      sequenceScoreBounded * 0.23,
  );
  const maxScore = Math.max(
    interpretableScore,
    treeScoreBounded,
    sequenceScoreBounded,
  );
  const minScore = Math.min(
    interpretableScore,
    treeScoreBounded,
    sequenceScoreBounded,
  );
  const confidence = clamp01(
    0.42 +
      (maxScore - minScore) * 0.35 +
      supportNorm * 0.18 +
      familiesCoveredNorm * 0.05,
  );

  return {
    family: inferComposerFamily(
      interpretableScore,
      treeScoreBounded,
      sequenceScoreBounded,
    ),
    interpretableScore,
    treeScore: treeScoreBounded,
    sequenceScore: sequenceScoreBounded,
    compositeScore,
    confidence,
    version: "composer_v2_r1",
  };
}

export function listScalpV2PrimitiveBlocks(): ScalpV2PrimitiveBlock[] {
  return PRIMITIVE_BLOCKS.slice();
}

export function listScalpV2StrategyPrimitiveReferences(): ScalpV2StrategyPrimitiveReference[] {
  return listScalpV2CatalogStrategies().map((strategy) => {
    const strategyId = strategy.id;
    const mapped = STRATEGY_REFERENCE_MAP[strategyId] || fallbackReferenceMap();
    return {
      strategyId,
      blocksByFamily: toBlockMap(mapped),
      notes: strategyNotes(strategyId),
    };
  });
}

export function buildScalpV2PrimitiveCatalogByFamily(): Record<
  ScalpV2PrimitiveFamily,
  ScalpV2PrimitiveBlock[]
> {
  const out: Record<ScalpV2PrimitiveFamily, ScalpV2PrimitiveBlock[]> = {
    pattern: [],
    session_filter: [],
    state_machine: [],
    entry_trigger: [],
    exit_rule: [],
    risk_rule: [],
  };
  for (const row of PRIMITIVE_BLOCKS) out[row.family].push(row);
  for (const family of PRIMITIVE_FAMILIES) {
    out[family].sort((a, b) => a.id.localeCompare(b.id));
  }
  return out;
}

export function toScalpV2ResearchCursorKey(params: {
  venue: ScalpV2Venue;
  symbol: string;
  entrySessionProfile: ScalpV2Session;
}): string {
  return `v2:${params.venue}:${normalizeSymbol(params.symbol)}:${params.entrySessionProfile}`;
}

export function buildScalpV2CandidateDslGrid(params: {
  venue: ScalpV2Venue;
  symbol: string;
  entrySessionProfile: ScalpV2Session;
  maxCandidates?: number;
}): ScalpV2CandidateDslSpec[] {
  const symbol = normalizeSymbol(params.symbol);
  const maxCandidates = Math.max(
    1,
    Math.min(2000, Math.floor(params.maxCandidates || 250)),
  );
  const references = listScalpV2StrategyPrimitiveReferences();
  const blockMap = blockByIdMap();

  const patternPool = sortedBlockPool({
    family: "pattern",
    references,
    symbol,
    venue: params.venue,
    entrySessionProfile: params.entrySessionProfile,
    maxSize: 8,
    blockMap,
  });
  const entryPool = sortedBlockPool({
    family: "entry_trigger",
    references,
    symbol,
    venue: params.venue,
    entrySessionProfile: params.entrySessionProfile,
    maxSize: 6,
    blockMap,
  });
  const exitPool = sortedBlockPool({
    family: "exit_rule",
    references,
    symbol,
    venue: params.venue,
    entrySessionProfile: params.entrySessionProfile,
    maxSize: 6,
    blockMap,
  });
  const statePool = sortedBlockPool({
    family: "state_machine",
    references,
    symbol,
    venue: params.venue,
    entrySessionProfile: params.entrySessionProfile,
    maxSize: 5,
    blockMap,
  });
  const riskPool = sortedBlockPool({
    family: "risk_rule",
    references,
    symbol,
    venue: params.venue,
    entrySessionProfile: params.entrySessionProfile,
    maxSize: 5,
    blockMap,
  });
  const sessionPoolRaw = sortedBlockPool({
    family: "session_filter",
    references,
    symbol,
    venue: params.venue,
    entrySessionProfile: params.entrySessionProfile,
    maxSize: 6,
    blockMap,
  });
  const primarySessionBlock =
    SESSION_PRIMARY_BLOCK_BY_PROFILE[params.entrySessionProfile];
  const sessionPool = uniqueStrings([primarySessionBlock, ...sessionPoolRaw]).slice(
    0,
    6,
  );

  // Phase 3: Deep exploration — iterate exit, state, and risk as independent
  // dimensions for each pattern×entry pair. This explores the full curated
  // block space (~14×10×7×6×7 = thousands) instead of the old ~48 combos.
  // Candidates are collected unsorted, then ranked by supportScore so the
  // best-supported combos survive the maxCandidates cap.
  type RawCandidate = {
    blocksByFamily: ScalpV2PrimitiveBlockMap;
    referenceStrategyIds: string[];
    supportScore: number;
    patternBlock: string;
  };
  const rawCandidates: RawCandidate[] = [];
  const seenSignatures = new Set<string>();
  const generatedAtMs = Date.now();

  for (let p = 0; p < patternPool.length; p += 1) {
    const patternBlock = patternPool[p]!;
    for (let e = 0; e < entryPool.length; e += 1) {
      const entryBlock = entryPool[e]!;
      for (let x = 0; x < exitPool.length; x += 1) {
        const exitBlock = exitPool[x]!;
        for (let s = 0; s < statePool.length; s += 1) {
          const stateBlock = statePool[s]!;
          for (let r = 0; r < riskPool.length; r += 1) {
            const riskBlock = riskPool[r]!;
            const sessionSecondary =
              sessionPool[(p + e) % sessionPool.length] || primarySessionBlock;

            const blocksByFamily = toBlockMap({
              pattern: [patternBlock],
              session_filter: uniqueStrings([primarySessionBlock, sessionSecondary]),
              state_machine: [stateBlock],
              entry_trigger: [entryBlock],
              exit_rule: [exitBlock],
              risk_rule: [riskBlock],
            });

            const signature = JSON.stringify(blocksByFamily);
            if (seenSignatures.has(signature)) continue;
            seenSignatures.add(signature);

            const referenceStrategyIds = uniqueStrings(
              PRIMITIVE_FAMILIES.flatMap((family) =>
                blocksByFamily[family].flatMap((blockId) => strategyIdsForBlock(blockId)),
              ),
            );
            const usageSupport = referenceStrategyIds.length;
            const supportScore =
              usageSupport +
              (patternBlock === "pattern_adaptive_pattern_router" ? 0.5 : 0);

            rawCandidates.push({
              blocksByFamily,
              referenceStrategyIds,
              supportScore,
              patternBlock,
            });
          }
        }
      }
    }
  }

  // Rank by support score (most v1 strategy backing first), then cap.
  rawCandidates.sort((a, b) => b.supportScore - a.supportScore);
  const capped = rawCandidates.slice(0, maxCandidates);

  const out: ScalpV2CandidateDslSpec[] = [];
  for (const raw of capped) {
    const digest = candidateHash(
      `${params.venue}:${symbol}:${params.entrySessionProfile}:${JSON.stringify(raw.blocksByFamily)}`,
    );
    out.push({
      candidateId: `dsl_${digest.slice(0, 16)}`,
      tuneId: `dsl_${digest.slice(0, 12)}`,
      venue: params.venue,
      symbol,
      entrySessionProfile: params.entrySessionProfile,
      blocksByFamily: raw.blocksByFamily,
      referenceStrategyIds: raw.referenceStrategyIds,
      supportScore: raw.supportScore,
      generatedAtMs,
    });
  }

  return out;
}

export function buildScalpV2ModelGuidedComposerGrid(params: {
  venue: ScalpV2Venue;
  symbol: string;
  entrySessionProfile: ScalpV2Session;
  maxCandidates?: number;
}): ScalpV2ModelGuidedCandidateDslSpec[] {
  const maxCandidates = Math.max(
    1,
    Math.min(2000, Math.floor(params.maxCandidates || 250)),
  );
  const preselectPoolSize = Math.max(
    maxCandidates,
    Math.min(2000, Math.floor(maxCandidates * 2)),
  );
  const blockMap = blockByIdMap();
  const baseGrid = buildScalpV2CandidateDslGrid({
    venue: params.venue,
    symbol: params.symbol,
    entrySessionProfile: params.entrySessionProfile,
    maxCandidates: preselectPoolSize,
  });

  const scored = baseGrid
    .map((row) => ({
      ...row,
      model: buildComposerModelScore({
        dsl: row,
        blockMap,
      }),
    }))
    .sort((a, b) => {
      if (b.model.compositeScore !== a.model.compositeScore) {
        return b.model.compositeScore - a.model.compositeScore;
      }
      if (b.model.confidence !== a.model.confidence) {
        return b.model.confidence - a.model.confidence;
      }
      if (b.supportScore !== a.supportScore) return b.supportScore - a.supportScore;
      return a.candidateId.localeCompare(b.candidateId);
    })
    .slice(0, maxCandidates);

  // Resolve base arm for each candidate and dedup by base arm first.
  // Then multiply each surviving candidate by all timeframe variants.
  const withBaseArm = scored.map((row) => ({
    ...row,
    _baseArm: resolveBaseArmFromPatternBlock(row.blocksByFamily),
  }));

  const seenBaseArms = new Set<ModelGuidedComposerBaseArm>();
  const dedupedByBaseArm = withBaseArm.filter((row) => {
    if (seenBaseArms.has(row._baseArm)) return false;
    seenBaseArms.add(row._baseArm);
    return true;
  });

  // Multiply each surviving base-arm candidate by TF variants × exit rule profiles.
  const expanded: ScalpV2ModelGuidedCandidateDslSpec[] = [];
  for (const row of dedupedByBaseArm) {
    for (const tfVariant of COMPOSER_TIMEFRAME_VARIANTS) {
      for (const exitRuleId of EXIT_RULE_RESEARCH_PROFILES) {
        const armId = `${row._baseArm}_${tfVariant.label}` as ModelGuidedComposerArmId;
        const executionPlan = resolveModelGuidedComposerExecutionPlanFromBlocks(
          row.blocksByFamily,
          tfVariant.label,
        );
        const digest = candidateHash(
          [
            row.venue,
            row.symbol,
            row.entrySessionProfile,
            armId,
            exitRuleId,
            row.model.version,
          ].join(":"),
        );
        expanded.push({
          candidateId: `mdl_${digest.slice(0, 16)}`,
          tuneId: buildModelGuidedComposerTuneId({
            armId: executionPlan.armId,
            digest,
            exitRuleId,
          }),
          venue: row.venue,
          symbol: row.symbol,
          entrySessionProfile: row.entrySessionProfile,
          blocksByFamily: {
            ...row.blocksByFamily,
            exit_rule: [exitRuleId],
          },
          referenceStrategyIds: row.referenceStrategyIds,
          supportScore: row.supportScore,
          generatedAtMs: row.generatedAtMs,
          model: row.model,
        });
      }
    }
  }

  return expanded;
}

export function buildScalpV2SessionCandidateDslGrid(params: {
  venue: ScalpV2Venue;
  symbol: string;
  sessions: ScalpV2Session[];
  maxCandidatesPerSession?: number;
}): ScalpV2CandidateDslSpec[] {
  const sessions = uniqueStrings(params.sessions).filter(
    (row): row is ScalpV2Session =>
      row === "tokyo" ||
      row === "berlin" ||
      row === "newyork" ||
      row === "pacific" ||
      row === "sydney",
  );
  const out: ScalpV2CandidateDslSpec[] = [];
  for (const session of sessions) {
    out.push(
      ...buildScalpV2CandidateDslGrid({
        venue: params.venue,
        symbol: params.symbol,
        entrySessionProfile: session,
        maxCandidates: params.maxCandidatesPerSession,
      }),
    );
  }
  return out;
}

export function strategyPrimitiveCoverageSummary(): {
  strategiesCovered: number;
  strategyCount: number;
  primitiveBlocks: number;
  families: Record<ScalpV2PrimitiveFamily, number>;
} {
  const strategies = listScalpV2CatalogStrategies().map((row) => row.id);
  const refs = listScalpV2StrategyPrimitiveReferences();
  const covered = refs.filter((row) =>
    PRIMITIVE_FAMILIES.some((family) => row.blocksByFamily[family].length > 0),
  ).length;
  const families = PRIMITIVE_FAMILIES.reduce<Record<ScalpV2PrimitiveFamily, number>>(
    (acc, family) => {
      acc[family] = PRIMITIVE_BLOCKS.filter((row) => row.family === family).length;
      return acc;
    },
    {
      pattern: 0,
      session_filter: 0,
      state_machine: 0,
      entry_trigger: 0,
      exit_rule: 0,
      risk_rule: 0,
    },
  );
  return {
    strategiesCovered: covered,
    strategyCount: strategies.length,
    primitiveBlocks: PRIMITIVE_BLOCKS.length,
    families,
  };
}
