/**
 * State machine presets — maps DSL state_machine block IDs to overrides.
 *
 * At execution time: applied via configOverride to live/shadow deployments.
 * At research time: only applied to variant candidates derived from
 *   enabled (graduated) deployments — not to the base research grid.
 */

export type StateMachineBlockId =
  | "state_consecutive_loss_pause"
  | "state_daily_loss_limit"
  | "state_max_trades_per_day"
  | "state_confirm_ttl_expiry"
  | "state_sweep_reject_timeout"
  | "state_adaptive_confidence_gate";

export interface StateMachineOverrides {
  // Risk / state machine fields (mapped to ScalpStrategyConfig.risk)
  consecutiveLossPauseThreshold?: number;
  consecutiveLossCooldownBars?: number;
  dailyLossLimitR?: number;
  maxTradesPerSymbolPerDay?: number;
  // Confirm / sweep fields (mapped to ScalpStrategyConfig.confirm / sweep)
  confirmTtlMinutes?: number;
  sweepRejectMaxBars?: number;
}

/**
 * Replay-compatible overrides — field names match
 * ScalpReplayRuntimeConfig.strategy (flat keys).
 */
export interface StateMachineReplayOverrides {
  maxTradesPerDay?: number;
  dailyLossLimitR?: number;
  consecutiveLossPauseThreshold?: number;
  consecutiveLossCooldownBars?: number;
  confirmTtlMinutes?: number;
  sweepRejectMaxBars?: number;
}

const STATE_MACHINE_PRESETS: Record<StateMachineBlockId, {
  replay: StateMachineReplayOverrides;
  execution: StateMachineOverrides;
}> = Object.freeze({
  // Pause after consecutive losses — aggressive cooldown.
  state_consecutive_loss_pause: {
    replay: { consecutiveLossPauseThreshold: 2, consecutiveLossCooldownBars: 5 },
    execution: { consecutiveLossPauseThreshold: 2, consecutiveLossCooldownBars: 5 },
  },

  // Tight daily loss limit — stop trading early on bad days.
  state_daily_loss_limit: {
    replay: { dailyLossLimitR: -1.5 },
    execution: { dailyLossLimitR: -1.5 },
  },

  // Limit trade frequency — fewer but higher-conviction entries.
  state_max_trades_per_day: {
    replay: { maxTradesPerDay: 1 },
    execution: { maxTradesPerSymbolPerDay: 1 },
  },

  // Short confirmation TTL — invalidate stale setups quickly.
  state_confirm_ttl_expiry: {
    replay: { confirmTtlMinutes: 20 },
    execution: { confirmTtlMinutes: 20 },
  },

  // Tight sweep rejection — discard sweeps that don't reject fast.
  state_sweep_reject_timeout: {
    replay: { sweepRejectMaxBars: 8 },
    execution: { sweepRejectMaxBars: 8 },
  },

  // Adaptive confidence gate — combine tight loss limit with low frequency.
  state_adaptive_confidence_gate: {
    replay: {
      consecutiveLossPauseThreshold: 1,
      consecutiveLossCooldownBars: 8,
      dailyLossLimitR: -1.0,
      maxTradesPerDay: 1,
    },
    execution: {
      consecutiveLossPauseThreshold: 1,
      consecutiveLossCooldownBars: 8,
      dailyLossLimitR: -1.0,
      maxTradesPerSymbolPerDay: 1,
    },
  },
});

/**
 * All SM profiles for variant generation on enabled deployments.
 * Since enabled deployments are few (typically < 10), we can afford
 * to test every SM variant for each without blowing up research time.
 */
export const STATE_MACHINE_RESEARCH_PROFILES: readonly StateMachineBlockId[] =
  Object.freeze([
    "state_consecutive_loss_pause",
    "state_daily_loss_limit",
    "state_max_trades_per_day",
    "state_confirm_ttl_expiry",
    "state_sweep_reject_timeout",
    "state_adaptive_confidence_gate",
  ]);

/** Short codes for tuneId encoding. */
export const STATE_MACHINE_SHORT_CODES: Record<string, string> = Object.freeze({
  state_consecutive_loss_pause: "sclp",
  state_daily_loss_limit: "sdll",
  state_max_trades_per_day: "smtd",
  state_confirm_ttl_expiry: "scte",
  state_sweep_reject_timeout: "ssrt",
  state_adaptive_confidence_gate: "sacg",
});

const SHORT_CODE_TO_BLOCK: Record<string, StateMachineBlockId> = Object.freeze(
  Object.fromEntries(
    Object.entries(STATE_MACHINE_SHORT_CODES).map(([block, code]) => [
      code,
      block as StateMachineBlockId,
    ]),
  ) as Record<string, StateMachineBlockId>,
);

export function resolveStateMachineBlockFromShortCode(
  code: string | null | undefined,
): StateMachineBlockId | null {
  if (!code) return null;
  return SHORT_CODE_TO_BLOCK[code.toLowerCase()] || null;
}

/**
 * Resolve replay-compatible overrides for research backtesting.
 */
export function resolveStateMachineReplayOverrides(
  stateBlocks: string[] | null | undefined,
): StateMachineReplayOverrides {
  if (!Array.isArray(stateBlocks)) return {};
  for (const block of stateBlocks) {
    const preset = STATE_MACHINE_PRESETS[block as StateMachineBlockId];
    if (preset) return { ...preset.replay };
  }
  return {};
}

/**
 * Resolve execution-time overrides (configOverride).
 */
export function resolveStateMachineOverrides(
  stateBlocks: string[] | null | undefined,
): StateMachineOverrides {
  if (!Array.isArray(stateBlocks)) return {};
  for (const block of stateBlocks) {
    const preset = STATE_MACHINE_PRESETS[block as StateMachineBlockId];
    if (preset) return { ...preset.execution };
  }
  return {};
}
