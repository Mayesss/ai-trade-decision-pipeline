/**
 * Timeframe-variant strategy registrations.
 *
 * Each base strategy gets M15/M3, M5/M1, and M5/M3 variants.
 * The original strategy singleton remains the default (M15/M3 or M5/M1).
 * This file creates the additional variants using builders for guarded
 * strategies and thin wrappers for non-guarded ones.
 */

import type { ScalpBaseTimeframe, ScalpConfirmTimeframe } from "../types";
import type { ScalpStrategyDefinition } from "./types";

import { buildRegimePullbackM15M3Strategy } from "./regimePullbackM15M3";
import { buildCompressionBreakoutPullbackM15M3Strategy } from "./compressionBreakoutPullbackM15M3";
import { buildOpeningRangeBreakoutRetestM5M1Strategy } from "./openingRangeBreakoutRetestM5M1";
import { buildFailedAuctionExtremeReversalM15M1Strategy } from "./failedAuctionExtremeReversalM15M1";
import { buildTrendDayReaccelerationM15M3Strategy } from "./trendDayReaccelerationM15M3";

// Non-guarded strategies — they accept any timeframe at runtime.
// We create thin wrappers with different preferredBaseTf/preferredConfirmTf.
import { anchoredVwapReversionM15M3Strategy } from "./anchoredVwapReversionM15M3";
import { basisDislocationReversionProxyM15M3Strategy } from "./basisDislocationReversionProxyM15M3";
import { relativeValueSpreadProxyM15M3Strategy } from "./relativeValueSpreadProxyM15M3";
import { sessionSeasonalityBiasM15M3Strategy } from "./sessionSeasonalityBiasM15M3";
import { buildPdhPdlReclaimM15M3Strategy } from "./pdhPdlReclaimM15M3";
import { hssIctM15M3GuardedStrategy } from "./hssIctM15M3Guarded";
import { adaptiveMetaSelectorM15M3Strategy } from "./adaptiveMetaSelectorM15M3";
import { fundingOiExhaustionProxyM15M3Strategy } from "./fundingOiExhaustionProxyM15M3";

type TfVariant = {
  label: string;
  baseTf: ScalpBaseTimeframe;
  confirmTf: ScalpConfirmTimeframe;
};

const TF_VARIANTS: TfVariant[] = [
  { label: "m15_m3", baseTf: "M15", confirmTf: "M3" },
  { label: "m5_m1", baseTf: "M5", confirmTf: "M1" },
  { label: "m5_m3", baseTf: "M5", confirmTf: "M3" },
];

function tfStrategyId(baseId: string, variant: TfVariant): string {
  const tfSuffix = `m${tfNum(variant.baseTf)}_m${tfNum(variant.confirmTf)}`;
  // Replace the TF portion in-place (preserves trailing suffixes like "_guarded")
  if (/_m\d+_m\d+/.test(baseId)) {
    return baseId.replace(/_m\d+_m\d+/, `_${tfSuffix}`);
  }
  return `${baseId}_${tfSuffix}`;
}

function tfNum(tf: string): string {
  return tf.replace(/^M/i, "");
}

function wrapStrategy(
  base: ScalpStrategyDefinition,
  variant: TfVariant,
): ScalpStrategyDefinition {
  const id = tfStrategyId(base.id, variant);
  return {
    id,
    shortName: base.shortName,
    longName: base.longName.replace(
      /\([A-Z]\d+\/[A-Z]\d+\)/,
      `(${variant.baseTf}/${variant.confirmTf})`,
    ),
    preferredBaseTf: variant.baseTf,
    preferredConfirmTf: variant.confirmTf,
    applyPhaseDetectors: base.applyPhaseDetectors,
  };
}

// --- Build all timeframe variants ---

const variants: ScalpStrategyDefinition[] = [];

// 1. Guarded strategies (5) — use builders with overridden TF
for (const v of TF_VARIANTS) {
  variants.push(
    buildRegimePullbackM15M3Strategy({
      id: tfStrategyId("regime_pullback_m15_m3", v),
      shortName: "Regime Pullback",
      longName: `Regime-Filtered Trend Pullback Continuation (${v.baseTf}/${v.confirmTf})`,
      requiredBaseTf: v.baseTf,
      requiredConfirmTf: v.confirmTf,
    }),
  );
  variants.push(
    buildCompressionBreakoutPullbackM15M3Strategy({
      id: tfStrategyId("compression_breakout_pullback_m15_m3", v),
      shortName: "Compression Breakout",
      longName: `Compression Breakout Pullback Continuation (${v.baseTf}/${v.confirmTf})`,
      requiredBaseTf: v.baseTf,
      requiredConfirmTf: v.confirmTf,
    }),
  );
  variants.push(
    buildOpeningRangeBreakoutRetestM5M1Strategy({
      id: tfStrategyId("opening_range_breakout_retest_m5_m1", v),
      shortName: "OR Breakout Retest",
      longName: `Opening Range Breakout Retest (${v.baseTf}/${v.confirmTf})`,
      requiredBaseTf: v.baseTf,
      requiredConfirmTf: v.confirmTf,
    }),
  );
  variants.push(
    buildFailedAuctionExtremeReversalM15M1Strategy({
      id: tfStrategyId("failed_auction_extreme_reversal_m15_m1", v),
      shortName: "Failed Auction",
      longName: `Failed Auction Extreme Reversal (${v.baseTf}/${v.confirmTf})`,
      requiredBaseTf: v.baseTf,
      requiredConfirmTf: v.confirmTf,
    }),
  );
  variants.push(
    buildTrendDayReaccelerationM15M3Strategy({
      id: tfStrategyId("trend_day_reacceleration_m15_m3", v),
      shortName: "Trend Day Reaccel",
      longName: `Trend Day Reacceleration (${v.baseTf}/${v.confirmTf})`,
      requiredBaseTf: v.baseTf,
      requiredConfirmTf: v.confirmTf,
    }),
  );

  // 2. Non-guarded strategies — thin wrappers with different preferred TFs
  variants.push(wrapStrategy(anchoredVwapReversionM15M3Strategy, v));
  variants.push(wrapStrategy(basisDislocationReversionProxyM15M3Strategy, v));
  variants.push(wrapStrategy(relativeValueSpreadProxyM15M3Strategy, v));
  variants.push(wrapStrategy(sessionSeasonalityBiasM15M3Strategy, v));
  variants.push(wrapStrategy(hssIctM15M3GuardedStrategy, v));
  variants.push(wrapStrategy(adaptiveMetaSelectorM15M3Strategy, v));
  variants.push(wrapStrategy(fundingOiExhaustionProxyM15M3Strategy, v));

  // PDH/PDL Reclaim has a builder but no guard
  variants.push(
    buildPdhPdlReclaimM15M3Strategy({
      id: tfStrategyId("pdh_pdl_reclaim_m15_m3", v),
      shortName: "PDH/PDL Reclaim",
      longName: `Previous-Day High/Low Sweep Reclaim (${v.baseTf}/${v.confirmTf})`,
    }),
  );
}

// Dedupe: keep unique by id (builders may produce the same id as the original singleton)
const seen = new Set<string>();
export const TIMEFRAME_VARIANT_STRATEGIES: ScalpStrategyDefinition[] = variants.filter((s) => {
  if (seen.has(s.id)) return false;
  seen.add(s.id);
  return true;
});

// Lookup map keyed by strategy ID
export const TIMEFRAME_VARIANT_STRATEGIES_BY_ID: Record<string, ScalpStrategyDefinition> =
  Object.freeze(
    Object.fromEntries(TIMEFRAME_VARIANT_STRATEGIES.map((s) => [s.id, s])),
  );
