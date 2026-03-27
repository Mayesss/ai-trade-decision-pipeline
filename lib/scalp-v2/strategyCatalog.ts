export interface ScalpV2CatalogStrategy {
  id: string;
  shortName: string;
}

const SCALP_V2_CATALOG_STRATEGIES: readonly ScalpV2CatalogStrategy[] =
  Object.freeze([
    { id: "regime_pullback_m15_m3", shortName: "Regime Pullback" },
    {
      id: "compression_breakout_pullback_m15_m3",
      shortName: "Compression Breakout Pullback",
    },
    {
      id: "opening_range_breakout_retest_m5_m1",
      shortName: "Opening Range Breakout Retest",
    },
    {
      id: "failed_auction_extreme_reversal_m15_m1",
      shortName: "Failed Auction Extreme Reversal",
    },
    {
      id: "trend_day_reacceleration_m15_m3",
      shortName: "Trend Day Reacceleration",
    },
    {
      id: "anchored_vwap_reversion_m15_m3",
      shortName: "Anchored VWAP Reversion",
    },
    {
      id: "funding_oi_exhaustion_proxy_m15_m3",
      shortName: "Funding/OI Exhaustion Proxy",
    },
    {
      id: "basis_dislocation_reversion_proxy_m15_m3",
      shortName: "Basis Dislocation Reversion Proxy",
    },
    {
      id: "relative_value_spread_proxy_m15_m3",
      shortName: "Relative Value Spread Proxy",
    },
    {
      id: "session_seasonality_bias_m15_m3",
      shortName: "Session Seasonality Bias",
    },
    { id: "pdh_pdl_reclaim_m15_m3", shortName: "PDH/PDL Reclaim" },
    { id: "hss_ict_m15_m3_guarded", shortName: "HSS/ICT Guarded" },
    {
      id: "adaptive_meta_selector_m15_m3",
      shortName: "Adaptive Meta Selector",
    },
    {
      id: "model_guided_composer_v2",
      shortName: "Model Guided Composer V2",
    },
  ]);

export function listScalpV2CatalogStrategies(): ScalpV2CatalogStrategy[] {
  return SCALP_V2_CATALOG_STRATEGIES.slice();
}

export function listScalpV2CatalogStrategyIds(): string[] {
  return SCALP_V2_CATALOG_STRATEGIES.map((row) => row.id);
}
