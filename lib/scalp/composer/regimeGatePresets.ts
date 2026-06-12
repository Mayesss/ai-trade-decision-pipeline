/**
 * Regime gate presets for model-guided composer tune variants.
 *
 * Gates are based on ATR percentile rank over recent base candles:
 * - compression: lower/mid volatility only
 * - expansion: high volatility only
 */

export type RegimeGateBlockId =
  | "regime_vol_compression"
  | "regime_vol_expansion";

export interface RegimeGateRule {
  atrLookback: number;
  minPercentile: number;
  maxPercentile: number;
}

const REGIME_GATE_PRESETS: Record<RegimeGateBlockId, RegimeGateRule> =
  Object.freeze({
    regime_vol_compression: {
      atrLookback: 200,
      minPercentile: 20,
      maxPercentile: 65,
    },
    regime_vol_expansion: {
      atrLookback: 200,
      minPercentile: 60,
      maxPercentile: 100,
    },
  });

/**
 * Curated research profiles. "off" is implicit (null / no code in tune id).
 */
export const REGIME_GATE_RESEARCH_PROFILES: readonly RegimeGateBlockId[] =
  Object.freeze([
    "regime_vol_compression",
    "regime_vol_expansion",
  ]);

/** Short codes for tuneId encoding. */
export const REGIME_GATE_SHORT_CODES: Record<string, string> = Object.freeze({
  regime_vol_compression: "grc",
  regime_vol_expansion: "gre",
});

const SHORT_CODE_TO_BLOCK: Record<string, RegimeGateBlockId> = Object.freeze(
  Object.fromEntries(
    Object.entries(REGIME_GATE_SHORT_CODES).map(([block, code]) => [
      code,
      block as RegimeGateBlockId,
    ]),
  ) as Record<string, RegimeGateBlockId>,
);

export function resolveRegimeGateBlockFromShortCode(
  code: string | null | undefined,
): RegimeGateBlockId | null {
  if (!code) return null;
  return SHORT_CODE_TO_BLOCK[code.toLowerCase()] || null;
}

export function resolveRegimeGateRule(
  regimeGateId: RegimeGateBlockId | null | undefined,
): RegimeGateRule | null {
  if (!regimeGateId) return null;
  return REGIME_GATE_PRESETS[regimeGateId] || null;
}
