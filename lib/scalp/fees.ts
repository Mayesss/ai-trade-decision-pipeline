import {
  DEFAULT_SCALP_VENUE,
  normalizeScalpVenue,
  type ScalpVenue,
} from "./venue";

export interface ScalpVenueFeeSchedule {
  model: "fixed_taker_pct" | "embedded_spread_or_broker";
  takerFeeRate: number | null;
  feeCurrency: "USDT" | "USD" | null;
}

export const SCALP_VENUE_FEE_SCHEDULE: Record<ScalpVenue, ScalpVenueFeeSchedule> =
  {
    bitget: {
      model: "fixed_taker_pct",
      takerFeeRate: 0.0006,
      feeCurrency: "USDT",
    },
  };

export function getScalpVenueFeeSchedule(
  venueRaw: unknown,
  fallbackVenue: ScalpVenue = DEFAULT_SCALP_VENUE,
): ScalpVenueFeeSchedule {
  const venue = normalizeScalpVenue(venueRaw, fallbackVenue);
  return SCALP_VENUE_FEE_SCHEDULE[venue];
}
