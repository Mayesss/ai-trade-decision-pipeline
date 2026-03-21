import { bitgetScalpVenueAdapter } from "./bitget";
import type { ScalpVenueAdapter } from "./types";
import { normalizeScalpVenue, type ScalpVenue } from "../venue";

export function getScalpVenueAdapter(
  venueRaw: unknown,
): ScalpVenueAdapter {
  const venue = normalizeScalpVenue(venueRaw);
  if (venue === "bitget") return bitgetScalpVenueAdapter;
  throw new Error(
    `scalp_venue_adapter_not_implemented:${String(
      venue,
    )}`,
  );
}

export function isScalpVenueAdapterSupported(venueRaw: unknown): boolean {
  const venue = normalizeScalpVenue(venueRaw);
  return venue === "bitget";
}

export function supportedScalpVenues(): ScalpVenue[] {
  return ["bitget"];
}

export type {
  ScalpVenueAdapter,
  ScalpBrokerPositionSnapshot,
} from "./types";
