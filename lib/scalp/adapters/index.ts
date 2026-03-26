import { capitalScalpVenueAdapter } from "./capital";
import { bitgetScalpVenueAdapter } from "./bitget";
import type { ScalpVenueAdapter } from "./types";
import { normalizeScalpVenue, type ScalpVenue } from "../venue";

export function getScalpVenueAdapter(
  venueRaw: unknown,
): ScalpVenueAdapter {
  const venue = normalizeScalpVenue(venueRaw);
  if (venue === "capital") return capitalScalpVenueAdapter;
  if (venue === "bitget") return bitgetScalpVenueAdapter;
  throw new Error(
    `scalp_venue_adapter_not_implemented:${String(
      venue,
    )}`,
  );
}

export function isScalpVenueAdapterSupported(venueRaw: unknown): boolean {
  const venue = normalizeScalpVenue(venueRaw);
  return venue === "capital" || venue === "bitget";
}

export function supportedScalpVenues(): ScalpVenue[] {
  return ["capital", "bitget"];
}

export type {
  ScalpVenueAdapter,
  ScalpBrokerPositionSnapshot,
} from "./types";
