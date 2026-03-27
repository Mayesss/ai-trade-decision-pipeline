import {
  getScalpVenueAdapter,
  type ScalpVenueAdapter,
} from "../scalp/adapters";

import type { ScalpV2Venue } from "./types";

export function getScalpV2VenueAdapter(venue: ScalpV2Venue): ScalpVenueAdapter {
  return getScalpVenueAdapter(venue);
}
