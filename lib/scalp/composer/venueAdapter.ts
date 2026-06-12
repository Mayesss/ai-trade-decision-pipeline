import {
  getScalpVenueAdapter,
  type ScalpVenueAdapter,
} from "../adapters";

import type { ScalpComposerVenue } from "./types";

export function getScalpComposerVenueAdapter(venue: ScalpComposerVenue): ScalpVenueAdapter {
  return getScalpVenueAdapter(venue);
}
