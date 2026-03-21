export type ScalpVenue = "bitget";

export const DEFAULT_SCALP_VENUE: ScalpVenue = "bitget";

export function normalizeScalpVenue(
  value: unknown,
  fallback: ScalpVenue = DEFAULT_SCALP_VENUE,
): ScalpVenue {
  const normalized = String(value || "")
    .trim()
    .toLowerCase();
  if (normalized === "bitget") return "bitget";
  return fallback;
}

export function parseScalpVenuePrefixedDeploymentId(value: unknown): {
  venue: ScalpVenue;
  deploymentKey: string;
} {
  const raw = String(value || "").trim();
  if (!raw) return { venue: DEFAULT_SCALP_VENUE, deploymentKey: "" };
  const match = raw.match(/^([a-z0-9_-]+):(.*)$/i);
  if (!match) {
    // Legacy unprefixed ids are normalized to current default venue.
    return { venue: DEFAULT_SCALP_VENUE, deploymentKey: raw };
  }
  const prefix = String(match[1] || "")
    .trim()
    .toLowerCase();
  if (prefix !== "bitget") {
    return { venue: DEFAULT_SCALP_VENUE, deploymentKey: raw };
  }
  const venue = normalizeScalpVenue(prefix, DEFAULT_SCALP_VENUE);
  const deploymentKey = String(match[2] || "").trim();
  if (!deploymentKey) return { venue: DEFAULT_SCALP_VENUE, deploymentKey: raw };
  return { venue, deploymentKey };
}

export function formatScalpVenueDeploymentId(
  venue: ScalpVenue,
  deploymentKey: string,
): string {
  const key = String(deploymentKey || "").trim();
  if (!key) return key;
  return `${venue}:${key}`;
}
