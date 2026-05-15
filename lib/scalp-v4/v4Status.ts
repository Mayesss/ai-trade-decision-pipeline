// Strategy "family" extracted from a tune_id prefix. The composer encodes
// strategy + timeframes in the first 4 underscore segments (e.g. `mdl_basis_m5_m1`),
// followed by exit / entry / state-machine / regime-gate variations and a hash.
// Two candidates with the same family but different suffixes are minor parameter
// variations of the SAME strategy idea — they fire on the same conditions, so
// walk-forwarding all of them is redundant.
export function extractScalpV4StrategyFamily(tuneId: string): string {
  const parts = String(tuneId || "").split("_");
  return parts.slice(0, 4).join("_") || "unknown";
}

// Cluster key — candidates with the same key are treated as the same bet.
// Used by walk-forward to cap effort per cluster and by promote-time gates
// to limit concurrent positions on equivalent strategies.
export function buildScalpV4ClusterKey(args: {
  venue: string;
  symbol: string;
  session: string;
  tuneId: string;
  v3TemporalVariantKind?: string | null;
}): string {
  const variant = (args.v3TemporalVariantKind || "baseline").toLowerCase();
  return [
    args.venue.toLowerCase(),
    args.symbol.toUpperCase(),
    args.session.toLowerCase(),
    extractScalpV4StrategyFamily(args.tuneId),
    variant,
  ].join(":");
}

// Per-deployment v4 lifecycle state used by dashboard endpoints.
// Pure helper — no DB / IO. Same labels used in `pages/index.tsx`.
export type ScalpV4DeploymentStatus =
  | "trading"
  | "dormant_wrong_regime"
  | "dormant_no_regime"
  | "pending_walkforward"
  | "eligible_not_promoted"
  | "failed_walkforward"
  | "disabled";

function isRecord(value: unknown): value is Record<string, unknown> {
  return Boolean(value) && typeof value === "object" && !Array.isArray(value);
}

export function classifyScalpV4DeploymentStatus(args: {
  enabled: boolean;
  envelope: Record<string, unknown> | null;
  currentCellId: string | null;
}): ScalpV4DeploymentStatus {
  const envelope = isRecord(args.envelope) ? args.envelope : null;
  const eligible = Boolean(envelope?.eligible);
  const envelopeStatus = String(envelope?.status || "");
  const allowed = Array.isArray(envelope?.allowedCells)
    ? (envelope!.allowedCells as unknown[]).map((c) => String(c || "")).filter(Boolean)
    : [];
  if (!args.enabled) {
    if (eligible) return "eligible_not_promoted";
    return "disabled";
  }
  if (!envelope || Object.keys(envelope).length === 0) return "pending_walkforward";
  if (
    envelopeStatus === "no_passing_cells" ||
    envelopeStatus === "regime_overbroad_auto_rejected" ||
    envelopeStatus === "regime_overbroad_pending_review"
  ) {
    return "failed_walkforward";
  }
  if (!eligible) return "failed_walkforward";
  if (!args.currentCellId) return "dormant_no_regime";
  if (!allowed.includes(args.currentCellId)) return "dormant_wrong_regime";
  return "trading";
}
