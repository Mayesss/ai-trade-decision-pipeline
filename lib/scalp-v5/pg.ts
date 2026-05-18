import { isScalpPgConfigured, scalpPrisma } from "../scalp/pg/client";
import { sql } from "../scalp/pg/sql";
import type { ScalpV2Venue } from "../scalp-v2/types";
import type { ScalpV5CellEvidence } from "./index";

function asRecord(value: unknown): Record<string, unknown> {
  if (!value || typeof value !== "object" || Array.isArray(value)) return {};
  return value as Record<string, unknown>;
}

export interface ScalpV5DeploymentRow {
  deploymentId: string;
  venue: ScalpV2Venue;
  symbol: string;
  strategyId: string;
  tuneId: string;
  entrySessionProfile: string;
  enabled: boolean;
  liveMode: string | null;
  v5Enabled: boolean;
  v5EvaluatedAtMs: number | null;
  // promotion_gate is large; the evaluator only needs to know that the row
  // is currently part of the v3 promotion universe — surface eligibility
  // and a few low-level worker hints if present.
  promotionGate: Record<string, unknown>;
}

// Load deployments that need v5 (re-)evaluation. Scope is every row that
// v3/v2 has promoted at least once (candidate_id IS NOT NULL), regardless
// of current `enabled` state. This lets v5 chew through the full pool of
// stage-C survivors and keep evidence fresh on all of them — when the
// promotion path eventually flips a row to enabled, the v5 gate already
// has data and the entry-time check is non-permissive immediately.
// Within scope, enabled rows are prioritised so the live set stays first
// in line.
export async function loadScalpV5DeploymentsForEvaluation(params: {
  limit?: number;
  staleOlderThanMs?: number;
  nowMs?: number;
  onlyEnabled?: boolean;
  // When set, restrict the result to a disjoint slice of deployments based on
  // a stable hash of deployment_id. Multiple bulk processes can each take a
  // different shardIndex and run in parallel without overlapping rows.
  shardCount?: number;
  shardIndex?: number;
}): Promise<ScalpV5DeploymentRow[]> {
  if (!isScalpPgConfigured()) return [];
  const limit = Math.max(1, Math.min(500, Math.floor(Number(params.limit || 50))));
  const staleOlderThanMs = Math.max(
    60_000,
    Math.floor(Number(params.staleOlderThanMs || 6 * 24 * 60 * 60_000)),
  );
  const onlyEnabled = Boolean(params.onlyEnabled);
  const nowMs = Math.floor(Number(params.nowMs || Date.now()));
  const staleBefore = new Date(nowMs - staleOlderThanMs);
  // Shard normalisation: count >= 2 enables filtering; otherwise unsharded.
  const shardCountRaw = Math.floor(Number(params.shardCount || 1));
  const shardCount = Number.isFinite(shardCountRaw) && shardCountRaw >= 2
    ? Math.max(2, Math.min(128, shardCountRaw))
    : 1;
  const shardIndexRaw = Math.floor(Number(params.shardIndex || 0));
  const shardIndex = shardCount > 1
    ? Math.max(0, Math.min(shardCount - 1, Number.isFinite(shardIndexRaw) ? shardIndexRaw : 0))
    : 0;
  // hashtext() returns a signed int32; mask the sign bit so the modulo is
  // always non-negative without the abs(INT_MIN) edge case.
  const sharded = shardCount > 1;
  const db = scalpPrisma();
  const rows = await db.$queryRaw<Array<{
    deploymentId: string;
    venue: string;
    symbol: string;
    strategyId: string;
    tuneId: string;
    entrySessionProfile: string;
    enabled: boolean;
    liveMode: string | null;
    v5Enabled: boolean;
    v5EvaluatedAt: Date | null;
    promotionGate: unknown;
  }>>(sql`
    SELECT
      deployment_id AS "deploymentId",
      venue,
      symbol,
      strategy_id AS "strategyId",
      tune_id AS "tuneId",
      entry_session_profile AS "entrySessionProfile",
      enabled,
      live_mode AS "liveMode",
      COALESCE(v5_enabled, FALSE) AS "v5Enabled",
      v5_evaluated_at AS "v5EvaluatedAt",
      promotion_gate AS "promotionGate"
    FROM scalp_v2_deployments
    WHERE candidate_id IS NOT NULL
      AND (${onlyEnabled} = FALSE OR enabled = TRUE)
      AND (v5_evaluated_at IS NULL OR v5_evaluated_at < ${staleBefore})
      AND (
        ${sharded} = FALSE
        OR ((hashtext(deployment_id) & 2147483647) % ${shardCount}) = ${shardIndex}
      )
    ORDER BY enabled DESC, v5_evaluated_at ASC NULLS FIRST, updated_at DESC
    LIMIT ${limit};
  `);
  return rows.map((row) => ({
    deploymentId: String(row.deploymentId || "").trim(),
    venue: (String(row.venue || "").toLowerCase() === "capital" ? "capital" : "bitget") as ScalpV2Venue,
    symbol: String(row.symbol || "").trim().toUpperCase(),
    strategyId: String(row.strategyId || "").trim().toLowerCase(),
    tuneId: String(row.tuneId || "").trim().toLowerCase() || "default",
    entrySessionProfile: String(row.entrySessionProfile || "").trim().toLowerCase(),
    enabled: Boolean(row.enabled),
    liveMode: row.liveMode ? String(row.liveMode) : null,
    v5Enabled: Boolean(row.v5Enabled),
    v5EvaluatedAtMs: row.v5EvaluatedAt ? row.v5EvaluatedAt.getTime() : null,
    promotionGate: asRecord(row.promotionGate),
  }));
}

export async function upsertScalpV5DeploymentEvidence(params: {
  deploymentId: string;
  evidence: ScalpV5CellEvidence;
  enabled: boolean;
}): Promise<void> {
  if (!isScalpPgConfigured()) return;
  const db = scalpPrisma();
  await db.$executeRaw(sql`
    UPDATE scalp_v2_deployments
    SET
      v5_cell_evidence = ${JSON.stringify(params.evidence)}::jsonb,
      v5_enabled = ${params.enabled},
      v5_evaluated_at = NOW(),
      updated_at = NOW()
    WHERE deployment_id = ${params.deploymentId};
  `);
}

// Lightweight read for the live entry gate. Returns null when the row has
// not been evaluated yet, which the gate treats as "no signal."
export async function loadScalpV5DeploymentEvidence(params: {
  deploymentId: string;
}): Promise<{
  v5Enabled: boolean;
  v5EvaluatedAtMs: number | null;
  evidence: ScalpV5CellEvidence | null;
} | null> {
  if (!isScalpPgConfigured()) return null;
  const db = scalpPrisma();
  const rows = await db.$queryRaw<Array<{
    v5Enabled: boolean;
    v5EvaluatedAt: Date | null;
    v5CellEvidence: unknown;
  }>>(sql`
    SELECT
      COALESCE(v5_enabled, FALSE) AS "v5Enabled",
      v5_evaluated_at AS "v5EvaluatedAt",
      v5_cell_evidence AS "v5CellEvidence"
    FROM scalp_v2_deployments
    WHERE deployment_id = ${params.deploymentId}
    LIMIT 1;
  `);
  const row = rows[0];
  if (!row) return null;
  const raw = row.v5CellEvidence;
  const evidence = raw && typeof raw === "object" && !Array.isArray(raw)
    ? (raw as unknown as ScalpV5CellEvidence)
    : null;
  return {
    v5Enabled: Boolean(row.v5Enabled),
    v5EvaluatedAtMs: row.v5EvaluatedAt ? row.v5EvaluatedAt.getTime() : null,
    evidence,
  };
}
