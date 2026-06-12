import { isScalpPgConfigured, scalpPrisma } from "../pg/client";
import { sql } from "../pg/sql";
import { SESSION_STRUCTURE_COMPOSER_V1_STRATEGY_ID } from "../composer/sessionStructureComposer";
import {
  evaluateDayRobustnessForPromotion,
  resolveDayRobustnessPolicy,
} from "../composer/dayRobustness";
import {
  getScalpV2DefaultRiskProfile,
  getScalpV2RuntimeConfig,
  isScalpV2RuntimeSymbolInScope,
} from "../composer/config";
import { loadScalpV2RuntimeConfig, upsertScalpV2Deployments } from "../composer/db";
import { toDeploymentId } from "../composer/logic";
import type { ScalpV2CandidateStatus, ScalpV2RiskProfile, ScalpV2Session, ScalpV2Venue } from "../composer/types";
import type { ScalpReplayCheckpoint } from "../replay/types";
import { startOfUtcWeekMondayMs } from "../regimes/week";
import {
  evaluateScalpV5PromotionEvidence,
  resolveScalpV5Config,
  SCALP_V5_VERSION,
  type ScalpV5CellEvidence,
  type ScalpV5EvidenceVersion,
  type ScalpV5PromotionMetrics,
  type ScalpV5PromotionThresholds,
} from "./index";

const ONE_WEEK_MS = 7 * 24 * 60 * 60 * 1000;

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
  // riskProfile drives position sizing / loss-pause limits in the replay,
  // so it has to mirror the deployment's actual profile rather than the
  // global default. JSONB on the row; defaults applied when missing.
  riskProfile: ScalpV2RiskProfile;
}

// Default work-lease TTL: the bulk eval processes one row in ~5-30s, so 10
// minutes covers a slow-machine evaluation plus generous overhead for stuck
// candle fetches before a different worker can reclaim a row.
const DEFAULT_V5_LEASE_MS = 10 * 60_000;

// Load deployments that need v5 (re-)evaluation. Scope is every row that
// v3/v2 has promoted at least once (candidate_id IS NOT NULL), regardless
// of current `enabled` state, except candidates explicitly removed from
// scope because their symbol has no candle history.
//
// CLAIM SEMANTICS — this function is a *claim*, not a read. It atomically
// stamps `v5_lease_until = NOW() + leaseMs` on each row it returns via
// UPDATE…RETURNING with FOR UPDATE SKIP LOCKED on the inner SELECT. Concurrent
// callers (hourly cron + N local bulk shards + ad-hoc admin scripts) get
// disjoint row sets without coordination. Failed evaluations don't block the
// queue — the lease auto-expires after leaseMs and another worker reclaims
// the row; successful evaluations clear the lease via
// upsertScalpV5DeploymentEvidence.
//
// Sort priorities, descending importance:
//   1. enabled DESC          — live rows refresh ahead of inactive
//   2. v5_enabled DESC       — known last-week winners refresh first so
//                              promotion-ready evidence stays current
//   3. v5_evaluated_at ASC NULLS FIRST — within a tier, never-evaluated and
//                              oldest-evaluated rows go first
export async function loadScalpV5DeploymentsForEvaluation(params: {
  limit?: number;
  staleOlderThanMs?: number;
  nowMs?: number;
  onlyEnabled?: boolean;
  deploymentIds?: string[];
  // When set, restrict the result to a disjoint slice of deployments based on
  // a stable hash of deployment_id. Multiple bulk processes can each take a
  // different shardIndex and run in parallel without overlapping rows.
  shardCount?: number;
  shardIndex?: number;
  // How long the claim lasts before another worker can reclaim a row.
  // Defaults to 10 minutes. Set lower for short evals, higher to give a slow
  // network more slack.
  leaseMs?: number;
}): Promise<ScalpV5DeploymentRow[]> {
  if (!isScalpPgConfigured()) return [];
  const limit = Math.max(1, Math.min(500, Math.floor(Number(params.limit || 50))));
  const staleOlderThanMs = Math.max(
    60_000,
    Math.floor(Number(params.staleOlderThanMs || 6 * 24 * 60 * 60_000)),
  );
  const leaseMs = Math.max(
    60_000,
    Math.min(60 * 60_000, Math.floor(Number(params.leaseMs || DEFAULT_V5_LEASE_MS))),
  );
  const onlyEnabled = Boolean(params.onlyEnabled);
  const deploymentIds = Array.from(
    new Set(
      (params.deploymentIds || [])
        .map((row) => String(row || "").trim())
        .filter(Boolean),
    ),
  );
  const directDeploymentFilter = deploymentIds.length > 0;
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
  const sharded = shardCount > 1;
  const db = scalpPrisma();

  // Atomic claim: the inner SELECT acquires row locks via FOR UPDATE SKIP
  // LOCKED (concurrent transactions get disjoint rows), the outer UPDATE
  // stamps the lease, and RETURNING gives us the full row payload in one
  // round-trip. hashtext() & 2147483647 masks the sign bit so the modulo is
  // non-negative without the abs(INT_MIN) edge case.
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
    riskProfile: unknown;
  }>>(sql`
    UPDATE scalp_v2_deployments AS t
    SET v5_lease_until = NOW() + (${leaseMs} * INTERVAL '1 millisecond'),
        updated_at = NOW()
    FROM (
      SELECT d.deployment_id
      FROM scalp_v2_deployments d
      WHERE d.candidate_id IS NOT NULL
        AND d.strategy_id = ${SESSION_STRUCTURE_COMPOSER_V1_STRATEGY_ID}
        AND (${directDeploymentFilter} = FALSE OR d.deployment_id = ANY(${deploymentIds}::text[]))
        AND NOT EXISTS (
          SELECT 1
          FROM scalp_v2_candidates c
          WHERE c.id = d.candidate_id
            AND c.metadata_json->'scopeRemoval'->>'reason' = 'bitget_symbol_removed_no_candles'
        )
        AND (${onlyEnabled} = FALSE OR d.enabled = TRUE)
        AND (${directDeploymentFilter} = TRUE OR d.v5_evaluated_at IS NULL OR d.v5_evaluated_at < ${staleBefore})
        AND (d.v5_lease_until IS NULL OR d.v5_lease_until < NOW())
        AND (
          ${sharded} = FALSE
          OR ((hashtext(d.deployment_id) & 2147483647) % ${shardCount}) = ${shardIndex}
        )
      ORDER BY d.enabled DESC, d.v5_enabled DESC, d.v5_evaluated_at ASC NULLS FIRST, d.updated_at DESC
      LIMIT ${limit}
      FOR UPDATE SKIP LOCKED
    ) AS picked
    WHERE t.deployment_id = picked.deployment_id
    RETURNING
      t.deployment_id AS "deploymentId",
      t.venue,
      t.symbol,
      t.strategy_id AS "strategyId",
      t.tune_id AS "tuneId",
      t.entry_session_profile AS "entrySessionProfile",
      t.enabled,
      t.live_mode AS "liveMode",
      COALESCE(t.v5_enabled, FALSE) AS "v5Enabled",
      t.v5_evaluated_at AS "v5EvaluatedAt",
      t.promotion_gate AS "promotionGate",
      t.risk_profile AS "riskProfile";
  `);
  const defaultRiskProfile = getScalpV2DefaultRiskProfile();
  return rows.map((row) => {
    const rpRaw = asRecord(row.riskProfile);
    const rp: ScalpV2RiskProfile = {
      riskPerTradePct:
        Number.isFinite(Number(rpRaw.riskPerTradePct))
          ? Number(rpRaw.riskPerTradePct)
          : defaultRiskProfile.riskPerTradePct,
      maxOpenPositionsPerSymbol:
        Number.isFinite(Number(rpRaw.maxOpenPositionsPerSymbol))
          ? Math.max(1, Math.floor(Number(rpRaw.maxOpenPositionsPerSymbol)))
          : defaultRiskProfile.maxOpenPositionsPerSymbol,
      autoPauseDailyR:
        Number.isFinite(Number(rpRaw.autoPauseDailyR))
          ? Number(rpRaw.autoPauseDailyR)
          : defaultRiskProfile.autoPauseDailyR,
      autoPause30dR:
        Number.isFinite(Number(rpRaw.autoPause30dR))
          ? Number(rpRaw.autoPause30dR)
          : defaultRiskProfile.autoPause30dR,
    };
    return {
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
      riskProfile: rp,
    };
  });
}

// Mark every v5 evidence row as needing re-evaluation.
//
// Two modes:
//
// - mode="stale" (default for weekly Sunday rollover): only NULLs
//   v5_evaluated_at so the rows become eligible for the loader's staleness
//   filter again. Evidence + checkpoint stay in place — the dispatcher
//   sees them on the next evaluation, validates the prerequisites, and
//   runs the cheap incremental path (1 week of replay instead of 12) when
//   they line up. Use after a normal weekly rollover.
//
// - mode="full" (use after a behavior change that invalidates checkpoints,
//   e.g. classifier version bump, evaluator logic change, DSL semantics
//   change): wipes evidence + v5_enabled + checkpoint. Every row will go
//   through a full 12-week replay on its next evaluation. The entry gate
//   goes permissive (V5_CELL_EVIDENCE_MISSING) on each row until its
//   re-eval lands fresh evidence — brief degradation, but unavoidable
//   when the underlying data shape changes.
//
// Either way, the `enabled` flag on the deployment is NOT touched — v5
// auto-promoted rows stay live.
export async function invalidateAllScalpV5Evidence(params: {
  onlyEnabled?: boolean;
  mode?: "full" | "stale";
} = {}): Promise<{ invalidated: number; mode: "full" | "stale" }> {
  const mode: "full" | "stale" = params.mode === "full" ? "full" : "stale";
  if (!isScalpPgConfigured()) return { invalidated: 0, mode };
  const onlyEnabled = Boolean(params.onlyEnabled);
  const db = scalpPrisma();
  if (mode === "stale") {
    const rows = await db.$queryRaw<Array<{ deploymentId: string }>>(sql`
      UPDATE scalp_v2_deployments
      SET v5_evaluated_at = NULL,
          v5_lease_until = NULL,
          updated_at = NOW()
      WHERE candidate_id IS NOT NULL
        AND (${onlyEnabled} = FALSE OR enabled = TRUE)
        AND v5_evaluated_at IS NOT NULL
      RETURNING deployment_id AS "deploymentId";
    `);
    return { invalidated: rows.length, mode };
  }
  // mode === "full"
  const rows = await db.$queryRaw<Array<{ deploymentId: string }>>(sql`
    UPDATE scalp_v2_deployments
    SET v5_cell_evidence = NULL,
        v5_enabled = FALSE,
        v5_evaluated_at = NULL,
        v5_lease_until = NULL,
        v5_replay_checkpoint = NULL,
        updated_at = NOW()
    WHERE candidate_id IS NOT NULL
      AND (${onlyEnabled} = FALSE OR enabled = TRUE)
      AND v5_evaluated_at IS NOT NULL
    RETURNING deployment_id AS "deploymentId";
  `);
  return { invalidated: rows.length, mode };
}

export interface ScalpV5AdvancementBreakdown {
  // Evidence row is missing entirely (never evaluated, refilled, or
  // wiped by a force=full path). Requires a full 12-week replay.
  missingEvidence: number;
  // Evidence shape/schema version is older than SCALP_V5_VERSION. The
  // dispatcher's incremental prereqs reject these, so re-eval = full replay.
  versionStale: number;
  // Evidence was built against a different classifier version than the one
  // currently configured. Cells from the old classifier may not even be
  // comparable to the new one, so full replay is required.
  classifierStale: number;
  // Evidence is current-version, current-classifier, but its holdoutToMs is
  // older than the new week boundary. The dispatcher's incremental path
  // can advance these in 1 week of replay each.
  weekStale: number;
  // Evidence already covers the new week. No work required.
  alreadyFresh: number;
  total: number;
}

// Smart Sunday queue: returns the deployment IDs whose v5 evidence has NOT
// yet been advanced to the new holdout boundary, along with a breakdown of
// WHY each row needs work. Replaces the blanket
// invalidateAllScalpV5Evidence({ mode: "stale" }) NULLing of v5_evaluated_at:
//
//   - Idempotent re-runs: a row that's already been advanced this run will
//     not appear in the returned ID list, so a crashed-and-restarted Sunday
//     script skips its already-completed work.
//   - Honest dashboards: `breakdown.alreadyFresh` reflects rows that genuinely
//     don't need work, instead of "all 2500 are stale because we just NULLed
//     them."
//   - Same compute as the previous flow in the steady-state case: each row
//     still needs exactly one incremental replay to slide the holdout
//     forward by a week. The savings are in correctness/observability, not
//     CPU.
//
// The reason classification is mutually exclusive in priority order:
//   missingEvidence > versionStale > classifierStale > weekStale > alreadyFresh
// — so a row with a stale version is reported as versionStale even if it
// also happens to be week-stale (the version mismatch is the more useful
// signal because it forces a full replay regardless).
//
// Holdout boundary is computed the same way evaluateScalpV5ForDeployment
// computes it: Sunday UTC advances the boundary to the upcoming Monday so
// the just-completed week becomes the 12th holdout week; Mon-Sat use the
// standard week-start boundary.
export async function selectScalpV5DeploymentsNeedingAdvancement(params: {
  nowMs?: number;
  evidenceVersion?: ScalpV5EvidenceVersion;
  classifierVersion?: string;
  holdoutWeeks?: number;
  // Restrict to live rows only. The Sunday flow leaves this false so v5
  // evidence stays current across the entire judgeable pool.
  onlyEnabled?: boolean;
} = {}): Promise<{
  newHoldoutToMs: number;
  newHoldoutFromMs: number;
  evidenceVersion: ScalpV5EvidenceVersion;
  classifierVersion: string;
  deploymentIds: string[];
  breakdown: ScalpV5AdvancementBreakdown;
}> {
  const cfg = resolveScalpV5Config();
  const evidenceVersion: ScalpV5EvidenceVersion = params.evidenceVersion || SCALP_V5_VERSION;
  const classifierVersion = String(
    params.classifierVersion || cfg.classifierVersion,
  ).trim() || cfg.classifierVersion;
  const holdoutWeeks = Math.max(
    1,
    Math.min(52, Math.floor(Number(params.holdoutWeeks ?? cfg.holdoutWeeks))),
  );
  const nowMs = Math.floor(Number(params.nowMs || Date.now()));
  const weekStart = startOfUtcWeekMondayMs(nowMs);
  const newHoldoutToMs =
    new Date(nowMs).getUTCDay() === 0 ? weekStart + ONE_WEEK_MS : weekStart;
  const newHoldoutFromMs = newHoldoutToMs - holdoutWeeks * ONE_WEEK_MS;
  const onlyEnabled = Boolean(params.onlyEnabled);
  const emptyBreakdown: ScalpV5AdvancementBreakdown = {
    missingEvidence: 0,
    versionStale: 0,
    classifierStale: 0,
    weekStale: 0,
    alreadyFresh: 0,
    total: 0,
  };
  if (!isScalpPgConfigured()) {
    return {
      newHoldoutToMs,
      newHoldoutFromMs,
      evidenceVersion,
      classifierVersion,
      deploymentIds: [],
      breakdown: emptyBreakdown,
    };
  }
  const db = scalpPrisma();
  const rows = await db.$queryRaw<Array<{ deploymentId: string; reason: string }>>(sql`
    SELECT
      d.deployment_id AS "deploymentId",
      CASE
        WHEN d.v5_cell_evidence IS NULL OR d.v5_evaluated_at IS NULL
          THEN 'missing_evidence'
        WHEN COALESCE(d.v5_cell_evidence->>'version', '') <> ${evidenceVersion}
          THEN 'version_stale'
        WHEN COALESCE(d.v5_cell_evidence->>'classifierVersion', '') <> ${classifierVersion}
          THEN 'classifier_stale'
        WHEN COALESCE((d.v5_cell_evidence->>'holdoutToMs')::bigint, 0) < ${newHoldoutToMs}
          THEN 'week_stale'
        ELSE 'already_fresh'
      END AS reason
    FROM scalp_v2_deployments d
    WHERE d.candidate_id IS NOT NULL
      AND d.strategy_id = ${SESSION_STRUCTURE_COMPOSER_V1_STRATEGY_ID}
      AND (${onlyEnabled} = FALSE OR d.enabled = TRUE)
      AND NOT EXISTS (
        SELECT 1
        FROM scalp_v2_candidates c
        WHERE c.id = d.candidate_id
          AND c.metadata_json->'scopeRemoval'->>'reason' = 'bitget_symbol_removed_no_candles'
      );
  `);
  const deploymentIds: string[] = [];
  const breakdown: ScalpV5AdvancementBreakdown = { ...emptyBreakdown, total: rows.length };
  for (const row of rows) {
    const id = String(row.deploymentId || "").trim();
    switch (row.reason) {
      case "missing_evidence":
        breakdown.missingEvidence += 1;
        if (id) deploymentIds.push(id);
        break;
      case "version_stale":
        breakdown.versionStale += 1;
        if (id) deploymentIds.push(id);
        break;
      case "classifier_stale":
        breakdown.classifierStale += 1;
        if (id) deploymentIds.push(id);
        break;
      case "week_stale":
        breakdown.weekStale += 1;
        if (id) deploymentIds.push(id);
        break;
      default:
        breakdown.alreadyFresh += 1;
        break;
    }
  }
  return {
    newHoldoutToMs,
    newHoldoutFromMs,
    evidenceVersion,
    classifierVersion,
    deploymentIds,
    breakdown,
  };
}

// Explicitly release a lease without writing evidence. Useful when an
// evaluation aborts early (e.g. no candles) and the caller wants the row to
// be available for retry sooner than the leaseMs TTL.
export async function releaseScalpV5DeploymentLease(params: {
  deploymentId: string;
}): Promise<void> {
  if (!isScalpPgConfigured()) return;
  const db = scalpPrisma();
  await db.$executeRaw(sql`
    UPDATE scalp_v2_deployments
    SET v5_lease_until = NULL,
        updated_at = NOW()
    WHERE deployment_id = ${params.deploymentId}
      AND v5_lease_until IS NOT NULL;
  `);
}

// Competitive cull: retire the worst N% of judgeable composer deployments by
// total NetR. Complementary to trim-tail (which retires chronic non-eligibles
// regardless of merit). Together they form the weekly retirement pipeline:
//
//   trim-tail:   "you've had 28 days and never passed v5" → retire
//   cull-bottom: "of those judgeable enough to rank, you're in the worst N%
//                AND your total NetR is negative" → retire
//
// Strict protections — we never retire:
//   - currently live rows (enabled = TRUE)
//   - rows young enough to still be accumulating evidence (< graceDays)
//   - rows with too few trades to rank fairly (< minTrades)
//   - rows with positive total NetR (a "worst" row that's still profitable
//     stays in the pool; only proven losers get culled)
//   - rows whose candidate was scope-removed (no candles) — they're already
//     functionally retired
//   - rows that would shrink the pool below minPoolSize
//
// Retirement action mirrors trim-tail: candidate_id = NULL drops the row
// from the v5 evaluation queue, enabled = FALSE stops new entries, and
// evidence + checkpoint are cleared. Reversible: future research that
// regenerates the same DSL combo creates a fresh deployment row.
//
// Returns the threshold NetR (the worst-performing kept row's score) so
// the cron log shows "we retired everything below -3.5R" — useful operator
// signal for tuning percentToRetire over time.
export async function cullBottomPerformersScalpV5Deployments(params: {
  percentToRetire?: number;
  graceDays?: number;
  minTrades?: number;
  minPoolSize?: number;
  maxRetireAbs?: number | null;
  dryRun?: boolean;
} = {}): Promise<{
  retired: number;
  deploymentIds: string[];
  eligibleCount: number;
  poolSize: number;
  thresholdNetR: number | null;
  dryRun: boolean;
}> {
  const percentToRetire = Math.max(0, Math.min(1, Number(params.percentToRetire ?? 0.15)));
  const graceDays = Math.max(7, Math.floor(Number(params.graceDays ?? 28)));
  const minTrades = Math.max(0, Math.floor(Number(params.minTrades ?? 30)));
  const minPoolSize = Math.max(0, Math.floor(Number(params.minPoolSize ?? 1500)));
  const maxRetireAbs =
    typeof params.maxRetireAbs === "number" && params.maxRetireAbs > 0
      ? Math.floor(params.maxRetireAbs)
      : null;
  const dryRun = Boolean(params.dryRun);
  if (!isScalpPgConfigured()) {
    return { retired: 0, deploymentIds: [], eligibleCount: 0, poolSize: 0, thresholdNetR: null, dryRun };
  }
  const db = scalpPrisma();
  const ageBefore = new Date(Date.now() - graceDays * 24 * 60 * 60_000);

  // One SQL: walk every composer row's v5 evidence cells, compute totals,
  // filter to eligible-for-cull, return ordered by net_r ASC.
  // Mirrors the scope-removal filter that the rest of the v5 path uses so
  // we don't double-act on already-retired candidates.
  const rows = await db.$queryRaw<Array<{
    deploymentId: string;
    totalNetR: string;   // numeric → string at the wire
    totalTrades: number;
  }>>(sql`
    WITH totals AS (
      SELECT
        d.deployment_id,
        d.enabled,
        d.created_at,
        (
          SELECT COALESCE(SUM((value->>'netR')::numeric), 0)
          FROM jsonb_each(COALESCE(d.v5_cell_evidence->'cells', '{}'::jsonb))
        ) AS total_net_r,
        (
          SELECT COALESCE(SUM((value->>'trades')::int), 0)
          FROM jsonb_each(COALESCE(d.v5_cell_evidence->'cells', '{}'::jsonb))
        ) AS total_trades
      FROM scalp_v2_deployments d
      WHERE d.candidate_id IS NOT NULL
        AND d.strategy_id = ${SESSION_STRUCTURE_COMPOSER_V1_STRATEGY_ID}
        AND d.v5_evaluated_at IS NOT NULL
        AND NOT EXISTS (
          SELECT 1
          FROM scalp_v2_candidates c
          WHERE c.id = d.candidate_id
            AND c.metadata_json->'scopeRemoval'->>'reason' = 'bitget_symbol_removed_no_candles'
        )
    )
    SELECT
      deployment_id AS "deploymentId",
      total_net_r::text AS "totalNetR",
      total_trades::int AS "totalTrades"
    FROM totals
    WHERE NOT COALESCE(enabled, FALSE)
      AND total_trades >= ${minTrades}
      AND created_at < ${ageBefore}
      AND total_net_r < 0
    ORDER BY total_net_r ASC;
  `);

  // Separate count of total active pool (for the floor check). Doesn't need
  // evidence walk; just the raw deployment count.
  const poolRows = await db.$queryRaw<Array<{ count: bigint }>>(sql`
    SELECT COUNT(*)::bigint AS count
    FROM scalp_v2_deployments d
    WHERE d.candidate_id IS NOT NULL
      AND d.strategy_id = ${SESSION_STRUCTURE_COMPOSER_V1_STRATEGY_ID}
      AND NOT EXISTS (
        SELECT 1
        FROM scalp_v2_candidates c
        WHERE c.id = d.candidate_id
          AND c.metadata_json->'scopeRemoval'->>'reason' = 'bitget_symbol_removed_no_candles'
      );
  `);
  const poolSize = Number(poolRows[0]?.count ?? 0);
  const eligibleCount = rows.length;

  // Compute the retirement budget:
  //   - percent of eligible
  //   - capped at maxRetireAbs (your weekly intake target — never retire
  //     faster than the v2 generator can refill)
  //   - capped by minPoolSize floor (never shrink active pool below this)
  let targetCount = Math.floor(eligibleCount * percentToRetire);
  if (maxRetireAbs !== null) targetCount = Math.min(targetCount, maxRetireAbs);
  const maxByFloor = Math.max(0, poolSize - minPoolSize);
  targetCount = Math.min(targetCount, maxByFloor, eligibleCount);

  const toRetire = rows.slice(0, targetCount);
  const deploymentIds = toRetire.map((r) => String(r.deploymentId || "").trim()).filter(Boolean);
  const thresholdNetR =
    toRetire.length > 0 ? Number(toRetire[toRetire.length - 1].totalNetR) : null;

  if (dryRun || deploymentIds.length === 0) {
    return { retired: 0, deploymentIds, eligibleCount, poolSize, thresholdNetR, dryRun };
  }

  await db.$executeRaw(sql`
    UPDATE scalp_v2_deployments
    SET candidate_id        = NULL,
        enabled             = FALSE,
        v5_evaluated_at     = NULL,
        v5_lease_until      = NULL,
        v5_cell_evidence    = NULL,
        v5_enabled          = FALSE,
        v5_replay_checkpoint = NULL,
        retired_at          = NOW(),
        updated_at          = NOW()
    WHERE deployment_id = ANY(${deploymentIds}::text[]);
  `);
  return { retired: deploymentIds.length, deploymentIds, eligibleCount, poolSize, thresholdNetR, dryRun };
}

// Trim the long tail of consistently-failing deployments. A deployment is
// "trim-eligible" when ALL of these hold:
//   - candidate_id IS NOT NULL                 (still in the active pool)
//   - created_at < NOW() - stalenessDays       (in the pool long enough to
//                                                have been evaluated several
//                                                times — 28 days covers ~4
//                                                Sunday rollovers)
//   - v5_evaluated_at IS NOT NULL              (v5 has evaluated it at least
//                                                once; otherwise we don't yet
//                                                know if it's bad)
//   - COALESCE(v5_enabled, FALSE) = FALSE      (v5 has consistently failed to
//                                                find a positive-expectancy
//                                                cell)
//   - COALESCE(enabled, FALSE) = FALSE         (not currently trading live —
//                                                we never disrupt live trading
//                                                via the trim path)
//   - last_promoted_at IS NULL                 (has never been live-promoted;
//                                                extra safety, lets us avoid
//                                                retiring rows that someone
//                                                manually promoted and later
//                                                de-promoted)
//
// Trim action mirrors the dedupe path: candidate_id = NULL drops the row from
// the v5 evaluation queue and the live execute scope. v5_evaluated_at is
// cleared so the dashboard doesn't count it as "evaluated." Evidence and
// checkpoint are NULLed to reclaim storage — the row is now considered
// proven dead.
//
// PERMANENT BAN: alongside clearing candidate_id, this stamps retired_at
// = NOW(). All three v5 refill pool queries (stagec / mutation /
// exploration) exclude any (venue, symbol, strategy_id, tune_id,
// entry_session_profile) tuple whose deployment row has retired_at IS
// NOT NULL. So if v2 research generates a fresh candidate with the same
// tuple, refill skips it — we already paid the compute proving the
// strategy doesn't work and won't pay it again. Manual reset requires
// clearing retired_at directly in SQL.
export async function retireConsistentlyFailingScalpV5Deployments(params: {
  stalenessDays?: number;
  dryRun?: boolean;
} = {}): Promise<{ retired: number; deploymentIds: string[]; dryRun: boolean }> {
  const dryRun = Boolean(params.dryRun);
  // Minimum 7 days. Going lower than that risks retiring candidates that
  // simply haven't been v5-evaluated yet on their natural staleness cycle.
  // Default 28 days = 4 weeks ≈ 4 Sunday rollovers.
  const stalenessDays = Math.max(7, Math.floor(Number(params.stalenessDays ?? 28)));
  if (!isScalpPgConfigured()) return { retired: 0, deploymentIds: [], dryRun };
  const db = scalpPrisma();
  const stalenessInterval = stalenessDays * 24 * 60 * 60_000;
  const ageBefore = new Date(Date.now() - stalenessInterval);
  if (dryRun) {
    const rows = await db.$queryRaw<Array<{ deploymentId: string }>>(sql`
      SELECT deployment_id AS "deploymentId"
      FROM scalp_v2_deployments
      WHERE candidate_id IS NOT NULL
        AND created_at < ${ageBefore}
        AND v5_evaluated_at IS NOT NULL
        AND COALESCE(v5_enabled, FALSE) = FALSE
        AND COALESCE(enabled, FALSE) = FALSE
        AND last_promoted_at IS NULL
      ORDER BY created_at ASC;
    `);
    return {
      retired: rows.length,
      deploymentIds: rows.map((r) => String(r.deploymentId || "").trim()).filter(Boolean),
      dryRun,
    };
  }
  const rows = await db.$queryRaw<Array<{ deploymentId: string }>>(sql`
    UPDATE scalp_v2_deployments
    SET candidate_id        = NULL,
        enabled             = FALSE,
        v5_evaluated_at     = NULL,
        v5_lease_until      = NULL,
        v5_cell_evidence    = NULL,
        v5_enabled          = FALSE,
        v5_replay_checkpoint = NULL,
        retired_at          = NOW(),
        updated_at          = NOW()
    WHERE candidate_id IS NOT NULL
      AND created_at < ${ageBefore}
      AND v5_evaluated_at IS NOT NULL
      AND COALESCE(v5_enabled, FALSE) = FALSE
      AND COALESCE(enabled, FALSE) = FALSE
      AND last_promoted_at IS NULL
    RETURNING deployment_id AS "deploymentId";
  `);
  return {
    retired: rows.length,
    deploymentIds: rows.map((r) => String(r.deploymentId || "").trim()).filter(Boolean),
    dryRun,
  };
}

export type ScalpV5StageCRefillCandidate = {
  id: number;
  venue: ScalpV2Venue;
  symbol: string;
  strategyId: string;
  tuneId: string;
  entrySessionProfile: ScalpV2Session;
  score: number;
  status: ScalpV2CandidateStatus;
  metadata: Record<string, unknown>;
  stageCNetR: number;
  stageCTrades: number;
  deploymentId: string;
  alreadyActive?: boolean;
  scopeRemoved?: boolean;
  inRuntimeScope?: boolean;
};

function normalizeVenue(value: unknown): ScalpV2Venue {
  return String(value || "").trim().toLowerCase() === "capital" ? "capital" : "bitget";
}

function normalizeSession(value: unknown): ScalpV2Session {
  const normalized = String(value || "").trim().toLowerCase();
  if (
    normalized === "tokyo" ||
    normalized === "berlin" ||
    normalized === "newyork" ||
    normalized === "pacific" ||
    normalized === "sydney"
  ) {
    return normalized;
  }
  return "berlin";
}

function normalizeStatus(value: unknown): ScalpV2CandidateStatus {
  const normalized = String(value || "").trim().toLowerCase();
  if (
    normalized === "discovered" ||
    normalized === "evaluated" ||
    normalized === "promoted" ||
    normalized === "rejected"
  ) {
    return normalized;
  }
  return "discovered";
}

function normalizeSymbol(value: unknown): string {
  return String(value || "")
    .trim()
    .toUpperCase()
    .replace(/[^A-Z0-9._-]/g, "");
}

function numericOrNull(value: unknown): number | null {
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

function resolveStageCRecord(metadata: Record<string, unknown>): Record<string, unknown> {
  const worker = asRecord(metadata.worker);
  const fromWorker = asRecord(worker.stageC);
  if (Object.keys(fromWorker).length > 0) return fromWorker;
  const fromWorkerSnake = asRecord(worker.stage_c);
  if (Object.keys(fromWorkerSnake).length > 0) return fromWorkerSnake;
  const direct = asRecord(metadata.stageC);
  if (Object.keys(direct).length > 0) return direct;
  return asRecord(metadata.stage_c);
}

export function rankScalpV5StageCRefillCandidates(params: {
  candidates: ScalpV5StageCRefillCandidate[];
  targetNewSeats?: number;
  minStageCNetR?: number;
  minStageCTrades?: number;
}): ScalpV5StageCRefillCandidate[] {
  const targetNewSeats = Math.max(0, Math.floor(Number(params.targetNewSeats ?? 500)));
  const minStageCNetR = Number.isFinite(Number(params.minStageCNetR)) ? Number(params.minStageCNetR) : 4;
  const minStageCTrades = Math.max(0, Math.floor(Number(params.minStageCTrades ?? 30)));
  if (targetNewSeats <= 0) return [];
  return (params.candidates || [])
    .filter((row) => !row.alreadyActive)
    .filter((row) => !row.scopeRemoved)
    .filter((row) => row.inRuntimeScope !== false)
    .filter((row) => row.status === "evaluated" || row.status === "rejected")
    .filter((row) => Number(row.stageCNetR) >= minStageCNetR)
    .filter((row) => Number(row.stageCTrades) >= minStageCTrades)
    .sort((a, b) =>
      Number(b.stageCNetR) - Number(a.stageCNetR) ||
      Number(b.stageCTrades) - Number(a.stageCTrades) ||
      Number(b.score) - Number(a.score) ||
      a.deploymentId.localeCompare(b.deploymentId),
    )
    .slice(0, targetNewSeats);
}

export async function listScalpV5StageCRankedRefillCandidates(params: {
  targetNewSeats?: number;
  minStageCNetR?: number;
  minStageCTrades?: number;
  fetchLimit?: number;
} = {}): Promise<ScalpV5StageCRefillCandidate[]> {
  if (!isScalpPgConfigured()) return [];
  const targetNewSeats = Math.max(0, Math.floor(Number(params.targetNewSeats ?? 500)));
  if (targetNewSeats <= 0) return [];
  const minStageCNetR = Number.isFinite(Number(params.minStageCNetR)) ? Number(params.minStageCNetR) : 4;
  const minStageCTrades = Math.max(0, Math.floor(Number(params.minStageCTrades ?? 30)));
  const fetchLimit = Math.max(
    targetNewSeats,
    Math.min(20_000, Math.floor(Number(params.fetchLimit ?? targetNewSeats * 10))),
  );
  const runtime = await loadScalpV2RuntimeConfig();
  const db = scalpPrisma();
  const rows = await db.$queryRaw<Array<{
    id: number | bigint;
    venue: string;
    symbol: string;
    strategyId: string;
    tuneId: string;
    entrySessionProfile: string;
    score: number | string | null;
    status: string;
    metadata: unknown;
    stageCNetR: string | number | null;
    stageCTrades: number | bigint | null;
  }>>(sql`
    WITH scored AS (
      SELECT
        c.id,
        c.venue,
        c.symbol,
        c.strategy_id AS "strategyId",
        c.tune_id AS "tuneId",
        c.entry_session_profile AS "entrySessionProfile",
        c.score,
        c.status,
        c.metadata_json AS metadata,
        COALESCE(
          CASE WHEN (c.metadata_json->'worker'->'stageC'->>'netR') ~ '^-?[0-9]+(\\.[0-9]+)?$'
            THEN (c.metadata_json->'worker'->'stageC'->>'netR')::numeric END,
          CASE WHEN (c.metadata_json->'stageC'->>'netR') ~ '^-?[0-9]+(\\.[0-9]+)?$'
            THEN (c.metadata_json->'stageC'->>'netR')::numeric END,
          0
        ) AS stage_c_net_r,
        COALESCE(
          CASE WHEN (c.metadata_json->'worker'->'stageC'->>'trades') ~ '^[0-9]+$'
            THEN (c.metadata_json->'worker'->'stageC'->>'trades')::int END,
          CASE WHEN (c.metadata_json->'stageC'->>'trades') ~ '^[0-9]+$'
            THEN (c.metadata_json->'stageC'->>'trades')::int END,
          0
        ) AS stage_c_trades
      FROM scalp_v2_candidates c
      WHERE c.status IN ('evaluated', 'rejected')
        AND c.strategy_id = ${SESSION_STRUCTURE_COMPOSER_V1_STRATEGY_ID}
        AND COALESCE(c.metadata_json->'scopeRemoval'->>'reason', '') <> 'bitget_symbol_removed_no_candles'
        AND NOT EXISTS (
          SELECT 1
          FROM scalp_v2_deployments d
          WHERE d.candidate_id = c.id
        )
        -- Permanent ban: skip any tuple whose previous deployment was
        -- retired (trim-tail or cull-bottom). The hard-ban lives on
        -- scalp_v2_deployments.retired_at; clear that column manually if
        -- you ever want to give a retired tuple a second chance.
        AND NOT EXISTS (
          SELECT 1
          FROM scalp_v2_deployments d
          WHERE d.venue = c.venue
            AND d.symbol = c.symbol
            AND d.strategy_id = c.strategy_id
            AND d.tune_id = c.tune_id
            AND d.entry_session_profile = c.entry_session_profile
            AND d.retired_at IS NOT NULL
        )
    )
    SELECT
      id,
      venue,
      symbol,
      "strategyId",
      "tuneId",
      "entrySessionProfile",
      score,
      status,
      metadata,
      stage_c_net_r::text AS "stageCNetR",
      stage_c_trades::int AS "stageCTrades"
    FROM scored
    WHERE stage_c_net_r >= ${minStageCNetR}
      AND stage_c_trades >= ${minStageCTrades}
    ORDER BY stage_c_net_r DESC, stage_c_trades DESC, score DESC NULLS LAST
    LIMIT ${fetchLimit};
  `);
  const candidates = rows.map((row) => {
    const venue = normalizeVenue(row.venue);
    const symbol = normalizeSymbol(row.symbol);
    const strategyId = String(row.strategyId || "").trim().toLowerCase();
    const tuneId = String(row.tuneId || "default").trim().toLowerCase() || "default";
    const entrySessionProfile = normalizeSession(row.entrySessionProfile);
    const metadata = asRecord(row.metadata);
    const stageC = resolveStageCRecord(metadata);
    const stageCNetR = numericOrNull(row.stageCNetR) ?? numericOrNull(stageC.netR) ?? 0;
    const stageCTrades = Math.max(0, Math.floor(numericOrNull(row.stageCTrades) ?? numericOrNull(stageC.trades) ?? 0));
    const deploymentId = toDeploymentId({
      venue,
      symbol,
      strategyId,
      tuneId,
      session: entrySessionProfile,
    });
    return {
      id: Math.floor(Number(row.id) || 0),
      venue,
      symbol,
      strategyId,
      tuneId,
      entrySessionProfile,
      score: numericOrNull(row.score) ?? 0,
      status: normalizeStatus(row.status),
      metadata,
      stageCNetR,
      stageCTrades,
      deploymentId,
      scopeRemoved: asRecord(metadata.scopeRemoval).reason === "bitget_symbol_removed_no_candles",
      inRuntimeScope: isScalpV2RuntimeSymbolInScope({
        runtime,
        venue,
        symbol,
        includeLiveSeeds: true,
      }),
    } satisfies ScalpV5StageCRefillCandidate;
  });
  return rankScalpV5StageCRefillCandidates({
    candidates,
    targetNewSeats,
    minStageCNetR,
    minStageCTrades,
  });
}

// Refill bucket: which sub-pool this candidate was drawn from. Stored on
// the new deployment's promotion_gate so survival/promotion rates can be
// tracked per-bucket over future Sundays — the operator signal that says
// "is the 60/25/15 split the right ratio for THIS market regime?"
export type ScalpV5RefillBucket = "stagec" | "mutation" | "exploration";

export type ScalpV5MutationRefillCandidate = ScalpV5StageCRefillCandidate & {
  // Which winner-similarity axis caught this candidate:
  //   strategy = shares (strategyId, tuneId) with at least one v5 winner
  //   regime   = shares (venue, symbol, entrySessionProfile)
  //   both     = both axes
  matchBasis: "strategy" | "regime" | "both";
};

// Pool B — winner-mutations. Returns candidates that share at least one
// identity axis with a current v5 winner (active, fresh evidence). Intent:
// when a strategy is proven on a particular (symbol, session), try the same
// strategy elsewhere (strategy-fixed) AND try other strategies on the same
// (symbol, session) (regime-fixed).
//
// "Mutation" here is identity-level, not DSL-element-level. A DSL-one-slot-
// diff query would be more precise but materially more complex; this looser
// definition catches the bulk of the value at a fraction of the SQL.
//
// Candidates that ARE already the winner row (same candidate_id deployed)
// are excluded by the NOT EXISTS deployment filter — only OTHER candidates
// sharing the identity slip through.
export async function listScalpV5WinnerMutationRefillCandidates(params: {
  targetNewSeats?: number;
  // Minimum stage-C netR floor. Set lower than the stage-C pool's 4R bar by
  // default — mutations are a "second chance" pool, the strict bar already
  // gated Pool A. 0 = "at least broke even globally."
  minStageCNetR?: number;
  minStageCTrades?: number;
  // How fresh a winner's evidence has to be to count for mutation seeding.
  // Defaults to 14 days (matches promotion staleness window).
  winnerFreshOlderThanMs?: number;
  fetchLimit?: number;
  nowMs?: number;
  // Cap mutation seats per symbol. Without this, the ORDER BY stage_c_net_r
  // DESC scoops up entire tune-variant clusters from one symbol (e.g. 19/50
  // mutation seats on AAVEUSDT in the 2026-05-24 dry-run). The cap leaves
  // room for cross-symbol discovery without sacrificing the strongest
  // candidate per symbol. Default 2; set higher to let dense clusters
  // through, set to 0 to disable.
  maxPerSymbol?: number;
} = {}): Promise<ScalpV5MutationRefillCandidate[]> {
  if (!isScalpPgConfigured()) return [];
  const targetNewSeats = Math.max(0, Math.floor(Number(params.targetNewSeats ?? 125)));
  if (targetNewSeats <= 0) return [];
  const minStageCNetR = Number.isFinite(Number(params.minStageCNetR))
    ? Number(params.minStageCNetR)
    : 0;
  const minStageCTrades = Math.max(0, Math.floor(Number(params.minStageCTrades ?? 15)));
  const winnerFreshOlderThanMs = Math.max(
    60_000,
    Math.floor(Number(params.winnerFreshOlderThanMs ?? 14 * 24 * 60 * 60_000)),
  );
  const nowMs = Math.floor(Number(params.nowMs || Date.now()));
  const staleBefore = new Date(nowMs - winnerFreshOlderThanMs);
  const fetchLimit = Math.max(
    targetNewSeats,
    Math.min(20_000, Math.floor(Number(params.fetchLimit ?? targetNewSeats * 10))),
  );
  const maxPerSymbolRaw = Number(params.maxPerSymbol);
  // 0 = disabled (no cap). Otherwise clamp to a sane range.
  const maxPerSymbol =
    Number.isFinite(maxPerSymbolRaw) && maxPerSymbolRaw >= 0
      ? Math.min(1000, Math.floor(maxPerSymbolRaw))
      : 2;
  const symbolCapEnabled = maxPerSymbol > 0;
  const runtime = await loadScalpV2RuntimeConfig();
  const db = scalpPrisma();
  const rows = await db.$queryRaw<Array<{
    id: number | bigint;
    venue: string;
    symbol: string;
    strategyId: string;
    tuneId: string;
    entrySessionProfile: string;
    score: number | string | null;
    status: string;
    metadata: unknown;
    stageCNetR: string | number | null;
    stageCTrades: number | bigint | null;
    matchBasis: string;
  }>>(sql`
    WITH winners AS (
      SELECT DISTINCT
        d.venue,
        d.symbol,
        d.strategy_id,
        d.tune_id,
        d.entry_session_profile
      FROM scalp_v2_deployments d
      WHERE d.candidate_id IS NOT NULL
        AND COALESCE(d.v5_enabled, FALSE) = TRUE
        AND d.v5_evaluated_at IS NOT NULL
        AND d.v5_evaluated_at >= ${staleBefore}
    ),
    scored AS (
      SELECT
        c.id,
        c.venue,
        c.symbol,
        c.strategy_id AS "strategyId",
        c.tune_id AS "tuneId",
        c.entry_session_profile AS "entrySessionProfile",
        c.score,
        c.status,
        c.metadata_json AS metadata,
        COALESCE(
          CASE WHEN (c.metadata_json->'worker'->'stageC'->>'netR') ~ '^-?[0-9]+(\\.[0-9]+)?$'
            THEN (c.metadata_json->'worker'->'stageC'->>'netR')::numeric END,
          CASE WHEN (c.metadata_json->'stageC'->>'netR') ~ '^-?[0-9]+(\\.[0-9]+)?$'
            THEN (c.metadata_json->'stageC'->>'netR')::numeric END,
          0
        ) AS stage_c_net_r,
        COALESCE(
          CASE WHEN (c.metadata_json->'worker'->'stageC'->>'trades') ~ '^[0-9]+$'
            THEN (c.metadata_json->'worker'->'stageC'->>'trades')::int END,
          CASE WHEN (c.metadata_json->'stageC'->>'trades') ~ '^[0-9]+$'
            THEN (c.metadata_json->'stageC'->>'trades')::int END,
          0
        ) AS stage_c_trades
      FROM scalp_v2_candidates c
      WHERE c.status IN ('evaluated', 'rejected')
        AND c.strategy_id = ${SESSION_STRUCTURE_COMPOSER_V1_STRATEGY_ID}
        AND COALESCE(c.metadata_json->'scopeRemoval'->>'reason', '') <> 'bitget_symbol_removed_no_candles'
        AND NOT EXISTS (
          SELECT 1 FROM scalp_v2_deployments d WHERE d.candidate_id = c.id
        )
        -- Permanent ban: see listScalpV5StageCRankedRefillCandidates.
        AND NOT EXISTS (
          SELECT 1 FROM scalp_v2_deployments d
          WHERE d.venue = c.venue
            AND d.symbol = c.symbol
            AND d.strategy_id = c.strategy_id
            AND d.tune_id = c.tune_id
            AND d.entry_session_profile = c.entry_session_profile
            AND d.retired_at IS NOT NULL
        )
    ),
    matched AS (
      SELECT
        s.*,
        EXISTS (
          SELECT 1 FROM winners w
          WHERE w.strategy_id = s."strategyId" AND w.tune_id = s."tuneId"
        ) AS strategy_match,
        EXISTS (
          SELECT 1 FROM winners w
          WHERE w.venue = s.venue
            AND w.symbol = s.symbol
            AND w.entry_session_profile = s."entrySessionProfile"
        ) AS regime_match
      FROM scored s
    ),
    eligible AS (
      SELECT
        m.*,
        CASE
          WHEN strategy_match AND regime_match THEN 'both'
          WHEN strategy_match THEN 'strategy'
          WHEN regime_match THEN 'regime'
          ELSE 'none'
        END AS match_basis,
        -- Rank candidates within the same venue+symbol so a per-symbol cap
        -- can be applied below. Tiebreakers mirror the global ORDER BY so
        -- the cap keeps the strongest-per-symbol rows.
        ROW_NUMBER() OVER (
          PARTITION BY m.venue, m.symbol
          ORDER BY m.stage_c_net_r DESC, m.stage_c_trades DESC, m.score DESC NULLS LAST
        ) AS per_symbol_rank
      FROM matched m
      WHERE (strategy_match OR regime_match)
        AND stage_c_net_r >= ${minStageCNetR}
        AND stage_c_trades >= ${minStageCTrades}
    )
    SELECT
      id,
      venue,
      symbol,
      "strategyId",
      "tuneId",
      "entrySessionProfile",
      score,
      status,
      metadata,
      stage_c_net_r::text AS "stageCNetR",
      stage_c_trades::int AS "stageCTrades",
      match_basis AS "matchBasis"
    FROM eligible
    WHERE ${symbolCapEnabled} = FALSE OR per_symbol_rank <= ${maxPerSymbol}
    ORDER BY stage_c_net_r DESC, stage_c_trades DESC, score DESC NULLS LAST
    LIMIT ${fetchLimit};
  `);
  const mapped: ScalpV5MutationRefillCandidate[] = rows.map((row) => {
    const venue = normalizeVenue(row.venue);
    const symbol = normalizeSymbol(row.symbol);
    const strategyId = String(row.strategyId || "").trim().toLowerCase();
    const tuneId = String(row.tuneId || "default").trim().toLowerCase() || "default";
    const entrySessionProfile = normalizeSession(row.entrySessionProfile);
    const metadata = asRecord(row.metadata);
    const stageC = resolveStageCRecord(metadata);
    const stageCNetR = numericOrNull(row.stageCNetR) ?? numericOrNull(stageC.netR) ?? 0;
    const stageCTrades = Math.max(
      0,
      Math.floor(numericOrNull(row.stageCTrades) ?? numericOrNull(stageC.trades) ?? 0),
    );
    const matchBasisRaw = String(row.matchBasis || "");
    const matchBasis: ScalpV5MutationRefillCandidate["matchBasis"] =
      matchBasisRaw === "both" || matchBasisRaw === "strategy" || matchBasisRaw === "regime"
        ? matchBasisRaw
        : "strategy";
    return {
      id: Math.floor(Number(row.id) || 0),
      venue,
      symbol,
      strategyId,
      tuneId,
      entrySessionProfile,
      score: numericOrNull(row.score) ?? 0,
      status: normalizeStatus(row.status),
      metadata,
      stageCNetR,
      stageCTrades,
      deploymentId: toDeploymentId({
        venue,
        symbol,
        strategyId,
        tuneId,
        session: entrySessionProfile,
      }),
      scopeRemoved: asRecord(metadata.scopeRemoval).reason === "bitget_symbol_removed_no_candles",
      inRuntimeScope: isScalpV2RuntimeSymbolInScope({
        runtime,
        venue,
        symbol,
        includeLiveSeeds: true,
      }),
      matchBasis,
    };
  });
  return mapped
    .filter((row) => !row.scopeRemoved)
    .filter((row) => row.inRuntimeScope !== false)
    .slice(0, targetNewSeats);
}

// Pool C — exploration. Globally-marginal candidates (positive netR but
// didn't clear the stage-C bar) that v5's regime-cell bucketing might still
// rescue: a strategy with mediocre global expectancy can still be excellent
// in one specific regime cell, which is exactly what v5's evidence shape
// surfaces.
export async function listScalpV5ExplorationRefillCandidates(params: {
  targetNewSeats?: number;
  // Lower bound on stage-C netR. 0 = "at least broke even globally."
  minStageCNetR?: number;
  // Upper bound on stage-C netR. Defaults to the stage-C bar (4) so Pool C
  // never overlaps Pool A — every candidate is in exactly one pool.
  maxStageCNetR?: number;
  // Lower trade-count bar. Defaults lower than stage-C's 30 so candidates
  // with thinner samples but plausible distributions still get a v5 chance.
  minStageCTrades?: number;
  fetchLimit?: number;
} = {}): Promise<ScalpV5StageCRefillCandidate[]> {
  if (!isScalpPgConfigured()) return [];
  const targetNewSeats = Math.max(0, Math.floor(Number(params.targetNewSeats ?? 75)));
  if (targetNewSeats <= 0) return [];
  const minStageCNetR = Number.isFinite(Number(params.minStageCNetR))
    ? Number(params.minStageCNetR)
    : 0;
  const maxStageCNetR = Number.isFinite(Number(params.maxStageCNetR))
    ? Number(params.maxStageCNetR)
    : 4;
  const minStageCTrades = Math.max(0, Math.floor(Number(params.minStageCTrades ?? 15)));
  const fetchLimit = Math.max(
    targetNewSeats,
    Math.min(20_000, Math.floor(Number(params.fetchLimit ?? targetNewSeats * 10))),
  );
  const runtime = await loadScalpV2RuntimeConfig();
  const db = scalpPrisma();
  const rows = await db.$queryRaw<Array<{
    id: number | bigint;
    venue: string;
    symbol: string;
    strategyId: string;
    tuneId: string;
    entrySessionProfile: string;
    score: number | string | null;
    status: string;
    metadata: unknown;
    stageCNetR: string | number | null;
    stageCTrades: number | bigint | null;
  }>>(sql`
    WITH scored AS (
      SELECT
        c.id,
        c.venue,
        c.symbol,
        c.strategy_id AS "strategyId",
        c.tune_id AS "tuneId",
        c.entry_session_profile AS "entrySessionProfile",
        c.score,
        c.status,
        c.metadata_json AS metadata,
        COALESCE(
          CASE WHEN (c.metadata_json->'worker'->'stageC'->>'netR') ~ '^-?[0-9]+(\\.[0-9]+)?$'
            THEN (c.metadata_json->'worker'->'stageC'->>'netR')::numeric END,
          CASE WHEN (c.metadata_json->'stageC'->>'netR') ~ '^-?[0-9]+(\\.[0-9]+)?$'
            THEN (c.metadata_json->'stageC'->>'netR')::numeric END,
          0
        ) AS stage_c_net_r,
        COALESCE(
          CASE WHEN (c.metadata_json->'worker'->'stageC'->>'trades') ~ '^[0-9]+$'
            THEN (c.metadata_json->'worker'->'stageC'->>'trades')::int END,
          CASE WHEN (c.metadata_json->'stageC'->>'trades') ~ '^[0-9]+$'
            THEN (c.metadata_json->'stageC'->>'trades')::int END,
          0
        ) AS stage_c_trades
      FROM scalp_v2_candidates c
      WHERE c.status IN ('evaluated', 'rejected')
        AND c.strategy_id = ${SESSION_STRUCTURE_COMPOSER_V1_STRATEGY_ID}
        AND COALESCE(c.metadata_json->'scopeRemoval'->>'reason', '') <> 'bitget_symbol_removed_no_candles'
        AND NOT EXISTS (
          SELECT 1 FROM scalp_v2_deployments d WHERE d.candidate_id = c.id
        )
        -- Permanent ban: see listScalpV5StageCRankedRefillCandidates.
        AND NOT EXISTS (
          SELECT 1 FROM scalp_v2_deployments d
          WHERE d.venue = c.venue
            AND d.symbol = c.symbol
            AND d.strategy_id = c.strategy_id
            AND d.tune_id = c.tune_id
            AND d.entry_session_profile = c.entry_session_profile
            AND d.retired_at IS NOT NULL
        )
    )
    SELECT
      id,
      venue,
      symbol,
      "strategyId",
      "tuneId",
      "entrySessionProfile",
      score,
      status,
      metadata,
      stage_c_net_r::text AS "stageCNetR",
      stage_c_trades::int AS "stageCTrades"
    FROM scored
    WHERE stage_c_net_r >= ${minStageCNetR}
      AND stage_c_net_r < ${maxStageCNetR}
      AND stage_c_trades >= ${minStageCTrades}
    ORDER BY stage_c_net_r DESC, stage_c_trades DESC, score DESC NULLS LAST
    LIMIT ${fetchLimit};
  `);
  return rows
    .map((row): ScalpV5StageCRefillCandidate => {
      const venue = normalizeVenue(row.venue);
      const symbol = normalizeSymbol(row.symbol);
      const strategyId = String(row.strategyId || "").trim().toLowerCase();
      const tuneId = String(row.tuneId || "default").trim().toLowerCase() || "default";
      const entrySessionProfile = normalizeSession(row.entrySessionProfile);
      const metadata = asRecord(row.metadata);
      const stageC = resolveStageCRecord(metadata);
      const stageCNetR = numericOrNull(row.stageCNetR) ?? numericOrNull(stageC.netR) ?? 0;
      const stageCTrades = Math.max(
        0,
        Math.floor(numericOrNull(row.stageCTrades) ?? numericOrNull(stageC.trades) ?? 0),
      );
      return {
        id: Math.floor(Number(row.id) || 0),
        venue,
        symbol,
        strategyId,
        tuneId,
        entrySessionProfile,
        score: numericOrNull(row.score) ?? 0,
        status: normalizeStatus(row.status),
        metadata,
        stageCNetR,
        stageCTrades,
        deploymentId: toDeploymentId({
          venue,
          symbol,
          strategyId,
          tuneId,
          session: entrySessionProfile,
        }),
        scopeRemoved: asRecord(metadata.scopeRemoval).reason === "bitget_symbol_removed_no_candles",
        inRuntimeScope: isScalpV2RuntimeSymbolInScope({
          runtime,
          venue,
          symbol,
          includeLiveSeeds: true,
        }),
      };
    })
    .filter((row) => !row.scopeRemoved)
    .filter((row) => row.inRuntimeScope !== false)
    .slice(0, targetNewSeats);
}

export async function refillScalpV5DeploymentsFromStageCRankedCandidates(params: {
  targetNewSeats?: number;
  minStageCNetR?: number;
  minStageCTrades?: number;
  dryRun?: boolean;
} = {}): Promise<{
  dryRun: boolean;
  selected: number;
  upserted: number;
  deploymentIds: string[];
  candidates: ScalpV5StageCRefillCandidate[];
}> {
  const dryRun = Boolean(params.dryRun);
  const candidates = await listScalpV5StageCRankedRefillCandidates({
    targetNewSeats: params.targetNewSeats,
    minStageCNetR: params.minStageCNetR,
    minStageCTrades: params.minStageCTrades,
  });
  const deploymentIds = candidates.map((row) => row.deploymentId);
  if (dryRun || candidates.length === 0 || !isScalpPgConfigured()) {
    return { dryRun, selected: candidates.length, upserted: 0, deploymentIds, candidates };
  }
  const riskProfile = getScalpV2DefaultRiskProfile();
  const refilledAtMs = Date.now();
  await upsertScalpV2Deployments({
    rows: candidates.map((row) => {
      const metadata = asRecord(row.metadata);
      const worker = asRecord(metadata.worker);
      return {
        candidateId: row.id,
        venue: row.venue,
        symbol: row.symbol,
        strategyId: row.strategyId,
        tuneId: row.tuneId,
        entrySessionProfile: row.entrySessionProfile,
        enabled: false,
        liveMode: "shadow",
        riskProfile,
        promotionGate: {
          eligible: false,
          reason: "v5_stagec_refill_pending_v5_evaluation",
          source: "v5_sunday_stagec_refill",
          refilledAtMs,
          refill: {
            candidateId: row.id,
            stageCNetR: row.stageCNetR,
            stageCTrades: row.stageCTrades,
            minStageCNetR: Number.isFinite(Number(params.minStageCNetR)) ? Number(params.minStageCNetR) : 4,
            minStageCTrades: Math.max(0, Math.floor(Number(params.minStageCTrades ?? 30))),
          },
          worker,
          holdout: asRecord(worker.holdout),
          v3TemporalFilter: asRecord(metadata.v3TemporalFilter),
          dsl: asRecord(metadata.researchDsl || metadata.dsl),
        },
      };
    }),
  });
  const db = scalpPrisma();
  await db.$executeRaw(sql`
    UPDATE scalp_v2_deployments
    SET
      v5_cell_evidence = NULL,
      v5_enabled = FALSE,
      v5_evaluated_at = NULL,
      v5_lease_until = NULL,
      v5_replay_checkpoint = NULL,
      updated_at = NOW()
    WHERE deployment_id = ANY(${deploymentIds}::text[]);
  `);
  return {
    dryRun,
    selected: candidates.length,
    upserted: candidates.length,
    deploymentIds,
    candidates,
  };
}

// Mixed-source refill: pulls from three sub-pools at a 60/25/15 split.
//
//   stagec (60%) — listScalpV5StageCRankedRefillCandidates: globally-strong
//   mutation (25%) — listScalpV5WinnerMutationRefillCandidates: identity
//                    neighbors of current v5 winners
//   exploration (15%) — listScalpV5ExplorationRefillCandidates: globally-
//                       marginal candidates that may have a regime cell
//                       where they excel
//
// Quotas are computed by floor() so they may under-fill by 1-2 seats on
// odd targets; remainders go to the stagec bucket. If a bucket is short
// (e.g., no mutations found because no current winners), the deficit
// spills to the next bucket in priority order (stagec > mutation > explore)
// so we always fill up to targetNewSeats when supply allows.
//
// Dedupe: a candidate could appear in multiple pools (e.g., a winner-
// identity neighbor that also passes the stage-C bar). We honor the FIRST
// pool that picks it — stagec wins ties — and tag its bucket accordingly
// so future operators can read survival rates per bucket from
// promotion_gate.refill.bucket.
export async function refillScalpV5DeploymentsMixed(params: {
  targetNewSeats?: number;
  stagecFraction?: number;       // default 0.60
  mutationFraction?: number;     // default 0.25
  explorationFraction?: number;  // default 0.15
  minStageCNetR?: number;        // stage-C pool floor (default 4)
  minStageCTrades?: number;      // stage-C pool floor (default 30)
  mutationMinStageCNetR?: number;   // default 0
  mutationMinStageCTrades?: number; // default 15
  explorationMinStageCNetR?: number; // default 0
  explorationMaxStageCNetR?: number; // default 4
  explorationMinStageCTrades?: number; // default 15
  dryRun?: boolean;
} = {}): Promise<{
  dryRun: boolean;
  targetNewSeats: number;
  quotas: { stagec: number; mutation: number; exploration: number };
  selected: { stagec: number; mutation: number; exploration: number };
  upserted: number;
  deploymentIds: string[];
  sampleByBucket: {
    stagec: string[];
    mutation: string[];
    exploration: string[];
  };
}> {
  const dryRun = Boolean(params.dryRun);
  const targetNewSeats = Math.max(0, Math.floor(Number(params.targetNewSeats ?? 500)));
  const emptyResult = {
    dryRun,
    targetNewSeats,
    quotas: { stagec: 0, mutation: 0, exploration: 0 },
    selected: { stagec: 0, mutation: 0, exploration: 0 },
    upserted: 0,
    deploymentIds: [] as string[],
    sampleByBucket: { stagec: [], mutation: [], exploration: [] },
  };
  if (targetNewSeats <= 0) return emptyResult;

  const stagecFraction = clampFraction(params.stagecFraction, 0.60);
  const mutationFraction = clampFraction(params.mutationFraction, 0.25);
  const explorationFraction = clampFraction(params.explorationFraction, 0.15);
  // Fractions are advisory; spill logic below handles a short bucket by
  // promoting the deficit to the next priority. We don't normalise/clamp
  // the sum: operators can intentionally pass 0.5/0.2/0.1 to under-fill.
  const stagecQuota = Math.floor(targetNewSeats * stagecFraction);
  const mutationQuota = Math.floor(targetNewSeats * mutationFraction);
  const explorationQuota = Math.floor(targetNewSeats * explorationFraction);
  const initialSum = stagecQuota + mutationQuota + explorationQuota;
  // Remainder from floor() goes to stagec (the safest pool).
  const stagecQuotaAdjusted = stagecQuota + Math.max(0, targetNewSeats - initialSum);
  const quotas = {
    stagec: stagecQuotaAdjusted,
    mutation: mutationQuota,
    exploration: explorationQuota,
  };

  // Load each pool with up to `targetNewSeats` candidates — NOT just its
  // own quota. The orchestrator below allocates `quotas.{bucket}` for the
  // primary fill and may need to draw additional rows for spill when a
  // sibling pool comes up short. If we capped each pool at its primary
  // quota, spill would have nothing to fall back to.
  const fetchLimit = Math.max(targetNewSeats * 5, 1000);
  const stagecPool = await listScalpV5StageCRankedRefillCandidates({
    targetNewSeats,
    fetchLimit,
    minStageCNetR: params.minStageCNetR,
    minStageCTrades: params.minStageCTrades,
  });
  const mutationPool = await listScalpV5WinnerMutationRefillCandidates({
    targetNewSeats,
    fetchLimit,
    minStageCNetR: params.mutationMinStageCNetR,
    minStageCTrades: params.mutationMinStageCTrades,
  });
  const explorationPool = await listScalpV5ExplorationRefillCandidates({
    targetNewSeats,
    fetchLimit,
    minStageCNetR: params.explorationMinStageCNetR,
    maxStageCNetR: params.explorationMaxStageCNetR,
    minStageCTrades: params.explorationMinStageCTrades,
  });

  // Allocate with dedupe in priority order: stagec → mutation → exploration.
  // A candidate appearing in multiple pools is tagged by the FIRST pool that
  // picks it (because that's the strongest signal for "why we took it"
  // — stage-C strength beats winner-similarity beats globally-marginal).
  type Picked = {
    candidate: ScalpV5StageCRefillCandidate | ScalpV5MutationRefillCandidate;
    bucket: ScalpV5RefillBucket;
    matchBasis?: ScalpV5MutationRefillCandidate["matchBasis"];
  };
  const seen = new Set<string>();
  const picked: Picked[] = [];
  const sampleByBucket = {
    stagec: [] as string[],
    mutation: [] as string[],
    exploration: [] as string[],
  };
  const selected = { stagec: 0, mutation: 0, exploration: 0 };
  function take(
    pool: Array<ScalpV5StageCRefillCandidate | ScalpV5MutationRefillCandidate>,
    bucket: ScalpV5RefillBucket,
    quota: number,
  ): number {
    let taken = 0;
    for (const candidate of pool) {
      if (taken >= quota) break;
      const id = candidate.deploymentId;
      if (!id || seen.has(id)) continue;
      seen.add(id);
      picked.push({
        candidate,
        bucket,
        matchBasis: "matchBasis" in candidate ? candidate.matchBasis : undefined,
      });
      if (sampleByBucket[bucket].length < 25) sampleByBucket[bucket].push(id);
      taken += 1;
    }
    selected[bucket] += taken;
    return taken;
  }
  const stagecTaken = take(stagecPool, "stagec", quotas.stagec);
  const mutationTaken = take(mutationPool, "mutation", quotas.mutation);
  const explorationTaken = take(explorationPool, "exploration", quotas.exploration);

  // Spill logic: any unfilled budget from a short pool gets reallocated
  // to the remaining pools in priority order. take()'s quota parameter is
  // "take up to N MORE candidates from this pool"; we pass the remaining
  // deficit directly so the spill never overshoots the global budget.
  let remaining = targetNewSeats - (stagecTaken + mutationTaken + explorationTaken);
  if (remaining > 0) {
    const spillOrder: Array<{
      pool: Array<ScalpV5StageCRefillCandidate | ScalpV5MutationRefillCandidate>;
      bucket: ScalpV5RefillBucket;
    }> = [
      { pool: stagecPool, bucket: "stagec" },
      { pool: mutationPool, bucket: "mutation" },
      { pool: explorationPool, bucket: "exploration" },
    ];
    for (const { pool, bucket } of spillOrder) {
      if (remaining <= 0) break;
      const taken = take(pool, bucket, remaining);
      remaining -= taken;
    }
  }

  const deploymentIds = picked.map((p) => p.candidate.deploymentId);
  if (dryRun || picked.length === 0 || !isScalpPgConfigured()) {
    return {
      ...emptyResult,
      quotas,
      selected,
      upserted: 0,
      deploymentIds,
      sampleByBucket,
    };
  }

  const riskProfile = getScalpV2DefaultRiskProfile();
  const refilledAtMs = Date.now();
  await upsertScalpV2Deployments({
    rows: picked.map(({ candidate, bucket, matchBasis }) => {
      const metadata = asRecord(candidate.metadata);
      const worker = asRecord(metadata.worker);
      return {
        candidateId: candidate.id,
        venue: candidate.venue,
        symbol: candidate.symbol,
        strategyId: candidate.strategyId,
        tuneId: candidate.tuneId,
        entrySessionProfile: candidate.entrySessionProfile,
        enabled: false,
        liveMode: "shadow",
        riskProfile,
        promotionGate: {
          eligible: false,
          reason: `v5_${bucket}_refill_pending_v5_evaluation`,
          source: `v5_sunday_${bucket}_refill`,
          refilledAtMs,
          refill: {
            bucket,
            matchBasis: matchBasis ?? null,
            candidateId: candidate.id,
            stageCNetR: candidate.stageCNetR,
            stageCTrades: candidate.stageCTrades,
          },
          worker,
          holdout: asRecord(worker.holdout),
          v3TemporalFilter: asRecord(metadata.v3TemporalFilter),
          dsl: asRecord(metadata.researchDsl || metadata.dsl),
        },
      };
    }),
  });
  const db = scalpPrisma();
  await db.$executeRaw(sql`
    UPDATE scalp_v2_deployments
    SET
      v5_cell_evidence = NULL,
      v5_enabled = FALSE,
      v5_evaluated_at = NULL,
      v5_lease_until = NULL,
      v5_replay_checkpoint = NULL,
      updated_at = NOW()
    WHERE deployment_id = ANY(${deploymentIds}::text[]);
  `);
  return {
    dryRun,
    targetNewSeats,
    quotas,
    selected,
    upserted: picked.length,
    deploymentIds,
    sampleByBucket,
  };
}

function clampFraction(value: unknown, fallback: number): number {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) return fallback;
  if (parsed < 0) return 0;
  if (parsed > 1) return 1;
  return parsed;
}

export async function getScalpV5EvaluationQueueStats(params: {
  staleOlderThanMs?: number;
  nowMs?: number;
} = {}): Promise<{
  active: number;
  missingEvidence: number;
  stale: number;
  leased: number;
}> {
  if (!isScalpPgConfigured()) return { active: 0, missingEvidence: 0, stale: 0, leased: 0 };
  const staleOlderThanMs = Math.max(
    60_000,
    Math.floor(Number(params.staleOlderThanMs || 6 * 24 * 60 * 60_000)),
  );
  const nowMs = Math.floor(Number(params.nowMs || Date.now()));
  const staleBefore = new Date(nowMs - staleOlderThanMs);
  const db = scalpPrisma();
  const rows = await db.$queryRaw<Array<{
    active: bigint;
    missingEvidence: bigint;
    stale: bigint;
    leased: bigint;
  }>>(sql`
    SELECT
      COUNT(*)::bigint AS active,
      COUNT(*) FILTER (WHERE d.v5_evaluated_at IS NULL)::bigint AS "missingEvidence",
      COUNT(*) FILTER (WHERE d.v5_evaluated_at IS NOT NULL AND d.v5_evaluated_at < ${staleBefore})::bigint AS stale,
      COUNT(*) FILTER (WHERE d.v5_lease_until IS NOT NULL AND d.v5_lease_until > NOW())::bigint AS leased
    FROM scalp_v2_deployments d
    WHERE d.candidate_id IS NOT NULL
      AND d.strategy_id = ${SESSION_STRUCTURE_COMPOSER_V1_STRATEGY_ID}
      AND NOT EXISTS (
        SELECT 1
        FROM scalp_v2_candidates c
        WHERE c.id = d.candidate_id
          AND c.metadata_json->'scopeRemoval'->>'reason' = 'bitget_symbol_removed_no_candles'
      );
  `);
  const row = rows[0];
  return {
    active: Math.max(0, Math.floor(Number(row?.active || 0))),
    missingEvidence: Math.max(0, Math.floor(Number(row?.missingEvidence || 0))),
    stale: Math.max(0, Math.floor(Number(row?.stale || 0))),
    leased: Math.max(0, Math.floor(Number(row?.leased || 0))),
  };
}

// Unified strict promotion: v5 is authoritative for v5-qualified rows. It
// flips `enabled = TRUE` from v5 cell evidence alone and writes a v5-owned
// marker into promotion_gate. v2 promote is disabled by default; v2 execute
// remains the live order engine but must treat this marker as authoritative.
//
// Criteria (ALL must hold for a row to be promoted):
//
//   v5-side (computed from v5_cell_evidence JSONB):
//     1. v5_enabled = TRUE                         (≥1 positive-expectancy cell)
//     2. v5_evaluated_at >= now - staleOlderThanMs (evidence fresh)
//     3. v5_lease_until IS NULL OR expired         (not mid-evaluation)
//     4. totalNetR >= minTotalNetR                 (sum across cells; default ≥ 4R, mirrors v2's 4w-NetR ≥ 4R)
//     5. totalTrades >= minTotalTrades             (sum across cells; default ≥ 60, mirrors v2's min total)
//     6. weeks_with_positive_cross_cell_netR >= minPositiveWeeks
//                                                   (default ≥ 8, mirrors v2's "8/12 four-week windows positive")
//     7. worst_single_week_cross_cell_netR >= -minWorstWeekR
//                                                   (default ≥ -3R, mirrors v2's worst-week bound)
//     8. trailing_4w_cross_cell_netR >= minTrailing4wNetR
//                                                   (default ≥ 4R, direct-live momentum gate)
//
// Low-sample consistency exception:
//   Rows below minTotalTrades can still pass when they have ≥30 trades,
//   ≥12R total, ≥11 positive weeks, no negative week, ≥4R trailing 4w,
//   and at least two active cells. This is intentionally stricter on
//   distribution quality instead of globally lowering the 60-trade gate.
//
// Implementation strategy: a single SELECT brings every v5-eligible row out
// with its evidence. Strict criteria 4-7 are computed in JS (the JSONB walk
// to "sum across cells per week" is too verbose in SQL); the qualifying rows
// get per-row promotion metrics merged into promotion_gate.
export async function autoPromoteScalpV5WinnersToEnabled(params: {
  staleOlderThanMs?: number;
  nowMs?: number;
  dryRun?: boolean;
  minTotalNetR?: number;
  minTotalTrades?: number;
  minPositiveWeeks?: number;
  minWorstWeekR?: number;
  minTrailing4wNetR?: number;
  minConsistencyTrades?: number;
  minConsistencyTotalNetR?: number;
  minConsistencyPositiveWeeks?: number;
  minConsistencyWorstWeekR?: number;
  minConsistencyTrailing4wNetR?: number;
  minConsistencyActiveCells?: number;
  maxPromotions?: number;
}): Promise<{
  promoted: number;
  deploymentIds: string[];
  liveMode: "live";
  runtimeLiveEnabled: boolean;
  v5LiveBypassesV2LiveEnabled: boolean;
  // Funnel breakdown — surfaces WHY rows didn't promote, so the operator can
  // see which v5-side criterion is binding.
  funnel: {
    candidates: number;        // passed v5_enabled + freshness + lease checks
    failedDayRobustnessMissing: number;
    failedDayRobustnessFailed: number;
    failedTotalNetR: number;
    failedTotalTrades: number;
    failedPositiveWeeks: number;
    failedWorstWeek: number;
    failedTrailing4wNetR: number;
    qualifiedByConsistencyException: number;
    qualified: number;          // passed everything
    shortlisted: number;
    promoted: number;
  };
}> {
  const emptyFunnel = {
    candidates: 0,
    failedDayRobustnessMissing: 0,
    failedDayRobustnessFailed: 0,
    failedTotalNetR: 0,
    failedTotalTrades: 0,
    failedPositiveWeeks: 0,
    failedWorstWeek: 0,
    failedTrailing4wNetR: 0,
    qualifiedByConsistencyException: 0,
    qualified: 0,
    shortlisted: 0,
    promoted: 0,
  };
  const runtime = getScalpV2RuntimeConfig();
  const runtimeLiveEnabled = Boolean(runtime.liveEnabled);
  const v5LiveBypassesV2LiveEnabled = true;
  const liveMode: "live" = "live";
  if (!isScalpPgConfigured()) {
    return {
      promoted: 0,
      deploymentIds: [],
      liveMode,
      runtimeLiveEnabled,
      v5LiveBypassesV2LiveEnabled,
      funnel: emptyFunnel,
    };
  }
  const staleOlderThanMs = Math.max(
    60_000,
    Math.floor(Number(params.staleOlderThanMs || 14 * 24 * 60 * 60_000)),
  );
  const thresholds: ScalpV5PromotionThresholds = {
    minTotalNetR: Number.isFinite(Number(params.minTotalNetR))
      ? Number(params.minTotalNetR)
      : 4,
    minTotalTrades: Math.max(0, Math.floor(Number(params.minTotalTrades ?? 60))),
    minPositiveWeeks: Math.max(0, Math.floor(Number(params.minPositiveWeeks ?? 8))),
    // worst-week floor: a row whose single-worst week is below this gets
    // rejected. Stored as a positive number; we compare via >= -minWorstWeekR.
    minWorstWeekR: Math.abs(
      Number.isFinite(Number(params.minWorstWeekR))
        ? Number(params.minWorstWeekR)
        : 3,
    ),
    minTrailing4wNetR: Number.isFinite(Number(params.minTrailing4wNetR))
      ? Number(params.minTrailing4wNetR)
      : 4,
    minConsistencyTrades: Math.max(0, Math.floor(Number(params.minConsistencyTrades ?? 30))),
    minConsistencyTotalNetR: Number.isFinite(Number(params.minConsistencyTotalNetR))
      ? Number(params.minConsistencyTotalNetR)
      : 12,
    minConsistencyPositiveWeeks: Math.max(0, Math.floor(Number(params.minConsistencyPositiveWeeks ?? 11))),
    minConsistencyWorstWeekR: Number.isFinite(Number(params.minConsistencyWorstWeekR))
      ? Number(params.minConsistencyWorstWeekR)
      : 0,
    minConsistencyTrailing4wNetR: Number.isFinite(Number(params.minConsistencyTrailing4wNetR))
      ? Number(params.minConsistencyTrailing4wNetR)
      : 4,
    minConsistencyActiveCells: Math.max(1, Math.floor(Number(params.minConsistencyActiveCells ?? 2))),
  };
  const nowMs = Math.floor(Number(params.nowMs || Date.now()));
  const staleBefore = new Date(nowMs - staleOlderThanMs);
  const dryRun = Boolean(params.dryRun);
  const db = scalpPrisma();

  // Pull v5-eligible candidates with everything we need to evaluate strict
  // criteria. Single SELECT — JSONB walk happens in JS.
  type CandidateRow = {
    deploymentId: string;
    candidateId: number | bigint | null;
    enabled: boolean;
    liveMode: string | null;
    v5CellEvidence: unknown;
    promotionGate: unknown;
    candidateMetadata: unknown;
  };
  const candidates = await db.$queryRaw<CandidateRow[]>(sql`
    SELECT
      deployment_id    AS "deploymentId",
      candidate_id     AS "candidateId",
      enabled          AS "enabled",
      live_mode        AS "liveMode",
      v5_cell_evidence AS "v5CellEvidence",
      d.promotion_gate AS "promotionGate",
      c.metadata_json  AS "candidateMetadata"
    FROM scalp_v2_deployments d
    INNER JOIN scalp_v2_candidates c ON c.id = d.candidate_id
    WHERE d.candidate_id IS NOT NULL
      AND d.strategy_id = ${SESSION_STRUCTURE_COMPOSER_V1_STRATEGY_ID}
      AND d.v5_enabled = TRUE
      AND d.v5_evaluated_at IS NOT NULL
      AND d.v5_evaluated_at >= ${staleBefore}
      AND (d.v5_lease_until IS NULL OR d.v5_lease_until < NOW())
      AND (
        d.enabled = FALSE
        OR d.live_mode IS DISTINCT FROM 'live'
        OR d.promotion_gate->>'source' IS DISTINCT FROM 'v5_cell_evidence'
        OR d.promotion_gate->'v5Promotion' IS NULL
      )
    ORDER BY d.v5_evaluated_at DESC;
  `);

  const funnel = { ...emptyFunnel, candidates: candidates.length };
  const dayRobustnessPolicy = resolveDayRobustnessPolicy();
  const qualifiedRows: Array<{
    deploymentId: string;
    candidateId: number | null;
    alreadyEnabled: boolean;
    liveRepair: boolean;
    passReason: string;
    metrics: ScalpV5PromotionMetrics;
  }> = [];

  for (const row of candidates) {
    const dayRobustness = evaluateDayRobustnessForPromotion({
      strategyId: SESSION_STRUCTURE_COMPOSER_V1_STRATEGY_ID,
      metadata: row.candidateMetadata,
      policy: dayRobustnessPolicy,
      nowMs,
    });
    if (!dayRobustness.passed) {
      if (dayRobustness.reason === "DAY_ROBUSTNESS_MISSING") funnel.failedDayRobustnessMissing += 1;
      else funnel.failedDayRobustnessFailed += 1;
    }

    const evidence = (row.v5CellEvidence && typeof row.v5CellEvidence === "object" && !Array.isArray(row.v5CellEvidence))
      ? (row.v5CellEvidence as ScalpV5CellEvidence)
      : null;
    const evaluation = evaluateScalpV5PromotionEvidence({ evidence, thresholds });
    if (evaluation.reason === "v5_total_net_r_below_threshold") {
      funnel.failedTotalNetR += 1;
      continue;
    }
    if (evaluation.reason === "v5_total_trades_below_threshold") {
      funnel.failedTotalTrades += 1;
      continue;
    }
    if (evaluation.reason === "v5_positive_weeks_below_threshold") {
      funnel.failedPositiveWeeks += 1;
      continue;
    }
    if (evaluation.reason === "v5_worst_week_below_threshold") {
      funnel.failedWorstWeek += 1;
      continue;
    }
    if (evaluation.reason === "v5_trailing_4w_net_r_below_threshold") {
      funnel.failedTrailing4wNetR += 1;
      continue;
    }

    funnel.qualified += 1;
    if (evaluation.reason === "v5_consistency_exception_passed") {
      funnel.qualifiedByConsistencyException += 1;
    }
    const gate = asRecord(row.promotionGate);
    const v5Owned =
      String(gate.source || "").trim().toLowerCase() === "v5_cell_evidence" ||
      Object.keys(asRecord(gate.v5Promotion)).length > 0;
    qualifiedRows.push({
      deploymentId: String(row.deploymentId || "").trim(),
      candidateId: row.candidateId === null || row.candidateId === undefined
        ? null
        : Math.floor(Number(row.candidateId) || 0) || null,
      alreadyEnabled: Boolean(row.enabled),
      liveRepair: Boolean(row.enabled) && (
        String(row.liveMode || "").trim().toLowerCase() !== "live" ||
        !v5Owned
      ),
      passReason: evaluation.reason,
      metrics: evaluation.metrics,
    });
  }

  const enabledRows = await db.$queryRaw<Array<{ count: bigint }>>(sql`
    SELECT COUNT(*)::bigint AS count
    FROM scalp_v2_deployments
    WHERE enabled = TRUE;
  `);
  const enabledCount = Math.max(0, Math.floor(Number(enabledRows[0]?.count || 0)));
  const maxNewPromotions = Math.max(
    0,
    Math.floor(
      Number.isFinite(Number(params.maxPromotions))
        ? Number(params.maxPromotions)
        : Math.max(0, Math.floor(Number(runtime.budgets.maxEnabledDeployments || 0)) - enabledCount),
    ),
  );
  const rankedRows = qualifiedRows
    .filter((row) => Boolean(row.deploymentId))
    .sort((a, b) =>
      b.metrics.totalNetR - a.metrics.totalNetR ||
      b.metrics.trailing4wNetR - a.metrics.trailing4wNetR ||
      b.metrics.expectancyR - a.metrics.expectancyR,
    );
  const repairRows = rankedRows.filter((row) => row.liveRepair);
  const newRows = rankedRows
    .filter((row) => !row.alreadyEnabled)
    .slice(0, maxNewPromotions);
  const filteredRows = Array.from(
    new Map([...repairRows, ...newRows].map((row) => [row.deploymentId, row])).values(),
  );
  funnel.shortlisted = filteredRows.length;
  const filteredIds = filteredRows.map((row) => row.deploymentId);
  if (dryRun || filteredRows.length === 0) {
    return {
      promoted: 0,
      deploymentIds: filteredIds,
      liveMode,
      runtimeLiveEnabled,
      v5LiveBypassesV2LiveEnabled,
      funnel,
    };
  }

  const promotedAtMs = nowMs;
  for (const row of filteredRows) {
    await db.$executeRaw(sql`
      UPDATE scalp_v2_deployments
      SET
        enabled = TRUE,
        live_mode = ${liveMode},
        promotion_gate = COALESCE(promotion_gate, '{}'::jsonb) || jsonb_build_object(
          'eligible', TRUE,
          'reason', ${row.passReason}::text,
          'source', 'v5_cell_evidence',
          'evaluatedAtMs', ${promotedAtMs}::bigint,
          'promotedAtMs', ${promotedAtMs}::bigint,
          'v5Promotion', ${JSON.stringify({
            promotedAtMs,
            metrics: row.metrics,
            thresholds,
            dayRobustness: {
              required: dayRobustnessPolicy.enabled,
              policy: dayRobustnessPolicy,
            },
            passReason: row.passReason,
            liveMode,
            runtimeLiveEnabled,
            v5LiveBypassesV2LiveEnabled,
          })}::jsonb
        ),
        last_promoted_at = NOW(),
        updated_at = NOW()
      WHERE deployment_id = ${row.deploymentId}
        AND v5_enabled = TRUE
        AND (v5_lease_until IS NULL OR v5_lease_until < NOW());
    `);
  }
  const candidateIds = Array.from(
    new Set(
      filteredRows
        .map((row) => row.candidateId)
        .filter((id): id is number => Number.isFinite(Number(id)) && Number(id) > 0),
    ),
  );
  if (candidateIds.length > 0) {
    await db.$executeRaw(sql`
      UPDATE scalp_v2_candidates
      SET
        status = 'promoted',
        metadata_json = COALESCE(metadata_json, '{}'::jsonb) || jsonb_build_object(
          'promotedAtMs', ${promotedAtMs}::bigint,
          'promotedBy', 'v5_cell_evidence'
        ),
        updated_at = NOW()
      WHERE id = ANY(${candidateIds}::bigint[]);
    `);
  }
  funnel.promoted = filteredRows.length;
  return {
    promoted: filteredRows.length,
    deploymentIds: filteredIds,
    liveMode,
    runtimeLiveEnabled,
    v5LiveBypassesV2LiveEnabled,
    funnel,
  };
}

export async function upsertScalpV5DeploymentEvidence(params: {
  deploymentId: string;
  evidence: ScalpV5CellEvidence;
  enabled: boolean;
  // Replay checkpoint paired with this evidence. The next evaluation can
  // resume from this checkpoint instead of replaying the full 12 weeks,
  // provided the holdout window slid by exactly one week and the config
  // hash matches. Pass null to clear an existing checkpoint (e.g. when a
  // full replay produced no usable checkpoint).
  checkpoint: ScalpReplayCheckpoint | null;
}): Promise<void> {
  if (!isScalpPgConfigured()) return;
  const db = scalpPrisma();
  const checkpointJson = params.checkpoint ? JSON.stringify(params.checkpoint) : null;
  // Always clear v5_lease_until alongside the evidence write — the worker
  // holding the lease just finished successfully, so the lease should drop
  // immediately rather than waiting for its TTL. Evidence + checkpoint are
  // written in the same UPDATE so they stay in lockstep — a checkpoint
  // pointing at a different holdout window would be useless.
  await db.$executeRaw(sql`
    UPDATE scalp_v2_deployments
    SET
      v5_cell_evidence = ${JSON.stringify(params.evidence)}::jsonb,
      v5_enabled = ${params.enabled},
      v5_evaluated_at = NOW(),
      v5_lease_until = NULL,
      v5_replay_checkpoint = ${checkpointJson}::jsonb,
      updated_at = NOW()
    WHERE deployment_id = ${params.deploymentId};
  `);
}

// Load just the replay checkpoint for a deployment. The dashboard never
// reads this (it's only useful to the incremental evaluator), so it's a
// separate query rather than bolted onto the deployment loader.
export async function loadScalpV5DeploymentCheckpoint(params: {
  deploymentId: string;
}): Promise<ScalpReplayCheckpoint | null> {
  if (!isScalpPgConfigured()) return null;
  const db = scalpPrisma();
  const rows = await db.$queryRaw<Array<{ checkpoint: unknown }>>(sql`
    SELECT v5_replay_checkpoint AS "checkpoint"
    FROM scalp_v2_deployments
    WHERE deployment_id = ${params.deploymentId}
    LIMIT 1;
  `);
  const raw = rows[0]?.checkpoint;
  if (!raw || typeof raw !== "object" || Array.isArray(raw)) return null;
  // Light shape validation. A malformed checkpoint (e.g. wrong version, or
  // missing required field after a code change) gets discarded so the
  // evaluator falls back to a full replay.
  const rec = raw as Record<string, unknown>;
  if (rec.version !== 1) return null;
  if (typeof rec.endTs !== "number" || typeof rec.nextRunTs !== "number") return null;
  if (typeof rec.configHash !== "string") return null;
  if (!rec.state || typeof rec.state !== "object") return null;
  if (!Array.isArray(rec.baseClosedCandles) || !Array.isArray(rec.confirmClosedCandles)) {
    return null;
  }
  return rec as unknown as ScalpReplayCheckpoint;
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
