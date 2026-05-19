import { isScalpPgConfigured, scalpPrisma } from "../scalp/pg/client";
import { sql } from "../scalp/pg/sql";
import { getScalpV2DefaultRiskProfile, getScalpV2RuntimeConfig } from "../scalp-v2/config";
import type { ScalpV2RiskProfile, ScalpV2Venue } from "../scalp-v2/types";
import type { ScalpReplayCheckpoint } from "../scalp/replay/types";
import {
  evaluateScalpV5PromotionEvidence,
  type ScalpV5CellEvidence,
  type ScalpV5PromotionMetrics,
  type ScalpV5PromotionThresholds,
} from "./index";

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
        AND NOT EXISTS (
          SELECT 1
          FROM scalp_v2_candidates c
          WHERE c.id = d.candidate_id
            AND c.metadata_json->'scopeRemoval'->>'reason' = 'bitget_symbol_removed_no_candles'
        )
        AND (${onlyEnabled} = FALSE OR d.enabled = TRUE)
        AND (d.v5_evaluated_at IS NULL OR d.v5_evaluated_at < ${staleBefore})
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
        AND d.strategy_id = 'model_guided_composer_v2'
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
      AND d.strategy_id = 'model_guided_composer_v2'
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
// Reversal: if a retired row's (venue, symbol, session, strategy, tune)
// combo gets regenerated later by v2 research, a new deployment row is
// created with the same key but a fresh candidate_id — automatic second
// chance, no manual revival needed.
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
    failedTotalNetR: number;
    failedTotalTrades: number;
    failedPositiveWeeks: number;
    failedWorstWeek: number;
    failedTrailing4wNetR: number;
    qualified: number;          // passed everything
    shortlisted: number;
    promoted: number;
  };
}> {
  const emptyFunnel = {
    candidates: 0,
    failedTotalNetR: 0,
    failedTotalTrades: 0,
    failedPositiveWeeks: 0,
    failedWorstWeek: 0,
    failedTrailing4wNetR: 0,
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
  };
  const candidates = await db.$queryRaw<CandidateRow[]>(sql`
    SELECT
      deployment_id    AS "deploymentId",
      candidate_id     AS "candidateId",
      enabled          AS "enabled",
      live_mode        AS "liveMode",
      v5_cell_evidence AS "v5CellEvidence",
      promotion_gate   AS "promotionGate"
    FROM scalp_v2_deployments
    WHERE candidate_id IS NOT NULL
      AND v5_enabled = TRUE
      AND v5_evaluated_at IS NOT NULL
      AND v5_evaluated_at >= ${staleBefore}
      AND (v5_lease_until IS NULL OR v5_lease_until < NOW())
      AND (
        enabled = FALSE
        OR live_mode IS DISTINCT FROM 'live'
        OR promotion_gate->>'source' IS DISTINCT FROM 'v5_cell_evidence'
        OR promotion_gate->'v5Promotion' IS NULL
      )
    ORDER BY v5_evaluated_at DESC;
  `);

  const funnel = { ...emptyFunnel, candidates: candidates.length };
  const qualifiedRows: Array<{
    deploymentId: string;
    candidateId: number | null;
    alreadyEnabled: boolean;
    liveRepair: boolean;
    metrics: ScalpV5PromotionMetrics;
  }> = [];

  for (const row of candidates) {
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
          'reason', 'v5_strict_passed',
          'source', 'v5_cell_evidence',
          'evaluatedAtMs', ${promotedAtMs}::bigint,
          'promotedAtMs', ${promotedAtMs}::bigint,
          'v5Promotion', ${JSON.stringify({
            promotedAtMs,
            metrics: row.metrics,
            thresholds,
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
