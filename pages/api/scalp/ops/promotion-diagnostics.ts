export const config = { runtime: "nodejs" };

import type { NextApiRequest, NextApiResponse } from "next";

import {
  isScalpPgConfigured,
  scalpPrisma,
} from "../../../../lib/scalp/pg/client";
import { sql } from "../../../../lib/scalp/pg/sql";
import { loadScalpPipelineJobsHealth } from "../../../../lib/scalp/pipelineJobs";
import {
  listScalpEntrySessionProfiles,
  parseScalpEntrySessionProfileStrict,
} from "../../../../lib/scalp/sessions";
import { resolveCompletedWeekWindowToUtc } from "../../../../lib/scalp/weekWindows";

const ONE_WEEK_MS = 7 * 24 * 60 * 60 * 1000;

type DiagnosticsScope = "actionable" | "all";

type LifecycleState =
  | "candidate"
  | "incumbent_refresh"
  | "graduated"
  | "suspended"
  | "retired";

type DiagnosticsRawRow = {
  deploymentId: string;
  symbol: string;
  strategyId: string;
  tuneId: string;
  enabled: boolean;
  inUniverse: boolean | null;
  source: string;
  entrySessionProfile: string;
  promotionDirty: boolean;
  updatedAtMs: bigint | number | null;
  promotionGate: unknown;
  enabledPeerCount: bigint | number | null;
  succeededWeeks: bigint | number | null;
  pendingWeeks: bigint | number | null;
  runningWeeks: bigint | number | null;
  retryWaitWeeks: bigint | number | null;
  failedWeeks: bigint | number | null;
  lastSucceededWeekEndMs: bigint | number | null;
  oldestOpenWeekStartMs: bigint | number | null;
  nextMetricRunAtMs: bigint | number | null;
};

type PromotionGateSnapshot = {
  eligible: boolean | null;
  reason: string | null;
  freshnessWindowToTs: number | null;
  freshnessMissingWeeks: number;
  lifecycleState: LifecycleState;
  suspendedUntilMs: number | null;
  retiredUntilMs: number | null;
  forwardValidation: {
    rollCount: number | null;
    profitableWindowPct: number | null;
    meanExpectancyR: number | null;
    minTradesPerWindow: number | null;
    selectionWindowDays: number | null;
    forwardWindowDays: number | null;
  } | null;
};

function setNoStoreHeaders(res: NextApiResponse): void {
  res.setHeader(
    "Cache-Control",
    "no-store, no-cache, must-revalidate, proxy-revalidate",
  );
  res.setHeader("Pragma", "no-cache");
  res.setHeader("Expires", "0");
}

function firstQueryValue(
  value: string | string[] | undefined,
): string | undefined {
  if (typeof value === "string") return value.trim() || undefined;
  if (Array.isArray(value) && value.length > 0)
    return String(value[0] || "").trim() || undefined;
  return undefined;
}

function parseLimit(value: string | undefined): number {
  const n = Number(value);
  if (!Number.isFinite(n)) return 200;
  return Math.max(1, Math.min(1000, Math.floor(n)));
}

function parseLookbackWeeks(value: string | undefined): number {
  const n = Number(value);
  if (!Number.isFinite(n)) return 12;
  return Math.max(1, Math.min(52, Math.floor(n)));
}

function parseScope(value: string | undefined): DiagnosticsScope | null {
  const normalized = String(value || "")
    .trim()
    .toLowerCase();
  if (!normalized || normalized === "actionable") return "actionable";
  if (normalized === "all") return "all";
  return null;
}

function parseSymbol(value: string | undefined): string {
  if (!value) return "";
  return String(value).trim().toUpperCase().slice(0, 40);
}

function parseStrategyId(value: string | undefined): string {
  if (!value) return "";
  return String(value).trim().slice(0, 120);
}

function parseDeploymentId(value: string | undefined): string {
  if (!value) return "";
  return String(value).trim().slice(0, 240);
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return Boolean(value) && typeof value === "object" && !Array.isArray(value);
}

function asTsMs(value: unknown): number | null {
  const n = Number(value);
  if (!Number.isFinite(n) || n <= 0) return null;
  return Math.floor(n);
}

function asFinite(value: unknown): number | null {
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

function normalizeLifecycleState(value: unknown): LifecycleState {
  const normalized = String(value || "")
    .trim()
    .toLowerCase();
  if (
    normalized === "candidate" ||
    normalized === "incumbent_refresh" ||
    normalized === "graduated" ||
    normalized === "suspended" ||
    normalized === "retired"
  ) {
    return normalized;
  }
  return "candidate";
}

function normalizeReasonCode(value: string): string {
  return String(value || "")
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9_]+/g, "_")
    .replace(/^_+|_+$/g, "")
    .slice(0, 120);
}

function summarizeGate(value: unknown): PromotionGateSnapshot {
  const gate = isRecord(value) ? value : {};
  const lifecycle = isRecord(gate.lifecycle) ? gate.lifecycle : {};
  const freshness = isRecord(gate.freshness) ? gate.freshness : {};
  const forward = isRecord(gate.forwardValidation) ? gate.forwardValidation : null;
  const missingWeeksRaw = Math.floor(Number(freshness.missingWeeks) || 0);
  return {
    eligible: typeof gate.eligible === "boolean" ? gate.eligible : null,
    reason: String(gate.reason || "").trim() || null,
    freshnessWindowToTs: asTsMs(freshness.windowToTs),
    freshnessMissingWeeks: Math.max(0, missingWeeksRaw),
    lifecycleState: normalizeLifecycleState(lifecycle.state),
    suspendedUntilMs: asTsMs(lifecycle.suspendedUntilMs),
    retiredUntilMs: asTsMs(lifecycle.retiredUntilMs),
    forwardValidation: forward
      ? {
          rollCount: asFinite(forward.rollCount),
          profitableWindowPct: asFinite(forward.profitableWindowPct),
          meanExpectancyR: asFinite(forward.meanExpectancyR),
          minTradesPerWindow: asFinite(forward.minTradesPerWindow),
          selectionWindowDays: asFinite(forward.selectionWindowDays),
          forwardWindowDays: asFinite(forward.forwardWindowDays),
        }
      : null,
  };
}

function lifecycleSuppressedNow(
  gate: PromotionGateSnapshot,
  nowMs: number,
): boolean {
  if (gate.lifecycleState === "suspended") {
    return gate.suspendedUntilMs === null || gate.suspendedUntilMs > nowMs;
  }
  if (gate.lifecycleState === "retired") {
    return gate.retiredUntilMs === null || gate.retiredUntilMs > nowMs;
  }
  return false;
}

function lifecycleReleaseDueNow(
  gate: PromotionGateSnapshot,
  nowMs: number,
): boolean {
  if (
    gate.lifecycleState === "suspended" &&
    gate.suspendedUntilMs !== null &&
    gate.suspendedUntilMs <= nowMs
  ) {
    return true;
  }
  if (
    gate.lifecycleState === "retired" &&
    gate.retiredUntilMs !== null &&
    gate.retiredUntilMs <= nowMs
  ) {
    return true;
  }
  return false;
}

function formatDelayLabel(tsMs: number | null, nowMs: number): string {
  if (tsMs === null) return "indefinite";
  if (tsMs <= nowMs) return "elapsed";
  const remainingMs = tsMs - nowMs;
  const remainingDays = Math.max(
    1,
    Math.ceil(remainingMs / (24 * 60 * 60 * 1000)),
  );
  return `${remainingDays}d`;
}

export default async function handler(
  req: NextApiRequest,
  res: NextApiResponse,
) {
  if (req.method !== "GET") {
    return res
      .status(405)
      .json({ error: "Method Not Allowed", message: "Use GET" });
  }
  setNoStoreHeaders(res);

  const generatedAtMs = Date.now();
  const scope = parseScope(firstQueryValue(req.query.scope));
  if (!scope) {
    return res.status(400).json({
      error: "invalid_scope",
      message: "Use scope=actionable|all",
      generatedAtMs,
    });
  }
  const entrySessionProfile = parseScalpEntrySessionProfileStrict(
    firstQueryValue(req.query.session),
  );
  if (!entrySessionProfile) {
    return res.status(400).json({
      error: "invalid_session",
      message: `Use session=${listScalpEntrySessionProfiles().join("|")}.`,
      generatedAtMs,
    });
  }

  const limit = parseLimit(firstQueryValue(req.query.limit));
  const lookbackWeeks = parseLookbackWeeks(firstQueryValue(req.query.lookbackWeeks));
  const symbol = parseSymbol(firstQueryValue(req.query.symbol));
  const strategyId = parseStrategyId(firstQueryValue(req.query.strategyId));
  const deploymentId = parseDeploymentId(firstQueryValue(req.query.deploymentId));
  const nowMs = Date.now();
  const windowToTs = resolveCompletedWeekWindowToUtc(nowMs);
  const windowFromTs = windowToTs - lookbackWeeks * ONE_WEEK_MS;

  try {
    const jobs = await loadScalpPipelineJobsHealth({ entrySessionProfile });
    if (!isScalpPgConfigured()) {
      return res.status(200).json({
        ok: true,
        generatedAtMs,
        entrySessionProfile,
        scope,
        limit,
        lookbackWeeks,
        windowFromTs,
        windowToTs,
        symbol: symbol || null,
        strategyId: strategyId || null,
        deploymentId: deploymentId || null,
        pgConfigured: false,
        jobs,
        queue: null,
        totals: {
          scanned: 0,
          returned: 0,
          blocked: 0,
          ignored: 0,
          suspended: 0,
          releaseDue: 0,
        },
        reasonCounts: {},
        rows: [],
      });
    }

    const db = scalpPrisma();
    const scanLimit =
      scope === "all" ? limit : Math.max(limit, Math.min(4000, limit * 4));

    const queueRows = await db.$queryRaw<
      Array<{
        pending: bigint | number | null;
        running: bigint | number | null;
        retryWait: bigint | number | null;
        failed: bigint | number | null;
        succeeded: bigint | number | null;
      }>
    >(sql`
      SELECT
        COUNT(*) FILTER (WHERE status = 'pending')::bigint AS pending,
        COUNT(*) FILTER (WHERE status = 'running')::bigint AS running,
        COUNT(*) FILTER (WHERE status = 'retry_wait')::bigint AS "retryWait",
        COUNT(*) FILTER (WHERE status = 'failed')::bigint AS failed,
        COUNT(*) FILTER (WHERE status = 'succeeded')::bigint AS succeeded
      FROM scalp_deployment_weekly_metrics
      WHERE entry_session_profile = ${entrySessionProfile}
        AND week_start >= ${new Date(windowFromTs)}
        AND week_start < ${new Date(windowToTs)};
    `);

    const latestPromotionRunRows = await db.$queryRaw<
      Array<{
        status: string;
        startedAtMs: bigint | number | null;
        finishedAtMs: bigint | number | null;
        durationMs: number | null;
        processed: number | null;
        succeeded: number | null;
        failed: number | null;
        pendingAfter: number | null;
        progressLabel: string | null;
        error: string | null;
      }>
    >(sql`
      SELECT
        status,
        (EXTRACT(EPOCH FROM started_at) * 1000)::bigint AS "startedAtMs",
        (EXTRACT(EPOCH FROM finished_at) * 1000)::bigint AS "finishedAtMs",
        duration_ms AS "durationMs",
        processed,
        succeeded,
        failed,
        pending_after AS "pendingAfter",
        progress_label AS "progressLabel",
        error
      FROM scalp_pipeline_job_runs
      WHERE job_kind = 'promotion'
        AND entry_session_profile = ${entrySessionProfile}
      ORDER BY started_at DESC
      LIMIT 1;
    `);

    const rows = await db.$queryRaw<Array<DiagnosticsRawRow>>(sql`
      SELECT
        d.deployment_id AS "deploymentId",
        d.symbol,
        d.strategy_id AS "strategyId",
        d.tune_id AS "tuneId",
        d.enabled,
        d.in_universe AS "inUniverse",
        d.source,
        d.entry_session_profile AS "entrySessionProfile",
        d.promotion_dirty AS "promotionDirty",
        (EXTRACT(EPOCH FROM d.updated_at) * 1000)::bigint AS "updatedAtMs",
        d.promotion_gate AS "promotionGate",
        COALESCE(peers.enabled_peer_count, 0)::bigint AS "enabledPeerCount",
        COALESCE(metrics.succeeded_count, 0)::bigint AS "succeededWeeks",
        COALESCE(metrics.pending_count, 0)::bigint AS "pendingWeeks",
        COALESCE(metrics.running_count, 0)::bigint AS "runningWeeks",
        COALESCE(metrics.retry_wait_count, 0)::bigint AS "retryWaitWeeks",
        COALESCE(metrics.failed_count, 0)::bigint AS "failedWeeks",
        metrics.last_succeeded_week_end_ms AS "lastSucceededWeekEndMs",
        metrics.oldest_open_week_start_ms AS "oldestOpenWeekStartMs",
        metrics.next_run_at_ms AS "nextMetricRunAtMs"
      FROM scalp_deployments d
      LEFT JOIN LATERAL (
        SELECT
          COUNT(*) FILTER (WHERE d2.enabled = TRUE) AS enabled_peer_count
        FROM scalp_deployments d2
        WHERE d2.symbol = d.symbol
          AND d2.strategy_id = d.strategy_id
          AND d2.entry_session_profile = d.entry_session_profile
      ) peers ON TRUE
      LEFT JOIN LATERAL (
        SELECT
          COUNT(*) FILTER (WHERE m.status = 'succeeded') AS succeeded_count,
          COUNT(*) FILTER (WHERE m.status = 'pending') AS pending_count,
          COUNT(*) FILTER (WHERE m.status = 'running') AS running_count,
          COUNT(*) FILTER (WHERE m.status = 'retry_wait') AS retry_wait_count,
          COUNT(*) FILTER (WHERE m.status = 'failed') AS failed_count,
          MAX((EXTRACT(EPOCH FROM m.week_end) * 1000)::bigint)
            FILTER (WHERE m.status = 'succeeded') AS last_succeeded_week_end_ms,
          MIN((EXTRACT(EPOCH FROM m.week_start) * 1000)::bigint)
            FILTER (WHERE m.status IN ('pending', 'running', 'retry_wait', 'failed')) AS oldest_open_week_start_ms,
          MIN((EXTRACT(EPOCH FROM m.next_run_at) * 1000)::bigint)
            FILTER (WHERE m.status IN ('pending', 'retry_wait')) AS next_run_at_ms
        FROM scalp_deployment_weekly_metrics m
        WHERE m.deployment_id = d.deployment_id
          AND m.entry_session_profile = d.entry_session_profile
          AND m.week_start >= ${new Date(windowFromTs)}
          AND m.week_start < ${new Date(windowToTs)}
      ) metrics ON TRUE
      WHERE d.entry_session_profile = ${entrySessionProfile}
        AND (${symbol} = '' OR d.symbol = ${symbol})
        AND (${strategyId} = '' OR d.strategy_id = ${strategyId})
        AND (${deploymentId} = '' OR d.deployment_id = ${deploymentId})
      ORDER BY d.updated_at DESC, d.deployment_id ASC
      LIMIT ${scanLimit};
    `);

    const queueRow = queueRows[0] || ({} as any);
    const queue = {
      pending: Math.max(0, Math.floor(Number(queueRow.pending || 0))),
      running: Math.max(0, Math.floor(Number(queueRow.running || 0))),
      retryWait: Math.max(0, Math.floor(Number(queueRow.retryWait || 0))),
      failed: Math.max(0, Math.floor(Number(queueRow.failed || 0))),
      succeeded: Math.max(0, Math.floor(Number(queueRow.succeeded || 0))),
    };

    const latestPromotionRun = (() => {
      const row = latestPromotionRunRows[0];
      if (!row) return null;
      return {
        status: String(row.status || "").trim().toLowerCase(),
        startedAtMs: asTsMs(row.startedAtMs),
        finishedAtMs: asTsMs(row.finishedAtMs),
        durationMs:
          Number.isFinite(Number(row.durationMs)) && Number(row.durationMs) >= 0
            ? Math.floor(Number(row.durationMs))
            : null,
        processed: Math.max(0, Math.floor(Number(row.processed || 0))),
        succeeded: Math.max(0, Math.floor(Number(row.succeeded || 0))),
        failed: Math.max(0, Math.floor(Number(row.failed || 0))),
        pendingAfter: Math.max(0, Math.floor(Number(row.pendingAfter || 0))),
        progressLabel: String(row.progressLabel || "").trim() || null,
        error: String(row.error || "").trim() || null,
      };
    })();

    const diagnosticsRows = rows.map((row) => {
      const gate = summarizeGate(row.promotionGate);
      const enabled = row.enabled === true;
      const promotionDirty = row.promotionDirty === true;
      const enabledPeerCount = Math.max(
        0,
        Math.floor(Number(row.enabledPeerCount || 0)),
      );
      const duplicateEnabled = enabled && enabledPeerCount > 1;

      const pendingWeeks = Math.max(0, Math.floor(Number(row.pendingWeeks || 0)));
      const runningWeeks = Math.max(0, Math.floor(Number(row.runningWeeks || 0)));
      const retryWaitWeeks = Math.max(
        0,
        Math.floor(Number(row.retryWaitWeeks || 0)),
      );
      const failedWeeks = Math.max(0, Math.floor(Number(row.failedWeeks || 0)));
      const succeededWeeks = Math.max(
        0,
        Math.floor(Number(row.succeededWeeks || 0)),
      );
      const workerBacklogCount = pendingWeeks + runningWeeks + retryWaitWeeks;
      const hasWorkerBacklog = workerBacklogCount > 0;
      const hasWorkerFailures = failedWeeks > 0;

      const suppressedNow = lifecycleSuppressedNow(gate, nowMs);
      const releaseDueNow = lifecycleReleaseDueNow(gate, nowMs);
      const staleFreshness =
        gate.freshnessWindowToTs !== null && gate.freshnessWindowToTs < windowToTs;

      const reasonCodes: string[] = [];
      const explanations: string[] = [];
      const addReason = (code: string, message: string) => {
        reasonCodes.push(code);
        explanations.push(message);
      };

      if (suppressedNow) {
        const untilMs =
          gate.lifecycleState === "retired"
            ? gate.retiredUntilMs
            : gate.suspendedUntilMs;
        addReason(
          gate.lifecycleState === "retired"
            ? "lifecycle_retired_active"
            : "lifecycle_suspended_active",
          `Lifecycle ${gate.lifecycleState} is active (${formatDelayLabel(untilMs, nowMs)}).`,
        );
      }
      if (releaseDueNow) {
        addReason(
          "lifecycle_release_due",
          "Lifecycle suppression window elapsed; promotion run can re-evaluate this deployment.",
        );
      }

      if (gate.reason) {
        addReason(
          `gate_${normalizeReasonCode(gate.reason)}`,
          `Promotion gate reason: ${gate.reason}.`,
        );
      } else if (gate.eligible === false) {
        addReason(
          "gate_ineligible",
          "Promotion gate currently marks this deployment ineligible.",
        );
      }

      if (gate.freshnessMissingWeeks > 0) {
        addReason(
          "freshness_missing_completed_weeks",
          `Freshness window is missing ${gate.freshnessMissingWeeks} completed week(s).`,
        );
      }

      if (hasWorkerBacklog) {
        addReason(
          "weekly_metrics_backlog",
          `Weekly metrics still queued/running (${workerBacklogCount} rows pending/running/retry_wait).`,
        );
      }

      if (hasWorkerFailures) {
        addReason(
          "weekly_metrics_failed",
          `${failedWeeks} weekly metrics row(s) failed in lookback window.`,
        );
      }

      if (duplicateEnabled) {
        addReason(
          "enabled_peer_conflict_same_symbol_strategy",
          "Multiple enabled deployments share symbol+strategy+session; uniqueness reconciliation will disable extras.",
        );
      }

      const ignoredNow =
        !promotionDirty &&
        !releaseDueNow &&
        !staleFreshness &&
        !hasWorkerBacklog &&
        !hasWorkerFailures &&
        !duplicateEnabled;
      if (ignoredNow) {
        addReason(
          "ignored_not_dirty",
          "Not promotion_dirty and no freshness/lifecycle trigger, so promotion pass will skip it this cycle.",
        );
      }

      if (
        !enabled &&
        gate.eligible === false &&
        !promotionDirty &&
        !hasWorkerBacklog &&
        !suppressedNow
      ) {
        addReason(
          "candidate_not_eligible",
          "Deployment remains disabled candidate because the gate is not eligible.",
        );
      }

      const blockedNow =
        suppressedNow ||
        gate.eligible === false ||
        hasWorkerBacklog ||
        hasWorkerFailures ||
        duplicateEnabled ||
        gate.freshnessMissingWeeks > 0;

      return {
        deploymentId: row.deploymentId,
        symbol: row.symbol,
        strategyId: row.strategyId,
        tuneId: row.tuneId,
        source: row.source,
        entrySessionProfile: row.entrySessionProfile,
        enabled,
        inUniverse: typeof row.inUniverse === "boolean" ? row.inUniverse : null,
        updatedAtMs: asTsMs(row.updatedAtMs),
        promotion: {
          dirty: promotionDirty,
          eligible: gate.eligible,
          reason: gate.reason,
          freshnessWindowToTs: gate.freshnessWindowToTs,
          freshnessMissingWeeks: gate.freshnessMissingWeeks,
          forwardValidation: gate.forwardValidation,
        },
        lifecycle: {
          state: gate.lifecycleState,
          suspendedUntilMs: gate.suspendedUntilMs,
          retiredUntilMs: gate.retiredUntilMs,
          suppressedNow,
          releaseDueNow,
        },
        peers: {
          enabledPeerCount,
          duplicateEnabled,
        },
        weeklyMetrics: {
          succeededWeeks,
          pendingWeeks,
          runningWeeks,
          retryWaitWeeks,
          failedWeeks,
          oldestOpenWeekStartMs: asTsMs(row.oldestOpenWeekStartMs),
          nextMetricRunAtMs: asTsMs(row.nextMetricRunAtMs),
          lastSucceededWeekEndMs: asTsMs(row.lastSucceededWeekEndMs),
        },
        analysis: {
          blocked: blockedNow,
          ignored: ignoredNow,
          suspended: suppressedNow,
          staleFreshness,
          reasonCodes,
          explanations,
        },
      };
    });

    const scopedRows =
      scope === "all"
        ? diagnosticsRows
        : diagnosticsRows.filter(
            (row) =>
              row.analysis.blocked ||
              row.analysis.ignored ||
              row.lifecycle.releaseDueNow,
          );
    const limitedRows = scopedRows.slice(0, limit);

    const reasonCounts: Record<string, number> = {};
    for (const row of limitedRows) {
      for (const code of row.analysis.reasonCodes) {
        reasonCounts[code] = (reasonCounts[code] || 0) + 1;
      }
    }

    const totals = {
      scanned: diagnosticsRows.length,
      returned: limitedRows.length,
      blocked: limitedRows.filter((row) => row.analysis.blocked).length,
      ignored: limitedRows.filter((row) => row.analysis.ignored).length,
      suspended: limitedRows.filter((row) => row.analysis.suspended).length,
      releaseDue: limitedRows.filter((row) => row.lifecycle.releaseDueNow).length,
    };

    return res.status(200).json({
      ok: true,
      generatedAtMs,
      entrySessionProfile,
      scope,
      limit,
      lookbackWeeks,
      windowFromTs,
      windowToTs,
      symbol: symbol || null,
      strategyId: strategyId || null,
      deploymentId: deploymentId || null,
      pgConfigured: true,
      jobs,
      latestPromotionRun,
      queue,
      totals,
      reasonCounts,
      rows: limitedRows,
    });
  } catch (err: any) {
    return res.status(500).json({
      error: "scalp_promotion_diagnostics_failed",
      message: err?.message || String(err),
      generatedAtMs,
    });
  }
}
