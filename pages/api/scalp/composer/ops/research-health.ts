export const config = { runtime: "nodejs" };

import type { NextApiRequest, NextApiResponse } from "next";

import { requireAdminAccess } from "../../../../../lib/admin";
import {
  countScalpComposerCandidatesByStatus,
  listScalpComposerJobs,
} from "../../../../../lib/scalp/composer/db";
import { SESSION_STRUCTURE_COMPOSER_V1_STRATEGY_ID } from "../../../../../lib/scalp/composer/sessionStructureComposer";
import { setNoStoreHeaders } from "../../../../../lib/scalp/composer/http";
import { isScalpPgConfigured, scalpPrisma, sql } from "../../../../../lib/scalp/composer/pg";
import type { ScalpComposerCandidateStatus } from "../../../../../lib/scalp/composer/types";

type HintTone = "ok" | "warn" | "critical" | "info";

const CANDIDATE_STATUSES: ScalpComposerCandidateStatus[] = [
  "discovered",
  "evaluated",
  "promoted",
  "rejected",
];

function toPositiveInt(value: unknown, fallback: number, max: number): number {
  const n = Math.floor(Number(value));
  if (!Number.isFinite(n) || n <= 0) return fallback;
  return Math.max(1, Math.min(max, n));
}

function resolveScalpComposerJobLockStaleMinutes(): number {
  return Math.max(
    2,
    Math.min(
      120,
      toPositiveInt(process.env.SCALP_COMPOSER_JOB_LOCK_STALE_MINUTES, 10, 120),
    ),
  );
}

function asRecord(value: unknown): Record<string, unknown> {
  if (!value || typeof value !== "object" || Array.isArray(value)) return {};
  return value as Record<string, unknown>;
}

function asFiniteMs(value: unknown): number | null {
  const n = Number(value);
  if (!Number.isFinite(n) || n <= 0) return null;
  return Math.floor(n);
}

function nonNegativeInt(...values: unknown[]): number {
  for (const value of values) {
    const n = Math.floor(Number(value));
    if (Number.isFinite(n) && n >= 0) return n;
  }
  return 0;
}

async function loadDayRobustnessQueue(): Promise<{
  stageCPassed: number;
  missing: number;
  passed: number;
  failed: number;
}> {
  if (!isScalpPgConfigured()) return { stageCPassed: 0, missing: 0, passed: 0, failed: 0 };
  const db = scalpPrisma();
  const [row] = await db.$queryRaw<Array<{
    stageCPassed: bigint | number;
    missing: bigint | number;
    passed: bigint | number;
    failed: bigint | number;
  }>>(sql`
    SELECT
      COUNT(*)::bigint AS "stageCPassed",
      COUNT(*) FILTER (
        WHERE metadata_json->'worker'->'robustness' IS NULL
      )::bigint AS missing,
      COUNT(*) FILTER (
        WHERE COALESCE((metadata_json->'worker'->'robustness'->>'passed')::boolean, false)
      )::bigint AS passed,
      COUNT(*) FILTER (
        WHERE metadata_json->'worker'->'robustness' IS NOT NULL
          AND COALESCE((metadata_json->'worker'->'robustness'->>'passed')::boolean, false) = false
      )::bigint AS failed
    FROM scalp_v2_candidates
    WHERE status = 'evaluated'
      AND strategy_id = ${SESSION_STRUCTURE_COMPOSER_V1_STRATEGY_ID}
      AND COALESCE((metadata_json->'worker'->'stageC'->>'passed')::boolean, false);
  `);
  return {
    stageCPassed: Math.max(0, Math.floor(Number(row?.stageCPassed || 0))),
    missing: Math.max(0, Math.floor(Number(row?.missing || 0))),
    passed: Math.max(0, Math.floor(Number(row?.passed || 0))),
    failed: Math.max(0, Math.floor(Number(row?.failed || 0))),
  };
}

function formatAgeMinutes(ms: number | null): string {
  if (!Number.isFinite(Number(ms)) || ms === null || ms < 0) return "n/a";
  const minutes = ms / 60_000;
  if (minutes < 1) return "<1m";
  return `${minutes.toFixed(minutes >= 10 ? 0 : 1)}m`;
}

function buildHint(params: {
  status: string;
  lockAgeMs: number | null;
  heartbeatAgeMs: number | null;
  staleThresholdMs: number;
  phase: string | null;
  reason: string | null;
}): { tone: HintTone; label: string; detail: string | null } {
  const status = String(params.status || "").trim().toLowerCase();
  const stale = status === "running" && params.lockAgeMs !== null && params.lockAgeMs >= params.staleThresholdMs;
  const nearStale =
    status === "running" &&
    params.lockAgeMs !== null &&
    params.lockAgeMs >= Math.floor(params.staleThresholdMs * 0.8);
  const phase = params.phase ? `phase=${params.phase}` : null;
  const reason = params.reason ? `reason=${params.reason}` : null;
  const context = [phase, reason].filter(Boolean).join(" · ") || null;

  if (stale) {
    return {
      tone: "critical",
      label: "Research lock looks stale",
      detail: `lock age ${formatAgeMinutes(params.lockAgeMs)} (threshold ${formatAgeMinutes(
        params.staleThresholdMs,
      )})${context ? ` · ${context}` : ""}`,
    };
  }
  if (nearStale) {
    return {
      tone: "warn",
      label: "Research lock nearing timeout",
      detail: `lock age ${formatAgeMinutes(params.lockAgeMs)} (threshold ${formatAgeMinutes(
        params.staleThresholdMs,
      )})${context ? ` · ${context}` : ""}`,
    };
  }
  if (status === "running") {
    return {
      tone: "ok",
      label: "Research running",
      detail: `lock age ${formatAgeMinutes(params.lockAgeMs)}${
        params.heartbeatAgeMs !== null
          ? ` · heartbeat ${formatAgeMinutes(params.heartbeatAgeMs)} ago`
          : ""
      }${context ? ` · ${context}` : ""}`,
    };
  }
  if (status === "failed") {
    return {
      tone: "warn",
      label: "Research failed last run",
      detail: context,
    };
  }
  if (status === "pending") {
    return {
      tone: "info",
      label: "Research pending",
      detail: context,
    };
  }
  if (status === "succeeded") {
    return {
      tone: "ok",
      label: "Research healthy",
      detail: context,
    };
  }
  return {
    tone: "info",
    label: "Research status unknown",
    detail: context,
  };
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
  if (!requireAdminAccess(req, res)) return;
  setNoStoreHeaders(res);

  try {
    const nowMs = Date.now();
    const staleLockMinutes = resolveScalpComposerJobLockStaleMinutes();
    const staleThresholdMs = staleLockMinutes * 60_000;
    const [jobs, robustness, ...candidateCounts] = await Promise.all([
      // Target the live research row directly. Work-leases mode writes a new
      // research job row per run, so a global recent-N scan can return a stale
      // succeeded row (or miss it behind execute/reconcile churn).
      listScalpComposerJobs({ jobKind: "research", preferRunning: true, limit: 1 }),
      loadDayRobustnessQueue(),
      ...CANDIDATE_STATUSES.map((status) =>
        countScalpComposerCandidatesByStatus({ status }),
      ),
    ]);
    const statusCounts = CANDIDATE_STATUSES.reduce<Record<ScalpComposerCandidateStatus, number>>(
      (acc, status, idx) => {
        acc[status] = Math.max(0, Math.floor(Number(candidateCounts[idx] || 0)));
        return acc;
      },
      {
        discovered: 0,
        evaluated: 0,
        promoted: 0,
        rejected: 0,
      },
    );
    const processedCandidates =
      statusCounts.evaluated + statusCounts.promoted + statusCounts.rejected;
    const totalCandidates = processedCandidates + statusCounts.discovered;
    const queue = {
      total: totalCandidates,
      processed: processedCandidates,
      discovered: statusCounts.discovered,
      evaluated: statusCounts.evaluated,
      promoted: statusCounts.promoted,
      rejected: statusCounts.rejected,
    };
    const researchJob =
      jobs.find(
        (job) => String(job?.jobKind || "").trim().toLowerCase() === "research",
      ) || null;

    if (!researchJob) {
      return res.status(200).json({
        ok: true,
        mode: "scalp_v2",
        nowMs,
        staleLockMinutes,
        health: {
          staleThresholdMs,
          stale: false,
          approachingStale: false,
          lockAgeMs: null,
          heartbeatAgeMs: null,
        },
        job: null,
        queue,
        robustness,
        hint: {
          tone: "info",
          label: "Research health unavailable",
          detail: "No research job row found",
        },
      });
    }

    const payload = asRecord(researchJob.payload);
    const progress = asRecord(payload.progress);
    const status = String(researchJob.status || "").trim().toLowerCase();
    const phaseRaw = String(payload.phase || "").trim();
    const phase = phaseRaw || null;
    const reasonRaw = String(payload.reason || "").trim();
    const reason = reasonRaw || null;
    const lockedAtMs = asFiniteMs(researchJob.lockedAtMs);
    const updatedAtMs = asFiniteMs(researchJob.updatedAtMs);
    const heartbeatAtMs =
      asFiniteMs(payload.heartbeatAtMs) ||
      asFiniteMs(progress.heartbeatAtMs) ||
      updatedAtMs;
    const lockAgeMs =
      lockedAtMs !== null && status === "running"
        ? Math.max(0, nowMs - lockedAtMs)
        : null;
    const heartbeatAgeMs =
      heartbeatAtMs !== null ? Math.max(0, nowMs - heartbeatAtMs) : null;
    const stale =
      status === "running" &&
      lockAgeMs !== null &&
      lockAgeMs >= staleThresholdMs;
    const approachingStale =
      status === "running" &&
      lockAgeMs !== null &&
      !stale &&
      lockAgeMs >= Math.floor(staleThresholdMs * 0.8);

    const hint = buildHint({
      status,
      lockAgeMs,
      heartbeatAgeMs,
      staleThresholdMs,
      phase,
      reason,
    });

    return res.status(200).json({
      ok: true,
      mode: "scalp_v2",
      nowMs,
      staleLockMinutes,
      health: {
        staleThresholdMs,
        stale,
        approachingStale,
        lockAgeMs,
        heartbeatAgeMs,
      },
      job: {
        status,
        attempts: Math.max(0, Math.floor(Number(researchJob.attempts || 0))),
        locked: status === "running" || Boolean(lockedAtMs),
        lockedAtMs,
        updatedAtMs,
        nextRunAtMs: asFiniteMs(researchJob.nextRunAtMs),
        phase,
        reason,
        progress: {
          processedSoFar: nonNegativeInt(progress.processedSoFar, payload.processedCandidates),
          totalSelected: nonNegativeInt(
            progress.selectedCandidates,
            progress.totalSelected,
            payload.backtested,
          ),
          selectedTotal: nonNegativeInt(progress.selectedTotal, payload.totalCandidates),
          discoveredTotal: nonNegativeInt(progress.discoveredTotal),
          workerStage: String(progress.workerStage || "").trim() || null,
          workerStageProcessed: nonNegativeInt(progress.workerStageProcessed),
          workerStageTotal: nonNegativeInt(progress.workerStageTotal),
          skippedByCache: nonNegativeInt(progress.skippedByCache, payload.skippedByCache),
          skippedByClearFail: nonNegativeInt(progress.skippedByClearFail, payload.skippedByClearFail),
          skippedByNetRPreFilter: nonNegativeInt(progress.skippedByNetRPreFilter, payload.skippedByNetRPreFilter),
          smartSkippedPersisted: nonNegativeInt(progress.smartSkippedPersisted, payload.smartSkippedPersisted),
          surrogateSkippedPersisted: nonNegativeInt(progress.surrogateSkippedPersisted, payload.surrogateSkippedPersisted),
          stageAPass: nonNegativeInt(progress.stageAPass, payload.stageAPass),
          stageAFail: nonNegativeInt(progress.stageAFail, payload.stageAFail),
          stageBPass: nonNegativeInt(progress.stageBPass, payload.stageBPass),
          stageBFail: nonNegativeInt(progress.stageBFail, payload.stageBFail),
          stageCPass: nonNegativeInt(progress.stageCPass, payload.stageCPass),
          stageCFail: nonNegativeInt(progress.stageCFail, payload.stageCFail),
          persisted: nonNegativeInt(progress.persisted, payload.persistedCount),
          replayErrors: nonNegativeInt(progress.replayErrors, payload.replayErrors),
          persistErrors: nonNegativeInt(progress.persistErrors, payload.persistErrors),
          stage0Replays: nonNegativeInt(progress.stage0Replays, payload.stage0Replays),
          stage0Skipped: nonNegativeInt(progress.stage0Skipped, payload.stage0Skipped),
          incrementalStageReplays: nonNegativeInt(progress.incrementalStageReplays, payload.incrementalStageReplays),
          fullStageReplays: nonNegativeInt(progress.fullStageReplays, payload.fullStageReplays),
          earlyAbortedStageReplays: nonNegativeInt(progress.earlyAbortedStageReplays, payload.earlyAbortedStageReplays),
          cachedStageReuses: nonNegativeInt(progress.cachedStageReuses, payload.cachedStageReuses),
          newestWeekReplayReuses: nonNegativeInt(progress.newestWeekReplayReuses, payload.newestWeekReplayReuses),
          stageBCacheHits: nonNegativeInt(progress.stageBCacheHits, payload.stageBCacheHits),
          stageCCacheHits: nonNegativeInt(progress.stageCCacheHits, payload.stageCCacheHits),
          deferredByCandleCoverage: nonNegativeInt(progress.deferredByCandleCoverage, payload.deferredByCandleCoverage),
          finalizedCoverageDeferrals: nonNegativeInt(progress.finalizedCoverageDeferrals, payload.finalizedCoverageDeferrals),
          pendingAfter: nonNegativeInt(payload.pendingAfter),
          remaining: nonNegativeInt(payload.remaining),
          timeBudgetExhausted: Boolean(payload.timeBudgetExhausted),
        },
        log: Array.isArray(payload.log) ? payload.log.slice(-30) : [],
      },
      queue,
      robustness,
      hint,
    });
  } catch (err: any) {
    return res.status(500).json({
      error: "scalp_v2_ops_research_health_failed",
      message: err?.message || String(err),
    });
  }
}
