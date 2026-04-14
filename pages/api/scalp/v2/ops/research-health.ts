export const config = { runtime: "nodejs" };

import type { NextApiRequest, NextApiResponse } from "next";

import { requireAdminAccess } from "../../../../../lib/admin";
import { listScalpV2Jobs } from "../../../../../lib/scalp-v2/db";
import { setNoStoreHeaders } from "../../../../../lib/scalp-v2/http";

type HintTone = "ok" | "warn" | "critical" | "info";

function toPositiveInt(value: unknown, fallback: number, max: number): number {
  const n = Math.floor(Number(value));
  if (!Number.isFinite(n) || n <= 0) return fallback;
  return Math.max(1, Math.min(max, n));
}

function resolveScalpV2JobLockStaleMinutes(): number {
  return Math.max(
    2,
    Math.min(
      120,
      toPositiveInt(process.env.SCALP_V2_JOB_LOCK_STALE_MINUTES, 10, 120),
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
    const staleLockMinutes = resolveScalpV2JobLockStaleMinutes();
    const staleThresholdMs = staleLockMinutes * 60_000;
    const jobs = await listScalpV2Jobs({ limit: 50 });
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
          processedSoFar: Math.max(
            0,
            Math.floor(Number(progress.processedSoFar || 0)),
          ),
          totalSelected: Math.max(
            0,
            Math.floor(Number(progress.selectedCandidates || progress.totalSelected || 0)),
          ),
          skippedByCache: Math.max(0, Math.floor(Number(progress.skippedByCache || 0))),
          skippedByClearFail: Math.max(0, Math.floor(Number(progress.skippedByClearFail || 0))),
          skippedByNetRPreFilter: Math.max(0, Math.floor(Number(progress.skippedByNetRPreFilter || 0))),
          stageCPass: Math.max(0, Math.floor(Number(progress.stageCPass || 0))),
          persisted: Math.max(0, Math.floor(Number(progress.persisted || 0))),
          replayErrors: Math.max(
            0,
            Math.floor(Number(progress.replayErrors || 0)),
          ),
        },
        log: Array.isArray(payload.log) ? payload.log.slice(-30) : [],
      },
      hint,
    });
  } catch (err: any) {
    return res.status(500).json({
      error: "scalp_v2_ops_research_health_failed",
      message: err?.message || String(err),
    });
  }
}
