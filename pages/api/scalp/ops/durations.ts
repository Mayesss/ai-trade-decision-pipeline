export const config = { runtime: "nodejs" };

import type { NextApiRequest, NextApiResponse } from "next";

import { requireAdminAccess } from "../../../../lib/admin";
import {
  SCALP_PIPELINE_JOB_KINDS,
  listScalpDurationTimelineRuns,
  type ScalpDurationTimelineSource,
  type ScalpPipelineJobKind,
} from "../../../../lib/scalp/pipelineJobs";
import { normalizeScalpEntrySessionProfile } from "../../../../lib/scalp/sessions";

type SourceFilter = "all" | ScalpDurationTimelineSource;
type JobKindFilter = "all" | ScalpPipelineJobKind;

function firstQueryValue(
  value: string | string[] | undefined,
): string | undefined {
  if (typeof value === "string") return value.trim() || undefined;
  if (Array.isArray(value) && value.length > 0)
    return String(value[0] || "").trim() || undefined;
  return undefined;
}

function parseSourceFilter(value: string | undefined): SourceFilter | null {
  const normalized = String(value || "")
    .trim()
    .toLowerCase();
  if (!normalized || normalized === "all") return "all";
  if (normalized === "pipeline" || normalized === "worker") return normalized;
  return null;
}

function parseJobKindFilter(value: string | undefined): JobKindFilter | null {
  const normalized = String(value || "")
    .trim()
    .toLowerCase();
  if (!normalized || normalized === "all") return "all";
  if (SCALP_PIPELINE_JOB_KINDS.includes(normalized as ScalpPipelineJobKind)) {
    return normalized as ScalpPipelineJobKind;
  }
  return null;
}

function parseOptionalMs(value: string | undefined): number | null {
  if (!value) return null;
  const n = Number(value);
  if (!Number.isFinite(n) || n <= 0) return null;
  return Math.floor(n);
}

function parseLimit(value: string | undefined): number {
  const n = Number(value);
  if (!Number.isFinite(n)) return 50;
  return Math.max(1, Math.min(500, Math.floor(n)));
}

function setNoStoreHeaders(res: NextApiResponse): void {
  res.setHeader(
    "Cache-Control",
    "no-store, no-cache, must-revalidate, proxy-revalidate",
  );
  res.setHeader("Pragma", "no-cache");
  res.setHeader("Expires", "0");
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

  const source = parseSourceFilter(firstQueryValue(req.query.source));
  if (!source) {
    return res.status(400).json({
      error: "invalid_source",
      message: "Use source=all|pipeline|worker",
    });
  }
  const jobKind = parseJobKindFilter(firstQueryValue(req.query.jobKind));
  if (!jobKind) {
    return res.status(400).json({
      error: "invalid_job_kind",
      message:
        "Use jobKind=all|discover|load_candles|prepare|worker|promotion",
    });
  }
  const nowMs = Date.now();
  const fromMsRaw = parseOptionalMs(firstQueryValue(req.query.fromMs));
  const toMsRaw = parseOptionalMs(firstQueryValue(req.query.toMs));
  const fromMs = fromMsRaw ?? nowMs - 7 * 24 * 60 * 60 * 1000;
  const toMs = toMsRaw ?? nowMs;
  const limit = parseLimit(firstQueryValue(req.query.limit));
  const sessionRaw = firstQueryValue(req.query.session);
  const entrySessionProfile = sessionRaw
    ? normalizeScalpEntrySessionProfile(sessionRaw, "berlin")
    : undefined;

  try {
    const runs = await listScalpDurationTimelineRuns({
      source,
      jobKind,
      entrySessionProfile,
      fromMs,
      toMs,
      limit,
    });
    return res.status(200).json({
      ok: true,
      generatedAtMs: nowMs,
      filters: {
        source,
        jobKind,
        entrySessionProfile: entrySessionProfile || null,
        fromMs,
        toMs,
        limit,
      },
      runs,
    });
  } catch (err: any) {
    return res.status(500).json({
      error: "scalp_duration_timeline_failed",
      message: err?.message || String(err),
    });
  }
}
