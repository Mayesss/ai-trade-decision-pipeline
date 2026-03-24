export const config = { runtime: "nodejs" };

import type { NextApiRequest, NextApiResponse } from "next";

import { requireAdminAccess } from "../../../../lib/admin";
import {
  ADAPTIVE_META_SELECTOR_M15_M3_STRATEGY_ID,
} from "../../../../lib/scalp/adaptive/types";
import {
  listScalpAdaptiveSelectorDecisions,
  listScalpAdaptiveSelectorSnapshots,
} from "../../../../lib/scalp/pg/adaptive";
import {
  listScalpEntrySessionProfiles,
  parseScalpEntrySessionProfileStrict,
} from "../../../../lib/scalp/sessions";

function firstQueryValue(
  value: string | string[] | undefined,
): string | undefined {
  if (typeof value === "string") return value.trim() || undefined;
  if (Array.isArray(value) && value.length > 0) {
    return String(value[0] || "").trim() || undefined;
  }
  return undefined;
}

function parseIntQuery(
  value: string | undefined,
  fallback: number,
  min: number,
  max: number,
): number {
  const n = Number(value);
  if (!Number.isFinite(n)) return fallback;
  return Math.max(min, Math.min(max, Math.floor(n)));
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

  const generatedAtMs = Date.now();
  const symbol = String(firstQueryValue(req.query.symbol) || "")
    .trim()
    .toUpperCase();
  if (!symbol) {
    return res.status(400).json({
      error: "symbol_required",
      message: "Use ?symbol=<SYMBOL>",
      generatedAtMs,
    });
  }
  const sessionRaw = firstQueryValue(req.query.session);
  const session = parseScalpEntrySessionProfileStrict(sessionRaw || "berlin");
  if (!session) {
    return res.status(400).json({
      error: "invalid_session",
      message: `Use session=${listScalpEntrySessionProfiles().join("|")}.`,
      generatedAtMs,
    });
  }

  const limit = parseIntQuery(firstQueryValue(req.query.limit), 50, 1, 500);
  const hours = parseIntQuery(firstQueryValue(req.query.hours), 0, 0, 24 * 180);

  try {
    const [snapshots, recentDecisions] = await Promise.all([
      listScalpAdaptiveSelectorSnapshots({
        symbol,
        entrySessionProfile: session,
        strategyId: ADAPTIVE_META_SELECTOR_M15_M3_STRATEGY_ID,
        status: "all",
        limit: 64,
      }),
      listScalpAdaptiveSelectorDecisions({
        symbol,
        entrySessionProfile: session,
        strategyId: ADAPTIVE_META_SELECTOR_M15_M3_STRATEGY_ID,
        hours,
        limit,
      }),
    ]);

    const activeSnapshot = snapshots.find((row) => row.status === "active") || null;
    const latestCandidateSnapshot =
      snapshots.find((row) => row.status === "shadow") ||
      snapshots.find((row) => row.status !== "active") ||
      null;

    const total = recentDecisions.length;
    const patternCount = recentDecisions.filter(
      (row) => row.selectedArmType === "pattern",
    ).length;
    const incumbentCount = recentDecisions.filter(
      (row) => row.selectedArmType === "incumbent",
    ).length;
    const skipCount = recentDecisions.filter(
      (row) => row.selectedArmType === "none",
    ).length;
    const confidenceRows = recentDecisions
      .map((row) => Number(row.confidence))
      .filter((row) => Number.isFinite(row));
    const avgConfidence = confidenceRows.length
      ? confidenceRows.reduce((acc, row) => acc + row, 0) / confidenceRows.length
      : 0;

    return res.status(200).json({
      ok: true,
      generatedAtMs,
      symbol,
      session,
      strategyId: ADAPTIVE_META_SELECTOR_M15_M3_STRATEGY_ID,
      activeSnapshot: activeSnapshot
        ? {
            snapshotId: activeSnapshot.snapshotId,
            trainedAtMs: activeSnapshot.trainedAtMs,
            lockUntilMs: activeSnapshot.lockUntilMs,
            locked:
              Number.isFinite(Number(activeSnapshot.lockUntilMs)) &&
              Number(activeSnapshot.lockUntilMs) > generatedAtMs,
            lockStartedAtMs: activeSnapshot.lockStartedAtMs,
            baselineMaxDrawdownR: activeSnapshot.baselineMaxDrawdownR,
          }
        : null,
      latestCandidateSnapshot: latestCandidateSnapshot
        ? {
            snapshotId: latestCandidateSnapshot.snapshotId,
            status: latestCandidateSnapshot.status,
            trainedAtMs: latestCandidateSnapshot.trainedAtMs,
            windowFromTs: latestCandidateSnapshot.windowFromTs,
            windowToTs: latestCandidateSnapshot.windowToTs,
          }
        : null,
      selectionStats: {
        patternPct: total > 0 ? (patternCount / total) * 100 : 0,
        incumbentPct: total > 0 ? (incumbentCount / total) * 100 : 0,
        skipPct: total > 0 ? (skipCount / total) * 100 : 0,
        avgConfidence,
        samples: total,
      },
      recentDecisions: recentDecisions.map((row) => ({
        tsMs: row.tsMs,
        deploymentId: row.deploymentId,
        snapshotId: row.snapshotId,
        selectedArmType: row.selectedArmType,
        selectedArmId: row.selectedArmId,
        confidence: row.confidence,
        skipReason: row.skipReason,
        reasonCodes: row.reasonCodes,
        featuresHash: row.featuresHash,
        details: row.details,
      })),
    });
  } catch (err: any) {
    return res.status(500).json({
      error: "scalp_adaptive_summary_failed",
      message: err?.message || String(err),
      generatedAtMs,
    });
  }
}
