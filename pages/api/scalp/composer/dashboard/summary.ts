export const config = { runtime: "nodejs" };

import type { NextApiRequest, NextApiResponse } from "next";

import { requireAdminAccess } from "../../../../../lib/admin";
import {
  listScalpComposerExecutionEvents,
  listScalpComposerDeployments,
  listScalpComposerJournalRows,
  listScalpComposerJobs,
  listScalpComposerLedgerRows,
  listScalpComposerSessionSnapshots,
  loadScalpComposerRuntimeConfig,
  loadScalpComposerSummary,
} from "../../../../../lib/scalp/composer/db";
import {
  parseBool,
  parseSession,
  parseVenue,
  parseIntBounded,
  setNoStoreHeaders,
} from "../../../../../lib/scalp/composer/http";
import { isScalpRegimeEnabled, loadScalpRegimeCurrentRegimeSnapshot } from "../../../../../lib/scalp/regimes";

// In-memory cache — avoids hammering Neon on every dashboard refresh
let summaryCache: { data: Record<string, unknown>; ts: number; key: string } | null = null;
const CACHE_TTL_MS = 30_000; // 30 seconds

function asPlainRecord(value: unknown): Record<string, unknown> {
  return value && typeof value === "object" && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : {};
}

function compactPromotionGateForDashboard(
  value: unknown,
): Record<string, unknown> | null {
  const gate = asPlainRecord(value);
  if (!Object.keys(gate).length) return null;
  const metadata = asPlainRecord(gate.metadata);
  const out: Record<string, unknown> = {};
  for (const key of [
    "eligible",
    "reason",
    "lifecycle",
    "forwardValidation",
    "holdout",
    "drift",
    "v3ValidationStatus",
    "regimeEnvelope",
  ]) {
    if (gate[key] !== undefined) out[key] = gate[key];
  }
  for (const key of [
    "v3TemporalFilter",
    "brokerSeat",
    "entryBlockReasonCodes",
    "v3Ranking",
  ]) {
    if (gate[key] !== undefined) {
      out[key] = gate[key];
    } else if (metadata[key] !== undefined) {
      out[key] = metadata[key];
    }
  }
  return Object.keys(out).length ? out : null;
}

function compactDeploymentForDashboard(row: Record<string, any>) {
  return {
    deploymentId: row.deploymentId,
    candidateId: row.candidateId,
    venue: row.venue,
    symbol: row.symbol,
    strategyId: row.strategyId,
    tuneId: row.tuneId,
    entrySessionProfile: row.entrySessionProfile,
    enabled: row.enabled,
    liveMode: row.liveMode,
    promotionGate: compactPromotionGateForDashboard(row.promotionGate),
    v4Regime: row.v4Regime || null,
    createdAtMs: row.createdAtMs,
    updatedAtMs: row.updatedAtMs,
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
    const deploymentLimit = parseIntBounded(req.query.deploymentLimit, 10, 1, 500);
    const jobLimit = parseIntBounded(req.query.jobLimit, 20, 5, 100);
    const eventLimit = parseIntBounded(req.query.eventLimit, 240, 20, 2_000);
    const ledgerLimit = parseIntBounded(req.query.ledgerLimit, 300, 20, 5_000);
    const runtimeDeploymentLimit = parseIntBounded(
      req.query.runtimeDeploymentLimit,
      80,
      1,
      500,
    );
    const session = parseSession(req.query.session);
    const venue = parseVenue(req.query.venue);
    const fresh = parseBool(req.query.fresh, false);
    const compactDeployments = parseBool(req.query.compactDeployments, false);

    const cacheKey = `${session || "all"}:${venue || "all"}:${deploymentLimit}:${runtimeDeploymentLimit}:${jobLimit}:${eventLimit}:${ledgerLimit}:${compactDeployments ? "compact" : "full"}`;
    if (!fresh && summaryCache && summaryCache.key === cacheKey && Date.now() - summaryCache.ts < CACHE_TTL_MS) {
      return res.status(200).json(summaryCache.data);
    }

    // Sequential queries — Neon serverless can't handle parallel reliably
    const runtime = await loadScalpComposerRuntimeConfig();
    const summary = await loadScalpComposerSummary();
    const jobs = await listScalpComposerJobs({ limit: jobLimit });
    const deploymentRows = await listScalpComposerDeployments({
      limit: deploymentLimit,
      session,
      venue,
      compactPromotionGate: compactDeployments,
    });
    const runtimeDeploymentIds = deploymentRows
      .filter((row) => row.enabled)
      .map((row) => String(row.deploymentId || "").trim())
      .filter(Boolean)
      .slice(0, runtimeDeploymentLimit);
    const v4RegimeByDeploymentId = new Map<string, Record<string, unknown>>();
    if (isScalpRegimeEnabled()) {
      for (const row of deploymentRows.filter((deployment) => deployment.enabled).slice(0, runtimeDeploymentLimit)) {
        const current = await loadScalpRegimeCurrentRegimeSnapshot({
          venue: row.venue,
          symbol: row.symbol,
          nowMs: Date.now(),
        }).catch(() => ({ cellId: null, stale: true, snapshot: null }));
        const envelope = asPlainRecord(asPlainRecord(row.promotionGate).regimeEnvelope);
        const allowedCells = Array.isArray(envelope.allowedCells)
          ? envelope.allowedCells.map((cell) => String(cell || "")).filter(Boolean)
          : [];
        const enabledDormantByRegime =
          row.enabled &&
          Boolean(envelope.eligible) &&
          Boolean(current.cellId) &&
          !allowedCells.includes(String(current.cellId));
        v4RegimeByDeploymentId.set(String(row.deploymentId), {
          currentCellId: current.cellId,
          stale: current.stale,
          enabledDormantByRegime,
          envelopeStatus: envelope.status || null,
          allowedCells,
        });
      }
    }
    const deploymentsWithV4 = deploymentRows.map((row) => ({
      ...row,
      v4Regime: v4RegimeByDeploymentId.get(String(row.deploymentId)) || null,
    }));
    const deployments = compactDeployments
      ? deploymentsWithV4.map((row) => compactDeploymentForDashboard(row))
      : deploymentsWithV4;
    const events = await listScalpComposerExecutionEvents({
      limit: eventLimit,
      venue,
      session,
    });
    const sessions = runtimeDeploymentIds.length
      ? await listScalpComposerSessionSnapshots({
          deploymentIds: runtimeDeploymentIds,
          limit: runtimeDeploymentIds.length,
        })
      : [];
    const journal = await listScalpComposerJournalRows({
      limit: eventLimit,
      venue,
      session,
    });
    const nowMs = Date.now();
    const THIRTY_DAYS_MS = 30 * 24 * 60 * 60 * 1000;
    const fromTsMs = Math.max(0, nowMs - THIRTY_DAYS_MS);
    const ledger = runtimeDeploymentIds.length
      ? await listScalpComposerLedgerRows({
          deploymentIds: runtimeDeploymentIds,
          fromTsMs,
          toTsMs: nowMs + 1,
          limit: ledgerLimit,
        })
      : [];
    const scopedSummary = {
      ...summary,
      events24h: events.filter((row) => row.tsMs >= nowMs - 24 * 60 * 60 * 1000)
        .length,
      ledgerRows30d: ledger.length,
      netR30d: ledger.reduce((acc, row) => acc + (Number(row.rMultiple) || 0), 0),
    };

    const payload = {
      ok: true,
      mode: "scalp_v2",
      runtime,
      summary: scopedSummary,
      deployments,
      events,
      sessions,
      journal,
      ledger,
      jobs,
      candidates: [],
      researchCursors: [],
      researchHighlights: [],
    };

    summaryCache = { data: payload, ts: Date.now(), key: cacheKey };
    return res.status(200).json(payload);
  } catch (err: any) {
    return res.status(500).json({
      error: "scalp_v2_dashboard_summary_failed",
      message: err?.message || String(err),
    });
  }
}
