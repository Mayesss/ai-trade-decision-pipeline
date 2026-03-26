import crypto from "crypto";

import { fetchCapitalOpenPositionSnapshots } from "../capital";
import { getScalpVenueAdapter } from "../scalp/adapters";
import { runScalpExecuteCycle } from "../scalp/engine";

import { getScalpV2RuntimeConfig } from "./config";
import {
  appendScalpV2ExecutionEvent,
  buildScalpV2JobResult,
  claimScalpV2Job,
  enforceScalpV2EnabledCap,
  finalizeScalpV2Job,
  listScalpV2Candidates,
  listScalpV2Deployments,
  listScalpV2OpenPositions,
  loadScalpV2RuntimeConfig,
  snapshotScalpV2DailyMetrics,
  toDeploymentId,
  trimScalpV2CandidatesByBudget,
  updateScalpV2CandidateStatuses,
  upsertScalpV2Candidates,
  upsertScalpV2Deployments,
  upsertScalpV2PositionSnapshot,
} from "./db";
import { enforceCandidateBudgets, isScalpV2DiscoverSymbolAllowed } from "./logic";
import type {
  ScalpV2ExecutionEvent,
  ScalpV2JobResult,
  ScalpV2Session,
  ScalpV2Venue,
} from "./types";

function hashScoreSeed(value: string): number {
  let hash = 0;
  const input = String(value || "");
  for (let idx = 0; idx < input.length; idx += 1) {
    hash = (hash << 5) - hash + input.charCodeAt(idx);
    hash |= 0;
  }
  const positive = Math.abs(hash);
  return positive % 1000;
}

function nowMs(): number {
  return Date.now();
}

function buildEvent(params: {
  deploymentId: string;
  venue: ScalpV2Venue;
  symbol: string;
  strategyId: string;
  tuneId: string;
  entrySessionProfile: ScalpV2Session;
  eventType: ScalpV2ExecutionEvent["eventType"];
  reasonCodes?: string[];
  brokerRef?: string | null;
  rawPayload?: Record<string, unknown>;
  sourceOfTruth?: ScalpV2ExecutionEvent["sourceOfTruth"];
}): ScalpV2ExecutionEvent {
  return {
    id: crypto.randomUUID(),
    tsMs: nowMs(),
    deploymentId: params.deploymentId,
    venue: params.venue,
    symbol: params.symbol,
    strategyId: params.strategyId,
    tuneId: params.tuneId,
    entrySessionProfile: params.entrySessionProfile,
    eventType: params.eventType,
    brokerRef: params.brokerRef || null,
    reasonCodes: params.reasonCodes || [],
    sourceOfTruth: params.sourceOfTruth || "system",
    rawPayload: params.rawPayload || {},
  };
}

function lockOwner(jobKind: string): string {
  return `scalp_v2_${jobKind}_${nowMs()}_${Math.floor(Math.random() * 1_000_000)}`;
}

function supportsScalpV2LiveExecution(venue: ScalpV2Venue): boolean {
  // Capital execution adapter is not wired into scalp-v2 yet.
  return venue === "bitget";
}

export async function runScalpV2DiscoverJob(): Promise<ScalpV2JobResult> {
  const owner = lockOwner("discover");
  const claimed = await claimScalpV2Job({ jobKind: "discover", lockOwner: owner });
  if (!claimed) {
    return buildScalpV2JobResult({
      jobKind: "discover",
      processed: 0,
      succeeded: 0,
      failed: 0,
      busy: true,
      pendingAfter: 0,
      details: { reason: "job_locked" },
    });
  }

  let ok = true;
  let processed = 0;
  let succeeded = 0;
  let failed = 0;
  let details: Record<string, unknown> = {};

  try {
    const runtime = await loadScalpV2RuntimeConfig();
    if (!runtime.enabled) {
      details = { skipped: true, reason: "SCALP_V2_DISABLED" };
      return buildScalpV2JobResult({
        jobKind: "discover",
        processed,
        succeeded,
        failed,
        pendingAfter: 0,
        details,
      });
    }

    const rows: Parameters<typeof upsertScalpV2Candidates>[0]["rows"] = [];

    let droppedByVenuePolicy = 0;
    for (const venue of runtime.supportedVenues) {
      const symbols = runtime.seedSymbolsByVenue[venue] || [];
      for (const symbol of symbols) {
        if (!isScalpV2DiscoverSymbolAllowed(venue, symbol)) {
          droppedByVenuePolicy += 1;
          continue;
        }
        for (const session of runtime.supportedSessions) {
          const score = 50 + hashScoreSeed(`${venue}:${symbol}:${session}`) / 100;
          rows.push({
            venue,
            symbol,
            strategyId: runtime.defaultStrategyId,
            tuneId: runtime.defaultTuneId,
            entrySessionProfile: session,
            score,
            status: "discovered",
            reasonCodes: ["SCALP_V2_DISCOVERY_SEED"],
            metadata: {
              discoveredAtMs: nowMs(),
              source: "seed_universe",
            },
          });
        }
      }
    }

    processed = rows.length;
    await upsertScalpV2Candidates({ rows });
    const trim = await trimScalpV2CandidatesByBudget({
      maxCandidatesTotal: runtime.budgets.maxCandidatesTotal,
      maxCandidatesPerSymbol: runtime.budgets.maxCandidatesPerSymbol,
    });
    succeeded = rows.length;
    details = {
      insertedOrUpdated: rows.length,
      trimmed: trim.deleted,
      droppedByVenuePolicy,
      budgets: runtime.budgets,
    };

    return buildScalpV2JobResult({
      jobKind: "discover",
      processed,
      succeeded,
      failed,
      pendingAfter: 0,
      details,
    });
  } catch (err: any) {
    ok = false;
    failed = Math.max(1, failed);
    details = { error: err?.message || String(err) };
    return buildScalpV2JobResult({
      jobKind: "discover",
      processed,
      succeeded,
      failed,
      pendingAfter: 0,
      details,
    });
  } finally {
    await finalizeScalpV2Job({
      jobKind: "discover",
      lockOwner: owner,
      ok,
      details,
    });
  }
}

export async function runScalpV2EvaluateJob(params: {
  batchSize?: number;
} = {}): Promise<ScalpV2JobResult> {
  const owner = lockOwner("evaluate");
  const claimed = await claimScalpV2Job({ jobKind: "evaluate", lockOwner: owner });
  if (!claimed) {
    return buildScalpV2JobResult({
      jobKind: "evaluate",
      processed: 0,
      succeeded: 0,
      failed: 0,
      busy: true,
      pendingAfter: 0,
      details: { reason: "job_locked" },
    });
  }

  let ok = true;
  let processed = 0;
  let succeeded = 0;
  let failed = 0;
  let details: Record<string, unknown> = {};

  try {
    const runtime = await loadScalpV2RuntimeConfig();
    const batchSize = Math.max(1, Math.min(2_000, Math.floor(params.batchSize || 200)));
    const candidates = await listScalpV2Candidates({ status: "discovered", limit: batchSize });
    if (!candidates.length) {
      details = { evaluated: 0, reason: "no_discovered_candidates" };
      return buildScalpV2JobResult({
        jobKind: "evaluate",
        processed: 0,
        succeeded: 0,
        failed: 0,
        pendingAfter: 0,
        details,
      });
    }

    const evaluatedRows: Parameters<typeof upsertScalpV2Candidates>[0]["rows"] = candidates.map((candidate) => ({
      venue: candidate.venue,
      symbol: candidate.symbol,
      strategyId: candidate.strategyId,
      tuneId: candidate.tuneId,
      entrySessionProfile: candidate.entrySessionProfile,
      score:
        candidate.score +
        hashScoreSeed(
          `${candidate.venue}:${candidate.symbol}:${candidate.entrySessionProfile}:${candidate.strategyId}`,
        ) /
          100,
      status: "evaluated",
      reasonCodes: ["SCALP_V2_EVALUATED"],
      metadata: {
        evaluatedAtMs: nowMs(),
        evaluator: "v2_alpha",
        liveEnabled: runtime.liveEnabled,
      },
    }));

    await upsertScalpV2Candidates({ rows: evaluatedRows });
    processed = candidates.length;
    succeeded = candidates.length;

    details = {
      evaluated: candidates.length,
      batchSize,
    };

    return buildScalpV2JobResult({
      jobKind: "evaluate",
      processed,
      succeeded,
      failed,
      pendingAfter: 0,
      details,
    });
  } catch (err: any) {
    ok = false;
    failed = Math.max(1, failed);
    details = { error: err?.message || String(err) };
    return buildScalpV2JobResult({
      jobKind: "evaluate",
      processed,
      succeeded,
      failed,
      pendingAfter: 0,
      details,
    });
  } finally {
    await finalizeScalpV2Job({
      jobKind: "evaluate",
      lockOwner: owner,
      ok,
      details,
    });
  }
}

export async function runScalpV2PromoteJob(): Promise<ScalpV2JobResult> {
  const owner = lockOwner("promote");
  const claimed = await claimScalpV2Job({ jobKind: "promote", lockOwner: owner });
  if (!claimed) {
    return buildScalpV2JobResult({
      jobKind: "promote",
      processed: 0,
      succeeded: 0,
      failed: 0,
      busy: true,
      pendingAfter: 0,
      details: { reason: "job_locked" },
    });
  }

  let ok = true;
  let processed = 0;
  let succeeded = 0;
  let failed = 0;
  let details: Record<string, unknown> = {};

  try {
    const runtime = await loadScalpV2RuntimeConfig();
    const allEvaluated = await listScalpV2Candidates({ status: "evaluated", limit: 10_000 });
    if (!allEvaluated.length) {
      details = { promoted: 0, reason: "no_evaluated_candidates" };
      return buildScalpV2JobResult({
        jobKind: "promote",
        processed: 0,
        succeeded: 0,
        failed: 0,
        pendingAfter: 0,
        details,
      });
    }

    const trimmed = enforceCandidateBudgets({
      candidates: allEvaluated,
      budgets: runtime.budgets,
    });

    const enabledSlots = runtime.budgets.maxEnabledDeployments;
    let enabledUsed = 0;

    const rows: Parameters<typeof upsertScalpV2Deployments>[0]["rows"] = [];
    for (const candidate of trimmed.kept) {
      const isSeedLive = (runtime.seedLiveSymbolsByVenue[candidate.venue] || []).includes(candidate.symbol);
      const enable =
        isSeedLive &&
        supportsScalpV2LiveExecution(candidate.venue) &&
        enabledUsed < enabledSlots;
      if (enable) enabledUsed += 1;

      rows.push({
        candidateId: candidate.id,
        venue: candidate.venue,
        symbol: candidate.symbol,
        strategyId: candidate.strategyId,
        tuneId: candidate.tuneId,
        entrySessionProfile: candidate.entrySessionProfile,
        enabled: enable,
        liveMode: enable && runtime.liveEnabled ? "live" : "shadow",
        promotionGate: {
          eligible: true,
          reason: null,
          source: "v2_budgeted_auto",
          promotedAtMs: nowMs(),
          score: candidate.score,
        },
        riskProfile: runtime.riskProfile,
      });
    }

    await upsertScalpV2Deployments({ rows });
    await updateScalpV2CandidateStatuses({
      ids: trimmed.kept.map((row) => row.id),
      status: "promoted",
      metadataPatch: { promotedAtMs: nowMs() },
    });
    await updateScalpV2CandidateStatuses({
      ids: trimmed.dropped.map((row) => row.id),
      status: "rejected",
      metadataPatch: { rejectedAtMs: nowMs(), reason: "BUDGET_CAP" },
    });

    const capOut = await enforceScalpV2EnabledCap({
      maxEnabledDeployments: runtime.budgets.maxEnabledDeployments,
    });

    processed = allEvaluated.length;
    succeeded = rows.length;
    details = {
      considered: allEvaluated.length,
      promoted: trimmed.kept.length,
      rejectedByBudget: trimmed.dropped.length,
      demotedByEnabledCap: capOut.demoted,
      enabledSlots,
      liveEnabled: runtime.liveEnabled,
    };

    return buildScalpV2JobResult({
      jobKind: "promote",
      processed,
      succeeded,
      failed,
      pendingAfter: 0,
      details,
    });
  } catch (err: any) {
    ok = false;
    failed = Math.max(1, failed);
    details = { error: err?.message || String(err) };
    return buildScalpV2JobResult({
      jobKind: "promote",
      processed,
      succeeded,
      failed,
      pendingAfter: 0,
      details,
    });
  } finally {
    await finalizeScalpV2Job({
      jobKind: "promote",
      lockOwner: owner,
      ok,
      details,
    });
  }
}

function pickBitgetSnapshotBySymbol(
  snapshots: Array<{
    epic: string;
    dealId: string | null;
    dealReference: string | null;
    side: "long" | "short" | null;
    entryPrice: number | null;
    leverage: number | null;
    size: number | null;
    updatedAtMs: number;
  }>,
  symbol: string,
) {
  const target = String(symbol || "").trim().toUpperCase();
  return snapshots.find((row) => String(row.epic || "").trim().toUpperCase() === target) || null;
}

function pickCapitalSnapshotBySymbol(
  snapshots: Array<{
    epic: string;
    dealId: string | null;
    dealReference: string | null;
    side: "long" | "short" | null;
    entryPrice: number | null;
    leverage: number | null;
    size: number | null;
    updatedAtMs: number;
  }>,
  symbol: string,
) {
  const target = String(symbol || "").trim().toUpperCase();
  return snapshots.find((row) => String(row.epic || "").trim().toUpperCase() === target) || null;
}

export async function runScalpV2ExecuteJob(params: {
  dryRun?: boolean;
  session?: ScalpV2Session;
  venue?: ScalpV2Venue;
} = {}): Promise<ScalpV2JobResult> {
  const owner = lockOwner("execute");
  const claimed = await claimScalpV2Job({ jobKind: "execute", lockOwner: owner });
  if (!claimed) {
    return buildScalpV2JobResult({
      jobKind: "execute",
      processed: 0,
      succeeded: 0,
      failed: 0,
      busy: true,
      pendingAfter: 0,
      details: { reason: "job_locked" },
    });
  }

  let ok = true;
  let processed = 0;
  let succeeded = 0;
  let failed = 0;
  let details: Record<string, unknown> = {};

  try {
    const runtime = await loadScalpV2RuntimeConfig();
    const effectiveDryRun = params.dryRun ?? runtime.dryRunDefault;

    const deployments = await listScalpV2Deployments({
      enabledOnly: true,
      venue: params.venue,
      session: params.session,
      limit: 500,
    });

    if (!deployments.length) {
      details = { executed: 0, reason: "no_enabled_deployments" };
      return buildScalpV2JobResult({
        jobKind: "execute",
        processed: 0,
        succeeded: 0,
        failed: 0,
        pendingAfter: 0,
        details,
      });
    }

    const bitgetAdapter = getScalpVenueAdapter("bitget");
    const bitgetSnapshots = await bitgetAdapter.broker.fetchOpenPositionSnapshots();
    const capitalSnapshots = await fetchCapitalOpenPositionSnapshots().catch(() => []);

    for (const deployment of deployments) {
      processed += 1;
      const deploymentDryRun = effectiveDryRun || !runtime.liveEnabled || deployment.liveMode !== "live";
      try {
        if (deployment.venue === "bitget") {
          const result = await runScalpExecuteCycle({
            venue: "bitget" as any,
            symbol: deployment.symbol,
            strategyId: deployment.strategyId,
            tuneId: deployment.tuneId,
            deploymentId: deployment.deploymentId,
            dryRun: deploymentDryRun,
          });

          await appendScalpV2ExecutionEvent(
            buildEvent({
              deploymentId: deployment.deploymentId,
              venue: deployment.venue,
              symbol: deployment.symbol,
              strategyId: deployment.strategyId,
              tuneId: deployment.tuneId,
              entrySessionProfile: deployment.entrySessionProfile,
              eventType: "position_snapshot",
              reasonCodes: result.reasonCodes,
              sourceOfTruth: "system",
              rawPayload: {
                state: result.state,
                dryRun: result.dryRun,
                runLockAcquired: result.runLockAcquired,
              },
            }),
          );

          const snapshot = pickBitgetSnapshotBySymbol(bitgetSnapshots, deployment.symbol);
          await upsertScalpV2PositionSnapshot({
            deploymentId: deployment.deploymentId,
            venue: deployment.venue,
            symbol: deployment.symbol,
            side: snapshot?.side || null,
            entryPrice: snapshot?.entryPrice ?? null,
            leverage: snapshot?.leverage ?? null,
            size: snapshot?.size ?? null,
            dealId: snapshot?.dealId ?? null,
            dealReference: snapshot?.dealReference ?? null,
            brokerSnapshotAtMs: snapshot?.updatedAtMs ?? nowMs(),
            status: snapshot?.side ? "open" : "flat",
            rawPayload: snapshot ? { snapshot } : {},
          });
        } else {
          const snapshot = pickCapitalSnapshotBySymbol(capitalSnapshots, deployment.symbol);
          await upsertScalpV2PositionSnapshot({
            deploymentId: deployment.deploymentId,
            venue: deployment.venue,
            symbol: deployment.symbol,
            side: snapshot?.side || null,
            entryPrice: snapshot?.entryPrice ?? null,
            leverage: snapshot?.leverage ?? null,
            size: snapshot?.size ?? null,
            dealId: snapshot?.dealId ?? null,
            dealReference: snapshot?.dealReference ?? null,
            brokerSnapshotAtMs: snapshot?.updatedAtMs ?? nowMs(),
            status: snapshot?.side ? "open" : "flat",
            rawPayload: snapshot ? { snapshot } : {},
          });

          const reasonCodes = deploymentDryRun
            ? ["SCALP_V2_CAPITAL_SHADOW_ONLY"]
            : ["SCALP_V2_CAPITAL_EXECUTION_NOT_IMPLEMENTED"];

          await appendScalpV2ExecutionEvent(
            buildEvent({
              deploymentId: deployment.deploymentId,
              venue: deployment.venue,
              symbol: deployment.symbol,
              strategyId: deployment.strategyId,
              tuneId: deployment.tuneId,
              entrySessionProfile: deployment.entrySessionProfile,
              eventType: deploymentDryRun ? "position_snapshot" : "order_rejected",
              reasonCodes,
              sourceOfTruth: "broker",
              rawPayload: {
                dryRun: deploymentDryRun,
                snapshot,
              },
            }),
          );
        }
        succeeded += 1;
      } catch (err: any) {
        failed += 1;
        await appendScalpV2ExecutionEvent(
          buildEvent({
            deploymentId: deployment.deploymentId,
            venue: deployment.venue,
            symbol: deployment.symbol,
            strategyId: deployment.strategyId,
            tuneId: deployment.tuneId,
            entrySessionProfile: deployment.entrySessionProfile,
            eventType: "order_rejected",
            reasonCodes: ["SCALP_V2_EXECUTION_ERROR"],
            sourceOfTruth: "system",
            rawPayload: {
              message: err?.message || String(err),
              dryRun: deploymentDryRun,
            },
          }),
        );
      }
    }

    details = {
      executedDeployments: deployments.length,
      dryRun: effectiveDryRun,
      liveEnabled: runtime.liveEnabled,
    };

    return buildScalpV2JobResult({
      jobKind: "execute",
      processed,
      succeeded,
      failed,
      pendingAfter: 0,
      details,
    });
  } catch (err: any) {
    ok = false;
    failed = Math.max(1, failed);
    details = { error: err?.message || String(err) };
    return buildScalpV2JobResult({
      jobKind: "execute",
      processed,
      succeeded,
      failed,
      pendingAfter: 0,
      details,
    });
  } finally {
    await snapshotScalpV2DailyMetrics().catch(() => undefined);
    await finalizeScalpV2Job({
      jobKind: "execute",
      lockOwner: owner,
      ok,
      details,
    });
  }
}

function snapshotExists(params: {
  venue: ScalpV2Venue;
  symbol: string;
  side: "long" | "short" | null;
  dealId: string | null;
  dealReference: string | null;
  bitgetSnapshots: Array<{
    epic: string;
    side: "long" | "short" | null;
    dealId: string | null;
    dealReference: string | null;
  }>;
  capitalSnapshots: Array<{
    epic: string;
    side: "long" | "short" | null;
    dealId: string | null;
    dealReference: string | null;
  }>;
}): boolean {
  const targetList = params.venue === "capital" ? params.capitalSnapshots : params.bitgetSnapshots;
  const symbol = String(params.symbol || "").trim().toUpperCase();

  for (const snapshot of targetList) {
    const sameSymbol = String(snapshot.epic || "").trim().toUpperCase() === symbol;
    if (!sameSymbol) continue;

    if (params.dealId && snapshot.dealId && params.dealId === snapshot.dealId) return true;
    if (
      params.dealReference &&
      snapshot.dealReference &&
      params.dealReference === snapshot.dealReference
    ) {
      return true;
    }
    if (params.side && snapshot.side && params.side === snapshot.side) return true;
    if (!params.dealId && !params.dealReference && !params.side) return true;
  }

  return false;
}

export async function runScalpV2ReconcileJob(): Promise<ScalpV2JobResult> {
  const owner = lockOwner("reconcile");
  const claimed = await claimScalpV2Job({ jobKind: "reconcile", lockOwner: owner });
  if (!claimed) {
    return buildScalpV2JobResult({
      jobKind: "reconcile",
      processed: 0,
      succeeded: 0,
      failed: 0,
      busy: true,
      pendingAfter: 0,
      details: { reason: "job_locked" },
    });
  }

  let ok = true;
  let processed = 0;
  let succeeded = 0;
  let failed = 0;
  let details: Record<string, unknown> = {};

  try {
    const openPositions = await listScalpV2OpenPositions();
    const bitgetAdapter = getScalpVenueAdapter("bitget");
    const bitgetSnapshots = await bitgetAdapter.broker.fetchOpenPositionSnapshots();
    const capitalSnapshots = await fetchCapitalOpenPositionSnapshots().catch(() => []);

    for (const position of openPositions) {
      processed += 1;
      const exists = snapshotExists({
        venue: position.venue,
        symbol: position.symbol,
        side: position.side,
        dealId: position.dealId,
        dealReference: position.dealReference,
        bitgetSnapshots,
        capitalSnapshots,
      });

      if (exists) {
        succeeded += 1;
        continue;
      }

      try {
        await appendScalpV2ExecutionEvent(
          buildEvent({
            deploymentId: position.deploymentId,
            venue: position.venue,
            symbol: position.symbol,
            strategyId: "unknown",
            tuneId: "unknown",
            entrySessionProfile: "berlin",
            eventType: "reconcile_close",
            reasonCodes: ["SCALP_V2_RECONCILE_CLOSE"],
            sourceOfTruth: "reconciler",
            rawPayload: {
              reason: "broker_position_missing",
              dealId: position.dealId,
              dealReference: position.dealReference,
              rMultiple: 0,
              pnlUsd: null,
            },
          }),
        );

        await upsertScalpV2PositionSnapshot({
          deploymentId: position.deploymentId,
          venue: position.venue,
          symbol: position.symbol,
          side: null,
          entryPrice: null,
          leverage: null,
          size: null,
          dealId: null,
          dealReference: null,
          brokerSnapshotAtMs: nowMs(),
          status: "flat",
          rawPayload: {
            reconciledAtMs: nowMs(),
            reason: "broker_position_missing",
          },
        });
        succeeded += 1;
      } catch {
        failed += 1;
      }
    }

    await snapshotScalpV2DailyMetrics().catch(() => undefined);

    details = {
      examinedOpenPositions: openPositions.length,
      reconciled: Math.max(0, succeeded - (openPositions.length - processed)),
    };

    return buildScalpV2JobResult({
      jobKind: "reconcile",
      processed,
      succeeded,
      failed,
      pendingAfter: 0,
      details,
    });
  } catch (err: any) {
    ok = false;
    failed = Math.max(1, failed);
    details = { error: err?.message || String(err) };
    return buildScalpV2JobResult({
      jobKind: "reconcile",
      processed,
      succeeded,
      failed,
      pendingAfter: 0,
      details,
    });
  } finally {
    await finalizeScalpV2Job({
      jobKind: "reconcile",
      lockOwner: owner,
      ok,
      details,
    });
  }
}

export async function runScalpV2FullAutoCycle(params: {
  executeDryRun?: boolean;
  venue?: ScalpV2Venue;
  session?: ScalpV2Session;
} = {}): Promise<{
  discover: ScalpV2JobResult;
  evaluate: ScalpV2JobResult;
  promote: ScalpV2JobResult;
  execute: ScalpV2JobResult;
  reconcile: ScalpV2JobResult;
}> {
  const discover = await runScalpV2DiscoverJob();
  const evaluate = await runScalpV2EvaluateJob();
  const promote = await runScalpV2PromoteJob();
  const execute = await runScalpV2ExecuteJob({
    dryRun: params.executeDryRun,
    venue: params.venue,
    session: params.session,
  });
  const reconcile = await runScalpV2ReconcileJob();
  return { discover, evaluate, promote, execute, reconcile };
}
