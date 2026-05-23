import {
  defaultScalpExecutionPersistenceAdapter,
  type ScalpExecutionPersistenceAdapter,
} from "../scalp/persistence";
import type {
  ScalpDeploymentKeyOptions,
  ScalpStrategyRuntimeSnapshot,
} from "../scalp/store";
import type {
  ScalpJournalEntry,
  ScalpSessionState,
  ScalpTradeLedgerAppendResult,
  ScalpTradeLedgerEntry,
} from "../scalp/types";

import {
  appendScalpV2JournalEntry,
  appendScalpV2LedgerRow,
  loadScalpV2DeploymentById,
  loadScalpV2SessionState,
  upsertScalpV2SessionState,
} from "./db";
import { deriveCloseTypeFromReasonCodes, toDeploymentId } from "./logic";
import { resolveBitgetBrokerCloseLedger } from "./bitgetCloseHistory";
import type { ScalpV2Session, ScalpV2Venue } from "./types";

function inferVenueFromDeploymentId(value: unknown): ScalpV2Venue {
  return String(value || "").trim().toLowerCase().startsWith("capital:")
    ? "capital"
    : "bitget";
}

function inferSessionFromTuneOrDeploymentId(value: unknown): ScalpV2Session {
  const raw = String(value || "")
    .trim()
    .toLowerCase();
  const match = raw.match(/__sp_([a-z]+)/);
  if (!match?.[1]) return "berlin";
  const normalized = match[1];
  if (normalized === "tokyo") return "tokyo";
  if (normalized === "newyork") return "newyork";
  if (normalized === "pacific") return "pacific";
  if (normalized === "sydney") return "sydney";
  return "berlin";
}

function asRecord(value: unknown): Record<string, unknown> {
  if (!value || typeof value !== "object" || Array.isArray(value)) return {};
  return value as Record<string, unknown>;
}

function resolveDeploymentId(
  symbol: string,
  strategyId: string | undefined,
  opts: ScalpDeploymentKeyOptions | undefined,
): string | null {
  const direct = String(opts?.deploymentId || "").trim();
  if (direct) return direct;
  const normalizedSymbol = String(symbol || "")
    .trim()
    .toUpperCase();
  const normalizedStrategyId = String(strategyId || "")
    .trim()
    .toLowerCase();
  const tuneId = String(opts?.tuneId || "")
    .trim()
    .toLowerCase();
  if (!normalizedSymbol || !normalizedStrategyId || !tuneId) return null;
  const venue = opts?.venue || inferVenueFromDeploymentId(opts?.deploymentId);
  const session = inferSessionFromTuneOrDeploymentId(tuneId);
  return toDeploymentId({
    venue,
    symbol: normalizedSymbol,
    strategyId: normalizedStrategyId,
    tuneId,
    session,
  });
}

export function createScalpV2ExecutionPersistenceAdapter(params: {
  runtimeSnapshot?: ScalpStrategyRuntimeSnapshot;
} = {}): ScalpExecutionPersistenceAdapter {
  const deploymentCache = new Map<
    string,
    Awaited<ReturnType<typeof loadScalpV2DeploymentById>>
  >();

  async function loadDeploymentCached(deploymentId: string) {
    const key = String(deploymentId || "").trim();
    if (!key) return null;
    if (deploymentCache.has(key)) return deploymentCache.get(key) || null;
    const row = await loadScalpV2DeploymentById(key);
    deploymentCache.set(key, row);
    return row;
  }

  return {
    loadRuntimeSnapshot: async (
      envEnabled: boolean,
      preferredStrategyId?: string,
    ): Promise<ScalpStrategyRuntimeSnapshot> => {
      if (params.runtimeSnapshot) return params.runtimeSnapshot;
      return defaultScalpExecutionPersistenceAdapter.loadRuntimeSnapshot(
        envEnabled,
        preferredStrategyId,
      );
    },
    loadSessionState: async (
      symbol: string,
      dayKey: string,
      strategyId?: string,
      opts?: ScalpDeploymentKeyOptions,
    ): Promise<ScalpSessionState | null> => {
      const deploymentId = resolveDeploymentId(symbol, strategyId, opts);
      if (!deploymentId) return null;
      return loadScalpV2SessionState({
        deploymentId,
        dayKey,
      });
    },
    saveSessionState: async (state: ScalpSessionState): Promise<void> => {
      await upsertScalpV2SessionState({
        ...state,
        version: 2,
      });
    },
    appendJournal: async (entry: ScalpJournalEntry): Promise<void> => {
      const payload = asRecord(entry.payload);
      const deploymentId = String(payload.deploymentId || "").trim() || null;
      const deployment = deploymentId
        ? await loadDeploymentCached(deploymentId)
        : null;
      await appendScalpV2JournalEntry({
        entry,
        deploymentId,
        venue: deployment?.venue || null,
        strategyId: deployment?.strategyId || null,
        tuneId: deployment?.tuneId || null,
        entrySessionProfile: deployment?.entrySessionProfile || null,
      });
    },
    appendTradeLedgerEntry: async (
      entry: ScalpTradeLedgerEntry,
    ): Promise<ScalpTradeLedgerAppendResult> => {
      const deploymentId = String(entry.deploymentId || "").trim();
      if (!deploymentId) {
        return { ok: false, reasonCodes: ["LEDGER_WRITE_SKIPPED_NO_DEPLOYMENT"] };
      }
      const deployment = await loadDeploymentCached(deploymentId);
      const venue = deployment?.venue || inferVenueFromDeploymentId(deploymentId);
      const session =
        deployment?.entrySessionProfile ||
        inferSessionFromTuneOrDeploymentId(entry.tuneId || deploymentId);

      let rMultiple = Number.isFinite(Number(entry.rMultiple))
        ? Number(entry.rMultiple)
        : 0;
      let pnlUsd =
        entry.pnlUsd !== null &&
        entry.pnlUsd !== undefined &&
        Number.isFinite(Number(entry.pnlUsd))
          ? Number(entry.pnlUsd)
          : null;
      let sourceOfTruth = entry.sourceOfTruth || "system";
      let exitRef = entry.exitRef ?? null;
      let tsExitMs = Number.isFinite(Number(entry.exitAtMs))
        ? Number(entry.exitAtMs)
        : Date.now();
      let reasonCodes = entry.reasonCodes || [];
      let rawPayload: Record<string, unknown> = {
        side: entry.side || null,
        dryRun: Boolean(entry.dryRun),
        timestampMs: entry.timestampMs,
        ...(entry.rawPayload || {}),
      };

      if (venue === "bitget" && !entry.dryRun) {
        const brokerClose = await resolveBitgetBrokerCloseLedger({
          symbol: entry.symbol,
          side: entry.side,
          dealReference: entry.dealReference,
          brokerOrderId: entry.brokerOrderId,
          openedAtMs: entry.openedAtMs,
          exitAtMs: tsExitMs,
          riskUsd: entry.riskUsd,
        });
        if (!brokerClose.found) {
          await appendScalpV2JournalEntry({
            entry: {
              id: `ledger-pending-${entry.id || Date.now()}`,
              timestampMs: Date.now(),
              type: "error",
              symbol: String(entry.symbol || "").trim().toUpperCase() || null,
              dayKey: null,
              level: "warn",
              reasonCodes: ["LEDGER_BROKER_CLOSE_PENDING", ...brokerClose.reasonCodes],
              payload: {
                deploymentId,
                dryRun: Boolean(entry.dryRun),
                exitAtMs: tsExitMs,
                ...brokerClose.rawPayload,
              },
            },
            deploymentId,
            venue,
            strategyId: deployment?.strategyId || null,
            tuneId: deployment?.tuneId || null,
            entrySessionProfile: session,
          });
          return {
            ok: false,
            pending: true,
            reasonCodes: ["LEDGER_BROKER_CLOSE_PENDING", ...brokerClose.reasonCodes],
          };
        }
        rMultiple = brokerClose.rMultiple;
        pnlUsd = brokerClose.pnlUsd;
        sourceOfTruth = "broker";
        exitRef = brokerClose.brokerRef;
        tsExitMs = brokerClose.tsExitMs || tsExitMs;
        reasonCodes = [...reasonCodes, ...brokerClose.reasonCodes];
        rawPayload = {
          ...rawPayload,
          ...brokerClose.rawPayload,
          riskUsd: entry.riskUsd ?? null,
        };
      }

      const inserted = await appendScalpV2LedgerRow({
        id: String(entry.id || "").trim(),
        tsExitMs,
        deploymentId,
        venue,
        symbol: String(entry.symbol || "").trim().toUpperCase(),
        strategyId: String(entry.strategyId || "").trim().toLowerCase(),
        tuneId: String(entry.tuneId || "").trim().toLowerCase(),
        entrySessionProfile: session,
        entryRef: entry.entryRef ?? entry.dealReference ?? null,
        exitRef,
        closeType: deriveCloseTypeFromReasonCodes(entry.reasonCodes || []),
        rMultiple,
        pnlUsd,
        sourceOfTruth,
        reasonCodes,
        rawPayload,
      });
      return {
        ok: inserted,
        reasonCodes: inserted ? ["LEDGER_WRITE_CONFIRMED"] : ["LEDGER_WRITE_SKIPPED_DUPLICATE"],
      };
    },
    tryAcquireRunLock: async (): Promise<boolean> => true,
    releaseRunLock: async (): Promise<void> => undefined,
  };
}
