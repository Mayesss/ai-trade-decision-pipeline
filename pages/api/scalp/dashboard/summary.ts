export const config = { runtime: "nodejs" };

import type { NextApiRequest, NextApiResponse } from "next";
import { Prisma } from "@prisma/client";

import { requireAdminAccess } from "../../../../lib/admin";
import { getScalpCronSymbolConfigs } from "../../../../lib/symbolRegistry";
import {
  listScalpCandleHistorySymbols,
  loadScalpCandleHistoryStatsBulk,
  normalizeHistoryTimeframe,
  timeframeToMs,
  type CandleHistoryBackend,
} from "../../../../lib/scalp/candleHistory";
import { getScalpStrategyConfig } from "../../../../lib/scalp/config";
import {
  listScalpDeploymentRegistryEntries,
  type ScalpForwardValidationMetrics,
} from "../../../../lib/scalp/deploymentRegistry";
import {
  DEFAULT_SCALP_TUNE_ID,
  resolveScalpDeployment,
} from "../../../../lib/scalp/deployments";
import {
  isScalpPgConfigured,
  scalpPrisma,
} from "../../../../lib/scalp/pg/client";
import {
  loadPromotionSyncProgressSnapshot,
  loadPromotionSyncStateFromPg,
} from "../../../../lib/scalp/researchPromotion";
import {
  inferScalpPipelineRuntimeOrchestratorStatus,
  loadScalpPipelineRuntimeSnapshot,
  patchScalpPipelineRuntimeSnapshot,
} from "../../../../lib/scalp/pipelineRuntime";
import { normalizeScalpStrategyId } from "../../../../lib/scalp/strategies/registry";
import { deriveScalpDayKey } from "../../../../lib/scalp/stateMachine";
import {
  loadScalpJournal,
  loadScalpSessionState,
  loadScalpStrategyRuntimeSnapshot,
  loadScalpTradeLedger,
} from "../../../../lib/scalp/store";
import type {
  ScalpJournalEntry,
  ScalpTradeLedgerEntry,
} from "../../../../lib/scalp/types";

type SummaryRangeKey = "7D" | "30D" | "6M";
const SUMMARY_RANGE_LOOKBACK_MS: Record<SummaryRangeKey, number> = {
  "7D": 7 * 24 * 60 * 60 * 1000,
  "30D": 30 * 24 * 60 * 60 * 1000,
  "6M": 183 * 24 * 60 * 60 * 1000,
};
const SUMMARY_CACHE_TTL_MS = (() => {
  const value = Number(
    process.env.SCALP_DASHBOARD_SUMMARY_CACHE_TTL_MS ?? 12_000,
  );
  if (!Number.isFinite(value)) return 12_000;
  return Math.max(0, Math.floor(value));
})();
const SUMMARY_CACHE_MAX_ENTRIES = 32;
const summaryResponseCache = new Map<
  string,
  { expiresAtMs: number; payload: Record<string, unknown> }
>();
const HISTORY_DISCOVERY_CACHE_TTL_MS = (() => {
  const value = Number(
    process.env.SCALP_DASHBOARD_HISTORY_CACHE_TTL_MS ?? 300_000,
  );
  if (!Number.isFinite(value)) return 300_000;
  return Math.max(0, Math.floor(value));
})();
const HISTORY_DISCOVERY_CACHE_MAX_ENTRIES = 8;
const HISTORY_DISCOVERY_SCAN_LIMIT = (() => {
  const value = Number(process.env.SCALP_DASHBOARD_HISTORY_SCAN_LIMIT ?? 120);
  if (!Number.isFinite(value)) return 120;
  return Math.max(10, Math.min(1_000, Math.floor(value)));
})();
const HISTORY_DISCOVERY_PREVIEW_LIMIT = (() => {
  const value = Number(process.env.SCALP_DASHBOARD_HISTORY_PREVIEW_LIMIT ?? 20);
  if (!Number.isFinite(value)) return 20;
  return Math.max(1, Math.min(500, Math.floor(value)));
})();
const historyDiscoveryCache = new Map<
  string,
  { expiresAtMs: number; payload: HistoryDiscoverySnapshot }
>();

type SymbolSnapshot = {
  symbol: string;
  strategyId: string;
  tuneId: string;
  deploymentId: string;
  tune: string;
  cronSchedule: string | null;
  cronRoute: "execute-deployments";
  cronPath: string;
  dayKey: string;
  state: string | null;
  updatedAtMs: number | null;
  lastRunAtMs: number | null;
  dryRunLast: boolean | null;
  tradesPlaced: number;
  wins: number;
  losses: number;
  inTrade: boolean;
  tradeSide: "BUY" | "SELL" | null;
  dealReference: string | null;
  reasonCodes: string[];
  netR: number | null;
  maxDrawdownR: number | null;
  promotionEligible: boolean | null;
  promotionReason: string | null;
  forwardValidation: ScalpForwardValidationMetrics | null;
};

type HistoryDiscoveryRow = {
  symbol: string;
  candles: number;
  depthDays: number | null;
  barsPerDay: number | null;
  coveragePct: number | null;
  fromTsMs: number | null;
  toTsMs: number | null;
  updatedAtMs: number | null;
};

type HistoryDiscoverySnapshot = {
  timeframe: string;
  backend: CandleHistoryBackend | "unknown";
  generatedAtMs: number;
  symbolCount: number;
  scannedCount: number;
  scannedLimit: number;
  previewLimit: number;
  previewCount: number;
  truncated: boolean;
  nonEmptyCount: number;
  emptyCount: number;
  totalCandles: number;
  avgCandles: number | null;
  medianCandles: number | null;
  minCandles: number | null;
  maxCandles: number | null;
  avgDepthDays: number | null;
  medianDepthDays: number | null;
  minDepthDays: number | null;
  maxDepthDays: number | null;
  oldestCandleAtMs: number | null;
  newestCandleAtMs: number | null;
  rows: HistoryDiscoveryRow[];
};

const PIPELINE_STEP_DEFS = [
  { id: "discover", label: "Discover" },
  { id: "load_candles", label: "Load candles" },
  { id: "prepare", label: "Prepare cycle" },
  { id: "worker", label: "Run worker" },
  { id: "aggregate", label: "Aggregate" },
  { id: "promotion", label: "Promotion gate" },
] as const;

type ScalpPipelineStepId = (typeof PIPELINE_STEP_DEFS)[number]["id"];
type ScalpPipelineStepState =
  | "pending"
  | "running"
  | "success"
  | "failed"
  | "blocked";
type ScalpPipelineStatus =
  | "idle"
  | "running"
  | "completed"
  | "failed"
  | "blocked";

type ScalpPipelineStatusPanel = {
  status: ScalpPipelineStatus;
  label: string;
  detail: string | null;
  cycleId: string | null;
  updatedAtMs: number | null;
  progressPct: number | null;
  steps: Array<{
    id: ScalpPipelineStepId;
    label: string;
    state: ScalpPipelineStepState;
    detail: string | null;
  }>;
};

type ScalpPipelineSnapshot = {
  panicStop: {
    enabled: boolean;
    reason: string | null;
    updatedAtMs: number | null;
    updatedBy: string | null;
  };
  orchestrator: {
    status: "working" | "idle" | "stale" | "failed";
    runId: string | null;
    stage: string | null;
    cycleId: string | null;
    startedAtMs: number | null;
    updatedAtMs: number | null;
    completedAtMs: number | null;
    runningSinceMs: number | null;
    isRunning: boolean;
    progressPct: number | null;
    progressLabel: string | null;
    loadCursor: number | null;
    selectedSymbolsCount: number | null;
    stageProgressPct: number | null;
    lastError: string | null;
  } | null;
  cycle: {
    cycleId: string | null;
    status: string | null;
    createdAtMs: number | null;
    updatedAtMs: number | null;
    completedAtMs: number | null;
    progressPct: number | null;
    totals: {
      tasks: number | null;
      pending: number | null;
      running: number | null;
      completed: number | null;
      failed: number | null;
      } | null;
  } | null;
  queue: {
    pending: number | null;
    running: number | null;
    outstanding: number | null;
  } | null;
  promotionSync: {
    status: "queued" | "running" | "succeeded" | "failed" | null;
    cycleId: string | null;
    phase: string | null;
    startedAtMs: number | null;
    updatedAtMs: number | null;
    finishedAtMs: number | null;
    totalDeployments: number | null;
    processedDeployments: number | null;
    matchedDeployments: number | null;
    updatedDeployments: number | null;
    currentSymbol: string | null;
    currentStrategyId: string | null;
    currentTuneId: string | null;
    lastError: string | null;
    lastCompletedCycleId: string | null;
    lastCompletedAtMs: number | null;
  } | null;
  statusPanel: ScalpPipelineStatusPanel;
};

type ScalpPipelineCycleTotals = NonNullable<
  NonNullable<ScalpPipelineSnapshot["cycle"]>["totals"]
>;

function asRecord(value: unknown): Record<string, unknown> {
  if (!value || typeof value !== "object" || Array.isArray(value)) return {};
  return value as Record<string, unknown>;
}

function normalizeOrchestratorStatus(
  value: unknown,
): "working" | "idle" | "stale" | "failed" | null {
  const status = String(value || "")
    .trim()
    .toLowerCase();
  if (
    status === "working" ||
    status === "idle" ||
    status === "stale" ||
    status === "failed"
  ) {
    return status;
  }
  return null;
}

function asTsMs(value: unknown): number | null {
  const n = Number(value);
  if (!Number.isFinite(n) || n <= 0) return null;
  return Math.floor(n);
}


function parseUnknownBool(value: unknown): boolean {
  if (typeof value === "boolean") return value;
  const normalized = String(value || "")
    .trim()
    .toLowerCase();
  return ["1", "true", "yes", "on"].includes(normalized);
}

function normalizeReason(value: unknown): string | null {
  const text = String(value || "").trim();
  if (!text) return null;
  return text.slice(0, 240);
}

function normalizeUpdatedBy(value: unknown): string | null {
  const text = String(value || "").trim();
  if (!text) return null;
  return text.slice(0, 120);
}

function pipelineStageMeta(stageRaw: unknown): {
  progressPct: number | null;
  progressLabel: string | null;
} {
  const stage = String(stageRaw || "")
    .trim()
    .toLowerCase();
  if (!stage) return { progressPct: null, progressLabel: null };
  const map: Record<string, { pct: number; label: string }> = {
    discover: { pct: 10, label: "discovering symbols" },
    load_candles: { pct: 24, label: "loading candle history" },
    prepare: { pct: 35, label: "preparing/backfilling history" },
    worker: { pct: 70, label: "running cycle worker" },
    aggregate: { pct: 88, label: "aggregating cycle results" },
    promotion: { pct: 96, label: "applying promotion gate" },
    done: { pct: 100, label: "completed" },
  };
  const hit = map[stage];
  if (!hit)
    return { progressPct: null, progressLabel: stage.replace(/_/g, " ") };
  return { progressPct: hit.pct, progressLabel: hit.label };
}

function safeProgressPct(value: unknown): number | null {
  const n = Number(value);
  if (!Number.isFinite(n)) return null;
  return Math.max(0, Math.min(100, n));
}

function safeCount(value: unknown): number | null {
  const n = Number(value);
  if (!Number.isFinite(n) || n < 0) return null;
  return Math.floor(n);
}

function workerStepDetail(
  totals: ScalpPipelineCycleTotals | null,
): string | null {
  if (!totals) return null;
  const tasks = safeCount(totals.tasks);
  const completed = safeCount(totals.completed);
  const running = safeCount(totals.running);
  const pending = safeCount(totals.pending);
  const failed = safeCount(totals.failed);
  if (
    tasks === null &&
    completed === null &&
    running === null &&
    pending === null &&
    failed === null
  )
    return null;

  const detailParts: string[] = [];
  if (tasks !== null) {
    detailParts.push(
      completed !== null ? `${completed} / ${tasks} tasks` : `${tasks} tasks`,
    );
  } else if (completed !== null) {
    detailParts.push(`${completed} completed`);
  }
  if (running !== null && running > 0) detailParts.push(`${running} running`);
  if (pending !== null && pending > 0) detailParts.push(`${pending} pending`);
  if (failed !== null && failed > 0) detailParts.push(`${failed} failed`);
  return detailParts.length ? detailParts.join(" · ") : null;
}

const PROMOTION_SYNC_STALE_AFTER_MS = 20 * 60_000;

function promotionSyncDetail(
  promotionSync: ScalpPipelineSnapshot["promotionSync"],
): string | null {
  if (!promotionSync) return null;
  const parts: string[] = [];
  const processed = safeCount(promotionSync.processedDeployments);
  const total = safeCount(promotionSync.totalDeployments);
  const matched = safeCount(promotionSync.matchedDeployments);
  if (
    promotionSync.status === "succeeded" &&
    matched !== null &&
    total !== null &&
    total > 0
  ) {
    parts.push(`${matched} matched / ${total} evaluated deployments`);
  } else if (processed !== null && total !== null && total > 0) {
    parts.push(`${processed} / ${total} evaluated deployments`);
  } else if (total !== null && total > 0) {
    parts.push(`${total} evaluated deployments`);
  }
  const currentTarget = [
    promotionSync.currentSymbol,
    promotionSync.currentStrategyId,
    promotionSync.currentTuneId,
  ]
    .map((value) => String(value || "").trim())
    .filter((value) => Boolean(value))
    .join(" · ");
  if (currentTarget) parts.push(currentTarget);
  if (promotionSync.lastError) parts.push(promotionSync.lastError);
  if (!parts.length && promotionSync.phase) parts.push(promotionSync.phase);
  return parts.length ? parts.join(" · ") : null;
}

function compositePromotionProgressPct(params: {
  promotionStepIndex: number;
  totalSteps: number;
  processedDeployments: number | null;
  totalDeployments: number | null;
}): number | null {
  if (params.promotionStepIndex < 0 || params.totalSteps <= 0) return null;
  const processed = safeCount(params.processedDeployments);
  const total = safeCount(params.totalDeployments);
  const frac =
    processed !== null && total !== null && total > 0
      ? Math.max(0, Math.min(1, processed / total))
      : 0;
  return Math.max(
    0,
    Math.min(100, ((params.promotionStepIndex + frac) / params.totalSteps) * 100),
  );
}

function buildPipelineStatusPanel(
  input: Omit<ScalpPipelineSnapshot, "statusPanel">,
): ScalpPipelineStatusPanel {
  const nowMs = Date.now();
  const panicStopEnabled = input.panicStop.enabled === true;
  const panicStopReason = normalizeReason(input.panicStop.reason);
  const stage = String(input.orchestrator?.stage || "")
    .trim()
    .toLowerCase();
  const orchestratorStatus = normalizeOrchestratorStatus(
    input.orchestrator?.status,
  );
  const stageIndex = PIPELINE_STEP_DEFS.findIndex((step) => step.id === stage);
  const workerStepIndex = PIPELINE_STEP_DEFS.findIndex(
    (step) => step.id === "worker",
  );
  const promotionStepIndex = PIPELINE_STEP_DEFS.findIndex(
    (step) => step.id === "promotion",
  );
  const orchestratorActive =
    (orchestratorStatus === "working" ||
      (input.orchestrator?.isRunning === true &&
        orchestratorStatus !== "stale" &&
        orchestratorStatus !== "failed")) &&
    Boolean(stage) &&
    stage !== "done";
  const currentCycleId =
    String(
      input.orchestrator?.cycleId ||
        input.promotionSync?.cycleId ||
        input.cycle?.cycleId ||
        "",
    ).trim() || null;
  const queuePending = safeCount(input.queue?.pending);
  const queueRunning = safeCount(input.queue?.running);
  const queueOutstanding =
    safeCount(input.queue?.outstanding) ??
    ((queuePending ?? 0) + (queueRunning ?? 0));
  const queueHasOutstanding = queueOutstanding > 0;
  const queueDetailParts: string[] = [];
  if (queueOutstanding > 0) queueDetailParts.push(`${queueOutstanding} queued`);
  if ((queueRunning ?? 0) > 0) queueDetailParts.push(`${queueRunning} running`);
  if ((queuePending ?? 0) > 0) queueDetailParts.push(`${queuePending} pending`);
  const queueDetail = queueDetailParts.length ? queueDetailParts.join(" · ") : null;
  const promotionSync = input.promotionSync;
  const promotionSyncFresh =
    promotionSync?.updatedAtMs !== null &&
    typeof promotionSync?.updatedAtMs === "number" &&
    Number.isFinite(promotionSync.updatedAtMs) &&
    nowMs - promotionSync.updatedAtMs <= PROMOTION_SYNC_STALE_AFTER_MS;
  const promotionRunning =
    Boolean(promotionSyncFresh) &&
    (promotionSync?.status === "queued" || promotionSync?.status === "running");
  const promotionFailed =
    Boolean(promotionSyncFresh) && promotionSync?.status === "failed";
  const promotionSucceeded = promotionSync?.status === "succeeded";
  const promotionDetail =
    promotionSync && (promotionSyncFresh || promotionSucceeded)
      ? promotionSyncDetail(promotionSync)
      : null;
  const orchestratorErrorRaw =
    String(input.orchestrator?.lastError || "").trim() || null;
  const orchestratorStatusFailed = orchestratorStatus === "failed";
  const orchestratorError =
    panicStopEnabled && orchestratorErrorRaw === "panic_stop_enabled"
      ? null
      : orchestratorErrorRaw || (orchestratorStatusFailed ? "orchestrator_failed" : null);
  const effectiveOrchestratorError =
    stage === "promotion" && (promotionRunning || promotionSucceeded)
      ? null
      : orchestratorError;
  const stageMeta = pipelineStageMeta(stage);
  const orchestratorProgressPct =
    safeProgressPct(input.orchestrator?.progressPct) ?? stageMeta.progressPct;
  const progressPct = promotionRunning
    ? compositePromotionProgressPct({
        promotionStepIndex,
        totalSteps: PIPELINE_STEP_DEFS.length,
        processedDeployments: safeCount(promotionSync?.processedDeployments) ?? 0,
        totalDeployments: safeCount(promotionSync?.totalDeployments) ?? 1,
      }) ?? orchestratorProgressPct
    : orchestratorProgressPct;
  const updatedAtMs = [
    input.panicStop.updatedAtMs,
    input.orchestrator?.updatedAtMs,
    input.cycle?.updatedAtMs,
    input.promotionSync?.updatedAtMs,
  ]
    .filter(
      (value): value is number =>
        typeof value === "number" && Number.isFinite(value),
    )
    .reduce<number | null>(
      (max, value) => (max === null ? value : Math.max(max, value)),
      null,
    );
  const currentStepIndex =
    promotionRunning
      ? promotionStepIndex
      : orchestratorActive && stageIndex >= 0
      ? stageIndex
      : queueHasOutstanding
      ? workerStepIndex
      : -1;
  const failureStepIndex =
    promotionFailed
      ? promotionStepIndex
      : effectiveOrchestratorError
      ? stageIndex >= 0
        ? stageIndex
        : queueHasOutstanding
        ? workerStepIndex
        : -1
      : -1;
  const workerDetail = queueDetail || workerStepDetail(input.cycle?.totals || null);
  const completedState =
    !orchestratorActive &&
    !queueHasOutstanding &&
    !promotionRunning &&
    !promotionFailed &&
    !effectiveOrchestratorError &&
    (stage === "done" || promotionSucceeded);

  const steps = PIPELINE_STEP_DEFS.map((step, index) => {
    let state: ScalpPipelineStepState = "pending";
    if (completedState) {
      state = "success";
    } else if (failureStepIndex >= 0) {
      if (index < failureStepIndex) state = "success";
      else if (index === failureStepIndex) state = "failed";
    } else if (currentStepIndex >= 0) {
      if (index < currentStepIndex) state = "success";
      else if (index === currentStepIndex) state = "running";
    } else if (promotionSucceeded && index <= promotionStepIndex) {
      state = "success";
    }

    if (panicStopEnabled && !orchestratorActive && state !== "success") {
      state = "blocked";
    }

    let detail: string | null = null;
    if (step.id === "worker") detail = workerDetail;
    if (step.id === "promotion") detail = promotionDetail;
    if (state === "failed")
      detail =
        step.id === "promotion"
          ? promotionDetail ||
            effectiveOrchestratorError ||
            "promotion sync failed"
          : effectiveOrchestratorError || workerDetail || "pipeline failed";
    if (state === "blocked") detail = panicStopReason || "panic stop enabled";
    return {
      id: step.id,
      label: step.label,
      state,
      detail,
    };
  });

  const runningStep = steps.find((step) => step.state === "running") || null;
  const failedStep = steps.find((step) => step.state === "failed") || null;
  const completedCount = steps.filter(
    (step) => step.state === "success",
  ).length;

  if (panicStopEnabled && !orchestratorActive && !promotionRunning) {
    return {
      status: "blocked",
      label: "Pipeline paused",
      detail: panicStopReason || "panic stop is blocking new cycle work",
      cycleId: currentCycleId,
      updatedAtMs,
      progressPct,
      steps,
    };
  }

  if (failedStep) {
    return {
      status: "failed",
      label: `${failedStep.label} failed`,
      detail: failedStep.detail,
      cycleId: currentCycleId,
      updatedAtMs,
      progressPct,
      steps,
    };
  }

  if (runningStep) {
    return {
      status: "running",
      label: `${runningStep.label} in progress`,
      detail:
        (runningStep.id === "promotion" ? promotionDetail : null) ||
        input.orchestrator?.progressLabel ||
        runningStep.detail ||
        (currentCycleId ? `cycle ${currentCycleId}` : null),
      cycleId: currentCycleId,
      updatedAtMs,
      progressPct,
      steps,
    };
  }

  if (completedState || completedCount === PIPELINE_STEP_DEFS.length) {
    return {
      status: "completed",
      label: "Latest cycle completed",
      detail:
        promotionDetail ||
        workerDetail ||
        (currentCycleId ? `cycle ${currentCycleId}` : null),
      cycleId: currentCycleId,
      updatedAtMs,
      progressPct,
      steps,
    };
  }

  return {
    status: "idle",
    label: "Awaiting next cycle",
    detail: null,
    cycleId: currentCycleId,
    updatedAtMs,
    progressPct,
    steps,
  };
}

async function loadScalpPipelineSnapshot(
  nowMs: number,
): Promise<ScalpPipelineSnapshot | null> {
  if (!isScalpPgConfigured()) return null;
  const db = scalpPrisma();
  const rows = await db.$queryRaw<
    Array<{
      orchestratorPayload: unknown;
      panicStopPayload: unknown;
      panicStopUpdatedAtMs: bigint | number | null;
      runningCycleId: string | null;
      runningCycleStatus: string | null;
      runningCycleCreatedAtMs: bigint | number | null;
      runningCycleUpdatedAtMs: bigint | number | null;
      runningCycleCompletedAtMs: bigint | number | null;
      runningCycleSummary: unknown;
      queuePending: bigint | number | string | null;
      queueRetryWait: bigint | number | string | null;
      queueRunning: bigint | number | string | null;
    }>
  >(Prisma.sql`
    WITH orchestrator_state AS (
      SELECT payload
      FROM scalp_jobs
      WHERE kind = 'execute_cycle'::scalp_job_kind
        AND dedupe_key = 'scalp_pipeline_orchestrator_state_v1'
      LIMIT 1
    ),
    panic_stop_state AS (
      SELECT
        payload,
        (EXTRACT(EPOCH FROM updated_at) * 1000)::bigint AS updated_at_ms
      FROM scalp_jobs
      WHERE kind = 'execute_cycle'::scalp_job_kind
        AND dedupe_key = 'scalp_panic_stop_v1'
      LIMIT 1
    ),
    running_cycle AS (
      SELECT
        cycle_id,
        status::text AS status,
        latest_summary_json,
        (EXTRACT(EPOCH FROM created_at) * 1000)::bigint AS created_at_ms,
        (EXTRACT(EPOCH FROM updated_at) * 1000)::bigint AS updated_at_ms,
        CASE
          WHEN completed_at IS NULL THEN NULL
          ELSE (EXTRACT(EPOCH FROM completed_at) * 1000)::bigint
        END AS completed_at_ms
      FROM scalp_research_cycles
      WHERE status = 'running'::scalp_cycle_status
      ORDER BY updated_at DESC
      LIMIT 1
    ),
    queue_totals AS (
      SELECT
        COUNT(*) FILTER (WHERE status = 'pending')::bigint AS pending,
        COUNT(*) FILTER (WHERE status = 'retry_wait')::bigint AS retry_wait,
        COUNT(*) FILTER (WHERE status = 'running')::bigint AS running
      FROM scalp_research_tasks
    )
    SELECT
      (SELECT payload FROM orchestrator_state) AS "orchestratorPayload",
      (SELECT payload FROM panic_stop_state) AS "panicStopPayload",
      (SELECT updated_at_ms FROM panic_stop_state) AS "panicStopUpdatedAtMs",
      rc.cycle_id AS "runningCycleId",
      rc.status AS "runningCycleStatus",
      rc.created_at_ms AS "runningCycleCreatedAtMs",
      rc.updated_at_ms AS "runningCycleUpdatedAtMs",
      rc.completed_at_ms AS "runningCycleCompletedAtMs",
      rc.latest_summary_json AS "runningCycleSummary",
      qt.pending AS "queuePending",
      qt.retry_wait AS "queueRetryWait",
      qt.running AS "queueRunning"
    FROM queue_totals qt
    LEFT JOIN running_cycle rc ON TRUE;
  `);
  const row = rows[0];
  if (!row) return null;
  const [runtimeSnapshot, promotionProgress, latestPromotionSync] =
    await Promise.all([
      loadScalpPipelineRuntimeSnapshot(),
      loadPromotionSyncProgressSnapshot(),
      loadPromotionSyncStateFromPg(),
    ]);
  const selectedCycle =
    String(row.runningCycleId || "").trim()
      ? {
          cycleId: row.runningCycleId,
          status: row.runningCycleStatus,
          createdAtMs: row.runningCycleCreatedAtMs,
          updatedAtMs: row.runningCycleUpdatedAtMs,
          completedAtMs: row.runningCycleCompletedAtMs,
          summary: row.runningCycleSummary,
        }
      : null;

  const orchestratorPayload = asRecord(row.orchestratorPayload);
  const panicStopPayload = asRecord(row.panicStopPayload);
  const panicStopEnabled = parseUnknownBool(panicStopPayload.enabled);
  const fallbackOrchestratorStage =
    String(orchestratorPayload.stage || "").trim() || null;
  const fallbackOrchestratorStartedAtMs = asTsMs(orchestratorPayload.startedAtMs);
  const fallbackOrchestratorCompletedAtMs = asTsMs(orchestratorPayload.completedAtMs);
  const fallbackOrchestratorUpdatedAtMs = asTsMs(orchestratorPayload.updatedAtMs);
  const fallbackOrchestratorLockUntilMs = asTsMs(orchestratorPayload.lockUntilMs);
  const fallbackOrchestratorLoadCursor = Number.isFinite(
    Number(orchestratorPayload.loadCursor),
  )
    ? Math.max(0, Math.floor(Number(orchestratorPayload.loadCursor)))
    : null;
  const fallbackOrchestratorSelectedSymbolsCount = Array.isArray(
    orchestratorPayload.selectedSymbols,
  )
    ? orchestratorPayload.selectedSymbols.length
    : Number.isFinite(Number(orchestratorPayload.selectedSymbolsCount))
    ? Math.max(0, Math.floor(Number(orchestratorPayload.selectedSymbolsCount)))
    : null;
  const fallbackOrchestratorLastError =
    String(orchestratorPayload.lastError || "").trim() || null;
  const orchestratorRunningStaleAfterMs = 20 * 60_000;
  const orchestratorFreshByLock =
    fallbackOrchestratorLockUntilMs === null ||
    fallbackOrchestratorLockUntilMs >= nowMs;
  const orchestratorFreshByUpdate =
    fallbackOrchestratorUpdatedAtMs !== null &&
    nowMs - fallbackOrchestratorUpdatedAtMs <= orchestratorRunningStaleAfterMs;
  const fallbackOrchestratorRunningRaw =
    Boolean(fallbackOrchestratorStage) &&
    fallbackOrchestratorStage !== "done" &&
    fallbackOrchestratorStartedAtMs !== null &&
    (fallbackOrchestratorCompletedAtMs === null ||
      fallbackOrchestratorCompletedAtMs < fallbackOrchestratorStartedAtMs) &&
    !fallbackOrchestratorLastError &&
    orchestratorFreshByLock &&
    orchestratorFreshByUpdate;
  const fallbackOrchestratorStatus = fallbackOrchestratorStage
    ? inferScalpPipelineRuntimeOrchestratorStatus({
        isRunning: fallbackOrchestratorRunningRaw,
        lastError: fallbackOrchestratorLastError,
      })
    : null;
  const fallbackOrchestratorStageProgressPct =
    fallbackOrchestratorStage?.toLowerCase() === "load_candles" &&
    fallbackOrchestratorLoadCursor !== null &&
    fallbackOrchestratorSelectedSymbolsCount !== null &&
    fallbackOrchestratorSelectedSymbolsCount > 0
      ? Math.max(
          0,
          Math.min(
            100,
            (fallbackOrchestratorLoadCursor / fallbackOrchestratorSelectedSymbolsCount) *
              100,
          ),
        )
      : null;
  const summary = asRecord(selectedCycle?.summary);
  const totals = asRecord(summary.totals);
  const cycleProgressPct = safeProgressPct(summary.progressPct);
  const queuePending = safeCount(row.queuePending);
  const queueRetryWait = safeCount(row.queueRetryWait);
  const queueRunning = safeCount(row.queueRunning);
  const queuePendingWithRetry =
    queuePending !== null || queueRetryWait !== null
      ? (queuePending ?? 0) + (queueRetryWait ?? 0)
      : null;
  const queueOutstanding =
    queuePendingWithRetry !== null || queueRunning !== null
      ? (queuePendingWithRetry ?? 0) + (queueRunning ?? 0)
      : null;
  const fallbackOrchestrator = fallbackOrchestratorStage
    ? {
        status: fallbackOrchestratorStatus || "idle",
        runId: String(orchestratorPayload.runId || "").trim() || null,
        stage: fallbackOrchestratorStage,
        cycleId: String(orchestratorPayload.cycleId || "").trim() || null,
        startedAtMs: fallbackOrchestratorStartedAtMs,
        updatedAtMs: fallbackOrchestratorUpdatedAtMs,
        completedAtMs: fallbackOrchestratorCompletedAtMs,
        runningSinceMs: fallbackOrchestratorRunningRaw
          ? fallbackOrchestratorStartedAtMs
          : null,
        isRunning: fallbackOrchestratorRunningRaw,
        progressPct: null,
        progressLabel: null,
        loadCursor: fallbackOrchestratorLoadCursor,
        selectedSymbolsCount: fallbackOrchestratorSelectedSymbolsCount,
        stageProgressPct: fallbackOrchestratorStageProgressPct,
        lastError: fallbackOrchestratorLastError,
      }
    : null;
  const runtimeOrchestrator = runtimeSnapshot?.orchestrator || null;
  let runtimeOrchestratorStatus =
    normalizeOrchestratorStatus(runtimeOrchestrator?.status) ||
    (runtimeOrchestrator
      ? inferScalpPipelineRuntimeOrchestratorStatus({
          isRunning: runtimeOrchestrator.isRunning === true,
          lastError: runtimeOrchestrator.lastError,
        })
      : null);
  const runtimeOrchestratorFreshByUpdate =
    runtimeOrchestrator?.updatedAtMs !== null &&
    typeof runtimeOrchestrator?.updatedAtMs === "number" &&
    Number.isFinite(runtimeOrchestrator.updatedAtMs) &&
    nowMs - runtimeOrchestrator.updatedAtMs <=
      orchestratorRunningStaleAfterMs;
  const runtimeOrchestratorStale =
    (runtimeOrchestratorStatus === "working" ||
      runtimeOrchestrator?.isRunning === true) &&
    !runtimeOrchestratorFreshByUpdate;
  if (
    runtimeOrchestrator &&
    runtimeOrchestratorStale &&
    runtimeOrchestratorStatus !== "stale"
  ) {
    runtimeOrchestratorStatus = "stale";
    await patchScalpPipelineRuntimeSnapshot({
      updatedAtMs: nowMs,
      orchestrator: {
        status: "stale",
        runId: runtimeOrchestrator.runId,
        stage: runtimeOrchestrator.stage,
        cycleId: runtimeOrchestrator.cycleId,
        startedAtMs: runtimeOrchestrator.startedAtMs,
        updatedAtMs: nowMs,
        completedAtMs: runtimeOrchestrator.completedAtMs,
        isRunning: false,
        progressPct: runtimeOrchestrator.progressPct,
        progressLabel: runtimeOrchestrator.progressLabel || "stale runtime state",
        lastError: runtimeOrchestrator.lastError || "stale_runtime_state",
      },
    });
  }
  const preferFallbackOrchestrator =
    runtimeOrchestratorStale && fallbackOrchestrator !== null;
  const effectiveRuntimeOrchestrator = preferFallbackOrchestrator
    ? null
    : runtimeOrchestrator;
  const effectiveRuntimeOrchestratorStatus = preferFallbackOrchestrator
    ? null
    : runtimeOrchestratorStatus;
  const hasRuntimeOrchestrator = effectiveRuntimeOrchestrator !== null;
  const mergedOrchestratorStage = hasRuntimeOrchestrator
    ? effectiveRuntimeOrchestrator.stage
    : fallbackOrchestrator?.stage ?? null;
  const mergedStageMeta = pipelineStageMeta(mergedOrchestratorStage);
  const mergedOrchestratorCycleId = hasRuntimeOrchestrator
    ? effectiveRuntimeOrchestrator.cycleId
    : fallbackOrchestrator?.cycleId ?? null;
  const cycleProgressForOrchestrator =
    cycleProgressPct !== null &&
    selectedCycle?.cycleId &&
    mergedOrchestratorCycleId &&
    selectedCycle.cycleId === mergedOrchestratorCycleId
      ? cycleProgressPct
      : null;
  const orchestratorBaseProgressPct =
    safeProgressPct(
      hasRuntimeOrchestrator
        ? effectiveRuntimeOrchestrator.progressPct
        : fallbackOrchestrator?.progressPct,
    ) ?? mergedStageMeta.progressPct;
  const mergedOrchestratorStartedAtMs = hasRuntimeOrchestrator
    ? effectiveRuntimeOrchestrator.startedAtMs
    : fallbackOrchestrator?.startedAtMs ?? null;
  const runtimeOrchestratorRunningRaw =
    effectiveRuntimeOrchestratorStatus === "working" &&
    effectiveRuntimeOrchestrator?.isRunning === true &&
    runtimeOrchestratorFreshByUpdate &&
    orchestratorFreshByLock;
  const mergedOrchestratorIsRunning = panicStopEnabled
    ? false
    : hasRuntimeOrchestrator
      ? runtimeOrchestratorRunningRaw
      : fallbackOrchestrator?.isRunning ??
        false;
  const mergedOrchestrator =
    effectiveRuntimeOrchestrator || fallbackOrchestrator
      ? {
          status: hasRuntimeOrchestrator
            ? effectiveRuntimeOrchestratorStatus || "idle"
            : fallbackOrchestrator?.status || "idle",
          runId: hasRuntimeOrchestrator
            ? effectiveRuntimeOrchestrator.runId
            : fallbackOrchestrator?.runId ?? null,
          stage: mergedOrchestratorStage,
          cycleId: mergedOrchestratorCycleId,
          startedAtMs: mergedOrchestratorStartedAtMs,
          updatedAtMs: hasRuntimeOrchestrator
            ? effectiveRuntimeOrchestrator.updatedAtMs
            : fallbackOrchestrator?.updatedAtMs ??
              null,
          completedAtMs: hasRuntimeOrchestrator
            ? effectiveRuntimeOrchestrator.completedAtMs
            : fallbackOrchestrator?.completedAtMs ??
              null,
          runningSinceMs:
            mergedOrchestratorIsRunning ? mergedOrchestratorStartedAtMs : null,
          isRunning: mergedOrchestratorIsRunning,
          progressPct:
            cycleProgressForOrchestrator !== null
              ? Math.max(
                  orchestratorBaseProgressPct ?? 0,
                  Math.min(100, cycleProgressForOrchestrator),
                )
              : orchestratorBaseProgressPct,
          progressLabel:
            (hasRuntimeOrchestrator
              ? runtimeOrchestratorStale
                ? "stale runtime state"
                : effectiveRuntimeOrchestrator.progressLabel
              : fallbackOrchestrator?.progressLabel) ??
            mergedStageMeta.progressLabel,
          loadCursor: hasRuntimeOrchestrator
            ? effectiveRuntimeOrchestrator.loadCursor ?? null
            : fallbackOrchestrator?.loadCursor ?? null,
          selectedSymbolsCount: hasRuntimeOrchestrator
            ? effectiveRuntimeOrchestrator.selectedSymbolsCount ?? null
            : fallbackOrchestrator?.selectedSymbolsCount ?? null,
          stageProgressPct: hasRuntimeOrchestrator
            ? effectiveRuntimeOrchestrator.stageProgressPct ?? null
            : fallbackOrchestrator?.stageProgressPct ?? null,
          lastError: hasRuntimeOrchestrator
            ? runtimeOrchestratorStale
              ? "stale_runtime_state"
              : effectiveRuntimeOrchestrator.lastError
            : fallbackOrchestrator?.lastError ??
              null,
        }
      : null;
  const fallbackPromotionSync =
    promotionProgress || latestPromotionSync
      ? {
          status:
            promotionProgress?.status ||
            (latestPromotionSync ? "succeeded" : null),
          cycleId: promotionProgress?.cycleId || latestPromotionSync?.cycleId || null,
          phase: promotionProgress?.phase || null,
          startedAtMs: promotionProgress?.startedAtMs || null,
          updatedAtMs:
            promotionProgress?.updatedAtMs ?? latestPromotionSync?.syncedAtMs ?? null,
          finishedAtMs:
            promotionProgress?.finishedAtMs ?? latestPromotionSync?.syncedAtMs ?? null,
          totalDeployments: promotionProgress?.totalDeployments ?? null,
          processedDeployments: promotionProgress?.processedDeployments ?? null,
          matchedDeployments:
            promotionProgress?.matchedDeployments ??
            latestPromotionSync?.deploymentsMatched ??
            null,
          updatedDeployments:
            promotionProgress?.updatedDeployments ??
            latestPromotionSync?.deploymentsUpdated ??
            null,
          currentSymbol: promotionProgress?.currentSymbol || null,
          currentStrategyId: promotionProgress?.currentStrategyId || null,
          currentTuneId: promotionProgress?.currentTuneId || null,
          lastError: promotionProgress?.lastError || null,
          lastCompletedCycleId: latestPromotionSync?.cycleId || null,
          lastCompletedAtMs: latestPromotionSync?.syncedAtMs || null,
        }
      : null;
  const runtimePromotionSync = runtimeSnapshot?.promotionSync || null;
  const mergedPromotionSync =
    runtimePromotionSync || fallbackPromotionSync
      ? {
          status:
            runtimePromotionSync?.status ?? fallbackPromotionSync?.status ?? null,
          cycleId:
            runtimePromotionSync?.cycleId ?? fallbackPromotionSync?.cycleId ?? null,
          phase: runtimePromotionSync?.phase ?? fallbackPromotionSync?.phase ?? null,
          startedAtMs:
            runtimePromotionSync?.startedAtMs ??
            fallbackPromotionSync?.startedAtMs ??
            null,
          updatedAtMs:
            runtimePromotionSync?.updatedAtMs ??
            fallbackPromotionSync?.updatedAtMs ??
            null,
          finishedAtMs:
            runtimePromotionSync?.finishedAtMs ??
            fallbackPromotionSync?.finishedAtMs ??
            null,
          totalDeployments:
            runtimePromotionSync?.totalDeployments ??
            fallbackPromotionSync?.totalDeployments ??
            null,
          processedDeployments:
            runtimePromotionSync?.processedDeployments ??
            fallbackPromotionSync?.processedDeployments ??
            null,
          matchedDeployments:
            runtimePromotionSync?.matchedDeployments ??
            fallbackPromotionSync?.matchedDeployments ??
            null,
          updatedDeployments:
            runtimePromotionSync?.updatedDeployments ??
            fallbackPromotionSync?.updatedDeployments ??
            null,
          currentSymbol:
            runtimePromotionSync?.currentSymbol ??
            fallbackPromotionSync?.currentSymbol ??
            null,
          currentStrategyId:
            runtimePromotionSync?.currentStrategyId ??
            fallbackPromotionSync?.currentStrategyId ??
            null,
          currentTuneId:
            runtimePromotionSync?.currentTuneId ??
            fallbackPromotionSync?.currentTuneId ??
            null,
          lastError:
            runtimePromotionSync?.lastError ??
            fallbackPromotionSync?.lastError ??
            null,
          lastCompletedCycleId:
            runtimePromotionSync?.lastCompletedCycleId ??
            fallbackPromotionSync?.lastCompletedCycleId ??
            null,
          lastCompletedAtMs:
            runtimePromotionSync?.lastCompletedAtMs ??
            fallbackPromotionSync?.lastCompletedAtMs ??
            null,
        }
      : null;
  const snapshotBase: Omit<ScalpPipelineSnapshot, "statusPanel"> = {
    panicStop: {
      enabled: panicStopEnabled,
      reason: normalizeReason(panicStopPayload.reason),
      updatedAtMs: asTsMs(row.panicStopUpdatedAtMs),
      updatedBy: normalizeUpdatedBy(panicStopPayload.updatedBy),
    },
    orchestrator: mergedOrchestrator,
    cycle: selectedCycle?.cycleId
      ? {
          cycleId: selectedCycle.cycleId,
          status: selectedCycle.status,
          createdAtMs: asTsMs(selectedCycle.createdAtMs),
          updatedAtMs: asTsMs(selectedCycle.updatedAtMs),
          completedAtMs: asTsMs(selectedCycle.completedAtMs),
          progressPct: cycleProgressPct,
          totals: Object.keys(totals).length
            ? {
                tasks: safeCount(totals.tasks),
                pending: safeCount(totals.pending),
                running: safeCount(totals.running),
                completed: safeCount(totals.completed),
                failed: safeCount(totals.failed),
              }
            : null,
        }
      : null,
    queue:
      queuePendingWithRetry !== null || queueRunning !== null
        ? {
            pending: queuePendingWithRetry,
            running: queueRunning,
            outstanding: queueOutstanding,
          }
        : null,
    promotionSync: mergedPromotionSync,
  };
  return {
    ...snapshotBase,
    statusPanel: buildPipelineStatusPanel(snapshotBase),
  };
}

function parseLimit(
  value: string | string[] | undefined,
  fallback: number,
): number {
  const first = Array.isArray(value) ? value[0] : value;
  const n = Number(first);
  if (!Number.isFinite(n) || n <= 0) return fallback;
  return Math.max(1, Math.min(300, Math.floor(n)));
}

function parseTradeLimit(
  value: string | string[] | undefined,
  fallback: number,
): number {
  const first = Array.isArray(value) ? value[0] : value;
  const n = Number(first);
  if (!Number.isFinite(n) || n <= 0) return fallback;
  return Math.max(200, Math.min(50_000, Math.floor(n)));
}

function parseBool(
  value: string | string[] | undefined,
  fallback: boolean,
): boolean {
  const first = firstQueryValue(value);
  if (!first) return fallback;
  const normalized = first.toLowerCase();
  if (["1", "true", "yes", "on"].includes(normalized)) return true;
  if (["0", "false", "no", "off"].includes(normalized)) return false;
  return fallback;
}

function resolveSummaryRange(raw: unknown): SummaryRangeKey {
  const normalized = String(raw || "")
    .trim()
    .toUpperCase();
  if (normalized === "30D") return "30D";
  if (normalized === "6M") return "6M";
  return "7D";
}

function firstQueryValue(
  value: string | string[] | undefined,
): string | undefined {
  if (typeof value === "string") return value.trim() || undefined;
  if (Array.isArray(value) && value.length > 0)
    return String(value[0] || "").trim() || undefined;
  return undefined;
}

function journalStrategyId(entry: ScalpJournalEntry): string | null {
  const payload =
    entry.payload && typeof entry.payload === "object"
      ? (entry.payload as Record<string, unknown>)
      : {};
  const normalized = normalizeScalpStrategyId(payload.strategyId);
  return normalized || null;
}

function journalDeploymentId(entry: ScalpJournalEntry): string | null {
  const payload =
    entry.payload && typeof entry.payload === "object"
      ? (entry.payload as Record<string, unknown>)
      : {};
  const normalized = String(payload.deploymentId || "").trim();
  return normalized || null;
}

function compactJournalEntry(
  entry: ScalpJournalEntry,
): Record<string, unknown> {
  const compactPayload = (value: unknown, depth = 0): unknown => {
    if (value === null || value === undefined) return null;
    if (depth >= 3) return "[truncated]";
    const t = typeof value;
    if (t === "string") {
      const text = String(value);
      return text.length > 400 ? `${text.slice(0, 397)}...` : text;
    }
    if (t === "number") {
      const n = Number(value);
      return Number.isFinite(n) ? n : null;
    }
    if (t === "boolean") return value;
    if (t === "bigint") {
      const n = Number(value);
      return Number.isFinite(n) ? n : String(value);
    }
    if (Array.isArray(value)) {
      return value.slice(0, 20).map((row) => compactPayload(row, depth + 1));
    }
    if (value && typeof value === "object") {
      const row = value as Record<string, unknown>;
      const out: Record<string, unknown> = {};
      let count = 0;
      for (const [key, raw] of Object.entries(row)) {
        out[key] = compactPayload(raw, depth + 1);
        count += 1;
        if (count >= 24) break;
      }
      return out;
    }
    return String(value);
  };

  return {
    id: entry.id,
    timestampMs: entry.timestampMs,
    type: entry.type,
    level: entry.level,
    symbol: entry.symbol,
    dayKey: entry.dayKey,
    reasonCodes: Array.isArray(entry.reasonCodes)
      ? entry.reasonCodes.slice(0, 8)
      : [],
    payload: compactPayload(entry.payload ?? {}),
  };
}

function computeRangePerformance(
  trades: ScalpTradeLedgerEntry[],
): { netR: number; maxDrawdownR: number } | null {
  if (!trades.length) return null;
  const ordered = trades.slice().sort((a, b) => a.exitAtMs - b.exitAtMs);
  let netR = 0;
  let equityR = 0;
  let peakR = 0;
  let maxDd = 0;
  for (const trade of ordered) {
    const r = Number(trade.rMultiple);
    if (!Number.isFinite(r)) continue;
    netR += r;
    equityR += r;
    peakR = Math.max(peakR, equityR);
    maxDd = Math.max(maxDd, peakR - equityR);
  }
  return { netR, maxDrawdownR: maxDd };
}

function deriveTuneLabel(params: {
  strategyId: string;
  defaultStrategyId: string;
  tuneId?: string | null;
}): string {
  const explicitTune = String(params.tuneId || "")
    .trim()
    .toLowerCase();
  if (explicitTune && explicitTune !== DEFAULT_SCALP_TUNE_ID)
    return explicitTune;
  const strategyId = normalizeScalpStrategyId(params.strategyId);
  const defaultStrategyId = normalizeScalpStrategyId(params.defaultStrategyId);
  if (!strategyId) return "default";
  if (!defaultStrategyId || strategyId === defaultStrategyId) return "default";
  const prefix = `${defaultStrategyId}_`;
  if (strategyId.startsWith(prefix) && strategyId.length > prefix.length) {
    return strategyId.slice(prefix.length);
  }
  return strategyId;
}

function setNoStoreHeaders(res: NextApiResponse): void {
  res.setHeader(
    "Cache-Control",
    "no-store, no-cache, must-revalidate, proxy-revalidate",
  );
  res.setHeader("Pragma", "no-cache");
  res.setHeader("Expires", "0");
}

function makeSummaryCacheKey(input: {
  useDeployments: boolean;
  requestedStrategyId?: string;
  range: SummaryRangeKey;
  journalLimit: number;
  tradeLimit: number;
}): string {
  return JSON.stringify({
    useDeployments: input.useDeployments,
    strategyId: input.requestedStrategyId || null,
    range: input.range,
    journalLimit: input.journalLimit,
    tradeLimit: input.tradeLimit,
  });
}

function pruneSummaryCache(nowMs: number): void {
  for (const [key, row] of summaryResponseCache.entries()) {
    if (row.expiresAtMs <= nowMs) summaryResponseCache.delete(key);
  }
  if (summaryResponseCache.size <= SUMMARY_CACHE_MAX_ENTRIES) return;
  const keys = Array.from(summaryResponseCache.keys());
  while (
    summaryResponseCache.size > SUMMARY_CACHE_MAX_ENTRIES &&
    keys.length > 0
  ) {
    const key = keys.shift();
    if (!key) break;
    summaryResponseCache.delete(key);
  }
}

function makeHistoryDiscoveryCacheKey(input: {
  timeframe: string;
  scanLimit: number;
  previewLimit: number;
}): string {
  return JSON.stringify({
    timeframe: input.timeframe,
    scanLimit: input.scanLimit,
    previewLimit: input.previewLimit,
  });
}

function pruneHistoryDiscoveryCache(nowMs: number): void {
  for (const [key, row] of historyDiscoveryCache.entries()) {
    if (row.expiresAtMs <= nowMs) historyDiscoveryCache.delete(key);
  }
  if (historyDiscoveryCache.size <= HISTORY_DISCOVERY_CACHE_MAX_ENTRIES) return;
  const keys = Array.from(historyDiscoveryCache.keys());
  while (
    historyDiscoveryCache.size > HISTORY_DISCOVERY_CACHE_MAX_ENTRIES &&
    keys.length > 0
  ) {
    const key = keys.shift();
    if (!key) break;
    historyDiscoveryCache.delete(key);
  }
}

function roundMetric(value: number | null, digits = 2): number | null {
  if (value === null || !Number.isFinite(value)) return null;
  const factor = 10 ** Math.max(0, Math.floor(digits));
  return Math.round(value * factor) / factor;
}

function meanValue(values: number[]): number | null {
  if (!values.length) return null;
  const total = values.reduce((acc, row) => acc + row, 0);
  return total / values.length;
}

function medianValue(values: number[]): number | null {
  if (!values.length) return null;
  const sorted = values.slice().sort((a, b) => a - b);
  const mid = Math.floor(sorted.length / 2);
  if (sorted.length % 2 === 1) return sorted[mid] ?? null;
  const left = sorted[mid - 1];
  const right = sorted[mid];
  if (!Number.isFinite(left) || !Number.isFinite(right)) return null;
  return (left + right) / 2;
}

async function loadHistoryDiscoverySnapshot(params: {
  nowMs: number;
  debugLogsEnabled: boolean;
  rowErrors: Array<Record<string, unknown>>;
  requestId: string;
  logDebug: (event: string, payload?: Record<string, unknown>) => void;
}): Promise<HistoryDiscoverySnapshot> {
  const timeframe = normalizeHistoryTimeframe(
    String(process.env.SCALP_DASHBOARD_HISTORY_TIMEFRAME || "1m"),
  );
  const scanLimit = HISTORY_DISCOVERY_SCAN_LIMIT;
  const previewLimit = HISTORY_DISCOVERY_PREVIEW_LIMIT;
  const timeframeMs = Math.max(60_000, timeframeToMs(timeframe));
  const emptySnapshot = (
    backend: CandleHistoryBackend | "unknown" = "unknown",
  ): HistoryDiscoverySnapshot => ({
    timeframe,
    backend,
    generatedAtMs: params.nowMs,
    symbolCount: 0,
    scannedCount: 0,
    scannedLimit: scanLimit,
    previewLimit,
    previewCount: 0,
    truncated: false,
    nonEmptyCount: 0,
    emptyCount: 0,
    totalCandles: 0,
    avgCandles: null,
    medianCandles: null,
    minCandles: null,
    maxCandles: null,
    avgDepthDays: null,
    medianDepthDays: null,
    minDepthDays: null,
    maxDepthDays: null,
    oldestCandleAtMs: null,
    newestCandleAtMs: null,
    rows: [],
  });

  if (!isScalpPgConfigured()) {
    params.logDebug("history_snapshot_skipped", {
      reason: "pg_not_configured",
      timeframe,
    });
    return emptySnapshot();
  }

  const useCache =
    !params.debugLogsEnabled && HISTORY_DISCOVERY_CACHE_TTL_MS > 0;
  const cacheKey = makeHistoryDiscoveryCacheKey({
    timeframe,
    scanLimit,
    previewLimit,
  });
  if (useCache) {
    const cached = historyDiscoveryCache.get(cacheKey);
    if (cached && cached.expiresAtMs > params.nowMs) {
      params.logDebug("history_cache_hit", {
        cacheKey,
        ttlMsRemaining: cached.expiresAtMs - params.nowMs,
      });
      return cached.payload;
    }
    if (cached) historyDiscoveryCache.delete(cacheKey);
  }

  let symbols: string[] = [];
  let loadedStats: Awaited<ReturnType<typeof loadScalpCandleHistoryStatsBulk>> =
    [];
  try {
    symbols = await listScalpCandleHistorySymbols(timeframe);
    const scannedSymbols = symbols.slice(0, scanLimit);
    loadedStats = await loadScalpCandleHistoryStatsBulk(
      scannedSymbols,
      timeframe,
    );
  } catch (err: any) {
    const rowError = {
      kind: "history_snapshot",
      timeframe,
      message: err?.message || String(err),
    };
    params.rowErrors.push(rowError);
    console.error(
      `[scalp-summary][${params.requestId}] history_snapshot_error`,
      rowError,
      err?.stack || "",
    );
    params.logDebug("history_snapshot_error", rowError);
    return emptySnapshot();
  }
  const scannedSymbols = symbols.slice(0, scanLimit);
  const rows: HistoryDiscoveryRow[] = [];
  let backend: CandleHistoryBackend | "unknown" = "unknown";
  const dayMs = 24 * 60 * 60 * 1000;
  for (const row of loadedStats) {
    try {
      if (backend === "unknown") backend = row.backend;
      const candleCount = Math.max(0, Math.floor(Number(row.candleCount) || 0));
      const fromTsMsRaw = Number(row.fromTsMs);
      const toTsMsRaw = Number(row.toTsMs);
      const fromTsMs =
        Number.isFinite(fromTsMsRaw) && fromTsMsRaw > 0
          ? Math.floor(fromTsMsRaw)
          : null;
      const toTsMs =
        Number.isFinite(toTsMsRaw) && toTsMsRaw > 0
          ? Math.floor(toTsMsRaw)
          : null;
      const updatedAtMsRaw = Number(row.updatedAtMs);
      const updatedAtMs =
        Number.isFinite(updatedAtMsRaw) && updatedAtMsRaw > 0
          ? Math.floor(updatedAtMsRaw)
          : null;
      const spanMs =
        fromTsMs !== null && toTsMs !== null && toTsMs >= fromTsMs
          ? Math.max(0, toTsMs - fromTsMs)
          : null;
      const depthDays = spanMs === null ? null : spanMs / dayMs;
      const expectedCandles =
        spanMs === null
          ? null
          : Math.max(1, Math.floor(spanMs / timeframeMs) + 1);
      const coveragePct =
        expectedCandles && expectedCandles > 0
          ? Math.max(0, Math.min(100, (candleCount / expectedCandles) * 100))
          : null;
      const barsPerDay =
        depthDays !== null && depthDays > 0 ? candleCount / depthDays : null;
      rows.push({
        symbol: row.symbol,
        candles: candleCount,
        depthDays: roundMetric(depthDays),
        barsPerDay: roundMetric(barsPerDay),
        coveragePct: roundMetric(coveragePct),
        fromTsMs,
        toTsMs,
        updatedAtMs,
      } satisfies HistoryDiscoveryRow);
    } catch (err: any) {
      const rowError = {
        kind: "history_row",
        symbol: row.symbol,
        timeframe,
        message: err?.message || String(err),
      };
      params.rowErrors.push(rowError);
      console.error(
        `[scalp-summary][${params.requestId}] history_row_error`,
        rowError,
        err?.stack || "",
      );
    }
  }

  rows.sort((a, b) => {
    if (a.candles !== b.candles) return b.candles - a.candles;
    const aDepth = a.depthDays ?? -1;
    const bDepth = b.depthDays ?? -1;
    if (aDepth !== bDepth) return bDepth - aDepth;
    return a.symbol.localeCompare(b.symbol);
  });

  const nonEmptyRows = rows.filter((row) => row.candles > 0);
  const candleCounts = nonEmptyRows.map((row) => row.candles);
  const depthValues = nonEmptyRows
    .map((row) => row.depthDays)
    .filter(
      (row): row is number => row !== null && Number.isFinite(row) && row >= 0,
    );
  const totalCandles = candleCounts.reduce((acc, row) => acc + row, 0);
  const oldestCandleAtMs = nonEmptyRows.reduce<number | null>((acc, row) => {
    if (row.fromTsMs === null) return acc;
    if (acc === null) return row.fromTsMs;
    return Math.min(acc, row.fromTsMs);
  }, null);
  const newestCandleAtMs = nonEmptyRows.reduce<number | null>((acc, row) => {
    if (row.toTsMs === null) return acc;
    if (acc === null) return row.toTsMs;
    return Math.max(acc, row.toTsMs);
  }, null);

  const snapshot: HistoryDiscoverySnapshot = {
    timeframe,
    backend,
    generatedAtMs: params.nowMs,
    symbolCount: symbols.length,
    scannedCount: scannedSymbols.length,
    scannedLimit: scanLimit,
    previewLimit,
    previewCount: Math.min(rows.length, previewLimit),
    truncated: symbols.length > scannedSymbols.length,
    nonEmptyCount: nonEmptyRows.length,
    emptyCount: Math.max(0, rows.length - nonEmptyRows.length),
    totalCandles,
    avgCandles: roundMetric(meanValue(candleCounts)),
    medianCandles: roundMetric(medianValue(candleCounts)),
    minCandles: candleCounts.length ? Math.min(...candleCounts) : null,
    maxCandles: candleCounts.length ? Math.max(...candleCounts) : null,
    avgDepthDays: roundMetric(meanValue(depthValues)),
    medianDepthDays: roundMetric(medianValue(depthValues)),
    minDepthDays: depthValues.length
      ? roundMetric(Math.min(...depthValues))
      : null,
    maxDepthDays: depthValues.length
      ? roundMetric(Math.max(...depthValues))
      : null,
    oldestCandleAtMs,
    newestCandleAtMs,
    rows: rows.slice(0, previewLimit),
  };
  params.logDebug("history_snapshot", {
    timeframe,
    backend,
    symbolCount: snapshot.symbolCount,
    scannedCount: snapshot.scannedCount,
    previewCount: snapshot.previewCount,
    truncated: snapshot.truncated,
    nonEmptyCount: snapshot.nonEmptyCount,
  });
  if (useCache) {
    pruneHistoryDiscoveryCache(params.nowMs);
    historyDiscoveryCache.set(cacheKey, {
      expiresAtMs: params.nowMs + HISTORY_DISCOVERY_CACHE_TTL_MS,
      payload: snapshot,
    });
  }
  return snapshot;
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

  const startedAtMs = Date.now();
  const requestId = `scalp_summary_${startedAtMs}_${Math.floor(Math.random() * 1e6)}`;
  const debugLogsEnabled =
    parseBool(req.query.debug, false) ||
    process.env.SCALP_DEBUG_SUMMARY === "1";
  const rowErrors: Array<Record<string, unknown>> = [];
  let stage = "init";
  const logDebug = (event: string, payload: Record<string, unknown> = {}) => {
    if (!debugLogsEnabled) return;
    try {
      console.info(
        `[scalp-summary][${requestId}] ${JSON.stringify({
          event,
          ...payload,
        })}`,
      );
    } catch {
      console.info(`[scalp-summary][${requestId}]`, event, payload);
    }
  };

  try {
    const nowMs = Date.now();
    stage = "parse_query";
    const journalLimit = parseLimit(req.query.journalLimit, 80);
    const tradeLimit = parseTradeLimit(req.query.tradeLimit, 5000);
    const rangeParam = Array.isArray(req.query.range)
      ? req.query.range[0]
      : req.query.range;
    const range = resolveSummaryRange(rangeParam);
    const rangeStartMs = nowMs - SUMMARY_RANGE_LOOKBACK_MS[range];
    const requestedStrategyId = firstQueryValue(req.query.strategyId);
    const useDeploymentRegistryRequested = parseBool(
      req.query.useDeploymentRegistry,
      false,
    );
    const pgConfigured = isScalpPgConfigured();
    const useDeployments = useDeploymentRegistryRequested && pgConfigured;
    let useDeploymentsEffective = useDeployments;
    const bypassCache = parseBool(req.query.fresh, false);
    const useResponseCache =
      !debugLogsEnabled && !bypassCache && SUMMARY_CACHE_TTL_MS > 0;
    const cacheKey = makeSummaryCacheKey({
      useDeployments,
      requestedStrategyId,
      range,
      journalLimit,
      tradeLimit,
    });
    if (useResponseCache) {
      const cached = summaryResponseCache.get(cacheKey);
      if (cached && cached.expiresAtMs > nowMs) {
        logDebug("cache_hit", {
          cacheKey,
          ttlMsRemaining: cached.expiresAtMs - nowMs,
        });
        res.setHeader("x-scalp-summary-cache", "hit");
        return res.status(200).json(cached.payload);
      }
      if (cached) {
        summaryResponseCache.delete(cacheKey);
      }
    }
    logDebug("request_parsed", {
      method: req.method,
      url: req.url || null,
      requestedStrategyId: requestedStrategyId || null,
      useDeployments,
      useDeploymentRegistryRequested,
      pgConfigured,
      bypassCache,
      useResponseCache,
      range,
      journalLimit,
      tradeLimit,
    });
    if (useDeploymentRegistryRequested && !pgConfigured) {
      console.warn(
        `[scalp-summary][${requestId}] deployment_registry_fallback`,
        {
          reason: "pg_not_configured",
          requested: true,
          fallbackSource: "cron_symbols",
        },
      );
    }

    stage = "load_runtime";
    const cfg = getScalpStrategyConfig();
    const runtime = await loadScalpStrategyRuntimeSnapshot(
      cfg.enabled,
      requestedStrategyId,
    );
    const runtimeStrategies = Array.isArray(runtime.strategies)
      ? runtime.strategies
      : [];
    const strategy = runtime.strategy ||
      runtimeStrategies.find((row) => row.strategyId === runtime.strategyId) ||
      runtimeStrategies[0] || {
        strategyId: runtime.defaultStrategyId,
        shortName: runtime.defaultStrategyId,
        longName: runtime.defaultStrategyId,
        enabled: cfg.enabled,
        envEnabled: cfg.enabled,
        kvEnabled: null,
        updatedAtMs: null,
        updatedBy: null,
      };
    const dayKey = deriveScalpDayKey(nowMs, cfg.sessions.clockMode);
    const cronSymbolConfigs = getScalpCronSymbolConfigs();
    const cronSymbolConfigBySymbol = new Map(
      cronSymbolConfigs.map((row) => [row.symbol.toUpperCase(), row]),
    );
    const cronAllConfig = cronSymbolConfigBySymbol.get("*") || null;
    stage = "load_pipeline_state";
    let pipeline: ScalpPipelineSnapshot | null = null;
    try {
      pipeline = await loadScalpPipelineSnapshot(nowMs);
    } catch (err: any) {
      const rowError = {
        kind: "pipeline_state",
        message: err?.message || String(err),
      };
      rowErrors.push(rowError);
      console.error(
        `[scalp-summary][${requestId}] pipeline_state_error`,
        rowError,
        err?.stack || "",
      );
    }
    stage = "load_deployments";
    let deploymentRows: Awaited<
      ReturnType<typeof listScalpDeploymentRegistryEntries>
    > = [];
    let allDeploymentRows: Awaited<
      ReturnType<typeof listScalpDeploymentRegistryEntries>
    > = [];
    if (useDeployments) {
      try {
        allDeploymentRows = await listScalpDeploymentRegistryEntries();
        deploymentRows = allDeploymentRows.filter(
          (row) => row.enabled === true,
        );
      } catch (err: any) {
        const rowError = {
          kind: "deployment_registry",
          message: err?.message || String(err),
          fallbackSource: "cron_symbols",
        };
        rowErrors.push(rowError);
        console.error(
          `[scalp-summary][${requestId}] deployment_registry_error`,
          rowError,
          err?.stack || "",
        );
        useDeploymentsEffective = false;
      }
    }
    const cronSymbols = useDeploymentsEffective ? [] : cronSymbolConfigs;
    logDebug("runtime_loaded", {
      defaultStrategyId: runtime.defaultStrategyId,
      runtimeStrategyCount: runtimeStrategies.length,
      cronSymbolCount: cronSymbolConfigs.length,
      deploymentRowCount: deploymentRows.length,
      useDeploymentsEffective,
      dayKey,
      clockMode: cfg.sessions.clockMode,
      entrySessionProfile: cfg.sessions.entrySessionProfile,
    });

    stage = "build_rows";
    const rows: SymbolSnapshot[] = [];
    if (useDeploymentsEffective) {
      for (let idx = 0; idx < deploymentRows.length; idx += 1) {
        const deploymentRow = deploymentRows[idx]!;
        try {
          const preferredStrategyId = normalizeScalpStrategyId(
            deploymentRow.strategyId,
          );
          const strategyControl =
            runtimeStrategies.find(
              (row) => row.strategyId === preferredStrategyId,
            ) || strategy;
          const effectiveStrategyId = strategyControl.strategyId;
          const cronSymbol =
            cronSymbolConfigBySymbol.get(
              String(deploymentRow.symbol || "").toUpperCase(),
            ) || cronAllConfig;
          const deployment = resolveScalpDeployment({
            symbol: deploymentRow.symbol,
            strategyId: effectiveStrategyId,
            tuneId: deploymentRow.tuneId,
            deploymentId: deploymentRow.deploymentId,
          });
          const state = await loadScalpSessionState(
            deployment.symbol,
            dayKey,
            effectiveStrategyId,
            {
              tuneId: deployment.tuneId,
              deploymentId: deployment.deploymentId,
            },
          );
          rows.push({
            symbol: deployment.symbol,
            strategyId: effectiveStrategyId,
            tuneId: deployment.tuneId,
            deploymentId: deployment.deploymentId,
            tune: deriveTuneLabel({
              strategyId: effectiveStrategyId,
              defaultStrategyId: runtime.defaultStrategyId,
              tuneId: deployment.tuneId,
            }),
            cronSchedule: cronSymbol?.schedule ?? null,
            cronRoute: "execute-deployments",
            cronPath:
              cronSymbol?.path ||
              "/api/scalp/cron/execute-deployments?all=true",
            dayKey,
            state: state?.state ?? null,
            updatedAtMs: state?.updatedAtMs ?? null,
            lastRunAtMs: state?.run?.lastRunAtMs ?? null,
            dryRunLast:
              typeof state?.run?.dryRunLast === "boolean"
                ? state.run.dryRunLast
                : null,
            tradesPlaced: state?.stats?.tradesPlaced ?? 0,
            wins: state?.stats?.wins ?? 0,
            losses: state?.stats?.losses ?? 0,
            inTrade: state?.state === "IN_TRADE" || Boolean(state?.trade),
            tradeSide: state?.trade?.side ?? null,
            dealReference: state?.trade?.dealReference ?? null,
            reasonCodes: Array.isArray(state?.run?.lastReasonCodes)
              ? state!.run.lastReasonCodes.slice(0, 8)
              : [],
            netR: null,
            maxDrawdownR: null,
            promotionEligible:
              typeof deploymentRow.promotionGate?.eligible === "boolean"
                ? deploymentRow.promotionGate.eligible
                : null,
            promotionReason: deploymentRow.promotionGate?.reason || null,
            forwardValidation:
              deploymentRow.promotionGate?.forwardValidation || null,
          });
        } catch (err: any) {
          const rowError = {
            kind: "deployment_row",
            index: idx,
            symbol: String((deploymentRow as any)?.symbol || ""),
            strategyId: String((deploymentRow as any)?.strategyId || ""),
            tuneId: String((deploymentRow as any)?.tuneId || ""),
            deploymentId: String((deploymentRow as any)?.deploymentId || ""),
            message: err?.message || String(err),
          };
          rowErrors.push(rowError);
          console.error(
            `[scalp-summary][${requestId}] deployment_row_error`,
            rowError,
            err?.stack || "",
          );
        }
      }
    } else {
      for (let idx = 0; idx < cronSymbols.length; idx += 1) {
        const cronSymbol = cronSymbols[idx]!;
        try {
          const preferredStrategyId = normalizeScalpStrategyId(
            cronSymbol.strategyId,
          );
          const strategyControl =
            runtimeStrategies.find(
              (row) => row.strategyId === preferredStrategyId,
            ) || strategy;
          const effectiveStrategyId = strategyControl.strategyId;
          const deployment = resolveScalpDeployment({
            symbol: cronSymbol.symbol,
            strategyId: effectiveStrategyId,
            tuneId: cronSymbol.tuneId,
            deploymentId: cronSymbol.deploymentId,
          });
          const state = await loadScalpSessionState(
            deployment.symbol,
            dayKey,
            effectiveStrategyId,
            {
              tuneId: deployment.tuneId,
              deploymentId: deployment.deploymentId,
            },
          );
          rows.push({
            symbol: deployment.symbol,
            strategyId: effectiveStrategyId,
            tuneId: deployment.tuneId,
            deploymentId: deployment.deploymentId,
            tune: deriveTuneLabel({
              strategyId: effectiveStrategyId,
              defaultStrategyId: runtime.defaultStrategyId,
              tuneId: deployment.tuneId,
            }),
            cronSchedule: cronSymbol.schedule,
            cronRoute: cronSymbol.route,
            cronPath: cronSymbol.path,
            dayKey,
            state: state?.state ?? null,
            updatedAtMs: state?.updatedAtMs ?? null,
            lastRunAtMs: state?.run?.lastRunAtMs ?? null,
            dryRunLast:
              typeof state?.run?.dryRunLast === "boolean"
                ? state.run.dryRunLast
                : null,
            tradesPlaced: state?.stats?.tradesPlaced ?? 0,
            wins: state?.stats?.wins ?? 0,
            losses: state?.stats?.losses ?? 0,
            inTrade: state?.state === "IN_TRADE" || Boolean(state?.trade),
            tradeSide: state?.trade?.side ?? null,
            dealReference: state?.trade?.dealReference ?? null,
            reasonCodes: Array.isArray(state?.run?.lastReasonCodes)
              ? state!.run.lastReasonCodes.slice(0, 8)
              : [],
            netR: null,
            maxDrawdownR: null,
            promotionEligible: null,
            promotionReason: null,
            forwardValidation: null,
          });
        } catch (err: any) {
          const rowError = {
            kind: "cron_row",
            index: idx,
            symbol: String((cronSymbol as any)?.symbol || ""),
            strategyId: String((cronSymbol as any)?.strategyId || ""),
            tuneId: String((cronSymbol as any)?.tuneId || ""),
            deploymentId: String((cronSymbol as any)?.deploymentId || ""),
            message: err?.message || String(err),
          };
          rowErrors.push(rowError);
          console.error(
            `[scalp-summary][${requestId}] cron_row_error`,
            rowError,
            err?.stack || "",
          );
        }
      }
    }
    logDebug("rows_built", { rows: rows.length, rowErrors: rowErrors.length });

    stage = "compute_trade_perf";
    const tradeLedger = await loadScalpTradeLedger(tradeLimit);
    const tradesByDeploymentId = new Map<string, ScalpTradeLedgerEntry[]>();
    for (const trade of tradeLedger) {
      if (trade.dryRun) continue;
      if (
        !(
          Number.isFinite(Number(trade.exitAtMs)) &&
          Number(trade.exitAtMs) >= rangeStartMs
        )
      )
        continue;
      const deploymentId = String(trade.deploymentId || "").trim();
      if (!deploymentId) continue;
      const bucket = tradesByDeploymentId.get(deploymentId) || [];
      bucket.push(trade);
      tradesByDeploymentId.set(deploymentId, bucket);
    }
    for (const row of rows) {
      const perf = computeRangePerformance(
        tradesByDeploymentId.get(row.deploymentId) || [],
      );
      row.netR = perf?.netR ?? null;
      row.maxDrawdownR = perf?.maxDrawdownR ?? null;
    }
    stage = "load_history_discovery";
    const history = await loadHistoryDiscoverySnapshot({
      nowMs,
      debugLogsEnabled,
      rowErrors,
      requestId,
      logDebug,
    });

    const stateCounts = rows.reduce<Record<string, number>>((acc, row) => {
      const key = row.state || "MISSING";
      acc[key] = (acc[key] || 0) + 1;
      return acc;
    }, {});
    const openCount = rows.filter((row) => row.inTrade).length;
    const runCount = rows.filter((row) =>
      Number.isFinite(row.lastRunAtMs as number),
    ).length;
    const dryRunCount = rows.filter((row) => row.dryRunLast === true).length;
    const totalTradesPlaced = rows.reduce(
      (acc, row) => acc + row.tradesPlaced,
      0,
    );

    const journal = await loadScalpJournal(journalLimit);
    const strategyBySymbol = new Map(
      rows.map((row) => [row.symbol.toUpperCase(), row.strategyId]),
    );
    const allowedStrategyIds = new Set(rows.map((row) => row.strategyId));
    const allowedDeploymentIds = new Set(rows.map((row) => row.deploymentId));
    const latestExecutionBySymbol: Record<string, Record<string, unknown>> = {};
    const latestExecutionByDeploymentId: Record<
      string,
      Record<string, unknown>
    > = {};
    for (let idx = 0; idx < journal.length; idx += 1) {
      const entry = journal[idx]!;
      try {
        const entryStrategy = journalStrategyId(entry);
        const entryDeploymentId = journalDeploymentId(entry);
        const symbol = String(entry.symbol || "").toUpperCase();
        if (!symbol) continue;
        const expectedStrategyId =
          strategyBySymbol.get(symbol) || strategy.strategyId;
        if (entryStrategy && entryStrategy !== expectedStrategyId) continue;
        if (!entryStrategy && expectedStrategyId !== runtime.defaultStrategyId)
          continue;
        if (
          entry.type !== "execution" &&
          entry.type !== "state" &&
          entry.type !== "error"
        )
          continue;
        const compacted = compactJournalEntry(entry);
        if (!latestExecutionBySymbol[symbol]) {
          latestExecutionBySymbol[symbol] = compacted;
        }
        if (
          entryDeploymentId &&
          allowedDeploymentIds.has(entryDeploymentId) &&
          !latestExecutionByDeploymentId[entryDeploymentId]
        ) {
          latestExecutionByDeploymentId[entryDeploymentId] = compacted;
        }
      } catch (err: any) {
        const rowError = {
          kind: "journal_row",
          index: idx,
          symbol: String((entry as any)?.symbol || ""),
          type: String((entry as any)?.type || ""),
          message: err?.message || String(err),
        };
        rowErrors.push(rowError);
        console.error(
          `[scalp-summary][${requestId}] journal_row_error`,
          rowError,
          err?.stack || "",
        );
      }
    }
    logDebug("journal_compacted", {
      journalRows: journal.length,
      latestExecutionBySymbol: Object.keys(latestExecutionBySymbol).length,
      latestExecutionByDeploymentId: Object.keys(latestExecutionByDeploymentId)
        .length,
      rowErrors: rowErrors.length,
    });

    stage = "respond";
    const durationMs = Date.now() - startedAtMs;
    if (rowErrors.length > 0) {
      console.warn(`[scalp-summary][${requestId}] completed_with_row_errors`, {
        rowErrors: rowErrors.length,
        durationMs,
        useDeployments,
        useDeploymentsEffective,
      });
    }
    logDebug("success", { durationMs, rowErrors: rowErrors.length });
    const responsePayload: Record<string, unknown> = {
      mode: "scalp",
      generatedAtMs: nowMs,
      dayKey,
      clockMode: cfg.sessions.clockMode,
      entrySessionProfile: cfg.sessions.entrySessionProfile,
      source: useDeploymentsEffective ? "deployment_registry" : "cron_symbols",
      strategyId: strategy.strategyId,
      defaultStrategyId: runtime.defaultStrategyId,
      strategy,
      strategies: runtimeStrategies,
      summary: {
        symbols: rows.length,
        openCount,
        runCount,
        dryRunCount,
        totalTradesPlaced,
        stateCounts,
      },
      deployments: allDeploymentRows.map((row) => ({
        deploymentId: row.deploymentId,
        symbol: row.symbol,
        strategyId: row.strategyId,
        tuneId: row.tuneId,
        source: row.source,
        enabled: row.enabled,
        promotionEligible:
          typeof row.promotionGate?.eligible === "boolean"
            ? row.promotionGate.eligible
            : null,
        promotionReason: row.promotionGate?.reason || null,
        forwardValidation: row.promotionGate?.forwardValidation || null,
        updatedAtMs: row.updatedAtMs,
      })),
      range,
      symbols: rows,
      pipeline,
      history,
      latestExecutionByDeploymentId,
      latestExecutionBySymbol,
      journal: journal
        .filter((entry) => {
          const entryStrategy = journalStrategyId(entry);
          if (entryStrategy && !allowedStrategyIds.has(entryStrategy))
            return false;
          if (
            !entryStrategy &&
            !allowedStrategyIds.has(runtime.defaultStrategyId)
          )
            return false;
          return true;
        })
        .map(compactJournalEntry),
      ...(debugLogsEnabled
        ? {
            debug: {
              requestId,
              durationMs,
              stage,
              rowErrors,
            },
          }
        : {}),
    };
    if (useResponseCache) {
      pruneSummaryCache(nowMs);
      summaryResponseCache.set(cacheKey, {
        expiresAtMs: nowMs + SUMMARY_CACHE_TTL_MS,
        payload: responsePayload,
      });
      res.setHeader("x-scalp-summary-cache", "miss");
    } else {
      res.setHeader("x-scalp-summary-cache", bypassCache ? "bypass" : "off");
    }
    return res.status(200).json(responsePayload);
  } catch (err: any) {
    console.error(`[scalp-summary][${requestId}] fatal_error`, {
      stage,
      message: err?.message || String(err),
      stack: err?.stack || "",
      url: req.url || "",
      method: req.method || "",
      query: req.query || {},
      durationMs: Date.now() - startedAtMs,
    });
    return res.status(500).json({
      error: "scalp_dashboard_summary_failed",
      message: err?.message || String(err),
    });
  }
}
