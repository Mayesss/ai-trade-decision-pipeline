export const config = { runtime: 'nodejs' };

import type { NextApiRequest, NextApiResponse } from 'next';
import { Prisma } from '@prisma/client';

import { requireAdminAccess } from '../../../../lib/admin';
import { getScalpCronSymbolConfigs } from '../../../../lib/symbolRegistry';
import {
  listScalpCandleHistorySymbols,
  loadScalpCandleHistory,
  loadScalpCandleHistoryBulk,
  normalizeHistoryTimeframe,
  timeframeToMs,
  type CandleHistoryBackend,
} from '../../../../lib/scalp/candleHistory';
import { getScalpStrategyConfig } from '../../../../lib/scalp/config';
import { listScalpDeploymentRegistryEntries, type ScalpForwardValidationMetrics } from '../../../../lib/scalp/deploymentRegistry';
import { DEFAULT_SCALP_TUNE_ID, resolveScalpDeployment } from '../../../../lib/scalp/deployments';
import { isScalpPgConfigured, scalpPrisma } from '../../../../lib/scalp/pg/client';
import { normalizeScalpStrategyId } from '../../../../lib/scalp/strategies/registry';
import { deriveScalpDayKey } from '../../../../lib/scalp/stateMachine';
import { loadScalpJournal, loadScalpSessionState, loadScalpStrategyRuntimeSnapshot, loadScalpTradeLedger } from '../../../../lib/scalp/store';
import type { ScalpJournalEntry, ScalpTradeLedgerEntry } from '../../../../lib/scalp/types';

type SummaryRangeKey = '7D' | '30D' | '6M';
const SUMMARY_RANGE_LOOKBACK_MS: Record<SummaryRangeKey, number> = {
  '7D': 7 * 24 * 60 * 60 * 1000,
  '30D': 30 * 24 * 60 * 60 * 1000,
  '6M': 183 * 24 * 60 * 60 * 1000,
};
const SUMMARY_CACHE_TTL_MS = (() => {
  const value = Number(process.env.SCALP_DASHBOARD_SUMMARY_CACHE_TTL_MS ?? 12_000);
  if (!Number.isFinite(value)) return 12_000;
  return Math.max(0, Math.floor(value));
})();
const SUMMARY_CACHE_MAX_ENTRIES = 32;
const summaryResponseCache = new Map<string, { expiresAtMs: number; payload: Record<string, unknown> }>();
const HISTORY_DISCOVERY_CACHE_TTL_MS = (() => {
  const value = Number(process.env.SCALP_DASHBOARD_HISTORY_CACHE_TTL_MS ?? 300_000);
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
const HISTORY_DISCOVERY_BATCH_SIZE = (() => {
  const value = Number(process.env.SCALP_DASHBOARD_HISTORY_BATCH_SIZE ?? 10);
  if (!Number.isFinite(value)) return 10;
  return Math.max(1, Math.min(100, Math.floor(value)));
})();
const historyDiscoveryCache = new Map<string, { expiresAtMs: number; payload: HistoryDiscoverySnapshot }>();

type SymbolSnapshot = {
  symbol: string;
  strategyId: string;
  tuneId: string;
  deploymentId: string;
  tune: string;
  cronSchedule: string | null;
  cronRoute: 'execute-deployments';
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
  tradeSide: 'BUY' | 'SELL' | null;
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
  backend: CandleHistoryBackend | 'unknown';
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

type ScalpPipelineSnapshot = {
  panicStop: {
    enabled: boolean;
    reason: string | null;
    updatedAtMs: number | null;
    updatedBy: string | null;
  };
  orchestrator: {
    runId: string | null;
    stage: string | null;
    startedAtMs: number | null;
    updatedAtMs: number | null;
    completedAtMs: number | null;
    runningSinceMs: number | null;
    isRunning: boolean;
    progressPct: number | null;
    progressLabel: string | null;
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
};

function asRecord(value: unknown): Record<string, unknown> {
  if (!value || typeof value !== 'object' || Array.isArray(value)) return {};
  return value as Record<string, unknown>;
}

function asTsMs(value: unknown): number | null {
  const n = Number(value);
  if (!Number.isFinite(n) || n <= 0) return null;
  return Math.floor(n);
}

function parseUnknownBool(value: unknown): boolean {
  if (typeof value === 'boolean') return value;
  const normalized = String(value || '')
    .trim()
    .toLowerCase();
  return ['1', 'true', 'yes', 'on'].includes(normalized);
}

function normalizeReason(value: unknown): string | null {
  const text = String(value || '').trim();
  if (!text) return null;
  return text.slice(0, 240);
}

function normalizeUpdatedBy(value: unknown): string | null {
  const text = String(value || '').trim();
  if (!text) return null;
  return text.slice(0, 120);
}

function pipelineStageMeta(stageRaw: unknown): { progressPct: number | null; progressLabel: string | null } {
  const stage = String(stageRaw || '')
    .trim()
    .toLowerCase();
  if (!stage) return { progressPct: null, progressLabel: null };
  const map: Record<string, { pct: number; label: string }> = {
    discover: { pct: 10, label: 'discovering symbols' },
    prepare: { pct: 35, label: 'preparing/backfilling history' },
    worker: { pct: 70, label: 'running cycle worker' },
    aggregate: { pct: 88, label: 'aggregating cycle results' },
    promotion: { pct: 96, label: 'applying promotion gate' },
    done: { pct: 100, label: 'completed' },
  };
  const hit = map[stage];
  if (!hit) return { progressPct: null, progressLabel: stage.replace(/_/g, ' ') };
  return { progressPct: hit.pct, progressLabel: hit.label };
}

async function loadScalpPipelineSnapshot(nowMs: number): Promise<ScalpPipelineSnapshot | null> {
  if (!isScalpPgConfigured()) return null;
  const db = scalpPrisma();
  const rows = await db.$queryRaw<
    Array<{
      orchestratorPayload: unknown;
      panicStopPayload: unknown;
      panicStopUpdatedAtMs: bigint | number | null;
      cycleId: string | null;
      cycleStatus: string | null;
      cycleCreatedAtMs: bigint | number | null;
      cycleUpdatedAtMs: bigint | number | null;
      cycleCompletedAtMs: bigint | number | null;
      cycleSummary: unknown;
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
    selected_cycle AS (
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
      ORDER BY
        CASE WHEN status = 'running'::scalp_cycle_status THEN 0 ELSE 1 END,
        updated_at DESC
      LIMIT 1
    )
    SELECT
      (SELECT payload FROM orchestrator_state) AS "orchestratorPayload",
      (SELECT payload FROM panic_stop_state) AS "panicStopPayload",
      (SELECT updated_at_ms FROM panic_stop_state) AS "panicStopUpdatedAtMs",
      sc.cycle_id AS "cycleId",
      sc.status AS "cycleStatus",
      sc.created_at_ms AS "cycleCreatedAtMs",
      sc.updated_at_ms AS "cycleUpdatedAtMs",
      sc.completed_at_ms AS "cycleCompletedAtMs",
      sc.latest_summary_json AS "cycleSummary"
    FROM selected_cycle sc
    UNION ALL
    SELECT
      (SELECT payload FROM orchestrator_state) AS "orchestratorPayload",
      (SELECT payload FROM panic_stop_state) AS "panicStopPayload",
      (SELECT updated_at_ms FROM panic_stop_state) AS "panicStopUpdatedAtMs",
      NULL::text AS "cycleId",
      NULL::text AS "cycleStatus",
      NULL::bigint AS "cycleCreatedAtMs",
      NULL::bigint AS "cycleUpdatedAtMs",
      NULL::bigint AS "cycleCompletedAtMs",
      NULL::jsonb AS "cycleSummary"
    WHERE NOT EXISTS (SELECT 1 FROM selected_cycle);
  `);
  const row = rows[0];
  if (!row) return null;

  const orchestratorPayload = asRecord(row.orchestratorPayload);
  const panicStopPayload = asRecord(row.panicStopPayload);
  const orchestratorStage = String(orchestratorPayload.stage || '').trim() || null;
  const orchestratorStartedAtMs = asTsMs(orchestratorPayload.startedAtMs);
  const orchestratorCompletedAtMs = asTsMs(orchestratorPayload.completedAtMs);
  const orchestratorUpdatedAtMs = asTsMs(orchestratorPayload.updatedAtMs);
  const orchestratorRunning =
    Boolean(orchestratorStage) &&
    orchestratorStage !== 'done' &&
    orchestratorStartedAtMs !== null &&
    (orchestratorCompletedAtMs === null || orchestratorCompletedAtMs < orchestratorStartedAtMs);
  const stageMeta = pipelineStageMeta(orchestratorStage);

  const summary = asRecord(row.cycleSummary);
  const totals = asRecord(summary.totals);
  const cycleProgressPct = Number(summary.progressPct);
  return {
    panicStop: {
      enabled: parseUnknownBool(panicStopPayload.enabled),
      reason: normalizeReason(panicStopPayload.reason),
      updatedAtMs: asTsMs(row.panicStopUpdatedAtMs),
      updatedBy: normalizeUpdatedBy(panicStopPayload.updatedBy),
    },
    orchestrator: orchestratorStage
      ? {
          runId: String(orchestratorPayload.runId || '').trim() || null,
          stage: orchestratorStage,
          startedAtMs: orchestratorStartedAtMs,
          updatedAtMs: orchestratorUpdatedAtMs,
          completedAtMs: orchestratorCompletedAtMs,
          runningSinceMs: orchestratorRunning ? orchestratorStartedAtMs : null,
          isRunning: orchestratorRunning,
          progressPct:
            Number.isFinite(cycleProgressPct) && cycleProgressPct >= 0
              ? Math.max(stageMeta.progressPct ?? 0, Math.min(100, cycleProgressPct))
              : stageMeta.progressPct,
          progressLabel: stageMeta.progressLabel,
          lastError: String(orchestratorPayload.lastError || '').trim() || null,
        }
      : null,
    cycle: row.cycleId
      ? {
          cycleId: row.cycleId,
          status: row.cycleStatus,
          createdAtMs: asTsMs(row.cycleCreatedAtMs),
          updatedAtMs: asTsMs(row.cycleUpdatedAtMs),
          completedAtMs: asTsMs(row.cycleCompletedAtMs),
          progressPct: Number.isFinite(cycleProgressPct) ? Math.max(0, Math.min(100, cycleProgressPct)) : null,
          totals: Object.keys(totals).length
            ? {
                tasks: Number.isFinite(Number(totals.tasks)) ? Math.max(0, Math.floor(Number(totals.tasks))) : null,
                pending: Number.isFinite(Number(totals.pending)) ? Math.max(0, Math.floor(Number(totals.pending))) : null,
                running: Number.isFinite(Number(totals.running)) ? Math.max(0, Math.floor(Number(totals.running))) : null,
                completed: Number.isFinite(Number(totals.completed))
                  ? Math.max(0, Math.floor(Number(totals.completed)))
                  : null,
                failed: Number.isFinite(Number(totals.failed)) ? Math.max(0, Math.floor(Number(totals.failed))) : null,
              }
            : null,
        }
      : null,
  };
}

function parseLimit(value: string | string[] | undefined, fallback: number): number {
  const first = Array.isArray(value) ? value[0] : value;
  const n = Number(first);
  if (!Number.isFinite(n) || n <= 0) return fallback;
  return Math.max(1, Math.min(300, Math.floor(n)));
}

function parseTradeLimit(value: string | string[] | undefined, fallback: number): number {
  const first = Array.isArray(value) ? value[0] : value;
  const n = Number(first);
  if (!Number.isFinite(n) || n <= 0) return fallback;
  return Math.max(200, Math.min(50_000, Math.floor(n)));
}

function parseBool(value: string | string[] | undefined, fallback: boolean): boolean {
  const first = firstQueryValue(value);
  if (!first) return fallback;
  const normalized = first.toLowerCase();
  if (['1', 'true', 'yes', 'on'].includes(normalized)) return true;
  if (['0', 'false', 'no', 'off'].includes(normalized)) return false;
  return fallback;
}

function resolveSummaryRange(raw: unknown): SummaryRangeKey {
  const normalized = String(raw || '')
    .trim()
    .toUpperCase();
  if (normalized === '30D') return '30D';
  if (normalized === '6M') return '6M';
  return '7D';
}

function firstQueryValue(value: string | string[] | undefined): string | undefined {
  if (typeof value === 'string') return value.trim() || undefined;
  if (Array.isArray(value) && value.length > 0) return String(value[0] || '').trim() || undefined;
  return undefined;
}

function journalStrategyId(entry: ScalpJournalEntry): string | null {
  const payload = entry.payload && typeof entry.payload === 'object' ? (entry.payload as Record<string, unknown>) : {};
  const normalized = normalizeScalpStrategyId(payload.strategyId);
  return normalized || null;
}

function journalDeploymentId(entry: ScalpJournalEntry): string | null {
  const payload = entry.payload && typeof entry.payload === 'object' ? (entry.payload as Record<string, unknown>) : {};
  const normalized = String(payload.deploymentId || '').trim();
  return normalized || null;
}

function compactJournalEntry(entry: ScalpJournalEntry): Record<string, unknown> {
  const compactPayload = (value: unknown, depth = 0): unknown => {
    if (value === null || value === undefined) return null;
    if (depth >= 3) return '[truncated]';
    const t = typeof value;
    if (t === 'string') {
      const text = String(value);
      return text.length > 400 ? `${text.slice(0, 397)}...` : text;
    }
    if (t === 'number') {
      const n = Number(value);
      return Number.isFinite(n) ? n : null;
    }
    if (t === 'boolean') return value;
    if (t === 'bigint') {
      const n = Number(value);
      return Number.isFinite(n) ? n : String(value);
    }
    if (Array.isArray(value)) {
      return value.slice(0, 20).map((row) => compactPayload(row, depth + 1));
    }
    if (value && typeof value === 'object') {
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
    reasonCodes: Array.isArray(entry.reasonCodes) ? entry.reasonCodes.slice(0, 8) : [],
    payload: compactPayload(entry.payload ?? {}),
  };
}

function computeRangePerformance(trades: ScalpTradeLedgerEntry[]): { netR: number; maxDrawdownR: number } | null {
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
  const explicitTune = String(params.tuneId || '').trim().toLowerCase();
  if (explicitTune && explicitTune !== DEFAULT_SCALP_TUNE_ID) return explicitTune;
  const strategyId = normalizeScalpStrategyId(params.strategyId);
  const defaultStrategyId = normalizeScalpStrategyId(params.defaultStrategyId);
  if (!strategyId) return 'default';
  if (!defaultStrategyId || strategyId === defaultStrategyId) return 'default';
  const prefix = `${defaultStrategyId}_`;
  if (strategyId.startsWith(prefix) && strategyId.length > prefix.length) {
    return strategyId.slice(prefix.length);
  }
  return strategyId;
}

function setNoStoreHeaders(res: NextApiResponse): void {
  res.setHeader('Cache-Control', 'no-store, no-cache, must-revalidate, proxy-revalidate');
  res.setHeader('Pragma', 'no-cache');
  res.setHeader('Expires', '0');
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
  while (summaryResponseCache.size > SUMMARY_CACHE_MAX_ENTRIES && keys.length > 0) {
    const key = keys.shift();
    if (!key) break;
    summaryResponseCache.delete(key);
  }
}

function makeHistoryDiscoveryCacheKey(input: { timeframe: string; scanLimit: number; previewLimit: number }): string {
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
  while (historyDiscoveryCache.size > HISTORY_DISCOVERY_CACHE_MAX_ENTRIES && keys.length > 0) {
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
  const timeframe = normalizeHistoryTimeframe(String(process.env.SCALP_DASHBOARD_HISTORY_TIMEFRAME || '1m'));
  const scanLimit = HISTORY_DISCOVERY_SCAN_LIMIT;
  const previewLimit = HISTORY_DISCOVERY_PREVIEW_LIMIT;
  const timeframeMs = Math.max(60_000, timeframeToMs(timeframe));
  const useCache = !params.debugLogsEnabled && HISTORY_DISCOVERY_CACHE_TTL_MS > 0;
  const cacheKey = makeHistoryDiscoveryCacheKey({ timeframe, scanLimit, previewLimit });
  if (useCache) {
    const cached = historyDiscoveryCache.get(cacheKey);
    if (cached && cached.expiresAtMs > params.nowMs) {
      params.logDebug('history_cache_hit', {
        cacheKey,
        ttlMsRemaining: cached.expiresAtMs - params.nowMs,
      });
      return cached.payload;
    }
    if (cached) historyDiscoveryCache.delete(cacheKey);
  }

  const symbols = await listScalpCandleHistorySymbols(timeframe);
  const scannedSymbols = symbols.slice(0, scanLimit);
  const rows: HistoryDiscoveryRow[] = [];
  let backend: CandleHistoryBackend | 'unknown' = 'unknown';
  const dayMs = 24 * 60 * 60 * 1000;
  for (let i = 0; i < scannedSymbols.length; i += HISTORY_DISCOVERY_BATCH_SIZE) {
    const batch = scannedSymbols.slice(i, i + HISTORY_DISCOVERY_BATCH_SIZE);
    const loadedBatch = await loadScalpCandleHistoryBulk(batch, timeframe);
    const batchRows = await Promise.all(
      batch.map(async (symbol, index) => {
        try {
          const loaded = loadedBatch[index] || (await loadScalpCandleHistory(symbol, timeframe));
          if (backend === 'unknown') backend = loaded.backend;
          const record = loaded.record;
          const candles = Array.isArray(record?.candles) ? record.candles : [];
          const candleCount = candles.length;
          const firstTsRaw = candleCount > 0 ? Number(candles[0]?.[0]) : NaN;
          const lastTsRaw = candleCount > 0 ? Number(candles[candleCount - 1]?.[0]) : NaN;
          const fromTsMs = Number.isFinite(firstTsRaw) && firstTsRaw > 0 ? Math.floor(firstTsRaw) : null;
          const toTsMs = Number.isFinite(lastTsRaw) && lastTsRaw > 0 ? Math.floor(lastTsRaw) : null;
          const updatedAtMsRaw = Number(record?.updatedAtMs);
          const updatedAtMs = Number.isFinite(updatedAtMsRaw) && updatedAtMsRaw > 0 ? Math.floor(updatedAtMsRaw) : null;
          const spanMs =
            fromTsMs !== null && toTsMs !== null && toTsMs >= fromTsMs ? Math.max(0, toTsMs - fromTsMs) : null;
          const depthDays = spanMs === null ? null : spanMs / dayMs;
          const expectedCandles =
            spanMs === null
              ? null
              : Math.max(1, Math.floor(spanMs / timeframeMs) + 1);
          const coveragePct =
            expectedCandles && expectedCandles > 0
              ? Math.max(0, Math.min(100, (candleCount / expectedCandles) * 100))
              : null;
          const barsPerDay = depthDays !== null && depthDays > 0 ? candleCount / depthDays : null;
          return {
            symbol,
            candles: Math.max(0, Math.floor(candleCount)),
            depthDays: roundMetric(depthDays),
            barsPerDay: roundMetric(barsPerDay),
            coveragePct: roundMetric(coveragePct),
            fromTsMs,
            toTsMs,
            updatedAtMs,
          } satisfies HistoryDiscoveryRow;
        } catch (err: any) {
          const rowError = {
            kind: 'history_row',
            symbol,
            timeframe,
            message: err?.message || String(err),
          };
          params.rowErrors.push(rowError);
          console.error(`[scalp-summary][${params.requestId}] history_row_error`, rowError, err?.stack || '');
          return null;
        }
      }),
    );
    for (const row of batchRows) {
      if (row) rows.push(row);
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
    .filter((row): row is number => row !== null && Number.isFinite(row) && row >= 0);
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
    minDepthDays: depthValues.length ? roundMetric(Math.min(...depthValues)) : null,
    maxDepthDays: depthValues.length ? roundMetric(Math.max(...depthValues)) : null,
    oldestCandleAtMs,
    newestCandleAtMs,
    rows: rows.slice(0, previewLimit),
  };
  params.logDebug('history_snapshot', {
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

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
  if (req.method !== 'GET') {
    return res.status(405).json({ error: 'Method Not Allowed', message: 'Use GET' });
  }
  if (!requireAdminAccess(req, res)) return;
  setNoStoreHeaders(res);

  const startedAtMs = Date.now();
  const requestId = `scalp_summary_${startedAtMs}_${Math.floor(Math.random() * 1e6)}`;
  const debugLogsEnabled = parseBool(req.query.debug, false) || process.env.SCALP_DEBUG_SUMMARY === '1';
  const rowErrors: Array<Record<string, unknown>> = [];
  let stage = 'init';
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
    stage = 'parse_query';
    const journalLimit = parseLimit(req.query.journalLimit, 80);
    const tradeLimit = parseTradeLimit(req.query.tradeLimit, 5000);
    const rangeParam = Array.isArray(req.query.range) ? req.query.range[0] : req.query.range;
    const range = resolveSummaryRange(rangeParam);
    const rangeStartMs = nowMs - SUMMARY_RANGE_LOOKBACK_MS[range];
    const requestedStrategyId = firstQueryValue(req.query.strategyId);
    const useDeployments = parseBool(req.query.useDeploymentRegistry, false);
    const bypassCache = parseBool(req.query.fresh, false);
    const useResponseCache = !debugLogsEnabled && !bypassCache && SUMMARY_CACHE_TTL_MS > 0;
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
        logDebug('cache_hit', {
          cacheKey,
          ttlMsRemaining: cached.expiresAtMs - nowMs,
        });
        res.setHeader('x-scalp-summary-cache', 'hit');
        return res.status(200).json(cached.payload);
      }
      if (cached) {
        summaryResponseCache.delete(cacheKey);
      }
    }
    logDebug('request_parsed', {
      method: req.method,
      url: req.url || null,
      requestedStrategyId: requestedStrategyId || null,
      useDeployments,
      bypassCache,
      useResponseCache,
      range,
      journalLimit,
      tradeLimit,
    });

    stage = 'load_runtime';
    const cfg = getScalpStrategyConfig();
    const runtime = await loadScalpStrategyRuntimeSnapshot(cfg.enabled, requestedStrategyId);
    const runtimeStrategies = Array.isArray(runtime.strategies) ? runtime.strategies : [];
    const strategy =
      runtime.strategy ||
      runtimeStrategies.find((row) => row.strategyId === runtime.strategyId) ||
      runtimeStrategies[0] ||
      {
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
    const cronSymbolConfigBySymbol = new Map(cronSymbolConfigs.map((row) => [row.symbol.toUpperCase(), row]));
    const cronAllConfig = cronSymbolConfigBySymbol.get('*') || null;
    const cronSymbols = useDeployments ? [] : cronSymbolConfigs;
    stage = 'load_pipeline_state';
    const pipeline = await loadScalpPipelineSnapshot(nowMs);
    stage = 'load_deployments';
    const deploymentRows = useDeployments ? await listScalpDeploymentRegistryEntries({ enabled: true }) : [];
    logDebug('runtime_loaded', {
      defaultStrategyId: runtime.defaultStrategyId,
      runtimeStrategyCount: runtimeStrategies.length,
      cronSymbolCount: cronSymbolConfigs.length,
      deploymentRowCount: deploymentRows.length,
      dayKey,
      clockMode: cfg.sessions.clockMode,
      entrySessionProfile: cfg.sessions.entrySessionProfile,
    });

    stage = 'build_rows';
    const rows: SymbolSnapshot[] = [];
    if (useDeployments) {
      for (let idx = 0; idx < deploymentRows.length; idx += 1) {
        const deploymentRow = deploymentRows[idx]!;
        try {
          const preferredStrategyId = normalizeScalpStrategyId(deploymentRow.strategyId);
          const strategyControl =
            runtimeStrategies.find((row) => row.strategyId === preferredStrategyId) || strategy;
          const effectiveStrategyId = strategyControl.strategyId;
          const cronSymbol = cronSymbolConfigBySymbol.get(String(deploymentRow.symbol || '').toUpperCase()) || cronAllConfig;
          const deployment = resolveScalpDeployment({
            symbol: deploymentRow.symbol,
            strategyId: effectiveStrategyId,
            tuneId: deploymentRow.tuneId,
            deploymentId: deploymentRow.deploymentId,
          });
          const state = await loadScalpSessionState(deployment.symbol, dayKey, effectiveStrategyId, {
            tuneId: deployment.tuneId,
            deploymentId: deployment.deploymentId,
          });
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
            cronRoute: 'execute-deployments',
            cronPath: cronSymbol?.path || '/api/scalp/cron/execute-deployments?all=true',
            dayKey,
            state: state?.state ?? null,
            updatedAtMs: state?.updatedAtMs ?? null,
            lastRunAtMs: state?.run?.lastRunAtMs ?? null,
            dryRunLast: typeof state?.run?.dryRunLast === 'boolean' ? state.run.dryRunLast : null,
            tradesPlaced: state?.stats?.tradesPlaced ?? 0,
            wins: state?.stats?.wins ?? 0,
            losses: state?.stats?.losses ?? 0,
            inTrade: state?.state === 'IN_TRADE' || Boolean(state?.trade),
            tradeSide: state?.trade?.side ?? null,
            dealReference: state?.trade?.dealReference ?? null,
            reasonCodes: Array.isArray(state?.run?.lastReasonCodes) ? state!.run.lastReasonCodes.slice(0, 8) : [],
            netR: null,
            maxDrawdownR: null,
            promotionEligible: typeof deploymentRow.promotionGate?.eligible === 'boolean' ? deploymentRow.promotionGate.eligible : null,
            promotionReason: deploymentRow.promotionGate?.reason || null,
            forwardValidation: deploymentRow.promotionGate?.forwardValidation || null,
          });
        } catch (err: any) {
          const rowError = {
            kind: 'deployment_row',
            index: idx,
            symbol: String((deploymentRow as any)?.symbol || ''),
            strategyId: String((deploymentRow as any)?.strategyId || ''),
            tuneId: String((deploymentRow as any)?.tuneId || ''),
            deploymentId: String((deploymentRow as any)?.deploymentId || ''),
            message: err?.message || String(err),
          };
          rowErrors.push(rowError);
          console.error(`[scalp-summary][${requestId}] deployment_row_error`, rowError, err?.stack || '');
        }
      }
    } else {
      for (let idx = 0; idx < cronSymbols.length; idx += 1) {
        const cronSymbol = cronSymbols[idx]!;
        try {
          const preferredStrategyId = normalizeScalpStrategyId(cronSymbol.strategyId);
          const strategyControl =
            runtimeStrategies.find((row) => row.strategyId === preferredStrategyId) || strategy;
          const effectiveStrategyId = strategyControl.strategyId;
          const deployment = resolveScalpDeployment({
            symbol: cronSymbol.symbol,
            strategyId: effectiveStrategyId,
            tuneId: cronSymbol.tuneId,
            deploymentId: cronSymbol.deploymentId,
          });
          const state = await loadScalpSessionState(deployment.symbol, dayKey, effectiveStrategyId, {
            tuneId: deployment.tuneId,
            deploymentId: deployment.deploymentId,
          });
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
            dryRunLast: typeof state?.run?.dryRunLast === 'boolean' ? state.run.dryRunLast : null,
            tradesPlaced: state?.stats?.tradesPlaced ?? 0,
            wins: state?.stats?.wins ?? 0,
            losses: state?.stats?.losses ?? 0,
            inTrade: state?.state === 'IN_TRADE' || Boolean(state?.trade),
            tradeSide: state?.trade?.side ?? null,
            dealReference: state?.trade?.dealReference ?? null,
            reasonCodes: Array.isArray(state?.run?.lastReasonCodes) ? state!.run.lastReasonCodes.slice(0, 8) : [],
            netR: null,
            maxDrawdownR: null,
            promotionEligible: null,
            promotionReason: null,
            forwardValidation: null,
          });
        } catch (err: any) {
          const rowError = {
            kind: 'cron_row',
            index: idx,
            symbol: String((cronSymbol as any)?.symbol || ''),
            strategyId: String((cronSymbol as any)?.strategyId || ''),
            tuneId: String((cronSymbol as any)?.tuneId || ''),
            deploymentId: String((cronSymbol as any)?.deploymentId || ''),
            message: err?.message || String(err),
          };
          rowErrors.push(rowError);
          console.error(`[scalp-summary][${requestId}] cron_row_error`, rowError, err?.stack || '');
        }
      }
    }
    logDebug('rows_built', { rows: rows.length, rowErrors: rowErrors.length });

    stage = 'compute_trade_perf';
    const tradeLedger = await loadScalpTradeLedger(tradeLimit);
    const tradesByDeploymentId = new Map<string, ScalpTradeLedgerEntry[]>();
    for (const trade of tradeLedger) {
      if (trade.dryRun) continue;
      if (!(Number.isFinite(Number(trade.exitAtMs)) && Number(trade.exitAtMs) >= rangeStartMs)) continue;
      const deploymentId = String(trade.deploymentId || '').trim();
      if (!deploymentId) continue;
      const bucket = tradesByDeploymentId.get(deploymentId) || [];
      bucket.push(trade);
      tradesByDeploymentId.set(deploymentId, bucket);
    }
    for (const row of rows) {
      const perf = computeRangePerformance(tradesByDeploymentId.get(row.deploymentId) || []);
      row.netR = perf?.netR ?? null;
      row.maxDrawdownR = perf?.maxDrawdownR ?? null;
    }
    stage = 'load_history_discovery';
    const history = await loadHistoryDiscoverySnapshot({
      nowMs,
      debugLogsEnabled,
      rowErrors,
      requestId,
      logDebug,
    });

    const stateCounts = rows.reduce<Record<string, number>>((acc, row) => {
      const key = row.state || 'MISSING';
      acc[key] = (acc[key] || 0) + 1;
      return acc;
    }, {});
    const openCount = rows.filter((row) => row.inTrade).length;
    const runCount = rows.filter((row) => Number.isFinite(row.lastRunAtMs as number)).length;
    const dryRunCount = rows.filter((row) => row.dryRunLast === true).length;
    const totalTradesPlaced = rows.reduce((acc, row) => acc + row.tradesPlaced, 0);

    const journal = await loadScalpJournal(journalLimit);
    const strategyBySymbol = new Map(rows.map((row) => [row.symbol.toUpperCase(), row.strategyId]));
    const allowedStrategyIds = new Set(rows.map((row) => row.strategyId));
    const allowedDeploymentIds = new Set(rows.map((row) => row.deploymentId));
    const latestExecutionBySymbol: Record<string, Record<string, unknown>> = {};
    const latestExecutionByDeploymentId: Record<string, Record<string, unknown>> = {};
    for (let idx = 0; idx < journal.length; idx += 1) {
      const entry = journal[idx]!;
      try {
        const entryStrategy = journalStrategyId(entry);
        const entryDeploymentId = journalDeploymentId(entry);
        const symbol = String(entry.symbol || '').toUpperCase();
        if (!symbol) continue;
        const expectedStrategyId = strategyBySymbol.get(symbol) || strategy.strategyId;
        if (entryStrategy && entryStrategy !== expectedStrategyId) continue;
        if (!entryStrategy && expectedStrategyId !== runtime.defaultStrategyId) continue;
        if (entry.type !== 'execution' && entry.type !== 'state' && entry.type !== 'error') continue;
        const compacted = compactJournalEntry(entry);
        if (!latestExecutionBySymbol[symbol]) {
          latestExecutionBySymbol[symbol] = compacted;
        }
        if (entryDeploymentId && allowedDeploymentIds.has(entryDeploymentId) && !latestExecutionByDeploymentId[entryDeploymentId]) {
          latestExecutionByDeploymentId[entryDeploymentId] = compacted;
        }
      } catch (err: any) {
        const rowError = {
          kind: 'journal_row',
          index: idx,
          symbol: String((entry as any)?.symbol || ''),
          type: String((entry as any)?.type || ''),
          message: err?.message || String(err),
        };
        rowErrors.push(rowError);
        console.error(`[scalp-summary][${requestId}] journal_row_error`, rowError, err?.stack || '');
      }
    }
    logDebug('journal_compacted', {
      journalRows: journal.length,
      latestExecutionBySymbol: Object.keys(latestExecutionBySymbol).length,
      latestExecutionByDeploymentId: Object.keys(latestExecutionByDeploymentId).length,
      rowErrors: rowErrors.length,
    });

    stage = 'respond';
    const durationMs = Date.now() - startedAtMs;
    if (rowErrors.length > 0) {
      console.warn(`[scalp-summary][${requestId}] completed_with_row_errors`, {
        rowErrors: rowErrors.length,
        durationMs,
        useDeployments,
      });
    }
    logDebug('success', { durationMs, rowErrors: rowErrors.length });
    const responsePayload: Record<string, unknown> = {
      mode: 'scalp',
      generatedAtMs: nowMs,
      dayKey,
      clockMode: cfg.sessions.clockMode,
      entrySessionProfile: cfg.sessions.entrySessionProfile,
      source: useDeployments ? 'deployment_registry' : 'cron_symbols',
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
      range,
      symbols: rows,
      pipeline,
      history,
      latestExecutionByDeploymentId,
      latestExecutionBySymbol,
      journal: journal
        .filter((entry) => {
          const entryStrategy = journalStrategyId(entry);
          if (entryStrategy && !allowedStrategyIds.has(entryStrategy)) return false;
          if (!entryStrategy && !allowedStrategyIds.has(runtime.defaultStrategyId)) return false;
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
      res.setHeader('x-scalp-summary-cache', 'miss');
    } else {
      res.setHeader('x-scalp-summary-cache', bypassCache ? 'bypass' : 'off');
    }
    return res.status(200).json(responsePayload);
  } catch (err: any) {
    console.error(`[scalp-summary][${requestId}] fatal_error`, {
      stage,
      message: err?.message || String(err),
      stack: err?.stack || '',
      url: req.url || '',
      method: req.method || '',
      query: req.query || {},
      durationMs: Date.now() - startedAtMs,
    });
    return res.status(500).json({
      error: 'scalp_dashboard_summary_failed',
      message: err?.message || String(err),
    });
  }
}
