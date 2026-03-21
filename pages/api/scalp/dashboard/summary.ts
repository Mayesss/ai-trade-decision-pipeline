export const config = { runtime: "nodejs" };

import type { NextApiRequest, NextApiResponse } from "next";

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
import { normalizeScalpEntrySessionProfile } from "../../../../lib/scalp/sessions";
import {
  listScalpDeploymentRegistryEntries,
  type ScalpForwardValidationMetrics,
} from "../../../../lib/scalp/deploymentRegistry";
import {
  DEFAULT_SCALP_TUNE_ID,
  resolveScalpDeployment,
} from "../../../../lib/scalp/deployments";
import { isScalpPgConfigured } from "../../../../lib/scalp/pg/client";
import { loadScalpPanicStopState } from "../../../../lib/scalp/panicStop";
import {
  listScalpDeploymentWeeklyMetricRows,
  loadScalpPipelineJobsHealth,
} from "../../../../lib/scalp/pipelineJobs";
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
const DEFAULT_WEEKLY_METRICS_RETENTION_DAYS = 1095;
const WORKER_ROWS_LIMIT_MIN = 8_000;
const WORKER_ROWS_LIMIT_MAX = 500_000;
const WORKER_ROWS_RETENTION_DAYS = (() => {
  const value = Number(
    process.env.SCALP_HOUSEKEEPING_CYCLE_RETENTION_DAYS ??
      DEFAULT_WEEKLY_METRICS_RETENTION_DAYS,
  );
  if (!Number.isFinite(value)) return DEFAULT_WEEKLY_METRICS_RETENTION_DAYS;
  return Math.max(30, Math.min(3650, Math.floor(value)));
})();
const WORKER_ROWS_LIMIT_OVERRIDE = (() => {
  const value = Number(process.env.SCALP_DASHBOARD_WORKER_ROWS_LIMIT);
  if (!Number.isFinite(value) || value <= 0) return null;
  return Math.max(
    WORKER_ROWS_LIMIT_MIN,
    Math.min(WORKER_ROWS_LIMIT_MAX, Math.floor(value)),
  );
})();
const historyDiscoveryCache = new Map<
  string,
  { expiresAtMs: number; payload: HistoryDiscoverySnapshot }
>();

function resolveWorkerRowsFetchLimit(expectedDeploymentCount: number): number {
  if (WORKER_ROWS_LIMIT_OVERRIDE !== null) return WORKER_ROWS_LIMIT_OVERRIDE;
  const deploymentCount = Math.max(
    1,
    Math.floor(Number(expectedDeploymentCount) || 0),
  );
  const retentionWeeks = Math.max(
    12,
    Math.ceil(WORKER_ROWS_RETENTION_DAYS / 7) + 2,
  );
  const expectedRows = deploymentCount * retentionWeeks;
  return Math.max(
    WORKER_ROWS_LIMIT_MIN,
    Math.min(WORKER_ROWS_LIMIT_MAX, expectedRows),
  );
}

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

function parseEntrySessionProfile(
  value: string | string[] | undefined,
) {
  return normalizeScalpEntrySessionProfile(firstQueryValue(value), "berlin");
}

function resolveDeploymentEntrySessionProfile(
  configOverride: unknown,
  explicitEntrySessionProfile?: unknown,
) {
  if (explicitEntrySessionProfile !== undefined) {
    return normalizeScalpEntrySessionProfile(explicitEntrySessionProfile, "berlin");
  }
  if (!configOverride || typeof configOverride !== "object") {
    return normalizeScalpEntrySessionProfile(undefined, "berlin");
  }
  const sessions =
    (configOverride as Record<string, unknown>).sessions &&
    typeof (configOverride as Record<string, unknown>).sessions === "object"
      ? ((configOverride as Record<string, unknown>)
          .sessions as Record<string, unknown>)
      : null;
  const raw = sessions ? sessions.entrySessionProfile : undefined;
  return normalizeScalpEntrySessionProfile(raw, "berlin");
}

function withSessionQuery(path: string, session: string): string {
  const value = String(path || "").trim();
  if (!value) return value;
  try {
    const isAbsolute = /^https?:\/\//i.test(value);
    const parsed = new URL(value, "http://localhost");
    parsed.searchParams.set("session", session);
    if (isAbsolute) return `${parsed.origin}${parsed.pathname}${parsed.search}`;
    return `${parsed.pathname}${parsed.search}`;
  } catch {
    const sep = value.includes("?") ? "&" : "?";
    return `${value}${sep}session=${encodeURIComponent(session)}`;
  }
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
  entrySessionProfile: string;
  range: SummaryRangeKey;
  journalLimit: number;
  tradeLimit: number;
}): string {
  return JSON.stringify({
    useDeployments: input.useDeployments,
    strategyId: input.requestedStrategyId || null,
    entrySessionProfile: input.entrySessionProfile,
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
    const entrySessionProfile = parseEntrySessionProfile(req.query.session);
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
      entrySessionProfile,
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
      entrySessionProfile,
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
    let jobs: Awaited<ReturnType<typeof loadScalpPipelineJobsHealth>> = [];
    let workerRows: Awaited<
      ReturnType<typeof listScalpDeploymentWeeklyMetricRows>
    > = [];
    let panicStop = {
      enabled: false,
      reason: null as string | null,
      updatedAtMs: null as number | null,
      updatedBy: null as string | null,
    };
    try {
      jobs = await loadScalpPipelineJobsHealth({ entrySessionProfile });
    } catch (err: any) {
      const rowError = {
        kind: "jobs_state",
        message: err?.message || String(err),
      };
      rowErrors.push(rowError);
      console.error(
        `[scalp-summary][${requestId}] jobs_state_error`,
        rowError,
        err?.stack || "",
      );
    }
    try {
      panicStop = await loadScalpPanicStopState();
    } catch (err: any) {
      const rowError = {
        kind: "panic_stop_state",
        message: err?.message || String(err),
      };
      rowErrors.push(rowError);
      console.error(
        `[scalp-summary][${requestId}] panic_stop_state_error`,
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
        const rawDeployments = await listScalpDeploymentRegistryEntries();
        allDeploymentRows = rawDeployments.filter(
          (row) =>
            resolveDeploymentEntrySessionProfile(
              row.configOverride,
              row.entrySessionProfile,
            ) ===
            entrySessionProfile,
        );
        deploymentRows = allDeploymentRows.filter((row) => row.enabled === true);
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
    const workerRowsFetchLimit = resolveWorkerRowsFetchLimit(
      useDeploymentsEffective ? allDeploymentRows.length : cronSymbols.length,
    );
    try {
      workerRows = await listScalpDeploymentWeeklyMetricRows({
        entrySessionProfile,
        limit: workerRowsFetchLimit,
      });
    } catch (err: any) {
      const rowError = {
        kind: "worker_rows_state",
        message: err?.message || String(err),
      };
      rowErrors.push(rowError);
      console.error(
        `[scalp-summary][${requestId}] worker_rows_state_error`,
        rowError,
        err?.stack || "",
      );
    }
    if (useDeploymentsEffective) {
      const allowedDeploymentIds = new Set(
        allDeploymentRows
          .map((row) => String(row.deploymentId || "").trim())
          .filter((row) => Boolean(row)),
      );
      workerRows = workerRows.filter((row) =>
        allowedDeploymentIds.has(String(row.deploymentId || "").trim()),
      );
    }
    logDebug("runtime_loaded", {
      defaultStrategyId: runtime.defaultStrategyId,
      runtimeStrategyCount: runtimeStrategies.length,
      cronSymbolCount: cronSymbolConfigs.length,
      deploymentRowCount: deploymentRows.length,
      useDeploymentsEffective,
      workerRowsFetched: workerRows.length,
      workerRowsFetchLimit,
      dayKey,
      clockMode: cfg.sessions.clockMode,
      entrySessionProfile,
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
            cronPath: withSessionQuery(
              cronSymbol?.path ||
                "/api/scalp/cron/execute-deployments?all=true",
              entrySessionProfile,
            ),
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
            cronPath: withSessionQuery(cronSymbol.path, entrySessionProfile),
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
      entrySessionProfile,
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
        entrySessionProfile: resolveDeploymentEntrySessionProfile(
          row.configOverride,
          row.entrySessionProfile,
        ),
        symbol: row.symbol,
        strategyId: row.strategyId,
        tuneId: row.tuneId,
        source: row.source,
        enabled: row.enabled,
        inUniverse:
          typeof row.inUniverse === "boolean" ? row.inUniverse : null,
        lifecycleState:
          typeof row.promotionGate?.lifecycle?.state === "string"
            ? row.promotionGate.lifecycle.state
            : null,
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
      jobs,
      workerRows,
      panicStop,
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
