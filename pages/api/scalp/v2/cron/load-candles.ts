export const config = { runtime: "nodejs", maxDuration: 800 };

import type { NextApiRequest, NextApiResponse } from "next";

import { requireAdminAccess } from "../../../../../lib/admin";
import {
  loadScalpCandleHistoryStatsBulk,
  type ScalpCandleHistoryStatsLoadResult,
} from "../../../../../lib/scalp/candleHistory";
import {
  invokeScalpV2CronEndpointDetached,
  type ScalpV2CronInvokeResult,
} from "../../../../../lib/scalp-v2/cronChaining";
import {
  clampScalpV2HardCap,
  resolveScalpV2ResearchHardCaps,
} from "../../../../../lib/scalp-v2/costControls";
import { loadScalpV2RuntimeConfig } from "../../../../../lib/scalp-v2/db";
import { runScalpV2LoadCandlesPipelineJob } from "../../../../../lib/scalp-v2/pipelineJobsAdapter";
import type { ScalpV2Venue } from "../../../../../lib/scalp-v2/types";

function firstQueryValue(
  value: string | string[] | undefined,
): string | undefined {
  if (typeof value === "string") return value.trim() || undefined;
  if (Array.isArray(value) && value.length > 0)
    return String(value[0] || "").trim() || undefined;
  return undefined;
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

function parseIntBounded(
  value: string | string[] | undefined,
  fallback: number,
  min: number,
  max: number,
): number {
  const first = firstQueryValue(value);
  const n = Number(first);
  if (!Number.isFinite(n)) return fallback;
  return Math.max(min, Math.min(max, Math.floor(n)));
}

function parseSuccessorMode(
  value: string | string[] | undefined,
): "cycle" | "discover" {
  const first = String(firstQueryValue(value) || "")
    .trim()
    .toLowerCase();
  return first === "discover" ? "discover" : "cycle";
}

function envIntBounded(
  name: string,
  fallback: number,
  min: number,
  max: number,
): number {
  const n = Math.floor(Number(process.env[name]));
  if (!Number.isFinite(n)) return fallback;
  return Math.max(min, Math.min(max, n));
}

function withTimeout<T>(
  task: Promise<T>,
  timeoutMs: number,
  label: string,
): Promise<T> {
  return new Promise<T>((resolve, reject) => {
    const timer = setTimeout(() => {
      reject(new Error(`${label}_timeout_${timeoutMs}ms`));
    }, timeoutMs);
    task.then(
      (value) => {
        clearTimeout(timer);
        resolve(value);
      },
      (error) => {
        clearTimeout(timer);
        reject(error);
      },
    );
  });
}

function normalizeSymbol(value: unknown): string {
  return String(value || "")
    .trim()
    .toUpperCase()
    .replace(/[^A-Z0-9._-]/g, "");
}

function collectRuntimeScopes(params: {
  supportedVenues: ScalpV2Venue[];
  seedSymbolsByVenue: Record<ScalpV2Venue, string[]>;
  seedLiveSymbolsByVenue: Record<ScalpV2Venue, string[]>;
}): Array<{ venue: ScalpV2Venue; symbol: string }> {
  const out = new Map<string, { venue: ScalpV2Venue; symbol: string }>();
  for (const venue of params.supportedVenues) {
    const seedSymbols = params.seedSymbolsByVenue[venue] || [];
    const liveSymbols = params.seedLiveSymbolsByVenue[venue] || [];
    for (const symbol of [...seedSymbols, ...liveSymbols]) {
      const normalized = normalizeSymbol(symbol);
      if (!normalized) continue;
      out.set(`${venue}:${normalized}`, { venue, symbol: normalized });
    }
  }
  return Array.from(out.values());
}

function setNoStoreHeaders(res: NextApiResponse): void {
  res.setHeader(
    "Cache-Control",
    "no-store, no-cache, must-revalidate, proxy-revalidate",
  );
  res.setHeader("Pragma", "no-cache");
  res.setHeader("Expires", "0");
}

function toTsMs(value: unknown): number | null {
  const n = Math.floor(Number(value));
  if (!Number.isFinite(n) || n <= 0) return null;
  return n;
}

function toIsoOrNull(value: number | null): string | null {
  if (!value) return null;
  return new Date(value).toISOString();
}

function statsBySymbol(
  rows: ScalpCandleHistoryStatsLoadResult[],
): Map<string, ScalpCandleHistoryStatsLoadResult> {
  const out = new Map<string, ScalpCandleHistoryStatsLoadResult>();
  for (const row of rows || []) {
    const symbol = normalizeSymbol(row.symbol);
    if (!symbol) continue;
    out.set(symbol, row);
  }
  return out;
}

function buildDebugScopeStats(params: {
  scopes: Array<{ venue: ScalpV2Venue; symbol: string }>;
  beforeRows: ScalpCandleHistoryStatsLoadResult[];
  afterRows: ScalpCandleHistoryStatsLoadResult[];
}) {
  const beforeBySymbol = statsBySymbol(params.beforeRows);
  const afterBySymbol = statsBySymbol(params.afterRows);
  return params.scopes.map((scope) => {
    const symbol = normalizeSymbol(scope.symbol);
    const before = beforeBySymbol.get(symbol);
    const after = afterBySymbol.get(symbol);
    const beforeCount = Math.max(0, Math.floor(Number(before?.candleCount || 0)));
    const afterCount = Math.max(0, Math.floor(Number(after?.candleCount || 0)));
    const beforeToTsMs = toTsMs(before?.toTsMs);
    const afterToTsMs = toTsMs(after?.toTsMs);
    return {
      venue: scope.venue,
      symbol,
      before: {
        candleCount: beforeCount,
        fromTsMs: toTsMs(before?.fromTsMs),
        fromIso: toIsoOrNull(toTsMs(before?.fromTsMs)),
        toTsMs: beforeToTsMs,
        toIso: toIsoOrNull(beforeToTsMs),
      },
      after: {
        candleCount: afterCount,
        fromTsMs: toTsMs(after?.fromTsMs),
        fromIso: toIsoOrNull(toTsMs(after?.fromTsMs)),
        toTsMs: afterToTsMs,
        toIso: toIsoOrNull(afterToTsMs),
      },
      deltaCandles: afterCount - beforeCount,
      advancedToTs: Boolean(
        Number.isFinite(Number(afterToTsMs)) &&
          Number.isFinite(Number(beforeToTsMs)) &&
          Number(afterToTsMs) > Number(beforeToTsMs),
      ),
    };
  });
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

  const hardCaps = resolveScalpV2ResearchHardCaps();
  const batchSize = clampScalpV2HardCap(
    parseIntBounded(req.query.batchSize, 1, 1, 120),
    hardCaps.maxBatchSizeLoad,
  );
  const maxAttempts = clampScalpV2HardCap(
    parseIntBounded(req.query.maxAttempts, 5, 1, 20),
    hardCaps.maxAttempts,
  );
  const offset = parseIntBounded(req.query.offset, 0, 0, 200_000);
  const autoSuccessor = parseBool(req.query.autoSuccessor, true);
  const autoContinue = parseBool(req.query.autoContinue, true);
  const successor = parseSuccessorMode(req.query.successor);
  const successorDryRun = parseBool(req.query.successorDryRun, false);
  const debug = parseBool(req.query.debug, false);
  const selfHop = parseIntBounded(req.query.selfHop, 0, 0, 40);
  const maxSelfHopsCap = envIntBounded(
    "SCALP_V2_LOAD_CANDLES_MAX_SELF_HOPS",
    80,
    0,
    120,
  );
  const routeTimeoutMs = envIntBounded(
    "SCALP_V2_LOAD_CANDLES_ROUTE_TIMEOUT_MS",
    7_500,
    1_000,
    120_000,
  );
  const selfRecallTimeoutMs = envIntBounded(
    "SCALP_V2_LOAD_CANDLES_SELF_RECALL_TIMEOUT_MS",
    1_500,
    300,
    20_000,
  );
  const downstreamInvokeTimeoutMs = envIntBounded(
    "SCALP_V2_LOAD_CANDLES_DOWNSTREAM_TIMEOUT_MS",
    1_500,
    300,
    20_000,
  );
  const selfMaxHops = Math.min(
    parseIntBounded(req.query.selfMaxHops, 40, 0, 120),
    maxSelfHopsCap,
  );
  const handlerStartedAtMs = Date.now();
  const runtime = await withTimeout(
    loadScalpV2RuntimeConfig(),
    Math.max(1_000, Math.min(20_000, Math.floor(routeTimeoutMs * 0.35))),
    "load_runtime_config",
  );
  const scope = collectRuntimeScopes({
    supportedVenues: runtime.supportedVenues,
    seedSymbolsByVenue: runtime.seedSymbolsByVenue,
    seedLiveSymbolsByVenue: runtime.seedLiveSymbolsByVenue,
  });
  const selectedScope = scope.slice(offset, offset + batchSize);
  const debugScopeSymbols = debug
    ? Array.from(
        new Set(
          selectedScope
            .map((row) => normalizeSymbol(row.symbol))
            .filter(Boolean),
        ),
      )
    : [];
  const debugStatsBefore =
    debugScopeSymbols.length > 0
      ? await loadScalpCandleHistoryStatsBulk(debugScopeSymbols, "1m").catch(
          () => [],
        )
      : [];
  const loadStartedAtMs = Date.now();

  let result: Awaited<ReturnType<typeof runScalpV2LoadCandlesPipelineJob>>;
  try {
    result = await withTimeout(
      runScalpV2LoadCandlesPipelineJob({
        batchSize,
        maxAttempts,
        offset,
        scopes: scope,
      }),
      routeTimeoutMs,
      "load_candles_job",
    );
  } catch (err: any) {
    return res.status(504).json({
      ok: false,
      busy: false,
      v2: true,
      error: "load_candles_timeout",
      message: err?.message || String(err),
      timeoutMs: routeTimeoutMs,
      elapsedMs: Date.now() - handlerStartedAtMs,
      chaining: {
        autoSuccessor,
        autoContinue,
        successor,
        successorDryRun,
        symbolScopeCount: scope.length,
        selfHop,
        selfMaxHops,
        offset,
        maxSelfHopsCap,
        selfRecallTimeoutMs,
        downstreamInvokeTimeoutMs,
      },
      ...(debug
        ? {
            debug: {
              enabled: true,
              selectedScope: selectedScope.map((row) => ({
                venue: row.venue,
                symbol: row.symbol,
              })),
              timing: {
                runtimeLoadElapsedMs: Math.max(
                  0,
                  loadStartedAtMs - handlerStartedAtMs,
                ),
                loadAttemptElapsedMs: Math.max(0, Date.now() - loadStartedAtMs),
              },
            },
          }
        : {}),
    });
  }
  const loadFinishedAtMs = Date.now();
  const details = (result.details || {}) as Record<string, unknown>;
  const nextOffset = Math.max(
    0,
    Math.floor(Number(details.nextOffset) || offset + result.processed),
  );
  const debugStatsAfter =
    debugScopeSymbols.length > 0
      ? await loadScalpCandleHistoryStatsBulk(debugScopeSymbols, "1m").catch(
          () => [],
        )
      : [];

  let downstream: ScalpV2CronInvokeResult | null = null;
  let selfRecall: ScalpV2CronInvokeResult | null = null;

  if (
    result.ok &&
    !result.busy &&
    autoContinue &&
    result.pendingAfter > 0 &&
    selfHop < selfMaxHops
  ) {
    selfRecall = await invokeScalpV2CronEndpointDetached(
      req,
      "/api/scalp/v2/cron/load-candles",
      {
        batchSize,
        maxAttempts,
        offset: nextOffset,
        autoContinue: 1,
        autoSuccessor: autoSuccessor ? 1 : 0,
        selfHop: selfHop + 1,
        selfMaxHops,
      },
      selfRecallTimeoutMs,
    );
  }

  if (
    result.ok &&
    !result.busy &&
    autoSuccessor &&
    result.pendingAfter <= 0
  ) {
    if (successor === "discover") {
      downstream = await invokeScalpV2CronEndpointDetached(
        req,
        "/api/scalp/v2/cron/discover",
        {
          autoSuccessor: 1,
          triggeredBy: "load-candles-v2",
        },
        downstreamInvokeTimeoutMs,
      );
    } else {
      downstream = await invokeScalpV2CronEndpointDetached(
        req,
        "/api/scalp/v2/cron/cycle",
        {
          dryRun: successorDryRun ? 1 : 0,
          triggeredBy: "load-candles-v2",
        },
        downstreamInvokeTimeoutMs,
      );
    }
  }

  return res.status(200).json({
    ok: result.ok,
    busy: result.busy,
    v2: true,
    job: result,
    chaining: {
      autoSuccessor,
      autoContinue,
      successor,
      successorDryRun,
      symbolScope: scope.map((row) => `${row.venue}:${row.symbol}`),
      selfHop,
      selfMaxHops,
      offset,
      nextOffset,
      maxSelfHopsCap,
      selfRecallTimeoutMs,
      downstreamInvokeTimeoutMs,
      maintenanceOnly: !autoSuccessor,
      downstream,
      selfRecall,
    },
    ...(debug
      ? {
          debug: {
            enabled: true,
            selectedScope: selectedScope.map((row) => ({
              venue: row.venue,
              symbol: row.symbol,
            })),
            timing: {
              runtimeLoadElapsedMs: Math.max(
                0,
                loadStartedAtMs - handlerStartedAtMs,
              ),
              loadJobElapsedMs: Math.max(0, loadFinishedAtMs - loadStartedAtMs),
              totalElapsedMs: Math.max(0, loadFinishedAtMs - handlerStartedAtMs),
            },
            scopeStats: buildDebugScopeStats({
              scopes: selectedScope,
              beforeRows: debugStatsBefore,
              afterRows: debugStatsAfter,
            }),
          },
        }
      : {}),
  });
}
