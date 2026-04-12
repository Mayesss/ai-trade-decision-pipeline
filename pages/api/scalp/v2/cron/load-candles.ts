export const config = { runtime: "nodejs", maxDuration: 800 };

import type { NextApiRequest, NextApiResponse } from "next";

import { requireAdminAccess } from "../../../../../lib/admin";
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
    parseIntBounded(req.query.batchSize, 4, 1, 120),
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
  const selfHop = parseIntBounded(req.query.selfHop, 0, 0, 40);
  const maxSelfHopsCap = envIntBounded(
    "SCALP_V2_LOAD_CANDLES_MAX_SELF_HOPS",
    24,
    0,
    120,
  );
  const selfMaxHops = Math.min(
    parseIntBounded(req.query.selfMaxHops, 12, 0, 120),
    maxSelfHopsCap,
  );
  const runtime = await loadScalpV2RuntimeConfig();
  const scope = collectRuntimeScopes({
    supportedVenues: runtime.supportedVenues,
    seedSymbolsByVenue: runtime.seedSymbolsByVenue,
    seedLiveSymbolsByVenue: runtime.seedLiveSymbolsByVenue,
  });

  const result = await runScalpV2LoadCandlesPipelineJob({
    batchSize,
    maxAttempts,
    offset,
    scopes: scope,
  });
  const details = (result.details || {}) as Record<string, unknown>;
  const nextOffset = Math.max(
    0,
    Math.floor(Number(details.nextOffset) || offset + result.processed),
  );

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
      700,
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
        850,
      );
    } else {
      downstream = await invokeScalpV2CronEndpointDetached(
        req,
        "/api/scalp/v2/cron/cycle",
        {
          dryRun: successorDryRun ? 1 : 0,
          triggeredBy: "load-candles-v2",
        },
        850,
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
      maintenanceOnly: !autoSuccessor,
      downstream,
      selfRecall,
    },
  });
}
