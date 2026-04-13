export const config = { runtime: "nodejs" };

import type { NextApiRequest, NextApiResponse } from "next";

import { requireAdminAccess } from "../../../../../lib/admin";
import {
  resolveScalpV2WorkerWeeklyCacheBackfillOptions,
  runScalpV2WorkerWeeklyCacheBackfill,
} from "../../../../../lib/scalp-v2/backfillWorkerWeeklyCache";
import { setNoStoreHeaders } from "../../../../../lib/scalp-v2/http";

function asRecord(value: unknown): Record<string, unknown> {
  if (!value || typeof value !== "object" || Array.isArray(value)) return {};
  return value as Record<string, unknown>;
}

function readValue(input: NextApiRequest["query"] | Record<string, unknown>, key: string): unknown {
  if (Array.isArray((input as any)[key])) return (input as any)[key][0];
  return (input as any)[key];
}

function toStringValue(value: unknown): string | undefined {
  const out = String(value || "").trim();
  return out || undefined;
}

function toNumberValue(value: unknown): number | undefined {
  const n = Number(value);
  return Number.isFinite(n) ? n : undefined;
}

function toBoolValue(value: unknown, fallback: boolean): boolean {
  const normalized = String(value || "").trim().toLowerCase();
  if (!normalized) return fallback;
  if (["1", "true", "yes", "on"].includes(normalized)) return true;
  if (["0", "false", "no", "off"].includes(normalized)) return false;
  return fallback;
}

export default async function handler(
  req: NextApiRequest,
  res: NextApiResponse,
) {
  if (!requireAdminAccess(req, res)) return;
  setNoStoreHeaders(res);

  if (req.method !== "GET" && req.method !== "POST") {
    return res.status(405).json({
      error: "method_not_allowed",
      message: "Use GET (dry-run by default) or POST",
    });
  }

  try {
    const input = req.method === "GET" ? req.query : asRecord(req.body);
    const apply = toBoolValue(readValue(input, "apply"), req.method === "POST");
    const dryRun = !apply;

    const opts = resolveScalpV2WorkerWeeklyCacheBackfillOptions({
      dryRun,
      verbose: toBoolValue(readValue(input, "verbose"), false),
      limit: toNumberValue(readValue(input, "limit")),
      offset: toNumberValue(readValue(input, "offset")),
      venue: toStringValue(readValue(input, "venue")) as any,
      session: toStringValue(readValue(input, "session")) as any,
      statuses: toStringValue(readValue(input, "statuses")),
      symbols: toStringValue(readValue(input, "symbols")),
      windowToTs: toNumberValue(readValue(input, "windowToTs")),
      stageAWeeks: toNumberValue(readValue(input, "stageAWeeks")),
      stageBWeeks: toNumberValue(readValue(input, "stageBWeeks")),
      stageCWeeks: toNumberValue(readValue(input, "stageCWeeks")),
      minCandles: toNumberValue(readValue(input, "minCandles")),
      upsertBatchSize: toNumberValue(readValue(input, "upsertBatchSize")),
      cacheVersion: toStringValue(readValue(input, "cacheVersion")),
    });

    const startedAtMs = Date.now();
    const stats = await runScalpV2WorkerWeeklyCacheBackfill(opts);

    return res.status(200).json({
      ok: true,
      mode: "scalp_v2",
      temporary: true,
      operation: "backfill_worker_weekly_cache",
      dryRun: opts.dryRun,
      opts: {
        limit: opts.limit,
        offset: opts.offset,
        venue: opts.venue,
        session: opts.session,
        statuses: Array.from(opts.statuses),
        symbols: Array.from(opts.symbolFilter),
        windowToTs: opts.windowToTs,
        windowToIso: new Date(opts.windowToTs).toISOString(),
        stageWeeks: {
          a: opts.stageAWeeks,
          b: opts.stageBWeeks,
          c: opts.stageCWeeks,
        },
        minCandles: opts.minCandles,
        cacheVersion: opts.cacheVersion,
        upsertBatchSize: opts.upsertBatchSize,
      },
      stats,
      elapsedMs: Date.now() - startedAtMs,
    });
  } catch (err: any) {
    return res.status(500).json({
      ok: false,
      error: "scalp_v2_backfill_worker_weekly_cache_failed",
      message: err?.message || String(err),
    });
  }
}
