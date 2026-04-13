export const config = { runtime: "nodejs", maxDuration: 120 };

import type { NextApiRequest, NextApiResponse } from "next";

import { requireAdminAccess } from "../../../../../lib/admin";
import {
  invokeScalpV2CronEndpointDetached,
  type ScalpV2CronInvokeResult,
} from "../../../../../lib/scalp-v2/cronChaining";
import {
  resolveScalpV2WorkerWeeklyCacheBackfillOptions,
  runScalpV2WorkerWeeklyCacheBackfillChunk,
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

function toNumberValueBounded(
  value: unknown,
  fallback: number,
  min: number,
  max: number,
): number {
  const n = Math.floor(Number(value));
  if (!Number.isFinite(n)) return fallback;
  return Math.max(min, Math.min(max, n));
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
    const batchSize = toNumberValueBounded(readValue(input, "batchSize"), 12, 1, 200);
    const offset = toNumberValueBounded(readValue(input, "offset"), 0, 0, 1_000_000);
    const autoContinue = toBoolValue(readValue(input, "autoContinue"), true);
    const selfHop = toNumberValueBounded(readValue(input, "selfHop"), 0, 0, 60);
    const selfMaxHops = toNumberValueBounded(
      readValue(input, "selfMaxHops"),
      15,
      0,
      120,
    );
    const explicitLimit = toNumberValue(readValue(input, "limit"));

    const opts = resolveScalpV2WorkerWeeklyCacheBackfillOptions({
      dryRun,
      verbose: toBoolValue(readValue(input, "verbose"), false),
      limit: explicitLimit ?? batchSize,
      offset,
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
    const chunk = await runScalpV2WorkerWeeklyCacheBackfillChunk(opts);
    let selfRecall: ScalpV2CronInvokeResult | null = null;

    if (
      autoContinue &&
      chunk.hasMore &&
      selfHop < selfMaxHops
    ) {
      selfRecall = await invokeScalpV2CronEndpointDetached(
        req,
        "/api/scalp/v2/ops/backfill-worker-weekly-cache",
        {
          apply: apply ? 1 : 0,
          verbose: opts.verbose ? 1 : 0,
          batchSize: opts.limit,
          offset: chunk.nextOffset,
          autoContinue: 1,
          selfHop: selfHop + 1,
          selfMaxHops,
          venue: opts.venue,
          session: opts.session,
          statuses: Array.from(opts.statuses).join(","),
          symbols: Array.from(opts.symbolFilter).join(","),
          windowToTs: opts.windowToTs,
          stageAWeeks: opts.stageAWeeks,
          stageBWeeks: opts.stageBWeeks,
          stageCWeeks: opts.stageCWeeks,
          minCandles: opts.minCandles,
          upsertBatchSize: opts.upsertBatchSize,
          cacheVersion: opts.cacheVersion,
        },
        900,
      );
    }

    return res.status(200).json({
      ok: true,
      mode: "scalp_v2",
      temporary: true,
      operation: "backfill_worker_weekly_cache",
      dryRun: opts.dryRun,
      job: {
        ok: true,
        busy: false,
        jobKind: "backfill_worker_weekly_cache",
        processed: chunk.stats.selectedCandidates,
        pendingAfter: chunk.pendingAfter,
        details: {
          offset: chunk.offset,
          nextOffset: chunk.nextOffset,
          hasMore: chunk.hasMore,
          totalMatched: chunk.totalMatched,
          batchSize: opts.limit,
        },
      },
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
      stats: chunk.stats,
      progress: {
        offset: chunk.offset,
        nextOffset: chunk.nextOffset,
        pendingAfter: chunk.pendingAfter,
        hasMore: chunk.hasMore,
        totalMatched: chunk.totalMatched,
      },
      chaining: {
        autoContinue,
        selfHop,
        selfMaxHops,
        selfRecall,
      },
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
