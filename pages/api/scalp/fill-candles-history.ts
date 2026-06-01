export const config = { runtime: "nodejs" };

import type { NextApiRequest, NextApiResponse } from "next";

import { requireAdminAccess } from "../../../lib/admin";
import { fetchBitgetCandlesByEpicDateRange } from "../../../lib/scalp/bitgetHistory";
import {
  type CandleHistoryBackend,
  loadScalpCandleHistoryStatsBulk,
  normalizeHistoryTimeframe,
  saveScalpCandleHistory,
  timeframeToMs,
} from "../../../lib/scalp/candleHistory";
import { ensureScalpSymbolMarketMetadata } from "../../../lib/scalp/symbolMarketMetadataSync";
import type { ScalpCandle } from "../../../lib/scalp/types";

const ONE_DAY_MS = 24 * 60 * 60 * 1000;

function parseBool(
  value: string | string[] | undefined,
  fallback: boolean,
): boolean {
  if (value === undefined) return fallback;
  const first = Array.isArray(value) ? value[0] : value;
  if (first === undefined) return fallback;
  const normalized = String(first).trim().toLowerCase();
  if (["false", "0", "no", "off"].includes(normalized)) return false;
  if (["true", "1", "yes", "on"].includes(normalized)) return true;
  return fallback;
}

function firstQueryValue(
  value: string | string[] | undefined,
): string | undefined {
  if (typeof value === "string") return value.trim() || undefined;
  if (Array.isArray(value) && value.length > 0) {
    return String(value[0] || "").trim() || undefined;
  }
  return undefined;
}

function parseNowMs(value: string | undefined): number | undefined {
  const n = Number(value);
  if (Number.isFinite(n) && n > 0) return Math.floor(n);
  return undefined;
}

function toPositiveInt(value: string | undefined, fallback: number): number {
  const n = Number(value);
  if (!Number.isFinite(n) || n <= 0) return fallback;
  return Math.max(1, Math.floor(n));
}

function parseDirection(value: string | undefined): "backfill" | "forward" {
  const normalized = String(value || "")
    .trim()
    .toLowerCase();
  if (normalized === "forward") return "forward";
  return "backfill";
}

function parseBackend(
  value: string | undefined,
): CandleHistoryBackend | undefined {
  const normalized = String(value || "")
    .trim()
    .toLowerCase();
  if (normalized === "pg" || normalized === "file" || normalized === "kv")
    return "pg";
  return undefined;
}

function normalizeSymbol(value: string | undefined): string {
  return String(value || "")
    .trim()
    .toUpperCase()
    .replace(/[^A-Z0-9._-]/g, "");
}

function normalizeFetchedCandles(rows: any[]): ScalpCandle[] {
  return rows
    .map((row) => {
      const ts = Number(row?.[0]);
      const open = Number(row?.[1]);
      const high = Number(row?.[2]);
      const low = Number(row?.[3]);
      const close = Number(row?.[4]);
      const volume = Number(row?.[5] ?? 0);
      if (
        ![ts, open, high, low, close].every((v) => Number.isFinite(v) && v > 0)
      )
        return null;
      return [
        Math.floor(ts),
        open,
        high,
        low,
        close,
        Number.isFinite(volume) ? volume : 0,
      ] as ScalpCandle;
    })
    .filter((row): row is ScalpCandle => Boolean(row))
    .sort((a, b) => a[0] - b[0]);
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

  try {
    const symbol = normalizeSymbol(firstQueryValue(req.query.symbol));
    if (!symbol) {
      return res
        .status(400)
        .json({
          error: "symbol_required",
          message: "Provide symbol (e.g. EURUSD).",
        });
    }

    const timeframe = normalizeHistoryTimeframe(
      firstQueryValue(req.query.timeframe) || "15m",
    );
    const direction = parseDirection(firstQueryValue(req.query.direction));
    const days = Math.max(
      1,
      Math.min(120, toPositiveInt(firstQueryValue(req.query.days), 30)),
    );
    const dryRun = parseBool(req.query.dryRun, true);
    const nowMs = parseNowMs(firstQueryValue(req.query.nowMs)) ?? Date.now();
    const backend = parseBackend(firstQueryValue(req.query.backend));

    const [beforeStats] = await loadScalpCandleHistoryStatsBulk([symbol], timeframe, {
      backend,
    });
    const existingCount = Math.max(0, Math.floor(Number(beforeStats?.candleCount || 0)));
    const existingFromTsMs = Number.isFinite(Number(beforeStats?.fromTsMs))
      ? Number(beforeStats?.fromTsMs)
      : null;
    const existingToTsMs = Number.isFinite(Number(beforeStats?.toTsMs))
      ? Number(beforeStats?.toTsMs)
      : null;
    const tfMs = timeframeToMs(timeframe);
    const maxRequests = Math.max(
      20,
      Math.min(240, toPositiveInt(firstQueryValue(req.query.maxRequests), 120)),
    );

    const baseAnchorMs =
      direction === "backfill"
        ? (existingFromTsMs ?? nowMs)
        : (existingToTsMs ?? nowMs - days * ONE_DAY_MS);

    let fetchFromMs =
      direction === "backfill"
        ? baseAnchorMs - days * ONE_DAY_MS
        : baseAnchorMs + tfMs;
    let fetchToMs =
      direction === "backfill"
        ? baseAnchorMs - tfMs
        : baseAnchorMs + days * ONE_DAY_MS;
    const clampedToNow = direction === "forward" && fetchToMs > nowMs;
    if (direction === "forward") fetchToMs = Math.min(fetchToMs, nowMs);
    fetchFromMs = Math.max(0, Math.floor(fetchFromMs));
    fetchToMs = Math.max(0, Math.floor(fetchToMs));

    const marketMetadata = await ensureScalpSymbolMarketMetadata(symbol, {
      fetchIfMissing: true,
      venue: "bitget",
    });
    const epicResolved = {
      epic: marketMetadata?.epic || symbol,
      source: marketMetadata?.epic ? ("metadata" as const) : ("symbol" as const),
    };

    if (!(fetchToMs > fetchFromMs)) {
      return res.status(200).json({
        ok: true,
        symbol,
        epic: epicResolved.epic,
        timeframe,
        direction,
        dryRun,
        days,
        backend: "pg",
        storageRef: `scalp_candle_history_weeks:${symbol}:${timeframe}`,
        existingCount,
        fetchedCount: 0,
        mergedCount: existingCount,
        addedCount: 0,
        clampedToNow,
        message: "No fetch needed for requested window.",
      });
    }

    const fetchedRaw = await fetchBitgetCandlesByEpicDateRange(
      epicResolved.epic,
      timeframe,
      fetchFromMs,
      fetchToMs,
      {
        maxPerRequest: 200,
        maxRequests,
      },
    );
    const fetched = normalizeFetchedCandles(fetchedRaw);
    const addedCount = fetched.length;

    let saveResult: {
      backend: string;
      storageRef: string;
      saved: boolean;
    } | null = null;
    if (!dryRun && fetched.length > 0) {
      saveResult = await saveScalpCandleHistory(
        {
          symbol,
          timeframe,
          epic: epicResolved.epic,
          source: "bitget",
          candles: fetched,
        },
        { backend },
      );
    }
    const [afterStats] =
      !dryRun && fetched.length > 0
        ? await loadScalpCandleHistoryStatsBulk([symbol], timeframe, { backend })
        : [beforeStats];
    const afterCount = Math.max(existingCount, Math.floor(Number(afterStats?.candleCount || existingCount)));
    const afterFromTsMs = Number.isFinite(Number(afterStats?.fromTsMs))
      ? Number(afterStats?.fromTsMs)
      : (fetched[0]?.[0] ?? existingFromTsMs);
    const afterToTsMs = Number.isFinite(Number(afterStats?.toTsMs))
      ? Number(afterStats?.toTsMs)
      : (fetched[fetched.length - 1]?.[0] ?? existingToTsMs);
    const dryRunFromCandidates = [existingFromTsMs, fetched[0]?.[0] ?? null].filter(
      (value): value is number => Number.isFinite(Number(value)),
    );
    const dryRunToCandidates = [existingToTsMs, fetched[fetched.length - 1]?.[0] ?? null].filter(
      (value): value is number => Number.isFinite(Number(value)),
    );

    return res.status(200).json({
      ok: true,
      symbol,
      epic: epicResolved.epic,
      timeframe,
      direction,
      dryRun,
      backendRequested: backend || "auto",
      days,
      fetchFromMs,
      fetchToMs,
      clampedToNow,
      backend: saveResult?.backend || "pg",
      storageRef:
        saveResult?.storageRef ||
        `scalp_candle_history_weeks:${symbol}:${timeframe}`,
      existingCount,
      fetchedCount: fetched.length,
      mergedCount: dryRun ? existingCount + addedCount : afterCount,
      addedCount,
      saved: saveResult?.saved ?? false,
      coverage: {
        before: {
          fromTsMs: existingFromTsMs,
          toTsMs: existingToTsMs,
        },
        after: {
          fromTsMs: dryRun
            ? (dryRunFromCandidates.length ? Math.min(...dryRunFromCandidates) : null)
            : afterFromTsMs,
          toTsMs: dryRun
            ? (dryRunToCandidates.length ? Math.max(...dryRunToCandidates) : null)
            : afterToTsMs,
        },
      },
    });
  } catch (err: any) {
    return res.status(500).json({
      error: "fill_candles_history_failed",
      message: err?.message || String(err),
    });
  }
}
