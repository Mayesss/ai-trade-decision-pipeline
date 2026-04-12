import { bitgetFetch, resolveProductType } from "../bitget";
import { timeframeToMs } from "./candleHistory";
import type { ScalpCandle } from "./types";

const ONE_MINUTE_MS = 60_000;
const BITGET_HISTORY_CANDLES_MAX_LIMIT = 200;
const BITGET_HISTORY_MAX_REQUESTS_HARD_CAP = 5000;

function normalizeSymbol(value: unknown): string {
  return String(value || "")
    .trim()
    .toUpperCase()
    .replace(/[^A-Z0-9._-]/g, "");
}

export function normalizeBitgetHistoryGranularity(timeframe: string): string {
  const normalized = String(timeframe || "").trim();
  const lower = normalized.toLowerCase();
  if (lower === "1m") return "1m";
  if (lower === "3m") return "3m";
  if (lower === "5m") return "5m";
  if (lower === "15m") return "15m";
  if (lower === "30m") return "30m";
  if (lower === "1h") return "1H";
  if (lower === "2h") return "2H";
  if (lower === "4h") return "4H";
  if (lower === "6h") return "6H";
  if (lower === "12h") return "12H";
  if (lower === "1d") return "1D";
  if (lower === "1w" || lower === "4d") return "1W";
  if (lower === "1mo" || lower === "1mth" || lower === "1month") return "1M";
  return normalized || "1m";
}

export function normalizeFetchedBitgetCandles(rows: unknown[]): ScalpCandle[] {
  return rows
    .map((row) => {
      const value = Array.isArray(row) ? row : [];
      const ts = Number(value[0]);
      const open = Number(value[1]);
      const high = Number(value[2]);
      const low = Number(value[3]);
      const close = Number(value[4]);
      const volume = Number(value[5] ?? 0);
      if (
        ![ts, open, high, low, close].every((v) => Number.isFinite(v) && v > 0)
      ) {
        return null;
      }
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

export async function fetchBitgetCandlesByEpic(
  epic: string,
  timeframe: string,
  limit: number,
): Promise<ScalpCandle[]> {
  const symbol = normalizeSymbol(epic);
  if (!symbol) return [];
  const granularity = normalizeBitgetHistoryGranularity(timeframe);
  const boundedLimit = Math.max(
    1,
    Math.min(1000, Math.floor(Number(limit) || 200)),
  );
  const productType = String(resolveProductType() || "usdt-futures")
    .trim()
    .toUpperCase();
  const rows = await bitgetFetch("GET", "/api/v2/mix/market/candles", {
    symbol,
    productType,
    granularity,
    limit: boundedLimit,
  });
  return Array.isArray(rows) ? normalizeFetchedBitgetCandles(rows) : [];
}

export async function fetchBitgetCandlesByEpicDateRange(
  epic: string,
  timeframe: string,
  fromTsMs: number,
  toTsMs: number,
  opts: {
    maxPerRequest?: number;
    maxRequests?: number;
  } = {},
): Promise<ScalpCandle[]> {
  const symbol = normalizeSymbol(epic);
  if (!symbol) return [];

  const startMs = Math.floor(Math.min(fromTsMs, toTsMs));
  const endMs = Math.floor(Math.max(fromTsMs, toTsMs));
  if (
    !(Number.isFinite(startMs) && Number.isFinite(endMs) && endMs > startMs)
  ) {
    return [];
  }

  const granularity = normalizeBitgetHistoryGranularity(timeframe);
  const timeframeMs = Math.max(ONE_MINUTE_MS, timeframeToMs(timeframe));
  const requestLimit = Math.max(
    20,
    Math.min(
      BITGET_HISTORY_CANDLES_MAX_LIMIT,
      Math.floor(opts.maxPerRequest ?? BITGET_HISTORY_CANDLES_MAX_LIMIT),
    ),
  );
  const configuredMaxRequests = Math.max(
    40,
    Math.floor(opts.maxRequests ?? 1200),
  );
  const requestedBars = Math.max(
    1,
    Math.floor((endMs - startMs) / timeframeMs) + 1,
  );
  const minimumRequestsForRange = Math.max(
    1,
    Math.ceil(requestedBars / Math.max(1, requestLimit)),
  );
  const adaptiveMaxRequests = Math.max(
    configuredMaxRequests,
    Math.ceil(minimumRequestsForRange * 1.35),
  );
  const maxRequests = Math.max(
    40,
    Math.min(BITGET_HISTORY_MAX_REQUESTS_HARD_CAP, adaptiveMaxRequests),
  );
  const requestSpanMs = requestLimit * timeframeMs;
  const productType = String(resolveProductType() || "usdt-futures")
    .trim()
    .toUpperCase();

  const candlesByTs = new Map<number, ScalpCandle>();
  let cursorEnd = endMs;
  let requests = 0;
  while (cursorEnd >= startMs) {
    if (requests >= maxRequests) {
      throw new Error(
        `bitget_history_max_requests_reached_for_${symbol}:max=${maxRequests}:requestedBars=${requestedBars}:limit=${requestLimit}`,
      );
    }
    const alignedEndTime = cursorEnd - (cursorEnd % timeframeMs);
    const endTime = Math.max(startMs, alignedEndTime);
    let rows: unknown;
    try {
      rows = await bitgetFetch(
        "GET",
        "/api/v2/mix/market/history-candles",
        {
          symbol,
          productType,
          granularity,
          limit: requestLimit,
          endTime,
        },
      );
    } catch (err: any) {
      const msg = err?.message || String(err);
      throw new Error(
        `bitget_history_request_failed:${symbol}:granularity=${granularity}:endTime=${endTime}:limit=${requestLimit}:${msg}`,
      );
    }
    requests += 1;

    const parsedRows = Array.isArray(rows)
      ? normalizeFetchedBitgetCandles(rows).filter(
          (row) => row[0] >= startMs && row[0] <= endMs,
        )
      : [];
    if (!parsedRows.length) {
      if (endTime <= startMs) break;
      cursorEnd = endTime - requestSpanMs;
      continue;
    }

    let oldestTs = Number.POSITIVE_INFINITY;
    for (const candle of parsedRows) {
      candlesByTs.set(candle[0], candle);
      if (candle[0] < oldestTs) oldestTs = candle[0];
    }
    if (!Number.isFinite(oldestTs)) break;
    if (oldestTs >= cursorEnd) {
      cursorEnd -= requestSpanMs;
    } else {
      cursorEnd = oldestTs - 1;
    }
  }

  return Array.from(candlesByTs.values()).sort((a, b) => a[0] - b[0]);
}
