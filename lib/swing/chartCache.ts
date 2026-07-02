import { kvGetJson, kvSetJson } from '../kv';
import type { AnalysisPlatform } from '../platform';

// Chart candle cache. Candles are immutable once closed and the whole series only
// gains one bar per slice interval, so we cache the OHLC array per
// (symbol, platform, timeframe) in a *boundary-aligned bucket*: the key carries
// `floor(now / sliceMs)`, so a cached entry is served for the rest of the current
// bar and is naturally superseded when a new bar opens. TTL is one slice + a small
// grace so the entry comfortably outlives its bucket.
//
// Only candles live here — the chart endpoint always computes markers and position
// overlays live, since those change more often than a bar closes. The analyze cron
// warms this at :00 from candles it already fetched for its own indicators; chart
// loads also write-through on a miss.
const PREFIX = 'swing:chart:candles:v1';

export type ChartCandle = { time: number; open: number; high: number; low: number; close: number };

export function chartTimeframeToSeconds(tf: string): number {
  const match = /^(\d+)([a-zA-Z])$/.exec(String(tf).trim());
  if (!match) return 3600;
  const value = Number(match[1]);
  const unit = match[2];
  if (unit === 'm') return value * 60; // lowercase m = minutes
  if (unit === 'M') return value * 60 * 60 * 24 * 30; // uppercase M = month (approx)
  switch (unit.toLowerCase()) {
    case 's':
      return value;
    case 'h':
      return value * 60 * 60;
    case 'd':
      return value * 24 * 60 * 60;
    case 'w':
      return value * 7 * 24 * 60 * 60;
    default:
      return 3600;
  }
}

// Coerce a raw broker candle row (`[tsMs, open, high, low, close, ...]`, the shape
// both Bitget and Capital produce) into a sorted, finite ChartCandle[] with
// second-resolution timestamps — identical to what the chart endpoint returns.
export function normalizeChartCandles(raw: unknown): ChartCandle[] {
  return (Array.isArray(raw) ? raw : [])
    .map((c: any) => ({
      time: Math.floor(Number(c?.[0]) / 1000),
      open: Number(c?.[1]),
      high: Number(c?.[2]),
      low: Number(c?.[3]),
      close: Number(c?.[4]),
    }))
    .filter(
      (c) =>
        Number.isFinite(c.time) &&
        Number.isFinite(c.open) &&
        Number.isFinite(c.high) &&
        Number.isFinite(c.low) &&
        Number.isFinite(c.close),
    )
    .sort((a, b) => a.time - b.time);
}

function cacheKey(symbol: string, platform: AnalysisPlatform, timeframe: string, nowMs: number): string {
  const sliceSeconds = chartTimeframeToSeconds(timeframe);
  const bucket = Math.floor(nowMs / (sliceSeconds * 1000));
  return `${PREFIX}:${platform}:${symbol.toUpperCase()}:${timeframe}:${bucket}`;
}

// Read the cached candle series for the current bucket. Returns null on miss or
// KV error (caller falls back to a live fetch).
export async function readChartCandlesCache(params: {
  symbol: string;
  platform: AnalysisPlatform;
  timeframe: string;
  nowMs: number;
}): Promise<ChartCandle[] | null> {
  try {
    const cached = await kvGetJson<ChartCandle[]>(
      cacheKey(params.symbol, params.platform, params.timeframe, params.nowMs),
    );
    return Array.isArray(cached) ? cached : null;
  } catch {
    return null;
  }
}

// Chart range presets the UI renders (components/ChartPanel.tsx). The analyze cron
// already fetches 1H/4H/1D (×200) for its indicators, so those warm for free; 15m
// is the only preset analyze doesn't fetch and needs one extra request.
const CHART_WARM_PRESETS: Array<{ timeframe: string; minBars: number }> = [
  { timeframe: '15m', minBars: 96 },
  { timeframe: '1H', minBars: 168 },
  { timeframe: '4H', minBars: 180 },
  { timeframe: '1D', minBars: 183 },
];

// Warm the chart candle cache for one symbol, reusing the raw candle arrays the
// analyze run already fetched for its indicators (1H/4H/1D) and doing a single
// extra fetch for 15m. Best-effort — a preset with too few bars (would miss the
// reader's length check anyway) or a failed 15m fetch is skipped, never thrown.
export async function warmChartCandlesFromAnalyze(params: {
  symbol: string;
  platform: AnalysisPlatform;
  nowMs: number;
  rawCandlesByTf: Record<string, any[]> | undefined;
  fetch15m: () => Promise<any[]>;
}): Promise<void> {
  const writes: Array<Promise<void>> = [];
  for (const preset of CHART_WARM_PRESETS) {
    let raw: unknown;
    if (preset.timeframe === '15m') {
      try {
        raw = await params.fetch15m();
      } catch (err) {
        console.warn(`chart warm 15m fetch failed for ${params.symbol}:`, err);
        continue;
      }
    } else {
      raw = params.rawCandlesByTf?.[preset.timeframe];
    }
    const candles = normalizeChartCandles(raw);
    // Skip short series: the reader only accepts a cache entry with >= requested
    // bars, so caching fewer would never hit — leave it to a live load.
    if (candles.length < preset.minBars) continue;
    writes.push(
      writeChartCandlesCache({
        symbol: params.symbol,
        platform: params.platform,
        timeframe: preset.timeframe,
        nowMs: params.nowMs,
        candles,
      }),
    );
  }
  await Promise.all(writes);
}

// Write a candle series into the current bucket. Best-effort — never throws.
export async function writeChartCandlesCache(params: {
  symbol: string;
  platform: AnalysisPlatform;
  timeframe: string;
  nowMs: number;
  candles: ChartCandle[];
}): Promise<void> {
  if (!params.candles.length) return;
  try {
    const ttlSeconds = chartTimeframeToSeconds(params.timeframe) + 300;
    await kvSetJson(
      cacheKey(params.symbol, params.platform, params.timeframe, params.nowMs),
      params.candles,
      ttlSeconds,
    );
  } catch (err) {
    console.warn(`chart candle cache write failed for ${params.symbol} ${params.timeframe}:`, err);
  }
}
