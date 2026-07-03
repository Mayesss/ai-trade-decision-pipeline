import { kvGetJson, kvSetJson } from '../kv';
import type { AnalysisPlatform } from '../platform';

// Chart position-overlay cache. The chart endpoint builds its position overlays
// from closed positions (Neon `swing.positions`) plus a live broker
// `fetchPositionInfo` call — both run on every chart load, even when candles are
// warm. This caches the *computed* overlay array per (symbol, platform, range) for
// a short window, so repeat loads skip the Neon read (cutting Neon data transfer)
// and the broker round-trip. Aligned with the client's ~60s chart cache; a
// just-opened/closed position may lag by up to the TTL, which is fine for a chart.
const PREFIX = 'swing:chart:overlay:v1';
const TTL_SECONDS = 60;

// Kept as an opaque array so the endpoint stays the single source of truth for the
// overlay shape — this module only stores and returns whatever it computed.
export type ChartPositionOverlay = Record<string, unknown>;

function cacheKey(
  symbol: string,
  platform: AnalysisPlatform,
  timeframe: string,
  limit: number,
): string {
  return `${PREFIX}:${platform}:${symbol.toUpperCase()}:${timeframe}:${limit}`;
}

// Returns the cached overlay array, or null on miss/error (caller computes live).
export async function readPositionOverlayCache(params: {
  symbol: string;
  platform: AnalysisPlatform;
  timeframe: string;
  limit: number;
}): Promise<ChartPositionOverlay[] | null> {
  try {
    const cached = await kvGetJson<ChartPositionOverlay[]>(
      cacheKey(params.symbol, params.platform, params.timeframe, params.limit),
    );
    return Array.isArray(cached) ? cached : null;
  } catch {
    return null;
  }
}

// Write the computed overlay array. Best-effort — never throws. Empty arrays are
// cached too, so symbols with no positions don't re-hit Neon/the broker each load.
export async function writePositionOverlayCache(params: {
  symbol: string;
  platform: AnalysisPlatform;
  timeframe: string;
  limit: number;
  overlay: ChartPositionOverlay[];
}): Promise<void> {
  try {
    await kvSetJson(
      cacheKey(params.symbol, params.platform, params.timeframe, params.limit),
      params.overlay,
      TTL_SECONDS,
    );
  } catch (err) {
    console.warn(`chart overlay cache write failed for ${params.symbol}:`, err);
  }
}
