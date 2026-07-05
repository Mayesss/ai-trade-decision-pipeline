import { kvGetJson, kvSetJson } from '../kv';
import { extractCapturedLeverages, loadSymbolMarkerHistory } from '../history';
import type { AnalysisPlatform } from '../platform';
import { loadClosedSwingPositions } from './pg';
import { chartTimeframeToSeconds } from './chartCache';

// Chart position-overlay cache. The chart endpoint builds its position overlays
// from closed positions (Neon `swing.positions`) plus a live broker
// `fetchPositionInfo` call — both run on every chart load, even when candles are
// warm. This caches the *computed* overlay array per (symbol, platform, range) for
// a short window, so repeat loads skip the Neon read (cutting Neon data transfer)
// and the broker round-trip. Aligned with the client's ~60s chart cache; a
// just-opened/closed position may lag by up to the TTL, which is fine for a chart.
const PREFIX = 'swing:chart:overlay:v3';
const TTL_SECONDS = (() => {
  const raw = Number(process.env.SWING_CHART_OVERLAY_CACHE_TTL_SECONDS);
  return Number.isFinite(raw) && raw >= 0 ? Math.floor(raw) : 65 * 60;
})();

// Kept as an opaque array so the endpoint stays the single source of truth for the
// overlay shape — this module only stores and returns whatever it computed.
export type ChartPositionOverlay = Record<string, unknown>;

const BTC_SYMBOL = 'BTCUSDT';
const BTC_CHART_LEVERAGE_OVERRIDE = 3;

const CHART_OVERLAY_WARM_PRESETS: Array<{ timeframe: string; limit: number }> = [
  { timeframe: '15m', limit: 96 },
  { timeframe: '1H', limit: 168 },
  { timeframe: '4H', limit: 180 },
  { timeframe: '1D', limit: 183 },
];

type OpenPositionInfo = {
  status?: string | null;
  holdSide?: 'long' | 'short' | string | null;
  entryTimestamp?: number | null;
  currentPnl?: string | number | null;
  entryPrice?: string | number | null;
  leverage?: number | null;
};

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

function finiteNumber(value: unknown): number | null {
  if (value === null || value === undefined || value === '') return null;
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

function positiveNumber(value: unknown): number | null {
  const n = finiteNumber(value);
  return n !== null && n > 0 ? n : null;
}

function parsePnl(value: unknown): number | null {
  const raw = typeof value === 'string' ? value.replace('%', '') : value;
  return finiteNumber(raw);
}

function findNearestDecision(history: any[], tsMs?: number | null) {
  if (!tsMs || !history?.length) return null;
  let best: any = null;
  let bestDiff = Number.POSITIVE_INFINITY;
  for (const h of history) {
    if (!h.timestamp) continue;
    const diff = Math.abs(Number(h.timestamp) - tsMs);
    if (diff < bestDiff) {
      bestDiff = diff;
      best = h;
    }
  }
  if (!best) return null;
  return {
    timestamp: Number(best.timestamp) || null,
    action: best.aiDecision?.action,
    summary: best.aiDecision?.summary,
    reason: best.aiDecision?.reason,
  };
}

function getPartialClosePct(entry: any): number | null {
  const pct =
    finiteNumber(entry?.execResult?.partialClosePct) ??
    finiteNumber(entry?.aiDecision?.exit_size_pct) ??
    finiteNumber(entry?.aiDecision?.close_size_pct) ??
    finiteNumber(entry?.aiDecision?.partial_close_pct);
  return pct !== null && pct > 0 && pct < 100 ? pct : null;
}

function buildPartialCloses(params: {
  history: any[];
  entryTsMs?: number | null;
  exitTsMs?: number | null;
  fromMs: number;
  nowMs: number;
}) {
  const fromMs = finiteNumber(params.entryTsMs) ?? params.fromMs;
  const toMs = finiteNumber(params.exitTsMs) ?? params.nowMs;
  return (params.history || [])
    .filter((h) => {
      const ts = finiteNumber(h?.timestamp);
      if (ts === null || ts < fromMs || ts > toMs) return false;
      if (String(h?.aiDecision?.action || '').toUpperCase() !== 'CLOSE') return false;
      if (h?.execResult?.placed !== true || h?.execResult?.closed !== true || h?.execResult?.partial !== true) {
        return false;
      }
      return getPartialClosePct(h) !== null;
    })
    .map((h) => ({
      timestamp: finiteNumber(h.timestamp),
      action: h.aiDecision?.action,
      summary: h.aiDecision?.summary,
      reason: h.aiDecision?.reason,
      closePct: getPartialClosePct(h),
      size: finiteNumber(h.execResult?.size),
    }))
    .sort((a, b) => Number(a.timestamp || 0) - Number(b.timestamp || 0));
}

function normalizeOverlayPositions(params: {
  symbol: string;
  platform: AnalysisPlatform;
  closed: any[];
  openPositionInfo?: OpenPositionInfo | null;
  history: any[];
  leverageFromHistory: number | null;
  fromMs: number;
  nowMs: number;
}): ChartPositionOverlay[] {
  const closedNormalized =
    params.platform === 'bitget' && params.symbol.toUpperCase() === BTC_SYMBOL
      ? params.closed.map((position) => {
          const rawLev = Number(position.leverage);
          const fallbackLev = Number(params.leverageFromHistory);
          const detectedLeverage =
            Number.isFinite(rawLev) && rawLev > 0
              ? rawLev
              : Number.isFinite(fallbackLev) && fallbackLev > 0
              ? fallbackLev
              : 1;
          const scale = BTC_CHART_LEVERAGE_OVERRIDE / detectedLeverage;
          const rawPnlPct = Number(position.pnlPct);
          return {
            ...position,
            pnlPct: Number.isFinite(rawPnlPct) ? rawPnlPct * scale : position.pnlPct,
            leverage: BTC_CHART_LEVERAGE_OVERRIDE,
          };
        })
      : params.closed;

  let openOverlay: any = null;
  const open = params.openPositionInfo;
  if (open?.status === 'open') {
    openOverlay = {
      id: `${params.platform}:${params.symbol}-open-position`,
      symbol: params.symbol,
      side: open.holdSide ?? null,
      entryTimestamp: open.entryTimestamp ?? null,
      exitTimestamp: null,
      pnlPct: parsePnl(open.currentPnl),
      entryPrice: Number(open.entryPrice) || null,
      exitPrice: null,
      leverage:
        Number.isFinite(open.leverage as number) && Number(open.leverage) > 0
          ? Number(open.leverage)
          : params.leverageFromHistory,
    };
  }

  const combined = [...closedNormalized];
  if (openOverlay) combined.push(openOverlay);

  return combined.map((p) => {
    const pnlPct = finiteNumber(p.pnlPct);
    const pnlNet = finiteNumber(p.pnlNet);
    const capitalPctLooksPlaceholder =
      params.platform === 'capital' &&
      pnlPct !== null &&
      Math.abs(pnlPct) < 0.005 &&
      pnlNet !== null &&
      Math.abs(pnlNet) > 0.005;
    return {
      id: p.id,
      status: p.exitTimestamp ? 'closed' : 'open',
      side: p.side ?? null,
      entryTime: p.entryTimestamp ? Math.floor(p.entryTimestamp / 1000) : null,
      exitTime: p.exitTimestamp ? Math.floor(p.exitTimestamp / 1000) : null,
      pnlPct: capitalPctLooksPlaceholder ? null : pnlPct,
      pnlNet,
      entryPrice: positiveNumber(p.entryPrice),
      exitPrice: positiveNumber(p.exitPrice),
      leverage: positiveNumber(p.leverage),
      entryDecision: findNearestDecision(params.history, p.entryTimestamp),
      exitDecision: findNearestDecision(params.history, p.exitTimestamp),
      partialCloses: buildPartialCloses({
        history: params.history,
        entryTsMs: p.entryTimestamp,
        exitTsMs: p.exitTimestamp,
        fromMs: params.fromMs,
        nowMs: params.nowMs,
      }),
    };
  });
}

function isWindowPosition(row: any, fromMs: number, toMs: number): boolean {
  const exitMs = finiteNumber(row?.exitTimestamp);
  const entryMs = finiteNumber(row?.entryTimestamp);
  if (exitMs !== null) return exitMs >= fromMs && exitMs <= toMs;
  return entryMs !== null && entryMs >= fromMs && entryMs <= toMs;
}

// Warm the chart overlay KV for the dashboard presets. This intentionally uses
// persisted closed positions plus the already-fetched open position from analyze;
// it does not call broker position-history endpoints or affect trading behavior.
export async function warmPositionOverlayCacheFromAnalyze(params: {
  symbol: string;
  platform: AnalysisPlatform;
  nowMs: number;
  openPositionInfo?: OpenPositionInfo | null;
}): Promise<void> {
  const symbol = params.symbol.toUpperCase();
  const presetWindows = CHART_OVERLAY_WARM_PRESETS.map((preset) => {
    const tfSeconds = chartTimeframeToSeconds(preset.timeframe);
    return {
      ...preset,
      fromMs: Math.max(0, params.nowMs - preset.limit * tfSeconds * 1000),
    };
  });
  const earliestFromMs = Math.min(...presetWindows.map((preset) => preset.fromMs));
  const maxHistoryLimit = Math.max(
    200,
    Math.min(1_200, Math.ceil((params.nowMs - earliestFromMs) / (60 * 60 * 1000)) * 8),
  );
  let allHistory: any[] = [];
  let allClosed: any[] = [];
  try {
    [allHistory, allClosed] = await Promise.all([
      loadSymbolMarkerHistory(symbol, params.platform, {
        fromMs: earliestFromMs,
        toMs: params.nowMs,
        limit: maxHistoryLimit,
      }),
      loadClosedSwingPositions({
        platform: params.platform,
        symbol,
        fromMs: earliestFromMs,
        toMs: params.nowMs,
        limit: 1000,
      }),
    ]);
  } catch (err) {
    console.warn(`chart overlay warm source load failed for ${symbol}:`, err);
    return;
  }

  await Promise.all(
    presetWindows.map(async (preset) => {
      try {
        const history = allHistory.filter((row) => {
          const ts = finiteNumber(row?.timestamp);
          return ts !== null && ts >= preset.fromMs && ts <= params.nowMs;
        });
        const closed = allClosed.filter((row) => isWindowPosition(row, preset.fromMs, params.nowMs));
        const capturedLevs = extractCapturedLeverages(history);
        const leverageFromHistory = capturedLevs[0]?.leverage ?? null;
        const overlay = normalizeOverlayPositions({
          symbol,
          platform: params.platform,
          closed,
          openPositionInfo: params.openPositionInfo,
          history,
          leverageFromHistory,
          fromMs: preset.fromMs,
          nowMs: params.nowMs,
        });
        await writePositionOverlayCache({
          symbol,
          platform: params.platform,
          timeframe: preset.timeframe,
          limit: preset.limit,
          overlay,
        });
      } catch (err) {
        console.warn(`chart overlay warm failed for ${symbol} ${preset.timeframe}:`, err);
      }
    }),
  );
}
