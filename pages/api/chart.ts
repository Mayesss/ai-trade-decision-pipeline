import type { NextApiRequest, NextApiResponse } from 'next';
import {
  fetchMarketBundle as fetchBitgetMarketBundle,
  fetchPositionInfo as fetchBitgetPositionInfo,
  fetchRecentPositionWindows,
} from '../../lib/analytics';
import { fetchCapitalMarketBundle, fetchCapitalPositionInfo } from '../../lib/capital';
import { loadDecisionHistory, loadSymbolMarkerHistory, extractCapturedLeverages } from '../../lib/history';
import { requireAdminAccess } from '../../lib/admin';
import { resolveAnalysisPlatform, type AnalysisPlatform } from '../../lib/platform';
import { loadClosedSwingPositions } from '../../lib/swing/pg';
import { syncSwingClosedPositions } from '../../lib/swing/sync';
import {
  readChartCandlesCache,
  writeChartCandlesCache,
  normalizeChartCandles,
  type ChartCandle,
} from '../../lib/swing/chartCache';
import {
  readPositionOverlayCache,
  writePositionOverlayCache,
} from '../../lib/swing/positionOverlayCache';

const BTC_SYMBOL = 'BTCUSDT';
const BTC_CHART_LEVERAGE_OVERRIDE = 3;

function isBitgetUnknownSymbolError(err: unknown): boolean {
  const msg = err instanceof Error ? err.message : String(err ?? '');
  const lower = msg.toLowerCase();
  return msg.includes('40034') || (lower.includes('parameter') && lower.includes('does not exist'));
}

function timeframeToSeconds(tf: string): number {
  const match = /^(\d+)([smhd])$/i.exec(tf.trim());
  if (!match) return 60; // default 1m
  const value = Number(match[1]);
  const unit = match[2].toLowerCase();
  switch (unit) {
    case 's':
      return value;
    case 'm':
      return value * 60;
    case 'h':
      return value * 60 * 60;
    case 'd':
      return value * 24 * 60 * 60;
    default:
      return value * 60;
  }
}

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
  const requestStartedAt = Date.now();
  if (req.method !== 'GET') {
    return res.status(405).json({ error: 'Method Not Allowed', message: 'Use GET' });
  }
  if (!requireAdminAccess(req, res)) return;

  const symbol = String(req.query.symbol || '').toUpperCase();
  const platformParam = Array.isArray(req.query.platform) ? req.query.platform[0] : req.query.platform;
  const explicitPlatform =
    typeof platformParam === 'string' && platformParam.trim().length
      ? resolveAnalysisPlatform(platformParam)
      : null;
  const timeframeRaw = String(req.query.timeframe || '1H');
  const tfMatch = /^(\d+)([a-zA-Z])$/.exec(timeframeRaw.trim());
  const timeframe = tfMatch
    ? (() => {
        const value = tfMatch[1];
        const unit = tfMatch[2];
        if (unit === 'M') return `${value}M`;
        const lower = unit.toLowerCase();
        if (lower === 'h') return `${value}H`;
        if (lower === 'd') return `${value}D`;
        if (lower === 'w') return `${value}W`;
        if (lower === 'm') return `${value}m`;
        if (lower === 's') return `${value}s`;
        return `${value}${unit}`;
      })()
    : timeframeRaw;
  const tfSeconds = timeframeToSeconds(timeframe);
  const defaultLimit = Math.max(16, Math.ceil((7 * 24 * 60 * 60) / Math.max(1, tfSeconds)));
  const rawLimit = Number(req.query.limit);
  const limit = Number.isFinite(rawLimit) && rawLimit > 0 ? Math.floor(rawLimit) : defaultLimit;
  const boundedLimit = Math.min(Math.max(limit, 16), 2_000);

  if (!symbol) {
    return res.status(400).json({ error: 'symbol_required' });
  }

  try {
    let platform: AnalysisPlatform = explicitPlatform ?? 'bitget';
    if (!explicitPlatform) {
      try {
        const latestHistory = await loadDecisionHistory(symbol, 1);
        const latest = latestHistory[0];
        const inferred =
          typeof latest?.platform === 'string'
            ? latest.platform
            : typeof latest?.snapshot?.platform === 'string'
            ? latest.snapshot.platform
            : undefined;
        platform = resolveAnalysisPlatform(inferred);
      } catch {
        platform = 'bitget';
      }
    }

    const nowMsForCandles = Date.now();
    let candleCacheStatus: 'hit' | 'miss' | 'short' = 'miss';
    let overlayCacheStatus: 'hit' | 'miss' = 'miss';
    let closedPositionSource: 'cache' | 'persisted' | 'broker' | 'none' = 'none';
    let candleLoadMs = 0;
    let markerLoadMs = 0;
    let overlayLoadMs = 0;

    // Boundary-bucketed candle cache: served for the rest of the current bar,
    // warmed hourly by the analyze cron and written-through on a live miss here.
    // Markers/positions below are always computed live.
    let candles: ChartCandle[] = [];
    const candleLoadStartedAt = Date.now();
    const cachedCandles = await readChartCandlesCache({ symbol, platform, timeframe, nowMs: nowMsForCandles });
    if (cachedCandles && cachedCandles.length >= boundedLimit) {
      candleCacheStatus = 'hit';
      candles = cachedCandles.slice(-boundedLimit);
    } else if (cachedCandles) {
      candleCacheStatus = 'short';
    }

    if (!candles.length) {
      const fetchBundleByPlatform = async (targetPlatform: AnalysisPlatform) => {
        const fetchMarketBundle = targetPlatform === 'capital' ? fetchCapitalMarketBundle : fetchBitgetMarketBundle;
        return fetchMarketBundle(symbol, timeframe, { includeTrades: false, candleLimit: boundedLimit + 10 });
      };
      let bundle: any;
      try {
        bundle = await fetchBundleByPlatform(platform);
      } catch (err) {
        if (!explicitPlatform && platform === 'bitget' && isBitgetUnknownSymbolError(err)) {
          platform = 'capital';
          bundle = await fetchBundleByPlatform(platform);
        } else {
          throw err;
        }
      }

      const normalized = normalizeChartCandles(bundle.candles);
      candles = normalized.slice(-boundedLimit);
      // write-through so the next load in this bucket skips the broker fetch (uses
      // the corrected platform if the bitget→capital fallback flipped it above)
      void writeChartCandlesCache({ symbol, platform, timeframe, nowMs: nowMsForCandles, candles: normalized });
    }
    candleLoadMs = Date.now() - candleLoadStartedAt;

    const fetchPositionInfo = platform === 'capital' ? fetchCapitalPositionInfo : fetchBitgetPositionInfo;

    const candleMap = new Map<number, { close: number; open: number; high: number; low: number }>();
    for (const c of candles) {
      candleMap.set(c.time, c);
    }
    const candleTimes = candles.map((c) => c.time);
    // Positions/decisions in chart window.
    const nowMs = Date.now();
    const inferredRangeMs = boundedLimit * tfSeconds * 1000;
    const firstCandleMs = candles.length ? candles[0].time * 1000 : nowMs - inferredRangeMs;
    const windowStartMs = Math.max(0, Math.min(firstCandleMs, nowMs - inferredRangeMs));
    const historyHours = Math.max(24, Math.ceil((nowMs - windowStartMs) / (60 * 60 * 1000)));
    const historyLimit = Math.max(200, Math.min(1_200, historyHours * 8));
    // Only entry/exit markers (BUY/SELL/CLOSE) are drawn on the chart, so read them
    // straight from the per-symbol marker index (window-bounded) rather than
    // scanning the full decision index. KV-only; same data, far fewer round-trips.
    const markerLoadStartedAt = Date.now();
    const history = await loadSymbolMarkerHistory(symbol, platform, {
      fromMs: windowStartMs,
      toMs: nowMs,
      limit: historyLimit,
    });
    const markers =
      history
        ?.filter((h) => {
          if (!h.timestamp) return false;
          const ts = Number(h.timestamp);
          if (!(ts >= windowStartMs && ts <= nowMs)) return false;
          // Only entry/exit decisions get a chart arrow. HOLD (and anything
          // else) would just clutter the price line with repeated markers.
          const a = (h.aiDecision?.action || '').toUpperCase();
          return a === 'BUY' || a === 'SELL' || a === 'CLOSE';
        })
        .map((h) => {
          const rawTsSec = Math.floor(Number(h.timestamp) / 1000);
          const action = (h.aiDecision?.action || '').toUpperCase() || 'DECISION';
          const isBuy = action === 'BUY';
          const isSell = action === 'SELL';
          const isClose = action === 'CLOSE';

          // snap marker to nearest candle timestamp so it renders even if the decision came mid-bar
          let markerTime = rawTsSec;
          if (candleTimes.length) {
            markerTime = candleTimes.reduce(
              (prev, curr) => (Math.abs(curr - rawTsSec) < Math.abs(prev - rawTsSec) ? curr : prev),
              candleTimes[0]
            );
          }

          const candleAtTime = candleMap.get(markerTime);
          const markerPrice = candleAtTime?.close;

          return {
            time: markerTime,
            position: isBuy ? 'belowBar' : 'aboveBar',
            color: isBuy ? '#16a34a' : isSell ? '#dc2626' : '#334155',
            shape: isClose ? 'arrowDown' : isBuy ? 'arrowUp' : 'arrowDown',
            text: '',
            price: markerPrice,
          };
        })
        .filter(
          (m) => candleTimes.length === 0 || (m.time >= candleTimes[0] && m.time <= candleTimes[candleTimes.length - 1])
        )
        .sort((a, b) => a.time - b.time) || [];
    markerLoadMs = Date.now() - markerLoadStartedAt;

    const capturedLevs = extractCapturedLeverages(history);
    const leverageFromHistory = capturedLevs[0]?.leverage ?? null;
    const finiteNumber = (value: unknown): number | null => {
      if (value === null || value === undefined || value === '') return null;
      const n = Number(value);
      return Number.isFinite(n) ? n : null;
    };
    const positiveNumber = (value: unknown): number | null => {
      const n = finiteNumber(value);
      return n !== null && n > 0 ? n : null;
    };

    const findNearestDecision = (tsMs?: number | null) => {
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
    };
    const getPartialClosePct = (entry: any): number | null => {
      const pct =
        finiteNumber(entry?.execResult?.partialClosePct) ??
        finiteNumber(entry?.aiDecision?.exit_size_pct) ??
        finiteNumber(entry?.aiDecision?.close_size_pct) ??
        finiteNumber(entry?.aiDecision?.partial_close_pct);
      return pct !== null && pct > 0 && pct < 100 ? pct : null;
    };
    const buildPartialCloses = (entryTsMs?: number | null, exitTsMs?: number | null) => {
      const fromMs = finiteNumber(entryTsMs) ?? windowStartMs;
      const toMs = finiteNumber(exitTsMs) ?? nowMs;
      return (history || [])
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
    };

    let positions: any[] = [];
    const overlayLoadStartedAt = Date.now();
    // Serve the position overlay from a short-lived KV cache when present: it skips
    // the Neon closed-positions read (lowers Neon transfer) and the live broker
    // fetch below. Only successful computations are cached, so an error path retries
    // next load rather than caching an empty overlay.
    const cachedOverlay = await readPositionOverlayCache({ symbol, platform, timeframe, limit: boundedLimit });
    if (cachedOverlay) {
      overlayCacheStatus = 'hit';
      closedPositionSource = 'cache';
      positions = cachedOverlay as any[];
    } else {
      try {
        const loadPersistedClosed = () =>
          loadClosedSwingPositions({
            platform,
            symbol,
            fromMs: windowStartMs,
            toMs: nowMs,
            limit: 1000,
          });
        let closed = await loadPersistedClosed();
        if (closed.length) {
          closedPositionSource = 'persisted';
        }
        if (!closed.length && platform === 'bitget') {
          closed = await fetchRecentPositionWindows(symbol, historyHours, capturedLevs);
          closedPositionSource = closed.length ? 'broker' : 'none';
          // Keep the mirror warm for the next chart load. Best-effort; never blocks
          // chart rendering beyond the fallback broker read we already had to do.
          await syncSwingClosedPositions(platform, closed, capturedLevs);
        }
        const closedNormalized =
          platform === 'bitget' && symbol === BTC_SYMBOL
            ? closed.map((position) => {
                const rawLev = Number(position.leverage);
                const fallbackLev = Number(leverageFromHistory);
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
            : closed;
        let openOverlay: any = null;
        try {
          const open = await fetchPositionInfo(symbol);
          if (open.status === 'open') {
            const pnl = typeof open.currentPnl === 'string' ? open.currentPnl.replace('%', '') : open.currentPnl;
            const pnlVal = Number(pnl);
            openOverlay = {
              id: `${platform}:${symbol}-open-position`,
              symbol,
              side: open.holdSide ?? null,
              entryTimestamp: open.entryTimestamp ?? null,
              exitTimestamp: null,
              pnlPct: Number.isFinite(pnlVal) ? pnlVal : null,
              entryPrice: Number(open.entryPrice) || null,
              exitPrice: null,
              leverage: Number.isFinite(open.leverage as number) ? (open.leverage as number) : leverageFromHistory,
            };
          }
        } catch (err) {
          console.warn(`Could not fetch open position for ${symbol}:`, err);
        }

        const combined = [...closedNormalized];
        if (openOverlay) combined.push(openOverlay);

        positions = combined.map((p) => {
          const pnlPct = finiteNumber(p.pnlPct);
          const pnlNet = finiteNumber(p.pnlNet);
          const capitalPctLooksPlaceholder =
            platform === 'capital' &&
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
            entryDecision: findNearestDecision(p.entryTimestamp),
            exitDecision: findNearestDecision(p.exitTimestamp),
            partialCloses: buildPartialCloses(p.entryTimestamp, p.exitTimestamp),
          };
        });
        // Cache only on success so an error path retries rather than caching empty.
        void writePositionOverlayCache({ symbol, platform, timeframe, limit: boundedLimit, overlay: positions });
      } catch (err) {
        console.warn(`Failed to build position overlays for ${symbol}:`, err);
        positions = [];
      }
    }
    overlayLoadMs = Date.now() - overlayLoadStartedAt;

    res.setHeader(
      'x-swing-chart-cache',
      `candles=${candleCacheStatus}; overlay=${overlayCacheStatus}; closedPositions=${closedPositionSource}`,
    );
    res.setHeader(
      'x-swing-chart-timing-ms',
      `candles=${candleLoadMs}; markers=${markerLoadMs}; overlay=${overlayLoadMs}; total=${Date.now() - requestStartedAt}`,
    );
    res.status(200).json({ symbol, platform, timeframe, candles, markers, positions });
  } catch (err: any) {
    console.error('Error fetching chart data:', err);
    res.status(500).json({ error: err?.message || 'chart_fetch_failed' });
  }
}
