import type { NextApiRequest, NextApiResponse } from 'next';
import {
  fetchMarketBundle as fetchBitgetMarketBundle,
  fetchPositionInfo as fetchBitgetPositionInfo,
  fetchRecentPositionWindows,
} from '../../lib/analytics';
import { fetchCapitalMarketBundle, fetchCapitalPositionInfo } from '../../lib/capital';
import { loadDecisionHistory } from '../../lib/history';
import { requireAdminAccess } from '../../lib/admin';
import { resolveAnalysisPlatform, type AnalysisPlatform } from '../../lib/platform';

const BTC_SYMBOL = 'BTCUSDT';
const BTC_CHART_LEVERAGE_OVERRIDE = 3;
type ChartCandle = {
  time: number;
  open: number;
  high: number;
  low: number;
  close: number;
};

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

    const fetchPositionInfo = platform === 'capital' ? fetchCapitalPositionInfo : fetchBitgetPositionInfo;

    const candles: ChartCandle[] = Array.isArray(bundle.candles)
      ? bundle.candles
          .slice(-boundedLimit)
          .map((c: any) => ({
            time: Math.floor(Number(c?.[0]) / 1000),
            open: Number(c?.[1]),
            high: Number(c?.[2]),
            low: Number(c?.[3]),
            close: Number(c?.[4]),
          }))
          .sort((a: ChartCandle, b: ChartCandle) => a.time - b.time)
      : [];

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
    const history = await loadDecisionHistory(symbol, historyLimit, platform);
    const markers =
      history
        ?.filter((h) => h.timestamp && Number(h.timestamp) >= windowStartMs && Number(h.timestamp) <= nowMs)
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
            text: action,
            price: markerPrice,
          };
        })
        .filter(
          (m) => candleTimes.length === 0 || (m.time >= candleTimes[0] && m.time <= candleTimes[candleTimes.length - 1])
        )
        .sort((a, b) => a.time - b.time) || [];

    const leverageFromHistory =
      history
        ?.map((h) => {
          const lev =
            Number((h.execResult as any)?.leverage) ||
            Number((h.aiDecision as any)?.leverage) ||
            Number((h.execResult as any)?.targetLeverage);
          return Number.isFinite(lev) && lev > 0 ? lev : null;
        })
        .find((v) => v !== null) ?? null;

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

    let positions: any[] = [];
    try {
      const closed = platform === 'capital' ? [] : await fetchRecentPositionWindows(symbol, historyHours);
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

      positions = combined.map((p) => ({
        id: p.id,
        status: p.exitTimestamp ? 'closed' : 'open',
        side: p.side ?? null,
        entryTime: p.entryTimestamp ? Math.floor(p.entryTimestamp / 1000) : null,
        exitTime: p.exitTimestamp ? Math.floor(p.exitTimestamp / 1000) : null,
        pnlPct: Number.isFinite(p.pnlPct) ? p.pnlPct : null,
        entryPrice: p.entryPrice ?? null,
        exitPrice: p.exitPrice ?? null,
        leverage: p.leverage ?? null,
        entryDecision: findNearestDecision(p.entryTimestamp),
        exitDecision: findNearestDecision(p.exitTimestamp),
      }));
    } catch (err) {
      console.warn(`Failed to build position overlays for ${symbol}:`, err);
      positions = [];
    }

    res.status(200).json({ symbol, platform, timeframe, candles, markers, positions });
  } catch (err: any) {
    console.error('Error fetching chart data:', err);
    res.status(500).json({ error: err?.message || 'chart_fetch_failed' });
  }
}
