import type { NextApiRequest, NextApiResponse } from 'next';
import { fetchMarketBundle, fetchPositionInfo, fetchRecentPositionWindows } from '../../lib/analytics';
import { loadDecisionHistory } from '../../lib/history';

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

  const symbol = String(req.query.symbol || '').toUpperCase();
  const timeframe = String(req.query.timeframe || '15m');
  const limit = Number(req.query.limit || 96); // 24h of 15m candles = 96

  if (!symbol) {
    return res.status(400).json({ error: 'symbol_required' });
  }

  try {
    const bundle = await fetchMarketBundle(symbol, timeframe, { includeTrades: false, candleLimit: limit + 10 });

    const candles = Array.isArray(bundle.candles)
      ? bundle.candles
          .slice(-limit)
          .map((c: any) => ({
            time: Math.floor(Number(c?.[0]) / 1000),
            open: Number(c?.[1]),
            high: Number(c?.[2]),
            low: Number(c?.[3]),
            close: Number(c?.[4]),
          }))
          .sort((a, b) => a.time - b.time)
      : [];

    const candleMap = new Map<number, { close: number; open: number; high: number; low: number }>();
    for (const c of candles) {
      candleMap.set(c.time, c);
    }
    const candleTimes = candles.map((c) => c.time);
    // Positions/decisions in last 24h
    const nowMs = Date.now();
    const dayAgo = nowMs - 24 * 60 * 60 * 1000;
    const history = await loadDecisionHistory(symbol, 200);
    const markers =
      history
        ?.filter((h) => h.timestamp && Number(h.timestamp) >= dayAgo && Number(h.timestamp) <= nowMs)
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
      const closed = await fetchRecentPositionWindows(symbol, 24);
      let openOverlay: any = null;
      try {
        const open = await fetchPositionInfo(symbol);
        if (open.status === 'open') {
          const pnl = typeof open.currentPnl === 'string' ? open.currentPnl.replace('%', '') : open.currentPnl;
          const pnlVal = Number(pnl);
          openOverlay = {
            id: `${symbol}-open-position`,
            symbol,
            side: open.holdSide ?? null,
            entryTimestamp: open.entryTimestamp ?? null,
            exitTimestamp: null,
            pnlPct: Number.isFinite(pnlVal) ? pnlVal : null,
            entryPrice: Number(open.entryPrice) || null,
            exitPrice: null,
            leverage: typeof open.leverage === 'number' ? open.leverage : null,
          };
        }
      } catch (err) {
        console.warn(`Could not fetch open position for ${symbol}:`, err);
      }

      const combined = [...closed];
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

    res.status(200).json({ symbol, timeframe, candles, markers, positions });
  } catch (err: any) {
    console.error('Error fetching chart data:', err);
    res.status(500).json({ error: err?.message || 'chart_fetch_failed' });
  }
}
