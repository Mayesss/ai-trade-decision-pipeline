import type { NextApiRequest, NextApiResponse } from 'next';
import { getAllEvaluations } from '../../lib/utils';
import { loadDecisionHistory } from '../../lib/history';
import { fetchPositionInfo, fetchRealizedRoi } from '../../lib/analytics';

type EnrichedEntry = {
  symbol: string;
  evaluation: any;
  pnl24h?: number | null;
  pnl24hTrades?: number | null;
  openPnl?: number | null;
  openDirection?: 'long' | 'short' | null;
  lastPositionPnl?: number | null;
  lastPositionDirection?: 'long' | 'short' | null;
  lastDecisionTs?: number | null;
  lastDecision?: any;
  lastMetrics?: any;
};

// Returns the latest evaluation per symbol from the in-memory store,
// plus last decision info + 24h change pulled from recent history.
export default async function handler(req: NextApiRequest, res: NextApiResponse) {
  if (req.method !== 'GET') {
    res.status(405).json({ error: 'Method Not Allowed', message: 'Use GET' });
    return;
  }

  const store = await getAllEvaluations();
  const symbols = Object.keys(store);

  const data: EnrichedEntry[] = await Promise.all(
    symbols.map(async (symbol) => {
      let pnl24h: number | null | undefined = null;
      let pnl24hTrades: number | null | undefined = null;
      let openPnl: number | null | undefined = null;
      let openDirection: 'long' | 'short' | null | undefined = null;
      let lastPositionPnl: number | null | undefined = null;
      let lastPositionDirection: 'long' | 'short' | null | undefined = null;
      let lastDecisionTs: number | null | undefined = null;
      let lastDecision: any = null;
      let lastMetrics: any = null;

      try {
        const history = await loadDecisionHistory(symbol, 120);

        const latest = history[0];
        if (latest) {
          lastDecision = latest.aiDecision ?? null;
          lastMetrics = latest.snapshot?.metrics ?? null;
          lastDecisionTs = Number.isFinite(latest.timestamp) ? Number(latest.timestamp) : null;
        }

        const roiRes = await fetchRealizedRoi(symbol, 24);
        pnl24h = Number.isFinite(roiRes.sumPct as number) ? (roiRes.sumPct as number) : null;
        pnl24hTrades = roiRes.count;
        lastPositionPnl = Number.isFinite(roiRes.lastNetPct as number) ? (roiRes.lastNetPct as number) : null;
        lastPositionDirection = roiRes.lastSide ?? null;

        try {
          const pos = await fetchPositionInfo(symbol);
          if (pos.status === 'open') {
            const raw = typeof pos.currentPnl === 'string' ? pos.currentPnl.replace('%', '') : pos.currentPnl;
            const val = Number(raw);
            openPnl = Number.isFinite(val) ? val : null;
            openDirection = pos.holdSide ?? null;
          } else {
            openPnl = null;
            openDirection = null;
          }
        } catch (err) {
          console.warn(`Could not fetch open PnL for ${symbol}:`, err);
        }
      } catch (err) {
        // Fail silently per symbol; still return evaluation
        console.warn(`Could not load history for ${symbol}:`, err);
      }

      return {
        symbol,
        evaluation: store[symbol],
        pnl24h,
        pnl24hTrades,
        openPnl,
        openDirection,
        lastPositionPnl,
        lastPositionDirection,
        lastDecisionTs,
        lastDecision,
        lastMetrics,
      };
    }),
  );

  res.status(200).json({ symbols, data });
}
