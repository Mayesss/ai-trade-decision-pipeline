import type { NextApiRequest, NextApiResponse } from 'next';
import { getAllEvaluations } from '../../lib/utils';
import { loadDecisionHistory } from '../../lib/history';
import { fetchPositionInfo } from '../../lib/analytics';

type EnrichedEntry = {
  symbol: string;
  evaluation: any;
  pnl24h?: number | null;
  pnl24hTrades?: number | null;
  openPnl?: number | null;
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
      let lastDecisionTs: number | null | undefined = null;
      let lastDecision: any = null;
      let lastMetrics: any = null;

      try {
        const history = await loadDecisionHistory(symbol, 120);
        const now = Date.now();

        const closeTrades = history.filter(
          (h) => h.aiDecision?.action === 'CLOSE' && now - Number(h.timestamp) <= 24 * 60 * 60 * 1000,
        );

        if (closeTrades.length) {
          const totalPnl = closeTrades.reduce((acc, h) => {
            const pnlCandidate =
              h.execResult?.pnlPct ??
              h.execResult?.pnl_pct ??
              h.snapshot?.metrics?.pnlPct ??
              h.snapshot?.positionContext?.unrealized_pnl_pct;
            const pnl = Number(pnlCandidate);
            return Number.isFinite(pnl) ? acc + pnl : acc;
          }, 0);
          pnl24h = totalPnl;
          pnl24hTrades = closeTrades.length;
        } else {
          pnl24h = null;
          pnl24hTrades = 0;
        }

        const latest = history[0];
        if (latest) {
          lastDecision = latest.aiDecision ?? null;
          lastMetrics = latest.snapshot?.metrics ?? null;
          lastDecisionTs = Number.isFinite(latest.timestamp) ? Number(latest.timestamp) : null;
        }

        try {
          const pos = await fetchPositionInfo(symbol);
          if (pos.status === 'open') {
            const raw = typeof pos.currentPnl === 'string' ? pos.currentPnl.replace('%', '') : pos.currentPnl;
            const val = Number(raw);
            openPnl = Number.isFinite(val) ? val : null;
          } else {
            openPnl = null;
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
        lastDecisionTs,
        lastDecision,
        lastMetrics,
      };
    }),
  );

  res.status(200).json({ symbols, data });
}
