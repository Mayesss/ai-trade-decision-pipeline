import type { NextApiRequest, NextApiResponse } from 'next';
import { getAllEvaluations } from '../../lib/utils';
import { loadDecisionHistory } from '../../lib/history';

type EnrichedEntry = {
  symbol: string;
  evaluation: any;
  pnl24h?: number | null;
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

  const store = getAllEvaluations();
  const symbols = Object.keys(store);

  const data: EnrichedEntry[] = await Promise.all(
    symbols.map(async (symbol) => {
      let pnl24h: number | null | undefined = null;
      let lastDecision: any = null;
      let lastMetrics: any = null;

      try {
        const history = await loadDecisionHistory(symbol, 3);
        const latest = history[0];
        if (latest) {
          lastDecision = latest.aiDecision ?? null;
          lastMetrics = latest.snapshot?.metrics ?? null;
          const change = latest.snapshot?.change24h;
          pnl24h = Number.isFinite(change) ? Number(change) : null;
        }
      } catch (err) {
        // Fail silently per symbol; still return evaluation
        console.warn(`Could not load history for ${symbol}:`, err);
      }

      return { symbol, evaluation: store[symbol], pnl24h, lastDecision, lastMetrics };
    }),
  );

  res.status(200).json({ symbols, data });
}
