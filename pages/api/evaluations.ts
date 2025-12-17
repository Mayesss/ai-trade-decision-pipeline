import type { NextApiRequest, NextApiResponse } from 'next';
import {
  getAllEvaluations,
  getEvaluationTimestamp,
  getExecEvaluation,
  getExecEvaluationTimestamp,
  getPlanEvaluation,
  getPlanEvaluationTimestamp,
} from '../../lib/utils';
import { loadDecisionHistory } from '../../lib/history';
import { fetchPositionInfo, fetchRealizedRoi, fetchRecentPositionWindows } from '../../lib/analytics';

type EnrichedEntry = {
  symbol: string;
  evaluation: any;
  evaluationTs?: number | null;
  planEvaluation?: any;
  planEvaluationTs?: number | null;
  execEvaluation?: any;
  execEvaluationTs?: number | null;
  lastBiasTimeframes?: Record<string, string | undefined> | null;
  pnl24h?: number | null;
  pnl24hWithOpen?: number | null;
  pnl24hNet?: number | null;
  pnl24hGross?: number | null;
  pnl24hTrades?: number | null;
  pnlSpark?: number[] | null;
  openPnl?: number | null;
  openDirection?: 'long' | 'short' | null;
  openLeverage?: number | null;
  lastPositionPnl?: number | null;
  lastPositionDirection?: 'long' | 'short' | null;
  lastPositionLeverage?: number | null;
  lastDecisionTs?: number | null;
  lastDecision?: any;
  lastMetrics?: any;
  lastPrompt?: { system?: string; user?: string } | null;
  winRate?: number | null;
  avgWinPct?: number | null;
  avgLossPct?: number | null;
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
      let pnl24hWithOpen: number | null | undefined = null;
      let pnl24hNet: number | null | undefined = null;
      let pnl24hGross: number | null | undefined = null;
      let pnl24hTrades: number | null | undefined = null;
      let pnlSpark: number[] | null | undefined = null;
      let openPnl: number | null | undefined = null;
      let openDirection: 'long' | 'short' | null | undefined = null;
      let openLeverage: number | null | undefined = null;
      let lastPositionPnl: number | null | undefined = null;
      let lastPositionDirection: 'long' | 'short' | null | undefined = null;
      let lastPositionLeverage: number | null | undefined = null;
      let lastDecisionTs: number | null | undefined = null;
      let lastDecision: any = null;
      let lastMetrics: any = null;
      let lastPrompt: { system?: string; user?: string } | null = null;
      let winRate: number | null | undefined = null;
      let avgWinPct: number | null | undefined = null;
      let avgLossPct: number | null | undefined = null;
      let evaluationTs: number | null | undefined = null;
      let planEvaluation: any = null;
      let planEvaluationTs: number | null | undefined = null;
      let execEvaluation: any = null;
      let execEvaluationTs: number | null | undefined = null;
      let lastBiasTimeframes: Record<string, string | undefined> | null = null;

      try {
        const history = await loadDecisionHistory(symbol, 120);

        const latest = history[0];
        if (latest) {
          lastDecision = latest.aiDecision ?? null;
          lastMetrics = latest.snapshot?.metrics ?? null;
          lastDecisionTs = Number.isFinite(latest.timestamp) ? Number(latest.timestamp) : null;
          lastPrompt = latest.prompt ?? null;
          lastBiasTimeframes = latest.biasTimeframes ?? null;
        }
        // Get latest leverage we set/applied from history (execResult or aiDecision hint)
        const leverageFromHistory = history
          .map((h) => {
            const lev =
              Number((h.execResult as any)?.leverage) ||
              Number((h.aiDecision as any)?.leverage) ||
              Number((h.execResult as any)?.targetLeverage);
            return Number.isFinite(lev) && lev > 0 ? lev : null;
          })
          .find((v) => v !== null);

        const roiRes = await fetchRealizedRoi(symbol, 24);
        pnl24h = Number.isFinite(roiRes.sumPct as number) ? (roiRes.sumPct as number) : null;
        pnl24hNet = Number.isFinite(roiRes.roi as number) ? (roiRes.roi as number) : null;
        pnl24hTrades = roiRes.count;
        lastPositionPnl = Number.isFinite(roiRes.lastNetPct as number) ? (roiRes.lastNetPct as number) : null;
        lastPositionDirection = roiRes.lastSide ?? null;

        try {
          const recentWindows = await fetchRecentPositionWindows(symbol, 24);
          const lastWindows = recentWindows.slice(-10);
          const spark = lastWindows
            .map((w) => (Number.isFinite(w.pnlPct as number) ? (w.pnlPct as number) : null))
            .filter((v): v is number => typeof v === 'number');
          pnlSpark = spark.length ? spark : null;

          const grossPcts = recentWindows
            .map((w) => (Number.isFinite(w.pnlGrossPct as number) ? (w.pnlGrossPct as number) : null))
            .filter((v): v is number => typeof v === 'number');
          const netPcts = recentWindows
            .map((w) => (Number.isFinite(w.pnlPct as number) ? (w.pnlPct as number) : null))
            .filter((v): v is number => typeof v === 'number');
          pnl24hGross = grossPcts.length ? grossPcts.reduce((a, b) => a + b, 0) : null;
          // pnl24h (net) already uses sumPct from ROI; if missing, fallback to netPcts sum
          if (pnl24h === null && netPcts.length) {
            pnl24h = netPcts.reduce((a, b) => a + b, 0);
          }

          const lastTen = lastWindows.filter((w) => Number.isFinite(w.pnlPct as number));
          if (lastTen.length) {
            const wins = lastTen.filter((w) => (w.pnlPct as number) > 0);
            const losses = lastTen.filter((w) => (w.pnlPct as number) < 0);
            winRate = (wins.length / lastTen.length) * 100;
            avgWinPct = wins.length
              ? wins.reduce((acc, w) => acc + (w.pnlPct as number), 0) / wins.length
              : null;
            avgLossPct = losses.length
              ? losses.reduce((acc, w) => acc + (w.pnlPct as number), 0) / losses.length
              : null;
            // Pick the most recent window that has leverage info
            const lastWithLev = lastWindows
              .slice()
              .reverse()
              .find((w) => Number.isFinite(w.leverage as number));
            lastPositionLeverage =
              lastWithLev && Number.isFinite(lastWithLev.leverage as number)
                ? (lastWithLev.leverage as number)
                : leverageFromHistory ?? null;
          }
        } catch (err) {
          console.warn(`Could not fetch sparkline PnL for ${symbol}:`, err);
        }

        try {
          const pos = await fetchPositionInfo(symbol);
          if (pos.status === 'open') {
            const raw = typeof pos.currentPnl === 'string' ? pos.currentPnl.replace('%', '') : pos.currentPnl;
            const val = Number(raw);
            openPnl = Number.isFinite(val) ? val : null;
            openDirection = pos.holdSide ?? null;
            openLeverage = leverageFromHistory ?? null;
          } else {
            openPnl = null;
            openDirection = null;
            openLeverage = null;
          }
        } catch (err) {
          console.warn(`Could not fetch open PnL for ${symbol}:`, err);
        }

        // Combine realized 24h PnL with current open PnL (percentage-based)
        if (typeof pnl24h === 'number' && typeof openPnl === 'number') {
          pnl24hWithOpen = pnl24h + openPnl;
        } else if (typeof pnl24h === 'number') {
          pnl24hWithOpen = pnl24h;
        } else if (typeof openPnl === 'number') {
          pnl24hWithOpen = openPnl;
        } else {
          pnl24hWithOpen = null;
        }

        evaluationTs = await getEvaluationTimestamp(symbol);

        try {
          planEvaluation = await getPlanEvaluation(symbol);
          planEvaluationTs = await getPlanEvaluationTimestamp(symbol);
        } catch (err) {
          console.warn(`Could not load plan evaluation for ${symbol}:`, err);
        }

        try {
          execEvaluation = await getExecEvaluation(symbol);
          execEvaluationTs = await getExecEvaluationTimestamp(symbol);
        } catch (err) {
          console.warn(`Could not load exec evaluation for ${symbol}:`, err);
        }
      } catch (err) {
        // Fail silently per symbol; still return evaluation
        console.warn(`Could not load history for ${symbol}:`, err);
      }

      return {
        symbol,
        evaluation: store[symbol],
        planEvaluation,
        planEvaluationTs,
        execEvaluation,
        execEvaluationTs,
        pnl24h,
        pnl24hWithOpen,
        pnl24hNet,
        pnl24hGross,
        pnl24hTrades,
        pnlSpark,
        openPnl,
        openDirection,
        openLeverage,
        lastPositionPnl,
        lastPositionDirection,
        lastPositionLeverage,
        evaluationTs,
        lastBiasTimeframes,
        lastDecisionTs,
        lastDecision,
        lastMetrics,
        lastPrompt,
        winRate,
        avgWinPct,
        avgLossPct,
      };
    }),
  );

  res.status(200).json({ symbols, data });
}
