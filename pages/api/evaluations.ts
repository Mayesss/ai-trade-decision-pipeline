import type { NextApiRequest, NextApiResponse } from 'next';
import { getAllEvaluations, getEvaluationTimestamp } from '../../lib/utils';
import { loadDecisionHistory, listHistorySymbols } from '../../lib/history';
import { fetchPositionInfo, fetchRealizedRoi, fetchRecentPositionWindows } from '../../lib/analytics';
import { requireAdminAccess } from '../../lib/admin';

type EnrichedEntry = {
  symbol: string;
  evaluation: any;
  evaluationTs?: number | null;
  lastBiasTimeframes?: Record<string, string | undefined> | null;
  pnl7d?: number | null;
  pnl7dWithOpen?: number | null;
  pnl7dNet?: number | null;
  pnl7dGross?: number | null;
  pnl7dTrades?: number | null;
  pnlSpark?: number[] | null;
  openPnl?: number | null;
  openDirection?: 'long' | 'short' | null;
  openLeverage?: number | null;
  openEntryPrice?: number | null;
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

const PNL_LOOKBACK_HOURS = 7 * 24;
const BTC_SYMBOL = 'BTCUSDT';
const BTC_LAST_POSITION_LEVERAGE_OVERRIDE = 3;

const scalePct = (value: number | null | undefined, factor: number): number | null | undefined => {
  if (typeof value !== 'number') return value;
  return value * factor;
};

// Returns the latest evaluation per symbol from the in-memory store,
// plus last decision info + 7d change pulled from recent history.
export default async function handler(req: NextApiRequest, res: NextApiResponse) {
  if (req.method !== 'GET') {
    res.status(405).json({ error: 'Method Not Allowed', message: 'Use GET' });
    return;
  }
  if (!requireAdminAccess(req, res)) return;

  const store = await getAllEvaluations();
  let symbols = Object.keys(store);
  try {
    const historySymbols = await listHistorySymbols();
    if (historySymbols.length) {
      symbols = historySymbols;
    }
  } catch {
    // If KV isn't configured, fall back to evaluations only.
  }

  const data: EnrichedEntry[] = await Promise.all(
    symbols.map(async (symbol) => {
      let pnl7d: number | null | undefined = null;
      let pnl7dWithOpen: number | null | undefined = null;
      let pnl7dNet: number | null | undefined = null;
      let pnl7dGross: number | null | undefined = null;
      let pnl7dTrades: number | null | undefined = null;
      let pnlSpark: number[] | null | undefined = null;
      let openPnl: number | null | undefined = null;
      let openDirection: 'long' | 'short' | null | undefined = null;
      let openLeverage: number | null | undefined = null;
      let openEntryPrice: number | null | undefined = null;
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

        const roiRes = await fetchRealizedRoi(symbol, PNL_LOOKBACK_HOURS);
        pnl7dNet = Number.isFinite(roiRes.roi as number) ? (roiRes.roi as number) : null;
        pnl7dTrades = roiRes.count;
        lastPositionPnl = Number.isFinite(roiRes.lastNetPct as number) ? (roiRes.lastNetPct as number) : null;
        lastPositionDirection = roiRes.lastSide ?? null;

        try {
          const recentWindows = await fetchRecentPositionWindows(symbol, PNL_LOOKBACK_HOURS);
          const lastWindows = recentWindows.slice(-14);
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
          pnl7dGross = grossPcts.length ? grossPcts.reduce((a, b) => a + b, 0) : null;
          pnl7d = netPcts.length ? netPcts.reduce((a, b) => a + b, 0) : null;

          const sampledWindows = lastWindows.filter((w) => Number.isFinite(w.pnlPct as number));
          if (sampledWindows.length) {
            const wins = sampledWindows.filter((w) => (w.pnlPct as number) > 0);
            const losses = sampledWindows.filter((w) => (w.pnlPct as number) < 0);
            winRate = (wins.length / sampledWindows.length) * 100;
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

          const lastWindow = recentWindows.length ? recentWindows[recentWindows.length - 1] : null;
          if (lastWindow) {
            lastPositionPnl = Number.isFinite(lastWindow.pnlPct as number) ? (lastWindow.pnlPct as number) : null;
            lastPositionDirection = lastWindow.side ?? null;
            if (lastPositionLeverage === null) {
              lastPositionLeverage = Number.isFinite(lastWindow.leverage as number)
                ? (lastWindow.leverage as number)
                : leverageFromHistory ?? null;
            }
          } else {
            lastPositionPnl = Number.isFinite(roiRes.lastNetPct as number) ? (roiRes.lastNetPct as number) : null;
            lastPositionDirection = roiRes.lastSide ?? null;
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
            openLeverage = Number.isFinite(pos.leverage as number)
              ? (pos.leverage as number)
              : leverageFromHistory ?? null;
            const entryPriceVal = Number(pos.entryPrice);
            openEntryPrice = Number.isFinite(entryPriceVal) && entryPriceVal > 0 ? entryPriceVal : null;
          } else {
            openPnl = null;
            openDirection = null;
            openLeverage = null;
            openEntryPrice = null;
          }
        } catch (err) {
          console.warn(`Could not fetch open PnL for ${symbol}:`, err);
        }

        // Temporary override requested by user: force BTCUSDT realized/last-position metrics to 3x leverage.
        if (symbol.toUpperCase() === BTC_SYMBOL) {
          const detectedLeverage =
            Number.isFinite(lastPositionLeverage as number) && (lastPositionLeverage as number) > 0
              ? (lastPositionLeverage as number)
              : 1;
          const scale = BTC_LAST_POSITION_LEVERAGE_OVERRIDE / detectedLeverage;
          if (Math.abs(scale - 1) > 1e-9) {
            lastPositionPnl = scalePct(lastPositionPnl, scale);
            pnl7d = scalePct(pnl7d, scale);
            pnl7dGross = scalePct(pnl7dGross, scale);
            avgWinPct = scalePct(avgWinPct, scale);
            avgLossPct = scalePct(avgLossPct, scale);
            pnlSpark = Array.isArray(pnlSpark) ? pnlSpark.map((v) => (typeof v === 'number' ? v * scale : v)) : pnlSpark;
          }
          lastPositionLeverage = BTC_LAST_POSITION_LEVERAGE_OVERRIDE;
        }

        // Combine realized 7d PnL with current open PnL (both percentage-based)
        if (typeof pnl7d === 'number' && typeof openPnl === 'number') {
          pnl7dWithOpen = pnl7d + openPnl;
        } else if (typeof pnl7d === 'number') {
          pnl7dWithOpen = pnl7d;
        } else if (typeof openPnl === 'number') {
          pnl7dWithOpen = openPnl;
        } else {
          pnl7dWithOpen = null;
        }

        evaluationTs = await getEvaluationTimestamp(symbol);
      } catch (err) {
        // Fail silently per symbol; still return evaluation
        console.warn(`Could not load history for ${symbol}:`, err);
      }

      return {
        symbol,
        evaluation: store[symbol],
        pnl7d,
        pnl7dWithOpen,
        pnl7dNet,
        pnl7dGross,
        pnl7dTrades,
        pnlSpark,
        openPnl,
        openDirection,
        openLeverage,
        openEntryPrice,
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
