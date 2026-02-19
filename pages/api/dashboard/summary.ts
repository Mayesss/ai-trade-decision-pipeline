import type { NextApiRequest, NextApiResponse } from 'next';

import {
  fetchPositionInfo as fetchBitgetPositionInfo,
  fetchRealizedRoi as fetchBitgetRealizedRoi,
  fetchRecentPositionWindows,
} from '../../../lib/analytics';
import { fetchCapitalPositionInfo, fetchCapitalRealizedRoi } from '../../../lib/capital';
import { loadDecisionHistory } from '../../../lib/history';
import { requireAdminAccess } from '../../../lib/admin';
import { getCronSymbolConfigs } from '../../../lib/symbolRegistry';
import type { AnalysisPlatform } from '../../../lib/platform';

type SummaryEntry = {
  symbol: string;
  lastPlatform: AnalysisPlatform;
  lastNewsSource?: string | null;
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
  winRate?: number | null;
  avgWinPct?: number | null;
  avgLossPct?: number | null;
};

type SummaryRangeKey = '7D' | '30D' | '6M';
const SUMMARY_RANGE_LOOKBACK_HOURS: Record<SummaryRangeKey, number> = {
  '7D': 7 * 24,
  '30D': 30 * 24,
  '6M': 183 * 24,
};
const BTC_SYMBOL = 'BTCUSDT';
const BTC_LAST_POSITION_LEVERAGE_OVERRIDE = 3;

const scalePct = (value: number | null | undefined, factor: number): number | null | undefined => {
  if (typeof value !== 'number') return value;
  return value * factor;
};

function resolveSummaryRange(raw: unknown): SummaryRangeKey {
  const normalized = String(raw || '')
    .trim()
    .toUpperCase();
  if (normalized === '30D') return '30D';
  if (normalized === '6M') return '6M';
  return '7D';
}

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
  if (req.method !== 'GET') {
    return res.status(405).json({ error: 'Method Not Allowed', message: 'Use GET' });
  }
  if (!requireAdminAccess(req, res)) return;

  const rangeParam = Array.isArray(req.query.range) ? req.query.range[0] : req.query.range;
  const range = resolveSummaryRange(rangeParam);
  const lookbackHours = SUMMARY_RANGE_LOOKBACK_HOURS[range];

  const configs = getCronSymbolConfigs();
  const symbols = configs.map((item) => item.symbol);

  const data: SummaryEntry[] = await Promise.all(
    configs.map(async (config) => {
      const symbol = config.symbol;
      const platform = config.platform;

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
      let winRate: number | null | undefined = null;
      let avgWinPct: number | null | undefined = null;
      let avgLossPct: number | null | undefined = null;
      let lastNewsSource: string | null | undefined = config.newsSource;

      try {
        const history = await loadDecisionHistory(symbol, 120, platform);
        const latest = history[0];
        if (latest) {
          lastNewsSource =
            typeof latest.newsSource === 'string'
              ? latest.newsSource
              : typeof latest.snapshot?.newsSource === 'string'
              ? latest.snapshot.newsSource
              : config.newsSource;
        }
        const leverageFromHistory = history
          .map((h) => {
            const lev =
              Number((h.execResult as any)?.leverage) ||
              Number((h.aiDecision as any)?.leverage) ||
              Number((h.execResult as any)?.targetLeverage);
            return Number.isFinite(lev) && lev > 0 ? lev : null;
          })
          .find((v) => v !== null);

        const fetchRealizedRoi = platform === 'capital' ? fetchCapitalRealizedRoi : fetchBitgetRealizedRoi;
        const fetchPositionInfo = platform === 'capital' ? fetchCapitalPositionInfo : fetchBitgetPositionInfo;

        const roiRes = await fetchRealizedRoi(symbol, lookbackHours);
        pnl7dNet = Number.isFinite(roiRes.roi as number) ? (roiRes.roi as number) : null;
        pnl7dTrades = roiRes.count;
        lastPositionPnl = Number.isFinite(roiRes.lastNetPct as number) ? (roiRes.lastNetPct as number) : null;
        lastPositionDirection = roiRes.lastSide ?? null;

        if (platform !== 'capital') {
          try {
            const recentWindows = await fetchRecentPositionWindows(symbol, lookbackHours);
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

        if (platform === 'bitget' && symbol.toUpperCase() === BTC_SYMBOL) {
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

        if (typeof pnl7d === 'number' && typeof openPnl === 'number') {
          pnl7dWithOpen = pnl7d + openPnl;
        } else if (typeof pnl7d === 'number') {
          pnl7dWithOpen = pnl7d;
        } else if (typeof openPnl === 'number') {
          pnl7dWithOpen = openPnl;
        } else {
          pnl7dWithOpen = null;
        }
      } catch (err) {
        console.warn(`Could not build summary for ${symbol}:`, err);
      }

      return {
        symbol,
        lastPlatform: platform,
        lastNewsSource,
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
        winRate,
        avgWinPct,
        avgLossPct,
      };
    }),
  );

  return res.status(200).json({ symbols, data, range });
}
