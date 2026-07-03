import type { NextApiRequest, NextApiResponse } from 'next';

import {
  fetchPositionInfo as fetchBitgetPositionInfo,
  fetchRealizedRoi as fetchBitgetRealizedRoi,
  fetchRecentPositionWindows,
} from '../../../lib/analytics';
import {
  fetchCapitalPositionInfo,
  fetchCapitalRealizedRoi,
  fetchCapitalTradeTransactions,
  type CapitalTradeTransactionRow,
} from '../../../lib/capital';
import { loadDecisionHistory, extractCapturedLeverages } from '../../../lib/history';
import { syncSwingClosedPositions } from '../../../lib/swing/sync';
import { loadClosedSwingPositions, upsertSwingPosition } from '../../../lib/swing/pg';
import { kvGetJson, kvSetJson } from '../../../lib/kv';
import { requireAdminAccess } from '../../../lib/admin';
import { getCronSymbolConfigs } from '../../../lib/symbolRegistry';
import type { AnalysisPlatform } from '../../../lib/platform';
import { buildForexEventContext, ensureForexEventsState } from '../../../lib/swing/forexEvents';
import { swingSummaryCacheKey } from '../../../lib/swing/summaryCache';
import type { PositionWindow } from '../../../lib/analytics';

type SummaryEntry = {
  symbol: string;
  category?: string | null;
  lastPlatform: AnalysisPlatform;
  lastNewsSource?: string | null;
  forexEventContext?: any | null;
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
  // Whether the most recent decision was a real AI call (not a calm-market /
  // signal-strength pre-AI skip). Crons run hourly, so this == "the AI decided
  // this symbol in the last hour". Drives the symbol-tab status dot.
  lastWasAiCall?: boolean;
  winRate?: number | null;
  avgWinPct?: number | null;
  avgLossPct?: number | null;
};

type SummaryRangeKey = '1D' | '7D' | '30D' | '6M';
const SUMMARY_RANGE_LOOKBACK_HOURS: Record<SummaryRangeKey, number> = {
  '1D': 24,
  '7D': 7 * 24,
  '30D': 30 * 24,
  '6M': 183 * 24,
};
const BTC_SYMBOL = 'BTCUSDT';
const BTC_LAST_POSITION_LEVERAGE_OVERRIDE = 3;
const BITGET_LIVE_POSITION_HISTORY_HOURS = 89 * 24;
const CAPITAL_TRANSACTION_CACHE_TTL_SECONDS = 60 * 60;

// Read-through KV cache. The summary is expensive to build (per-symbol Bitget /
// Capital calls + decision history), and swing data only changes at the hourly
// cron tick — so we cache it for a long window and let the analyze cron bust it
// (invalidateSwingSummaryCache) whenever a new decision is recorded. Result: fresh
// right after each tick, served from KV in between. The active symbol stays live
// via the separate /live-price endpoint, so a long TTL here costs no live-ness.
// Bypass with ?fresh=1.
const SUMMARY_CACHE_TTL_SECONDS = (() => {
  const n = Number(process.env.SWING_DASHBOARD_SUMMARY_TTL_SECONDS);
  return Number.isFinite(n) && n >= 0 ? n : 3600;
})();

type SummaryPayload = { symbols: string[]; data: SummaryEntry[]; range: SummaryRangeKey };
type CachedSummary = { payload: SummaryPayload; generatedAtMs: number };

const scalePct = (value: number | null | undefined, factor: number): number | null | undefined => {
  if (typeof value !== 'number') return value;
  return value * factor;
};

function mergePositionWindows(persisted: PositionWindow[], live: PositionWindow[]): PositionWindow[] {
  const byId = new Map<string, PositionWindow>();
  for (const w of persisted) {
    byId.set(String(w.id || `${w.symbol}-${w.entryTimestamp ?? 'nots'}`), w);
  }
  for (const w of live) {
    byId.set(String(w.id || `${w.symbol}-${w.entryTimestamp ?? 'nots'}`), w);
  }
  return Array.from(byId.values()).sort(
    (a, b) => Number(a.entryTimestamp ?? a.exitTimestamp ?? 0) - Number(b.entryTimestamp ?? b.exitTimestamp ?? 0),
  );
}

function mergeCapitalPositionWindows(windows: PositionWindow[]): PositionWindow[] {
  const sorted = windows
    .slice()
    .sort((a, b) => Number(a.exitTimestamp ?? a.entryTimestamp ?? 0) - Number(b.exitTimestamp ?? b.entryTimestamp ?? 0));
  const merged: PositionWindow[] = [];
  for (const window of sorted) {
    const ts = Number(window.exitTimestamp ?? window.entryTimestamp ?? 0);
    const match = merged.find((row) => {
      const rowTs = Number(row.exitTimestamp ?? row.entryTimestamp ?? 0);
      return (
        normalizeCapitalSymbolKey(row.symbol) === normalizeCapitalSymbolKey(window.symbol) &&
        Number.isFinite(rowTs) &&
        Number.isFinite(ts) &&
        Math.abs(rowTs - ts) <= 5 * 60 * 1000
      );
    });
    if (!match) {
      merged.push({ ...window });
      continue;
    }
    match.id = `${match.id}|${window.id}`;
    match.entryTimestamp = match.entryTimestamp ?? window.entryTimestamp ?? null;
    match.exitTimestamp = match.exitTimestamp ?? window.exitTimestamp ?? null;
    match.entryPrice = match.entryPrice ?? window.entryPrice ?? null;
    match.exitPrice = match.exitPrice ?? window.exitPrice ?? null;
    match.side = match.side ?? window.side ?? null;
    match.pnlNet = match.pnlNet ?? window.pnlNet ?? null;
    match.pnlGross = match.pnlGross ?? window.pnlGross ?? null;
    match.pnlPct = match.pnlPct ?? window.pnlPct ?? null;
    match.pnlGrossPct = match.pnlGrossPct ?? window.pnlGrossPct ?? null;
    match.notional = match.notional ?? window.notional ?? null;
    match.leverage = match.leverage ?? window.leverage ?? null;
  }
  return merged;
}

function finiteNumber(value: unknown): number | null {
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

function positiveNumber(value: unknown): number | null {
  const n = finiteNumber(value);
  return n !== null && n > 0 ? n : null;
}

function derivePnlPctFromNetExposure(window: PositionWindow): number | null {
  const pnlNet = finiteNumber(window.pnlNet);
  const notional = positiveNumber(window.notional);
  if (pnlNet === null || notional === null) return null;
  const leverage = positiveNumber(window.leverage);
  const basis = leverage !== null ? notional / leverage : notional;
  return basis > 0 ? (pnlNet / basis) * 100 : null;
}

function withDerivedPnlPct(window: PositionWindow): PositionWindow {
  const derivedPct = derivePnlPctFromNetExposure(window);
  if (derivedPct === null) return window;
  const existingPct = finiteNumber(window.pnlPct);
  const existingGrossPct = finiteNumber(window.pnlGrossPct);
  const existingPctLooksPlaceholder =
    existingPct !== null &&
    Math.abs(existingPct) < 0.005 &&
    finiteNumber(window.pnlNet) !== null &&
    Math.abs(finiteNumber(window.pnlNet) as number) > 0.005;
  const existingGrossPctLooksPlaceholder =
    existingGrossPct !== null &&
    Math.abs(existingGrossPct) < 0.005 &&
    finiteNumber(window.pnlGross ?? window.pnlNet) !== null &&
    Math.abs(finiteNumber(window.pnlGross ?? window.pnlNet) as number) > 0.005;
  return {
    ...window,
    pnlPct: existingPct !== null && !existingPctLooksPlaceholder ? existingPct : derivedPct,
    pnlGrossPct: existingGrossPct !== null && !existingGrossPctLooksPlaceholder ? existingGrossPct : derivedPct,
  };
}

function normalizeCapitalSymbolKey(value: unknown): string {
  return String(value || '')
    .trim()
    .toUpperCase()
    .replace(/[^A-Z0-9]/g, '');
}

function capitalTransactionToWindow(row: CapitalTradeTransactionRow): PositionWindow | null {
  const ts = finiteNumber(row.dateUtcMs);
  if (ts === null || ts <= 0) return null;
  const status = String(row.status || '').trim().toUpperCase();
  if (status && status !== 'PROCESSED') return null;
  const type = String(row.transactionType || '').trim().toUpperCase();
  if (type && type !== 'TRADE') return null;
  const note = String(row.note || '').trim().toLowerCase();
  if (note && !note.includes('closed')) return null;
  const symbol = String(row.instrumentName || '').trim().toUpperCase();
  if (!symbol) return null;
  const pnlNet = finiteNumber(row.pnlNet);
  if (pnlNet === null) return null;
  const reference = String(row.reference || '').trim() || `${symbol}-${Math.floor(ts)}`;
  return {
    id: `capital-tx:${reference}:${Math.floor(ts)}`,
    symbol,
    side: null,
    entryTimestamp: null,
    exitTimestamp: ts,
    entryPrice: null,
    exitPrice: null,
    pnlNet,
    pnlGross: pnlNet,
    pnlPct: null,
    pnlGrossPct: null,
    notional: null,
    leverage: null,
  };
}

function enrichCapitalWindowFromHistory(window: PositionWindow, history: any[]): PositionWindow {
  if (window.entryTimestamp) return window;
  const exitTs = Number(window.exitTimestamp);
  if (!Number.isFinite(exitTs) || exitTs <= 0) return window;
  const priorEntries = (history || [])
    .filter((entry) => {
      const ts = Number(entry?.timestamp);
      if (!Number.isFinite(ts) || ts <= 0 || ts > exitTs) return false;
      const action = String(entry?.aiDecision?.action || '').toUpperCase();
      const placed = entry?.execResult?.placed === true;
      return placed && (action === 'BUY' || action === 'SELL');
    })
    .sort((a, b) => Number(b.timestamp) - Number(a.timestamp));
  const entry = priorEntries[0];
  if (!entry) return window;
  const action = String(entry?.aiDecision?.action || '').toUpperCase();
  const entryPrice =
    finiteNumber(entry?.snapshot?.positionContext?.entry_price) ??
    finiteNumber(entry?.snapshot?.price) ??
    window.entryPrice ??
    null;
  const notional =
    positiveNumber(window.notional) ??
    positiveNumber(entry?.execResult?.notionalUsd) ??
    positiveNumber(entry?.execResult?.notionalUSDT) ??
    positiveNumber(entry?.execResult?.orderNotionalUsd) ??
    positiveNumber(entry?.snapshot?.gates?.notionalUSDT) ??
    positiveNumber(entry?.snapshot?.gates?.notionalUsd) ??
    null;
  return {
    ...window,
    entryTimestamp: Number(entry.timestamp),
    entryPrice,
    side: window.side ?? (action === 'BUY' ? 'long' : action === 'SELL' ? 'short' : null),
    leverage: window.leverage ?? finiteNumber(entry?.execResult?.leverage) ?? finiteNumber(entry?.aiDecision?.leverage),
    notional,
  };
}

function capitalTransactionCacheKey(range: SummaryRangeKey, nowMs: number): string {
  const hourBucket = Math.floor(nowMs / (60 * 60 * 1000));
  return `swing:capital:trade-windows:v1:${range}:${hourBucket}`;
}

async function loadCapitalTradeWindowsForSummary(params: {
  range: SummaryRangeKey;
  fromMs: number;
  toMs: number;
}): Promise<PositionWindow[]> {
  const cacheKey = capitalTransactionCacheKey(params.range, params.toMs);
  const cached = await kvGetJson<PositionWindow[]>(cacheKey);
  if (Array.isArray(cached)) return cached;

  const transactions = await fetchCapitalTradeTransactions({
    fromTsMs: params.fromMs,
    toTsMs: params.toMs,
  });
  const windows = transactions
    .map(capitalTransactionToWindow)
    .filter((row): row is PositionWindow => row !== null);

  await Promise.all(
    windows.map((window) =>
      upsertSwingPosition('capital', {
        ...window,
        status: 'closed',
        leverageSource: null,
      }).catch((err) => {
        console.warn(`Could not persist Capital transaction ${window.id}:`, err);
      }),
    ),
  );

  await kvSetJson(cacheKey, windows, CAPITAL_TRANSACTION_CACHE_TTL_SECONDS);
  return windows;
}

function parseHistoryPnlPct(entry: any): number | null {
  const direct =
    finiteNumber(entry?.snapshot?.positionContext?.unrealized_pnl_pct) ??
    finiteNumber(entry?.snapshot?.positionContext?.pnlPct) ??
    finiteNumber(entry?.execResult?.pnlPct);
  if (direct !== null) return direct;
  const raw = String(entry?.snapshot?.positionContext?.currentPnl || entry?.execResult?.currentPnl || '');
  const match = raw.match(/-?\d+(\.\d+)?/);
  return match ? Number(match[0]) : null;
}

function capitalHistoryClosedWindows(history: any[], symbol: string, fromMs: number, toMs: number): PositionWindow[] {
  return (history || [])
    .map((entry): PositionWindow | null => {
      const ts = finiteNumber(entry?.timestamp);
      if (ts === null || ts < fromMs || ts > toMs) return null;
      const action = String(entry?.aiDecision?.action || '').toUpperCase();
      if (action !== 'CLOSE' && action !== 'REVERSE') return null;
      const exec = entry?.execResult || {};
      if (!(exec?.placed === true && (exec?.closed === true || exec?.reversed === true))) return null;
      const positionContext = entry?.snapshot?.positionContext || {};
      const pnlPct = parseHistoryPnlPct(entry);
      if (pnlPct === null) return null;
      const sideRaw = String(positionContext?.side || '').toLowerCase();
      const side = sideRaw === 'long' || sideRaw === 'short' ? sideRaw : null;
      const entryTsRaw = Date.parse(String(positionContext?.entry_ts || ''));
      const entryTimestamp = Number.isFinite(entryTsRaw) ? entryTsRaw : null;
      const id = String(exec?.orderId || exec?.clientOid || `${symbol}-${ts}`);
      return {
        id: `capital-history:${symbol}:${id}:${Math.floor(ts)}`,
        symbol,
        side,
        entryTimestamp,
        exitTimestamp: ts,
        entryPrice: finiteNumber(positionContext?.entry_price),
        exitPrice: finiteNumber(entry?.snapshot?.price),
        pnlNet: null,
        pnlGross: null,
        pnlPct,
        pnlGrossPct: pnlPct,
        notional: null,
        leverage: finiteNumber(exec?.leverage),
      };
    })
    .filter((row): row is PositionWindow => row !== null)
    .sort((a, b) => Number(a.entryTimestamp ?? a.exitTimestamp ?? 0) - Number(b.entryTimestamp ?? b.exitTimestamp ?? 0));
}

function applyClosedWindowSummary(params: {
  windows: PositionWindow[];
  fallbackNetUsd?: number | null;
  fallbackTradeCount?: number | null;
}) {
  const recentWindows = params.windows.map(withDerivedPnlPct);
  const lastWindows = recentWindows.slice(-14);
  const spark = lastWindows
    .map((w) => (Number.isFinite(w.pnlPct as number) ? (w.pnlPct as number) : null))
    .filter((v): v is number => typeof v === 'number');
  const grossPcts = recentWindows
    .map((w) => (Number.isFinite(w.pnlGrossPct as number) ? (w.pnlGrossPct as number) : null))
    .filter((v): v is number => typeof v === 'number');
  const netPcts = recentWindows
    .map((w) => (Number.isFinite(w.pnlPct as number) ? (w.pnlPct as number) : null))
    .filter((v): v is number => typeof v === 'number');
  const netUsd = recentWindows
    .map((w) => (Number.isFinite(w.pnlNet as number) ? (w.pnlNet as number) : null))
    .filter((v): v is number => typeof v === 'number');
  const sampledWindows = lastWindows.filter((w) => Number.isFinite(w.pnlPct as number));
  const wins = sampledWindows.filter((w) => (w.pnlPct as number) > 0);
  const losses = sampledWindows.filter((w) => (w.pnlPct as number) < 0);
  const lastWithLev = lastWindows
    .slice()
    .reverse()
    .find((w) => Number.isFinite(w.leverage as number));
  const lastWindow = recentWindows.length ? recentWindows[recentWindows.length - 1] : null;

  return {
    pnlSpark: spark.length ? spark : null,
    pnl7dGross: grossPcts.length ? grossPcts.reduce((a, b) => a + b, 0) : null,
    pnl7d: netPcts.length ? netPcts.reduce((a, b) => a + b, 0) : null,
    pnl7dNet: netUsd.length ? netUsd.reduce((a, b) => a + b, 0) : params.fallbackNetUsd ?? null,
    pnl7dTrades: recentWindows.length || params.fallbackTradeCount || 0,
    winRate: sampledWindows.length ? (wins.length / sampledWindows.length) * 100 : null,
    avgWinPct: wins.length ? wins.reduce((acc, w) => acc + (w.pnlPct as number), 0) / wins.length : null,
    avgLossPct: losses.length ? losses.reduce((acc, w) => acc + (w.pnlPct as number), 0) / losses.length : null,
    lastPositionLeverage:
      lastWithLev && Number.isFinite(lastWithLev.leverage as number) ? (lastWithLev.leverage as number) : null,
    lastPositionPnl:
      lastWindow && Number.isFinite(lastWindow.pnlPct as number) ? (lastWindow.pnlPct as number) : null,
    lastPositionDirection: lastWindow?.side ?? null,
  };
}

function resolveSummaryRange(raw: unknown): SummaryRangeKey {
  const normalized = String(raw || '')
    .trim()
    .toUpperCase();
  if (normalized === '1D') return '1D';
  if (normalized === '30D') return '30D';
  if (normalized === '6M') return '6M';
  return '7D';
}

// Compute the summary for a range and write it to KV. Shared by the HTTP handler
// (on cache miss) and the warm cron, so the two paths can never drift.
export async function buildAndCacheSwingSummary(range: SummaryRangeKey): Promise<CachedSummary> {
  const lookbackHours = SUMMARY_RANGE_LOOKBACK_HOURS[range];
  const configs = getCronSymbolConfigs();
  const symbols = configs.map((item) => item.symbol);
  const nowMs = Date.now();
  const windowFromMs = nowMs - lookbackHours * 60 * 60 * 1000;
  const hasForexCategory = configs.some((item) => item.category === 'forex');
  const forexEventsState = hasForexCategory ? await ensureForexEventsState(nowMs) : null;
  const hasCapitalSymbols = configs.some((item) => item.platform === 'capital');
  const capitalTradeWindows = hasCapitalSymbols
    ? await loadCapitalTradeWindowsForSummary({
        range,
        fromMs: windowFromMs,
        toMs: nowMs,
      }).catch((err) => {
        console.warn('Could not load Capital trade transactions for summary:', err);
        return [] as PositionWindow[];
      })
    : [];
  const capitalTradeWindowsBySymbol = new Map<string, PositionWindow[]>();
  for (const window of capitalTradeWindows) {
    const key = normalizeCapitalSymbolKey(window.symbol);
    if (!key) continue;
    const rows = capitalTradeWindowsBySymbol.get(key) ?? [];
    rows.push(window);
    capitalTradeWindowsBySymbol.set(key, rows);
  }

  const data: SummaryEntry[] = await Promise.all(
    configs.map(async (config) => {
      const symbol = config.symbol;
      const platform = config.platform;
      let category: string | null | undefined = config.category;

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
      let forexEventContext: any | null = null;
      let lastWasAiCall = false;

      try {
        const history = await loadDecisionHistory(symbol, 120, platform);
        const latest = history[0];
        // Was the most recent decision (history is newest-first) a real AI call,
        // or a calm-market / below-min-signal-strength pre-AI skip?
        lastWasAiCall = latest
          ? (latest.aiDecision as any)?.decision_source !== 'pre_ai_skip' &&
            !(latest.aiDecision as any)?.promptSkipped
          : false;
        if (latest) {
          category =
            typeof latest.category === 'string'
              ? latest.category
              : typeof latest.snapshot?.category === 'string'
              ? latest.snapshot.category
              : config.category;
          lastNewsSource =
            typeof latest.newsSource === 'string'
              ? latest.newsSource
              : typeof latest.snapshot?.newsSource === 'string'
              ? latest.snapshot.newsSource
              : config.newsSource;

          if (category === 'forex' && forexEventsState) {
            forexEventContext = buildForexEventContext({
              symbol,
              instrumentId:
                typeof latest.instrumentId === 'string'
                  ? latest.instrumentId
                  : typeof latest.snapshot?.instrumentId === 'string'
                  ? latest.snapshot.instrumentId
                  : null,
              state: forexEventsState,
              nowMs,
            });
          }
        }
        if (!forexEventContext && category === 'forex' && forexEventsState) {
          forexEventContext = buildForexEventContext({
            symbol,
            state: forexEventsState,
            nowMs,
          });
        }
        const capturedLevs = extractCapturedLeverages(history);
        const leverageFromHistory = capturedLevs[0]?.leverage ?? null;

        const fetchRealizedRoi = platform === 'capital' ? fetchCapitalRealizedRoi : fetchBitgetRealizedRoi;
        const fetchPositionInfo = platform === 'capital' ? fetchCapitalPositionInfo : fetchBitgetPositionInfo;

        const roiRes = await fetchRealizedRoi(symbol, lookbackHours);
        pnl7dNet = Number.isFinite(roiRes.roi as number) ? (roiRes.roi as number) : null;
        pnl7dTrades = roiRes.count;
        lastPositionPnl = Number.isFinite(roiRes.lastNetPct as number) ? (roiRes.lastNetPct as number) : null;
        lastPositionDirection = roiRes.lastSide ?? null;

        if (platform === 'capital') {
          try {
            const persistedWindows = await loadClosedSwingPositions({
              platform,
              symbol,
              fromMs: windowFromMs,
              toMs: nowMs,
              limit: 5000,
            });
            const historyWindows = capitalHistoryClosedWindows(history, symbol, windowFromMs, nowMs);
            const transactionWindows = (capitalTradeWindowsBySymbol.get(normalizeCapitalSymbolKey(symbol)) ?? []).map((window) =>
              enrichCapitalWindowFromHistory(window, history),
            );
            await Promise.all(
              transactionWindows
                .filter((window) => window.entryTimestamp || window.exitTimestamp)
                .map((window) =>
                  upsertSwingPosition('capital', {
                    ...window,
                    status: 'closed',
                    leverageSource: window.leverage ? 'captured' : null,
                  }).catch((err) => {
                    console.warn(`Could not persist enriched Capital transaction ${window.id}:`, err);
                  }),
                ),
            );
            const recentWindows = mergeCapitalPositionWindows([
              ...mergePositionWindows(persistedWindows, historyWindows),
              ...transactionWindows,
            ]).map(withDerivedPnlPct);
            if (recentWindows.length) {
              const summary = applyClosedWindowSummary({
                windows: recentWindows,
                fallbackNetUsd: pnl7dNet,
                fallbackTradeCount: pnl7dTrades,
              });
              const capitalPctLooksPlaceholder =
                typeof summary.pnl7d === 'number' &&
                Math.abs(summary.pnl7d) < 0.005 &&
                typeof summary.pnl7dNet === 'number' &&
                Math.abs(summary.pnl7dNet) > 0.005;
              pnlSpark = summary.pnlSpark;
              pnl7dGross = summary.pnl7dGross;
              pnl7d = capitalPctLooksPlaceholder ? null : summary.pnl7d;
              pnl7dNet = summary.pnl7dNet;
              pnl7dTrades = summary.pnl7dTrades;
              winRate = summary.winRate;
              avgWinPct = summary.avgWinPct;
              avgLossPct = summary.avgLossPct;
              lastPositionPnl = capitalPctLooksPlaceholder ? null : summary.lastPositionPnl ?? lastPositionPnl;
              lastPositionDirection = summary.lastPositionDirection ?? lastPositionDirection;
              lastPositionLeverage = summary.lastPositionLeverage ?? leverageFromHistory ?? null;
            }
          } catch (err) {
            console.warn(`Could not load Capital persisted PnL for ${symbol}:`, err);
          }
        } else {
          try {
            const liveWindows = await fetchRecentPositionWindows(symbol, lookbackHours, capturedLevs);
            // write-through: mirror closed positions to Postgres with captured leverage
            await syncSwingClosedPositions(platform, liveWindows, capturedLevs);
            const persistedWindows =
              lookbackHours > BITGET_LIVE_POSITION_HISTORY_HOURS
                ? await loadClosedSwingPositions({
                    platform,
                    symbol,
                    fromMs: nowMs - lookbackHours * 60 * 60 * 1000,
                    toMs: nowMs,
                    limit: 5000,
                  })
                : [];
            const recentWindows = persistedWindows.length
              ? mergePositionWindows(persistedWindows, liveWindows)
              : liveWindows;
            if (recentWindows.length) {
              const summary = applyClosedWindowSummary({
                windows: recentWindows,
                fallbackNetUsd: pnl7dNet,
                fallbackTradeCount: pnl7dTrades,
              });
              pnlSpark = summary.pnlSpark;
              pnl7dGross = summary.pnl7dGross;
              pnl7d = summary.pnl7d;
              pnl7dNet = summary.pnl7dNet;
              pnl7dTrades = summary.pnl7dTrades;
              winRate = summary.winRate;
              avgWinPct = summary.avgWinPct;
              avgLossPct = summary.avgLossPct;
              lastPositionPnl = summary.lastPositionPnl ?? lastPositionPnl;
              lastPositionDirection = summary.lastPositionDirection ?? lastPositionDirection;
              lastPositionLeverage = summary.lastPositionLeverage ?? leverageFromHistory ?? null;
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
        category,
        lastPlatform: platform,
        lastNewsSource,
        forexEventContext,
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
        lastWasAiCall,
        winRate,
        avgWinPct,
        avgLossPct,
      };
    }),
  );

  const payload: SummaryPayload = { symbols, data, range };
  const generatedAtMs = Date.now();

  // write-through: materialize the blob so subsequent polls within the window
  // are served from KV instead of recomputing. Best-effort — never fail on a
  // cache write.
  if (SUMMARY_CACHE_TTL_SECONDS > 0) {
    try {
      await kvSetJson<CachedSummary>(swingSummaryCacheKey(range), { payload, generatedAtMs }, SUMMARY_CACHE_TTL_SECONDS);
    } catch (err) {
      console.warn('summary cache write failed:', err);
    }
  }

  return { payload, generatedAtMs };
}

const ALL_SUMMARY_RANGES: SummaryRangeKey[] = ['1D', '7D', '30D', '6M'];

// Rebuild every range blob and write them to KV. Ranges run concurrently so the
// warm cron's wall-clock ≈ a single fan-out (matching what the analyze crons
// already tolerate). Best-effort per range — a failure just falls back to an
// on-demand rebuild on the next dashboard load.
export async function warmAllSwingSummaries(): Promise<Array<{ range: SummaryRangeKey; ok: boolean; symbols: number }>> {
  return Promise.all(
    ALL_SUMMARY_RANGES.map(async (range) => {
      try {
        const { payload } = await buildAndCacheSwingSummary(range);
        return { range, ok: true, symbols: payload.data.length };
      } catch (err) {
        console.warn(`warm summary failed for ${range}:`, err);
        return { range, ok: false, symbols: 0 };
      }
    }),
  );
}

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
  if (req.method !== 'GET') {
    return res.status(405).json({ error: 'Method Not Allowed', message: 'Use GET' });
  }
  if (!requireAdminAccess(req, res)) return;

  const rangeParam = Array.isArray(req.query.range) ? req.query.range[0] : req.query.range;
  const range = resolveSummaryRange(rangeParam);

  const freshParam = Array.isArray(req.query.fresh) ? req.query.fresh[0] : req.query.fresh;
  const bypassCache = freshParam === '1' || freshParam === 'true';

  if (!bypassCache && SUMMARY_CACHE_TTL_SECONDS > 0) {
    try {
      const cached = await kvGetJson<CachedSummary>(swingSummaryCacheKey(range));
      if (cached?.payload) {
        return res.status(200).json({ ...cached.payload, cached: true, generatedAtMs: cached.generatedAtMs });
      }
    } catch (err) {
      console.warn('summary cache read failed; computing fresh:', err);
    }
  }

  const { payload, generatedAtMs } = await buildAndCacheSwingSummary(range);
  return res.status(200).json({ ...payload, cached: false, generatedAtMs });
}
