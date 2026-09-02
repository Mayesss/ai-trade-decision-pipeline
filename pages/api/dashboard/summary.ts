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
} from '../../../lib/capital';
import {
  capitalTransactionToWindow,
  enrichCapitalWindowFromHistory,
  foldCapitalTrimChunks,
  mergeCapitalPositionWindows,
  normalizeCapitalSymbolKey,
  withDerivedPnlPct,
  type FoldedCapitalWindow,
} from '../../../lib/swing/capitalWindows';
import { loadDecisionHistory, extractCapturedLeverages, type DecisionHistoryEntry } from '../../../lib/history';
import { readSwingLastScan, readSwingLastScanMany, type LastScanMarker } from '../../../lib/swing/lastScan';
import { syncSwingClosedPositions, mergePositionWindows } from '../../../lib/swing/sync';
import { loadClosedSwingPositions, upsertSwingPosition, listSwingPendingEntryThreads } from '../../../lib/swing/pg';
import { kvGetJson, kvSetJson } from '../../../lib/kv';
import { requireAdminAccess } from '../../../lib/admin';
import { getCronSymbolConfigs } from '../../../lib/symbolRegistry';
import type { AnalysisPlatform } from '../../../lib/platform';
import { buildForexEventContext, ensureForexEventsState, type ForexEventContext } from '../../../lib/swing/forexEvents';
import { swingSummaryCacheKey } from '../../../lib/swing/summaryCache';
import type { PositionWindow } from '../../../lib/analytics';

type SummaryEntry = {
  symbol: string;
  category?: string | null;
  lastPlatform: AnalysisPlatform;
  lastNewsSource?: string | null;
  forexEventContext?: ForexEventContext | null;
  pnl7d?: number | null;
  pnl7dWithOpen?: number | null;
  pnl7dNet?: number | null;
  pnl7dGross?: number | null;
  pnl7dTrades?: number | null;
  pnlSpark?: number[] | null;
  // Per-Berlin-calendar-day closed PnL over the range's windows: venue-cash
  // net (USDT for Bitget, € for Capital — the client folds currencies) +
  // trade count, keyed YYYY-MM-DD. Days without closes are absent. Feeds the
  // dashboard's week-calendar strip.
  pnlDaily?: Array<{ day: string; net: number | null; trades: number }> | null;
  // A resting entry order is live on the venue (ai_threads status
  // 'pending_entry') — ranks the symbol pill between open positions and
  // fresh AI decisions.
  pendingEntry?: boolean;
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
  // Timestamp of the freshest real AI call in the history window (pre-AI skips
  // don't count) — drives the recency-sorted symbol pill order.
  lastAiDecisionTs?: number | null;
  // Its action (BUY/SELL/CLOSE/HOLD/…) — colors the pill's decision dot.
  lastAiDecisionAction?: string | null;
  // Flat-HOLD cooldown that call armed, if any — draws clock hands inside the
  // pill's decision dot.
  lastAiDecisionCooldownMinutes?: number | null;
  // Whether the venue was closed at the most recent decision (Capital reports
  // marketStatus != TRADEABLE → analyze.ts persists a capital_market_closed
  // pre-AI skip). Crons run hourly, so this tracks "market currently closed".
  // Drives the deactivated look on the symbol tab.
  marketClosed?: boolean;
  // Most recent analyze cron scan of this symbol (KV marker, written on EVERY
  // automation tick including unpersisted quarter-tick skips). Decision history
  // only shows meaningful outcomes; this shows liveness — and, when the scan
  // ended in an unpersisted quarter-tick skip, which gate stopped it and why.
  lastScanAt?: number | null;
  lastScanStage?: string | null;
  lastScanReason?: string | null;
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

function finiteNumber(value: unknown): number | null {
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

// mergeCapitalPositionWindows / withDerivedPnlPct / enrichCapitalWindowFromHistory /
// capitalTransactionToWindow / normalizeCapitalSymbolKey now live in
// lib/swing/capitalWindows.ts, shared with the chart overlay endpoint and the
// analyze-tick bracket-close reconcile.

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

function parseHistoryPnlPct(entry: DecisionHistoryEntry): number | null {
  const positionContext = (entry?.snapshot?.positionContext ?? null) as {
    unrealized_pnl_pct_on_margin?: unknown;
    unrealized_pnl_pct?: unknown;
    pnlPct?: unknown;
    currentPnl?: unknown;
  } | null;
  const direct =
    finiteNumber(positionContext?.unrealized_pnl_pct_on_margin) ??
    // legacy key on rows written before the on-margin rename
    finiteNumber(positionContext?.unrealized_pnl_pct) ??
    finiteNumber(positionContext?.pnlPct) ??
    finiteNumber(entry?.execResult?.pnlPct);
  if (direct !== null) return direct;
  const raw = String(positionContext?.currentPnl || entry?.execResult?.currentPnl || '');
  const match = raw.match(/-?\d+(\.\d+)?/);
  return match ? Number(match[0]) : null;
}

function capitalHistoryClosedWindows(history: DecisionHistoryEntry[], symbol: string, fromMs: number, toMs: number): PositionWindow[] {
  return (history || [])
    .map((entry): PositionWindow | null => {
      const ts = finiteNumber(entry?.timestamp);
      if (ts === null || ts < fromMs || ts > toMs) return null;
      const action = String(entry?.aiDecision?.action || '').toUpperCase();
      if (action !== 'CLOSE' && action !== 'REVERSE') return null;
      const exec = entry?.execResult;
      if (!(exec?.placed === true && (exec?.closed === true || exec?.reversed === true))) return null;
      const positionContext = entry?.snapshot?.positionContext;
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

// Berlin calendar-day key (YYYY-MM-DD — en-CA renders ISO order) for the
// daily PnL buckets; the dashboard's clock is Europe/Berlin throughout.
const BERLIN_DAY_FORMAT = new Intl.DateTimeFormat('en-CA', {
  timeZone: 'Europe/Berlin',
  year: 'numeric',
  month: '2-digit',
  day: '2-digit',
});

// Bucket closed windows into per-day venue-cash nets (keyed by exit day).
// `net` stays null when no window in the bucket carried a cash figure (e.g.
// Capital history rows that only have a percent). Folded Capital windows
// (trimmed positions) split back into their chunks here: cash lands on the day
// each chunk actually realized, but the position counts as ONE trade, on its
// final close day — a Monday trim must not move Monday's cash to Wednesday,
// nor count as a Monday trade.
function bucketWindowsByDay(
  windows: PositionWindow[],
): Array<{ day: string; net: number | null; trades: number }> | null {
  const byDay = new Map<string, { net: number; hasNet: boolean; trades: number }>();
  const credit = (ts: number, pnlNet: number | null, countTrade: boolean) => {
    if (!Number.isFinite(ts) || ts <= 0) return;
    const day = BERLIN_DAY_FORMAT.format(new Date(ts));
    const bucket = byDay.get(day) ?? { net: 0, hasNet: false, trades: 0 };
    if (countTrade) bucket.trades += 1;
    if (Number.isFinite(pnlNet as number)) {
      bucket.net += pnlNet as number;
      bucket.hasNet = true;
    }
    byDay.set(day, bucket);
  };
  for (const window of windows) {
    const chunks = (window as FoldedCapitalWindow).chunks;
    if (chunks?.length) {
      chunks.forEach((chunk, idx) => credit(chunk.exitTimestamp, chunk.pnlNet, idx === chunks.length - 1));
      continue;
    }
    credit(Number(window.exitTimestamp ?? window.entryTimestamp), Number.isFinite(window.pnlNet as number) ? (window.pnlNet as number) : null, true);
  }
  if (!byDay.size) return null;
  return Array.from(byDay.entries())
    .map(([day, bucket]) => ({ day, net: bucket.hasNet ? bucket.net : null, trades: bucket.trades }))
    .sort((a, b) => (a.day < b.day ? -1 : 1));
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
    pnlDaily: bucketWindowsByDay(recentWindows),
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

// Decision rows read per symbol. Deliberately range-independent: the widest
// range needs no more than this, and every range derives its numbers from the
// same rows — which is what makes the prefetch below sound.
const HISTORY_ROWS_PER_SYMBOL = 120;

// The per-symbol KV reads a summary build needs, hoisted so the four range
// blobs can share one set. Keyed by `platform:symbol`.
export type SummarySymbolReads = Map<string, { history: DecisionHistoryEntry[]; lastScan: LastScanMarker | null }>;

function symbolReadKey(platform: string, symbol: string): string {
  return `${String(platform || 'bitget').toLowerCase()}:${String(symbol).toUpperCase()}`;
}

// Read every symbol's decision history and scan marker ONCE. The four range
// blobs ask for identical rows (HISTORY_ROWS_PER_SYMBOL does not vary by range),
// so building them independently re-issued the same ~375 KV commands three
// extra times per warm — the single largest line on the Upstash bill. Markers
// come back in one MGET on top of that.
export async function prefetchSummarySymbolReads(): Promise<SummarySymbolReads> {
  const configs = getCronSymbolConfigs();
  const [histories, markers] = await Promise.all([
    Promise.all(
      configs.map((config) =>
        loadDecisionHistory(config.symbol, HISTORY_ROWS_PER_SYMBOL, config.platform).catch(() => [] as DecisionHistoryEntry[]),
      ),
    ),
    readSwingLastScanMany(configs.map((config) => ({ platform: config.platform, symbol: config.symbol }))).catch(
      () => configs.map(() => null),
    ),
  ]);
  const reads: SummarySymbolReads = new Map();
  configs.forEach((config, i) => {
    reads.set(symbolReadKey(config.platform, config.symbol), {
      history: histories[i] ?? [],
      lastScan: markers[i] ?? null,
    });
  });
  return reads;
}

// Compute the summary for a range and write it to KV. Shared by the HTTP handler
// (on cache miss) and the warm cron, so the two paths can never drift. `prefetched`
// lets the warm reuse one set of per-symbol reads across all four ranges; the
// on-demand path passes nothing and reads for itself.
export async function buildAndCacheSwingSummary(
  range: SummaryRangeKey,
  prefetched?: SummarySymbolReads,
): Promise<CachedSummary> {
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

  // Resting entries, one query for all symbols. Best-effort: a miss
  // just leaves the pills unflagged until the next rebuild.
  const pendingEntryKeys = new Set(
    (await listSwingPendingEntryThreads().catch(() => [])).map(
      (row) => `${String(row.platform).toLowerCase()}:${String(row.symbol).toUpperCase()}`,
    ),
  );

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
      let pnlDaily: Array<{ day: string; net: number | null; trades: number }> | null | undefined = null;
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
      let forexEventContext: ForexEventContext | null = null;
      let lastWasAiCall = false;
      let lastAiDecisionTs: number | null = null;
      let lastAiDecisionAction: string | null = null;
      let lastAiDecisionCooldownMinutes: number | null = null;
      let marketClosed = false;
      let lastScanAt: number | null = null;
      let lastScanStage: string | null = null;
      let lastScanReason: string | null = null;

      try {
        const shared = prefetched?.get(symbolReadKey(platform, symbol));
        const [history, lastScan] = shared
          ? [shared.history, shared.lastScan]
          : await Promise.all([
              loadDecisionHistory(symbol, HISTORY_ROWS_PER_SYMBOL, platform),
              readSwingLastScan(platform, symbol),
            ]);
        lastScanAt = lastScan?.ts ?? null;
        lastScanStage = lastScan?.stage ?? null;
        lastScanReason = lastScan?.reason ?? null;
        const latest = history[0];
        const isRealAiCall = (entry: (typeof history)[number]): boolean =>
          entry.aiDecision?.decision_source !== 'pre_ai_skip' &&
          !entry.aiDecision?.promptSkipped;
        // Was the most recent decision (history is newest-first) a real AI call,
        // or a calm-market / below-min-signal-strength pre-AI skip?
        lastWasAiCall = latest ? isRealAiCall(latest) : false;
        // …and when did the AI last actually look at this symbol, regardless of
        // how many skip rows landed since — plus what it decided.
        const latestAiCall = history.find(isRealAiCall);
        lastAiDecisionTs = latestAiCall?.timestamp ?? null;
        lastAiDecisionAction = String(latestAiCall?.aiDecision?.action || '').toUpperCase() || null;
        const cooldownMin = Number(latestAiCall?.aiDecision?.cooldown_minutes);
        lastAiDecisionCooldownMinutes = Number.isFinite(cooldownMin) && cooldownMin > 0 ? cooldownMin : null;
        // Venue closed at the last cron tick — analyze.ts skips before the AI
        // with skipStage === 'capital_market_closed'. Crypto (bitget) never
        // hits this gate, so it stays open 24/7.
        marketClosed = latest?.aiDecision?.skipStage === 'capital_market_closed';
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
            // Fold same-position trim chunks into one window per position:
            // trades/winRate/avg count positions (not realized chunks), while
            // bucketWindowsByDay splits the fold back out so daily cash stays
            // on the day each chunk realized.
            const recentWindows = foldCapitalTrimChunks(
              mergeCapitalPositionWindows([
                ...mergePositionWindows(persistedWindows, historyWindows),
                ...transactionWindows,
              ]).map(withDerivedPnlPct),
            ).windows;
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
              pnlDaily = summary.pnlDaily;
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
              pnlDaily = summary.pnlDaily;
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

        // Legacy repair only: old BTC rows predate leverage capture and were
        // recorded as if 1x while the venue traded 3x — rescale those. Rows
        // with a real captured leverage (> 1) are authoritative and must NOT
        // be rescaled (entries now run 5–10x).
        if (
          platform === 'bitget' &&
          symbol.toUpperCase() === BTC_SYMBOL &&
          !(Number.isFinite(lastPositionLeverage as number) && (lastPositionLeverage as number) > 1)
        ) {
          const scale = BTC_LAST_POSITION_LEVERAGE_OVERRIDE;
          lastPositionPnl = scalePct(lastPositionPnl, scale);
          pnl7d = scalePct(pnl7d, scale);
          pnl7dGross = scalePct(pnl7dGross, scale);
          avgWinPct = scalePct(avgWinPct, scale);
          avgLossPct = scalePct(avgLossPct, scale);
          pnlSpark = Array.isArray(pnlSpark) ? pnlSpark.map((v) => (typeof v === 'number' ? v * scale : v)) : pnlSpark;
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
        pnlDaily,
        pendingEntry: pendingEntryKeys.has(
          `${String(platform).toLowerCase()}:${symbol.toUpperCase()}`,
        ),
        openPnl,
        openDirection,
        openLeverage,
        openEntryPrice,
        lastPositionPnl,
        lastPositionDirection,
        lastPositionLeverage,
        lastWasAiCall,
        lastAiDecisionTs,
        lastAiDecisionAction,
        lastAiDecisionCooldownMinutes,
        marketClosed,
        lastScanAt,
        lastScanStage,
        lastScanReason,
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
  // One set of per-symbol reads for all four ranges (see prefetchSummarySymbolReads).
  // Best-effort: an empty prefetch just sends each range back to reading for itself.
  const prefetched = await prefetchSummarySymbolReads().catch((err) => {
    console.warn('summary symbol prefetch failed; ranges will read individually:', err);
    return undefined;
  });
  return Promise.all(
    ALL_SUMMARY_RANGES.map(async (range) => {
      try {
        const { payload } = await buildAndCacheSwingSummary(range, prefetched);
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
        // The blob can be up to an hour old and quarter-tick scans don't
        // invalidate it (they persist no decision rows) — so overlay the live
        // last-scan markers, which are the whole point of scan freshness.
        // ONE batched KV read for the whole universe (it was a GET per symbol,
        // i.e. 26 commands to serve a cached blob); failures keep the cached values.
        try {
          const rows = Array.isArray(cached.payload.data) ? cached.payload.data : [];
          const markers = await readSwingLastScanMany(
            rows.map((row) => ({ platform: String(row.lastPlatform || 'bitget'), symbol: row.symbol })),
          ).catch(() => rows.map(() => null));
          markers.forEach((marker, i) => {
            if (!marker) return;
            rows[i].lastScanAt = marker.ts;
            rows[i].lastScanStage = marker.stage ?? null;
            rows[i].lastScanReason = marker.reason ?? null;
          });
        } catch (err) {
          console.warn('last-scan overlay on cached summary failed:', err);
        }
        return res.status(200).json({ ...cached.payload, cached: true, generatedAtMs: cached.generatedAtMs });
      }
    } catch (err) {
      console.warn('summary cache read failed; computing fresh:', err);
    }
  }

  const { payload, generatedAtMs } = await buildAndCacheSwingSummary(range);
  return res.status(200).json({ ...payload, cached: false, generatedAtMs });
}
