import type { PositionWindow } from '../analytics';
import {
  fetchCapitalTradeTransactions,
  resolveCapitalEpic,
  type CapitalTradeTransactionRow,
} from '../capital';
import { loadDecisionHistory } from '../history';
import { upsertSwingPosition } from './pg';

const MATCH_WINDOW_MS = 5 * 60 * 1000;

function finite(value: unknown): number | null {
  if (value === null || value === undefined || value === '') return null;
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

function positive(value: unknown): number | null {
  const n = finite(value);
  return n !== null && n > 0 ? n : null;
}

export function normalizeCapitalInstrument(value: unknown): string {
  return String(value || '').trim().toUpperCase().replace(/[^A-Z0-9]/g, '');
}

export function deriveCapitalPnlPct(window: PositionWindow): number | null {
  const pnlNet = finite(window.pnlNet);
  const notional = positive(window.notional);
  const leverage = positive(window.leverage);
  if (pnlNet === null || notional === null || leverage === null) return null;
  const marginBasis = notional / leverage;
  return marginBasis > 0 ? (pnlNet / marginBasis) * 100 : null;
}

export function capitalTransactionToPositionWindow(
  row: CapitalTradeTransactionRow,
  symbol: string,
): PositionWindow | null {
  const ts = finite(row.dateUtcMs);
  if (ts === null || ts <= 0) return null;
  const status = String(row.status || '').trim().toUpperCase();
  if (status && status !== 'PROCESSED') return null;
  const type = String(row.transactionType || '').trim().toUpperCase();
  if (type && type !== 'TRADE') return null;
  const note = String(row.note || '').trim().toLowerCase();
  if (note && !note.includes('closed')) return null;
  const pnlNet = finite(row.pnlNet);
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

export function enrichCapitalCloseFromHistory(
  window: PositionWindow,
  history: any[],
): PositionWindow {
  const exitTs = finite(window.exitTimestamp);
  if (exitTs === null) return window;
  const entry = (history || [])
    .filter((row) => {
      const ts = finite(row?.timestamp);
      const action = String(row?.aiDecision?.action || '').toUpperCase();
      return ts !== null && ts <= exitTs && row?.execResult?.placed === true &&
        (action === 'BUY' || action === 'SELL');
    })
    .sort((a, b) => Number(b.timestamp) - Number(a.timestamp))[0];
  if (!entry) return window;
  const action = String(entry.aiDecision?.action || '').toUpperCase();
  const notional =
    positive(entry?.execResult?.notionalUsd) ??
    positive(entry?.execResult?.notionalUSDT) ??
    positive(entry?.execResult?.orderNotionalUsd) ??
    positive(entry?.snapshot?.gates?.notionalUSDT) ??
    positive(entry?.snapshot?.gates?.notionalUsd);
  const leverage = positive(entry?.execResult?.leverage) ?? positive(entry?.aiDecision?.leverage);
  const enriched: PositionWindow = {
    ...window,
    entryTimestamp: finite(entry.timestamp),
    entryPrice:
      positive(entry?.snapshot?.positionContext?.entry_price) ??
      positive(entry?.snapshot?.price) ??
      window.entryPrice ??
      null,
    side: window.side ?? (action === 'BUY' ? 'long' : action === 'SELL' ? 'short' : null),
    notional: positive(window.notional) ?? notional,
    leverage: positive(window.leverage) ?? leverage,
  };
  const derived = deriveCapitalPnlPct(enriched);
  return derived === null
    ? enriched
    : { ...enriched, pnlPct: enriched.pnlPct ?? derived, pnlGrossPct: enriched.pnlGrossPct ?? derived };
}

export function mergeCapitalCloseWindows(windows: PositionWindow[]): PositionWindow[] {
  const merged: PositionWindow[] = [];
  for (const window of windows.slice().sort((a, b) => Number(a.exitTimestamp || 0) - Number(b.exitTimestamp || 0))) {
    const ts = finite(window.exitTimestamp);
    const match = merged.find((row) => {
      const rowTs = finite(row.exitTimestamp);
      return normalizeCapitalInstrument(row.symbol) === normalizeCapitalInstrument(window.symbol) &&
        rowTs !== null && ts !== null && Math.abs(rowTs - ts) <= MATCH_WINDOW_MS &&
        (row.id === window.id || finite(row.pnlNet) === null || finite(window.pnlNet) === null);
    });
    if (!match) {
      merged.push({ ...window });
      continue;
    }
    match.id = `${match.id}|${window.id}`;
    for (const key of ['entryTimestamp', 'exitTimestamp', 'entryPrice', 'exitPrice', 'side', 'pnlNet',
      'pnlGross', 'pnlPct', 'pnlGrossPct', 'notional', 'leverage'] as const) {
      (match as any)[key] ??= window[key] ?? null;
    }
  }
  return merged;
}

export async function reconcileCapitalClosedPositions(params: {
  symbols: string[];
  fromMs: number;
  toMs: number;
}): Promise<Map<string, PositionWindow[]>> {
  const symbols = Array.from(new Set(params.symbols.map((s) => s.trim().toUpperCase()).filter(Boolean)));
  const symbolByAlias = new Map<string, string>();
  for (const symbol of symbols) {
    symbolByAlias.set(normalizeCapitalInstrument(symbol), symbol);
    symbolByAlias.set(normalizeCapitalInstrument(resolveCapitalEpic(symbol).epic), symbol);
  }
  const [transactions, histories] = await Promise.all([
    fetchCapitalTradeTransactions({ fromTsMs: params.fromMs, toTsMs: params.toMs }),
    Promise.all(symbols.map((symbol) => loadDecisionHistory(symbol, 240, 'capital'))),
  ]);
  const historyBySymbol = new Map(symbols.map((symbol, index) => [symbol, histories[index]]));
  const grouped = new Map<string, PositionWindow[]>();
  for (const transaction of transactions) {
    const symbol = symbolByAlias.get(normalizeCapitalInstrument(transaction.instrumentName));
    if (!symbol) continue;
    const raw = capitalTransactionToPositionWindow(transaction, symbol);
    if (!raw) continue;
    const enriched = enrichCapitalCloseFromHistory(raw, historyBySymbol.get(symbol) ?? []);
    const rows = grouped.get(symbol) ?? [];
    rows.push(enriched);
    grouped.set(symbol, rows);
  }
  await Promise.all(symbols.map(async (symbol) => {
    const merged = mergeCapitalCloseWindows(grouped.get(symbol) ?? []);
    grouped.set(symbol, merged);
    await Promise.all(merged.map((window) => upsertSwingPosition('capital', {
      ...window,
      status: 'closed',
      leverageSource: window.leverage ? 'captured' : null,
    }).catch((err) => console.warn(`Could not persist reconciled Capital close ${window.id}:`, err))));
  }));
  return grouped;
}
