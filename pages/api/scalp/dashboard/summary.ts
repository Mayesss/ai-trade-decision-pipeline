export const config = { runtime: 'nodejs' };

import type { NextApiRequest, NextApiResponse } from 'next';

import { requireAdminAccess } from '../../../../lib/admin';
import { getScalpCronSymbolConfigs } from '../../../../lib/symbolRegistry';
import { getScalpStrategyConfig } from '../../../../lib/scalp/config';
import { normalizeScalpStrategyId } from '../../../../lib/scalp/strategies/registry';
import { deriveScalpDayKey } from '../../../../lib/scalp/stateMachine';
import { loadScalpJournal, loadScalpSessionState, loadScalpStrategyRuntimeSnapshot } from '../../../../lib/scalp/store';
import type { ScalpJournalEntry } from '../../../../lib/scalp/types';

type SymbolSnapshot = {
  symbol: string;
  strategyId: string;
  tune: string;
  cronSchedule: string | null;
  cronRoute: 'execute' | 'execute-hybrid';
  cronPath: string;
  dayKey: string;
  state: string | null;
  updatedAtMs: number | null;
  lastRunAtMs: number | null;
  dryRunLast: boolean | null;
  tradesPlaced: number;
  wins: number;
  losses: number;
  inTrade: boolean;
  tradeSide: 'BUY' | 'SELL' | null;
  dealReference: string | null;
  reasonCodes: string[];
};

function parseLimit(value: string | string[] | undefined, fallback: number): number {
  const first = Array.isArray(value) ? value[0] : value;
  const n = Number(first);
  if (!Number.isFinite(n) || n <= 0) return fallback;
  return Math.max(1, Math.min(300, Math.floor(n)));
}

function firstQueryValue(value: string | string[] | undefined): string | undefined {
  if (typeof value === 'string') return value.trim() || undefined;
  if (Array.isArray(value) && value.length > 0) return String(value[0] || '').trim() || undefined;
  return undefined;
}

function journalStrategyId(entry: ScalpJournalEntry): string | null {
  const payload = entry.payload && typeof entry.payload === 'object' ? (entry.payload as Record<string, unknown>) : {};
  const normalized = normalizeScalpStrategyId(payload.strategyId);
  return normalized || null;
}

function compactJournalEntry(entry: ScalpJournalEntry): Record<string, unknown> {
  return {
    id: entry.id,
    timestampMs: entry.timestampMs,
    type: entry.type,
    level: entry.level,
    symbol: entry.symbol,
    dayKey: entry.dayKey,
    reasonCodes: Array.isArray(entry.reasonCodes) ? entry.reasonCodes.slice(0, 8) : [],
    payload: entry.payload ?? {},
  };
}

function deriveTuneLabel(params: {
  strategyId: string;
  defaultStrategyId: string;
}): string {
  const strategyId = normalizeScalpStrategyId(params.strategyId);
  const defaultStrategyId = normalizeScalpStrategyId(params.defaultStrategyId);
  if (!strategyId) return 'default';
  if (!defaultStrategyId || strategyId === defaultStrategyId) return 'default';
  const prefix = `${defaultStrategyId}_`;
  if (strategyId.startsWith(prefix) && strategyId.length > prefix.length) {
    return strategyId.slice(prefix.length);
  }
  return strategyId;
}

function setNoStoreHeaders(res: NextApiResponse): void {
  res.setHeader('Cache-Control', 'no-store, no-cache, must-revalidate, proxy-revalidate');
  res.setHeader('Pragma', 'no-cache');
  res.setHeader('Expires', '0');
}

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
  if (req.method !== 'GET') {
    return res.status(405).json({ error: 'Method Not Allowed', message: 'Use GET' });
  }
  if (!requireAdminAccess(req, res)) return;
  setNoStoreHeaders(res);

  try {
    const nowMs = Date.now();
    const journalLimit = parseLimit(req.query.journalLimit, 80);
    const requestedStrategyId = firstQueryValue(req.query.strategyId);
    const cfg = getScalpStrategyConfig();
    const runtime = await loadScalpStrategyRuntimeSnapshot(cfg.enabled, requestedStrategyId);
    const strategy = runtime.strategy;
    const dayKey = deriveScalpDayKey(nowMs, cfg.sessions.clockMode);
    const cronSymbols = getScalpCronSymbolConfigs();

    const rows: SymbolSnapshot[] = [];
    for (const cronSymbol of cronSymbols) {
      const preferredStrategyId = normalizeScalpStrategyId(cronSymbol.strategyId);
      const strategyControl =
        runtime.strategies.find((row) => row.strategyId === preferredStrategyId) || strategy;
      const effectiveStrategyId = strategyControl.strategyId;
      const state = await loadScalpSessionState(cronSymbol.symbol, dayKey, effectiveStrategyId);
      rows.push({
        symbol: cronSymbol.symbol,
        strategyId: effectiveStrategyId,
        tune: deriveTuneLabel({
          strategyId: effectiveStrategyId,
          defaultStrategyId: runtime.defaultStrategyId,
        }),
        cronSchedule: cronSymbol.schedule,
        cronRoute: cronSymbol.route,
        cronPath: cronSymbol.path,
        dayKey,
        state: state?.state ?? null,
        updatedAtMs: state?.updatedAtMs ?? null,
        lastRunAtMs: state?.run?.lastRunAtMs ?? null,
        dryRunLast: typeof state?.run?.dryRunLast === 'boolean' ? state.run.dryRunLast : null,
        tradesPlaced: state?.stats?.tradesPlaced ?? 0,
        wins: state?.stats?.wins ?? 0,
        losses: state?.stats?.losses ?? 0,
        inTrade: state?.state === 'IN_TRADE' || Boolean(state?.trade),
        tradeSide: state?.trade?.side ?? null,
        dealReference: state?.trade?.dealReference ?? null,
        reasonCodes: Array.isArray(state?.run?.lastReasonCodes) ? state!.run.lastReasonCodes.slice(0, 8) : [],
      });
    }

    const stateCounts = rows.reduce<Record<string, number>>((acc, row) => {
      const key = row.state || 'MISSING';
      acc[key] = (acc[key] || 0) + 1;
      return acc;
    }, {});
    const openCount = rows.filter((row) => row.inTrade).length;
    const runCount = rows.filter((row) => Number.isFinite(row.lastRunAtMs as number)).length;
    const dryRunCount = rows.filter((row) => row.dryRunLast === true).length;
    const totalTradesPlaced = rows.reduce((acc, row) => acc + row.tradesPlaced, 0);

    const journal = await loadScalpJournal(journalLimit);
    const strategyBySymbol = new Map(rows.map((row) => [row.symbol.toUpperCase(), row.strategyId]));
    const allowedStrategyIds = new Set(rows.map((row) => row.strategyId));
    const latestExecutionBySymbol: Record<string, Record<string, unknown>> = {};
    for (const entry of journal) {
      const entryStrategy = journalStrategyId(entry);
      const symbol = String(entry.symbol || '').toUpperCase();
      if (!symbol || latestExecutionBySymbol[symbol]) continue;
      const expectedStrategyId = strategyBySymbol.get(symbol) || strategy.strategyId;
      if (entryStrategy && entryStrategy !== expectedStrategyId) continue;
      if (!entryStrategy && expectedStrategyId !== runtime.defaultStrategyId) continue;
      if (entry.type !== 'execution' && entry.type !== 'state' && entry.type !== 'error') continue;
      latestExecutionBySymbol[symbol] = compactJournalEntry(entry);
    }

    return res.status(200).json({
      mode: 'scalp',
      generatedAtMs: nowMs,
      dayKey,
      clockMode: cfg.sessions.clockMode,
      strategyId: strategy.strategyId,
      defaultStrategyId: runtime.defaultStrategyId,
      strategy,
      strategies: runtime.strategies,
      summary: {
        symbols: rows.length,
        openCount,
        runCount,
        dryRunCount,
        totalTradesPlaced,
        stateCounts,
      },
      symbols: rows,
      latestExecutionBySymbol,
      journal: journal
        .filter((entry) => {
          const entryStrategy = journalStrategyId(entry);
          if (entryStrategy && !allowedStrategyIds.has(entryStrategy)) return false;
          if (!entryStrategy && !allowedStrategyIds.has(runtime.defaultStrategyId)) return false;
          return true;
        })
        .map(compactJournalEntry),
    });
  } catch (err: any) {
    return res.status(500).json({
      error: 'scalp_dashboard_summary_failed',
      message: err?.message || String(err),
    });
  }
}
