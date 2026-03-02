export const config = { runtime: 'nodejs' };

import type { NextApiRequest, NextApiResponse } from 'next';

import { requireAdminAccess } from '../../../../lib/admin';
import { getScalpStrategyConfig } from '../../../../lib/scalp/config';
import { getScalpHybridPolicy, listScalpHybridSymbols, resolveScalpHybridSelection } from '../../../../lib/scalp/hybridPolicy';
import { deriveScalpDayKey } from '../../../../lib/scalp/stateMachine';
import { loadScalpJournal, loadScalpSessionState } from '../../../../lib/scalp/store';
import type { ScalpJournalEntry } from '../../../../lib/scalp/types';

type SymbolSnapshot = {
  symbol: string;
  profile: string;
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
    const policy = getScalpHybridPolicy();
    const cfg = getScalpStrategyConfig();
    const dayKey = deriveScalpDayKey(nowMs, cfg.sessions.clockMode);
    const symbols = listScalpHybridSymbols(policy);

    const rows: SymbolSnapshot[] = [];
    for (const symbol of symbols) {
      const selection = resolveScalpHybridSelection(symbol, policy);
      const state = await loadScalpSessionState(selection.symbol, dayKey);
      rows.push({
        symbol: selection.symbol,
        profile: selection.profile,
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
    const latestExecutionBySymbol: Record<string, Record<string, unknown>> = {};
    for (const entry of journal) {
      const symbol = String(entry.symbol || '').toUpperCase();
      if (!symbol || latestExecutionBySymbol[symbol]) continue;
      if (entry.type !== 'execution' && entry.type !== 'state' && entry.type !== 'error') continue;
      latestExecutionBySymbol[symbol] = compactJournalEntry(entry);
    }

    return res.status(200).json({
      mode: 'scalp',
      generatedAtMs: nowMs,
      dayKey,
      clockMode: cfg.sessions.clockMode,
      policy: {
        version: policy.version,
        defaultProfile: policy.defaultProfile,
        profiles: Object.keys(policy.profiles),
        symbolProfileCount: Object.keys(policy.symbolProfiles).length,
      },
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
      journal: journal.map(compactJournalEntry),
    });
  } catch (err: any) {
    return res.status(500).json({
      error: 'scalp_dashboard_summary_failed',
      message: err?.message || String(err),
    });
  }
}

