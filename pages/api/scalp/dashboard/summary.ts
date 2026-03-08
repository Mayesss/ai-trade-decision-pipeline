export const config = { runtime: 'nodejs' };

import type { NextApiRequest, NextApiResponse } from 'next';

import { requireAdminAccess } from '../../../../lib/admin';
import { getScalpCronSymbolConfigs } from '../../../../lib/symbolRegistry';
import { getScalpStrategyConfig } from '../../../../lib/scalp/config';
import { listScalpDeploymentRegistryEntries, type ScalpForwardValidationMetrics } from '../../../../lib/scalp/deploymentRegistry';
import { DEFAULT_SCALP_TUNE_ID, resolveScalpDeployment } from '../../../../lib/scalp/deployments';
import { normalizeScalpStrategyId } from '../../../../lib/scalp/strategies/registry';
import { deriveScalpDayKey } from '../../../../lib/scalp/stateMachine';
import { loadScalpJournal, loadScalpSessionState, loadScalpStrategyRuntimeSnapshot, loadScalpTradeLedger } from '../../../../lib/scalp/store';
import type { ScalpJournalEntry, ScalpTradeLedgerEntry } from '../../../../lib/scalp/types';

type SummaryRangeKey = '7D' | '30D' | '6M';
const SUMMARY_RANGE_LOOKBACK_MS: Record<SummaryRangeKey, number> = {
  '7D': 7 * 24 * 60 * 60 * 1000,
  '30D': 30 * 24 * 60 * 60 * 1000,
  '6M': 183 * 24 * 60 * 60 * 1000,
};

type SymbolSnapshot = {
  symbol: string;
  strategyId: string;
  tuneId: string;
  deploymentId: string;
  tune: string;
  cronSchedule: string | null;
  cronRoute: 'execute-deployments';
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
  netR: number | null;
  maxDrawdownR: number | null;
  promotionEligible: boolean | null;
  promotionReason: string | null;
  forwardValidation: ScalpForwardValidationMetrics | null;
};

function parseLimit(value: string | string[] | undefined, fallback: number): number {
  const first = Array.isArray(value) ? value[0] : value;
  const n = Number(first);
  if (!Number.isFinite(n) || n <= 0) return fallback;
  return Math.max(1, Math.min(300, Math.floor(n)));
}

function parseTradeLimit(value: string | string[] | undefined, fallback: number): number {
  const first = Array.isArray(value) ? value[0] : value;
  const n = Number(first);
  if (!Number.isFinite(n) || n <= 0) return fallback;
  return Math.max(200, Math.min(50_000, Math.floor(n)));
}

function parseBool(value: string | string[] | undefined, fallback: boolean): boolean {
  const first = firstQueryValue(value);
  if (!first) return fallback;
  const normalized = first.toLowerCase();
  if (['1', 'true', 'yes', 'on'].includes(normalized)) return true;
  if (['0', 'false', 'no', 'off'].includes(normalized)) return false;
  return fallback;
}

function resolveSummaryRange(raw: unknown): SummaryRangeKey {
  const normalized = String(raw || '')
    .trim()
    .toUpperCase();
  if (normalized === '30D') return '30D';
  if (normalized === '6M') return '6M';
  return '7D';
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

function journalDeploymentId(entry: ScalpJournalEntry): string | null {
  const payload = entry.payload && typeof entry.payload === 'object' ? (entry.payload as Record<string, unknown>) : {};
  const normalized = String(payload.deploymentId || '').trim();
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

function computeRangePerformance(trades: ScalpTradeLedgerEntry[]): { netR: number; maxDrawdownR: number } | null {
  if (!trades.length) return null;
  const ordered = trades.slice().sort((a, b) => a.exitAtMs - b.exitAtMs);
  let netR = 0;
  let equityR = 0;
  let peakR = 0;
  let maxDd = 0;
  for (const trade of ordered) {
    const r = Number(trade.rMultiple);
    if (!Number.isFinite(r)) continue;
    netR += r;
    equityR += r;
    peakR = Math.max(peakR, equityR);
    maxDd = Math.max(maxDd, peakR - equityR);
  }
  return { netR, maxDrawdownR: maxDd };
}

function deriveTuneLabel(params: {
  strategyId: string;
  defaultStrategyId: string;
  tuneId?: string | null;
}): string {
  const explicitTune = String(params.tuneId || '').trim().toLowerCase();
  if (explicitTune && explicitTune !== DEFAULT_SCALP_TUNE_ID) return explicitTune;
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
    const tradeLimit = parseTradeLimit(req.query.tradeLimit, 5000);
    const rangeParam = Array.isArray(req.query.range) ? req.query.range[0] : req.query.range;
    const range = resolveSummaryRange(rangeParam);
    const rangeStartMs = nowMs - SUMMARY_RANGE_LOOKBACK_MS[range];
    const requestedStrategyId = firstQueryValue(req.query.strategyId);
    const useDeployments = parseBool(req.query.useDeploymentRegistry, false);
    const cfg = getScalpStrategyConfig();
    const runtime = await loadScalpStrategyRuntimeSnapshot(cfg.enabled, requestedStrategyId);
    const strategy = runtime.strategy;
    const dayKey = deriveScalpDayKey(nowMs, cfg.sessions.clockMode);
    const cronSymbolConfigs = getScalpCronSymbolConfigs();
    const cronSymbolConfigBySymbol = new Map(cronSymbolConfigs.map((row) => [row.symbol.toUpperCase(), row]));
    const cronAllConfig = cronSymbolConfigBySymbol.get('*') || null;
    const cronSymbols = useDeployments ? [] : cronSymbolConfigs;
    const deploymentRows = useDeployments ? await listScalpDeploymentRegistryEntries({ enabled: true }) : [];

    const rows: SymbolSnapshot[] = [];
    if (useDeployments) {
      for (const deploymentRow of deploymentRows) {
        const preferredStrategyId = normalizeScalpStrategyId(deploymentRow.strategyId);
        const strategyControl =
          runtime.strategies.find((row) => row.strategyId === preferredStrategyId) || strategy;
        const effectiveStrategyId = strategyControl.strategyId;
        const cronSymbol = cronSymbolConfigBySymbol.get(deploymentRow.symbol.toUpperCase()) || cronAllConfig;
        const deployment = resolveScalpDeployment({
          symbol: deploymentRow.symbol,
          strategyId: effectiveStrategyId,
          tuneId: deploymentRow.tuneId,
          deploymentId: deploymentRow.deploymentId,
        });
        const state = await loadScalpSessionState(deployment.symbol, dayKey, effectiveStrategyId, {
          tuneId: deployment.tuneId,
          deploymentId: deployment.deploymentId,
        });
        rows.push({
          symbol: deployment.symbol,
          strategyId: effectiveStrategyId,
          tuneId: deployment.tuneId,
          deploymentId: deployment.deploymentId,
          tune: deriveTuneLabel({
            strategyId: effectiveStrategyId,
            defaultStrategyId: runtime.defaultStrategyId,
            tuneId: deployment.tuneId,
          }),
          cronSchedule: cronSymbol?.schedule ?? null,
          cronRoute: 'execute-deployments',
          cronPath: cronSymbol?.path || '/api/scalp/cron/execute-deployments?all=true',
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
          netR: null,
          maxDrawdownR: null,
          promotionEligible: typeof deploymentRow.promotionGate?.eligible === 'boolean' ? deploymentRow.promotionGate.eligible : null,
          promotionReason: deploymentRow.promotionGate?.reason || null,
          forwardValidation: deploymentRow.promotionGate?.forwardValidation || null,
        });
      }
    } else {
      for (const cronSymbol of cronSymbols) {
        const preferredStrategyId = normalizeScalpStrategyId(cronSymbol.strategyId);
        const strategyControl =
          runtime.strategies.find((row) => row.strategyId === preferredStrategyId) || strategy;
        const effectiveStrategyId = strategyControl.strategyId;
        const deployment = resolveScalpDeployment({
          symbol: cronSymbol.symbol,
          strategyId: effectiveStrategyId,
          tuneId: cronSymbol.tuneId,
          deploymentId: cronSymbol.deploymentId,
        });
        const state = await loadScalpSessionState(deployment.symbol, dayKey, effectiveStrategyId, {
          tuneId: deployment.tuneId,
          deploymentId: deployment.deploymentId,
        });
        rows.push({
          symbol: deployment.symbol,
          strategyId: effectiveStrategyId,
          tuneId: deployment.tuneId,
          deploymentId: deployment.deploymentId,
          tune: deriveTuneLabel({
            strategyId: effectiveStrategyId,
            defaultStrategyId: runtime.defaultStrategyId,
            tuneId: deployment.tuneId,
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
          netR: null,
          maxDrawdownR: null,
          promotionEligible: null,
          promotionReason: null,
          forwardValidation: null,
        });
      }
    }

    const tradeLedger = await loadScalpTradeLedger(tradeLimit);
    const tradesByDeploymentId = new Map<string, ScalpTradeLedgerEntry[]>();
    for (const trade of tradeLedger) {
      if (trade.dryRun) continue;
      if (!(Number.isFinite(Number(trade.exitAtMs)) && Number(trade.exitAtMs) >= rangeStartMs)) continue;
      const deploymentId = String(trade.deploymentId || '').trim();
      if (!deploymentId) continue;
      const bucket = tradesByDeploymentId.get(deploymentId) || [];
      bucket.push(trade);
      tradesByDeploymentId.set(deploymentId, bucket);
    }
    for (const row of rows) {
      const perf = computeRangePerformance(tradesByDeploymentId.get(row.deploymentId) || []);
      row.netR = perf?.netR ?? null;
      row.maxDrawdownR = perf?.maxDrawdownR ?? null;
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
    const allowedDeploymentIds = new Set(rows.map((row) => row.deploymentId));
    const latestExecutionBySymbol: Record<string, Record<string, unknown>> = {};
    const latestExecutionByDeploymentId: Record<string, Record<string, unknown>> = {};
    for (const entry of journal) {
      const entryStrategy = journalStrategyId(entry);
      const entryDeploymentId = journalDeploymentId(entry);
      const symbol = String(entry.symbol || '').toUpperCase();
      if (!symbol) continue;
      const expectedStrategyId = strategyBySymbol.get(symbol) || strategy.strategyId;
      if (entryStrategy && entryStrategy !== expectedStrategyId) continue;
      if (!entryStrategy && expectedStrategyId !== runtime.defaultStrategyId) continue;
      if (entry.type !== 'execution' && entry.type !== 'state' && entry.type !== 'error') continue;
      const compacted = compactJournalEntry(entry);
      if (!latestExecutionBySymbol[symbol]) {
        latestExecutionBySymbol[symbol] = compacted;
      }
      if (entryDeploymentId && allowedDeploymentIds.has(entryDeploymentId) && !latestExecutionByDeploymentId[entryDeploymentId]) {
        latestExecutionByDeploymentId[entryDeploymentId] = compacted;
      }
    }

    return res.status(200).json({
      mode: 'scalp',
      generatedAtMs: nowMs,
      dayKey,
      clockMode: cfg.sessions.clockMode,
      source: useDeployments ? 'deployment_registry' : 'cron_symbols',
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
      range,
      symbols: rows,
      latestExecutionByDeploymentId,
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
