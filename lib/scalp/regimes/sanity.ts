import { countScalpRegimeEpochs } from "./classifier";
import type { ScalpRegimeSnapshot, ScalpRegimeWeeklyBar } from "./types";

function mean(values: number[]): number {
  return values.length ? values.reduce((acc, row) => acc + row, 0) / values.length : 0;
}

function weeklyReturns(bars: ScalpRegimeWeeklyBar[]): Map<number, number> {
  const out = new Map<number, number>();
  for (let idx = 1; idx < bars.length; idx += 1) {
    const prev = bars[idx - 1]!;
    const cur = bars[idx]!;
    if (prev.close > 0) out.set(cur.weekStartMs, (cur.close - prev.close) / prev.close);
  }
  return out;
}

export function buildScalpRegimeClassifierValidityReport(params: {
  snapshots: ScalpRegimeSnapshot[];
  marketBarsByName?: Record<string, ScalpRegimeWeeklyBar[]>;
  minEpochs?: number;
  maxEpochs?: number;
}): {
  passed: boolean;
  reason: string | null;
  epochCount: number;
  cellCount: number;
  marketSummaries: Record<string, Record<string, { weeks: number; meanReturnPct: number; realizedVolPct: number }>>;
} {
  const minEpochs = Math.max(1, Math.floor(params.minEpochs || 3));
  const maxEpochs = Math.max(minEpochs, Math.floor(params.maxEpochs || 12));
  const snapshots = (params.snapshots || []).filter((row) => row.cellId !== "unknown");
  const epochCount = countScalpRegimeEpochs(snapshots);
  const cellCount = new Set(snapshots.map((row) => row.cellId)).size;
  const marketSummaries: Record<string, Record<string, { weeks: number; meanReturnPct: number; realizedVolPct: number }>> = {};
  for (const [name, bars] of Object.entries(params.marketBarsByName || {})) {
    const returns = weeklyReturns(bars || []);
    const byCell: Record<string, number[]> = {};
    for (const snap of snapshots) {
      const ret = returns.get(snap.weekStartMs);
      if (!Number.isFinite(ret)) continue;
      const bucket = byCell[snap.cellId] || [];
      bucket.push(Number(ret));
      byCell[snap.cellId] = bucket;
    }
    marketSummaries[name] = Object.fromEntries(
      Object.entries(byCell).map(([cell, rows]) => {
        const m = mean(rows);
        const variance = rows.length > 1 ? mean(rows.map((row) => (row - m) * (row - m))) : 0;
        return [
          cell,
          {
            weeks: rows.length,
            meanReturnPct: m * 100,
            realizedVolPct: Math.sqrt(Math.max(0, variance)) * Math.sqrt(52) * 100,
          },
        ];
      }),
    );
  }
  let reason: string | null = null;
  if (epochCount < minEpochs) reason = "too_few_regime_epochs";
  else if (epochCount > maxEpochs) reason = "too_many_regime_epochs";
  else if (cellCount < 2) reason = "insufficient_cell_diversity";
  const nonEmptySummaryCount = Object.values(marketSummaries).reduce(
    (acc, summary) => acc + Object.keys(summary).length,
    0,
  );
  if (!reason && Object.keys(params.marketBarsByName || {}).length > 0 && nonEmptySummaryCount <= 0) {
    reason = "market_behavior_summary_empty";
  }
  return {
    passed: reason === null,
    reason,
    epochCount,
    cellCount,
    marketSummaries,
  };
}
