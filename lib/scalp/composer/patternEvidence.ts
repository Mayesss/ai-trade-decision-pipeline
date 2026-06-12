import type { ScalpReplayTrade } from "../replay/types";
import type { ScalpComposerSession, ScalpComposerVenue } from "./types";

export const SCALP_V2_PATTERN_EVIDENCE_VERSION = "scalp_v2_pattern_evidence_r1";
export const SCALP_V2_PATTERN_EVIDENCE_POPULATION_STAGE_C_PASSED =
  "stage_c_passed_survivors";
export const SCALP_V2_PATTERN_EVIDENCE_SURVIVOR_WARNING =
  "Survivor-only stage-C-passed population; use for pattern comparison, not unbiased universe expectancy.";

const DEFAULT_BUCKET_MINUTES = 60;

export interface ScalpComposerPatternCandidateSummary {
  candidateId: number | null;
  venue: ScalpComposerVenue;
  symbol: string;
  session: ScalpComposerSession;
  behaviorFingerprint: string;
  windowToTs: number;
  stageCLowerBoundR: number | null;
  stageCNetR: number;
  stageCTrades: number;
}

export interface ScalpComposerPatternTradeVector {
  candidateId: number | null;
  venue: ScalpComposerVenue;
  symbol: string;
  strategyId: string;
  tuneId: string;
  session: ScalpComposerSession;
  windowToTs: number;
  stageId: "c";
  replayTradeIndex: number;
  behaviorFingerprint: string;
  patternKey: string;
  entryTs: number;
  exitTs: number;
  bucketStartTs: number;
  side: "BUY" | "SELL";
  exitReason: string;
  rMultiple: number;
  feeR: number | null;
  grossRMultiple: number | null;
}

export interface ScalpComposerPatternEdge {
  patternKey: string;
  venue: ScalpComposerVenue;
  session: ScalpComposerSession;
  behaviorFingerprint: string;
  windowToTs: number;
  bucketMinutes: number;
  populationScope: string;
  candidateCount: number;
  representativeCandidateCount: number;
  symbolCount: number;
  positiveSymbolCount: number;
  positiveSymbolPct: number;
  topSymbol: string | null;
  topSymbolNetR: number;
  topSymbolConcentrationPct: number;
  rawTrades: number;
  rawNetR: number;
  rawMeanR: number;
  rawStdR: number;
  rawLowerBoundR: number;
  bucketCount: number;
  bucketNetR: number;
  bucketMeanR: number;
  bucketStdR: number;
  bucketLowerBoundR: number;
  leaveOneSymbolOutBucketLowerBoundR: number | null;
  scoreJson: Record<string, unknown>;
}

function finite(value: unknown, fallback = 0): number {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

function sampleStd(values: number[]): number {
  if (values.length <= 1) return 0;
  const mean = values.reduce((acc, row) => acc + row, 0) / values.length;
  const variance =
    values.reduce((acc, row) => acc + Math.pow(row - mean, 2), 0) /
    (values.length - 1);
  return Math.sqrt(Math.max(0, variance));
}

function lowerBound(values: number[]): {
  count: number;
  netR: number;
  meanR: number;
  stdR: number;
  lowerBoundR: number;
} {
  const rows = values.map((row) => finite(row)).filter(Number.isFinite);
  const count = rows.length;
  const netR = rows.reduce((acc, row) => acc + row, 0);
  const meanR = count > 0 ? netR / count : 0;
  const stdR = sampleStd(rows);
  const stderrR = count > 0 ? stdR / Math.sqrt(count) : 0;
  return {
    count,
    netR,
    meanR,
    stdR,
    lowerBoundR: meanR - 1.64 * stderrR,
  };
}

export function buildScalpComposerPatternKey(params: {
  venue: ScalpComposerVenue | string;
  session: ScalpComposerSession | string;
  behaviorFingerprint: string;
}): string {
  return [
    String(params.venue || "").trim().toLowerCase(),
    String(params.session || "").trim().toLowerCase(),
    String(params.behaviorFingerprint || "").trim(),
  ].join(":");
}

export function scalpComposerPatternBucketStartTs(
  ts: number,
  bucketMinutes = DEFAULT_BUCKET_MINUTES,
): number {
  const bucketMs = Math.max(1, Math.floor(bucketMinutes || DEFAULT_BUCKET_MINUTES)) * 60_000;
  return Math.floor(Math.max(0, Math.floor(Number(ts) || 0)) / bucketMs) * bucketMs;
}

export function extractScalpComposerPatternTradeVectors(params: {
  candidateId: number | null;
  venue: ScalpComposerVenue;
  symbol: string;
  strategyId: string;
  tuneId: string;
  session: ScalpComposerSession;
  windowToTs: number;
  behaviorFingerprint: string;
  trades: ScalpReplayTrade[];
  bucketMinutes?: number;
}): ScalpComposerPatternTradeVector[] {
  const behaviorFingerprint = String(params.behaviorFingerprint || "").trim();
  if (!behaviorFingerprint) return [];
  const venue = String(params.venue || "").trim().toLowerCase() as ScalpComposerVenue;
  const session = String(params.session || "").trim().toLowerCase() as ScalpComposerSession;
  const symbol = String(params.symbol || "").trim().toUpperCase();
  const strategyId = String(params.strategyId || "").trim().toLowerCase();
  const tuneId = String(params.tuneId || "").trim().toLowerCase();
  const patternKey = buildScalpComposerPatternKey({ venue, session, behaviorFingerprint });
  const bucketMinutes = Math.max(1, Math.floor(params.bucketMinutes || DEFAULT_BUCKET_MINUTES));
  const windowToTs = Math.floor(Number(params.windowToTs) || 0);

  return (params.trades || []).map((trade, replayTradeIndex) => ({
    candidateId: params.candidateId ?? null,
    venue,
    symbol,
    strategyId,
    tuneId,
    session,
    windowToTs,
    stageId: "c" as const,
    replayTradeIndex,
    behaviorFingerprint,
    patternKey,
    entryTs: Math.floor(Number(trade.entryTs) || 0),
    exitTs: Math.floor(Number(trade.exitTs) || 0),
    bucketStartTs: scalpComposerPatternBucketStartTs(trade.exitTs, bucketMinutes),
    side: trade.side === "SELL" ? "SELL" : "BUY",
    exitReason: String(trade.exitReason || "UNKNOWN"),
    rMultiple: finite(trade.rMultiple),
    feeR: Number.isFinite(Number(trade.feeR)) ? Number(trade.feeR) : null,
    grossRMultiple: Number.isFinite(Number(trade.grossRMultiple))
      ? Number(trade.grossRMultiple)
      : null,
  }));
}

export function selectScalpComposerPatternRepresentativeCandidates<T extends ScalpComposerPatternCandidateSummary>(
  candidates: T[],
): T[] {
  const byKey = new Map<string, T>();
  for (const row of candidates) {
    const key = [
      String(row.venue || "").toLowerCase(),
      String(row.session || "").toLowerCase(),
      String(row.symbol || "").toUpperCase(),
      String(row.behaviorFingerprint || ""),
      Math.floor(Number(row.windowToTs) || 0),
    ].join(":");
    const existing = byKey.get(key);
    if (!existing || comparePatternCandidateRank(row, existing) < 0) {
      byKey.set(key, row);
    }
  }
  return Array.from(byKey.values());
}

function comparePatternCandidateRank(
  a: ScalpComposerPatternCandidateSummary,
  b: ScalpComposerPatternCandidateSummary,
): number {
  const aLb = Number.isFinite(Number(a.stageCLowerBoundR))
    ? Number(a.stageCLowerBoundR)
    : Number.NEGATIVE_INFINITY;
  const bLb = Number.isFinite(Number(b.stageCLowerBoundR))
    ? Number(b.stageCLowerBoundR)
    : Number.NEGATIVE_INFINITY;
  if (aLb !== bLb) return bLb - aLb;
  const aNet = finite(a.stageCNetR);
  const bNet = finite(b.stageCNetR);
  if (aNet !== bNet) return bNet - aNet;
  const aTrades = Math.max(0, Math.floor(finite(a.stageCTrades)));
  const bTrades = Math.max(0, Math.floor(finite(b.stageCTrades)));
  return bTrades - aTrades;
}

export function aggregateScalpComposerPatternEdges(params: {
  trades: ScalpComposerPatternTradeVector[];
  candidateCount: number;
  representativeCandidateCount: number;
  candidateCountsByPattern?: Map<string, number> | Record<string, number>;
  representativeCandidateCountsByPattern?: Map<string, number> | Record<string, number>;
  bucketMinutes?: number;
  populationScope?: string;
}): ScalpComposerPatternEdge[] {
  const bucketMinutes = Math.max(1, Math.floor(params.bucketMinutes || DEFAULT_BUCKET_MINUTES));
  const populationScope =
    String(params.populationScope || "").trim() ||
    SCALP_V2_PATTERN_EVIDENCE_POPULATION_STAGE_C_PASSED;
  const byPattern = new Map<string, ScalpComposerPatternTradeVector[]>();
  for (const trade of params.trades || []) {
    if (!trade.patternKey) continue;
    const rows = byPattern.get(trade.patternKey) || [];
    rows.push(trade);
    byPattern.set(trade.patternKey, rows);
  }

  return Array.from(byPattern.entries())
    .map(([patternKey, rows]) =>
      aggregateOnePattern({
        patternKey,
        rows,
        candidateCount:
          lookupPatternCount(params.candidateCountsByPattern, patternKey) ??
          params.candidateCount,
        representativeCandidateCount:
          lookupPatternCount(params.representativeCandidateCountsByPattern, patternKey) ??
          params.representativeCandidateCount,
        bucketMinutes,
        populationScope,
      }),
    )
    .sort((a, b) => b.bucketLowerBoundR - a.bucketLowerBoundR || b.bucketNetR - a.bucketNetR);
}

function lookupPatternCount(
  counts: Map<string, number> | Record<string, number> | undefined,
  patternKey: string,
): number | null {
  if (!counts) return null;
  const value = counts instanceof Map ? counts.get(patternKey) : counts[patternKey];
  const n = Math.floor(Number(value) || 0);
  return n > 0 ? n : null;
}

function aggregateOnePattern(params: {
  patternKey: string;
  rows: ScalpComposerPatternTradeVector[];
  candidateCount: number;
  representativeCandidateCount: number;
  bucketMinutes: number;
  populationScope: string;
}): ScalpComposerPatternEdge {
  const first = params.rows[0]!;
  const rawStats = lowerBound(params.rows.map((row) => row.rMultiple));
  const bucketReturns = new Map<number, number>();
  const symbolReturns = new Map<string, number>();
  for (const row of params.rows) {
    bucketReturns.set(row.bucketStartTs, (bucketReturns.get(row.bucketStartTs) || 0) + row.rMultiple);
    symbolReturns.set(row.symbol, (symbolReturns.get(row.symbol) || 0) + row.rMultiple);
  }
  const bucketStats = lowerBound(Array.from(bucketReturns.values()));
  const symbols = Array.from(symbolReturns.entries());
  const positiveSymbols = symbols.filter(([, netR]) => netR > 0);
  const symbolCount = symbols.length;
  const positiveSymbolCount = positiveSymbols.length;
  const positiveSymbolPct = symbolCount > 0 ? (positiveSymbolCount / symbolCount) * 100 : 0;
  const topSymbolRow = symbols
    .slice()
    .sort((a, b) => b[1] - a[1])[0] || null;
  const topSymbol = topSymbolRow?.[0] || null;
  const topSymbolNetR = topSymbolRow ? topSymbolRow[1] : 0;
  const positiveNetR = positiveSymbols.reduce((acc, [, netR]) => acc + netR, 0);
  const topSymbolConcentrationPct =
    positiveNetR > 0 && topSymbolNetR > 0 ? (topSymbolNetR / positiveNetR) * 100 : 0;
  const leaveOneSymbolOutBucketLowerBoundR = topSymbol && symbolCount > 1
    ? lowerBound(
        Array.from(
          params.rows
            .filter((row) => row.symbol !== topSymbol)
            .reduce((acc, row) => {
              acc.set(row.bucketStartTs, (acc.get(row.bucketStartTs) || 0) + row.rMultiple);
              return acc;
            }, new Map<number, number>())
            .values(),
        ),
      ).lowerBoundR
    : null;

  return {
    patternKey: params.patternKey,
    venue: first.venue,
    session: first.session,
    behaviorFingerprint: first.behaviorFingerprint,
    windowToTs: first.windowToTs,
    bucketMinutes: params.bucketMinutes,
    populationScope: params.populationScope,
    candidateCount: params.candidateCount,
    representativeCandidateCount: params.representativeCandidateCount,
    symbolCount,
    positiveSymbolCount,
    positiveSymbolPct,
    topSymbol,
    topSymbolNetR,
    topSymbolConcentrationPct,
    rawTrades: rawStats.count,
    rawNetR: rawStats.netR,
    rawMeanR: rawStats.meanR,
    rawStdR: rawStats.stdR,
    rawLowerBoundR: rawStats.lowerBoundR,
    bucketCount: bucketStats.count,
    bucketNetR: bucketStats.netR,
    bucketMeanR: bucketStats.meanR,
    bucketStdR: bucketStats.stdR,
    bucketLowerBoundR: bucketStats.lowerBoundR,
    leaveOneSymbolOutBucketLowerBoundR,
    scoreJson: {
      version: SCALP_V2_PATTERN_EVIDENCE_VERSION,
      survivorOnly: params.populationScope === SCALP_V2_PATTERN_EVIDENCE_POPULATION_STAGE_C_PASSED,
      warning:
        params.populationScope === SCALP_V2_PATTERN_EVIDENCE_POPULATION_STAGE_C_PASSED
          ? SCALP_V2_PATTERN_EVIDENCE_SURVIVOR_WARNING
          : null,
      authoritativeMetric: "bucketLowerBoundR",
      rawLowerBoundRLabel: "naive_trade_level_lower_bound",
      bucketMinutes: params.bucketMinutes,
    },
  };
}
