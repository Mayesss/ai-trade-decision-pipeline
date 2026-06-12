export type ScalpRegimeVenue = "bitget" | "capital";

export interface ScalpRegimeResearchCandidate {
  id: number;
  venue: ScalpRegimeVenue;
  symbol: string;
  strategyId: string;
  tuneId: string;
  entrySessionProfile: string;
  metadata: Record<string, unknown>;
}

export type ScalpRegimeAxisBucket = "low" | "mid" | "high" | "unknown";
export type ScalpRegimeTrendAxis = "trending_up" | "trending_down" | "choppy" | "unknown";
export type ScalpRegimeRiskAxis = "risk_on" | "risk_off" | "neutral" | "unknown";

export type ScalpRegimeCellId =
  | `vol=${Exclude<ScalpRegimeAxisBucket, "unknown">}|trend=${Exclude<ScalpRegimeTrendAxis, "unknown">}|risk=${Exclude<ScalpRegimeRiskAxis, "unknown">}`
  | "unknown";

export interface ScalpRegimeCandle {
  ts: number;
  open: number;
  high: number;
  low: number;
  close: number;
  volume?: number;
}

export interface ScalpRegimeWeeklyBar {
  weekStartMs: number;
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
}

export interface ScalpRegimeRawRegimeLabel {
  weekStartMs: number;
  classifierVersion: string;
  venue: ScalpRegimeVenue;
  symbol: string;
  volAxis: ScalpRegimeAxisBucket;
  trendAxis: ScalpRegimeTrendAxis;
  riskAxis: ScalpRegimeRiskAxis;
  rawCellId: ScalpRegimeCellId;
  confidence: {
    volDistancePct: number | null;
    trendStrength: number | null;
    riskStrength: number | null;
  };
  sourceCoverage: {
    symbolWeeks: number;
    riskWeeks: number;
    warmupComplete: boolean;
    postWarmupUnknown?: boolean;
  };
  details: Record<string, unknown>;
}

export interface ScalpRegimeSnapshot extends ScalpRegimeRawRegimeLabel {
  cellId: ScalpRegimeCellId;
  pendingCellId: ScalpRegimeCellId | null;
  pendingWeeks: number;
  transition: {
    fromCellId: ScalpRegimeCellId | null;
    toCellId: ScalpRegimeCellId | null;
  } | null;
}

export interface ScalpRegimeClassifierOptions {
  classifierVersion?: string;
  minVolLookbackWeeks?: number;
  preferredVolLookbackWeeks?: number;
  hysteresisWeeks?: number;
  trendFastWeeks?: number;
  trendSlowWeeks?: number;
  adxWeeks?: number;
}

export interface ScalpRegimeMarketContext {
  usdJpy?: ScalpRegimeWeeklyBar[];
  audJpy?: ScalpRegimeWeeklyBar[];
  btcUsdt?: ScalpRegimeWeeklyBar[];
}

export interface ScalpRegimeTradeLike {
  entryTs: number;
  exitTs: number;
  rMultiple: number;
}

export interface ScalpRegimeWindowResult {
  windowStartMs: number;
  windowEndMs: number;
  trades: ScalpRegimeTradeLike[];
}

export interface ScalpRegimeCellAggregate {
  cellId: ScalpRegimeCellId;
  windows: number;
  trades: number;
  distinctEpochCount: number;
  netR: number;
  expectancyR: number;
  positiveWindowPct: number;
  p25ExpectancyR: number;
  maxDrawdownR: number;
  crossRegimeTradePct: number;
  bootstrapP05ExpectancyR: number | null;
  bootstrapResamples: number;
  deflatedSharpe: {
    sharpe: number | null;
    effectiveTrials: number;
    diagnosticScore: number | null;
  };
  strictPassed: boolean;
  relaxedPassed: boolean;
  reason: string | null;
}

export interface ScalpRegimeEnvelope {
  version: "scalp_v4_regime_envelope_r1";
  classifierVersion: string;
  evaluatedAtMs: number;
  eligible: boolean;
  status:
    | "eligible"
    | "no_passing_cells"
    | "regime_overbroad_pending_review"
    | "regime_overbroad_auto_rejected";
  allowedCells: ScalpRegimeCellId[];
  occupiedCells: number;
  strictPassingCells: number;
  relaxedPassingCells: number;
  overbroad: boolean;
  overbroadReviewUntilMs: number | null;
  thresholds: ScalpRegimeEnvelopeThresholds;
  cells: ScalpRegimeCellAggregate[];
}

// Per-cell cumulative stats — incrementally updated each sweep when a new
// window is appended. Lets future sweeps skip re-replaying historical windows.
export interface ScalpRegimeCellCumulativeStat {
  trades: number;
  netR: number;
  maxDrawdownR: number;
  crossRegimeTrades: number;
  epochsSeen: number[];                 // distinct epoch IDs encountered
  windowExpectancyR: number[];          // per-window mean R for this cell
  windowNetR: number[];                 // per-window total R for this cell
}

export interface ScalpRegimeIncrementalState {
  version: "scalp_v4_incremental_r1";
  classifierVersion: string;
  windowFromMs: number;                 // earliest window start ever included
  lastWindowEndMs: number;              // latest window end already aggregated
  cells: Record<string, ScalpRegimeCellCumulativeStat>;
  synthesizedAt?: number;               // set when cell stats were backfilled from envelope (not from real per-window data)
}

export interface ScalpRegimeEnvelopeThresholds {
  minCellWindows: number;
  minCellTrades: number;
  minDistinctEpochs: number;
  minPositiveWindowPct: number;
  minBootstrapP05ExpectancyR: number;
  relaxedPositiveWindowPct: number;
  relaxedBootstrapP05ExpectancyR: number;
  overbroadCellPassPct: number;
  overbroadMinCells: number;
  bootstrapBlockWeeks: number;
  bootstrapResamples: number;
}
