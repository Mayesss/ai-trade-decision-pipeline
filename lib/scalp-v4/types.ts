export type ScalpV4Venue = "bitget" | "capital";

export interface ScalpV4ResearchCandidate {
  id: number;
  venue: ScalpV4Venue;
  symbol: string;
  strategyId: string;
  tuneId: string;
  entrySessionProfile: string;
  metadata: Record<string, unknown>;
}

export type ScalpV4AxisBucket = "low" | "mid" | "high" | "unknown";
export type ScalpV4TrendAxis = "trending_up" | "trending_down" | "choppy" | "unknown";
export type ScalpV4RiskAxis = "risk_on" | "risk_off" | "neutral" | "unknown";

export type ScalpV4CellId =
  | `vol=${Exclude<ScalpV4AxisBucket, "unknown">}|trend=${Exclude<ScalpV4TrendAxis, "unknown">}|risk=${Exclude<ScalpV4RiskAxis, "unknown">}`
  | "unknown";

export interface ScalpV4Candle {
  ts: number;
  open: number;
  high: number;
  low: number;
  close: number;
  volume?: number;
}

export interface ScalpV4WeeklyBar {
  weekStartMs: number;
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
}

export interface ScalpV4RawRegimeLabel {
  weekStartMs: number;
  classifierVersion: string;
  venue: ScalpV4Venue;
  symbol: string;
  volAxis: ScalpV4AxisBucket;
  trendAxis: ScalpV4TrendAxis;
  riskAxis: ScalpV4RiskAxis;
  rawCellId: ScalpV4CellId;
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

export interface ScalpV4RegimeSnapshot extends ScalpV4RawRegimeLabel {
  cellId: ScalpV4CellId;
  pendingCellId: ScalpV4CellId | null;
  pendingWeeks: number;
  transition: {
    fromCellId: ScalpV4CellId | null;
    toCellId: ScalpV4CellId | null;
  } | null;
}

export interface ScalpV4ClassifierOptions {
  classifierVersion?: string;
  minVolLookbackWeeks?: number;
  preferredVolLookbackWeeks?: number;
  hysteresisWeeks?: number;
  trendFastWeeks?: number;
  trendSlowWeeks?: number;
  adxWeeks?: number;
}

export interface ScalpV4MarketContext {
  usdJpy?: ScalpV4WeeklyBar[];
  audJpy?: ScalpV4WeeklyBar[];
  btcUsdt?: ScalpV4WeeklyBar[];
}

export interface ScalpV4TradeLike {
  entryTs: number;
  exitTs: number;
  rMultiple: number;
}

export interface ScalpV4WindowResult {
  windowStartMs: number;
  windowEndMs: number;
  trades: ScalpV4TradeLike[];
}

export interface ScalpV4CellAggregate {
  cellId: ScalpV4CellId;
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

export interface ScalpV4RegimeEnvelope {
  version: "scalp_v4_regime_envelope_r1";
  classifierVersion: string;
  evaluatedAtMs: number;
  eligible: boolean;
  status:
    | "eligible"
    | "no_passing_cells"
    | "regime_overbroad_pending_review"
    | "regime_overbroad_auto_rejected";
  allowedCells: ScalpV4CellId[];
  occupiedCells: number;
  strictPassingCells: number;
  relaxedPassingCells: number;
  overbroad: boolean;
  overbroadReviewUntilMs: number | null;
  thresholds: ScalpV4EnvelopeThresholds;
  cells: ScalpV4CellAggregate[];
}

export interface ScalpV4EnvelopeThresholds {
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
