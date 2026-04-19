export type ScalpV2Venue = "bitget" | "capital";

export type ScalpV2Session = "tokyo" | "berlin" | "newyork" | "pacific" | "sydney";

export type ScalpV2CandidateStatus =
  | "discovered"
  | "evaluated"
  | "promoted"
  | "rejected";

export type ScalpV2LiveMode = "shadow" | "live";

export type ScalpV2EventType =
  | "order_submitted"
  | "order_rejected"
  | "position_snapshot"
  | "fill"
  | "stop_loss"
  | "liquidation"
  | "manual_close"
  | "reconcile_close";

export type ScalpV2CloseType =
  | "fill"
  | "stop_loss"
  | "liquidation"
  | "manual_close"
  | "reconcile_close";

export type ScalpV2SourceOfTruth = "broker" | "reconciler" | "system" | "legacy_v1_import";

export type ScalpV2JobKind =
  | "discover"
  | "evaluate"
  | "worker"
  | "research"
  | "promote"
  | "execute"
  | "reconcile";

export type ScalpV2JobStatus =
  | "pending"
  | "running"
  | "succeeded"
  | "failed";

export type ScalpV2WorkerStageId = "a" | "b" | "c";

export interface ScalpV2WorkerStageWeeklyMetrics {
  trades: number;
  wins: number;
  netR: number;
  grossProfitR: number;
  grossLossR: number;
  maxDrawdownR: number;
  maxPrefixR: number;
  minPrefixR: number;
  largestTradeR: number;
  exitStop: number;
  exitTp: number;
  exitTimeStop: number;
  exitForceClose: number;
}

export interface ScalpV2RiskProfile {
  riskPerTradePct: number;
  maxOpenPositionsPerSymbol: number;
  autoPauseDailyR: number;
  autoPause30dR: number;
}

export interface ScalpV2BudgetConfig {
  maxCandidatesTotal: number;
  maxCandidatesPerSymbol: number;
  maxEnabledDeployments: number;
}

export interface ScalpV2RuntimePrunedScopeEntry {
  venue: ScalpV2Venue;
  symbol: string;
  session: ScalpV2Session;
  prunedAtMs: number;
  expiresAtMs: number;
  source: string;
  reason: string;
  thresholds: {
    minCandidatesPerWindow: number;
    minStageAFailPct: number;
    requiredWindows: number;
  };
  windows: number[];
}

export interface ScalpV2RuntimeScopePruneMeta {
  lastPruneWindowToTs: number | null;
  lastPrunedAtMs: number | null;
  lastActiveScopeCount: number;
  lastNewlyPrunedScopeCount: number;
}

export interface ScalpV2RuntimeConfig {
  enabled: boolean;
  liveEnabled: boolean;
  dryRunDefault: boolean;
  defaultStrategyId: string;
  defaultTuneId: string;
  supportedVenues: ScalpV2Venue[];
  supportedSessions: ScalpV2Session[];
  seedSymbolsByVenue: Record<ScalpV2Venue, string[]>;
  seedLiveSymbolsByVenue: Record<ScalpV2Venue, string[]>;
  budgets: ScalpV2BudgetConfig;
  riskProfile: ScalpV2RiskProfile;
  prunedScopes?: Record<string, ScalpV2RuntimePrunedScopeEntry>;
  scopePruneMeta?: ScalpV2RuntimeScopePruneMeta;
}

export interface ScalpV2Candidate {
  id: number;
  venue: ScalpV2Venue;
  symbol: string;
  strategyId: string;
  tuneId: string;
  entrySessionProfile: ScalpV2Session;
  score: number;
  status: ScalpV2CandidateStatus;
  reasonCodes: string[];
  metadata: Record<string, unknown>;
  createdAtMs: number;
  updatedAtMs: number;
}

export interface ScalpV2Deployment {
  deploymentId: string;
  candidateId: number | null;
  venue: ScalpV2Venue;
  symbol: string;
  strategyId: string;
  tuneId: string;
  entrySessionProfile: ScalpV2Session;
  enabled: boolean;
  liveMode: ScalpV2LiveMode;
  promotionGate: Record<string, unknown>;
  riskProfile: ScalpV2RiskProfile;
  createdAtMs: number;
  updatedAtMs: number;
}

export interface ScalpV2ExecutionEvent {
  id: string;
  tsMs: number;
  deploymentId: string;
  venue: ScalpV2Venue;
  symbol: string;
  strategyId: string;
  tuneId: string;
  entrySessionProfile: ScalpV2Session;
  eventType: ScalpV2EventType;
  brokerRef: string | null;
  reasonCodes: string[];
  sourceOfTruth: ScalpV2SourceOfTruth;
  rawPayload: Record<string, unknown>;
}

export interface ScalpV2LedgerRow {
  id: string;
  tsExitMs: number;
  deploymentId: string;
  venue: ScalpV2Venue;
  symbol: string;
  strategyId: string;
  tuneId: string;
  entrySessionProfile: ScalpV2Session;
  entryRef: string | null;
  exitRef: string | null;
  closeType: ScalpV2CloseType;
  rMultiple: number;
  pnlUsd: number | null;
  sourceOfTruth: ScalpV2SourceOfTruth;
  reasonCodes: string[];
  rawPayload: Record<string, unknown>;
}

export interface ScalpV2JobResult {
  ok: boolean;
  busy: boolean;
  jobKind: ScalpV2JobKind;
  processed: number;
  succeeded: number;
  failed: number;
  pendingAfter: number;
  details: Record<string, unknown>;
}

export type ScalpV2PrimitiveFamily =
  | "pattern"
  | "session_filter"
  | "state_machine"
  | "entry_trigger"
  | "exit_rule"
  | "risk_rule";

export type ScalpV2PrimitiveBlockMap = Record<ScalpV2PrimitiveFamily, string[]>;

export interface ScalpV2PrimitiveBlock {
  id: string;
  family: ScalpV2PrimitiveFamily;
  label: string;
  description: string;
  tags: string[];
  sourceStrategyIds: string[];
}

export interface ScalpV2StrategyPrimitiveReference {
  strategyId: string;
  blocksByFamily: ScalpV2PrimitiveBlockMap;
  notes: string[];
}

export interface ScalpV2CandidateDslSpec {
  candidateId: string;
  tuneId: string;
  venue: ScalpV2Venue;
  symbol: string;
  entrySessionProfile: ScalpV2Session;
  blocksByFamily: ScalpV2PrimitiveBlockMap;
  referenceStrategyIds: string[];
  supportScore: number;
  generatedAtMs: number;
}

export type ScalpV2ComposerModelFamily =
  | "interpretable_pattern_blend"
  | "tree_split_proxy"
  | "sequence_state_proxy";

export interface ScalpV2ComposerModelScore {
  family: ScalpV2ComposerModelFamily;
  interpretableScore: number;
  treeScore: number;
  sequenceScore: number;
  compositeScore: number;
  confidence: number;
  version: string;
}

export interface ScalpV2ModelGuidedCandidateDslSpec
  extends ScalpV2CandidateDslSpec {
  model: ScalpV2ComposerModelScore;
  regimeGateId?: string | null;
}

export interface ScalpV2ResearchCursor {
  cursorKey: string;
  venue: ScalpV2Venue;
  symbol: string;
  entrySessionProfile: ScalpV2Session;
  phase: "scan" | "score" | "validate" | "promote";
  lastCandidateOffset: number;
  lastWeekStartMs: number | null;
  progress: Record<string, unknown>;
  updatedAtMs: number;
}

export interface ScalpV2ResearchHighlight {
  id: number;
  candidateId: string;
  venue: ScalpV2Venue;
  symbol: string;
  entrySessionProfile: ScalpV2Session;
  score: number;
  trades12w: number;
  winningWeeks12w: number;
  consecutiveWinningWeeks: number;
  robustness: Record<string, unknown>;
  dsl: Record<string, unknown>;
  notes: string | null;
  remarkable: boolean;
  createdAtMs: number;
  updatedAtMs: number;
}
