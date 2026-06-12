export type ScalpComposerVenue = "bitget" | "capital";

export type ScalpComposerSession = "tokyo" | "berlin" | "newyork" | "pacific" | "sydney";

export type ScalpComposerCandidateStatus =
  | "discovered"
  | "evaluated"
  | "promoted"
  | "rejected";

export type ScalpComposerLiveMode = "shadow" | "live";

export type ScalpComposerEventType =
  | "order_submitted"
  | "order_rejected"
  | "position_snapshot"
  | "fill"
  | "stop_loss"
  | "liquidation"
  | "manual_close"
  | "reconcile_close";

export type ScalpComposerCloseType =
  | "fill"
  | "stop_loss"
  | "trailing_stop"
  | "liquidation"
  | "manual_close"
  | "reconcile_close";

export type ScalpComposerSourceOfTruth = "broker" | "reconciler" | "system" | "legacy_v1_import";

export type ScalpComposerJobKind =
  | "discover"
  | "evaluate"
  | "worker"
  | "research"
  | "robustness"
  | "promote"
  | "execute"
  | "reconcile";

export type ScalpComposerJobStatus =
  | "pending"
  | "running"
  | "succeeded"
  | "failed";

export type ScalpComposerWorkerStageId = "a" | "b" | "c";

export interface ScalpComposerWorkerStageWeeklyMetrics {
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

export interface ScalpComposerRiskProfile {
  riskPerTradePct: number;
  maxOpenPositionsPerSymbol: number;
  autoPauseDailyR: number;
  autoPause30dR: number;
}

export interface ScalpComposerBudgetConfig {
  maxCandidatesTotal: number;
  maxCandidatesPerSymbol: number;
  maxEnabledDeployments: number;
}

export interface ScalpComposerRuntimePrunedScopeEntry {
  venue: ScalpComposerVenue;
  symbol: string;
  session: ScalpComposerSession;
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

export interface ScalpComposerRuntimeScopePruneMeta {
  lastPruneWindowToTs: number | null;
  lastPrunedAtMs: number | null;
  lastActiveScopeCount: number;
  lastNewlyPrunedScopeCount: number;
}

export interface ScalpComposerRuntimeConfig {
  enabled: boolean;
  liveEnabled: boolean;
  dryRunDefault: boolean;
  defaultStrategyId: string;
  defaultTuneId: string;
  supportedVenues: ScalpComposerVenue[];
  supportedSessions: ScalpComposerSession[];
  seedSymbolsByVenue: Record<ScalpComposerVenue, string[]>;
  seedLiveSymbolsByVenue: Record<ScalpComposerVenue, string[]>;
  budgets: ScalpComposerBudgetConfig;
  riskProfile: ScalpComposerRiskProfile;
  prunedScopes?: Record<string, ScalpComposerRuntimePrunedScopeEntry>;
  scopePruneMeta?: ScalpComposerRuntimeScopePruneMeta;
}

export interface ScalpComposerCandidate {
  id: number;
  venue: ScalpComposerVenue;
  symbol: string;
  strategyId: string;
  tuneId: string;
  entrySessionProfile: ScalpComposerSession;
  score: number;
  status: ScalpComposerCandidateStatus;
  reasonCodes: string[];
  metadata: Record<string, unknown>;
  researchAttempts?: number;
  deploymentId?: string | null;
  deploymentEnabled?: boolean | null;
  createdAtMs: number;
  updatedAtMs: number;
}

export interface ScalpComposerDeployment {
  deploymentId: string;
  candidateId: number | null;
  venue: ScalpComposerVenue;
  symbol: string;
  strategyId: string;
  tuneId: string;
  entrySessionProfile: ScalpComposerSession;
  enabled: boolean;
  liveMode: ScalpComposerLiveMode;
  promotionGate: Record<string, unknown>;
  riskProfile: ScalpComposerRiskProfile;
  createdAtMs: number;
  updatedAtMs: number;
}

export interface ScalpComposerExecutionEvent {
  id: string;
  tsMs: number;
  deploymentId: string;
  venue: ScalpComposerVenue;
  symbol: string;
  strategyId: string;
  tuneId: string;
  entrySessionProfile: ScalpComposerSession;
  eventType: ScalpComposerEventType;
  brokerRef: string | null;
  reasonCodes: string[];
  sourceOfTruth: ScalpComposerSourceOfTruth;
  rawPayload: Record<string, unknown>;
}

export interface ScalpComposerLedgerRow {
  id: string;
  tsExitMs: number;
  deploymentId: string;
  venue: ScalpComposerVenue;
  symbol: string;
  strategyId: string;
  tuneId: string;
  entrySessionProfile: ScalpComposerSession;
  entryRef: string | null;
  exitRef: string | null;
  closeType: ScalpComposerCloseType;
  rMultiple: number;
  pnlUsd: number | null;
  sourceOfTruth: ScalpComposerSourceOfTruth;
  reasonCodes: string[];
  rawPayload: Record<string, unknown>;
}

export interface ScalpComposerJobResult {
  ok: boolean;
  busy: boolean;
  jobKind: ScalpComposerJobKind;
  processed: number;
  succeeded: number;
  failed: number;
  pendingAfter: number;
  details: Record<string, unknown>;
}

export type ScalpComposerPrimitiveFamily =
  | "pattern"
  | "session_filter"
  | "state_machine"
  | "entry_trigger"
  | "exit_rule"
  | "risk_rule";

export type ScalpComposerPrimitiveBlockMap = Record<ScalpComposerPrimitiveFamily, string[]>;

export interface ScalpComposerPrimitiveBlock {
  id: string;
  family: ScalpComposerPrimitiveFamily;
  label: string;
  description: string;
  tags: string[];
  sourceStrategyIds: string[];
}

export interface ScalpComposerStrategyPrimitiveReference {
  strategyId: string;
  blocksByFamily: ScalpComposerPrimitiveBlockMap;
  notes: string[];
}

export interface ScalpComposerCandidateDslSpec {
  candidateId: string;
  tuneId: string;
  venue: ScalpComposerVenue;
  symbol: string;
  entrySessionProfile: ScalpComposerSession;
  blocksByFamily: ScalpComposerPrimitiveBlockMap;
  referenceStrategyIds: string[];
  supportScore: number;
  generatedAtMs: number;
}

export type ScalpComposerModelFamily =
  | "interpretable_pattern_blend"
  | "tree_split_proxy"
  | "sequence_state_proxy";

export interface ScalpComposerModelScore {
  family: ScalpComposerModelFamily;
  interpretableScore: number;
  treeScore: number;
  sequenceScore: number;
  compositeScore: number;
  confidence: number;
  version: string;
}

export interface ScalpComposerModelGuidedCandidateDslSpec
  extends ScalpComposerCandidateDslSpec {
  model: ScalpComposerModelScore;
  regimeGateId?: string | null;
}

export interface ScalpComposerResearchCursor {
  cursorKey: string;
  venue: ScalpComposerVenue;
  symbol: string;
  entrySessionProfile: ScalpComposerSession;
  phase: "scan" | "score" | "validate" | "promote";
  lastCandidateOffset: number;
  lastWeekStartMs: number | null;
  progress: Record<string, unknown>;
  updatedAtMs: number;
}

export interface ScalpComposerResearchHighlight {
  id: number;
  candidateId: string;
  venue: ScalpComposerVenue;
  symbol: string;
  entrySessionProfile: ScalpComposerSession;
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
