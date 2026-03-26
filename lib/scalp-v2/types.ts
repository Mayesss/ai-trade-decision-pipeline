export type ScalpV2Venue = "bitget" | "capital";

export type ScalpV2Session = "tokyo" | "berlin" | "newyork" | "sydney";

export type ScalpV2CandidateStatus =
  | "discovered"
  | "evaluated"
  | "promoted"
  | "rejected"
  | "shadow";

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
  | "promote"
  | "execute"
  | "reconcile";

export type ScalpV2JobStatus =
  | "pending"
  | "running"
  | "succeeded"
  | "failed";

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
