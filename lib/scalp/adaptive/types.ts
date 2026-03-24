import type { ScalpEntrySessionProfile } from '../types';

export const ADAPTIVE_META_SELECTOR_M15_M3_STRATEGY_ID = 'adaptive_meta_selector_m15_m3';

export type ScalpAdaptiveSnapshotStatus = 'shadow' | 'active' | 'archived';

export type ScalpAdaptiveArmType = 'pattern' | 'incumbent' | 'none';

export interface ScalpAdaptivePatternArm {
  armId: string;
  ngram: string[];
  support: number;
  winRate: number;
  meanProxyR: number;
  edgeLocal: number;
  edgeSession: number;
  edgeGlobal: number;
  score: number;
  confidence: number;
}

export interface ScalpAdaptiveIncumbentArm {
  armId: string;
  strategyId: string;
  strategyLabel: string;
}

export interface ScalpAdaptiveSnapshotCatalog {
  version: 1;
  minConfidence: number;
  minSupport: number;
  edgeScoreThreshold: number;
  generatedAtMs: number;
  patternArms: ScalpAdaptivePatternArm[];
  incumbentArm: ScalpAdaptiveIncumbentArm;
}

export interface ScalpAdaptiveSnapshotMetrics {
  localSamples: number;
  sessionSamples: number;
  globalSamples: number;
  positivePctLocal: number;
  avgProxyRLocal: number;
  trainedAtMs: number;
  note?: string | null;
}

export interface ScalpAdaptiveSelectorSnapshotRecord {
  snapshotId: string;
  symbol: string;
  entrySessionProfile: ScalpEntrySessionProfile;
  strategyId: string;
  status: ScalpAdaptiveSnapshotStatus;
  trainedAtMs: number;
  windowFromTs: number;
  windowToTs: number;
  catalog: ScalpAdaptiveSnapshotCatalog;
  metrics: ScalpAdaptiveSnapshotMetrics;
  lockStartedAtMs: number | null;
  lockUntilMs: number | null;
  baselineMaxDrawdownR: number | null;
  updatedBy: string | null;
  createdAtMs: number;
  updatedAtMs: number;
}

export interface ScalpAdaptiveDecisionRecord {
  id: number;
  tsMs: number;
  deploymentId: string;
  symbol: string;
  strategyId: string;
  entrySessionProfile: ScalpEntrySessionProfile;
  snapshotId: string | null;
  selectedArmId: string | null;
  selectedArmType: ScalpAdaptiveArmType;
  confidence: number | null;
  skipReason: string | null;
  reasonCodes: string[];
  featuresHash: string | null;
  details: Record<string, unknown> | null;
  createdAtMs: number;
}

export interface ScalpAdaptiveFeatureContext {
  tsMs: number;
  tokens: string[];
  featureHash: string;
  tokenMap: Record<string, string>;
  quarterHourBucket: number;
}

export interface ScalpAdaptiveTrainingRow extends ScalpAdaptiveFeatureContext {
  symbol: string;
  entrySessionProfile: ScalpEntrySessionProfile;
  proxyR: number;
  positive: boolean;
}

export interface ScalpAdaptivePatternStats {
  ngram: string[];
  support: number;
  wins: number;
  winRate: number;
  meanProxyR: number;
  edge: number;
}

export interface ScalpAdaptiveSelectionResult {
  selectedArmType: ScalpAdaptiveArmType;
  selectedArmId: string | null;
  confidence: number;
  skipReason: string | null;
  snapshotId: string | null;
  reasonCodes: string[];
  featureHash: string;
}
