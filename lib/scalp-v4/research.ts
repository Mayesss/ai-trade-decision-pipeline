import {
  ensureScalpV4WeeklyRegimesBuilt,
  type ScalpV4WeeklyBuildResult,
} from "./build";
import {
  runScalpV4WalkforwardSweep,
  type ScalpV4WalkforwardProgressEvent,
  type ScalpV4WalkforwardSweepResult,
} from "./walkforwardSweep";
import type { ScalpCandle } from "../scalp/types";

export interface ScalpV4ResearchJobResult {
  ok: boolean;
  busy: false;
  jobKind: "v4_research";
  processed: number;
  succeeded: number;
  failed: number;
  pendingAfter: number;
  details: {
    weeklyBuild: {
      skipped: boolean;
      reason: string;
      result?: ScalpV4WeeklyBuildResult;
    };
    walkforward: ScalpV4WalkforwardSweepResult;
  };
}

export async function runScalpV4ResearchJob(params: {
  classifierVersion?: string;
  forceValidity?: boolean;
  maxCandidatesPerCall?: number;
  candidateFetchLimit?: number;
  effectiveTrials?: number;
  windowToMs?: number;
  candleCacheRef?: Map<string, ScalpCandle[]>;
  candleCacheSoftCap?: number;
  autoBackfillCandles?: boolean;
  minCandleCoverageRatio?: number;
  candleBackfillChunkWeeks?: number;
  candleBackfillMaxRequestsPerChunk?: number;
  workClaimLeaseMs?: number;
  progressIntervalMs?: number;
  runWalkforward?: boolean;
  onProgress?: (event: ScalpV4WalkforwardProgressEvent) => void;
} = {}): Promise<ScalpV4ResearchJobResult> {
  const weeklyBuild = await ensureScalpV4WeeklyRegimesBuilt({
    classifierVersion: params.classifierVersion,
    forceValidity: params.forceValidity,
  });
  const runWalkforward = params.runWalkforward !== false;
  const walkforward = await runScalpV4WalkforwardSweep({
    classifierVersion: params.classifierVersion,
    forceValidity: params.forceValidity,
    maxCandidatesPerCall: runWalkforward ? params.maxCandidatesPerCall : 0,
    candidateFetchLimit: runWalkforward ? params.candidateFetchLimit : 0,
    effectiveTrials: params.effectiveTrials,
    windowToMs: params.windowToMs,
    candleCacheRef: runWalkforward ? params.candleCacheRef : undefined,
    candleCacheSoftCap: runWalkforward ? params.candleCacheSoftCap : undefined,
    autoBackfillCandles: runWalkforward ? params.autoBackfillCandles : false,
    minCandleCoverageRatio: params.minCandleCoverageRatio,
    candleBackfillChunkWeeks: params.candleBackfillChunkWeeks,
    candleBackfillMaxRequestsPerChunk: runWalkforward ? params.candleBackfillMaxRequestsPerChunk : 0,
    workClaimLeaseMs: params.workClaimLeaseMs,
    progressIntervalMs: runWalkforward ? params.progressIntervalMs : 0,
    onProgress: runWalkforward ? params.onProgress : undefined,
  });
  return {
    ok: true,
    busy: false,
    jobKind: "v4_research",
    processed: walkforward.processed,
    succeeded: walkforward.eligible + walkforward.ineligible,
    failed: 0,
    pendingAfter: 0,
    details: {
      weeklyBuild,
      walkforward,
    },
  };
}
