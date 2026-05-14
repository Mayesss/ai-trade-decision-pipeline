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
  onProgress?: (event: ScalpV4WalkforwardProgressEvent) => void;
} = {}): Promise<ScalpV4ResearchJobResult> {
  const weeklyBuild = await ensureScalpV4WeeklyRegimesBuilt({
    classifierVersion: params.classifierVersion,
    forceValidity: params.forceValidity,
  });
  const walkforward = await runScalpV4WalkforwardSweep({
    classifierVersion: params.classifierVersion,
    forceValidity: params.forceValidity,
    maxCandidatesPerCall: params.maxCandidatesPerCall,
    candidateFetchLimit: params.candidateFetchLimit,
    effectiveTrials: params.effectiveTrials,
    windowToMs: params.windowToMs,
    candleCacheRef: params.candleCacheRef,
    candleCacheSoftCap: params.candleCacheSoftCap,
    autoBackfillCandles: params.autoBackfillCandles,
    minCandleCoverageRatio: params.minCandleCoverageRatio,
    candleBackfillChunkWeeks: params.candleBackfillChunkWeeks,
    candleBackfillMaxRequestsPerChunk: params.candleBackfillMaxRequestsPerChunk,
    workClaimLeaseMs: params.workClaimLeaseMs,
    progressIntervalMs: params.progressIntervalMs,
    onProgress: params.onProgress,
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
