import {
  ensureScalpRegimeWeeklyRegimesBuilt,
  type ScalpRegimeWeeklyBuildResult,
} from "./build";
import {
  runScalpRegimeWalkforwardSweep,
  type ScalpRegimeWalkforwardProgressEvent,
  type ScalpRegimeWalkforwardSweepResult,
} from "./walkforwardSweep";
import type { ScalpCandle } from "../types";

export interface ScalpRegimeResearchJobResult {
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
      result?: ScalpRegimeWeeklyBuildResult;
    };
    walkforward: ScalpRegimeWalkforwardSweepResult;
  };
}

export async function runScalpRegimeResearchJob(params: {
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
  onProgress?: (event: ScalpRegimeWalkforwardProgressEvent) => void;
} = {}): Promise<ScalpRegimeResearchJobResult> {
  const weeklyBuild = await ensureScalpRegimeWeeklyRegimesBuilt({
    classifierVersion: params.classifierVersion,
    forceValidity: params.forceValidity,
  });
  const runWalkforward = params.runWalkforward !== false;
  const walkforward = await runScalpRegimeWalkforwardSweep({
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
