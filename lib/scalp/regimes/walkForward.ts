import { SCALP_V4_ONE_WEEK_MS } from "./week";
import { buildScalpRegimeEnvelope } from "./envelope";
import type {
  ScalpRegimeEnvelope,
  ScalpRegimeSnapshot,
  ScalpRegimeTradeLike,
  ScalpRegimeWindowResult,
} from "./types";

export interface ScalpRegimeWalkForwardRunnerParams {
  classifierVersion: string;
  snapshots: ScalpRegimeSnapshot[];
  windowFromMs: number;
  windowToMs: number;
  selectionWeeks?: number;
  stepWeeks?: number;
  effectiveTrials: number;
  runWindow(params: { windowStartMs: number; windowEndMs: number }): Promise<ScalpRegimeTradeLike[]>;
}

export interface ScalpRegimeWalkForwardRunResult {
  windows: ScalpRegimeWindowResult[];
  envelope: ScalpRegimeEnvelope;
}

export async function runScalpRegimeWalkForward(params: ScalpRegimeWalkForwardRunnerParams): Promise<ScalpRegimeWalkForwardRunResult> {
  const selectionWeeks = Math.max(1, Math.floor(params.selectionWeeks || 12));
  const stepWeeks = Math.max(1, Math.floor(params.stepWeeks || 1));
  const windowSpanMs = selectionWeeks * SCALP_V4_ONE_WEEK_MS;
  const stepMs = stepWeeks * SCALP_V4_ONE_WEEK_MS;
  const windows: ScalpRegimeWindowResult[] = [];
  for (
    let windowStartMs = params.windowFromMs;
    windowStartMs + windowSpanMs <= params.windowToMs;
    windowStartMs += stepMs
  ) {
    const windowEndMs = windowStartMs + windowSpanMs;
    const trades = await params.runWindow({ windowStartMs, windowEndMs });
    windows.push({ windowStartMs, windowEndMs, trades });
  }
  return {
    windows,
    envelope: buildScalpRegimeEnvelope({
      classifierVersion: params.classifierVersion,
      snapshots: params.snapshots,
      windows,
      effectiveTrials: params.effectiveTrials,
    }),
  };
}

