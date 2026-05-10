import { SCALP_V4_ONE_WEEK_MS } from "./week";
import { buildScalpV4RegimeEnvelope } from "./envelope";
import type {
  ScalpV4RegimeEnvelope,
  ScalpV4RegimeSnapshot,
  ScalpV4TradeLike,
  ScalpV4WindowResult,
} from "./types";

export interface ScalpV4WalkForwardRunnerParams {
  classifierVersion: string;
  snapshots: ScalpV4RegimeSnapshot[];
  windowFromMs: number;
  windowToMs: number;
  selectionWeeks?: number;
  stepWeeks?: number;
  effectiveTrials: number;
  runWindow(params: { windowStartMs: number; windowEndMs: number }): Promise<ScalpV4TradeLike[]>;
}

export interface ScalpV4WalkForwardRunResult {
  windows: ScalpV4WindowResult[];
  envelope: ScalpV4RegimeEnvelope;
}

export async function runScalpV4WalkForward(params: ScalpV4WalkForwardRunnerParams): Promise<ScalpV4WalkForwardRunResult> {
  const selectionWeeks = Math.max(1, Math.floor(params.selectionWeeks || 12));
  const stepWeeks = Math.max(1, Math.floor(params.stepWeeks || 1));
  const windowSpanMs = selectionWeeks * SCALP_V4_ONE_WEEK_MS;
  const stepMs = stepWeeks * SCALP_V4_ONE_WEEK_MS;
  const windows: ScalpV4WindowResult[] = [];
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
    envelope: buildScalpV4RegimeEnvelope({
      classifierVersion: params.classifierVersion,
      snapshots: params.snapshots,
      windows,
      effectiveTrials: params.effectiveTrials,
    }),
  };
}

