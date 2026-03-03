import type { ScalpMarketSnapshot, ScalpSessionState, ScalpSessionWindows, ScalpStrategyConfig } from '../types';

export interface ScalpStrategyPhaseInput {
    state: ScalpSessionState;
    market: ScalpMarketSnapshot;
    windows: ScalpSessionWindows;
    nowMs: number;
    cfg: ScalpStrategyConfig;
}

export interface ScalpStrategyPhaseOutput {
    state: ScalpSessionState;
    reasonCodes: string[];
}

export interface ScalpStrategyDefinition {
    id: string;
    shortName: string;
    longName: string;
    applyPhaseDetectors(input: ScalpStrategyPhaseInput): ScalpStrategyPhaseOutput;
}
