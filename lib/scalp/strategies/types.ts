import type { ScalpMarketSnapshot, ScalpSessionState, ScalpSessionWindows, ScalpStrategyConfig } from '../types';

export type ScalpStrategyEntryIntent = {
    model: 'ifvg_touch';
};

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
    entryIntent?: ScalpStrategyEntryIntent | null;
}

export interface ScalpStrategyDefinition {
    id: string;
    shortName: string;
    longName: string;
    applyPhaseDetectors(input: ScalpStrategyPhaseInput): ScalpStrategyPhaseOutput;
}
