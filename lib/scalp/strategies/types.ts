import type {
    ScalpBaseTimeframe,
    ScalpConfirmTimeframe,
    ScalpMarketSnapshot,
    ScalpSessionState,
    ScalpSessionWindows,
    ScalpStrategyConfig,
} from '../types';

export type ScalpStrategyEntryIntent =
    | {
          model: 'ifvg_touch';
      }
    | {
          model: 'structure_level';
          side: 'BUY' | 'SELL';
          entryMode: 'market' | 'limit_retest';
          entryReferencePrice: number;
          stopPrice: number;
          takeProfitPrice?: number | null;
          setupKey: string;
          reasonCodes?: string[];
          metadata?: Record<string, unknown>;
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
    meta?: Record<string, unknown>;
}

export interface ScalpStrategyDefinition {
    id: string;
    shortName: string;
    longName: string;
    preferredBaseTf?: ScalpBaseTimeframe;
    preferredConfirmTf?: ScalpConfirmTimeframe;
    applyPhaseDetectors(input: ScalpStrategyPhaseInput): ScalpStrategyPhaseOutput;
}
