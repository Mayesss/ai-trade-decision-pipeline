import type { ScalpStrategyConfig } from '../types';
import { hssIctM15M3Strategy } from './hssIctM15M3';
import type { ScalpStrategyDefinition, ScalpStrategyPhaseInput } from './types';

function toGuardedConfig(cfg: ScalpStrategyConfig): ScalpStrategyConfig {
    return {
        ...cfg,
        sweep: {
            ...cfg.sweep,
            bufferPips: Math.max(cfg.sweep.bufferPips, 0.2),
            rejectMaxBars: Math.max(4, Math.min(cfg.sweep.rejectMaxBars, 12)),
            minWickBodyRatio: Math.max(cfg.sweep.minWickBodyRatio, 1.1),
        },
        confirm: {
            ...cfg.confirm,
            displacementBodyAtrMult: Math.max(cfg.confirm.displacementBodyAtrMult, 0.12),
            displacementRangeAtrMult: Math.max(cfg.confirm.displacementRangeAtrMult, 0.24),
            mssLookbackBars: Math.max(cfg.confirm.mssLookbackBars, 2),
            ttlMinutes: Math.max(20, Math.min(cfg.confirm.ttlMinutes, 75)),
        },
        ifvg: {
            ...cfg.ifvg,
            minAtrMult: Math.max(cfg.ifvg.minAtrMult, 0.05),
            maxAtrMult: Math.max(0.2, Math.min(cfg.ifvg.maxAtrMult, 2)),
            entryMode: cfg.ifvg.entryMode === 'first_touch' ? 'midline_touch' : cfg.ifvg.entryMode,
        },
        risk: {
            ...cfg.risk,
            maxTradesPerSymbolPerDay: 1,
        },
    };
}

function applyPhaseDetectors(input: ScalpStrategyPhaseInput) {
    return hssIctM15M3Strategy.applyPhaseDetectors({
        ...input,
        cfg: toGuardedConfig(input.cfg),
    });
}

export const HSS_ICT_M15_M3_GUARDED_STRATEGY_ID = 'hss_ict_m15_m3_guarded';

export const hssIctM15M3GuardedStrategy: ScalpStrategyDefinition = {
    id: HSS_ICT_M15_M3_GUARDED_STRATEGY_ID,
    shortName: 'HSS-ICT Guarded',
    longName: 'Hybrid Session-Scoped ICT Scalp Guarded (M15/M3)',
    applyPhaseDetectors,
};
