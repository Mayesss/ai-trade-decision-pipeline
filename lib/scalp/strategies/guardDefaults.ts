import type { ScalpReplayRuntimeConfig } from '../replay/types';
import type { ScalpStrategyConfig } from '../types';
import { applyBtcusdtGuardRiskDefaultsToReplayRuntime, applyBtcusdtGuardRiskDefaultsToStrategyConfig } from './regimePullbackM15M3BtcusdtGuarded';
import { applyXauusdGuardRiskDefaultsToReplayRuntime, applyXauusdGuardRiskDefaultsToStrategyConfig } from './regimePullbackM15M3XauusdGuarded';

export function applySymbolGuardRiskDefaultsToReplayRuntime(runtime: ScalpReplayRuntimeConfig): ScalpReplayRuntimeConfig {
    const withXauDefaults = applyXauusdGuardRiskDefaultsToReplayRuntime(runtime);
    return applyBtcusdtGuardRiskDefaultsToReplayRuntime(withXauDefaults);
}

export function applySymbolGuardRiskDefaultsToStrategyConfig(params: {
    cfg: ScalpStrategyConfig;
    symbol: string;
    strategyId: string;
}): ScalpStrategyConfig {
    const withXauDefaults = applyXauusdGuardRiskDefaultsToStrategyConfig(params);
    return applyBtcusdtGuardRiskDefaultsToStrategyConfig({
        ...params,
        cfg: withXauDefaults,
    });
}
