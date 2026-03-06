import { compressionBreakoutPullbackM15M3Strategy } from './compressionBreakoutPullbackM15M3';
import { failedAuctionExtremeReversalM15M1Strategy } from './failedAuctionExtremeReversalM15M1';
import { REGIME_PULLBACK_M15_M3_STRATEGY_ID, regimePullbackM15M3Strategy } from './regimePullbackM15M3';
import { regimePullbackM15M3BtcusdtGuardedStrategy } from './regimePullbackM15M3BtcusdtGuarded';
import { regimePullbackM15M3XauusdGuardedStrategy } from './regimePullbackM15M3XauusdGuarded';
import { hssIctM15M3GuardedStrategy } from './hssIctM15M3Guarded';
import { openingRangeBreakoutRetestM5M1Strategy } from './openingRangeBreakoutRetestM5M1';
import { pdhPdlReclaimM15M3Strategy } from './pdhPdlReclaimM15M3';
import { trendDayReaccelerationM15M3Strategy } from './trendDayReaccelerationM15M3';
import type { ScalpStrategyDefinition } from './types';

export const DEFAULT_SCALP_STRATEGY_ID = REGIME_PULLBACK_M15_M3_STRATEGY_ID;

const REGISTRY: Record<string, ScalpStrategyDefinition> = Object.freeze({
    [regimePullbackM15M3Strategy.id]: regimePullbackM15M3Strategy,
    [regimePullbackM15M3BtcusdtGuardedStrategy.id]: regimePullbackM15M3BtcusdtGuardedStrategy,
    [regimePullbackM15M3XauusdGuardedStrategy.id]: regimePullbackM15M3XauusdGuardedStrategy,
    [hssIctM15M3GuardedStrategy.id]: hssIctM15M3GuardedStrategy,
    [openingRangeBreakoutRetestM5M1Strategy.id]: openingRangeBreakoutRetestM5M1Strategy,
    [pdhPdlReclaimM15M3Strategy.id]: pdhPdlReclaimM15M3Strategy,
    [compressionBreakoutPullbackM15M3Strategy.id]: compressionBreakoutPullbackM15M3Strategy,
    [failedAuctionExtremeReversalM15M1Strategy.id]: failedAuctionExtremeReversalM15M1Strategy,
    [trendDayReaccelerationM15M3Strategy.id]: trendDayReaccelerationM15M3Strategy,
});

export function listScalpStrategies(): ScalpStrategyDefinition[] {
    return Object.values(REGISTRY);
}

export function getScalpStrategyById(id: string): ScalpStrategyDefinition | null {
    const normalized = normalizeScalpStrategyId(id);
    if (!normalized) return null;
    return REGISTRY[normalized] || null;
}

export function getScalpStrategyPreferredTimeframes(id: string): {
    asiaBaseTf: 'M1' | 'M3' | 'M5' | 'M15';
    confirmTf: 'M1' | 'M3';
} | null {
    const strategy = getScalpStrategyById(id);
    if (!strategy) return null;
    return {
        asiaBaseTf: strategy.preferredBaseTf ?? 'M15',
        confirmTf: strategy.preferredConfirmTf ?? 'M3',
    };
}

export function normalizeScalpStrategyId(value: unknown): string {
    return String(value || '')
        .trim()
        .toLowerCase()
        .replace(/[^a-z0-9._-]/g, '');
}

export function resolveScalpStrategyIdForSymbol(params: {
    symbol: string;
    fallbackStrategyId?: string;
}): string {
    const fallback = getScalpStrategyById(params.fallbackStrategyId || '');
    if (fallback) return fallback.id;
    return getDefaultScalpStrategy().id;
}

export function getDefaultScalpStrategy(): ScalpStrategyDefinition {
    const strategy = getScalpStrategyById(DEFAULT_SCALP_STRATEGY_ID);
    if (!strategy) {
        throw new Error(`Default scalp strategy not found: ${DEFAULT_SCALP_STRATEGY_ID}`);
    }
    return strategy;
}
