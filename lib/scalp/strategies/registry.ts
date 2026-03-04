import { REGIME_PULLBACK_M15_M3_STRATEGY_ID, regimePullbackM15M3Strategy } from './regimePullbackM15M3';
import { regimePullbackM15M3BtcusdtGuardedStrategy } from './regimePullbackM15M3BtcusdtGuarded';
import { regimePullbackM15M3XauusdGuardedStrategy } from './regimePullbackM15M3XauusdGuarded';
import type { ScalpStrategyDefinition } from './types';

export const DEFAULT_SCALP_STRATEGY_ID = REGIME_PULLBACK_M15_M3_STRATEGY_ID;

const REGISTRY: Record<string, ScalpStrategyDefinition> = Object.freeze({
    [regimePullbackM15M3Strategy.id]: regimePullbackM15M3Strategy,
    [regimePullbackM15M3BtcusdtGuardedStrategy.id]: regimePullbackM15M3BtcusdtGuardedStrategy,
    [regimePullbackM15M3XauusdGuardedStrategy.id]: regimePullbackM15M3XauusdGuardedStrategy,
});

export function listScalpStrategies(): ScalpStrategyDefinition[] {
    return Object.values(REGISTRY);
}

export function getScalpStrategyById(id: string): ScalpStrategyDefinition | null {
    const normalized = normalizeScalpStrategyId(id);
    if (!normalized) return null;
    return REGISTRY[normalized] || null;
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
