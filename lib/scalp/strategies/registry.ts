import { hssIctM15M3GuardedStrategy } from './hssIctM15M3Guarded';
import { HSS_ICT_M15_M3_STRATEGY_ID, hssIctM15M3Strategy } from './hssIctM15M3';
import type { ScalpStrategyDefinition } from './types';

export const DEFAULT_SCALP_STRATEGY_ID = HSS_ICT_M15_M3_STRATEGY_ID;

const REGISTRY: Record<string, ScalpStrategyDefinition> = Object.freeze({
    [hssIctM15M3Strategy.id]: hssIctM15M3Strategy,
    [hssIctM15M3GuardedStrategy.id]: hssIctM15M3GuardedStrategy,
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

export function getDefaultScalpStrategy(): ScalpStrategyDefinition {
    const strategy = getScalpStrategyById(DEFAULT_SCALP_STRATEGY_ID);
    if (!strategy) {
        throw new Error(`Default scalp strategy not found: ${DEFAULT_SCALP_STRATEGY_ID}`);
    }
    return strategy;
}
