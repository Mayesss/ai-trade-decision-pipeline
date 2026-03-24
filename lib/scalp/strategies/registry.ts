import { compressionBreakoutPullbackM15M3Strategy } from './compressionBreakoutPullbackM15M3';
import { failedAuctionExtremeReversalM15M1Strategy } from './failedAuctionExtremeReversalM15M1';
import { adaptiveMetaSelectorM15M3Strategy } from './adaptiveMetaSelectorM15M3';
import { anchoredVwapReversionM15M3Strategy } from './anchoredVwapReversionM15M3';
import { basisDislocationReversionProxyM15M3Strategy } from './basisDislocationReversionProxyM15M3';
import { fundingOiExhaustionProxyM15M3Strategy } from './fundingOiExhaustionProxyM15M3';
import { REGIME_PULLBACK_M15_M3_STRATEGY_ID, regimePullbackM15M3Strategy } from './regimePullbackM15M3';
import { REGIME_PULLBACK_M15_M3_BTCUSDT_STRATEGY_ID } from './regimePullbackM15M3BtcusdtGuarded';
import { REGIME_PULLBACK_M15_M3_XAUUSD_STRATEGY_ID } from './regimePullbackM15M3XauusdGuarded';
import { hssIctM15M3GuardedStrategy } from './hssIctM15M3Guarded';
import { openingRangeBreakoutRetestM5M1Strategy } from './openingRangeBreakoutRetestM5M1';
import { pdhPdlReclaimM15M3Strategy } from './pdhPdlReclaimM15M3';
import { relativeValueSpreadProxyM15M3Strategy } from './relativeValueSpreadProxyM15M3';
import { sessionSeasonalityBiasM15M3Strategy } from './sessionSeasonalityBiasM15M3';
import { trendDayReaccelerationM15M3Strategy } from './trendDayReaccelerationM15M3';
import type { ScalpStrategyDefinition } from './types';

export const DEFAULT_SCALP_STRATEGY_ID = REGIME_PULLBACK_M15_M3_STRATEGY_ID;

const REGISTRY: Record<string, ScalpStrategyDefinition> = Object.freeze({
    [regimePullbackM15M3Strategy.id]: regimePullbackM15M3Strategy,
    [hssIctM15M3GuardedStrategy.id]: hssIctM15M3GuardedStrategy,
    [openingRangeBreakoutRetestM5M1Strategy.id]: openingRangeBreakoutRetestM5M1Strategy,
    [pdhPdlReclaimM15M3Strategy.id]: pdhPdlReclaimM15M3Strategy,
    [compressionBreakoutPullbackM15M3Strategy.id]: compressionBreakoutPullbackM15M3Strategy,
    [failedAuctionExtremeReversalM15M1Strategy.id]: failedAuctionExtremeReversalM15M1Strategy,
    [trendDayReaccelerationM15M3Strategy.id]: trendDayReaccelerationM15M3Strategy,
    [anchoredVwapReversionM15M3Strategy.id]: anchoredVwapReversionM15M3Strategy,
    [fundingOiExhaustionProxyM15M3Strategy.id]: fundingOiExhaustionProxyM15M3Strategy,
    [basisDislocationReversionProxyM15M3Strategy.id]: basisDislocationReversionProxyM15M3Strategy,
    [relativeValueSpreadProxyM15M3Strategy.id]: relativeValueSpreadProxyM15M3Strategy,
    [sessionSeasonalityBiasM15M3Strategy.id]: sessionSeasonalityBiasM15M3Strategy,
    [adaptiveMetaSelectorM15M3Strategy.id]: adaptiveMetaSelectorM15M3Strategy,
});

const STRATEGY_ID_ALIASES: Record<string, string> = Object.freeze({
    [REGIME_PULLBACK_M15_M3_BTCUSDT_STRATEGY_ID]: REGIME_PULLBACK_M15_M3_STRATEGY_ID,
    [REGIME_PULLBACK_M15_M3_XAUUSD_STRATEGY_ID]: REGIME_PULLBACK_M15_M3_STRATEGY_ID,
});

export function resolveScalpStrategyIdAlias(value: unknown): string {
    const normalized = normalizeScalpStrategyId(value);
    if (!normalized) return '';
    return STRATEGY_ID_ALIASES[normalized] || normalized;
}

export function listScalpStrategies(): ScalpStrategyDefinition[] {
    return Object.values(REGISTRY);
}

export function getScalpStrategyById(id: string): ScalpStrategyDefinition | null {
    const normalized = resolveScalpStrategyIdAlias(id);
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
