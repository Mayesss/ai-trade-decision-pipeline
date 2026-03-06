import type { ScalpDeploymentRef } from './types';
import { getDefaultScalpStrategy, getScalpStrategyById, normalizeScalpStrategyId } from './strategies/registry';

export const DEFAULT_SCALP_TUNE_ID = 'default';

function normalizeScalpSymbolPart(value: unknown): string | null {
    const normalized = String(value || '')
        .trim()
        .toUpperCase()
        .replace(/[^A-Z0-9._-]/g, '');
    return normalized || null;
}

function resolveStrategyId(value: unknown, fallback = getDefaultScalpStrategy().id): string {
    const normalized = normalizeScalpStrategyId(value);
    if (!normalized) return fallback;
    return getScalpStrategyById(normalized)?.id || fallback;
}

export function normalizeScalpTuneId(value: unknown, fallback = DEFAULT_SCALP_TUNE_ID): string {
    const normalized = String(value || '')
        .trim()
        .toLowerCase()
        .replace(/[^a-z0-9._-]/g, '-')
        .replace(/-{2,}/g, '-')
        .replace(/^[-._]+|[-._]+$/g, '')
        .slice(0, 80);
    return normalized || fallback;
}

export function buildScalpDeploymentId(params: {
    symbol: string;
    strategyId: string;
    tuneId?: string | null;
}): string {
    const symbol = normalizeScalpSymbolPart(params.symbol) || 'EURUSD';
    const strategyId = resolveStrategyId(params.strategyId);
    const tuneId = normalizeScalpTuneId(params.tuneId);
    return `${symbol}~${strategyId}~${tuneId}`;
}

export function parseScalpDeploymentId(value: unknown): ScalpDeploymentRef | null {
    const normalized = String(value || '').trim();
    if (!normalized) return null;
    const parts = normalized.split('~');
    if (parts.length !== 3) return null;
    const [symbolRaw, strategyRaw, tuneRaw] = parts;
    const symbol = normalizeScalpSymbolPart(symbolRaw);
    const strategyId = normalizeScalpStrategyId(strategyRaw);
    const tuneId = normalizeScalpTuneId(tuneRaw);
    if (!symbol || !strategyId) return null;
    return {
        symbol,
        strategyId,
        tuneId,
        deploymentId: buildScalpDeploymentId({ symbol, strategyId, tuneId }),
        tuneLabel: tuneId,
    };
}

export function resolveScalpDeployment(params: {
    symbol?: unknown;
    strategyId?: unknown;
    tuneId?: unknown;
    deploymentId?: unknown;
    fallbackSymbol?: string;
    fallbackStrategyId?: string;
    fallbackTuneId?: string;
}): ScalpDeploymentRef {
    const parsed = parseScalpDeploymentId(params.deploymentId);
    const symbol =
        normalizeScalpSymbolPart(params.symbol) ||
        parsed?.symbol ||
        normalizeScalpSymbolPart(params.fallbackSymbol) ||
        'EURUSD';
    const strategyId = resolveStrategyId(
        params.strategyId ?? parsed?.strategyId,
        resolveStrategyId(params.fallbackStrategyId, getDefaultScalpStrategy().id),
    );
    const tuneId = normalizeScalpTuneId(params.tuneId ?? parsed?.tuneId, normalizeScalpTuneId(params.fallbackTuneId));

    return {
        symbol,
        strategyId,
        tuneId,
        deploymentId: buildScalpDeploymentId({ symbol, strategyId, tuneId }),
        tuneLabel: tuneId,
    };
}
