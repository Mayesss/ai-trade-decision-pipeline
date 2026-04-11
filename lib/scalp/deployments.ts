import type { ScalpDeploymentRef } from './types';
import { DEFAULT_SCALP_VENUE, formatScalpVenueDeploymentId, normalizeScalpVenue, parseScalpVenuePrefixedDeploymentId, type ScalpVenue } from './venue';
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
    venue?: ScalpVenue | null;
    symbol: string;
    strategyId: string;
    tuneId?: string | null;
}): string {
    const venue = normalizeScalpVenue(params.venue, DEFAULT_SCALP_VENUE);
    const symbol = normalizeScalpSymbolPart(params.symbol) || 'EURUSD';
    const strategyId = resolveStrategyId(params.strategyId);
    const tuneId = normalizeScalpTuneId(params.tuneId);
    const deploymentKey = `${symbol}~${strategyId}~${tuneId}`;
    return formatScalpVenueDeploymentId(venue, deploymentKey);
}

export function parseScalpDeploymentId(value: unknown): ScalpDeploymentRef | null {
    const parsed = parseScalpVenuePrefixedDeploymentId(value);
    if (!parsed.deploymentKey) return null;
    const parts = parsed.deploymentKey.split('~');
    if (parts.length !== 3) return null;
    const [symbolRaw, strategyRaw, tuneRaw] = parts;
    const symbol = normalizeScalpSymbolPart(symbolRaw);
    const strategyId = normalizeScalpStrategyId(strategyRaw);
    const tuneId = normalizeScalpTuneId(tuneRaw);
    if (!symbol || !strategyId) return null;
    return {
        venue: parsed.venue,
        symbol,
        strategyId,
        tuneId,
        deploymentId: buildScalpDeploymentId({ venue: parsed.venue, symbol, strategyId, tuneId }),
        tuneLabel: tuneId,
    };
}

export function resolveScalpDeploymentVenueFromId(value: unknown): ScalpVenue {
    return parseScalpVenuePrefixedDeploymentId(value).venue;
}

export function resolveScalpDeployment(params: {
    venue?: unknown;
    symbol?: unknown;
    strategyId?: unknown;
    tuneId?: unknown;
    deploymentId?: unknown;
    fallbackVenue?: ScalpVenue;
    fallbackSymbol?: string;
    fallbackStrategyId?: string;
    fallbackTuneId?: string;
}): ScalpDeploymentRef {
    const parsed = parseScalpDeploymentId(params.deploymentId);
    const venue = normalizeScalpVenue(
        params.venue ?? parsed?.venue,
        normalizeScalpVenue(params.fallbackVenue, DEFAULT_SCALP_VENUE),
    );
    const symbol =
        normalizeScalpSymbolPart(params.symbol) ||
        parsed?.symbol ||
        normalizeScalpSymbolPart(params.fallbackSymbol) ||
        'EURUSD';
    const strategyId = resolveStrategyId(
        params.strategyId ?? parsed?.strategyId,
        resolveStrategyId(params.fallbackStrategyId, getDefaultScalpStrategy().id),
    );
    // When a full deployment id is provided (for example v2 ids with session-scoped
    // tune suffixes), treat its tune segment as authoritative so we don't collapse
    // identity back to an unscoped tune id.
    const tuneId = normalizeScalpTuneId(parsed?.tuneId ?? params.tuneId, normalizeScalpTuneId(params.fallbackTuneId));

    return {
        venue,
        symbol,
        strategyId,
        tuneId,
        deploymentId: buildScalpDeploymentId({ venue, symbol, strategyId, tuneId }),
        tuneLabel: tuneId,
    };
}
