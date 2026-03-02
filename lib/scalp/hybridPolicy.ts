import defaultTickerEpicMap from '../../data/capitalTickerMap.json';
import rawPolicy from '../../data/scalp-hybrid-policy.json';
import type { ScalpStrategyConfigOverride } from './config';

export interface ScalpHybridPolicy {
    version: number;
    defaultProfile: string;
    profiles: Record<string, ScalpStrategyConfigOverride>;
    symbolProfiles: Record<string, string>;
    symbols: string[];
}

const DEFAULT_POLICY: ScalpHybridPolicy = {
    version: 1,
    defaultProfile: 'baseline',
    profiles: { baseline: {} },
    symbolProfiles: {},
    symbols: Object.keys(defaultTickerEpicMap as Record<string, string>).map((symbol) => symbol.toUpperCase()),
};

function normalizeSymbol(value: string): string {
    return String(value || '')
        .trim()
        .toUpperCase()
        .replace(/[^A-Z0-9._-]/g, '');
}

function isRecord(value: unknown): value is Record<string, unknown> {
    return Boolean(value) && typeof value === 'object' && !Array.isArray(value);
}

function parsePolicy(raw: unknown): ScalpHybridPolicy {
    if (!isRecord(raw)) return DEFAULT_POLICY;
    const defaultProfileRaw = String(raw.defaultProfile || DEFAULT_POLICY.defaultProfile).trim() || DEFAULT_POLICY.defaultProfile;

    const profilesRaw = isRecord(raw.profiles) ? raw.profiles : {};
    const profiles: Record<string, ScalpStrategyConfigOverride> = {};
    for (const [profileName, profileOverride] of Object.entries(profilesRaw)) {
        const normalizedName = String(profileName || '').trim();
        if (!normalizedName) continue;
        if (isRecord(profileOverride)) {
            profiles[normalizedName] = profileOverride as ScalpStrategyConfigOverride;
            continue;
        }
        profiles[normalizedName] = {};
    }
    if (!profiles[defaultProfileRaw]) {
        profiles[defaultProfileRaw] = {};
    }

    const symbolProfilesRaw = isRecord(raw.symbolProfiles) ? raw.symbolProfiles : {};
    const symbolProfiles: Record<string, string> = {};
    for (const [symbolRaw, profileRaw] of Object.entries(symbolProfilesRaw)) {
        const symbol = normalizeSymbol(symbolRaw);
        const profile = String(profileRaw || '').trim();
        if (!symbol || !profile) continue;
        if (!profiles[profile]) continue;
        symbolProfiles[symbol] = profile;
    }

    const symbolsRaw = Array.isArray(raw.symbols) ? raw.symbols : Object.keys(defaultTickerEpicMap as Record<string, string>);
    const symbols = symbolsRaw
        .map((symbol) => normalizeSymbol(String(symbol || '')))
        .filter((symbol) => Boolean(symbol));
    const uniqueSymbols = Array.from(new Set(symbols.length ? symbols : DEFAULT_POLICY.symbols));

    const version = Number.isFinite(Number(raw.version)) ? Number(raw.version) : DEFAULT_POLICY.version;
    return {
        version,
        defaultProfile: defaultProfileRaw,
        profiles,
        symbolProfiles,
        symbols: uniqueSymbols,
    };
}

export function getScalpHybridPolicy(): ScalpHybridPolicy {
    return parsePolicy(rawPolicy);
}

export function listScalpHybridSymbols(policy: ScalpHybridPolicy = getScalpHybridPolicy()): string[] {
    return policy.symbols.slice();
}

export function resolveScalpHybridSelection(
    symbolRaw: string,
    policy: ScalpHybridPolicy = getScalpHybridPolicy(),
    forcedProfile?: string,
): {
    symbol: string;
    profile: string;
    configOverride: ScalpStrategyConfigOverride;
} {
    const symbol = normalizeSymbol(symbolRaw);
    if (!symbol) throw new Error('Invalid symbol for hybrid selection');
    const forced = String(forcedProfile || '').trim();
    const profile =
        (forced && policy.profiles[forced] ? forced : '') ||
        policy.symbolProfiles[symbol] ||
        policy.defaultProfile;
    return {
        symbol,
        profile,
        configOverride: policy.profiles[profile] || {},
    };
}

