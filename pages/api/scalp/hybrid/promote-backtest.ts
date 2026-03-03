export const config = { runtime: 'nodejs' };

import { readFile, writeFile } from 'node:fs/promises';

import type { NextApiRequest, NextApiResponse } from 'next';

import { requireAdminAccess } from '../../../../lib/admin';
import type { ScalpStrategyConfigOverride } from '../../../../lib/scalp/config';
import { getScalpHybridPolicy, scalpHybridPolicyPath } from '../../../../lib/scalp/hybridPolicy';

type PromoteBody = {
    symbol?: string;
    profile?: string;
    applyProfileParams?: boolean | string;
    replaceProfile?: boolean | string;
    includeTimeframes?: boolean | string;
    dryRun?: boolean | string;
    effectiveConfig?: unknown;
    profileOverride?: unknown;
};

type PromoteResult = {
    ok: boolean;
    dryRun: boolean;
    promoted: {
        symbol: string;
        profile: string;
        applyProfileParams: boolean;
        replaceProfile: boolean;
    };
    policy: {
        path: string;
        version: number | null;
        updatedAt: string | null;
        defaultProfile: string;
        profiles: string[];
        symbolProfileCount: number;
        symbolProfile: string | null;
    };
    profileOverrideApplied: ScalpStrategyConfigOverride | null;
};

function isRecord(value: unknown): value is Record<string, unknown> {
    return Boolean(value) && typeof value === 'object' && !Array.isArray(value);
}

function toBool(value: unknown, fallback: boolean): boolean {
    if (typeof value === 'boolean') return value;
    if (typeof value === 'string') {
        const normalized = value.trim().toLowerCase();
        if (['true', '1', 'yes', 'on'].includes(normalized)) return true;
        if (['false', '0', 'no', 'off'].includes(normalized)) return false;
    }
    return fallback;
}

function toPositiveNumber(value: unknown): number | null {
    const n = Number(value);
    if (!Number.isFinite(n) || n <= 0) return null;
    return n;
}

function toNonNegativeNumber(value: unknown): number | null {
    const n = Number(value);
    if (!Number.isFinite(n) || n < 0) return null;
    return n;
}

function toPositiveInt(value: unknown): number | null {
    const n = toPositiveNumber(value);
    if (n === null) return null;
    return Math.max(1, Math.floor(n));
}

function normalizeSymbol(value: unknown): string {
    return String(value || '')
        .trim()
        .toUpperCase()
        .replace(/[^A-Z0-9._-]/g, '');
}

function normalizeProfileName(value: unknown): string {
    const normalized = String(value || '')
        .trim()
        .replace(/\s+/g, '_')
        .replace(/[^a-zA-Z0-9_-]/g, '')
        .toLowerCase();
    return normalized.slice(0, 64);
}

function deepClone<T>(value: T): T {
    return JSON.parse(JSON.stringify(value)) as T;
}

function deepMergeMutable(target: Record<string, unknown>, source: Record<string, unknown>): Record<string, unknown> {
    for (const [key, rawValue] of Object.entries(source)) {
        if (rawValue === undefined) continue;
        const targetValue = target[key];
        if (Array.isArray(rawValue)) {
            target[key] = rawValue.slice();
            continue;
        }
        if (isRecord(rawValue) && isRecord(targetValue)) {
            target[key] = deepMergeMutable({ ...targetValue }, rawValue);
            continue;
        }
        target[key] = rawValue;
    }
    return target;
}

function compactObject(value: unknown): unknown {
    if (Array.isArray(value)) {
        return value.map((entry) => compactObject(entry));
    }
    if (!isRecord(value)) return value;
    const next: Record<string, unknown> = {};
    for (const [key, raw] of Object.entries(value)) {
        if (raw === undefined) continue;
        const compacted = compactObject(raw);
        if (isRecord(compacted) && Object.keys(compacted).length === 0) continue;
        next[key] = compacted;
    }
    return next;
}

function parseEntryMode(value: unknown): 'first_touch' | 'midline_touch' | 'full_fill' | null {
    const normalized = String(value || '')
        .trim()
        .toLowerCase();
    if (normalized === 'first_touch' || normalized === 'midline_touch' || normalized === 'full_fill') {
        return normalized;
    }
    return null;
}

function buildProfileOverrideFromEffectiveConfig(
    effectiveConfig: unknown,
    opts: { includeTimeframes: boolean },
): ScalpStrategyConfigOverride | null {
    if (!isRecord(effectiveConfig)) return null;
    const strategy = isRecord(effectiveConfig.strategy) ? effectiveConfig.strategy : {};

    const risk: Record<string, unknown> = {};
    const maxTrades = toPositiveInt(strategy.maxTradesPerDay);
    if (maxTrades !== null) risk.maxTradesPerSymbolPerDay = maxTrades;
    const riskPct = toPositiveNumber(strategy.riskPerTradePct);
    if (riskPct !== null) risk.riskPerTradePct = riskPct;
    const referenceEquityUsd = toPositiveNumber(strategy.referenceEquityUsd);
    if (referenceEquityUsd !== null) risk.referenceEquityUsd = referenceEquityUsd;
    const minNotionalUsd = toPositiveNumber(strategy.minNotionalUsd);
    if (minNotionalUsd !== null) risk.minNotionalUsd = minNotionalUsd;
    const maxNotionalUsd = toPositiveNumber(strategy.maxNotionalUsd);
    if (maxNotionalUsd !== null) risk.maxNotionalUsd = maxNotionalUsd;
    const takeProfitR = toPositiveNumber(strategy.takeProfitR);
    if (takeProfitR !== null) risk.takeProfitR = takeProfitR;
    const stopBufferPips = toNonNegativeNumber(strategy.stopBufferPips);
    if (stopBufferPips !== null) risk.stopBufferPips = stopBufferPips;
    const stopBufferSpreadMult = toNonNegativeNumber(strategy.stopBufferSpreadMult);
    if (stopBufferSpreadMult !== null) risk.stopBufferSpreadMult = stopBufferSpreadMult;
    const minStopDistancePips = toPositiveNumber(strategy.minStopDistancePips);
    if (minStopDistancePips !== null) risk.minStopDistancePips = minStopDistancePips;

    const sweep: Record<string, unknown> = {};
    const sweepBufferPips = toNonNegativeNumber(strategy.sweepBufferPips);
    if (sweepBufferPips !== null) sweep.bufferPips = sweepBufferPips;
    const sweepBufferAtrMult = toNonNegativeNumber(strategy.sweepBufferAtrMult);
    if (sweepBufferAtrMult !== null) sweep.bufferAtrMult = sweepBufferAtrMult;
    const sweepBufferSpreadMult = toNonNegativeNumber(strategy.sweepBufferSpreadMult);
    if (sweepBufferSpreadMult !== null) sweep.bufferSpreadMult = sweepBufferSpreadMult;
    const sweepRejectInsidePips = toNonNegativeNumber(strategy.sweepRejectInsidePips);
    if (sweepRejectInsidePips !== null) sweep.rejectInsidePips = sweepRejectInsidePips;
    const sweepRejectMaxBars = toPositiveInt(strategy.sweepRejectMaxBars);
    if (sweepRejectMaxBars !== null) sweep.rejectMaxBars = sweepRejectMaxBars;
    const sweepMinWickBodyRatio = toNonNegativeNumber(strategy.sweepMinWickBodyRatio);
    if (sweepMinWickBodyRatio !== null) sweep.minWickBodyRatio = sweepMinWickBodyRatio;

    const confirm: Record<string, unknown> = {};
    const displacementBodyAtrMult = toNonNegativeNumber(strategy.displacementBodyAtrMult);
    if (displacementBodyAtrMult !== null) confirm.displacementBodyAtrMult = displacementBodyAtrMult;
    const displacementRangeAtrMult = toNonNegativeNumber(strategy.displacementRangeAtrMult);
    if (displacementRangeAtrMult !== null) confirm.displacementRangeAtrMult = displacementRangeAtrMult;
    const displacementCloseInExtremePct = toPositiveNumber(strategy.displacementCloseInExtremePct);
    if (displacementCloseInExtremePct !== null) confirm.closeInExtremePct = displacementCloseInExtremePct;
    const mssLookbackBars = toPositiveInt(strategy.mssLookbackBars);
    if (mssLookbackBars !== null) confirm.mssLookbackBars = mssLookbackBars;
    const mssBreakBufferPips = toNonNegativeNumber(strategy.mssBreakBufferPips);
    if (mssBreakBufferPips !== null) confirm.mssBreakBufferPips = mssBreakBufferPips;
    const mssBreakBufferAtrMult = toNonNegativeNumber(strategy.mssBreakBufferAtrMult);
    if (mssBreakBufferAtrMult !== null) confirm.mssBreakBufferAtrMult = mssBreakBufferAtrMult;
    const confirmTtlMinutes = toPositiveInt(strategy.confirmTtlMinutes);
    if (confirmTtlMinutes !== null) confirm.ttlMinutes = confirmTtlMinutes;

    const ifvg: Record<string, unknown> = {};
    const ifvgMinAtrMult = toNonNegativeNumber(strategy.ifvgMinAtrMult);
    if (ifvgMinAtrMult !== null) ifvg.minAtrMult = ifvgMinAtrMult;
    const ifvgMaxAtrMult = toPositiveNumber(strategy.ifvgMaxAtrMult);
    if (ifvgMaxAtrMult !== null) ifvg.maxAtrMult = ifvgMaxAtrMult;
    const ifvgTtlMinutes = toPositiveInt(strategy.ifvgTtlMinutes);
    if (ifvgTtlMinutes !== null) ifvg.ttlMinutes = ifvgTtlMinutes;
    const ifvgEntryMode = parseEntryMode(strategy.ifvgEntryMode);
    if (ifvgEntryMode) ifvg.entryMode = ifvgEntryMode;

    const override: Record<string, unknown> = {};
    if (Object.keys(risk).length) override.risk = risk;
    if (Object.keys(sweep).length) override.sweep = sweep;
    if (Object.keys(confirm).length) override.confirm = confirm;
    if (Object.keys(ifvg).length) override.ifvg = ifvg;

    if (opts.includeTimeframes) {
        const timeframes: Record<string, unknown> = {};
        const asiaBaseTf = String(strategy.asiaBaseTf || '')
            .trim()
            .toUpperCase();
        if (asiaBaseTf === 'M1' || asiaBaseTf === 'M3' || asiaBaseTf === 'M5' || asiaBaseTf === 'M15') {
            timeframes.asiaBase = asiaBaseTf;
        }
        const confirmTf = String(strategy.confirmTf || '')
            .trim()
            .toUpperCase();
        if (confirmTf === 'M1' || confirmTf === 'M3') {
            timeframes.confirm = confirmTf;
        }
        if (Object.keys(timeframes).length) {
            override.timeframes = timeframes;
        }
    }

    const compacted = compactObject(override);
    if (!isRecord(compacted) || Object.keys(compacted).length === 0) return null;
    return compacted as ScalpStrategyConfigOverride;
}

function readRawPolicySafe(raw: unknown): Record<string, unknown> {
    if (isRecord(raw)) return deepClone(raw);
    return {};
}

function toClientError(message: string, status: number, error: string): { status: number; body: Record<string, unknown> } {
    return {
        status,
        body: {
            error,
            message,
        },
    };
}

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
    if (req.method !== 'POST') {
        return res.status(405).json({ error: 'method_not_allowed', message: 'Use POST' });
    }
    if (!requireAdminAccess(req, res)) return;

    try {
        const body = (req.body || {}) as PromoteBody;
        const symbol = normalizeSymbol(body.symbol);
        const profile = normalizeProfileName(body.profile || 'loose');
        const applyProfileParams = toBool(body.applyProfileParams, false);
        const replaceProfile = toBool(body.replaceProfile, false);
        const includeTimeframes = toBool(body.includeTimeframes, false);
        const dryRun = toBool(body.dryRun, false);

        if (!symbol) {
            const out = toClientError('Provide a valid symbol.', 400, 'symbol_required');
            return res.status(out.status).json(out.body);
        }
        if (!profile) {
            const out = toClientError('Provide a valid target profile.', 400, 'profile_required');
            return res.status(out.status).json(out.body);
        }

        let profilePatch: ScalpStrategyConfigOverride | null = null;
        if (applyProfileParams) {
            if (isRecord(body.profileOverride)) {
                profilePatch = compactObject(body.profileOverride) as ScalpStrategyConfigOverride;
            } else {
                profilePatch = buildProfileOverrideFromEffectiveConfig(body.effectiveConfig, {
                    includeTimeframes,
                });
            }
            if (!profilePatch || !Object.keys(profilePatch).length) {
                const out = toClientError(
                    'applyProfileParams=true requires profileOverride or effectiveConfig with tunable strategy fields.',
                    400,
                    'profile_override_missing',
                );
                return res.status(out.status).json(out.body);
            }
        }

        const policyPath = scalpHybridPolicyPath();
        const text = await readFile(policyPath, 'utf8');
        const raw = readRawPolicySafe(JSON.parse(text));

        const next = deepClone(raw);
        const profiles = isRecord(next.profiles) ? { ...next.profiles } : {};
        const symbolProfiles = isRecord(next.symbolProfiles) ? { ...next.symbolProfiles } : {};
        const symbols = Array.isArray(next.symbols) ? next.symbols.slice() : [];

        if (!isRecord(profiles[profile])) {
            profiles[profile] = {};
        }

        if (applyProfileParams && profilePatch) {
            const existing = isRecord(profiles[profile]) ? deepClone(profiles[profile]) : {};
            profiles[profile] = replaceProfile
                ? deepClone(profilePatch as unknown as Record<string, unknown>)
                : deepMergeMutable(existing, profilePatch as unknown as Record<string, unknown>);
        }

        symbolProfiles[symbol] = profile;

        const symbolSet = new Set(
            symbols
                .map((row) => normalizeSymbol(row))
                .filter((row) => Boolean(row)),
        );
        symbolSet.add(symbol);

        next.profiles = profiles;
        next.symbolProfiles = symbolProfiles;
        next.symbols = Array.from(symbolSet);
        if (typeof next.defaultProfile !== 'string' || !String(next.defaultProfile).trim()) {
            next.defaultProfile = 'baseline';
        }
        const currentVersion = Number(next.version);
        const safeCurrentVersion = Number.isFinite(currentVersion) ? Math.max(1, Math.floor(currentVersion)) : 1;
        if (!dryRun) {
            next.version = safeCurrentVersion + 1;
            next.updatedAt = new Date().toISOString();
        } else {
            next.version = safeCurrentVersion;
        }

        if (!dryRun) {
            const serialized = `${JSON.stringify(next, null, 2)}\n`;
            await writeFile(policyPath, serialized, 'utf8');
        }

        const parsed = getScalpHybridPolicy();
        const result: PromoteResult = {
            ok: true,
            dryRun,
            promoted: {
                symbol,
                profile,
                applyProfileParams,
                replaceProfile,
            },
            policy: {
                path: policyPath,
                version: Number.isFinite(Number(next.version)) ? Number(next.version) : null,
                updatedAt: typeof next.updatedAt === 'string' ? next.updatedAt : null,
                defaultProfile: parsed.defaultProfile,
                profiles: Object.keys(parsed.profiles),
                symbolProfileCount: Object.keys(parsed.symbolProfiles).length,
                symbolProfile: parsed.symbolProfiles[symbol] || null,
            },
            profileOverrideApplied: applyProfileParams ? (parsed.profiles[profile] || null) : null,
        };
        return res.status(200).json(result);
    } catch (err: any) {
        const code = String(err?.code || '');
        if (code === 'EROFS' || code === 'EACCES' || code === 'EPERM') {
            return res.status(500).json({
                error: 'policy_write_failed',
                message: 'Policy file could not be written in this runtime. Run this endpoint in a writable environment.',
            });
        }
        console.error('Error in /api/scalp/hybrid/promote-backtest:', err);
        return res.status(500).json({
            error: 'promote_backtest_failed',
            message: err?.message || String(err),
        });
    }
}
