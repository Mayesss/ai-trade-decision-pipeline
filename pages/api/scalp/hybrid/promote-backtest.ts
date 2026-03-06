export const config = { runtime: 'nodejs' };

import { readFile, writeFile } from 'node:fs/promises';

import type { NextApiRequest, NextApiResponse } from 'next';

import { requireAdminAccess } from '../../../../lib/admin';
import type { ScalpStrategyConfigOverride } from '../../../../lib/scalp/config';
import { getScalpHybridPolicy, scalpHybridPolicyPath } from '../../../../lib/scalp/hybridPolicy';
import { buildScalpConfigOverrideFromEffectiveConfig, compactScalpStrategyConfigOverride } from '../../../../lib/scalp/tuning';

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
                profilePatch = compactScalpStrategyConfigOverride(body.profileOverride);
            } else {
                profilePatch = buildScalpConfigOverrideFromEffectiveConfig(body.effectiveConfig, {
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
