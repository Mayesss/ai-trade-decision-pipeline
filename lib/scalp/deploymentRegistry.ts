import { mkdir, readFile, writeFile } from 'node:fs/promises';
import path from 'node:path';

import type { ScalpStrategyConfigOverride } from './config';
import { resolveScalpDeployment } from './deployments';
import type { ScalpBacktestLeaderboardEntry } from './replay/types';
import type { ScalpDeploymentRef } from './types';

export type ScalpDeploymentRegistrySource = 'manual' | 'backtest' | 'matrix';

export interface ScalpDeploymentRegistryEntry extends ScalpDeploymentRef {
    enabled: boolean;
    source: ScalpDeploymentRegistrySource;
    notes: string | null;
    configOverride: ScalpStrategyConfigOverride | null;
    leaderboardEntry: ScalpBacktestLeaderboardEntry | null;
    createdAtMs: number;
    updatedAtMs: number;
    updatedBy: string | null;
}

export interface ScalpDeploymentRegistrySnapshot {
    version: 1;
    updatedAt: string | null;
    deployments: ScalpDeploymentRegistryEntry[];
}

type RegistryWriteParams = {
    symbol?: unknown;
    strategyId?: unknown;
    tuneId?: unknown;
    deploymentId?: unknown;
    enabled?: unknown;
    source?: unknown;
    notes?: unknown;
    configOverride?: unknown;
    leaderboardEntry?: unknown;
    updatedBy?: unknown;
};

const DEFAULT_SCALP_DEPLOYMENT_REGISTRY_PATH = 'data/scalp-deployments.json';
const REGISTRY_VERSION = 1 as const;

function isRecord(value: unknown): value is Record<string, unknown> {
    return Boolean(value) && typeof value === 'object' && !Array.isArray(value);
}

function deepClone<T>(value: T): T {
    return JSON.parse(JSON.stringify(value)) as T;
}

function normalizeBool(value: unknown, fallback: boolean): boolean {
    if (typeof value === 'boolean') return value;
    if (typeof value === 'string') {
        const normalized = value.trim().toLowerCase();
        if (['1', 'true', 'yes', 'on'].includes(normalized)) return true;
        if (['0', 'false', 'no', 'off'].includes(normalized)) return false;
    }
    return fallback;
}

function normalizeSource(value: unknown, fallback: ScalpDeploymentRegistrySource): ScalpDeploymentRegistrySource {
    const normalized = String(value || '')
        .trim()
        .toLowerCase();
    if (normalized === 'manual' || normalized === 'backtest' || normalized === 'matrix') return normalized;
    return fallback;
}

function normalizeOptionalText(value: unknown, maxLen: number): string | null {
    const normalized = String(value || '').trim();
    if (!normalized) return null;
    return normalized.slice(0, maxLen);
}

function normalizePositiveTime(value: unknown): number | null {
    const n = Number(value);
    if (!Number.isFinite(n) || n <= 0) return null;
    return Math.floor(n);
}

function normalizeConfigOverride(value: unknown): ScalpStrategyConfigOverride | null {
    if (!isRecord(value)) return null;
    return deepClone(value) as ScalpStrategyConfigOverride;
}

function normalizeLeaderboardEntry(value: unknown, deployment: ScalpDeploymentRef): ScalpBacktestLeaderboardEntry | null {
    if (!isRecord(value)) return null;
    const netR = Number(value.netR);
    const rawProfitFactor = value.profitFactor;
    const profitFactorRaw =
        rawProfitFactor === null || rawProfitFactor === undefined || rawProfitFactor === ''
            ? null
            : Number(rawProfitFactor);
    const maxDrawdownR = Number(value.maxDrawdownR);
    const trades = Number(value.trades);
    const winRatePct = Number(value.winRatePct);
    const avgHoldMinutes = Number(value.avgHoldMinutes);
    const expectancyR = Number(value.expectancyR);
    if (![netR, maxDrawdownR, trades, winRatePct, avgHoldMinutes, expectancyR].every((row) => Number.isFinite(row))) {
        return null;
    }
    return {
        symbol: deployment.symbol,
        strategyId: deployment.strategyId,
        tuneId: deployment.tuneId,
        deploymentId: deployment.deploymentId,
        tuneLabel: deployment.tuneLabel,
        netR,
        profitFactor: profitFactorRaw !== null && Number.isFinite(profitFactorRaw) ? profitFactorRaw : null,
        maxDrawdownR,
        trades: Math.max(0, Math.floor(trades)),
        winRatePct,
        avgHoldMinutes,
        expectancyR,
    };
}

function normalizeRegistryEntry(raw: unknown): ScalpDeploymentRegistryEntry | null {
    if (!isRecord(raw)) return null;
    const deployment = resolveScalpDeployment({
        symbol: raw.symbol,
        strategyId: raw.strategyId,
        tuneId: raw.tuneId,
        deploymentId: raw.deploymentId,
    });
    const createdAtMs = normalizePositiveTime(raw.createdAtMs) || Date.now();
    const updatedAtMs = normalizePositiveTime(raw.updatedAtMs) || createdAtMs;
    return {
        ...deployment,
        enabled: normalizeBool(raw.enabled, true),
        source: normalizeSource(raw.source, 'manual'),
        notes: normalizeOptionalText(raw.notes, 400),
        configOverride: normalizeConfigOverride(raw.configOverride),
        leaderboardEntry: normalizeLeaderboardEntry(raw.leaderboardEntry, deployment),
        createdAtMs,
        updatedAtMs,
        updatedBy: normalizeOptionalText(raw.updatedBy, 120),
    };
}

function normalizeRegistrySnapshot(raw: unknown): ScalpDeploymentRegistrySnapshot {
    if (!isRecord(raw)) {
        return {
            version: REGISTRY_VERSION,
            updatedAt: null,
            deployments: [],
        };
    }
    const deploymentsRaw = Array.isArray(raw.deployments) ? raw.deployments : [];
    const deployments = deploymentsRaw
        .map((entry) => normalizeRegistryEntry(entry))
        .filter((entry): entry is ScalpDeploymentRegistryEntry => Boolean(entry))
        .sort((a, b) => {
            if (a.symbol !== b.symbol) return a.symbol.localeCompare(b.symbol);
            if (a.strategyId !== b.strategyId) return a.strategyId.localeCompare(b.strategyId);
            return a.tuneId.localeCompare(b.tuneId);
        });
    return {
        version: REGISTRY_VERSION,
        updatedAt: typeof raw.updatedAt === 'string' ? raw.updatedAt : null,
        deployments,
    };
}

export function scalpDeploymentRegistryPath(): string {
    const configured = String(process.env.SCALP_DEPLOYMENTS_REGISTRY_PATH || DEFAULT_SCALP_DEPLOYMENT_REGISTRY_PATH).trim();
    return path.isAbsolute(configured) ? configured : path.resolve(process.cwd(), configured);
}

export async function loadScalpDeploymentRegistry(): Promise<ScalpDeploymentRegistrySnapshot> {
    const filePath = scalpDeploymentRegistryPath();
    try {
        const raw = await readFile(filePath, 'utf8');
        return normalizeRegistrySnapshot(JSON.parse(raw));
    } catch {
        return normalizeRegistrySnapshot(null);
    }
}

async function saveScalpDeploymentRegistry(snapshot: ScalpDeploymentRegistrySnapshot): Promise<void> {
    const filePath = scalpDeploymentRegistryPath();
    await mkdir(path.dirname(filePath), { recursive: true });
    await writeFile(filePath, `${JSON.stringify(snapshot, null, 2)}\n`, 'utf8');
}

export function filterScalpDeploymentRegistry(
    snapshot: ScalpDeploymentRegistrySnapshot,
    params: { symbol?: unknown; strategyId?: unknown; tuneId?: unknown; enabled?: unknown } = {},
): ScalpDeploymentRegistryEntry[] {
    const symbol = String(params.symbol || '')
        .trim()
        .toUpperCase();
    const strategyId = String(params.strategyId || '')
        .trim()
        .toLowerCase();
    const tuneId = String(params.tuneId || '')
        .trim()
        .toLowerCase();
    const enabledFilter =
        params.enabled === undefined ? null : normalizeBool(params.enabled, true);
    return snapshot.deployments.filter((entry) => {
        if (symbol && entry.symbol !== symbol) return false;
        if (strategyId && entry.strategyId !== strategyId) return false;
        if (tuneId && entry.tuneId !== tuneId) return false;
        if (enabledFilter !== null && entry.enabled !== enabledFilter) return false;
        return true;
    });
}

export async function listScalpDeploymentRegistryEntries(
    params: { symbol?: unknown; strategyId?: unknown; tuneId?: unknown; enabled?: unknown } = {},
): Promise<ScalpDeploymentRegistryEntry[]> {
    const snapshot = await loadScalpDeploymentRegistry();
    return filterScalpDeploymentRegistry(snapshot, params);
}

export async function upsertScalpDeploymentRegistryEntry(
    params: RegistryWriteParams,
): Promise<{ snapshot: ScalpDeploymentRegistrySnapshot; entry: ScalpDeploymentRegistryEntry }> {
    const snapshot = await loadScalpDeploymentRegistry();
    const deployment = resolveScalpDeployment({
        symbol: params.symbol,
        strategyId: params.strategyId,
        tuneId: params.tuneId,
        deploymentId: params.deploymentId,
    });
    const existing = snapshot.deployments.find((entry) => entry.deploymentId === deployment.deploymentId) || null;
    const nowMs = Date.now();
    const entry: ScalpDeploymentRegistryEntry = {
        ...deployment,
        enabled: normalizeBool(params.enabled, existing?.enabled ?? true),
        source: normalizeSource(params.source, existing?.source ?? 'manual'),
        notes: normalizeOptionalText(params.notes, 400) ?? existing?.notes ?? null,
        configOverride: normalizeConfigOverride(params.configOverride) ?? existing?.configOverride ?? null,
        leaderboardEntry: normalizeLeaderboardEntry(params.leaderboardEntry, deployment) ?? existing?.leaderboardEntry ?? null,
        createdAtMs: existing?.createdAtMs ?? nowMs,
        updatedAtMs: nowMs,
        updatedBy: normalizeOptionalText(params.updatedBy, 120) ?? existing?.updatedBy ?? null,
    };
    const deployments = snapshot.deployments
        .filter((row) => row.deploymentId !== deployment.deploymentId)
        .concat(entry)
        .sort((a, b) => {
            if (a.symbol !== b.symbol) return a.symbol.localeCompare(b.symbol);
            if (a.strategyId !== b.strategyId) return a.strategyId.localeCompare(b.strategyId);
            return a.tuneId.localeCompare(b.tuneId);
        });
    const next: ScalpDeploymentRegistrySnapshot = {
        version: REGISTRY_VERSION,
        updatedAt: new Date(nowMs).toISOString(),
        deployments,
    };
    await saveScalpDeploymentRegistry(next);
    return { snapshot: next, entry };
}

export async function removeScalpDeploymentRegistryEntry(
    params: { symbol?: unknown; strategyId?: unknown; tuneId?: unknown; deploymentId?: unknown },
): Promise<{ snapshot: ScalpDeploymentRegistrySnapshot; removed: boolean; deploymentId: string }> {
    const snapshot = await loadScalpDeploymentRegistry();
    const deployment = resolveScalpDeployment({
        symbol: params.symbol,
        strategyId: params.strategyId,
        tuneId: params.tuneId,
        deploymentId: params.deploymentId,
    });
    const deployments = snapshot.deployments.filter((entry) => entry.deploymentId !== deployment.deploymentId);
    const removed = deployments.length !== snapshot.deployments.length;
    const next: ScalpDeploymentRegistrySnapshot = {
        version: REGISTRY_VERSION,
        updatedAt: removed ? new Date().toISOString() : snapshot.updatedAt,
        deployments,
    };
    if (removed) {
        await saveScalpDeploymentRegistry(next);
    }
    return { snapshot: next, removed, deploymentId: deployment.deploymentId };
}
