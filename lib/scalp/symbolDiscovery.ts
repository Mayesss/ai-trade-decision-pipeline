import { mkdir, readFile, writeFile } from 'node:fs/promises';
import path from 'node:path';

import { Prisma } from '@prisma/client';

import { bitgetFetch, resolveProductType } from '../bitget';
import { fetchBitgetCandlesByEpicDateRange } from './bitgetHistory';
import { listScalpCandleHistorySymbols, loadScalpCandleHistory, mergeScalpCandleHistory, normalizeHistoryTimeframe, saveScalpCandleHistory, timeframeToMs } from './candleHistory';
import { loadScalpDeploymentRegistry } from './deploymentRegistry';
import { pipSizeForScalpSymbol } from './marketData';
import { isScalpPgConfigured, scalpPrisma } from './pg/client';
import { inferScalpAssetCategory, type ScalpAssetCategory } from './symbolInfo';
import { listScalpStrategies } from './strategies/registry';

type ScalpSymbolDiscoveryCriteriaBase = {
    minHistoryDays: number;
    minHistoryCoveragePct: number;
    minAvgBarsPerDay: number;
    minRecentBars7d: number;
    minMedianRangePct: number;
    maxSpreadPips: number | null;
    requireTradableQuote: boolean;
};

type ScalpSymbolDiscoveryCriteria = ScalpSymbolDiscoveryCriteriaBase & {
    byCategory?: Partial<Record<ScalpAssetCategory, Partial<ScalpSymbolDiscoveryCriteriaBase>>>;
};

export interface ScalpSymbolDiscoveryPolicy {
    version: 1;
    updatedAt: string | null;
    notes: string | null;
    limits: {
        maxUniverseSymbols: number;
        minUniverseSymbols: number;
        maxWeeklyAdds: number;
        maxWeeklyRemoves: number;
        maxCandidates: number;
    };
    criteria: ScalpSymbolDiscoveryCriteria;
    sources: {
        includeBitgetMarketsApi: boolean;
        includeDeploymentSymbols: boolean;
        includeHistorySymbols: boolean;
        requireHistoryPresence: boolean;
        explicitSymbols: string[];
        excludedSymbols: string[];
    };
    pinnedSymbols: string[];
    strategyAllowlist: string[];
}

export interface ScalpSymbolCandidateMetrics {
    historyBars1m: number;
    historyFromTs: number | null;
    historyToTs: number | null;
    historySpanDays: number;
    historyCoveragePct: number;
    avgBarsPerDay: number;
    recentBars7d: number;
    medianRangePct: number;
    livePrice: number | null;
    liveSpreadPips: number | null;
}

export interface ScalpSymbolCandidateRow {
    symbol: string;
    eligible: boolean;
    score: number;
    reasons: string[];
    recommendedStrategyIds: string[];
    metrics: ScalpSymbolCandidateMetrics;
}

export interface ScalpSymbolUniverseSnapshot {
    version: 1;
    generatedAtIso: string;
    policy: ScalpSymbolDiscoveryPolicy;
    source: 'weekly_discovery_v1';
    dryRun: boolean;
    previousSymbols: string[];
    selectedSymbols: string[];
    addedSymbols: string[];
    removedSymbols: string[];
    candidatesEvaluated: number;
    evaluationWindow?: {
        poolSize: number;
        maxCandidates: number;
        startOffset: number;
        evaluatedCount: number;
        nextOffset: number;
    } | null;
    selectedRows: ScalpSymbolCandidateRow[];
    topRejectedRows: ScalpSymbolCandidateRow[];
    diagnostics?: {
        sourceEnabled: {
            includeBitgetMarketsApi: boolean;
            includeDeploymentSymbols: boolean;
            includeHistorySymbols: boolean;
            requireHistoryPresence: boolean;
            explicitSymbols: boolean;
        };
        sourceCounts: {
            bitgetMarketsApi: number;
            bitgetMarketsApiRows: number;
            deploymentSymbols: number;
            historySymbols: number;
            explicitSymbols: number;
            totalUnique: number;
        };
        bitgetMarketsApiError: string | null;
        includeLiveQuotes: boolean;
        requireTradableQuote: boolean;
        quoteGateApplied: boolean;
    };
    seedSummary?: ScalpSymbolDiscoverySeedSummary | null;
}

export interface ScalpSymbolDiscoverySourceOverrides {
    includeBitgetMarketsApi?: boolean;
    includeDeploymentSymbols?: boolean;
    includeHistorySymbols?: boolean;
    requireHistoryPresence?: boolean;
}

export interface ScalpSymbolDiscoveryRunParams {
    dryRun?: boolean;
    includeLiveQuotes?: boolean;
    restrictToBitgetSymbols?: boolean;
    nowMs?: number;
    maxCandidatesOverride?: number;
    seedTopSymbols?: number;
    seedTargetHistoryDays?: number;
    seedChunkDays?: number;
    seedMaxRequestsPerSymbol?: number;
    seedMaxSymbolsPerRun?: number;
    seedMaxHistoryDays?: number;
    seedTimeframe?: string;
    seedOnDryRun?: boolean;
    seedAllowBootstrapSymbols?: boolean;
    sourceOverrides?: ScalpSymbolDiscoverySourceOverrides;
}

export interface ScalpSymbolDiscoverySeedSymbolResult {
    symbol: string;
    status: 'seeded' | 'skipped' | 'failed';
    reason: string;
    epic: string | null;
    existingCount: number;
    mergedCount: number;
    fetchedCount: number;
    addedCount: number;
    trimmedCount: number;
    beforeSpanDays: number;
    afterSpanDays: number;
}

export interface ScalpSymbolDiscoverySeedSummary {
    enabled: boolean;
    dryRun: boolean;
    timeframe: string;
    requestedTopSymbols: number;
    processedSymbols: number;
    seededSymbols: number;
    skippedSymbols: number;
    failedSymbols: number;
    targetHistoryDays: number;
    maxHistoryDays: number;
    chunkDays: number;
    maxRequestsPerSymbol: number;
    maxSymbolsPerRun: number;
    candidateUniverseSize: number;
    results: ScalpSymbolDiscoverySeedSymbolResult[];
}

type BitgetDiscoveryResult = {
    symbols: string[];
    diagnostics: {
        rowsSeen: number;
        mappedSymbols: number;
        errors: string[];
        productType: string;
    };
};

const DEFAULT_POLICY_PATH = path.resolve(process.cwd(), 'data/scalp-symbol-discovery-policy.json');
const DEFAULT_UNIVERSE_FILE_PATH = path.resolve(process.cwd(), 'data/scalp-symbol-universe.json');
const UNIVERSE_SNAPSHOT_KEY = 'latest:v1';
const UNIVERSE_SNAPSHOT_JOBS_KEY = 'state:symbol_universe_snapshot:v1';
const ONE_DAY_MS = 24 * 60 * 60 * 1000;
const ONE_WEEK_MS = 7 * ONE_DAY_MS;
let latestUniverseSnapshot: ScalpSymbolUniverseSnapshot | null = null;
const SEED_FRESHNESS_MAX_LAG_MS = 12 * 60 * 60 * 1000;

const DEFAULT_POLICY: ScalpSymbolDiscoveryPolicy = {
    version: 1,
    updatedAt: null,
    notes: 'Default scalp symbol discovery policy.',
    limits: {
        maxUniverseSymbols: 16,
        minUniverseSymbols: 4,
        maxWeeklyAdds: 2,
        maxWeeklyRemoves: 2,
        maxCandidates: 40,
    },
    criteria: {
        minHistoryDays: 45,
        minHistoryCoveragePct: 80,
        minAvgBarsPerDay: 900,
        minRecentBars7d: 4000,
        minMedianRangePct: 0.025,
        maxSpreadPips: 35,
        requireTradableQuote: true,
        byCategory: {
            // Forex is not a full 24/7 market; use realistic density/recency gates.
            forex: {
                minHistoryCoveragePct: 55,
                minAvgBarsPerDay: 800,
                minRecentBars7d: 1500,
                minMedianRangePct: 0.01,
            },
        },
    },
    sources: {
        includeBitgetMarketsApi: true,
        includeDeploymentSymbols: false,
        includeHistorySymbols: true,
        requireHistoryPresence: true,
        explicitSymbols: [],
        excludedSymbols: [],
    },
    pinnedSymbols: ['BTCUSDT', 'XAUUSDT'],
    strategyAllowlist: [
        'compression_breakout_pullback_m15_m3',
        'regime_pullback_m15_m3',
        'hss_ict_m15_m3_guarded',
        'failed_auction_extreme_reversal_m15_m1',
        'trend_day_reacceleration_m15_m3',
        'pdh_pdl_reclaim_m15_m3',
    ],
};

function normalizeSymbol(value: unknown): string {
    return String(value || '')
        .trim()
        .toUpperCase()
        .replace(/[^A-Z0-9._-]/g, '');
}

function isBitgetCompatibleSymbol(value: unknown): boolean {
    const symbol = normalizeSymbol(value);
    if (!symbol) return false;
    const productType = String(resolveProductType() || 'usdt-futures')
        .trim()
        .toLowerCase();
    if (productType === 'usdc-futures') return symbol.endsWith('USDC');
    if (productType === 'coin-futures') return symbol.endsWith('USD');
    return symbol.endsWith('USDT');
}

function toBool(value: unknown, fallback: boolean): boolean {
    if (typeof value === 'boolean') return value;
    const normalized = String(value || '')
        .trim()
        .toLowerCase();
    if (!normalized) return fallback;
    if (['1', 'true', 'yes', 'on'].includes(normalized)) return true;
    if (['0', 'false', 'no', 'off'].includes(normalized)) return false;
    return fallback;
}

function toNumber(value: unknown, fallback: number): number {
    const n = Number(value);
    if (!Number.isFinite(n)) return fallback;
    return n;
}

function toPositiveInt(value: unknown, fallback: number): number {
    const n = Math.floor(toNumber(value, fallback));
    if (!Number.isFinite(n) || n <= 0) return fallback;
    return n;
}

function toNonNegativeInt(value: unknown, fallback: number): number {
    const n = Math.floor(toNumber(value, fallback));
    if (!Number.isFinite(n) || n < 0) return fallback;
    return n;
}

function toNonNegativeNumber(value: unknown, fallback: number): number {
    const n = toNumber(value, fallback);
    if (!Number.isFinite(n) || n < 0) return fallback;
    return n;
}

function asRecord(value: unknown): Record<string, unknown> {
    if (!value || typeof value !== 'object' || Array.isArray(value)) return {};
    return value as Record<string, unknown>;
}

function hasOwn(record: Record<string, unknown>, key: string): boolean {
    return Object.prototype.hasOwnProperty.call(record, key);
}

const SCALP_ASSET_CATEGORIES: ScalpAssetCategory[] = ['forex', 'index', 'commodity', 'equity', 'crypto', 'other'];

function normalizeCategoryCriteriaOverrides(
    raw: unknown,
    fallback: ScalpSymbolDiscoveryCriteria['byCategory'] | undefined,
): NonNullable<ScalpSymbolDiscoveryCriteria['byCategory']> {
    const out: NonNullable<ScalpSymbolDiscoveryCriteria['byCategory']> = {};
    const rawByCategory = asRecord(raw);
    const fallbackByCategory = fallback || {};

    for (const category of SCALP_ASSET_CATEGORIES) {
        const rawOverride = asRecord(rawByCategory[category]);
        const fallbackOverride = asRecord(fallbackByCategory[category]);
        const override: Partial<ScalpSymbolDiscoveryCriteriaBase> = {};

        const resolvedMinHistoryDays = hasOwn(rawOverride, 'minHistoryDays')
            ? Math.max(1, toNonNegativeNumber(rawOverride.minHistoryDays, fallbackOverride.minHistoryDays as number))
            : hasOwn(fallbackOverride, 'minHistoryDays')
              ? Math.max(1, toNonNegativeNumber(fallbackOverride.minHistoryDays, 1))
              : undefined;
        if (resolvedMinHistoryDays !== undefined) override.minHistoryDays = resolvedMinHistoryDays;

        const resolvedMinCoverage = hasOwn(rawOverride, 'minHistoryCoveragePct')
            ? Math.max(1, Math.min(100, toNonNegativeNumber(rawOverride.minHistoryCoveragePct, fallbackOverride.minHistoryCoveragePct as number)))
            : hasOwn(fallbackOverride, 'minHistoryCoveragePct')
              ? Math.max(1, Math.min(100, toNonNegativeNumber(fallbackOverride.minHistoryCoveragePct, 1)))
              : undefined;
        if (resolvedMinCoverage !== undefined) override.minHistoryCoveragePct = resolvedMinCoverage;

        const resolvedMinBarsPerDay = hasOwn(rawOverride, 'minAvgBarsPerDay')
            ? Math.max(1, toNonNegativeNumber(rawOverride.minAvgBarsPerDay, fallbackOverride.minAvgBarsPerDay as number))
            : hasOwn(fallbackOverride, 'minAvgBarsPerDay')
              ? Math.max(1, toNonNegativeNumber(fallbackOverride.minAvgBarsPerDay, 1))
              : undefined;
        if (resolvedMinBarsPerDay !== undefined) override.minAvgBarsPerDay = resolvedMinBarsPerDay;

        const resolvedMinRecentBars = hasOwn(rawOverride, 'minRecentBars7d')
            ? Math.max(0, toNonNegativeNumber(rawOverride.minRecentBars7d, fallbackOverride.minRecentBars7d as number))
            : hasOwn(fallbackOverride, 'minRecentBars7d')
              ? Math.max(0, toNonNegativeNumber(fallbackOverride.minRecentBars7d, 0))
              : undefined;
        if (resolvedMinRecentBars !== undefined) override.minRecentBars7d = resolvedMinRecentBars;

        const resolvedMedianRangePct = hasOwn(rawOverride, 'minMedianRangePct')
            ? Math.max(0, toNonNegativeNumber(rawOverride.minMedianRangePct, fallbackOverride.minMedianRangePct as number))
            : hasOwn(fallbackOverride, 'minMedianRangePct')
              ? Math.max(0, toNonNegativeNumber(fallbackOverride.minMedianRangePct, 0))
              : undefined;
        if (resolvedMedianRangePct !== undefined) override.minMedianRangePct = resolvedMedianRangePct;

        let resolvedMaxSpread: number | null | undefined;
        if (hasOwn(rawOverride, 'maxSpreadPips')) {
            const maxSpreadRaw = toNumber(rawOverride.maxSpreadPips, NaN);
            resolvedMaxSpread = Number.isFinite(maxSpreadRaw) && maxSpreadRaw >= 0 ? maxSpreadRaw : null;
        } else if (hasOwn(fallbackOverride, 'maxSpreadPips')) {
            const fallbackMaxSpreadRaw = toNumber(fallbackOverride.maxSpreadPips, NaN);
            resolvedMaxSpread =
                Number.isFinite(fallbackMaxSpreadRaw) && fallbackMaxSpreadRaw >= 0 ? fallbackMaxSpreadRaw : null;
        }
        if (resolvedMaxSpread !== undefined) override.maxSpreadPips = resolvedMaxSpread;

        const resolvedRequireTradableQuote = hasOwn(rawOverride, 'requireTradableQuote')
            ? toBool(rawOverride.requireTradableQuote, toBool(fallbackOverride.requireTradableQuote, true))
            : hasOwn(fallbackOverride, 'requireTradableQuote')
              ? toBool(fallbackOverride.requireTradableQuote, true)
              : undefined;
        if (resolvedRequireTradableQuote !== undefined) override.requireTradableQuote = resolvedRequireTradableQuote;

        if (Object.keys(override).length > 0) {
            out[category] = override;
        }
    }

    return out;
}

function resolveCriteriaForSymbol(
    policy: ScalpSymbolDiscoveryPolicy,
    symbol: string,
): ScalpSymbolDiscoveryCriteriaBase {
    const base = policy.criteria;
    const category = inferScalpAssetCategory(symbol);
    const override = base.byCategory?.[category] || null;
    return {
        minHistoryDays: Number(override?.minHistoryDays ?? base.minHistoryDays),
        minHistoryCoveragePct: Number(override?.minHistoryCoveragePct ?? base.minHistoryCoveragePct),
        minAvgBarsPerDay: Number(override?.minAvgBarsPerDay ?? base.minAvgBarsPerDay),
        minRecentBars7d: Number(override?.minRecentBars7d ?? base.minRecentBars7d),
        minMedianRangePct: Number(override?.minMedianRangePct ?? base.minMedianRangePct),
        maxSpreadPips:
            override?.maxSpreadPips === undefined ? base.maxSpreadPips : Number(override.maxSpreadPips) >= 0 ? Number(override.maxSpreadPips) : null,
        requireTradableQuote:
            override?.requireTradableQuote === undefined ? base.requireTradableQuote : Boolean(override.requireTradableQuote),
    };
}

function normalizeStringArray(value: unknown): string[] {
    const rows = Array.isArray(value) ? value : [];
    const normalized = rows.map((row) => normalizeSymbol(row)).filter((row) => Boolean(row));
    return Array.from(new Set(normalized));
}

function normalizeStrategyIdArray(value: unknown): string[] {
    const rows = Array.isArray(value) ? value : [];
    const normalized = rows
        .map((row) =>
            String(row || '')
                .trim()
                .toLowerCase()
                .replace(/[^a-z0-9._-]/g, ''),
        )
        .filter((row) => Boolean(row));
    return Array.from(new Set(normalized));
}

export function resolveCompletedWeekCoverageStartMs(nowMs: number, requiredWeeks: number): number {
    const safeNowMs = Number.isFinite(Number(nowMs)) ? Math.max(0, Math.floor(Number(nowMs))) : Date.now();
    const normalizedRequiredWeeks = Math.max(1, Math.min(52, Math.floor(Number(requiredWeeks) || 0)));
    const dayStartMs = (() => {
        const d = new Date(safeNowMs);
        return Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate());
    })();
    const dayOfWeek = new Date(dayStartMs).getUTCDay(); // 0=Sunday ... 6=Saturday
    const daysSinceMonday = (dayOfWeek + 6) % 7;
    const startCurrentWeekMondayMs = dayStartMs - daysSinceMonday * ONE_DAY_MS;
    return Math.max(0, startCurrentWeekMondayMs - normalizedRequiredWeeks * ONE_WEEK_MS);
}

export function resolveRequiredHistoryDaysForCompletedWeeks(params: {
    nowMs: number;
    targetHistoryDays: number;
    requiredSuccessiveWeeks: number;
}): number {
    const safeNowMs = Number.isFinite(Number(params.nowMs)) ? Math.max(0, Math.floor(Number(params.nowMs))) : Date.now();
    const targetHistoryDays = Math.max(1, Math.floor(Number(params.targetHistoryDays) || 0));
    const coverageStartMs = resolveCompletedWeekCoverageStartMs(safeNowMs, params.requiredSuccessiveWeeks);
    const coverageSpanDays = Math.max(1, Math.ceil((safeNowMs - coverageStartMs) / ONE_DAY_MS));
    return Math.max(targetHistoryDays, coverageSpanDays);
}

function resolvePolicyPath(): string {
    const configured = String(process.env.SCALP_SYMBOL_DISCOVERY_POLICY_PATH || '').trim();
    if (!configured) return DEFAULT_POLICY_PATH;
    return path.isAbsolute(configured) ? configured : path.resolve(process.cwd(), configured);
}

function resolveUniverseFilePath(): string {
    const configured = String(process.env.SCALP_SYMBOL_UNIVERSE_PATH || '').trim();
    if (!configured) return DEFAULT_UNIVERSE_FILE_PATH;
    return path.isAbsolute(configured) ? configured : path.resolve(process.cwd(), configured);
}

function normalizePolicy(raw: unknown): ScalpSymbolDiscoveryPolicy {
    if (!raw || typeof raw !== 'object' || Array.isArray(raw)) return { ...DEFAULT_POLICY };
    const row = raw as Record<string, unknown>;
    const criteriaRow = asRecord(row.criteria);

    const maxUniverseSymbols = Math.max(1, toPositiveInt((row.limits as any)?.maxUniverseSymbols, DEFAULT_POLICY.limits.maxUniverseSymbols));
    const minUniverseSymbols = Math.max(
        1,
        Math.min(maxUniverseSymbols, toPositiveInt((row.limits as any)?.minUniverseSymbols, DEFAULT_POLICY.limits.minUniverseSymbols)),
    );

    const maxSpreadPipsRaw = toNumber(criteriaRow.maxSpreadPips, NaN);
    const maxSpreadPips = Number.isFinite(maxSpreadPipsRaw) && maxSpreadPipsRaw >= 0 ? maxSpreadPipsRaw : null;
    const byCategory = normalizeCategoryCriteriaOverrides(criteriaRow.byCategory, DEFAULT_POLICY.criteria.byCategory);

    return {
        version: 1,
        updatedAt: typeof row.updatedAt === 'string' ? row.updatedAt : null,
        notes: typeof row.notes === 'string' ? row.notes : null,
        limits: {
            maxUniverseSymbols,
            minUniverseSymbols,
            maxWeeklyAdds: toNonNegativeInt((row.limits as any)?.maxWeeklyAdds, DEFAULT_POLICY.limits.maxWeeklyAdds),
            maxWeeklyRemoves: toNonNegativeInt((row.limits as any)?.maxWeeklyRemoves, DEFAULT_POLICY.limits.maxWeeklyRemoves),
            maxCandidates: Math.max(1, toPositiveInt((row.limits as any)?.maxCandidates, DEFAULT_POLICY.limits.maxCandidates)),
        },
        criteria: {
            minHistoryDays: Math.max(1, toNonNegativeNumber(criteriaRow.minHistoryDays, DEFAULT_POLICY.criteria.minHistoryDays)),
            minHistoryCoveragePct: Math.max(
                1,
                Math.min(100, toNonNegativeNumber(criteriaRow.minHistoryCoveragePct, DEFAULT_POLICY.criteria.minHistoryCoveragePct)),
            ),
            minAvgBarsPerDay: Math.max(
                1,
                toNonNegativeNumber(criteriaRow.minAvgBarsPerDay, DEFAULT_POLICY.criteria.minAvgBarsPerDay),
            ),
            minRecentBars7d: Math.max(
                0,
                toNonNegativeNumber(criteriaRow.minRecentBars7d, DEFAULT_POLICY.criteria.minRecentBars7d),
            ),
            minMedianRangePct: Math.max(
                0,
                toNonNegativeNumber(criteriaRow.minMedianRangePct, DEFAULT_POLICY.criteria.minMedianRangePct),
            ),
            maxSpreadPips,
            requireTradableQuote: toBool(criteriaRow.requireTradableQuote, DEFAULT_POLICY.criteria.requireTradableQuote),
            byCategory,
        },
        sources: {
            includeBitgetMarketsApi: toBool(
                (row.sources as any)?.includeBitgetMarketsApi,
                DEFAULT_POLICY.sources.includeBitgetMarketsApi,
            ),
            includeDeploymentSymbols: toBool(
                (row.sources as any)?.includeDeploymentSymbols,
                DEFAULT_POLICY.sources.includeDeploymentSymbols,
            ),
            includeHistorySymbols: toBool((row.sources as any)?.includeHistorySymbols, DEFAULT_POLICY.sources.includeHistorySymbols),
            requireHistoryPresence: toBool((row.sources as any)?.requireHistoryPresence, DEFAULT_POLICY.sources.requireHistoryPresence),
            explicitSymbols: normalizeStringArray((row.sources as any)?.explicitSymbols),
            excludedSymbols: normalizeStringArray((row.sources as any)?.excludedSymbols),
        },
        pinnedSymbols: normalizeStringArray(row.pinnedSymbols),
        strategyAllowlist: normalizeStrategyIdArray(row.strategyAllowlist),
    };
}

function applyPolicySourceOverrides(
    policy: ScalpSymbolDiscoveryPolicy,
    overrides: ScalpSymbolDiscoverySourceOverrides | null | undefined,
): ScalpSymbolDiscoveryPolicy {
    if (!overrides) return policy;
    const hasBitgetOverride = Object.prototype.hasOwnProperty.call(overrides, 'includeBitgetMarketsApi');
    const hasDeploymentOverride = Object.prototype.hasOwnProperty.call(overrides, 'includeDeploymentSymbols');
    const hasHistoryOverride = Object.prototype.hasOwnProperty.call(overrides, 'includeHistorySymbols');
    const hasRequireHistoryPresenceOverride = Object.prototype.hasOwnProperty.call(overrides, 'requireHistoryPresence');
    if (!hasBitgetOverride && !hasDeploymentOverride && !hasHistoryOverride && !hasRequireHistoryPresenceOverride) return policy;
    return {
        ...policy,
        sources: {
            ...policy.sources,
            ...(hasBitgetOverride ? { includeBitgetMarketsApi: Boolean(overrides.includeBitgetMarketsApi) } : {}),
            ...(hasDeploymentOverride ? { includeDeploymentSymbols: Boolean(overrides.includeDeploymentSymbols) } : {}),
            ...(hasHistoryOverride ? { includeHistorySymbols: Boolean(overrides.includeHistorySymbols) } : {}),
            ...(hasRequireHistoryPresenceOverride
                ? { requireHistoryPresence: Boolean(overrides.requireHistoryPresence) }
                : {}),
        },
    };
}

export async function loadScalpSymbolDiscoveryPolicy(): Promise<ScalpSymbolDiscoveryPolicy> {
    const filePath = resolvePolicyPath();
    try {
        const raw = await readFile(filePath, 'utf8');
        return normalizePolicy(JSON.parse(raw));
    } catch {
        return normalizePolicy(DEFAULT_POLICY);
    }
}

export async function loadScalpSymbolUniverseSnapshot(): Promise<ScalpSymbolUniverseSnapshot | null> {
    if (latestUniverseSnapshot) return latestUniverseSnapshot;
    if (isScalpPgConfigured()) {
        try {
            const db = scalpPrisma();
            const rows = await db.$queryRaw<Array<{ payload: unknown }>>(Prisma.sql`
                SELECT payload_json AS payload
                FROM scalp_symbol_universe_snapshots
                WHERE snapshot_key = ${UNIVERSE_SNAPSHOT_KEY}
                LIMIT 1;
            `);
            const parsed = rows[0]?.payload;
            if (parsed && typeof parsed === 'object' && !Array.isArray(parsed)) {
                latestUniverseSnapshot = parsed as ScalpSymbolUniverseSnapshot;
                return latestUniverseSnapshot;
            }
        } catch {
            // best effort fallback to jobs/file
        }
        try {
            const db = scalpPrisma();
            const rows = await db.$queryRaw<Array<{ payload: unknown }>>(Prisma.sql`
                SELECT payload
                FROM scalp_jobs
                WHERE kind = 'guardrail_check'::scalp_job_kind
                  AND dedupe_key = ${UNIVERSE_SNAPSHOT_JOBS_KEY}
                LIMIT 1;
            `);
            const parsed = rows[0]?.payload;
            if (parsed && typeof parsed === 'object' && !Array.isArray(parsed)) {
                latestUniverseSnapshot = parsed as ScalpSymbolUniverseSnapshot;
                return latestUniverseSnapshot;
            }
        } catch {
            // best effort fallback to file backend below
        }
    }
    if (process.env.ALLOW_SCALP_FILE_BACKEND !== '1') return null;
    try {
        const raw = await readFile(resolveUniverseFilePath(), 'utf8');
        const parsed = JSON.parse(raw) as ScalpSymbolUniverseSnapshot;
        latestUniverseSnapshot = parsed;
        return parsed;
    } catch {
        return null;
    }
}

async function saveScalpSymbolUniverseSnapshot(snapshot: ScalpSymbolUniverseSnapshot): Promise<void> {
    latestUniverseSnapshot = snapshot;
    if (isScalpPgConfigured()) {
        const db = scalpPrisma();
        try {
            await db.$executeRaw(
                Prisma.sql`
                    INSERT INTO scalp_symbol_universe_snapshots(
                        snapshot_key,
                        payload_json,
                        generated_at,
                        updated_at
                    )
                    VALUES(
                        ${UNIVERSE_SNAPSHOT_KEY},
                        ${JSON.stringify(snapshot)}::jsonb,
                        to_timestamp(${Math.floor(new Date(snapshot.generatedAtIso).getTime())} / 1000.0),
                        NOW()
                    )
                    ON CONFLICT(snapshot_key)
                    DO UPDATE SET
                        payload_json = EXCLUDED.payload_json,
                        generated_at = EXCLUDED.generated_at,
                        updated_at = NOW();
                `,
            );
        } catch {
            await db.$executeRaw(
                Prisma.sql`
                    INSERT INTO scalp_jobs(
                        kind,
                        dedupe_key,
                        payload,
                        status,
                        attempts,
                        max_attempts,
                        scheduled_for,
                        next_run_at,
                        last_error
                    )
                    VALUES(
                        'guardrail_check'::scalp_job_kind,
                        ${UNIVERSE_SNAPSHOT_JOBS_KEY},
                        ${JSON.stringify(snapshot)}::jsonb,
                        'succeeded'::scalp_job_status,
                        1,
                        1,
                        NOW(),
                        NOW(),
                        NULL
                    )
                    ON CONFLICT(kind, dedupe_key)
                    DO UPDATE SET
                        payload = EXCLUDED.payload,
                        status = EXCLUDED.status,
                        attempts = EXCLUDED.attempts,
                        max_attempts = EXCLUDED.max_attempts,
                        scheduled_for = EXCLUDED.scheduled_for,
                        next_run_at = EXCLUDED.next_run_at,
                        locked_by = NULL,
                        locked_at = NULL,
                        last_error = NULL,
                        updated_at = NOW();
                `,
            );
        }
        return;
    }
    if (process.env.ALLOW_SCALP_FILE_BACKEND !== '1') return;
    const filePath = resolveUniverseFilePath();
    await mkdir(path.dirname(filePath), { recursive: true });
    await writeFile(filePath, `${JSON.stringify(snapshot, null, 2)}\n`, 'utf8');
}

function median(values: number[]): number {
    if (!values.length) return 0;
    const sorted = values.slice().sort((a, b) => a - b);
    const mid = Math.floor(sorted.length / 2);
    if (sorted.length % 2 === 1) return sorted[mid] || 0;
    return ((sorted[mid - 1] || 0) + (sorted[mid] || 0)) / 2;
}

function startOfUtcDay(tsMs: number): number {
    const date = new Date(tsMs);
    return Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), date.getUTCDate());
}

interface SymbolHistoryStats {
    bars1m: number;
    fromTs: number | null;
    toTs: number | null;
    recentBars7d: number;
    medianRangePct: number;
}

async function loadSymbolHistoryStats(symbol: string, nowMs: number): Promise<SymbolHistoryStats> {
    const normalized = normalizeSymbol(symbol);
    if (!normalized) {
        return {
            bars1m: 0,
            fromTs: null,
            toTs: null,
            recentBars7d: 0,
            medianRangePct: 0,
        };
    }

    if (isScalpPgConfigured()) {
        const db = scalpPrisma();
        const recentStartTs = startOfUtcDay(nowMs - 7 * 24 * 60 * 60_000);
        const recentScanStartTs = Math.max(0, recentStartTs - 7 * 24 * 60 * 60_000);
        const medianWindowStartTs = startOfUtcDay(nowMs - 21 * 24 * 60 * 60_000);
        const rows = await db.$queryRaw<
            Array<{
                totalBars: bigint | number | null;
                minWeekStartMs: bigint | number | null;
                maxWeekStartMs: bigint | number | null;
                recentBars7d: bigint | number | null;
                medianRangePct: number | null;
            }>
        >(Prisma.sql`
            WITH agg AS (
                SELECT
                    COALESCE(SUM(jsonb_array_length(candles_json)), 0)::bigint AS "totalBars",
                    MIN((EXTRACT(EPOCH FROM week_start) * 1000)::bigint) AS "minWeekStartMs",
                    MAX((EXTRACT(EPOCH FROM week_start) * 1000)::bigint) AS "maxWeekStartMs"
                FROM scalp_candle_history_weeks
                WHERE symbol = ${normalized}
                  AND timeframe = '1m'
            ),
            recent AS (
                SELECT
                    COALESCE(COUNT(*), 0)::bigint AS "recentBars7d"
                FROM scalp_candle_history_weeks w
                CROSS JOIN LATERAL jsonb_array_elements(w.candles_json) elem
                WHERE w.symbol = ${normalized}
                  AND w.timeframe = '1m'
                  AND w.week_start >= to_timestamp(${recentScanStartTs} / 1000.0)
                  AND (elem->>0)::bigint >= ${recentStartTs}
            ),
            sample AS (
                SELECT
                    (elem->>0)::bigint AS ts,
                    ((elem->>2)::double precision - (elem->>3)::double precision)
                        / NULLIF((elem->>4)::double precision, 0) * 100.0 AS range_pct
                FROM scalp_candle_history_weeks w
                CROSS JOIN LATERAL jsonb_array_elements(w.candles_json) elem
                WHERE w.symbol = ${normalized}
                  AND w.timeframe = '1m'
                  AND w.week_start >= to_timestamp(${medianWindowStartTs} / 1000.0)
            ),
            sample_limited AS (
                SELECT range_pct
                FROM sample
                WHERE range_pct IS NOT NULL
                  AND range_pct >= 0
                ORDER BY ts DESC
                LIMIT 2000
            )
            SELECT
                a."totalBars",
                a."minWeekStartMs",
                a."maxWeekStartMs",
                r."recentBars7d",
                (
                    SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY range_pct)
                    FROM sample_limited
                ) AS "medianRangePct"
            FROM agg a
            CROSS JOIN recent r;
        `);
        const row = rows[0];
        const bars1m = Math.max(0, Math.floor(Number(row?.totalBars || 0)));
        const minWeekStartMs = Math.floor(Number(row?.minWeekStartMs || 0));
        const maxWeekStartMs = Math.floor(Number(row?.maxWeekStartMs || 0));
        return {
            bars1m,
            fromTs: Number.isFinite(minWeekStartMs) && minWeekStartMs > 0 ? minWeekStartMs : null,
            toTs: Number.isFinite(maxWeekStartMs) && maxWeekStartMs > 0 ? maxWeekStartMs + 7 * 24 * 60 * 60_000 - 1 : null,
            recentBars7d: Math.max(0, Math.floor(Number(row?.recentBars7d || 0))),
            medianRangePct: Number.isFinite(Number(row?.medianRangePct)) ? Number(row?.medianRangePct) : 0,
        };
    }

    const history = await loadScalpCandleHistory(normalized, '1m');
    const candles = history.record?.candles || [];
    const fromTs = candles.length ? Number(candles[0]?.[0]) : null;
    const toTs = candles.length ? Number(candles[candles.length - 1]?.[0]) : null;
    const recentStartTs = nowMs - 7 * 24 * 60 * 60_000;
    const recentBars7d = candles.filter((row) => Number(row[0]) >= recentStartTs).length;
    const medianRangePct = median(
        candles
            .slice(-Math.min(candles.length, 2000))
            .map((row) => {
                const high = Number(row[2]);
                const low = Number(row[3]);
                const close = Number(row[4]);
                if (!Number.isFinite(high) || !Number.isFinite(low) || !Number.isFinite(close) || close <= 0) return 0;
                return ((high - low) / close) * 100;
            })
            .filter((row) => Number.isFinite(row) && row >= 0),
    );
    return {
        bars1m: candles.length,
        fromTs,
        toTs,
        recentBars7d,
        medianRangePct,
    };
}

function clamp(value: number, min: number, max: number): number {
    return Math.max(min, Math.min(max, value));
}

function toOptionalPositiveInt(value: unknown, fallback: number): number {
    const n = Number(value);
    if (!Number.isFinite(n) || n <= 0) return fallback;
    return Math.max(1, Math.floor(n));
}

function historySpanDaysFromCandles(candles: Array<[number, number, number, number, number, number]>): number {
    if (!candles.length) return 0;
    const fromTs = Number(candles[0]?.[0]);
    const toTs = Number(candles[candles.length - 1]?.[0]);
    if (!Number.isFinite(fromTs) || !Number.isFinite(toTs) || toTs <= fromTs) return 0;
    return (toTs - fromTs) / ONE_DAY_MS;
}

export interface SeedHistoryQuality {
    fromTs: number | null;
    toTs: number | null;
    spanDays: number;
    lagHours: number;
    avgBarsPerDay: number;
    recentBars7d: number;
}

export function summarizeSeedHistoryQuality(
    candles: Array<[number, number, number, number, number, number]>,
    nowMs: number,
): SeedHistoryQuality {
    const fromTs = candles.length ? Number(candles[0]?.[0]) : null;
    const toTs = candles.length ? Number(candles[candles.length - 1]?.[0]) : null;
    const spanMs = fromTs !== null && toTs !== null && toTs >= fromTs ? toTs - fromTs : 0;
    const spanDays = spanMs > 0 ? spanMs / ONE_DAY_MS : 0;
    const avgBarsPerDay = spanDays > 0 ? candles.length / spanDays : 0;
    const recentStartTs = nowMs - 7 * ONE_DAY_MS;
    const recentBars7d = candles.filter((row) => Number(row[0]) >= recentStartTs).length;
    const lagHours = toTs !== null ? Math.max(0, (nowMs - toTs) / (60 * 60 * 1000)) : Number.POSITIVE_INFINITY;
    return {
        fromTs,
        toTs,
        spanDays: Number(spanDays.toFixed(4)),
        lagHours: Number.isFinite(lagHours) ? Number(lagHours.toFixed(4)) : Number.POSITIVE_INFINITY,
        avgBarsPerDay: Number(avgBarsPerDay.toFixed(4)),
        recentBars7d,
    };
}

export function resolveSeedSymbolEligibility(params: {
    policy: ScalpSymbolDiscoveryPolicy;
    symbol?: string;
    nowMs: number;
    candles: Array<[number, number, number, number, number, number]>;
    hasStrategyFit: boolean;
    allowBootstrapSymbols: boolean;
}): { eligible: boolean; reason: string | null; quality: SeedHistoryQuality } {
    const quality = summarizeSeedHistoryQuality(params.candles, params.nowMs);
    const criteria = resolveCriteriaForSymbol(params.policy, String(params.symbol || ''));
    if (!params.hasStrategyFit) {
        return { eligible: false, reason: 'seed_no_strategy_fit', quality };
    }
    if (quality.toTs === null && !params.allowBootstrapSymbols) {
        return { eligible: false, reason: 'seed_bootstrap_disabled', quality };
    }
    // Bootstrap mode should also repair corrupted or ultra-sparse history.
    // Final post-fetch quality checks still decide whether the seed succeeds.
    if (params.allowBootstrapSymbols) {
        return { eligible: true, reason: null, quality };
    }
    if (quality.toTs !== null && quality.avgBarsPerDay < criteria.minAvgBarsPerDay) {
        return { eligible: false, reason: 'seed_avg_bars_per_day_below_min', quality };
    }
    if (quality.toTs !== null && quality.recentBars7d < criteria.minRecentBars7d) {
        return { eligible: false, reason: 'seed_recent_bars_7d_below_min', quality };
    }
    return { eligible: true, reason: null, quality };
}

function estimateSeedWindowRequestBudget(params: { fromMs: number; toMs: number; timeframeMs: number; maxPerRequest: number }): number {
    const fromMs = Math.floor(Math.min(params.fromMs, params.toMs));
    const toMs = Math.floor(Math.max(params.fromMs, params.toMs));
    const timeframeMs = Math.max(60_000, Math.floor(params.timeframeMs));
    const maxPerRequest = Math.max(20, Math.min(1000, Math.floor(params.maxPerRequest)));
    if (!(Number.isFinite(fromMs) && Number.isFinite(toMs) && toMs > fromMs)) return 1;
    const bars = Math.max(1, Math.floor((toMs - fromMs) / timeframeMs) + 1);
    const chunkBars = Math.max(20, maxPerRequest - 10);
    return Math.max(1, Math.ceil(bars / chunkBars));
}

function resolveNextSeedFetchWindow(params: {
    candles: Array<[number, number, number, number, number, number]>;
    nowMs: number;
    timeframeMs: number;
    targetHistoryDays: number;
    chunkDays: number;
}): { fromMs: number; toMs: number; reason: 'bootstrap' | 'backfill' | 'forward' } | null {
    const quality = summarizeSeedHistoryQuality(params.candles, params.nowMs);
    const tfMs = Math.max(60_000, Math.floor(params.timeframeMs));
    const chunkSpanMs = Math.max(tfMs, Math.floor(Math.max(1, params.chunkDays) * ONE_DAY_MS));

    if (quality.toTs === null || quality.fromTs === null) {
        const toMs = Math.max(tfMs, Math.floor(params.nowMs));
        const bootstrapDays = Math.max(1, Math.min(params.targetHistoryDays, params.chunkDays));
        const fromMs = Math.max(0, Math.floor(toMs - bootstrapDays * ONE_DAY_MS));
        if (toMs <= fromMs) return null;
        return {
            fromMs,
            toMs,
            reason: 'bootstrap',
        };
    }

    if (quality.spanDays + 1e-6 < params.targetHistoryDays) {
        const toMs = Math.max(0, Math.floor(quality.fromTs - tfMs));
        const fromMs = Math.max(0, Math.floor(toMs - chunkSpanMs));
        if (toMs <= fromMs) return null;
        return {
            fromMs,
            toMs,
            reason: 'backfill',
        };
    }

    const lagMs = Math.max(0, Math.floor(params.nowMs - quality.toTs));
    if (lagMs > SEED_FRESHNESS_MAX_LAG_MS) {
        const fromMs = Math.max(0, Math.floor(quality.toTs + tfMs));
        const toMs = Math.max(fromMs + tfMs, Math.floor(Math.min(params.nowMs, fromMs + chunkSpanMs)));
        if (toMs <= fromMs) return null;
        return {
            fromMs,
            toMs,
            reason: 'forward',
        };
    }

    return null;
}

function trimHistoryToMaxDays(
    candles: Array<[number, number, number, number, number, number]>,
    maxHistoryDays: number,
): Array<[number, number, number, number, number, number]> {
    if (!candles.length) return candles;
    const normalizedMaxDays = Math.max(1, maxHistoryDays);
    const latestTs = Number(candles[candles.length - 1]?.[0] ?? 0);
    if (!Number.isFinite(latestTs) || latestTs <= 0) return candles;
    const cutoffTs = latestTs - normalizedMaxDays * ONE_DAY_MS;
    if (!Number.isFinite(cutoffTs) || cutoffTs <= 0) return candles;
    let firstKeepIdx = 0;
    while (firstKeepIdx < candles.length && Number(candles[firstKeepIdx]?.[0] ?? 0) < cutoffTs) {
        firstKeepIdx += 1;
    }
    if (firstKeepIdx <= 0 || firstKeepIdx >= candles.length) return candles;
    return candles.slice(firstKeepIdx);
}

function historyChanged(
    existing: Array<[number, number, number, number, number, number]>,
    next: Array<[number, number, number, number, number, number]>,
): boolean {
    if (existing.length !== next.length) return true;
    if (!existing.length && !next.length) return false;
    const existingFirst = Number(existing[0]?.[0] ?? 0);
    const nextFirst = Number(next[0]?.[0] ?? 0);
    const existingLast = Number(existing[existing.length - 1]?.[0] ?? 0);
    const nextLast = Number(next[next.length - 1]?.[0] ?? 0);
    return existingFirst !== nextFirst || existingLast !== nextLast;
}

function normalizeFetchedCandles(rows: any[]): Array<[number, number, number, number, number, number]> {
    return rows
        .map((row) => {
            const ts = Number(row?.[0]);
            const open = Number(row?.[1]);
            const high = Number(row?.[2]);
            const low = Number(row?.[3]);
            const close = Number(row?.[4]);
            const volume = Number(row?.[5] ?? 0);
            if (![ts, open, high, low, close].every((v) => Number.isFinite(v) && v > 0)) return null;
            return [Math.floor(ts), open, high, low, close, Number.isFinite(volume) ? volume : 0] as [
                number,
                number,
                number,
                number,
                number,
                number,
            ];
        })
        .filter((row): row is [number, number, number, number, number, number] => Boolean(row))
        .sort((a, b) => a[0] - b[0]);
}

function resolveSeedConfig(
    params: ScalpSymbolDiscoveryRunParams,
    policy: ScalpSymbolDiscoveryPolicy,
): {
    enabled: boolean;
    timeframe: string;
    requestedTopSymbols: number;
    targetHistoryDays: number;
    maxHistoryDays: number;
    chunkDays: number;
    maxRequestsPerSymbol: number;
    maxSymbolsPerRun: number;
    seedOnDryRun: boolean;
    allowBootstrapSymbols: boolean;
} {
    const defaultRequestedTopSymbols = Math.max(
        1,
        Math.min(policy.limits.maxCandidates, Math.max(policy.limits.maxUniverseSymbols, policy.limits.minUniverseSymbols)),
    );
    const requestedTopSymbolsRaw = params.seedTopSymbols ?? process.env.SCALP_SYMBOL_DISCOVERY_SEED_TOP_SYMBOLS;
    const requestedTopSymbols =
        String(requestedTopSymbolsRaw ?? '').trim() === '0'
            ? 0
            : toOptionalPositiveInt(requestedTopSymbolsRaw, defaultRequestedTopSymbols);
    const targetHistoryDays = Math.max(
        7,
        Math.min(365, toOptionalPositiveInt(params.seedTargetHistoryDays ?? process.env.SCALP_SYMBOL_DISCOVERY_SEED_TARGET_DAYS, 90)),
    );
    const maxHistoryDays = Math.max(
        targetHistoryDays,
        Math.min(
            365,
            toOptionalPositiveInt(
                params.seedMaxHistoryDays ?? process.env.SCALP_SYMBOL_DISCOVERY_SEED_MAX_HISTORY_DAYS,
                targetHistoryDays + 5,
            ),
        ),
    );
    const chunkDays = Math.max(
        1,
        Math.min(60, toOptionalPositiveInt(params.seedChunkDays ?? process.env.SCALP_SYMBOL_DISCOVERY_SEED_CHUNK_DAYS, 10)),
    );
    const maxRequestsPerSymbol = Math.max(
        5,
        Math.min(
            300,
            toOptionalPositiveInt(params.seedMaxRequestsPerSymbol ?? process.env.SCALP_SYMBOL_DISCOVERY_SEED_MAX_REQUESTS_PER_SYMBOL, 40),
        ),
    );
    const maxSymbolsPerRun = Math.max(
        1,
        Math.min(
            500,
            toOptionalPositiveInt(params.seedMaxSymbolsPerRun ?? process.env.SCALP_SYMBOL_DISCOVERY_SEED_MAX_SYMBOLS_PER_RUN, requestedTopSymbols || 1),
        ),
    );
    const timeframe = normalizeHistoryTimeframe(String(params.seedTimeframe || process.env.SCALP_SYMBOL_DISCOVERY_SEED_TIMEFRAME || '1m'));
    const seedOnDryRun = Boolean(params.seedOnDryRun);
    const allowBootstrapSymbols = toBool(
        params.seedAllowBootstrapSymbols ?? process.env.SCALP_SYMBOL_DISCOVERY_SEED_ALLOW_BOOTSTRAP_SYMBOLS,
        false,
    );
    return {
        enabled: requestedTopSymbols > 0,
        timeframe,
        requestedTopSymbols,
        targetHistoryDays,
        maxHistoryDays,
        chunkDays,
        maxRequestsPerSymbol,
        maxSymbolsPerRun,
        seedOnDryRun,
        allowBootstrapSymbols,
    };
}

function isBerlinWeekendMs(tsMs: number): boolean {
    try {
        const weekday = new Intl.DateTimeFormat('en-US', { timeZone: 'Europe/Berlin', weekday: 'short' }).format(new Date(tsMs));
        return weekday === 'Sat' || weekday === 'Sun';
    } catch {
        const day = new Date(tsMs).getUTCDay();
        return day === 0 || day === 6;
    }
}

function normalizeBitgetProductTypeForQuery(value: string): string {
    const normalized = String(value || '')
        .trim()
        .toUpperCase()
        .replace(/_/g, '-');
    if (normalized === 'USDT-FUTURES') return 'USDT-FUTURES';
    if (normalized === 'USDC-FUTURES') return 'USDC-FUTURES';
    if (normalized === 'COIN-FUTURES') return 'COIN-FUTURES';
    return 'USDT-FUTURES';
}

function isBitgetDiscoverySymbolCandidate(symbolRaw: string): boolean {
    const symbol = normalizeSymbol(symbolRaw);
    if (!symbol) return false;
    if (/^[A-Z]{6}$/.test(symbol)) return true;
    if (symbol.endsWith('USDT')) return true;
    if (symbol.startsWith('XAU') || symbol.startsWith('XAG')) return true;
    if (
        symbol.endsWith('USD') ||
        symbol.endsWith('EUR') ||
        symbol.endsWith('GBP') ||
        symbol.endsWith('JPY') ||
        symbol.endsWith('AUD') ||
        symbol.endsWith('CAD') ||
        symbol.endsWith('CHF') ||
        symbol.endsWith('NZD')
    ) {
        return symbol.length >= 6 && symbol.length <= 12;
    }
    return false;
}

async function discoverBitgetMarketSymbols(params: {
    maxSymbols?: number;
} = {}): Promise<BitgetDiscoveryResult> {
    const maxSymbols = Math.max(1, Math.min(5000, Math.floor(Number(params.maxSymbols) || 500)));
    const productType = normalizeBitgetProductTypeForQuery(resolveProductType());
    const diagnostics: BitgetDiscoveryResult['diagnostics'] = {
        rowsSeen: 0,
        mappedSymbols: 0,
        errors: [],
        productType,
    };
    const out: string[] = [];
    const seen = new Set<string>();
    const url = `https://api.bitget.com/api/v2/mix/market/contracts?productType=${encodeURIComponent(productType)}`;

    try {
        const res = await fetch(url, {
            method: 'GET',
            headers: {
                'Content-Type': 'application/json',
            },
        });
        const payload = await res.json().catch(() => null);
        if (!res.ok || !payload || payload.code !== '00000') {
            const details =
                payload && typeof payload === 'object'
                    ? `${String((payload as any).code || res.status)}:${String((payload as any).msg || res.statusText)}`
                    : `${res.status}:${res.statusText}`;
            throw new Error(`bitget_contracts_fetch_failed:${details}`);
        }

        const rows = Array.isArray(payload.data) ? payload.data : [];
        diagnostics.rowsSeen = rows.length;
        for (const row of rows) {
            const symbol = normalizeSymbol((row as Record<string, unknown>)?.symbol);
            if (!symbol) continue;
            const status = String((row as Record<string, unknown>)?.symbolStatus || '')
                .trim()
                .toLowerCase();
            if (status && status !== 'normal' && status !== 'listed' && status !== 'trading' && status !== 'online') {
                continue;
            }
            if (!isBitgetDiscoverySymbolCandidate(symbol)) continue;
            if (seen.has(symbol)) continue;
            seen.add(symbol);
            out.push(symbol);
            diagnostics.mappedSymbols += 1;
            if (out.length >= maxSymbols) break;
        }
    } catch (err: any) {
        diagnostics.errors.push(String(err?.message || err || 'bitget_discovery_failed').slice(0, 220));
    }

    return {
        symbols: out,
        diagnostics,
    };
}

export function resolveRecommendedStrategiesForSymbol(symbolRaw: string, allowlist: string[]): string[] {
    const symbol = normalizeSymbol(symbolRaw);
    const has = (id: string) => allowlist.length === 0 || allowlist.includes(id);
    const out: string[] = [];

    const push = (id: string) => {
        if (!has(id)) return;
        out.push(id);
    };

    const fxCcy = new Set([
        'USD',
        'EUR',
        'GBP',
        'JPY',
        'AUD',
        'CAD',
        'CHF',
        'NZD',
        'SEK',
        'NOK',
        'DKK',
        'CNH',
        'HKD',
        'SGD',
        'ZAR',
        'TRY',
        'MXN',
        'PLN',
        'HUF',
        'CZK',
    ]);
    const quoteSuffixes = ['USDT', 'USD', 'EUR', 'GBP', 'JPY'];
    const cryptoBases = new Set([
        'BTC',
        'ETH',
        'SOL',
        'BNB',
        'XRP',
        'ADA',
        'DOGE',
        'LTC',
        'DOT',
        'AVAX',
        'MATIC',
        'LINK',
        'ATOM',
        'BCH',
        'TRX',
        'ETC',
        'UNI',
        'APT',
        'SUI',
        'NEAR',
        'ARB',
        'OP',
        'FIL',
        'AAVE',
        'ICP',
        'XLM',
        'EOS',
        'XTZ',
        'SHIB',
    ]);
    const indexTokens = [
        'US500',
        'SPX500',
        'US30',
        'DJ30',
        'NAS100',
        'USTEC',
        'GER40',
        'DE40',
        'FRA40',
        'EU50',
        'UK100',
        'AUS200',
        'JPN225',
        'JP225',
        'HK50',
        'CHINA50',
        'CN50',
        'ES35',
        'IT40',
        'SWI20',
        'SA40',
    ];
    const energyTokens = ['BRENT', 'WTI', 'USOIL', 'UKOIL', 'NATGAS', 'NGAS', 'CL', 'OIL'];

    const stripQuoteSuffix = (value: string): string => {
        for (const suffix of quoteSuffixes) {
            if (value.endsWith(suffix) && value.length > suffix.length) {
                return value.slice(0, value.length - suffix.length);
            }
        }
        return value;
    };

    const isFx =
        symbol.length === 6 &&
        fxCcy.has(symbol.slice(0, 3)) &&
        fxCcy.has(symbol.slice(3, 6)) &&
        !symbol.endsWith('USDT');
    const isBtc = symbol === 'BTCUSDT' || symbol === 'BTCUSD';
    const isXau = symbol === 'XAUUSDT' || symbol === 'XAUUSD' || symbol === 'GOLD';
    const isMetals =
        isXau ||
        symbol === 'XAGUSDT' ||
        symbol === 'XAGUSD' ||
        symbol === 'SILVER' ||
        symbol.startsWith('XPT') ||
        symbol.startsWith('XPD');
    const isCrypto =
        !isMetals &&
        (symbol.endsWith('USDT') ||
            (symbol.endsWith('USD') && cryptoBases.has(stripQuoteSuffix(symbol))) ||
            cryptoBases.has(symbol));
    const isIndex = indexTokens.some((token) => symbol.startsWith(token) || symbol.includes(token));
    const isEnergy = energyTokens.some((token) => symbol.startsWith(token) || symbol.includes(token));
    const strippedSymbol = stripQuoteSuffix(symbol);
    const isEquityLike =
        !isFx &&
        !isCrypto &&
        !isMetals &&
        !isIndex &&
        !isEnergy &&
        /^[A-Z]{1,6}$/.test(strippedSymbol) &&
        !fxCcy.has(strippedSymbol);

    if (isBtc) {
        push('regime_pullback_m15_m3');
        push('compression_breakout_pullback_m15_m3');
        push('trend_day_reacceleration_m15_m3');
        push('failed_auction_extreme_reversal_m15_m1');
    } else if (isCrypto) {
        push('regime_pullback_m15_m3');
        push('compression_breakout_pullback_m15_m3');
        push('trend_day_reacceleration_m15_m3');
        push('failed_auction_extreme_reversal_m15_m1');
        push('hss_ict_m15_m3_guarded');
    } else if (isXau) {
        push('regime_pullback_m15_m3');
        push('trend_day_reacceleration_m15_m3');
        push('failed_auction_extreme_reversal_m15_m1');
    } else if (isMetals) {
        push('trend_day_reacceleration_m15_m3');
        push('regime_pullback_m15_m3');
        push('failed_auction_extreme_reversal_m15_m1');
    } else if (isIndex) {
        push('trend_day_reacceleration_m15_m3');
        push('regime_pullback_m15_m3');
        push('failed_auction_extreme_reversal_m15_m1');
    } else if (isEnergy) {
        push('trend_day_reacceleration_m15_m3');
        push('failed_auction_extreme_reversal_m15_m1');
        push('regime_pullback_m15_m3');
    } else if (isEquityLike) {
        push('trend_day_reacceleration_m15_m3');
        push('regime_pullback_m15_m3');
        push('pdh_pdl_reclaim_m15_m3');
    } else if (isFx) {
        push('regime_pullback_m15_m3');
        push('pdh_pdl_reclaim_m15_m3');
        push('hss_ict_m15_m3_guarded');
    } else {
        push('regime_pullback_m15_m3');
        push('trend_day_reacceleration_m15_m3');
        push('failed_auction_extreme_reversal_m15_m1');
    }

    return Array.from(new Set(out));
}

async function runScalpSymbolHistorySeedStage(params: {
    policy: ScalpSymbolDiscoveryPolicy;
    nowMs: number;
    dryRun: boolean;
    knownStrategyIds: Set<string>;
    config: ReturnType<typeof resolveSeedConfig>;
    preloadedDiscovery?: BitgetDiscoveryResult | null;
    preloadedDiscoveryError?: string | null;
}): Promise<ScalpSymbolDiscoverySeedSummary> {
    const cfg = params.config;
    const requiredSuccessiveWeeksConfigured = Math.max(
        1,
        Math.min(
            52,
            toOptionalPositiveInt(
                process.env.SCALP_RESEARCH_PREFLIGHT_REQUIRED_SUCCESSIVE_WEEKS,
                12,
            ),
        ),
    );
    const requiredSuccessiveWeeks = Math.max(
        requiredSuccessiveWeeksConfigured,
        Math.ceil(cfg.targetHistoryDays / 7),
    );
    const effectiveTargetHistoryDays = resolveRequiredHistoryDaysForCompletedWeeks({
        nowMs: params.nowMs,
        targetHistoryDays: cfg.targetHistoryDays,
        requiredSuccessiveWeeks,
    });
    const effectiveMaxHistoryDays = Math.max(cfg.maxHistoryDays, effectiveTargetHistoryDays + 5);
    const summary: ScalpSymbolDiscoverySeedSummary = {
        enabled: cfg.enabled,
        dryRun: params.dryRun,
        timeframe: cfg.timeframe,
        requestedTopSymbols: cfg.requestedTopSymbols,
        processedSymbols: 0,
        seededSymbols: 0,
        skippedSymbols: 0,
        failedSymbols: 0,
        targetHistoryDays: effectiveTargetHistoryDays,
        maxHistoryDays: effectiveMaxHistoryDays,
        chunkDays: cfg.chunkDays,
        maxRequestsPerSymbol: cfg.maxRequestsPerSymbol,
        maxSymbolsPerRun: cfg.maxSymbolsPerRun,
        candidateUniverseSize: 0,
        results: [],
    };
    if (!cfg.enabled) return summary;

    const hasPreloadedDiscovery = params.preloadedDiscovery !== undefined;
    let discovered: BitgetDiscoveryResult | null = params.preloadedDiscovery ?? null;
    if (!discovered && !hasPreloadedDiscovery) {
        const maxSymbols = Math.max(cfg.requestedTopSymbols * 6, 200);
        try {
            discovered = await discoverBitgetMarketSymbols({ maxSymbols });
        } catch (err: any) {
            summary.failedSymbols = 1;
            summary.results.push({
                symbol: 'DISCOVERY',
                status: 'failed',
                reason: String(err?.message || err || 'bitget_discovery_failed').slice(0, 180),
                epic: null,
                existingCount: 0,
                mergedCount: 0,
                fetchedCount: 0,
                addedCount: 0,
                trimmedCount: 0,
                beforeSpanDays: 0,
                afterSpanDays: 0,
            });
            return summary;
        }
    }
    if (!discovered) {
        const errorMessage = String(params.preloadedDiscoveryError || 'bitget_discovery_failed').slice(0, 180);
        summary.failedSymbols = 1;
        summary.results.push({
            symbol: 'DISCOVERY',
            status: 'failed',
            reason: errorMessage,
            epic: null,
            existingCount: 0,
            mergedCount: 0,
            fetchedCount: 0,
            addedCount: 0,
            trimmedCount: 0,
            beforeSpanDays: 0,
            afterSpanDays: 0,
        });
        return summary;
    }

    const candidateSymbols = Array.from(
        new Set(
            discovered.symbols
                .map((row) => normalizeSymbol(row))
                .filter((row) => Boolean(row))
                .filter((symbol) => {
                    const strategies = resolveRecommendedStrategiesForSymbol(symbol, params.policy.strategyAllowlist).filter((id) =>
                        params.knownStrategyIds.has(id),
                    );
                    return strategies.length > 0;
                }),
        ),
    );
    summary.candidateUniverseSize = candidateSymbols.length;

    const targetCount = Math.min(candidateSymbols.length, cfg.requestedTopSymbols, cfg.maxSymbolsPerRun);
    const seedEvaluationCap = Math.min(candidateSymbols.length, Math.max(targetCount * 5, targetCount));
    const tfMs = timeframeToMs(cfg.timeframe);
    const maxFetchPassesPerSymbol = Math.max(
        1,
        Math.min(128, Math.ceil(effectiveTargetHistoryDays / Math.max(1, cfg.chunkDays)) + 4),
    );
    const eligibleTargets: Array<{
        symbol: string;
        existing: Array<[number, number, number, number, number, number]>;
        beforeQuality: SeedHistoryQuality;
        epicHint: string | null;
    }> = [];

    for (const symbol of candidateSymbols.slice(0, seedEvaluationCap)) {
        if (eligibleTargets.length >= targetCount) break;
        try {
            const history = await loadScalpCandleHistory(symbol, cfg.timeframe);
            const existing = history.record?.candles || [];
            const strategies = resolveRecommendedStrategiesForSymbol(symbol, params.policy.strategyAllowlist).filter((id) =>
                params.knownStrategyIds.has(id),
            );
            const eligibility = resolveSeedSymbolEligibility({
                policy: params.policy,
                symbol,
                nowMs: params.nowMs,
                candles: existing,
                hasStrategyFit: strategies.length > 0,
                allowBootstrapSymbols: cfg.allowBootstrapSymbols,
            });
            summary.processedSymbols += 1;

            if (!eligibility.eligible) {
                summary.skippedSymbols += 1;
                summary.results.push({
                    symbol,
                    status: 'skipped',
                    reason: String(eligibility.reason || 'seed_ineligible').slice(0, 180),
                    epic: history.record?.epic || null,
                    existingCount: existing.length,
                    mergedCount: existing.length,
                    fetchedCount: 0,
                    addedCount: 0,
                    trimmedCount: 0,
                    beforeSpanDays: eligibility.quality.spanDays,
                    afterSpanDays: eligibility.quality.spanDays,
                });
                continue;
            }

            eligibleTargets.push({
                symbol,
                existing,
                beforeQuality: eligibility.quality,
                epicHint: history.record?.epic || null,
            });
        } catch (err: any) {
            summary.failedSymbols += 1;
            summary.results.push({
                symbol,
                status: 'failed',
                reason: String(err?.message || err || 'seed_eligibility_check_failed').slice(0, 180),
                epic: null,
                existingCount: 0,
                mergedCount: 0,
                fetchedCount: 0,
                addedCount: 0,
                trimmedCount: 0,
                beforeSpanDays: 0,
                afterSpanDays: 0,
            });
        }
    }

    for (const target of eligibleTargets) {
        const symbol = target.symbol;
        try {
            const existing = target.existing;
            const beforeSpanDays = target.beforeQuality.spanDays;
            const epicResolved = symbol;

            let working = trimHistoryToMaxDays(existing.slice(), cfg.maxHistoryDays);
            let totalFetchedCount = 0;
            let totalAddedCount = 0;
            let totalTrimmedCount = Math.max(0, existing.length - working.length);
            let requestBudget = Math.max(1, cfg.maxRequestsPerSymbol);
            let fetchPasses = 0;
            let lastWindowReason: 'bootstrap' | 'backfill' | 'forward' | 'already_seeded' = 'already_seeded';

            while (fetchPasses < maxFetchPassesPerSymbol && requestBudget > 0) {
                const window = resolveNextSeedFetchWindow({
                    candles: working,
                    nowMs: params.nowMs,
                    timeframeMs: tfMs,
                    targetHistoryDays: effectiveTargetHistoryDays,
                    chunkDays: cfg.chunkDays,
                });
                if (!window) break;
                lastWindowReason = window.reason;

                const estimatedRequests = estimateSeedWindowRequestBudget({
                    fromMs: window.fromMs,
                    toMs: window.toMs,
                    timeframeMs: tfMs,
                    maxPerRequest: 1000,
                });
                const requestAllowance = Math.max(1, Math.min(requestBudget, estimatedRequests + 1));
                const fetchedRaw = await fetchBitgetCandlesByEpicDateRange(
                    epicResolved,
                    cfg.timeframe,
                    window.fromMs,
                    window.toMs,
                    {
                        maxPerRequest: 1000,
                        maxRequests: requestAllowance,
                    },
                );
                requestBudget -= requestAllowance;
                fetchPasses += 1;

                const fetched = normalizeFetchedCandles(fetchedRaw);
                totalFetchedCount += fetched.length;
                const merged = mergeScalpCandleHistory(working, fetched);
                totalAddedCount += Math.max(0, merged.length - working.length);
                const trimmed = trimHistoryToMaxDays(merged, effectiveMaxHistoryDays);
                totalTrimmedCount += Math.max(0, merged.length - trimmed.length);
                const changed = historyChanged(working, trimmed);
                working = trimmed;

                if (!changed && fetched.length === 0) {
                    break;
                }
            }

            const afterQuality = summarizeSeedHistoryQuality(working, params.nowMs);
            const lagMs = afterQuality.toTs !== null ? Math.max(0, params.nowMs - afterQuality.toTs) : Number.POSITIVE_INFINITY;
            const targetMet = afterQuality.spanDays + 1e-6 >= effectiveTargetHistoryDays && lagMs <= SEED_FRESHNESS_MAX_LAG_MS;
            const changed = historyChanged(existing, working);

            if (!params.dryRun && changed) {
                await saveScalpCandleHistory({
                    symbol,
                    timeframe: cfg.timeframe,
                    epic: epicResolved,
                    source: 'bitget',
                    candles: working,
                });
            }

            if (!targetMet) {
                summary.failedSymbols += 1;
                summary.results.push({
                    symbol,
                    status: 'failed',
                    reason: `seed_target_unmet:spanDays=${afterQuality.spanDays}:lagHours=${afterQuality.lagHours}`.slice(0, 180),
                    epic: epicResolved,
                    existingCount: existing.length,
                    mergedCount: working.length,
                    fetchedCount: totalFetchedCount,
                    addedCount: totalAddedCount,
                    trimmedCount: totalTrimmedCount,
                    beforeSpanDays,
                    afterSpanDays: afterQuality.spanDays,
                });
            } else if (changed && (totalAddedCount > 0 || totalFetchedCount > 0)) {
                summary.seededSymbols += 1;
                summary.results.push({
                    symbol,
                    status: 'seeded',
                    reason: lastWindowReason,
                    epic: epicResolved,
                    existingCount: existing.length,
                    mergedCount: working.length,
                    fetchedCount: totalFetchedCount,
                    addedCount: totalAddedCount,
                    trimmedCount: totalTrimmedCount,
                    beforeSpanDays,
                    afterSpanDays: afterQuality.spanDays,
                });
            } else if (changed) {
                summary.skippedSymbols += 1;
                summary.results.push({
                    symbol,
                    status: 'skipped',
                    reason: 'history_pruned',
                    epic: target.epicHint || epicResolved,
                    existingCount: existing.length,
                    mergedCount: working.length,
                    fetchedCount: totalFetchedCount,
                    addedCount: 0,
                    trimmedCount: totalTrimmedCount,
                    beforeSpanDays,
                    afterSpanDays: afterQuality.spanDays,
                });
            } else {
                summary.skippedSymbols += 1;
                summary.results.push({
                    symbol,
                    status: 'skipped',
                    reason: 'already_seeded',
                    epic: target.epicHint || epicResolved,
                    existingCount: existing.length,
                    mergedCount: working.length,
                    fetchedCount: totalFetchedCount,
                    addedCount: 0,
                    trimmedCount: totalTrimmedCount,
                    beforeSpanDays,
                    afterSpanDays: afterQuality.spanDays,
                });
            }
        } catch (err: any) {
            summary.failedSymbols += 1;
            summary.results.push({
                symbol,
                status: 'failed',
                reason: String(err?.message || err || 'seed_failed').slice(0, 180),
                epic: null,
                existingCount: 0,
                mergedCount: 0,
                fetchedCount: 0,
                addedCount: 0,
                trimmedCount: 0,
                beforeSpanDays: 0,
                afterSpanDays: 0,
            });
        }
    }

    return summary;
}

async function buildCandidatePool(
    policy: ScalpSymbolDiscoveryPolicy,
    params: {
        includeLiveQuotes: boolean;
        nowMs: number;
        restrictToBitgetSymbols?: boolean;
        discoveredBitgetSymbols?: BitgetDiscoveryResult | null;
        bitgetDiscoveryError?: string | null;
    },
): Promise<{
    symbols: string[];
    diagnostics: NonNullable<ScalpSymbolUniverseSnapshot['diagnostics']>;
}> {
    const pool = new Set<string>();
    const diagnostics: NonNullable<ScalpSymbolUniverseSnapshot['diagnostics']> = {
        sourceEnabled: {
            includeBitgetMarketsApi: policy.sources.includeBitgetMarketsApi,
            includeDeploymentSymbols: policy.sources.includeDeploymentSymbols,
            includeHistorySymbols: policy.sources.includeHistorySymbols,
            requireHistoryPresence: policy.sources.requireHistoryPresence,
            explicitSymbols: policy.sources.explicitSymbols.length > 0,
        },
        sourceCounts: {
            bitgetMarketsApi: 0,
            bitgetMarketsApiRows: 0,
            deploymentSymbols: 0,
            historySymbols: 0,
            explicitSymbols: 0,
            totalUnique: 0,
        },
        bitgetMarketsApiError: null,
        includeLiveQuotes: false,
        requireTradableQuote: policy.criteria.requireTradableQuote,
        quoteGateApplied: false,
    };

    const historySymbols =
        policy.sources.includeHistorySymbols || policy.sources.requireHistoryPresence
            ? await listScalpCandleHistorySymbols('1m')
            : [];
    const historySymbolSet = new Set(historySymbols.map((row) => normalizeSymbol(row)).filter((row) => Boolean(row)));

    const addFromSource = (
        source: keyof NonNullable<ScalpSymbolUniverseSnapshot['diagnostics']>['sourceCounts'],
        rawSymbol: string,
    ) => {
        const normalized = normalizeSymbol(rawSymbol);
        if (!normalized) return;
        if (params.restrictToBitgetSymbols && !isBitgetCompatibleSymbol(normalized)) {
            return;
        }
        if (pool.has(normalized)) return;
        pool.add(normalized);
        diagnostics.sourceCounts[source] += 1;
    };

    if (policy.sources.includeBitgetMarketsApi) {
        const maxSymbols = Math.max(policy.limits.maxCandidates * 3, policy.limits.maxUniverseSymbols * 4, 200);
        const hasPreloadedBitgetDiscovery = params.discoveredBitgetSymbols !== undefined;
        try {
            const discoveredBitgetSymbols =
                hasPreloadedBitgetDiscovery
                    ? params.discoveredBitgetSymbols
                    : await discoverBitgetMarketSymbols({ maxSymbols });
            diagnostics.sourceCounts.bitgetMarketsApiRows = discoveredBitgetSymbols?.diagnostics.rowsSeen || 0;
            if (!discoveredBitgetSymbols) {
                diagnostics.bitgetMarketsApiError = String(
                    params.bitgetDiscoveryError || 'bitget_markets_api_discovery_failed',
                ).slice(0, 220);
            } else if ((discoveredBitgetSymbols.diagnostics.errors || []).length > 0) {
                diagnostics.bitgetMarketsApiError = (discoveredBitgetSymbols.diagnostics.errors || []).slice(0, 3).join(' | ');
            } else if (
                (discoveredBitgetSymbols.diagnostics.rowsSeen || 0) > 0 &&
                (discoveredBitgetSymbols.diagnostics.mappedSymbols || 0) === 0
            ) {
                diagnostics.bitgetMarketsApiError = 'bitget_markets_rows_unmapped';
            }
            for (const symbol of discoveredBitgetSymbols?.symbols || []) {
                if (policy.sources.requireHistoryPresence && !historySymbolSet.has(normalizeSymbol(symbol))) continue;
                addFromSource('bitgetMarketsApi', symbol);
            }
        } catch (err: any) {
            const fallback =
                hasPreloadedBitgetDiscovery && params.bitgetDiscoveryError
                    ? params.bitgetDiscoveryError
                    : String(err?.message || err || 'bitget_markets_api_discovery_failed');
            diagnostics.bitgetMarketsApiError = String(fallback).slice(0, 220);
        }
    }

    if (policy.sources.includeDeploymentSymbols) {
        const deployments = await loadScalpDeploymentRegistry();
        for (const row of deployments.deployments) {
            addFromSource('deploymentSymbols', row.symbol);
        }
    }

    if (policy.sources.includeHistorySymbols) {
        for (const symbol of historySymbols) {
            addFromSource('historySymbols', symbol);
        }
    }

    for (const symbol of policy.sources.explicitSymbols) {
        addFromSource('explicitSymbols', symbol);
    }

    for (const symbol of policy.sources.excludedSymbols) {
        pool.delete(normalizeSymbol(symbol));
    }

    diagnostics.sourceCounts.totalUnique = pool.size;

    return {
        symbols: Array.from(pool).filter((row) => Boolean(row)),
        diagnostics,
    };
}

export async function evaluateScalpSymbolCandidate(params: {
    symbol: string;
    policy: ScalpSymbolDiscoveryPolicy;
    nowMs: number;
    includeLiveQuotes: boolean;
    knownStrategyIds: Set<string>;
}): Promise<ScalpSymbolCandidateRow> {
    const symbol = normalizeSymbol(params.symbol);
    const criteria = resolveCriteriaForSymbol(params.policy, symbol);
    const historyStats = await loadSymbolHistoryStats(symbol, params.nowMs);
    const fromTs = historyStats.fromTs;
    const toTs = historyStats.toTs;
    const spanMs = fromTs !== null && toTs !== null && toTs >= fromTs ? toTs - fromTs : 0;
    const spanDays = spanMs > 0 ? spanMs / (24 * 60 * 60_000) : 0;
    const expectedBars = spanMs > 0 ? Math.max(1, Math.floor(spanMs / 60_000) + 1) : 0;
    const coveragePct = expectedBars > 0 ? clamp((historyStats.bars1m / expectedBars) * 100, 0, 100) : 0;
    const avgBarsPerDay = spanDays > 0 ? historyStats.bars1m / spanDays : 0;
    const recentBars7d = historyStats.recentBars7d;
    const medianRangePct = historyStats.medianRangePct;

    let livePrice: number | null = null;
    let liveSpreadPips: number | null = null;
    if (params.includeLiveQuotes) {
        try {
            const quoteRaw = await bitgetFetch("GET", "/api/v2/mix/market/ticker", {
                symbol,
                productType: String(resolveProductType() || "usdt-futures")
                    .trim()
                    .toUpperCase(),
            });
            const quote = Array.isArray(quoteRaw) ? quoteRaw[0] : quoteRaw;
            const bid = Number((quote as any)?.bidPr);
            const offer = Number((quote as any)?.askPr);
            const mid = Number((quote as any)?.lastPr ?? (quote as any)?.last);
            if (Number.isFinite(mid) && mid > 0) livePrice = mid;
            if (Number.isFinite(bid) && Number.isFinite(offer) && offer >= bid) {
                const pipSize = pipSizeForScalpSymbol(symbol);
                if (pipSize > 0) {
                    liveSpreadPips = (offer - bid) / pipSize;
                }
                if (livePrice === null && offer > 0 && bid > 0) {
                    livePrice = (offer + bid) / 2;
                }
            }
        } catch {
            // Keep quote metrics null if Bitget query fails.
        }
    }

    const reasons: string[] = [];
    if (spanDays < criteria.minHistoryDays) reasons.push('history_days_below_min');
    if (coveragePct < criteria.minHistoryCoveragePct) reasons.push('history_coverage_below_min');
    if (avgBarsPerDay < criteria.minAvgBarsPerDay) reasons.push('avg_bars_per_day_below_min');
    if (recentBars7d < criteria.minRecentBars7d) reasons.push('recent_bars_7d_below_min');
    if (medianRangePct < criteria.minMedianRangePct) reasons.push('median_range_pct_below_min');

    const isBerlinWeekend = isBerlinWeekendMs(params.nowMs);
    const quoteGateApplied = criteria.requireTradableQuote && params.includeLiveQuotes && !isBerlinWeekend;
    if (quoteGateApplied) {
        if (livePrice === null || livePrice <= 0) reasons.push('live_quote_missing_or_invalid');
    }
    if (
        !isBerlinWeekend &&
        criteria.maxSpreadPips !== null &&
        liveSpreadPips !== null &&
        liveSpreadPips > criteria.maxSpreadPips
    ) {
        reasons.push('live_spread_above_max');
    }

    let score = 0;
    score += clamp((coveragePct / 100) * 25, 0, 25);
    score += clamp((spanDays / Math.max(criteria.minHistoryDays, 1)) * 20, 0, 20);
    score += clamp((avgBarsPerDay / Math.max(criteria.minAvgBarsPerDay, 1)) * 20, 0, 20);
    score += clamp((recentBars7d / Math.max(criteria.minRecentBars7d, 1)) * 15, 0, 15);
    score += clamp((medianRangePct / Math.max(criteria.minMedianRangePct, 0.0001)) * 10, 0, 10);
    if (liveSpreadPips !== null && criteria.maxSpreadPips !== null) {
        const spreadScore = liveSpreadPips <= 0 ? 10 : clamp((criteria.maxSpreadPips / liveSpreadPips) * 10, 0, 10);
        score += spreadScore;
    } else if (liveSpreadPips !== null) {
        score += 5;
    }

    const strategies = resolveRecommendedStrategiesForSymbol(symbol, params.policy.strategyAllowlist).filter((id) =>
        params.knownStrategyIds.has(id),
    );
    if (strategies.length === 0) {
        reasons.push('no_strategy_fit');
    }

    return {
        symbol,
        eligible: reasons.length === 0,
        score: Number(score.toFixed(4)),
        reasons,
        recommendedStrategyIds: strategies,
        metrics: {
            historyBars1m: historyStats.bars1m,
            historyFromTs: fromTs,
            historyToTs: toTs,
            historySpanDays: Number(spanDays.toFixed(4)),
            historyCoveragePct: Number(coveragePct.toFixed(4)),
            avgBarsPerDay: Number(avgBarsPerDay.toFixed(4)),
            recentBars7d,
            medianRangePct: Number(medianRangePct.toFixed(6)),
            livePrice: livePrice !== null ? Number(livePrice.toFixed(10)) : null,
            liveSpreadPips: liveSpreadPips !== null ? Number(liveSpreadPips.toFixed(4)) : null,
        },
    };
}

function byScoreDesc(a: ScalpSymbolCandidateRow, b: ScalpSymbolCandidateRow): number {
    if (b.score !== a.score) return b.score - a.score;
    return a.symbol.localeCompare(b.symbol);
}

function normalizeCursorOffset(value: unknown): number {
    const n = Math.floor(Number(value));
    if (!Number.isFinite(n) || n < 0) return 0;
    return n;
}

export function resolveCandidateEvaluationWindow(params: {
    symbols: string[];
    maxCandidates: number;
    startOffset?: number;
}): {
    selectedSymbols: string[];
    poolSize: number;
    maxCandidates: number;
    startOffset: number;
    evaluatedCount: number;
    nextOffset: number;
} {
    const symbols = Array.isArray(params.symbols) ? params.symbols.filter((row) => Boolean(row)) : [];
    const poolSize = symbols.length;
    const maxCandidates = Math.max(1, Math.floor(Number(params.maxCandidates) || 1));
    if (poolSize === 0) {
        return {
            selectedSymbols: [],
            poolSize,
            maxCandidates,
            startOffset: 0,
            evaluatedCount: 0,
            nextOffset: 0,
        };
    }
    const evaluatedCount = Math.min(poolSize, maxCandidates);
    const startOffset = normalizeCursorOffset(params.startOffset) % poolSize;
    const selectedSymbols: string[] = [];
    for (let i = 0; i < evaluatedCount; i += 1) {
        selectedSymbols.push(symbols[(startOffset + i) % poolSize] as string);
    }
    const nextOffset = (startOffset + evaluatedCount) % poolSize;
    return {
        selectedSymbols,
        poolSize,
        maxCandidates,
        startOffset,
        evaluatedCount,
        nextOffset,
    };
}

const SCALP_ASSET_LEVERAGE_PRIORITY: Record<ScalpAssetCategory, number> = {
    forex: 0,
    index: 1,
    commodity: 2,
    equity: 3,
    crypto: 4,
    other: 5,
};

function leveragePriorityForSymbol(symbol: string): number {
    const category = inferScalpAssetCategory(symbol);
    return SCALP_ASSET_LEVERAGE_PRIORITY[category] ?? SCALP_ASSET_LEVERAGE_PRIORITY.other;
}

function compareByLeveragePriorityThenScore(
    a: Pick<ScalpSymbolCandidateRow, 'symbol' | 'score'>,
    b: Pick<ScalpSymbolCandidateRow, 'symbol' | 'score'>,
): number {
    const leverageCmp = leveragePriorityForSymbol(a.symbol) - leveragePriorityForSymbol(b.symbol);
    if (leverageCmp !== 0) return leverageCmp;
    if (b.score !== a.score) return b.score - a.score;
    return a.symbol.localeCompare(b.symbol);
}

function sortSymbolsByLeveragePriority(
    symbols: string[],
    scoreBySymbol: Map<string, number>,
): string[] {
    return symbols.slice().sort((a, b) => {
        const leverageCmp = leveragePriorityForSymbol(a) - leveragePriorityForSymbol(b);
        if (leverageCmp !== 0) return leverageCmp;
        const aScore = Number(scoreBySymbol.get(a) ?? -1);
        const bScore = Number(scoreBySymbol.get(b) ?? -1);
        if (bScore !== aScore) return bScore - aScore;
        return a.localeCompare(b);
    });
}

export function buildNextUniverseWithChurnCaps(params: {
    previousSymbols: string[];
    candidateRows: ScalpSymbolCandidateRow[];
    policy: ScalpSymbolDiscoveryPolicy;
    pinnedSymbols: string[];
}): { selectedSymbols: string[]; addedSymbols: string[]; removedSymbols: string[] } {
    const previous = Array.from(new Set(params.previousSymbols.map((row) => normalizeSymbol(row)).filter((row) => Boolean(row))));
    const pinned = new Set(params.pinnedSymbols.map((row) => normalizeSymbol(row)).filter((row) => Boolean(row)));
    const scoreBySymbol = new Map(params.candidateRows.map((row) => [row.symbol, row.score]));

    const eligibleSorted = params.candidateRows
        .filter((row) => row.eligible)
        .slice()
        .sort(compareByLeveragePriorityThenScore);
    const targetTop = eligibleSorted.slice(0, params.policy.limits.maxUniverseSymbols).map((row) => row.symbol);
    const targetSet = new Set(targetTop);

    if (previous.length === 0) {
        const first = sortSymbolsByLeveragePriority(Array.from(new Set([...Array.from(pinned), ...targetTop])), scoreBySymbol).slice(
            0,
            params.policy.limits.maxUniverseSymbols,
        );
        return {
            selectedSymbols: first,
            addedSymbols: first.slice(),
            removedSymbols: [],
        };
    }

    const next = new Set(previous);

    const removable = previous
        .filter((symbol) => !pinned.has(symbol))
        .filter((symbol) => !targetSet.has(symbol));
    const removeCount = Math.min(params.policy.limits.maxWeeklyRemoves, removable.length);
    for (const symbol of removable.slice(0, removeCount)) {
        next.delete(symbol);
    }

    const addable = targetTop.filter((symbol) => !next.has(symbol));
    const addCount = Math.min(params.policy.limits.maxWeeklyAdds, addable.length);
    for (const symbol of addable.slice(0, addCount)) {
        if (next.size >= params.policy.limits.maxUniverseSymbols) break;
        next.add(symbol);
    }

    for (const symbol of pinned) {
        next.add(symbol);
    }

    if (next.size < params.policy.limits.minUniverseSymbols) {
        for (const symbol of targetTop) {
            next.add(symbol);
            if (next.size >= params.policy.limits.minUniverseSymbols) break;
        }
    }

    if (next.size > params.policy.limits.maxUniverseSymbols) {
        const sorted = Array.from(next).sort((a, b) => {
            const aPinned = pinned.has(a) ? 1 : 0;
            const bPinned = pinned.has(b) ? 1 : 0;
            if (aPinned !== bPinned) return bPinned - aPinned;
            const leverageCmp = leveragePriorityForSymbol(a) - leveragePriorityForSymbol(b);
            if (leverageCmp !== 0) return leverageCmp;
            const aScore = scoreBySymbol.get(a) ?? -1;
            const bScore = scoreBySymbol.get(b) ?? -1;
            if (bScore !== aScore) return bScore - aScore;
            return a.localeCompare(b);
        });
        next.clear();
        for (const symbol of sorted.slice(0, params.policy.limits.maxUniverseSymbols)) {
            next.add(symbol);
        }
    }

    const selectedSymbols = sortSymbolsByLeveragePriority(Array.from(next), scoreBySymbol);
    const previousSet = new Set(previous);
    const selectedSet = new Set(selectedSymbols);
    const addedSymbols = selectedSymbols.filter((row) => !previousSet.has(row));
    const removedSymbols = previous.filter((row) => !selectedSet.has(row));

    return {
        selectedSymbols,
        addedSymbols,
        removedSymbols,
    };
}

export async function runScalpSymbolDiscoveryCycle(
    params: ScalpSymbolDiscoveryRunParams = {},
): Promise<ScalpSymbolUniverseSnapshot> {
    const nowMs = Number.isFinite(Number(params.nowMs)) ? Math.floor(Number(params.nowMs)) : Date.now();
    const dryRun = Boolean(params.dryRun);
    const includeLiveQuotes = params.includeLiveQuotes ?? true;
    const restrictToBitgetSymbols = Boolean(params.restrictToBitgetSymbols);

    const policy = applyPolicySourceOverrides(
        await loadScalpSymbolDiscoveryPolicy(),
        params.sourceOverrides,
    );
    const previous = await loadScalpSymbolUniverseSnapshot();

    const knownStrategies = new Set(listScalpStrategies().map((row) => row.id));
    const seedConfig = resolveSeedConfig(params, policy);
    const shouldLoadBitgetDiscovery = policy.sources.includeBitgetMarketsApi;
    const seedShouldRun = shouldLoadBitgetDiscovery && seedConfig.enabled && (!dryRun || seedConfig.seedOnDryRun);
    let preloadedBitgetDiscovery: BitgetDiscoveryResult | null | undefined = undefined;
    let preloadedBitgetDiscoveryError: string | null = null;
    if (shouldLoadBitgetDiscovery) {
        const maxSymbols = Math.max(
            seedConfig.requestedTopSymbols * 6,
            policy.limits.maxCandidates * 3,
            policy.limits.maxUniverseSymbols * 4,
            200,
        );
        preloadedBitgetDiscovery = await discoverBitgetMarketSymbols({ maxSymbols });
        if ((preloadedBitgetDiscovery.diagnostics.errors || []).length > 0) {
            preloadedBitgetDiscoveryError = (preloadedBitgetDiscovery.diagnostics.errors || []).slice(0, 3).join(' | ');
        }
    }
    const seedSummary = seedShouldRun
        ? await runScalpSymbolHistorySeedStage({
              policy,
              nowMs,
              dryRun,
              knownStrategyIds: knownStrategies,
              config: seedConfig,
              preloadedDiscovery: preloadedBitgetDiscovery,
              preloadedDiscoveryError: preloadedBitgetDiscoveryError,
          })
        : seedConfig.enabled && shouldLoadBitgetDiscovery
          ? {
                enabled: true,
                dryRun,
                timeframe: seedConfig.timeframe,
                requestedTopSymbols: seedConfig.requestedTopSymbols,
                processedSymbols: 0,
                seededSymbols: 0,
                skippedSymbols: 0,
                failedSymbols: 0,
                targetHistoryDays: seedConfig.targetHistoryDays,
                maxHistoryDays: seedConfig.maxHistoryDays,
                chunkDays: seedConfig.chunkDays,
                maxRequestsPerSymbol: seedConfig.maxRequestsPerSymbol,
                maxSymbolsPerRun: seedConfig.maxSymbolsPerRun,
                candidateUniverseSize: 0,
                results: [],
            }
          : null;

    const candidatePool = await buildCandidatePool(policy, {
        includeLiveQuotes,
        nowMs,
        restrictToBitgetSymbols,
        discoveredBitgetSymbols: preloadedBitgetDiscovery,
        bitgetDiscoveryError: preloadedBitgetDiscoveryError,
    });
    const maxCandidates = Math.max(1, Math.floor(Number(params.maxCandidatesOverride || policy.limits.maxCandidates) || 1));
    const previousCursorOffset = normalizeCursorOffset(previous?.evaluationWindow?.nextOffset);
    const evaluationWindow = resolveCandidateEvaluationWindow({
        symbols: candidatePool.symbols,
        maxCandidates,
        startOffset: previousCursorOffset,
    });
    const cappedPool = evaluationWindow.selectedSymbols;

    const rows: ScalpSymbolCandidateRow[] = [];
    for (const symbol of cappedPool) {
        rows.push(
            await evaluateScalpSymbolCandidate({
                symbol,
                policy,
                nowMs,
                includeLiveQuotes,
                knownStrategyIds: knownStrategies,
            }),
        );
    }

    const sorted = rows.slice().sort(byScoreDesc);
    const previousSymbolsForChurn = restrictToBitgetSymbols
        ? (previous?.selectedSymbols || []).filter((row) => isBitgetCompatibleSymbol(row))
        : previous?.selectedSymbols || [];
    const pinnedSymbolsForChurn = restrictToBitgetSymbols
        ? policy.pinnedSymbols.filter((row) => isBitgetCompatibleSymbol(row))
        : policy.pinnedSymbols;
    const churn = buildNextUniverseWithChurnCaps({
        previousSymbols: previousSymbolsForChurn,
        candidateRows: sorted,
        policy,
        pinnedSymbols: pinnedSymbolsForChurn,
    });

    const selectedSet = new Set(churn.selectedSymbols);
    const selectedRows = sorted.filter((row) => selectedSet.has(row.symbol));
    const rejectedRows = sorted.filter((row) => !selectedSet.has(row.symbol));

    const snapshot: ScalpSymbolUniverseSnapshot = {
        version: 1,
        generatedAtIso: new Date(nowMs).toISOString(),
        policy,
        source: 'weekly_discovery_v1',
        dryRun,
        previousSymbols: previousSymbolsForChurn,
        selectedSymbols: churn.selectedSymbols,
        addedSymbols: churn.addedSymbols,
        removedSymbols: churn.removedSymbols,
        candidatesEvaluated: evaluationWindow.evaluatedCount,
        evaluationWindow,
        selectedRows,
        topRejectedRows: rejectedRows.slice(0, Math.max(10, policy.limits.maxUniverseSymbols)),
        diagnostics: {
            ...candidatePool.diagnostics,
            includeLiveQuotes,
            requireTradableQuote: policy.criteria.requireTradableQuote,
            quoteGateApplied: includeLiveQuotes && policy.criteria.requireTradableQuote && !isBerlinWeekendMs(nowMs),
        },
        seedSummary,
    };

    if (!dryRun) {
        await saveScalpSymbolUniverseSnapshot(snapshot);
    }

    return snapshot;
}
