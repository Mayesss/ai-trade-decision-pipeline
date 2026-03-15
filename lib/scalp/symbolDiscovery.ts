import { mkdir, readFile, writeFile } from 'node:fs/promises';
import path from 'node:path';

import { Prisma } from '@prisma/client';

import defaultTickerEpicMap from '../../data/capitalTickerMap.json';
import { discoverCapitalMarketSymbols, fetchCapitalCandlesByEpicDateRange, fetchCapitalLivePrice, resolveCapitalEpicRuntime } from '../capital';
import { listScalpCandleHistorySymbols, loadScalpCandleHistory, mergeScalpCandleHistory, normalizeHistoryTimeframe, saveScalpCandleHistory, timeframeToMs } from './candleHistory';
import { loadScalpDeploymentRegistry } from './deploymentRegistry';
import { pipSizeForScalpSymbol } from './marketData';
import { isScalpPgConfigured, scalpPrisma } from './pg/client';
import { listScalpStrategies } from './strategies/registry';

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
    criteria: {
        minHistoryDays: number;
        minHistoryCoveragePct: number;
        minAvgBarsPerDay: number;
        minRecentBars7d: number;
        minMedianRangePct: number;
        maxSpreadPips: number | null;
        requireTradableQuote: boolean;
    };
    sources: {
        includeCapitalMarketsApi: boolean;
        includeCapitalTickerMap: boolean;
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
    selectedRows: ScalpSymbolCandidateRow[];
    topRejectedRows: ScalpSymbolCandidateRow[];
    diagnostics?: {
        sourceEnabled: {
            includeCapitalMarketsApi: boolean;
            includeCapitalTickerMap: boolean;
            includeDeploymentSymbols: boolean;
            includeHistorySymbols: boolean;
            requireHistoryPresence: boolean;
            explicitSymbols: boolean;
        };
        sourceCounts: {
            capitalMarketsApi: number;
            capitalMarketsApiRows: number;
            capitalMarketsApiRowsTradeable: number;
            capitalTickerMap: number;
            deploymentSymbols: number;
            historySymbols: number;
            explicitSymbols: number;
            totalUnique: number;
        };
        capitalMarketsApiError: string | null;
        includeLiveQuotes: boolean;
        requireTradableQuote: boolean;
        quoteGateApplied: boolean;
    };
    seedSummary?: ScalpSymbolDiscoverySeedSummary | null;
}

export interface ScalpSymbolDiscoveryRunParams {
    dryRun?: boolean;
    includeLiveQuotes?: boolean;
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

type CapitalDiscoveryResult = Awaited<ReturnType<typeof discoverCapitalMarketSymbols>>;

const DEFAULT_POLICY_PATH = path.resolve(process.cwd(), 'data/scalp-symbol-discovery-policy.json');
const DEFAULT_UNIVERSE_FILE_PATH = path.resolve(process.cwd(), 'data/scalp-symbol-universe.json');
const UNIVERSE_SNAPSHOT_KEY = 'latest:v1';
const UNIVERSE_SNAPSHOT_JOBS_KEY = 'state:symbol_universe_snapshot:v1';
const ONE_DAY_MS = 24 * 60 * 60 * 1000;
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
    },
    sources: {
        includeCapitalMarketsApi: true,
        includeCapitalTickerMap: false,
        includeDeploymentSymbols: true,
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

    const maxUniverseSymbols = Math.max(1, toPositiveInt((row.limits as any)?.maxUniverseSymbols, DEFAULT_POLICY.limits.maxUniverseSymbols));
    const minUniverseSymbols = Math.max(
        1,
        Math.min(maxUniverseSymbols, toPositiveInt((row.limits as any)?.minUniverseSymbols, DEFAULT_POLICY.limits.minUniverseSymbols)),
    );

    const maxSpreadPipsRaw = toNumber((row.criteria as any)?.maxSpreadPips, NaN);
    const maxSpreadPips = Number.isFinite(maxSpreadPipsRaw) && maxSpreadPipsRaw >= 0 ? maxSpreadPipsRaw : null;

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
            minHistoryDays: Math.max(1, toNonNegativeNumber((row.criteria as any)?.minHistoryDays, DEFAULT_POLICY.criteria.minHistoryDays)),
            minHistoryCoveragePct: Math.max(
                1,
                Math.min(100, toNonNegativeNumber((row.criteria as any)?.minHistoryCoveragePct, DEFAULT_POLICY.criteria.minHistoryCoveragePct)),
            ),
            minAvgBarsPerDay: Math.max(
                1,
                toNonNegativeNumber((row.criteria as any)?.minAvgBarsPerDay, DEFAULT_POLICY.criteria.minAvgBarsPerDay),
            ),
            minRecentBars7d: Math.max(
                0,
                toNonNegativeNumber((row.criteria as any)?.minRecentBars7d, DEFAULT_POLICY.criteria.minRecentBars7d),
            ),
            minMedianRangePct: Math.max(
                0,
                toNonNegativeNumber((row.criteria as any)?.minMedianRangePct, DEFAULT_POLICY.criteria.minMedianRangePct),
            ),
            maxSpreadPips,
            requireTradableQuote: toBool((row.criteria as any)?.requireTradableQuote, DEFAULT_POLICY.criteria.requireTradableQuote),
        },
        sources: {
            includeCapitalMarketsApi: toBool(
                (row.sources as any)?.includeCapitalMarketsApi,
                DEFAULT_POLICY.sources.includeCapitalMarketsApi,
            ),
            includeCapitalTickerMap: toBool(
                (row.sources as any)?.includeCapitalTickerMap,
                DEFAULT_POLICY.sources.includeCapitalTickerMap,
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
                    MAX((EXTRACT(EPOCH FROM week_start) * 1000)::bigint) AS "maxWeekStartMs",
                    COALESCE(
                        SUM(
                            CASE
                                WHEN week_start >= to_timestamp(${recentStartTs} / 1000.0)
                                THEN jsonb_array_length(candles_json)
                                ELSE 0
                            END
                        ),
                        0
                    )::bigint AS "recentBars7d"
                FROM scalp_candle_history_weeks
                WHERE symbol = ${normalized}
                  AND timeframe = '1m'
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
                a."recentBars7d",
                (
                    SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY range_pct)
                    FROM sample_limited
                ) AS "medianRangePct"
            FROM agg a;
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
    nowMs: number;
    candles: Array<[number, number, number, number, number, number]>;
    hasStrategyFit: boolean;
    allowBootstrapSymbols: boolean;
}): { eligible: boolean; reason: string | null; quality: SeedHistoryQuality } {
    const quality = summarizeSeedHistoryQuality(params.candles, params.nowMs);
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
    if (quality.toTs !== null && quality.avgBarsPerDay < params.policy.criteria.minAvgBarsPerDay) {
        return { eligible: false, reason: 'seed_avg_bars_per_day_below_min', quality };
    }
    if (quality.toTs !== null && quality.recentBars7d < params.policy.criteria.minRecentBars7d) {
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
        push('failed_auction_extreme_reversal_m15_m1');
    } else if (isCrypto) {
        push('regime_pullback_m15_m3');
        push('compression_breakout_pullback_m15_m3');
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
    preloadedDiscovery?: CapitalDiscoveryResult | null;
    preloadedDiscoveryError?: string | null;
}): Promise<ScalpSymbolDiscoverySeedSummary> {
    const cfg = params.config;
    const summary: ScalpSymbolDiscoverySeedSummary = {
        enabled: cfg.enabled,
        dryRun: params.dryRun,
        timeframe: cfg.timeframe,
        requestedTopSymbols: cfg.requestedTopSymbols,
        processedSymbols: 0,
        seededSymbols: 0,
        skippedSymbols: 0,
        failedSymbols: 0,
        targetHistoryDays: cfg.targetHistoryDays,
        maxHistoryDays: cfg.maxHistoryDays,
        chunkDays: cfg.chunkDays,
        maxRequestsPerSymbol: cfg.maxRequestsPerSymbol,
        maxSymbolsPerRun: cfg.maxSymbolsPerRun,
        candidateUniverseSize: 0,
        results: [],
    };
    if (!cfg.enabled) return summary;

    const hasPreloadedDiscovery = params.preloadedDiscovery !== undefined;
    let discovered: CapitalDiscoveryResult | null = params.preloadedDiscovery ?? null;
    if (!discovered && !hasPreloadedDiscovery) {
        const maxSymbols = Math.max(cfg.requestedTopSymbols * 6, 200);
        try {
            discovered = await discoverCapitalMarketSymbols({
                maxSymbols,
                // Keep weekend/non-tradeable rows to build a broader seed queue.
                requireTradeable: false,
            });
        } catch (err: any) {
            summary.failedSymbols = 1;
            summary.results.push({
                symbol: 'DISCOVERY',
                status: 'failed',
                reason: String(err?.message || err || 'capital_discovery_failed').slice(0, 180),
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
        const errorMessage = String(params.preloadedDiscoveryError || 'capital_discovery_failed').slice(0, 180);
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
        Math.min(128, Math.ceil(cfg.targetHistoryDays / Math.max(1, cfg.chunkDays)) + 4),
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
            const epicResolved = await resolveCapitalEpicRuntime(symbol);

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
                    targetHistoryDays: cfg.targetHistoryDays,
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
                const fetchedRaw = await fetchCapitalCandlesByEpicDateRange(
                    epicResolved.epic,
                    cfg.timeframe,
                    window.fromMs,
                    window.toMs,
                    {
                        maxPerRequest: 1000,
                        maxRequests: requestAllowance,
                        debug: false,
                        debugLabel: `discovery-seed:${symbol}:${cfg.timeframe}:${window.reason}`,
                    },
                );
                requestBudget -= requestAllowance;
                fetchPasses += 1;

                const fetched = normalizeFetchedCandles(fetchedRaw);
                totalFetchedCount += fetched.length;
                const merged = mergeScalpCandleHistory(working, fetched);
                totalAddedCount += Math.max(0, merged.length - working.length);
                const trimmed = trimHistoryToMaxDays(merged, cfg.maxHistoryDays);
                totalTrimmedCount += Math.max(0, merged.length - trimmed.length);
                const changed = historyChanged(working, trimmed);
                working = trimmed;

                if (!changed && fetched.length === 0) {
                    break;
                }
            }

            const afterQuality = summarizeSeedHistoryQuality(working, params.nowMs);
            const lagMs = afterQuality.toTs !== null ? Math.max(0, params.nowMs - afterQuality.toTs) : Number.POSITIVE_INFINITY;
            const targetMet = afterQuality.spanDays + 1e-6 >= cfg.targetHistoryDays && lagMs <= SEED_FRESHNESS_MAX_LAG_MS;
            const changed = historyChanged(existing, working);

            if (!params.dryRun && changed) {
                await saveScalpCandleHistory({
                    symbol,
                    timeframe: cfg.timeframe,
                    epic: epicResolved.epic,
                    source: 'capital',
                    candles: working,
                });
            }

            if (!targetMet) {
                summary.failedSymbols += 1;
                summary.results.push({
                    symbol,
                    status: 'failed',
                    reason: `seed_target_unmet:spanDays=${afterQuality.spanDays}:lagHours=${afterQuality.lagHours}`.slice(0, 180),
                    epic: epicResolved.epic,
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
                    epic: epicResolved.epic,
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
                    epic: target.epicHint || epicResolved.epic,
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
                    epic: target.epicHint || epicResolved.epic,
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
        discoveredSymbols?: CapitalDiscoveryResult | null;
        discoveryError?: string | null;
    },
): Promise<{
    symbols: string[];
    diagnostics: NonNullable<ScalpSymbolUniverseSnapshot['diagnostics']>;
}> {
    const pool = new Set<string>();
    const diagnostics: NonNullable<ScalpSymbolUniverseSnapshot['diagnostics']> = {
        sourceEnabled: {
            includeCapitalMarketsApi: policy.sources.includeCapitalMarketsApi,
            includeCapitalTickerMap: policy.sources.includeCapitalTickerMap,
            includeDeploymentSymbols: policy.sources.includeDeploymentSymbols,
            includeHistorySymbols: policy.sources.includeHistorySymbols,
            requireHistoryPresence: policy.sources.requireHistoryPresence,
            explicitSymbols: policy.sources.explicitSymbols.length > 0,
        },
        sourceCounts: {
            capitalMarketsApi: 0,
            capitalMarketsApiRows: 0,
            capitalMarketsApiRowsTradeable: 0,
            capitalTickerMap: 0,
            deploymentSymbols: 0,
            historySymbols: 0,
            explicitSymbols: 0,
            totalUnique: 0,
        },
        capitalMarketsApiError: null,
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
        if (pool.has(normalized)) return;
        pool.add(normalized);
        diagnostics.sourceCounts[source] += 1;
    };

    const isBerlinWeekend = isBerlinWeekendMs(params.nowMs);
    const requireTradeableForDiscovery = policy.criteria.requireTradableQuote && params.includeLiveQuotes && !isBerlinWeekend;

    if (policy.sources.includeCapitalMarketsApi) {
        const maxSymbols = Math.max(policy.limits.maxCandidates * 3, policy.limits.maxUniverseSymbols * 4, 200);
        const hasPreloadedDiscovery = params.discoveredSymbols !== undefined;
        try {
            const discoveredSymbols =
                hasPreloadedDiscovery
                    ? params.discoveredSymbols
                    : await discoverCapitalMarketSymbols({
                          maxSymbols,
                          requireTradeable: requireTradeableForDiscovery,
                      });
            diagnostics.sourceCounts.capitalMarketsApiRows = discoveredSymbols?.diagnostics.rowsSeen || 0;
            diagnostics.sourceCounts.capitalMarketsApiRowsTradeable = discoveredSymbols?.diagnostics.rowsTradeable || 0;
            if (!discoveredSymbols) {
                diagnostics.capitalMarketsApiError = String(
                    params.discoveryError || 'capital_markets_api_discovery_failed',
                ).slice(0, 220);
            }
            if (
                (discoveredSymbols?.diagnostics.termsSucceeded || 0) === 0 &&
                (discoveredSymbols?.diagnostics.errors.length || 0) > 0
            ) {
                diagnostics.capitalMarketsApiError = (discoveredSymbols?.diagnostics.errors || []).slice(0, 3).join(' | ');
            } else if (
                (discoveredSymbols?.diagnostics.rowsSeen || 0) > 0 &&
                (discoveredSymbols?.diagnostics.mappedSymbols || 0) === 0
            ) {
                diagnostics.capitalMarketsApiError = 'capital_markets_rows_unmapped';
            }
            if (policy.sources.requireHistoryPresence && historySymbolSet.size === 0) {
                diagnostics.capitalMarketsApiError = diagnostics.capitalMarketsApiError
                    ? `${diagnostics.capitalMarketsApiError} | history_presence_index_empty`
                    : 'history_presence_index_empty';
            }
            for (const symbol of discoveredSymbols?.symbols || []) {
                if (policy.sources.requireHistoryPresence && !historySymbolSet.has(normalizeSymbol(symbol))) continue;
                addFromSource('capitalMarketsApi', symbol);
            }
        } catch (err: any) {
            const fallback =
                hasPreloadedDiscovery && params.discoveryError
                    ? params.discoveryError
                    : String(err?.message || err || 'capital_markets_api_discovery_failed');
            diagnostics.capitalMarketsApiError = String(fallback).slice(0, 220);
        }
    }

    if (policy.sources.includeCapitalTickerMap) {
        for (const symbol of Object.keys(defaultTickerEpicMap as Record<string, string>)) {
            addFromSource('capitalTickerMap', symbol);
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
            const quote = await fetchCapitalLivePrice(symbol);
            const bid = Number(quote?.bid);
            const offer = Number(quote?.offer);
            const mid = Number(quote?.price);
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
            // Keep quote metrics null if Capital query fails.
        }
    }

    const reasons: string[] = [];
    if (spanDays < params.policy.criteria.minHistoryDays) reasons.push('history_days_below_min');
    if (coveragePct < params.policy.criteria.minHistoryCoveragePct) reasons.push('history_coverage_below_min');
    if (avgBarsPerDay < params.policy.criteria.minAvgBarsPerDay) reasons.push('avg_bars_per_day_below_min');
    if (recentBars7d < params.policy.criteria.minRecentBars7d) reasons.push('recent_bars_7d_below_min');
    if (medianRangePct < params.policy.criteria.minMedianRangePct) reasons.push('median_range_pct_below_min');

    const isBerlinWeekend = isBerlinWeekendMs(params.nowMs);
    const quoteGateApplied = params.policy.criteria.requireTradableQuote && params.includeLiveQuotes && !isBerlinWeekend;
    if (quoteGateApplied) {
        if (livePrice === null || livePrice <= 0) reasons.push('live_quote_missing_or_invalid');
    }
    if (
        !isBerlinWeekend &&
        params.policy.criteria.maxSpreadPips !== null &&
        liveSpreadPips !== null &&
        liveSpreadPips > params.policy.criteria.maxSpreadPips
    ) {
        reasons.push('live_spread_above_max');
    }

    let score = 0;
    score += clamp((coveragePct / 100) * 25, 0, 25);
    score += clamp((spanDays / Math.max(params.policy.criteria.minHistoryDays, 1)) * 20, 0, 20);
    score += clamp((avgBarsPerDay / Math.max(params.policy.criteria.minAvgBarsPerDay, 1)) * 20, 0, 20);
    score += clamp((recentBars7d / Math.max(params.policy.criteria.minRecentBars7d, 1)) * 15, 0, 15);
    score += clamp((medianRangePct / Math.max(params.policy.criteria.minMedianRangePct, 0.0001)) * 10, 0, 10);
    if (liveSpreadPips !== null && params.policy.criteria.maxSpreadPips !== null) {
        const spreadScore = liveSpreadPips <= 0 ? 10 : clamp((params.policy.criteria.maxSpreadPips / liveSpreadPips) * 10, 0, 10);
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

export function buildNextUniverseWithChurnCaps(params: {
    previousSymbols: string[];
    candidateRows: ScalpSymbolCandidateRow[];
    policy: ScalpSymbolDiscoveryPolicy;
    pinnedSymbols: string[];
}): { selectedSymbols: string[]; addedSymbols: string[]; removedSymbols: string[] } {
    const previous = Array.from(new Set(params.previousSymbols.map((row) => normalizeSymbol(row)).filter((row) => Boolean(row))));
    const pinned = new Set(params.pinnedSymbols.map((row) => normalizeSymbol(row)).filter((row) => Boolean(row)));

    const eligibleSorted = params.candidateRows.filter((row) => row.eligible).slice().sort(byScoreDesc);
    const targetTop = eligibleSorted.slice(0, params.policy.limits.maxUniverseSymbols).map((row) => row.symbol);
    const targetSet = new Set(targetTop);

    if (previous.length === 0) {
        const first = Array.from(new Set([...Array.from(pinned), ...targetTop])).slice(0, params.policy.limits.maxUniverseSymbols);
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
        const scoreBySymbol = new Map(params.candidateRows.map((row) => [row.symbol, row.score]));
        const sorted = Array.from(next).sort((a, b) => {
            const aPinned = pinned.has(a) ? 1 : 0;
            const bPinned = pinned.has(b) ? 1 : 0;
            if (aPinned !== bPinned) return bPinned - aPinned;
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

    const selectedSymbols = Array.from(next).sort();
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

    const policy = await loadScalpSymbolDiscoveryPolicy();
    const previous = await loadScalpSymbolUniverseSnapshot();

    const knownStrategies = new Set(listScalpStrategies().map((row) => row.id));
    const seedConfig = resolveSeedConfig(params, policy);
    const seedShouldRun = seedConfig.enabled && (!dryRun || seedConfig.seedOnDryRun);
    const shouldLoadCapitalDiscovery = seedShouldRun || policy.sources.includeCapitalMarketsApi;
    let preloadedDiscovery: CapitalDiscoveryResult | null | undefined = undefined;
    let preloadedDiscoveryError: string | null = null;
    if (shouldLoadCapitalDiscovery) {
        const maxSymbols = Math.max(
            seedConfig.requestedTopSymbols * 6,
            policy.limits.maxCandidates * 3,
            policy.limits.maxUniverseSymbols * 4,
            200,
        );
        try {
            preloadedDiscovery = await discoverCapitalMarketSymbols({
                maxSymbols,
                // Use a broad snapshot once and reuse it in both seed + candidate stages.
                requireTradeable: false,
            });
        } catch (err: any) {
            preloadedDiscovery = null;
            preloadedDiscoveryError = String(err?.message || err || 'capital_markets_api_discovery_failed').slice(0, 220);
        }
    }
    const seedSummary = seedShouldRun
        ? await runScalpSymbolHistorySeedStage({
              policy,
              nowMs,
              dryRun,
              knownStrategyIds: knownStrategies,
              config: seedConfig,
              preloadedDiscovery,
              preloadedDiscoveryError,
          })
        : seedConfig.enabled
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
        discoveredSymbols: preloadedDiscovery,
        discoveryError: preloadedDiscoveryError,
    });
    const cappedPool = candidatePool.symbols.slice(0, Math.max(1, params.maxCandidatesOverride || policy.limits.maxCandidates));

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
    const churn = buildNextUniverseWithChurnCaps({
        previousSymbols: previous?.selectedSymbols || [],
        candidateRows: sorted,
        policy,
        pinnedSymbols: policy.pinnedSymbols,
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
        previousSymbols: previous?.selectedSymbols || [],
        selectedSymbols: churn.selectedSymbols,
        addedSymbols: churn.addedSymbols,
        removedSymbols: churn.removedSymbols,
        candidatesEvaluated: sorted.length,
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
