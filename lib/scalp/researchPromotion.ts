import {
    listScalpDeploymentRegistryEntries,
    upsertScalpDeploymentRegistryEntry,
    type ScalpDeploymentPromotionGate,
    type ScalpDeploymentRegistryEntry,
    type ScalpDeploymentRegistrySource,
    type ScalpForwardValidationMetrics,
} from './deploymentRegistry';
import { loadScalpCandleHistory } from './candleHistory';
import { pipSizeForScalpSymbol } from './marketData';
import { runScalpReplay } from './replay/harness';
import { buildScalpReplayRuntimeFromDeployment } from './replay/runtimeConfig';
import {
    listResearchCycleTasks,
    loadActiveResearchCycleId,
    loadLatestCompletedResearchCycleId,
    loadResearchCycle,
    type ScalpResearchCycleSnapshot,
    type ScalpResearchTask,
} from './researchCycle';

const DAY_MS = 24 * 60 * 60_000;
const WEEK_MS = 7 * DAY_MS;

type CandleRow = [number, number, number, number, number, number];

type WeeklySliceMetric = {
    fromTs: number;
    toTs: number;
    trades: number;
    netR: number;
    expectancyR: number;
    maxDrawdownR: number;
};

export interface ScalpWeeklyRobustnessMetrics {
    slices: number;
    profitableSlices: number;
    profitablePct: number;
    meanExpectancyR: number;
    medianExpectancyR: number;
    worstNetR: number;
    worstMaxDrawdownR: number;
    topWeekPnlConcentrationPct: number;
    totalNetR: number;
    evaluatedAtMs: number;
}

export interface SyncResearchWeeklyPolicy {
    enabled: boolean;
    topKPerSymbol: number;
    lookbackDays: number;
    minCandlesPerSlice: number;
    requireWinnerShortlist: boolean;
    minSlices: number;
    minProfitablePct: number;
    minMedianExpectancyR: number;
    maxTopWeekPnlConcentrationPct: number;
}

function keyOf(symbol: string, strategyId: string, tuneId?: string): string {
    return `${String(symbol || '').trim().toUpperCase()}::${String(strategyId || '').trim().toLowerCase()}::${String(tuneId || '').trim().toLowerCase()}`;
}

function strategyKeyOf(symbol: string, strategyId: string): string {
    return `${String(symbol || '').trim().toUpperCase()}::${String(strategyId || '').trim().toLowerCase()}`;
}

function toFinite(value: unknown, fallback = 0): number {
    const n = Number(value);
    if (!Number.isFinite(n)) return fallback;
    return n;
}

function toPositiveInt(value: unknown, fallback: number): number {
    const n = Math.floor(Number(value));
    if (!Number.isFinite(n) || n <= 0) return fallback;
    return n;
}

function toNonNegativeInt(value: unknown, fallback: number): number {
    const n = Math.floor(Number(value));
    if (!Number.isFinite(n) || n < 0) return fallback;
    return n;
}

function toBool(value: unknown, fallback: boolean): boolean {
    if (typeof value === 'boolean') return value;
    if (typeof value === 'string') {
        const normalized = value.trim().toLowerCase();
        if (['1', 'true', 'yes', 'on'].includes(normalized)) return true;
        if (['0', 'false', 'no', 'off'].includes(normalized)) return false;
    }
    return fallback;
}

function toBoundedPercent(value: unknown, fallback: number): number {
    const n = Number(value);
    if (!Number.isFinite(n)) return fallback;
    return Math.max(0, Math.min(100, n));
}

function envOrFallbackNumber(envKey: string, fallback: number): number {
    const n = Number(process.env[envKey]);
    if (!Number.isFinite(n)) return fallback;
    return n;
}

function envOrFallbackBool(envKey: string, fallback: boolean): boolean {
    const raw = process.env[envKey];
    if (raw === undefined) return fallback;
    return toBool(raw, fallback);
}

function toReplayCandles(rows: CandleRow[], spreadPips: number) {
    return rows.map((row) => ({
        ts: Number(row[0]),
        open: Number(row[1]),
        high: Number(row[2]),
        low: Number(row[3]),
        close: Number(row[4]),
        volume: Number(row[5] ?? 0),
        spreadPips,
    }));
}

function median(values: number[]): number {
    if (!values.length) return 0;
    const sorted = values.slice().sort((a, b) => a - b);
    const mid = Math.floor(sorted.length / 2);
    if (sorted.length % 2 === 1) {
        return sorted[mid] as number;
    }
    return ((sorted[mid - 1] as number) + (sorted[mid] as number)) / 2;
}

function compareCandidates(a: ScalpResearchForwardValidationCandidate, b: ScalpResearchForwardValidationCandidate): number {
    if (b.meanExpectancyR !== a.meanExpectancyR) return b.meanExpectancyR - a.meanExpectancyR;
    if (b.profitableWindowPct !== a.profitableWindowPct) return b.profitableWindowPct - a.profitableWindowPct;
    if (a.maxDrawdownR !== b.maxDrawdownR) return a.maxDrawdownR - b.maxDrawdownR;
    if (b.rollCount !== a.rollCount) return b.rollCount - a.rollCount;
    if (a.strategyId !== b.strategyId) return a.strategyId.localeCompare(b.strategyId);
    return a.tuneId.localeCompare(b.tuneId);
}

export function buildCandidateMaterializationShortlist(
    candidates: ScalpResearchForwardValidationCandidate[],
    topKPerSymbol: number,
): ScalpResearchForwardValidationCandidate[] {
    const bySymbol = new Map<string, ScalpResearchForwardValidationCandidate[]>();
    for (const candidate of candidates) {
        if (!bySymbol.has(candidate.symbol)) {
            bySymbol.set(candidate.symbol, []);
        }
        bySymbol.get(candidate.symbol)!.push(candidate);
    }

    const shortlist: ScalpResearchForwardValidationCandidate[] = [];
    const topK = Math.max(1, Math.floor(topKPerSymbol));
    for (const rows of bySymbol.values()) {
        rows.sort(compareCandidates);
        shortlist.push(...rows.slice(0, Math.min(topK, rows.length)));
    }

    return shortlist.sort(compareCandidates);
}

export function buildWinnerCandidateKeySet(
    candidates: ScalpResearchForwardValidationCandidate[],
    topKPerSymbol: number,
): Set<string> {
    const winnerKeys = new Set<string>();
    for (const row of buildCandidateMaterializationShortlist(candidates, topKPerSymbol)) {
        winnerKeys.add(keyOf(row.symbol, row.strategyId, row.tuneId));
    }
    return winnerKeys;
}

export function evaluateWeeklyRobustnessGate(
    metrics: ScalpWeeklyRobustnessMetrics | null,
    policy: SyncResearchWeeklyPolicy,
): { passed: boolean; reason: string | null } {
    if (!policy.enabled) return { passed: true, reason: null };
    if (!metrics) return { passed: false, reason: 'weekly_robustness_missing' };
    if (metrics.slices < policy.minSlices) {
        return { passed: false, reason: 'weekly_slice_count_below_threshold' };
    }
    if (metrics.profitablePct < policy.minProfitablePct) {
        return { passed: false, reason: 'weekly_profitable_pct_below_threshold' };
    }
    if (metrics.medianExpectancyR < policy.minMedianExpectancyR) {
        return { passed: false, reason: 'weekly_median_expectancy_below_threshold' };
    }
    if (metrics.topWeekPnlConcentrationPct > policy.maxTopWeekPnlConcentrationPct) {
        return { passed: false, reason: 'weekly_top_week_concentration_above_threshold' };
    }
    return { passed: true, reason: null };
}

function resolveWeeklyPolicy(
    params: SyncResearchPromotionParams,
    cycle: ScalpResearchCycleSnapshot,
): SyncResearchWeeklyPolicy {
    const enabled = params.weeklyRobustnessEnabled ?? envOrFallbackBool('SCALP_WEEKLY_ROBUSTNESS_ENABLED', true);
    const topKPerSymbol = toPositiveInt(
        params.weeklyRobustnessTopKPerSymbol ?? envOrFallbackNumber('SCALP_WEEKLY_ROBUSTNESS_TOPK_PER_SYMBOL', 2),
        2,
    );
    const lookbackDays = toPositiveInt(
        params.weeklyRobustnessLookbackDays ?? envOrFallbackNumber('SCALP_WEEKLY_ROBUSTNESS_LOOKBACK_DAYS', cycle.params.lookbackDays),
        cycle.params.lookbackDays,
    );
    const minCandlesPerSlice = toPositiveInt(
        params.weeklyRobustnessMinCandlesPerSlice ?? envOrFallbackNumber('SCALP_WEEKLY_ROBUSTNESS_MIN_CANDLES_PER_SLICE', Math.max(180, Math.floor(cycle.params.minCandlesPerTask / 2))),
        Math.max(180, Math.floor(cycle.params.minCandlesPerTask / 2)),
    );
    const requireWinnerShortlist =
        params.weeklyRobustnessRequireWinnerShortlist ??
        envOrFallbackBool('SCALP_WEEKLY_ROBUSTNESS_REQUIRE_WINNER_SHORTLIST', true);
    const minSlices = toPositiveInt(
        params.weeklyRobustnessMinSlices ?? envOrFallbackNumber('SCALP_WEEKLY_ROBUSTNESS_MIN_SLICES', 8),
        8,
    );
    const minProfitablePct = toBoundedPercent(
        params.weeklyRobustnessMinProfitablePct ?? envOrFallbackNumber('SCALP_WEEKLY_ROBUSTNESS_MIN_PROFITABLE_PCT', 45),
        45,
    );
    const minMedianExpectancyR = toFinite(
        params.weeklyRobustnessMinMedianExpectancyR ?? envOrFallbackNumber('SCALP_WEEKLY_ROBUSTNESS_MIN_MEDIAN_EXPECTANCY_R', 0),
        0,
    );
    const maxTopWeekPnlConcentrationPct = toBoundedPercent(
        params.weeklyRobustnessMaxTopWeekPnlConcentrationPct ??
            envOrFallbackNumber('SCALP_WEEKLY_ROBUSTNESS_MAX_TOP_WEEK_PNL_CONCENTRATION_PCT', 80),
        80,
    );

    return {
        enabled,
        topKPerSymbol,
        lookbackDays,
        minCandlesPerSlice,
        requireWinnerShortlist,
        minSlices,
        minProfitablePct,
        minMedianExpectancyR,
        maxTopWeekPnlConcentrationPct,
    };
}

function buildReplayRuntimeForDeployment(entry: ScalpDeploymentRegistryEntry) {
    return buildScalpReplayRuntimeFromDeployment({
        deployment: entry,
        configOverride: entry.configOverride,
    });
}

async function runWeeklyRobustnessForDeployment(params: {
    deployment: ScalpDeploymentRegistryEntry;
    candles: CandleRow[];
    nowMs: number;
    lookbackDays: number;
    minCandlesPerSlice: number;
}): Promise<ScalpWeeklyRobustnessMetrics | null> {
    const fromTs = params.nowMs - Math.max(1, Math.floor(params.lookbackDays)) * DAY_MS;
    const windowRows = params.candles.filter((row) => row[0] >= fromTs && row[0] < params.nowMs);
    if (windowRows.length < params.minCandlesPerSlice) return null;

    const runtime = buildReplayRuntimeForDeployment(params.deployment);
    const slices: WeeklySliceMetric[] = [];

    for (let sliceFrom = fromTs; sliceFrom < params.nowMs; sliceFrom += WEEK_MS) {
        const sliceTo = Math.min(params.nowMs, sliceFrom + WEEK_MS);
        const rows = windowRows.filter((row) => row[0] >= sliceFrom && row[0] < sliceTo);
        if (rows.length < params.minCandlesPerSlice) continue;

        const replay = await runScalpReplay({
            candles: toReplayCandles(rows, runtime.defaultSpreadPips),
            pipSize: pipSizeForScalpSymbol(params.deployment.symbol),
            config: runtime,
            captureTimeline: false,
        });
        slices.push({
            fromTs: sliceFrom,
            toTs: sliceTo,
            trades: replay.summary.trades,
            netR: replay.summary.netR,
            expectancyR: replay.summary.expectancyR,
            maxDrawdownR: replay.summary.maxDrawdownR,
        });
    }

    if (!slices.length) return null;

    const profitableSlices = slices.filter((row) => row.netR > 0).length;
    const profitablePct = (profitableSlices / slices.length) * 100;
    const expectancyRows = slices.map((row) => row.expectancyR);
    const meanExpectancyR = expectancyRows.reduce((acc, row) => acc + row, 0) / slices.length;
    const medianExpectancyR = median(expectancyRows);
    const worstNetR = slices.reduce((acc, row) => Math.min(acc, row.netR), Number.POSITIVE_INFINITY);
    const worstMaxDrawdownR = slices.reduce((acc, row) => Math.max(acc, row.maxDrawdownR), 0);
    const totalNetR = slices.reduce((acc, row) => acc + row.netR, 0);

    const positiveNet = slices.map((row) => Math.max(0, row.netR));
    const totalPositive = positiveNet.reduce((acc, row) => acc + row, 0);
    const topPositive = positiveNet.length ? Math.max(...positiveNet) : 0;
    const topWeekPnlConcentrationPct = totalPositive > 0 ? (topPositive / totalPositive) * 100 : 100;

    return {
        slices: slices.length,
        profitableSlices,
        profitablePct,
        meanExpectancyR,
        medianExpectancyR,
        worstNetR: Number.isFinite(worstNetR) ? worstNetR : 0,
        worstMaxDrawdownR,
        topWeekPnlConcentrationPct,
        totalNetR,
        evaluatedAtMs: params.nowMs,
    };
}

export interface ScalpResearchForwardValidationCandidate {
    symbol: string;
    strategyId: string;
    tuneId: string;
    deploymentId: string;
    rollCount: number;
    profitableWindowPct: number;
    profitableWindows: number;
    meanExpectancyR: number;
    meanProfitFactor: number | null;
    maxDrawdownR: number;
    minTradesPerWindow: number | null;
    totalTrades: number;
    selectionWindowDays: number;
    forwardWindowDays: number;
    forwardValidation: ScalpForwardValidationMetrics;
}

export function buildForwardValidationByCandidate(
    cycle: ScalpResearchCycleSnapshot,
    tasks: ScalpResearchTask[],
): ScalpResearchForwardValidationCandidate[] {
    const byCandidate = new Map<
        string,
        {
            symbol: string;
            strategyId: string;
            tuneId: string;
            deploymentId: string;
            rollCount: number;
            profitableWindows: number;
            expectancySum: number;
            profitFactorSum: number;
            profitFactorCount: number;
            maxDrawdownR: number;
            minTradesPerWindow: number | null;
            totalTrades: number;
        }
    >();

    for (const task of tasks) {
        if (task.status !== 'completed' || !task.result) continue;
        const symbol = String(task.symbol || '').trim().toUpperCase();
        const strategyId = String(task.strategyId || '').trim().toLowerCase();
        const tuneId = String(task.tuneId || task.result.tuneId || '').trim().toLowerCase();
        const deploymentId = String(task.deploymentId || task.result.deploymentId || '').trim();
        if (!symbol || !strategyId || !tuneId || !deploymentId) continue;

        const key = keyOf(symbol, strategyId, tuneId);
        if (!byCandidate.has(key)) {
            byCandidate.set(key, {
                symbol,
                strategyId,
                tuneId,
                deploymentId,
                rollCount: 0,
                profitableWindows: 0,
                expectancySum: 0,
                profitFactorSum: 0,
                profitFactorCount: 0,
                maxDrawdownR: 0,
                minTradesPerWindow: null,
                totalTrades: 0,
            });
        }

        const row = byCandidate.get(key)!;
        const trades = Math.max(0, Math.floor(toFinite(task.result.trades, 0)));
        const expectancyR = toFinite(task.result.expectancyR, 0);
        const netR = toFinite(task.result.netR, 0);
        const maxDrawdownR = Math.max(0, toFinite(task.result.maxDrawdownR, 0));
        const rawProfitFactor = task.result.profitFactor;
        const profitFactor =
            rawProfitFactor === null || rawProfitFactor === undefined ? Number.NaN : Number(rawProfitFactor);

        row.rollCount += 1;
        row.totalTrades += trades;
        row.expectancySum += expectancyR;
        row.maxDrawdownR = Math.max(row.maxDrawdownR, maxDrawdownR);
        row.minTradesPerWindow = row.minTradesPerWindow === null ? trades : Math.min(row.minTradesPerWindow, trades);
        if (netR > 0) {
            row.profitableWindows += 1;
        }
        if (Number.isFinite(profitFactor) && profitFactor >= 0) {
            row.profitFactorSum += profitFactor;
            row.profitFactorCount += 1;
        }
    }

    return Array.from(byCandidate.values())
        .filter((row) => row.rollCount > 0)
        .map((row) => {
            const profitableWindowPct = (row.profitableWindows / row.rollCount) * 100;
            const meanExpectancyR = row.expectancySum / row.rollCount;
            const meanProfitFactor = row.profitFactorCount > 0 ? row.profitFactorSum / row.profitFactorCount : null;
            const forwardValidation: ScalpForwardValidationMetrics = {
                rollCount: row.rollCount,
                profitableWindowPct,
                meanExpectancyR,
                meanProfitFactor,
                maxDrawdownR: row.maxDrawdownR,
                minTradesPerWindow: row.minTradesPerWindow,
                selectionWindowDays: cycle.params.lookbackDays,
                forwardWindowDays: cycle.params.chunkDays,
                weeklySlices: null,
                weeklyProfitablePct: null,
                weeklyMeanExpectancyR: null,
                weeklyMedianExpectancyR: null,
                weeklyWorstNetR: null,
                weeklyTopWeekPnlConcentrationPct: null,
                weeklyEvaluatedAtMs: null,
            };
            return {
                symbol: row.symbol,
                strategyId: row.strategyId,
                tuneId: row.tuneId,
                deploymentId: row.deploymentId,
                rollCount: row.rollCount,
                profitableWindowPct,
                profitableWindows: row.profitableWindows,
                meanExpectancyR,
                meanProfitFactor,
                maxDrawdownR: row.maxDrawdownR,
                minTradesPerWindow: row.minTradesPerWindow,
                totalTrades: row.totalTrades,
                selectionWindowDays: cycle.params.lookbackDays,
                forwardWindowDays: cycle.params.chunkDays,
                forwardValidation,
            };
        })
        .sort(compareCandidates);
}

export interface SyncResearchPromotionParams {
    cycleId?: string;
    dryRun?: boolean;
    requireCompletedCycle?: boolean;
    sources?: ScalpDeploymentRegistrySource[];
    updatedBy?: string;
    nowMs?: number;
    weeklyRobustnessEnabled?: boolean;
    weeklyRobustnessTopKPerSymbol?: number;
    weeklyRobustnessLookbackDays?: number;
    weeklyRobustnessMinCandlesPerSlice?: number;
    weeklyRobustnessRequireWinnerShortlist?: boolean;
    weeklyRobustnessMinSlices?: number;
    weeklyRobustnessMinProfitablePct?: number;
    weeklyRobustnessMinMedianExpectancyR?: number;
    weeklyRobustnessMaxTopWeekPnlConcentrationPct?: number;
    materializeMissingCandidates?: boolean;
    materializeTopKPerSymbol?: number;
    materializeSource?: 'matrix' | 'backtest';
    materializeEnabled?: boolean;
}

export interface SyncResearchPromotionRow {
    deploymentId: string;
    symbol: string;
    strategyId: string;
    tuneId: string;
    source: ScalpDeploymentRegistrySource;
    inWinnerShortlist: boolean;
    weeklyRobustness: ScalpWeeklyRobustnessMetrics | null;
    weeklyGateReason: string | null;
    matchedCandidate: Pick<
        ScalpResearchForwardValidationCandidate,
        'rollCount' | 'profitableWindowPct' | 'meanExpectancyR' | 'meanProfitFactor' | 'maxDrawdownR' | 'minTradesPerWindow'
    > | null;
    previousGate: ScalpDeploymentPromotionGate | null;
    nextGate: ScalpDeploymentPromotionGate | null;
    changed: boolean;
}

export interface SyncResearchPromotionResult {
    ok: boolean;
    cycleId: string | null;
    cycleStatus: string | null;
    dryRun: boolean;
    requireCompletedCycle: boolean;
    reason: string | null;
    weeklyPolicy: SyncResearchWeeklyPolicy;
    candidates: ScalpResearchForwardValidationCandidate[];
    materialization: {
        enabled: boolean;
        source: 'matrix' | 'backtest';
        topKPerSymbol: number;
        shortlistedCandidates: number;
        missingCandidates: number;
        createdCandidates: number;
        rows: Array<{
            deploymentId: string;
            symbol: string;
            strategyId: string;
            tuneId: string;
            source: 'matrix' | 'backtest';
            exists: boolean;
            created: boolean;
        }>;
    };
    deploymentsConsidered: number;
    deploymentsMatched: number;
    deploymentsUpdated: number;
    rows: SyncResearchPromotionRow[];
}

function resolveMaterializeSource(
    value: unknown,
    fallback: 'matrix' | 'backtest',
): 'matrix' | 'backtest' {
    const normalized = String(value || '')
        .trim()
        .toLowerCase();
    if (normalized === 'matrix' || normalized === 'backtest') return normalized;
    return fallback;
}

function changedPromotionGate(
    a: ScalpDeploymentPromotionGate | null | undefined,
    b: ScalpDeploymentPromotionGate | null | undefined,
): boolean {
    return JSON.stringify(a || null) !== JSON.stringify(b || null);
}

function buildForcedIneligibleGate(params: {
    baseGate: ScalpDeploymentPromotionGate | null;
    reason: string;
    forwardValidation: ScalpForwardValidationMetrics;
    nowMs: number;
}): ScalpDeploymentPromotionGate {
    return {
        eligible: false,
        reason: params.reason,
        source: 'walk_forward',
        evaluatedAtMs: params.nowMs,
        forwardValidation: params.baseGate?.forwardValidation || params.forwardValidation,
        thresholds: params.baseGate?.thresholds || null,
    };
}

export async function syncResearchCyclePromotionGates(
    params: SyncResearchPromotionParams = {},
): Promise<SyncResearchPromotionResult> {
    const requestedCycleId = String(params.cycleId || '').trim();
    let cycleId = requestedCycleId || (await loadActiveResearchCycleId());
    if (!cycleId) {
        cycleId = await loadLatestCompletedResearchCycleId();
    }
    const dryRun = Boolean(params.dryRun);
    const requireCompletedCycle = params.requireCompletedCycle ?? true;
    const allowedSources = new Set<ScalpDeploymentRegistrySource>(
        (params.sources && params.sources.length ? params.sources : ['matrix', 'backtest'])
            .map((row) => String(row || '').trim().toLowerCase())
            .filter((row): row is ScalpDeploymentRegistrySource => row === 'manual' || row === 'backtest' || row === 'matrix'),
    );
    const defaultMaterializeSource: 'matrix' | 'backtest' = allowedSources.has('matrix')
        ? 'matrix'
        : allowedSources.has('backtest')
          ? 'backtest'
          : 'matrix';
    const materializeSource = resolveMaterializeSource(params.materializeSource, defaultMaterializeSource);
    const materializeMissingCandidates =
        params.materializeMissingCandidates ??
        envOrFallbackBool('SCALP_RESEARCH_MATERIALIZE_MISSING_CANDIDATES', true);
    const materializeEnabled =
        params.materializeEnabled ??
        envOrFallbackBool('SCALP_RESEARCH_MATERIALIZE_ENABLED', true);
    const materializeTopKPerSymbol = toPositiveInt(
        params.materializeTopKPerSymbol ??
            envOrFallbackNumber('SCALP_RESEARCH_MATERIALIZE_TOPK_PER_SYMBOL', 2),
        2,
    );
    const emptyMaterialization = {
        enabled: materializeMissingCandidates,
        source: materializeSource,
        topKPerSymbol: materializeTopKPerSymbol,
        shortlistedCandidates: 0,
        missingCandidates: 0,
        createdCandidates: 0,
        rows: [] as Array<{
            deploymentId: string;
            symbol: string;
            strategyId: string;
            tuneId: string;
            source: 'matrix' | 'backtest';
            exists: boolean;
            created: boolean;
        }>,
    };

    const fallbackPolicy: SyncResearchWeeklyPolicy = {
        enabled: true,
        topKPerSymbol: 2,
        lookbackDays: 90,
        minCandlesPerSlice: 180,
        requireWinnerShortlist: true,
        minSlices: 8,
        minProfitablePct: 45,
        minMedianExpectancyR: 0,
        maxTopWeekPnlConcentrationPct: 80,
    };

    if (!cycleId) {
        return {
            ok: false,
            cycleId: null,
            cycleStatus: null,
            dryRun,
            requireCompletedCycle,
            reason: 'cycle_not_found',
            weeklyPolicy: fallbackPolicy,
            candidates: [],
            materialization: emptyMaterialization,
            deploymentsConsidered: 0,
            deploymentsMatched: 0,
            deploymentsUpdated: 0,
            rows: [],
        };
    }

    const cycle = await loadResearchCycle(cycleId);
    if (!cycle) {
        return {
            ok: false,
            cycleId,
            cycleStatus: null,
            dryRun,
            requireCompletedCycle,
            reason: 'cycle_not_found',
            weeklyPolicy: fallbackPolicy,
            candidates: [],
            materialization: emptyMaterialization,
            deploymentsConsidered: 0,
            deploymentsMatched: 0,
            deploymentsUpdated: 0,
            rows: [],
        };
    }

    if (requireCompletedCycle && cycle.status !== 'completed') {
        return {
            ok: false,
            cycleId,
            cycleStatus: cycle.status,
            dryRun,
            requireCompletedCycle,
            reason: 'cycle_not_completed',
            weeklyPolicy: resolveWeeklyPolicy(params, cycle),
            candidates: [],
            materialization: emptyMaterialization,
            deploymentsConsidered: 0,
            deploymentsMatched: 0,
            deploymentsUpdated: 0,
            rows: [],
        };
    }

    const nowMs = Number.isFinite(Number(params.nowMs)) ? Math.floor(Number(params.nowMs)) : Date.now();
    const weeklyPolicy = resolveWeeklyPolicy(params, cycle);

    const tasks = await listResearchCycleTasks(cycleId, 10000);
    const candidates = buildForwardValidationByCandidate(cycle, tasks);
    const candidateByKey = new Map(candidates.map((row) => [keyOf(row.symbol, row.strategyId, row.tuneId), row]));
    const candidatesBySymbolStrategy = new Map<string, ScalpResearchForwardValidationCandidate[]>();
    for (const row of candidates) {
        const strategyKey = strategyKeyOf(row.symbol, row.strategyId);
        if (!candidatesBySymbolStrategy.has(strategyKey)) {
            candidatesBySymbolStrategy.set(strategyKey, []);
        }
        candidatesBySymbolStrategy.get(strategyKey)!.push(row);
    }
    for (const rows of candidatesBySymbolStrategy.values()) {
        rows.sort(compareCandidates);
    }
    const winnerCandidateKeys = buildWinnerCandidateKeySet(candidates, weeklyPolicy.topKPerSymbol);
    const materializationShortlist = buildCandidateMaterializationShortlist(candidates, materializeTopKPerSymbol);

    let deployments = await listScalpDeploymentRegistryEntries({});
    const deploymentIds = new Set(deployments.map((row) => row.deploymentId));
    const materializationRows: Array<{
        deploymentId: string;
        symbol: string;
        strategyId: string;
        tuneId: string;
        source: 'matrix' | 'backtest';
        exists: boolean;
        created: boolean;
    }> = [];
    let materializationMissing = 0;
    let materializationCreated = 0;

    for (const candidate of materializationShortlist) {
        const exists = deploymentIds.has(candidate.deploymentId);
        let created = false;
        if (!exists) {
            materializationMissing += 1;
            if (materializeMissingCandidates && !dryRun) {
                const upserted = await upsertScalpDeploymentRegistryEntry({
                    deploymentId: candidate.deploymentId,
                    symbol: candidate.symbol,
                    strategyId: candidate.strategyId,
                    tuneId: candidate.tuneId,
                    source: materializeSource,
                    enabled: materializeEnabled,
                    forwardValidation: candidate.forwardValidation,
                    notes: `auto_materialized_from_cycle:${cycleId}`,
                    updatedBy: params.updatedBy || 'research-cycle-sync',
                });
                deployments = deployments
                    .filter((row) => row.deploymentId !== upserted.entry.deploymentId)
                    .concat(upserted.entry);
                deploymentIds.add(upserted.entry.deploymentId);
                created = true;
                materializationCreated += 1;
            }
        }
        materializationRows.push({
            deploymentId: candidate.deploymentId,
            symbol: candidate.symbol,
            strategyId: candidate.strategyId,
            tuneId: candidate.tuneId,
            source: materializeSource,
            exists,
            created,
        });
    }
    const materialization = {
        enabled: materializeMissingCandidates,
        source: materializeSource,
        topKPerSymbol: materializeTopKPerSymbol,
        shortlistedCandidates: materializationShortlist.length,
        missingCandidates: materializationMissing,
        createdCandidates: materializationCreated,
        rows: materializationRows,
    };

    const considered = deployments.filter((row) => allowedSources.has(row.source));

    const rows: SyncResearchPromotionRow[] = [];
    let deploymentsMatched = 0;
    let deploymentsUpdated = 0;

    const candlesBySymbol = new Map<string, CandleRow[]>();

    for (const deployment of considered) {
        const candidateKey = keyOf(deployment.symbol, deployment.strategyId, deployment.tuneId);
        const candidate =
            candidateByKey.get(candidateKey) ||
            candidatesBySymbolStrategy.get(strategyKeyOf(deployment.symbol, deployment.strategyId))?.[0] ||
            null;
        if (!candidate) {
            const weeklyGateReason = 'missing_cycle_candidate';
            if (dryRun) {
                rows.push({
                    deploymentId: deployment.deploymentId,
                    symbol: deployment.symbol,
                    strategyId: deployment.strategyId,
                    tuneId: deployment.tuneId,
                    source: deployment.source,
                    inWinnerShortlist: false,
                    weeklyRobustness: null,
                    weeklyGateReason,
                    matchedCandidate: null,
                    previousGate: deployment.promotionGate,
                    nextGate: null,
                    changed: false,
                });
                continue;
            }

            const forcedGate = buildForcedIneligibleGate({
                baseGate: deployment.promotionGate,
                reason: weeklyGateReason,
                forwardValidation: deployment.promotionGate?.forwardValidation || {
                    rollCount: 0,
                    profitableWindowPct: 0,
                    meanExpectancyR: 0,
                    meanProfitFactor: null,
                    maxDrawdownR: null,
                    minTradesPerWindow: null,
                    selectionWindowDays: null,
                    forwardWindowDays: null,
                },
                nowMs,
            });
            const updated = await upsertScalpDeploymentRegistryEntry({
                deploymentId: deployment.deploymentId,
                source: deployment.source,
                enabled: deployment.enabled,
                promotionGate: forcedGate,
                updatedBy: params.updatedBy || 'research-cycle-sync',
            });
            const changed = changedPromotionGate(deployment.promotionGate, updated.entry.promotionGate);
            if (changed) deploymentsUpdated += 1;

            rows.push({
                deploymentId: deployment.deploymentId,
                symbol: deployment.symbol,
                strategyId: deployment.strategyId,
                tuneId: deployment.tuneId,
                source: deployment.source,
                inWinnerShortlist: false,
                weeklyRobustness: null,
                weeklyGateReason,
                matchedCandidate: null,
                previousGate: deployment.promotionGate,
                nextGate: updated.entry.promotionGate,
                changed,
            });
            continue;
        }
        deploymentsMatched += 1;

        const matchedCandidate = {
            rollCount: candidate.rollCount,
            profitableWindowPct: candidate.profitableWindowPct,
            meanExpectancyR: candidate.meanExpectancyR,
            meanProfitFactor: candidate.meanProfitFactor,
            maxDrawdownR: candidate.maxDrawdownR,
            minTradesPerWindow: candidate.minTradesPerWindow,
        };

        const inWinnerShortlist = winnerCandidateKeys.has(keyOf(candidate.symbol, candidate.strategyId, candidate.tuneId));
        let weeklyRobustness: ScalpWeeklyRobustnessMetrics | null = null;
        let weeklyGateReason: string | null = null;

        if (weeklyPolicy.enabled) {
            if (!inWinnerShortlist && weeklyPolicy.requireWinnerShortlist) {
                weeklyGateReason = 'not_in_90d_winner_shortlist';
            } else if (inWinnerShortlist) {
                if (!candlesBySymbol.has(deployment.symbol)) {
                    const history = await loadScalpCandleHistory(deployment.symbol, '1m');
                    candlesBySymbol.set(deployment.symbol, (history.record?.candles || []) as CandleRow[]);
                }
                const symbolCandles = candlesBySymbol.get(deployment.symbol) || [];
                weeklyRobustness = await runWeeklyRobustnessForDeployment({
                    deployment,
                    candles: symbolCandles,
                    nowMs,
                    lookbackDays: weeklyPolicy.lookbackDays,
                    minCandlesPerSlice: weeklyPolicy.minCandlesPerSlice,
                });
                const weeklyGate = evaluateWeeklyRobustnessGate(weeklyRobustness, weeklyPolicy);
                if (!weeklyGate.passed) {
                    weeklyGateReason = weeklyGate.reason || 'weekly_robustness_failed';
                }
            }
        }

        const forwardValidation: ScalpForwardValidationMetrics = {
            ...candidate.forwardValidation,
            weeklySlices: weeklyRobustness?.slices ?? null,
            weeklyProfitablePct: weeklyRobustness?.profitablePct ?? null,
            weeklyMeanExpectancyR: weeklyRobustness?.meanExpectancyR ?? null,
            weeklyMedianExpectancyR: weeklyRobustness?.medianExpectancyR ?? null,
            weeklyWorstNetR: weeklyRobustness?.worstNetR ?? null,
            weeklyTopWeekPnlConcentrationPct: weeklyRobustness?.topWeekPnlConcentrationPct ?? null,
            weeklyEvaluatedAtMs: weeklyRobustness?.evaluatedAtMs ?? null,
        };

        if (dryRun) {
            rows.push({
                deploymentId: deployment.deploymentId,
                symbol: deployment.symbol,
                strategyId: deployment.strategyId,
                tuneId: deployment.tuneId,
                source: deployment.source,
                inWinnerShortlist,
                weeklyRobustness,
                weeklyGateReason,
                matchedCandidate,
                previousGate: deployment.promotionGate,
                nextGate: null,
                changed: false,
            });
            continue;
        }

        const baseline = await upsertScalpDeploymentRegistryEntry({
            deploymentId: deployment.deploymentId,
            source: deployment.source,
            enabled: deployment.enabled,
            forwardValidation,
            updatedBy: params.updatedBy || 'research-cycle-sync',
        });

        let nextGate = baseline.entry.promotionGate;
        if (weeklyGateReason) {
            const forcedGate = buildForcedIneligibleGate({
                baseGate: baseline.entry.promotionGate,
                reason: weeklyGateReason,
                forwardValidation,
                nowMs,
            });
            const forced = await upsertScalpDeploymentRegistryEntry({
                deploymentId: deployment.deploymentId,
                source: deployment.source,
                enabled: deployment.enabled,
                promotionGate: forcedGate,
                updatedBy: params.updatedBy || 'research-cycle-sync',
            });
            nextGate = forced.entry.promotionGate;
        }

        const changed = changedPromotionGate(deployment.promotionGate, nextGate);
        if (changed) deploymentsUpdated += 1;

        rows.push({
            deploymentId: deployment.deploymentId,
            symbol: deployment.symbol,
            strategyId: deployment.strategyId,
            tuneId: deployment.tuneId,
            source: deployment.source,
            inWinnerShortlist,
            weeklyRobustness,
            weeklyGateReason,
            matchedCandidate,
            previousGate: deployment.promotionGate,
            nextGate,
            changed,
        });
    }

    return {
        ok: true,
        cycleId,
        cycleStatus: cycle.status,
        dryRun,
        requireCompletedCycle,
        reason: null,
        weeklyPolicy,
        candidates,
        materialization,
        deploymentsConsidered: considered.length,
        deploymentsMatched,
        deploymentsUpdated,
        rows,
    };
}

export function inferCyclePromotionSummaryRows(deployments: ScalpDeploymentRegistryEntry[]): Array<{
    deploymentId: string;
    symbol: string;
    strategyId: string;
    tuneId: string;
    source: ScalpDeploymentRegistrySource;
    enabled: boolean;
    promotionEligible: boolean;
    promotionReason: string | null;
}> {
    return deployments.map((row) => ({
        deploymentId: row.deploymentId,
        symbol: row.symbol,
        strategyId: row.strategyId,
        tuneId: row.tuneId,
        source: row.source,
        enabled: row.enabled,
        promotionEligible: Boolean(row.promotionGate?.eligible),
        promotionReason: row.promotionGate?.reason || null,
    }));
}
