import {
    listScalpDeploymentRegistryEntries,
    upsertScalpDeploymentRegistryEntry,
    type ScalpDeploymentPromotionGate,
    type ScalpDeploymentRegistryEntry,
    type ScalpDeploymentRegistrySource,
    type ScalpForwardValidationMetrics,
} from './deploymentRegistry';
import {
    listResearchCycleTasks,
    loadActiveResearchCycleId,
    loadLatestCompletedResearchCycleId,
    loadResearchCycle,
    type ScalpResearchCycleSnapshot,
    type ScalpResearchTask,
} from './researchCycle';

function keyOf(symbol: string, strategyId: string): string {
    return `${String(symbol || '').trim().toUpperCase()}::${String(strategyId || '').trim().toLowerCase()}`;
}

function toFinite(value: unknown, fallback = 0): number {
    const n = Number(value);
    if (!Number.isFinite(n)) return fallback;
    return n;
}

export interface ScalpResearchForwardValidationCandidate {
    symbol: string;
    strategyId: string;
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
        if (!symbol || !strategyId) continue;

        const key = keyOf(symbol, strategyId);
        if (!byCandidate.has(key)) {
            byCandidate.set(key, {
                symbol,
                strategyId,
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
            };
            return {
                symbol: row.symbol,
                strategyId: row.strategyId,
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
        .sort((a, b) => {
            if (b.meanExpectancyR !== a.meanExpectancyR) return b.meanExpectancyR - a.meanExpectancyR;
            if (b.profitableWindowPct !== a.profitableWindowPct) return b.profitableWindowPct - a.profitableWindowPct;
            if (a.maxDrawdownR !== b.maxDrawdownR) return a.maxDrawdownR - b.maxDrawdownR;
            if (b.rollCount !== a.rollCount) return b.rollCount - a.rollCount;
            if (a.symbol !== b.symbol) return a.symbol.localeCompare(b.symbol);
            return a.strategyId.localeCompare(b.strategyId);
        });
}

export interface SyncResearchPromotionParams {
    cycleId?: string;
    dryRun?: boolean;
    requireCompletedCycle?: boolean;
    sources?: ScalpDeploymentRegistrySource[];
    updatedBy?: string;
}

export interface SyncResearchPromotionRow {
    deploymentId: string;
    symbol: string;
    strategyId: string;
    tuneId: string;
    source: ScalpDeploymentRegistrySource;
    matchedCandidate: Pick<
        ScalpResearchForwardValidationCandidate,
        'rollCount' | 'profitableWindowPct' | 'meanExpectancyR' | 'meanProfitFactor' | 'maxDrawdownR' | 'minTradesPerWindow'
    >;
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
    candidates: ScalpResearchForwardValidationCandidate[];
    deploymentsConsidered: number;
    deploymentsMatched: number;
    deploymentsUpdated: number;
    rows: SyncResearchPromotionRow[];
}

function changedPromotionGate(
    a: ScalpDeploymentPromotionGate | null | undefined,
    b: ScalpDeploymentPromotionGate | null | undefined,
): boolean {
    return JSON.stringify(a || null) !== JSON.stringify(b || null);
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

    if (!cycleId) {
        return {
            ok: false,
            cycleId: null,
            cycleStatus: null,
            dryRun,
            requireCompletedCycle,
            reason: 'cycle_not_found',
            candidates: [],
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
            candidates: [],
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
            candidates: [],
            deploymentsConsidered: 0,
            deploymentsMatched: 0,
            deploymentsUpdated: 0,
            rows: [],
        };
    }

    const tasks = await listResearchCycleTasks(cycleId, 10000);
    const candidates = buildForwardValidationByCandidate(cycle, tasks);
    const candidateByKey = new Map(candidates.map((row) => [keyOf(row.symbol, row.strategyId), row]));

    const deployments = await listScalpDeploymentRegistryEntries({});
    const considered = deployments.filter((row) => allowedSources.has(row.source));

    const rows: SyncResearchPromotionRow[] = [];
    let deploymentsMatched = 0;
    let deploymentsUpdated = 0;

    for (const deployment of considered) {
        const candidate = candidateByKey.get(keyOf(deployment.symbol, deployment.strategyId));
        if (!candidate) continue;
        deploymentsMatched += 1;

        const matchedCandidate = {
            rollCount: candidate.rollCount,
            profitableWindowPct: candidate.profitableWindowPct,
            meanExpectancyR: candidate.meanExpectancyR,
            meanProfitFactor: candidate.meanProfitFactor,
            maxDrawdownR: candidate.maxDrawdownR,
            minTradesPerWindow: candidate.minTradesPerWindow,
        };

        if (dryRun) {
            rows.push({
                deploymentId: deployment.deploymentId,
                symbol: deployment.symbol,
                strategyId: deployment.strategyId,
                tuneId: deployment.tuneId,
                source: deployment.source,
                matchedCandidate,
                previousGate: deployment.promotionGate,
                nextGate: null,
                changed: false,
            });
            continue;
        }

        const updated = await upsertScalpDeploymentRegistryEntry({
            deploymentId: deployment.deploymentId,
            source: deployment.source,
            enabled: deployment.enabled,
            forwardValidation: candidate.forwardValidation,
            updatedBy: params.updatedBy || 'research-cycle-sync',
        });
        const changed = changedPromotionGate(deployment.promotionGate, updated.entry.promotionGate);
        if (changed) deploymentsUpdated += 1;

        rows.push({
            deploymentId: updated.entry.deploymentId,
            symbol: updated.entry.symbol,
            strategyId: updated.entry.strategyId,
            tuneId: updated.entry.tuneId,
            source: updated.entry.source,
            matchedCandidate,
            previousGate: deployment.promotionGate,
            nextGate: updated.entry.promotionGate,
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
        candidates,
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
