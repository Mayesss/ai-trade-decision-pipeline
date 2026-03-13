import type { ScalpStrategyConfigOverride } from './config';
import { resolveScalpDeployment } from './deployments';
import { buildScalpReplayRuntimeFromDeployment } from './replay/runtimeConfig';
import { buildScalpResearchTuneVariants } from './researchTuner';
import { normalizeScalpEntrySessionProfile, scalpEntrySessionProfileDistance } from './sessions';
import { listScalpStrategies } from './strategies/registry';
import { resolveRecommendedStrategiesForSymbol } from './symbolDiscovery';
import type { ScalpEntrySessionProfile } from './types';
import type { ScalpResearchCandidateAggregate, ScalpResearchCycleSummary } from './researchCycle';

export type ScalpResearchPlanTier = 'champion' | 'neighbor' | 'challenger' | 'incubator' | 'fallback';

export interface ScalpResearchPlanRow {
    symbol: string;
    strategyId: string;
    tuneId: string;
    configOverride: ScalpStrategyConfigOverride | null;
    tier: ScalpResearchPlanTier;
}

export interface ScalpResearchPlannerPolicy {
    enabled: boolean;
    championCandidatesPerSymbol: number;
    neighborVariantsPerCandidate: number;
    challengerStrategiesPerSymbol: number;
    challengerTunesPerStrategy: number;
    incubatorSymbolsPerCycle: number;
    incubatorStrategiesPerSymbol: number;
    incubatorTunesPerStrategy: number;
    fallbackStrategiesPerSymbol: number;
    fallbackTunesPerStrategy: number;
    minChampionTrades: number;
}

export interface BuildScalpResearchPlanParams {
    symbols: string[];
    strategyAllowlist: string[];
    tunerEnabled: boolean;
    maxTuneVariantsPerStrategy: number;
    previousSummary?: ScalpResearchCycleSummary | null;
    policy?: Partial<ScalpResearchPlannerPolicy>;
}

const DEFAULT_CHAMPION_CANDIDATES_PER_SYMBOL = 1;
const DEFAULT_NEIGHBOR_VARIANTS_PER_CANDIDATE = 2;
const DEFAULT_CHALLENGER_STRATEGIES_PER_SYMBOL = 1;
const DEFAULT_CHALLENGER_TUNES_PER_STRATEGY = 1;
const DEFAULT_INCUBATOR_SYMBOLS_PER_CYCLE = 5;
const DEFAULT_INCUBATOR_STRATEGIES_PER_SYMBOL = 2;
const DEFAULT_INCUBATOR_TUNES_PER_STRATEGY = 2;
const DEFAULT_FALLBACK_STRATEGIES_PER_SYMBOL = 1;
const DEFAULT_FALLBACK_TUNES_PER_STRATEGY = 2;
const DEFAULT_MIN_CHAMPION_TRADES = 2;

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

function normalizeSymbol(value: unknown): string {
    return String(value || '')
        .trim()
        .toUpperCase()
        .replace(/[^A-Z0-9._-]/g, '');
}

function normalizeHours(value: unknown): number[] {
    if (!Array.isArray(value)) return [];
    return Array.from(
        new Set(
            value
                .map((row) => Math.floor(Number(row)))
                .filter((row) => Number.isFinite(row) && row >= 0 && row <= 23),
        ),
    ).sort((a, b) => a - b);
}

function blockedHoursDistance(a: number[], b: number[]): number {
    const aa = new Set(normalizeHours(a));
    const bb = new Set(normalizeHours(b));
    const union = new Set([...aa, ...bb]);
    let diff = 0;
    for (const hour of union) {
        if (aa.has(hour) !== bb.has(hour)) diff += 1;
    }
    return diff;
}

function comboKey(row: Pick<ScalpResearchPlanRow, 'symbol' | 'strategyId' | 'tuneId'>): string {
    return `${normalizeSymbol(row.symbol)}::${String(row.strategyId || '').trim().toLowerCase()}::${String(row.tuneId || '')
        .trim()
        .toLowerCase()}`;
}

function compareCandidateAggregates(a: ScalpResearchCandidateAggregate, b: ScalpResearchCandidateAggregate): number {
    const aHasCompleted = a.completedTasks > 0 ? 1 : 0;
    const bHasCompleted = b.completedTasks > 0 ? 1 : 0;
    if (bHasCompleted !== aHasCompleted) return bHasCompleted - aHasCompleted;
    if (b.expectancyR !== a.expectancyR) return b.expectancyR - a.expectancyR;
    if (b.netR !== a.netR) return b.netR - a.netR;
    const pfA = a.profitFactor ?? -1;
    const pfB = b.profitFactor ?? -1;
    if (pfB !== pfA) return pfB - pfA;
    if (a.maxDrawdownR !== b.maxDrawdownR) return a.maxDrawdownR - b.maxDrawdownR;
    if (b.trades !== a.trades) return b.trades - a.trades;
    if (a.failedTasks !== b.failedTasks) return a.failedTasks - b.failedTasks;
    if (a.strategyId !== b.strategyId) return a.strategyId.localeCompare(b.strategyId);
    return a.tuneId.localeCompare(b.tuneId);
}

function isReusableCandidate(row: ScalpResearchCandidateAggregate, minChampionTrades: number): boolean {
    return row.completedTasks > 0 && row.trades >= minChampionTrades;
}

function bestCandidateByStrategy(rows: ScalpResearchCandidateAggregate[]): Map<string, ScalpResearchCandidateAggregate> {
    const out = new Map<string, ScalpResearchCandidateAggregate>();
    for (const row of rows.slice().sort(compareCandidateAggregates)) {
        if (!out.has(row.strategyId)) {
            out.set(row.strategyId, row);
        }
    }
    return out;
}

type RuntimeFingerprint = {
    trailAtrMult: number;
    timeStopBars: number;
    tp1ClosePct: number;
    sweepBufferPips: number;
    entrySessionProfile: ScalpEntrySessionProfile;
    blockedBerlinEntryHours: number[];
};

function buildRuntimeFingerprint(params: {
    symbol: string;
    strategyId: string;
    tuneId: string;
    configOverride: ScalpStrategyConfigOverride | null;
}): RuntimeFingerprint {
    const deployment = resolveScalpDeployment({
        symbol: params.symbol,
        strategyId: params.strategyId,
        tuneId: params.tuneId,
    });
    const runtime = buildScalpReplayRuntimeFromDeployment({
        deployment,
        configOverride: params.configOverride,
    });
    return {
        trailAtrMult: Number(runtime.strategy.trailAtrMult),
        timeStopBars: Number(runtime.strategy.timeStopBars),
        tp1ClosePct: Number(runtime.strategy.tp1ClosePct),
        sweepBufferPips: Number(runtime.strategy.sweepBufferPips),
        entrySessionProfile: normalizeScalpEntrySessionProfile(runtime.strategy.entrySessionProfile, 'berlin'),
        blockedBerlinEntryHours: normalizeHours(runtime.strategy.blockedBerlinEntryHours),
    };
}

function runtimeDistance(a: RuntimeFingerprint, b: RuntimeFingerprint): number {
    return (
        Math.abs(a.trailAtrMult - b.trailAtrMult) +
        Math.abs(a.timeStopBars - b.timeStopBars) / 6 +
        Math.abs(a.tp1ClosePct - b.tp1ClosePct) / 10 +
        Math.abs(a.sweepBufferPips - b.sweepBufferPips) * 2 +
        scalpEntrySessionProfileDistance(a.entrySessionProfile, b.entrySessionProfile) * 1.5 +
        blockedHoursDistance(a.blockedBerlinEntryHours, b.blockedBerlinEntryHours) * 0.75
    );
}

function seedTuneRows(params: {
    symbol: string;
    strategyId: string;
    tunerEnabled: boolean;
    maxTuneVariantsPerStrategy: number;
    limit: number;
    tier: ScalpResearchPlanTier;
}): ScalpResearchPlanRow[] {
    const limit = Math.max(0, Math.floor(params.limit));
    if (limit <= 0) return [];
    if (!params.tunerEnabled) {
        return [
            {
                symbol: params.symbol,
                strategyId: params.strategyId,
                tuneId: 'default',
                configOverride: null,
                tier: params.tier,
            },
        ].slice(0, limit);
    }
    return buildScalpResearchTuneVariants({
        symbol: params.symbol,
        strategyId: params.strategyId,
        maxVariantsPerStrategy: params.maxTuneVariantsPerStrategy,
        includeBaseline: true,
    })
        .slice(0, limit)
        .map((row) => ({
            symbol: params.symbol,
            strategyId: params.strategyId,
            tuneId: row.tuneId,
            configOverride: row.configOverride || null,
            tier: params.tier,
        }));
}

function nearestTuneRows(params: {
    symbol: string;
    strategyId: string;
    targetTuneId: string;
    targetConfigOverride: ScalpStrategyConfigOverride | null;
    tunerEnabled: boolean;
    maxTuneVariantsPerStrategy: number;
    limit: number;
    excludeTuneIds?: Set<string>;
    tier: ScalpResearchPlanTier;
}): ScalpResearchPlanRow[] {
    const limit = Math.max(0, Math.floor(params.limit));
    if (!params.tunerEnabled || limit <= 0) return [];

    const target = buildRuntimeFingerprint({
        symbol: params.symbol,
        strategyId: params.strategyId,
        tuneId: params.targetTuneId,
        configOverride: params.targetConfigOverride,
    });
    const excluded = params.excludeTuneIds || new Set<string>();

    return buildScalpResearchTuneVariants({
        symbol: params.symbol,
        strategyId: params.strategyId,
        maxVariantsPerStrategy: params.maxTuneVariantsPerStrategy,
        includeBaseline: true,
    })
        .filter((row) => !excluded.has(row.tuneId))
        .map((row) => ({
            tuneId: row.tuneId,
            configOverride: row.configOverride || null,
            distance: runtimeDistance(
                target,
                buildRuntimeFingerprint({
                    symbol: params.symbol,
                    strategyId: params.strategyId,
                    tuneId: row.tuneId,
                    configOverride: row.configOverride || null,
                }),
            ),
        }))
        .sort((a, b) => {
            if (a.distance !== b.distance) return a.distance - b.distance;
            return a.tuneId.localeCompare(b.tuneId);
        })
        .slice(0, limit)
        .map((row) => ({
            symbol: params.symbol,
            strategyId: params.strategyId,
            tuneId: row.tuneId,
            configOverride: row.configOverride,
            tier: params.tier,
        }));
}

export function resolveScalpResearchPlannerPolicy(
    overrides: Partial<ScalpResearchPlannerPolicy> = {},
): ScalpResearchPlannerPolicy {
    return {
        enabled: overrides.enabled ?? toBool(process.env.SCALP_RESEARCH_PLANNER_ENABLED, true),
        championCandidatesPerSymbol: Math.max(
            1,
            toPositiveInt(
                overrides.championCandidatesPerSymbol ??
                    process.env.SCALP_RESEARCH_PLANNER_CHAMPIONS_PER_SYMBOL,
                DEFAULT_CHAMPION_CANDIDATES_PER_SYMBOL,
            ),
        ),
        neighborVariantsPerCandidate: Math.max(
            0,
            toNonNegativeInt(
                overrides.neighborVariantsPerCandidate ??
                    process.env.SCALP_RESEARCH_PLANNER_NEIGHBORS_PER_CANDIDATE,
                DEFAULT_NEIGHBOR_VARIANTS_PER_CANDIDATE,
            ),
        ),
        challengerStrategiesPerSymbol: Math.max(
            0,
            toNonNegativeInt(
                overrides.challengerStrategiesPerSymbol ??
                    process.env.SCALP_RESEARCH_PLANNER_CHALLENGER_STRATEGIES_PER_SYMBOL,
                DEFAULT_CHALLENGER_STRATEGIES_PER_SYMBOL,
            ),
        ),
        challengerTunesPerStrategy: Math.max(
            1,
            toPositiveInt(
                overrides.challengerTunesPerStrategy ??
                    process.env.SCALP_RESEARCH_PLANNER_CHALLENGER_TUNES_PER_STRATEGY,
                DEFAULT_CHALLENGER_TUNES_PER_STRATEGY,
            ),
        ),
        incubatorSymbolsPerCycle: Math.max(
            0,
            toNonNegativeInt(
                overrides.incubatorSymbolsPerCycle ??
                    process.env.SCALP_RESEARCH_PLANNER_INCUBATOR_SYMBOLS_PER_CYCLE ??
                    process.env.SCALP_RESEARCH_INCREMENTAL_MAX_NEW_SYMBOLS_PER_CYCLE,
                DEFAULT_INCUBATOR_SYMBOLS_PER_CYCLE,
            ),
        ),
        incubatorStrategiesPerSymbol: Math.max(
            1,
            toPositiveInt(
                overrides.incubatorStrategiesPerSymbol ??
                    process.env.SCALP_RESEARCH_PLANNER_INCUBATOR_STRATEGIES_PER_SYMBOL,
                DEFAULT_INCUBATOR_STRATEGIES_PER_SYMBOL,
            ),
        ),
        incubatorTunesPerStrategy: Math.max(
            1,
            toPositiveInt(
                overrides.incubatorTunesPerStrategy ??
                    process.env.SCALP_RESEARCH_PLANNER_INCUBATOR_TUNES_PER_STRATEGY,
                DEFAULT_INCUBATOR_TUNES_PER_STRATEGY,
            ),
        ),
        fallbackStrategiesPerSymbol: Math.max(
            1,
            toPositiveInt(
                overrides.fallbackStrategiesPerSymbol ??
                    process.env.SCALP_RESEARCH_PLANNER_FALLBACK_STRATEGIES_PER_SYMBOL,
                DEFAULT_FALLBACK_STRATEGIES_PER_SYMBOL,
            ),
        ),
        fallbackTunesPerStrategy: Math.max(
            1,
            toPositiveInt(
                overrides.fallbackTunesPerStrategy ??
                    process.env.SCALP_RESEARCH_PLANNER_FALLBACK_TUNES_PER_STRATEGY,
                DEFAULT_FALLBACK_TUNES_PER_STRATEGY,
            ),
        ),
        minChampionTrades: Math.max(
            1,
            toPositiveInt(
                overrides.minChampionTrades ?? process.env.SCALP_RESEARCH_PLANNER_MIN_CHAMPION_TRADES,
                DEFAULT_MIN_CHAMPION_TRADES,
            ),
        ),
    };
}

export function buildScalpResearchPlan(params: BuildScalpResearchPlanParams): ScalpResearchPlanRow[] {
    const policy = resolveScalpResearchPlannerPolicy(params.policy);
    if (!policy.enabled) return [];

    const symbols = Array.from(
        new Set(
            (params.symbols || [])
                .map((row) => normalizeSymbol(row))
                .filter((row) => Boolean(row)),
        ),
    );
    if (!symbols.length) return [];

    const knownStrategies = new Set(listScalpStrategies().map((row) => row.id));
    const previousRows = (params.previousSummary?.candidateAggregates || [])
        .filter((row) => knownStrategies.has(row.strategyId))
        .map((row) => ({
            ...row,
            symbol: normalizeSymbol(row.symbol),
        }))
        .filter((row) => Boolean(row.symbol));
    const previousBySymbol = new Map<string, ScalpResearchCandidateAggregate[]>();
    for (const row of previousRows) {
        const bucket = previousBySymbol.get(row.symbol) || [];
        bucket.push(row);
        previousBySymbol.set(row.symbol, bucket);
    }

    const bootstrap = previousRows.length === 0;
    const newSymbols = symbols.filter((symbol) => !previousBySymbol.has(symbol));
    const incubatorBudget = new Set(
        (bootstrap ? newSymbols : newSymbols.slice(0, policy.incubatorSymbolsPerCycle)).map((row) => normalizeSymbol(row)),
    );
    const bucketsBySymbol = new Map<
        string,
        {
            champion: ScalpResearchPlanRow[];
            neighbor: ScalpResearchPlanRow[];
            challenger: ScalpResearchPlanRow[];
            incubator: ScalpResearchPlanRow[];
            fallback: ScalpResearchPlanRow[];
        }
    >();

    for (const symbol of symbols) {
        const recommendedStrategies = resolveRecommendedStrategiesForSymbol(symbol, params.strategyAllowlist).filter((row) =>
            knownStrategies.has(row),
        );
        if (!recommendedStrategies.length) continue;

        const buckets = {
            champion: [] as ScalpResearchPlanRow[],
            neighbor: [] as ScalpResearchPlanRow[],
            challenger: [] as ScalpResearchPlanRow[],
            incubator: [] as ScalpResearchPlanRow[],
            fallback: [] as ScalpResearchPlanRow[],
        };

        const previousForSymbol = (previousBySymbol.get(symbol) || [])
            .filter((row) => recommendedStrategies.includes(row.strategyId))
            .sort(compareCandidateAggregates);
        const reusablePrevious = previousForSymbol.filter((row) => isReusableCandidate(row, policy.minChampionTrades));
        const cappedNewSymbol = previousForSymbol.length === 0 && !bootstrap && !incubatorBudget.has(symbol);

        if (reusablePrevious.length > 0) {
            const champions = reusablePrevious.slice(0, policy.championCandidatesPerSymbol);
            const championStrategies = new Set<string>();
            for (const row of champions) {
                buckets.champion.push({
                    symbol,
                    strategyId: row.strategyId,
                    tuneId: row.tuneId,
                    configOverride: row.configOverride || null,
                    tier: 'champion',
                });
                championStrategies.add(row.strategyId);
            }

            if (params.tunerEnabled && policy.neighborVariantsPerCandidate > 0) {
                for (const row of champions) {
                    const excludeTuneIds = new Set<string>([
                        ...buckets.champion.filter((entry) => entry.strategyId === row.strategyId).map((entry) => entry.tuneId),
                        ...buckets.neighbor.filter((entry) => entry.strategyId === row.strategyId).map((entry) => entry.tuneId),
                    ]);
                    buckets.neighbor.push(
                        ...nearestTuneRows({
                            symbol,
                            strategyId: row.strategyId,
                            targetTuneId: row.tuneId,
                            targetConfigOverride: row.configOverride || null,
                            tunerEnabled: params.tunerEnabled,
                            maxTuneVariantsPerStrategy: params.maxTuneVariantsPerStrategy,
                            limit: policy.neighborVariantsPerCandidate,
                            excludeTuneIds,
                            tier: 'neighbor',
                        }),
                    );
                }
            }

            const bestByStrategy = bestCandidateByStrategy(previousForSymbol);
            const rankedAlternateStrategies = recommendedStrategies
                .filter((strategyId) => !championStrategies.has(strategyId))
                .sort((a, b) => {
                    const rowA = bestByStrategy.get(a);
                    const rowB = bestByStrategy.get(b);
                    if (rowA && rowB) return compareCandidateAggregates(rowA, rowB);
                    if (rowA) return -1;
                    if (rowB) return 1;
                    return recommendedStrategies.indexOf(a) - recommendedStrategies.indexOf(b);
                })
                .slice(0, policy.challengerStrategiesPerSymbol);

            for (const strategyId of rankedAlternateStrategies) {
                const bestExisting = bestByStrategy.get(strategyId) || null;
                const challengerRows: ScalpResearchPlanRow[] = [];
                if (bestExisting) {
                    challengerRows.push({
                        symbol,
                        strategyId,
                        tuneId: bestExisting.tuneId,
                        configOverride: bestExisting.configOverride || null,
                        tier: 'challenger',
                    });
                }

                const remaining = Math.max(0, policy.challengerTunesPerStrategy - challengerRows.length);
                if (remaining > 0) {
                    if (bestExisting) {
                        challengerRows.push(
                            ...nearestTuneRows({
                                symbol,
                                strategyId,
                                targetTuneId: bestExisting.tuneId,
                                targetConfigOverride: bestExisting.configOverride || null,
                                tunerEnabled: params.tunerEnabled,
                                maxTuneVariantsPerStrategy: params.maxTuneVariantsPerStrategy,
                                limit: remaining,
                                excludeTuneIds: new Set(challengerRows.map((row) => row.tuneId)),
                                tier: 'challenger',
                            }),
                        );
                    } else {
                        challengerRows.push(
                            ...seedTuneRows({
                                symbol,
                                strategyId,
                                tunerEnabled: params.tunerEnabled,
                                maxTuneVariantsPerStrategy: params.maxTuneVariantsPerStrategy,
                                limit: remaining,
                                tier: 'challenger',
                            }),
                        );
                    }
                }
                buckets.challenger.push(...challengerRows.slice(0, policy.challengerTunesPerStrategy));
            }
        } else if (previousForSymbol.length > 0) {
            for (const strategyId of recommendedStrategies.slice(0, policy.fallbackStrategiesPerSymbol)) {
                buckets.fallback.push(
                    ...seedTuneRows({
                        symbol,
                        strategyId,
                        tunerEnabled: params.tunerEnabled,
                        maxTuneVariantsPerStrategy: params.maxTuneVariantsPerStrategy,
                        limit: policy.fallbackTunesPerStrategy,
                        tier: 'fallback',
                    }),
                );
            }
        } else if (incubatorBudget.has(symbol)) {
            for (const strategyId of recommendedStrategies.slice(0, policy.incubatorStrategiesPerSymbol)) {
                buckets.incubator.push(
                    ...seedTuneRows({
                        symbol,
                        strategyId,
                        tunerEnabled: params.tunerEnabled,
                        maxTuneVariantsPerStrategy: params.maxTuneVariantsPerStrategy,
                        limit: policy.incubatorTunesPerStrategy,
                        tier: 'incubator',
                    }),
                );
            }
        }

        if (
            !cappedNewSymbol &&
            buckets.champion.length === 0 &&
            buckets.neighbor.length === 0 &&
            buckets.challenger.length === 0 &&
            buckets.incubator.length === 0 &&
            buckets.fallback.length === 0
        ) {
            const strategyId = recommendedStrategies[0];
            if (strategyId) {
                buckets.fallback.push(
                    ...seedTuneRows({
                        symbol,
                        strategyId,
                        tunerEnabled: params.tunerEnabled,
                        maxTuneVariantsPerStrategy: params.maxTuneVariantsPerStrategy,
                        limit: policy.fallbackTunesPerStrategy,
                        tier: 'fallback',
                    }),
                );
            }
        }

        bucketsBySymbol.set(symbol, buckets);
    }

    const out: ScalpResearchPlanRow[] = [];
    const seenCombos = new Set<string>();
    const tierOrder: ScalpResearchPlanTier[] = ['champion', 'neighbor', 'challenger', 'incubator', 'fallback'];

    for (const tier of tierOrder) {
        for (const symbol of symbols) {
            const bucket = bucketsBySymbol.get(symbol);
            if (!bucket) continue;
            for (const row of bucket[tier]) {
                const key = comboKey(row);
                if (seenCombos.has(key)) continue;
                seenCombos.add(key);
                out.push(row);
            }
        }
    }

    return out;
}
