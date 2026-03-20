import type { ScalpStrategyConfigOverride } from './config';
import { resolveScalpDeployment, normalizeScalpTuneId } from './deployments';
import { buildScalpReplayRuntimeFromDeployment } from './replay/runtimeConfig';
import {
    listScalpEntrySessionProfiles,
    normalizeScalpEntrySessionProfile,
    scalpEntrySessionProfileDistance,
} from './sessions';
import type { ScalpEntrySessionProfile } from './types';
import { compactScalpStrategyConfigOverride } from './tuning';

export interface ScalpResearchTuneVariant {
    tuneId: string;
    configOverride: ScalpStrategyConfigOverride | null;
}

export interface ScalpResearchTunerPolicy {
    enabled: boolean;
    maxVariantsPerStrategy: number;
    includeBaseline: boolean;
}

const DEFAULT_MAX_VARIANTS_PER_STRATEGY = 16;
const HARD_MAX_VARIANTS_PER_STRATEGY = 256;

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

function normalizeHours(value: number[]): number[] {
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

function numberToken(value: number, digits = 2): string {
    const rounded = Number.isFinite(value) ? value : 0;
    const fixed = rounded.toFixed(digits).replace(/\.?0+$/, '');
    return fixed.replace('.', 'p').replace(/[^a-zA-Z0-9_-]/g, '');
}

function hoursToken(value: number[]): string {
    const hours = normalizeHours(value);
    if (!hours.length) return 'none';
    return hours.join('-');
}

function sessionProfileToken(value: ScalpEntrySessionProfile): string {
    return String(value).replace(/[^a-z0-9_]/g, '');
}

function pickNearestNumberAlternatives(values: number[], baseline: number, max = Number.POSITIVE_INFINITY): number[] {
    const deduped = Array.from(new Set(values.filter((row) => Number.isFinite(row))));
    const limit = Number.isFinite(max) ? Math.max(0, Math.floor(max)) : deduped.length;
    return deduped
        .filter((row) => Math.abs(row - baseline) > 1e-9)
        .sort((a, b) => {
            const distA = Math.abs(a - baseline);
            const distB = Math.abs(b - baseline);
            if (distA !== distB) return distA - distB;
            return a - b;
        })
        .slice(0, limit);
}

function pickNearestHoursAlternatives(values: number[][], baseline: number[], max = Number.POSITIVE_INFINITY): number[][] {
    const baselineNormalized = normalizeHours(baseline);
    const seen = new Set<string>([JSON.stringify(baselineNormalized)]);
    const out = values
        .map((row) => normalizeHours(row))
        .filter((row) => {
            const key = JSON.stringify(row);
            if (seen.has(key)) return false;
            seen.add(key);
            return true;
        })
        .sort((a, b) => {
            const distA = blockedHoursDistance(a, baselineNormalized);
            const distB = blockedHoursDistance(b, baselineNormalized);
            if (distA !== distB) return distA - distB;
            if (a.length !== b.length) return a.length - b.length;
            return JSON.stringify(a).localeCompare(JSON.stringify(b));
        });
    const limit = Number.isFinite(max) ? Math.max(0, Math.floor(max)) : out.length;
    return out.slice(0, limit);
}

function pickNearestSessionProfileAlternatives(
    values: ScalpEntrySessionProfile[],
    baseline: ScalpEntrySessionProfile,
    max = Number.POSITIVE_INFINITY,
): ScalpEntrySessionProfile[] {
    const baselineNormalized = normalizeScalpEntrySessionProfile(baseline, 'berlin');
    const deduped = Array.from(new Set(values.map((row) => normalizeScalpEntrySessionProfile(row, 'berlin'))));
    const limit = Number.isFinite(max) ? Math.max(0, Math.floor(max)) : deduped.length;
    return deduped
        .filter((row) => row !== baselineNormalized)
        .sort((a, b) => {
            const distA = scalpEntrySessionProfileDistance(a, baselineNormalized);
            const distB = scalpEntrySessionProfileDistance(b, baselineNormalized);
            if (distA !== distB) return distA - distB;
            return a.localeCompare(b);
        })
        .slice(0, limit);
}

function clamp(value: number, min: number, max: number): number {
    return Math.max(min, Math.min(max, value));
}

function round(value: number, digits = 2): number {
    const p = 10 ** digits;
    return Math.round(value * p) / p;
}

function dynamicTrailCandidates(base: number): number[] {
    return [round(base * 0.7), round(base * 0.85), round(base), round(base * 1.15), round(base * 1.3)];
}

function dynamicTimeStopCandidates(base: number): number[] {
    const b = Math.max(1, Math.floor(base));
    return [Math.max(4, b - 12), Math.max(4, b - 6), b, b + 6, b + 12];
}

function dynamicTp1Candidates(base: number): number[] {
    const b = clamp(round(base, 0), 0, 100);
    return [clamp(b - 20, 0, 100), clamp(b - 10, 0, 100), b, clamp(b + 10, 0, 100), clamp(b + 20, 0, 100)];
}

function dynamicSweepBufferCandidates(base: number): number[] {
    const b = Math.max(0.01, round(base, 2));
    return [round(b * 0.5), round(b * 0.75), b, round(b * 1.25), round(b * 1.5)];
}

export function resolveScalpResearchTunerPolicy(): ScalpResearchTunerPolicy {
    return {
        enabled: toBool(process.env.SCALP_RESEARCH_TUNER_ENABLED, true),
        maxVariantsPerStrategy: Math.max(
            1,
            Math.min(
                HARD_MAX_VARIANTS_PER_STRATEGY,
                toPositiveInt(
                    process.env.SCALP_RESEARCH_TUNER_MAX_VARIANTS_PER_STRATEGY,
                    DEFAULT_MAX_VARIANTS_PER_STRATEGY,
                ),
            ),
        ),
        includeBaseline: toBool(process.env.SCALP_RESEARCH_TUNER_INCLUDE_BASELINE, true),
    };
}

type InternalVariant = {
    tuneId: string;
    configOverride: ScalpStrategyConfigOverride | null;
    score: number;
};

export function buildScalpResearchTuneVariants(params: {
    symbol: string;
    strategyId: string;
    maxVariantsPerStrategy?: number;
    includeBaseline?: boolean;
    includeSessionProfileVariants?: boolean;
}): ScalpResearchTuneVariant[] {
    const deployment = resolveScalpDeployment({
        symbol: params.symbol,
        strategyId: params.strategyId,
        tuneId: 'default',
    });
    const runtime = buildScalpReplayRuntimeFromDeployment({
        deployment,
        configOverride: null,
    });

    const maxVariants = Math.max(
        1,
        Math.min(
            HARD_MAX_VARIANTS_PER_STRATEGY,
            toPositiveInt(params.maxVariantsPerStrategy, DEFAULT_MAX_VARIANTS_PER_STRATEGY),
        ),
    );
    const includeBaseline = params.includeBaseline !== false;
    const includeSessionProfileVariants = params.includeSessionProfileVariants !== false;
    const strategyId = deployment.strategyId;
    const baselineTrail = runtime.strategy.trailAtrMult;
    const baselineTimeStop = runtime.strategy.timeStopBars;
    const baselineTp1 = runtime.strategy.tp1ClosePct;
    const baselineSweepBuffer = runtime.strategy.sweepBufferPips;

    const variants: InternalVariant[] = [];
    const seenTuneIds = new Set<string>();
    const seenOverrides = new Set<string>();
    const combinationBudget = Math.max(0, Math.min(4_000, maxVariants * 8));
    let combinationsAdded = 0;

    const pushVariant = (variant: InternalVariant) => {
        const compacted = compactScalpStrategyConfigOverride(variant.configOverride);
        const overrideKey = JSON.stringify(compacted || null);
        if (seenOverrides.has(overrideKey)) return;
        const tuneId = normalizeScalpTuneId(variant.tuneId, 'auto');
        if (seenTuneIds.has(tuneId)) return;
        seenTuneIds.add(tuneId);
        seenOverrides.add(overrideKey);
        variants.push({
            ...variant,
            tuneId,
            configOverride: compacted,
        });
    };

    if (includeBaseline) {
        pushVariant({
            tuneId: 'default',
            configOverride: null,
            score: -1,
        });
    }

    const addTrail = (values: number[]) => {
        const baseline = baselineTrail;
        const alts = pickNearestNumberAlternatives(values, baseline);
        for (const value of alts) {
            pushVariant({
                tuneId: `auto_tr${numberToken(value, 2)}`,
                configOverride: { risk: { trailAtrMult: value } },
                score: Math.abs(value - baseline),
            });
        }
    };

    const addTimeStop = (values: number[]) => {
        const baseline = baselineTimeStop;
        const alts = pickNearestNumberAlternatives(values, baseline);
        for (const value of alts) {
            pushVariant({
                tuneId: `auto_ts${Math.floor(value)}`,
                configOverride: { risk: { timeStopBars: Math.max(1, Math.floor(value)) } },
                score: Math.abs(value - baseline),
            });
        }
    };

    const addTp1 = (values: number[]) => {
        const baseline = baselineTp1;
        const alts = pickNearestNumberAlternatives(values, baseline);
        for (const value of alts) {
            pushVariant({
                tuneId: `auto_tp${Math.floor(value)}`,
                configOverride: { risk: { tp1ClosePct: clamp(value, 0, 100) } },
                score: Math.abs(value - baseline),
            });
        }
    };

    const addSweepBuffer = (values: number[]) => {
        const baseline = baselineSweepBuffer;
        const alts = pickNearestNumberAlternatives(values, baseline);
        for (const value of alts) {
            pushVariant({
                tuneId: `auto_sw${numberToken(value, 2)}`,
                configOverride: { sweep: { bufferPips: Math.max(0, value) } },
                score: Math.abs(value - baseline),
            });
        }
    };

    const addBlockedHours = (values: number[][]) => {
        const baseline = runtime.strategy.blockedBerlinEntryHours;
        const alts = pickNearestHoursAlternatives(values, baseline);
        for (const value of alts) {
            pushVariant({
                tuneId: `auto_bh${hoursToken(value)}`,
                configOverride: { sessions: { blockedBerlinEntryHours: normalizeHours(value) } },
                score: blockedHoursDistance(value, baseline),
            });
        }
    };

    const addSessionProfiles = (values: ScalpEntrySessionProfile[]) => {
        const baseline = normalizeScalpEntrySessionProfile(runtime.strategy.entrySessionProfile, 'berlin');
        const alts = pickNearestSessionProfileAlternatives(values, baseline);
        for (const value of alts) {
            pushVariant({
                tuneId: `auto_sp_${sessionProfileToken(value)}`,
                configOverride: { sessions: { entrySessionProfile: value } },
                score: scalpEntrySessionProfileDistance(value, baseline),
            });
        }
    };

    const addRiskMixVariants = (params: {
        trailValues: number[];
        timeStopValues: number[];
        tp1Values: number[];
        sweepValues?: number[];
    }) => {
        const trailValues = Array.from(new Set([baselineTrail, ...params.trailValues]));
        const timeStopValues = Array.from(new Set([baselineTimeStop, ...params.timeStopValues])).map((row) =>
            Math.max(1, Math.floor(row)),
        );
        const tp1Values = Array.from(new Set([baselineTp1, ...params.tp1Values])).map((row) => clamp(row, 0, 100));
        const sweepValues =
            params.sweepValues && params.sweepValues.length > 0
                ? Array.from(new Set([baselineSweepBuffer, ...params.sweepValues])).map((row) => Math.max(0, row))
                : null;

        for (const trailAtrMult of trailValues) {
            for (const timeStopBars of timeStopValues) {
                for (const tp1ClosePct of tp1Values) {
                    const sweepLoop = sweepValues || [null];
                    for (const sweepBufferPips of sweepLoop) {
                        if (combinationsAdded >= combinationBudget) return;

                        let changed = 0;
                        if (Math.abs(trailAtrMult - baselineTrail) > 1e-9) changed += 1;
                        if (Math.abs(timeStopBars - baselineTimeStop) > 1e-9) changed += 1;
                        if (Math.abs(tp1ClosePct - baselineTp1) > 1e-9) changed += 1;
                        if (
                            sweepBufferPips !== null &&
                            Math.abs(sweepBufferPips - baselineSweepBuffer) > 1e-9
                        ) {
                            changed += 1;
                        }
                        if (changed < 2) continue;

                        const before = variants.length;
                        const tuneIdParts = [
                            `tr${numberToken(trailAtrMult, 2)}`,
                            `ts${Math.floor(timeStopBars)}`,
                            `tp${Math.floor(tp1ClosePct)}`,
                        ];
                        const configOverride: ScalpStrategyConfigOverride = {
                            risk: {
                                trailAtrMult,
                                timeStopBars,
                                tp1ClosePct,
                            },
                        };
                        if (sweepBufferPips !== null) {
                            tuneIdParts.push(`sw${numberToken(sweepBufferPips, 2)}`);
                            configOverride.sweep = { bufferPips: sweepBufferPips };
                        }
                        pushVariant({
                            tuneId: `auto_mix_${tuneIdParts.join('_')}`,
                            configOverride,
                            score:
                                Math.abs(trailAtrMult - baselineTrail) +
                                Math.abs(timeStopBars - baselineTimeStop) / 6 +
                                Math.abs(tp1ClosePct - baselineTp1) / 10 +
                                (sweepBufferPips === null
                                    ? 0
                                    : Math.abs(sweepBufferPips - baselineSweepBuffer) * 2) +
                                0.75,
                        });
                        if (variants.length > before) combinationsAdded += 1;
                    }
                }
            }
        }
    };

    if (includeSessionProfileVariants) {
        addSessionProfiles(listScalpEntrySessionProfiles());
    }

    if (strategyId === 'compression_breakout_pullback_m15_m3') {
        const trailValues = [1.3, 1.4, 1.5, 1.6, 1.7];
        const timeStopValues = [12, 15, 18, 21];
        const sweepValues = [0.1, 0.15, 0.2, 0.25, 0.3];
        const tp1Values = [0, 8, 15, 20];
        addTrail(trailValues);
        addTimeStop(timeStopValues);
        addSweepBuffer(sweepValues);
        addTp1(tp1Values);
        addRiskMixVariants({
            trailValues,
            timeStopValues,
            tp1Values,
            sweepValues,
        });
    } else if (strategyId === 'regime_pullback_m15_m3') {
        const trailValues = [1.2, 1.3, 1.4, 1.5, 1.6];
        const timeStopValues = [12, 15, 18];
        const tp1Values = [10, 20, 30];
        addBlockedHours([[], [10, 11], [9, 10], [11, 12], [10], [11]]);
        addTrail(trailValues);
        addTimeStop(timeStopValues);
        addTp1(tp1Values);
        addRiskMixVariants({
            trailValues,
            timeStopValues,
            tp1Values,
        });
    } else {
        const trailValues = dynamicTrailCandidates(runtime.strategy.trailAtrMult);
        const timeStopValues = dynamicTimeStopCandidates(runtime.strategy.timeStopBars);
        const tp1Values = dynamicTp1Candidates(runtime.strategy.tp1ClosePct);
        const sweepValues = dynamicSweepBufferCandidates(runtime.strategy.sweepBufferPips);
        addTrail(trailValues);
        addTimeStop(timeStopValues);
        addTp1(tp1Values);
        addSweepBuffer(sweepValues);
        addRiskMixVariants({
            trailValues,
            timeStopValues,
            tp1Values,
            sweepValues,
        });
    }

    const sortedVariants = variants
        .sort((a, b) => {
            if (a.score !== b.score) return a.score - b.score;
            return a.tuneId.localeCompare(b.tuneId);
        });
    return sortedVariants.slice(0, maxVariants).map((row) => ({
        tuneId: row.tuneId,
        configOverride: row.configOverride,
    }));
}
