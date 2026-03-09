import { mkdir, readFile, writeFile } from 'node:fs/promises';
import path from 'node:path';

import { kvGetJson, kvSetJson } from '../kv';
import type { ScalpStrategyConfigOverride } from './config';
import { resolveScalpDeployment } from './deployments';
import type { ScalpBacktestLeaderboardEntry } from './replay/types';
import {
    REGIME_PULLBACK_M15_M3_BTCUSDT_STRATEGY_ID,
    resolveBtcusdtGuardBlockedBerlinHours,
    resolveBtcusdtGuardOptimizedRiskDefaults,
} from './strategies/regimePullbackM15M3BtcusdtGuarded';
import {
    REGIME_PULLBACK_M15_M3_XAUUSD_STRATEGY_ID,
    resolveXauusdGuardBlockedBerlinHours,
    resolveXauusdGuardOptimizedRiskDefaults,
} from './strategies/regimePullbackM15M3XauusdGuarded';
import type { ScalpDeploymentRef } from './types';

export type ScalpDeploymentRegistrySource = 'manual' | 'backtest' | 'matrix';

export interface ScalpForwardValidationMetrics {
    rollCount: number;
    profitableWindowPct: number;
    meanExpectancyR: number;
    meanProfitFactor: number | null;
    maxDrawdownR: number | null;
    minTradesPerWindow: number | null;
    selectionWindowDays: number | null;
    forwardWindowDays: number | null;
    weeklySlices?: number | null;
    weeklyProfitablePct?: number | null;
    weeklyMeanExpectancyR?: number | null;
    weeklyMedianExpectancyR?: number | null;
    weeklyWorstNetR?: number | null;
    weeklyTopWeekPnlConcentrationPct?: number | null;
    weeklyEvaluatedAtMs?: number | null;
}

export interface ScalpDeploymentPromotionGateThresholds {
    minRollCount: number;
    minProfitableWindowPct: number;
    minMeanExpectancyR: number;
    minTradesPerWindow: number;
    maxDrawdownR: number | null;
    minWeeklySlices?: number | null;
    minWeeklyProfitablePct?: number | null;
    minWeeklyMedianExpectancyR?: number | null;
    maxWeeklyTopWeekPnlConcentrationPct?: number | null;
}

export type ScalpDeploymentPromotionGateSource = 'walk_forward' | 'manual' | 'none';

export interface ScalpDeploymentPromotionGate {
    eligible: boolean;
    reason: string | null;
    source: ScalpDeploymentPromotionGateSource;
    evaluatedAtMs: number;
    forwardValidation: ScalpForwardValidationMetrics | null;
    thresholds: ScalpDeploymentPromotionGateThresholds | null;
}

export interface ScalpDeploymentRegistryEntry extends ScalpDeploymentRef {
    enabled: boolean;
    source: ScalpDeploymentRegistrySource;
    notes: string | null;
    configOverride: ScalpStrategyConfigOverride | null;
    leaderboardEntry: ScalpBacktestLeaderboardEntry | null;
    promotionGate: ScalpDeploymentPromotionGate | null;
    createdAtMs: number;
    updatedAtMs: number;
    updatedBy: string | null;
}

export interface ScalpDeploymentRegistrySnapshot {
    version: 1;
    updatedAt: string | null;
    deployments: ScalpDeploymentRegistryEntry[];
}

export interface CanonicalizeScalpDeploymentRegistryResult {
    dryRun: boolean;
    storeMode: 'kv' | 'file';
    registryPath: string;
    registryKvKey: string;
    beforeCount: number;
    afterCount: number;
    dedupedCount: number;
    legacyStrategyRows: number;
    legacyDeploymentIdRows: number;
    wrote: boolean;
    updatedAt: string;
    snapshot: ScalpDeploymentRegistrySnapshot;
}

export type ScalpDeploymentRegistryWriteParams = {
    symbol?: unknown;
    strategyId?: unknown;
    tuneId?: unknown;
    deploymentId?: unknown;
    enabled?: unknown;
    source?: unknown;
    notes?: unknown;
    configOverride?: unknown;
    leaderboardEntry?: unknown;
    forwardValidation?: unknown;
    promotionGate?: unknown;
    updatedBy?: unknown;
};

type RegistryWriteParams = ScalpDeploymentRegistryWriteParams;

const DEFAULT_SCALP_DEPLOYMENT_REGISTRY_PATH = 'data/scalp-deployments.json';
const DEFAULT_SCALP_DEPLOYMENT_REGISTRY_KV_KEY = 'scalp:deployments:registry:v1';
const REGISTRY_VERSION = 1 as const;
const DEFAULT_FORWARD_GATE_THRESHOLDS: ScalpDeploymentPromotionGateThresholds = {
    minRollCount: 6,
    minProfitableWindowPct: 55,
    minMeanExpectancyR: 0,
    minTradesPerWindow: 2,
    maxDrawdownR: null,
    minWeeklySlices: null,
    minWeeklyProfitablePct: null,
    minWeeklyMedianExpectancyR: null,
    maxWeeklyTopWeekPnlConcentrationPct: null,
};

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

function normalizeFiniteNumber(value: unknown): number | null {
    const n = Number(value);
    if (!Number.isFinite(n)) return null;
    return n;
}

function normalizeConfigOverride(value: unknown): ScalpStrategyConfigOverride | null {
    if (!isRecord(value)) return null;
    return deepClone(value) as ScalpStrategyConfigOverride;
}

function isLegacyRegimeGuardStrategyId(value: unknown): boolean {
    const strategyId = String(value || '')
        .trim()
        .toLowerCase();
    return (
        strategyId === REGIME_PULLBACK_M15_M3_BTCUSDT_STRATEGY_ID ||
        strategyId === REGIME_PULLBACK_M15_M3_XAUUSD_STRATEGY_ID
    );
}

function hasLegacyRegimeGuardIdInDeployment(value: unknown): boolean {
    const deploymentId = String(value || '')
        .trim()
        .toLowerCase();
    if (!deploymentId) return false;
    return deploymentId.includes(`~${REGIME_PULLBACK_M15_M3_BTCUSDT_STRATEGY_ID}~`) || deploymentId.includes(`~${REGIME_PULLBACK_M15_M3_XAUUSD_STRATEGY_ID}~`);
}

function mergeConfigOverrides(
    base: ScalpStrategyConfigOverride | null,
    patch: ScalpStrategyConfigOverride | null,
): ScalpStrategyConfigOverride | null {
    if (!base && !patch) return null;
    if (!base) return deepClone(patch) as ScalpStrategyConfigOverride;
    if (!patch) return deepClone(base) as ScalpStrategyConfigOverride;
    const out = deepClone(base) as Record<string, unknown>;
    const apply = (target: Record<string, unknown>, source: Record<string, unknown>) => {
        for (const [key, raw] of Object.entries(source)) {
            if (raw === undefined) continue;
            if (Array.isArray(raw)) {
                target[key] = raw.slice();
                continue;
            }
            if (isRecord(raw) && isRecord(target[key])) {
                apply(target[key] as Record<string, unknown>, raw);
                continue;
            }
            target[key] = isRecord(raw) ? deepClone(raw) : raw;
        }
    };
    apply(out, patch as Record<string, unknown>);
    return out as ScalpStrategyConfigOverride;
}

function normalizeTuneLabel(value: unknown): string {
    return String(value || '')
        .trim()
        .toLowerCase();
}

function resolveLegacyBtcusdtBlockedHours(tuneId: unknown): number[] {
    const tune = normalizeTuneLabel(tuneId);
    if (tune.includes('high_pf')) return [10, 11];
    if (tune.includes('low_dd')) return [10];
    if (tune.includes('return')) return [];
    return resolveBtcusdtGuardBlockedBerlinHours();
}

function resolveLegacyXauusdBlockedHours(tuneId: unknown): number[] {
    const tune = normalizeTuneLabel(tuneId);
    if (tune.includes('high_pf')) return [15, 17];
    if (tune.includes('low_dd')) return [9, 15];
    if (tune.includes('return')) return [15];
    return resolveXauusdGuardBlockedBerlinHours();
}

function legacyRegimeGuardConfigOverride(rawStrategyId: unknown, rawTuneId: unknown): ScalpStrategyConfigOverride | null {
    const strategyId = String(rawStrategyId || '')
        .trim()
        .toLowerCase();
    if (strategyId === REGIME_PULLBACK_M15_M3_BTCUSDT_STRATEGY_ID) {
        const risk = resolveBtcusdtGuardOptimizedRiskDefaults();
        return {
            risk: {
                tp1ClosePct: risk.tp1ClosePct,
                trailAtrMult: risk.trailAtrMult,
                timeStopBars: risk.timeStopBars,
            },
            sessions: {
                blockedBerlinEntryHours: resolveLegacyBtcusdtBlockedHours(rawTuneId),
            },
            confirm: {
                allowPullbackSwingBreakTrigger: true,
            },
        };
    }
    if (strategyId === REGIME_PULLBACK_M15_M3_XAUUSD_STRATEGY_ID) {
        const risk = resolveXauusdGuardOptimizedRiskDefaults();
        return {
            risk: {
                tp1ClosePct: risk.tp1ClosePct,
                trailAtrMult: risk.trailAtrMult,
                timeStopBars: risk.timeStopBars,
            },
            sessions: {
                blockedBerlinEntryHours: resolveLegacyXauusdBlockedHours(rawTuneId),
            },
            confirm: {
                allowPullbackSwingBreakTrigger: false,
            },
        };
    }
    return null;
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

function normalizeForwardValidation(value: unknown): ScalpForwardValidationMetrics | null {
    if (!isRecord(value)) return null;
    const rollCount = Math.floor(Number(value.rollCount));
    const profitableWindowPctRaw = normalizeFiniteNumber(value.profitableWindowPct);
    const meanExpectancyR = normalizeFiniteNumber(value.meanExpectancyR);
    if (!Number.isFinite(rollCount) || rollCount <= 0) return null;
    if (profitableWindowPctRaw === null || meanExpectancyR === null) return null;

    const meanProfitFactor = normalizeFiniteNumber(value.meanProfitFactor);
    const maxDrawdownR = normalizeFiniteNumber(value.maxDrawdownR);
    const minTradesPerWindowRaw = normalizeFiniteNumber(value.minTradesPerWindow);
    const selectionWindowDaysRaw = normalizeFiniteNumber(value.selectionWindowDays);
    const forwardWindowDaysRaw = normalizeFiniteNumber(value.forwardWindowDays);
    const weeklySlicesRaw = normalizeFiniteNumber(value.weeklySlices);
    const weeklyProfitablePctRaw = normalizeFiniteNumber(value.weeklyProfitablePct);
    const weeklyMeanExpectancyRRaw = normalizeFiniteNumber(value.weeklyMeanExpectancyR);
    const weeklyMedianExpectancyRRaw = normalizeFiniteNumber(value.weeklyMedianExpectancyR);
    const weeklyWorstNetRRaw = normalizeFiniteNumber(value.weeklyWorstNetR);
    const weeklyTopWeekPnlConcentrationPctRaw = normalizeFiniteNumber(value.weeklyTopWeekPnlConcentrationPct);
    const weeklyEvaluatedAtMsRaw = normalizeFiniteNumber(value.weeklyEvaluatedAtMs);

    return {
        rollCount,
        profitableWindowPct: Math.max(0, Math.min(100, profitableWindowPctRaw)),
        meanExpectancyR,
        meanProfitFactor: meanProfitFactor !== null ? meanProfitFactor : null,
        maxDrawdownR: maxDrawdownR !== null && maxDrawdownR >= 0 ? maxDrawdownR : null,
        minTradesPerWindow:
            minTradesPerWindowRaw !== null && minTradesPerWindowRaw >= 0 ? Math.floor(minTradesPerWindowRaw) : null,
        selectionWindowDays:
            selectionWindowDaysRaw !== null && selectionWindowDaysRaw > 0 ? Math.floor(selectionWindowDaysRaw) : null,
        forwardWindowDays:
            forwardWindowDaysRaw !== null && forwardWindowDaysRaw > 0 ? Math.floor(forwardWindowDaysRaw) : null,
        weeklySlices: weeklySlicesRaw !== null && weeklySlicesRaw > 0 ? Math.floor(weeklySlicesRaw) : null,
        weeklyProfitablePct:
            weeklyProfitablePctRaw !== null ? Math.max(0, Math.min(100, weeklyProfitablePctRaw)) : null,
        weeklyMeanExpectancyR: weeklyMeanExpectancyRRaw !== null ? weeklyMeanExpectancyRRaw : null,
        weeklyMedianExpectancyR: weeklyMedianExpectancyRRaw !== null ? weeklyMedianExpectancyRRaw : null,
        weeklyWorstNetR: weeklyWorstNetRRaw !== null ? weeklyWorstNetRRaw : null,
        weeklyTopWeekPnlConcentrationPct:
            weeklyTopWeekPnlConcentrationPctRaw !== null
                ? Math.max(0, Math.min(100, weeklyTopWeekPnlConcentrationPctRaw))
                : null,
        weeklyEvaluatedAtMs: weeklyEvaluatedAtMsRaw !== null && weeklyEvaluatedAtMsRaw > 0 ? Math.floor(weeklyEvaluatedAtMsRaw) : null,
    };
}

function normalizePromotionGateSource(value: unknown, fallback: ScalpDeploymentPromotionGateSource): ScalpDeploymentPromotionGateSource {
    const normalized = String(value || '')
        .trim()
        .toLowerCase();
    if (normalized === 'walk_forward' || normalized === 'manual' || normalized === 'none') return normalized;
    return fallback;
}

function normalizeForwardGateThresholds(value: unknown): ScalpDeploymentPromotionGateThresholds | null {
    if (!isRecord(value)) return null;
    const minRollCount = Math.floor(Number(value.minRollCount));
    const minProfitableWindowPct = normalizeFiniteNumber(value.minProfitableWindowPct);
    const minMeanExpectancyR = normalizeFiniteNumber(value.minMeanExpectancyR);
    const minTradesPerWindow = Math.floor(Number(value.minTradesPerWindow));
    const maxDrawdownRRaw = normalizeFiniteNumber(value.maxDrawdownR);
    const minWeeklySlicesRaw = normalizeFiniteNumber(value.minWeeklySlices);
    const minWeeklyProfitablePctRaw = normalizeFiniteNumber(value.minWeeklyProfitablePct);
    const minWeeklyMedianExpectancyRRaw = normalizeFiniteNumber(value.minWeeklyMedianExpectancyR);
    const maxWeeklyTopWeekPnlConcentrationPctRaw = normalizeFiniteNumber(value.maxWeeklyTopWeekPnlConcentrationPct);
    if (!Number.isFinite(minRollCount) || minRollCount <= 0) return null;
    if (minProfitableWindowPct === null || minMeanExpectancyR === null) return null;
    if (!Number.isFinite(minTradesPerWindow) || minTradesPerWindow < 0) return null;
    return {
        minRollCount,
        minProfitableWindowPct: Math.max(0, Math.min(100, minProfitableWindowPct)),
        minMeanExpectancyR,
        minTradesPerWindow,
        maxDrawdownR: maxDrawdownRRaw !== null && maxDrawdownRRaw >= 0 ? maxDrawdownRRaw : null,
        minWeeklySlices: minWeeklySlicesRaw !== null && minWeeklySlicesRaw > 0 ? Math.floor(minWeeklySlicesRaw) : null,
        minWeeklyProfitablePct:
            minWeeklyProfitablePctRaw !== null ? Math.max(0, Math.min(100, minWeeklyProfitablePctRaw)) : null,
        minWeeklyMedianExpectancyR: minWeeklyMedianExpectancyRRaw !== null ? minWeeklyMedianExpectancyRRaw : null,
        maxWeeklyTopWeekPnlConcentrationPct:
            maxWeeklyTopWeekPnlConcentrationPctRaw !== null
                ? Math.max(0, Math.min(100, maxWeeklyTopWeekPnlConcentrationPctRaw))
                : null,
    };
}

function normalizePromotionGate(value: unknown): ScalpDeploymentPromotionGate | null {
    if (!isRecord(value)) return null;
    const eligible = normalizeBool(value.eligible, false);
    const evaluatedAtMs = normalizePositiveTime(value.evaluatedAtMs) || Date.now();
    const reason = normalizeOptionalText(value.reason, 220);
    return {
        eligible,
        reason,
        source: normalizePromotionGateSource(value.source, 'manual'),
        evaluatedAtMs,
        forwardValidation: normalizeForwardValidation(value.forwardValidation),
        thresholds: normalizeForwardGateThresholds(value.thresholds),
    };
}

function resolveForwardGateThresholds(): ScalpDeploymentPromotionGateThresholds {
    const minRollCount = Math.floor(Number(process.env.SCALP_DEPLOYMENT_FORWARD_GATE_MIN_ROLLS));
    const minProfitableWindowPct = normalizeFiniteNumber(process.env.SCALP_DEPLOYMENT_FORWARD_GATE_MIN_PROFITABLE_PCT);
    const minMeanExpectancyR = normalizeFiniteNumber(process.env.SCALP_DEPLOYMENT_FORWARD_GATE_MIN_MEAN_EXPECTANCY_R);
    const minTradesPerWindow = Math.floor(Number(process.env.SCALP_DEPLOYMENT_FORWARD_GATE_MIN_TRADES_PER_WINDOW));
    const maxDrawdownR = normalizeFiniteNumber(process.env.SCALP_DEPLOYMENT_FORWARD_GATE_MAX_DRAWDOWN_R);
    const minWeeklySlices = normalizeFiniteNumber(process.env.SCALP_DEPLOYMENT_FORWARD_GATE_MIN_WEEKLY_SLICES);
    const minWeeklyProfitablePct = normalizeFiniteNumber(process.env.SCALP_DEPLOYMENT_FORWARD_GATE_MIN_WEEKLY_PROFITABLE_PCT);
    const minWeeklyMedianExpectancyR = normalizeFiniteNumber(
        process.env.SCALP_DEPLOYMENT_FORWARD_GATE_MIN_WEEKLY_MEDIAN_EXPECTANCY_R,
    );
    const maxWeeklyTopWeekPnlConcentrationPct = normalizeFiniteNumber(
        process.env.SCALP_DEPLOYMENT_FORWARD_GATE_MAX_WEEKLY_TOP_WEEK_PNL_CONCENTRATION_PCT,
    );
    return {
        minRollCount:
            Number.isFinite(minRollCount) && minRollCount > 0 ? minRollCount : DEFAULT_FORWARD_GATE_THRESHOLDS.minRollCount,
        minProfitableWindowPct:
            minProfitableWindowPct !== null
                ? Math.max(0, Math.min(100, minProfitableWindowPct))
                : DEFAULT_FORWARD_GATE_THRESHOLDS.minProfitableWindowPct,
        minMeanExpectancyR:
            minMeanExpectancyR !== null ? minMeanExpectancyR : DEFAULT_FORWARD_GATE_THRESHOLDS.minMeanExpectancyR,
        minTradesPerWindow:
            Number.isFinite(minTradesPerWindow) && minTradesPerWindow >= 0
                ? minTradesPerWindow
                : DEFAULT_FORWARD_GATE_THRESHOLDS.minTradesPerWindow,
        maxDrawdownR: maxDrawdownR !== null && maxDrawdownR >= 0 ? maxDrawdownR : null,
        minWeeklySlices:
            minWeeklySlices !== null && minWeeklySlices > 0
                ? Math.floor(minWeeklySlices)
                : DEFAULT_FORWARD_GATE_THRESHOLDS.minWeeklySlices,
        minWeeklyProfitablePct:
            minWeeklyProfitablePct !== null
                ? Math.max(0, Math.min(100, minWeeklyProfitablePct))
                : DEFAULT_FORWARD_GATE_THRESHOLDS.minWeeklyProfitablePct,
        minWeeklyMedianExpectancyR:
            minWeeklyMedianExpectancyR !== null
                ? minWeeklyMedianExpectancyR
                : DEFAULT_FORWARD_GATE_THRESHOLDS.minWeeklyMedianExpectancyR,
        maxWeeklyTopWeekPnlConcentrationPct:
            maxWeeklyTopWeekPnlConcentrationPct !== null
                ? Math.max(0, Math.min(100, maxWeeklyTopWeekPnlConcentrationPct))
                : DEFAULT_FORWARD_GATE_THRESHOLDS.maxWeeklyTopWeekPnlConcentrationPct,
    };
}

function evaluateForwardValidationAgainstThresholds(
    validation: ScalpForwardValidationMetrics,
    thresholds: ScalpDeploymentPromotionGateThresholds,
): { eligible: boolean; reason: string | null } {
    if (validation.rollCount < thresholds.minRollCount) {
        return { eligible: false, reason: 'forward_roll_count_below_threshold' };
    }
    if (validation.profitableWindowPct < thresholds.minProfitableWindowPct) {
        return { eligible: false, reason: 'forward_profitable_window_pct_below_threshold' };
    }
    if (validation.meanExpectancyR < thresholds.minMeanExpectancyR) {
        return { eligible: false, reason: 'forward_mean_expectancy_below_threshold' };
    }
    if (thresholds.minTradesPerWindow > 0) {
        const minTrades = validation.minTradesPerWindow;
        if (minTrades === null || minTrades < thresholds.minTradesPerWindow) {
            return { eligible: false, reason: 'forward_min_trades_per_window_below_threshold' };
        }
    }
    if (thresholds.maxDrawdownR !== null) {
        const maxDrawdownR = validation.maxDrawdownR;
        if (maxDrawdownR === null || maxDrawdownR > thresholds.maxDrawdownR) {
            return { eligible: false, reason: 'forward_max_drawdown_above_threshold' };
        }
    }
    if (typeof thresholds.minWeeklySlices === 'number') {
        const weeklySlices = validation.weeklySlices;
        if (weeklySlices === null || weeklySlices === undefined || weeklySlices < thresholds.minWeeklySlices) {
            return { eligible: false, reason: 'weekly_slice_count_below_threshold' };
        }
    }
    if (typeof thresholds.minWeeklyProfitablePct === 'number') {
        const weeklyProfitablePct = validation.weeklyProfitablePct;
        if (
            weeklyProfitablePct === null ||
            weeklyProfitablePct === undefined ||
            weeklyProfitablePct < thresholds.minWeeklyProfitablePct
        ) {
            return { eligible: false, reason: 'weekly_profitable_pct_below_threshold' };
        }
    }
    if (typeof thresholds.minWeeklyMedianExpectancyR === 'number') {
        const weeklyMedianExpectancyR = validation.weeklyMedianExpectancyR;
        if (
            weeklyMedianExpectancyR === null ||
            weeklyMedianExpectancyR === undefined ||
            weeklyMedianExpectancyR < thresholds.minWeeklyMedianExpectancyR
        ) {
            return { eligible: false, reason: 'weekly_median_expectancy_below_threshold' };
        }
    }
    if (typeof thresholds.maxWeeklyTopWeekPnlConcentrationPct === 'number') {
        const concentration = validation.weeklyTopWeekPnlConcentrationPct;
        if (
            concentration === null ||
            concentration === undefined ||
            concentration > thresholds.maxWeeklyTopWeekPnlConcentrationPct
        ) {
            return { eligible: false, reason: 'weekly_top_week_concentration_above_threshold' };
        }
    }
    return { eligible: true, reason: null };
}

function requiresForwardGate(source: ScalpDeploymentRegistrySource): boolean {
    return source === 'backtest' || source === 'matrix';
}

function allowIneligibleEnable(): boolean {
    return normalizeBool(process.env.SCALP_DEPLOYMENT_ALLOW_INELIGIBLE_ENABLE, false);
}

function buildPendingForwardGate(nowMs: number, thresholds: ScalpDeploymentPromotionGateThresholds): ScalpDeploymentPromotionGate {
    return {
        eligible: false,
        reason: 'missing_forward_validation',
        source: 'none',
        evaluatedAtMs: nowMs,
        forwardValidation: null,
        thresholds,
    };
}

export function isScalpDeploymentPromotionEligible(entry: Pick<ScalpDeploymentRegistryEntry, 'promotionGate'>): boolean {
    return Boolean(entry.promotionGate?.eligible);
}

function normalizeRegistryEntry(raw: unknown): ScalpDeploymentRegistryEntry | null {
    if (!isRecord(raw)) return null;
    const legacyOverride = legacyRegimeGuardConfigOverride(raw.strategyId, raw.tuneId);
    const configOverride = mergeConfigOverrides(legacyOverride, normalizeConfigOverride(raw.configOverride));
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
        configOverride,
        leaderboardEntry: normalizeLeaderboardEntry(raw.leaderboardEntry, deployment),
        promotionGate: normalizePromotionGate(raw.promotionGate),
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
    const deduped = new Map<string, ScalpDeploymentRegistryEntry>();
    for (const entry of deploymentsRaw
        .map((entry) => normalizeRegistryEntry(entry))
        .filter((entry): entry is ScalpDeploymentRegistryEntry => Boolean(entry))) {
        const prev = deduped.get(entry.deploymentId);
        if (!prev || entry.updatedAtMs >= prev.updatedAtMs) {
            deduped.set(entry.deploymentId, entry);
        }
    }
    const deployments = Array.from(deduped.values())
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

export function scalpDeploymentRegistryKvKey(): string {
    const configured = String(process.env.SCALP_DEPLOYMENTS_REGISTRY_KV_KEY || DEFAULT_SCALP_DEPLOYMENT_REGISTRY_KV_KEY).trim();
    return configured || DEFAULT_SCALP_DEPLOYMENT_REGISTRY_KV_KEY;
}

function hasKvConfig(): boolean {
    return Boolean(process.env.upstash_payasyougo_KV_REST_API_URL && process.env.upstash_payasyougo_KV_REST_API_TOKEN);
}

let kvBootstrapWriteBlocked = false;

function isKvQuotaExceededError(err: unknown): boolean {
    const message = err instanceof Error ? err.message : String(err || '');
    const normalized = message.toLowerCase();
    return normalized.includes('max requests limit exceeded') || normalized.includes('usage:');
}

export function scalpDeploymentRegistryStoreMode(): 'kv' | 'file' {
    const configured = String(process.env.SCALP_DEPLOYMENTS_REGISTRY_STORE || 'auto')
        .trim()
        .toLowerCase();
    if (configured === 'kv') return 'kv';
    if (configured === 'file') return 'file';
    return hasKvConfig() ? 'kv' : 'file';
}

async function loadScalpDeploymentRegistryFromFile(): Promise<ScalpDeploymentRegistrySnapshot> {
    const filePath = scalpDeploymentRegistryPath();
    try {
        const raw = await readFile(filePath, 'utf8');
        return normalizeRegistrySnapshot(JSON.parse(raw));
    } catch {
        return normalizeRegistrySnapshot(null);
    }
}

async function saveScalpDeploymentRegistryToFile(snapshot: ScalpDeploymentRegistrySnapshot): Promise<void> {
    const filePath = scalpDeploymentRegistryPath();
    await mkdir(path.dirname(filePath), { recursive: true });
    await writeFile(filePath, `${JSON.stringify(snapshot, null, 2)}\n`, 'utf8');
}

async function loadRawScalpDeploymentRegistryFromFile(): Promise<unknown> {
    const filePath = scalpDeploymentRegistryPath();
    try {
        const raw = await readFile(filePath, 'utf8');
        return JSON.parse(raw);
    } catch {
        return null;
    }
}

function hasNonEmptyRegistrySnapshot(raw: unknown): boolean {
    const snapshot = normalizeRegistrySnapshot(raw);
    return snapshot.deployments.length > 0 || Boolean(snapshot.updatedAt);
}

export async function loadScalpDeploymentRegistry(): Promise<ScalpDeploymentRegistrySnapshot> {
    const storeMode = scalpDeploymentRegistryStoreMode();
    if (storeMode === 'kv') {
        const kvSnapshot = normalizeRegistrySnapshot(await kvGetJson<unknown>(scalpDeploymentRegistryKvKey()));
        if (kvSnapshot.deployments.length > 0 || kvSnapshot.updatedAt) {
            return kvSnapshot;
        }

        // First KV bootstrapping pass: seed from the repo snapshot so updates persist after deployment.
        const fileSnapshot = await loadScalpDeploymentRegistryFromFile();
        if (fileSnapshot.deployments.length > 0 || fileSnapshot.updatedAt) {
            if (!kvBootstrapWriteBlocked) {
                try {
                    await kvSetJson(scalpDeploymentRegistryKvKey(), fileSnapshot);
                } catch (err) {
                    if (isKvQuotaExceededError(err)) {
                        kvBootstrapWriteBlocked = true;
                        console.warn(
                            'KV quota exceeded while bootstrapping scalp deployment registry; using file snapshot fallback for reads.',
                        );
                    } else {
                        console.warn(
                            'Failed to bootstrap scalp deployment registry into KV; using file snapshot fallback for reads.',
                            err,
                        );
                    }
                }
            }
            return fileSnapshot;
        }
        return kvSnapshot;
    }
    return loadScalpDeploymentRegistryFromFile();
}

async function saveScalpDeploymentRegistry(snapshot: ScalpDeploymentRegistrySnapshot): Promise<void> {
    const storeMode = scalpDeploymentRegistryStoreMode();
    if (storeMode === 'kv') {
        await kvSetJson(scalpDeploymentRegistryKvKey(), snapshot);
        return;
    }
    await saveScalpDeploymentRegistryToFile(snapshot);
}

export async function canonicalizeScalpDeploymentRegistry(params: {
    dryRun?: boolean;
} = {}): Promise<CanonicalizeScalpDeploymentRegistryResult> {
    const dryRun = Boolean(params.dryRun);
    const storeMode = scalpDeploymentRegistryStoreMode();
    const registryPath = scalpDeploymentRegistryPath();
    const registryKvKey = scalpDeploymentRegistryKvKey();
    let raw: unknown = null;

    if (storeMode === 'kv') {
        const kvRaw = await kvGetJson<unknown>(registryKvKey);
        if (hasNonEmptyRegistrySnapshot(kvRaw)) {
            raw = kvRaw;
        } else {
            raw = await loadRawScalpDeploymentRegistryFromFile();
        }
    } else {
        raw = await loadRawScalpDeploymentRegistryFromFile();
    }

    const rawRows = isRecord(raw) && Array.isArray(raw.deployments) ? raw.deployments : [];
    const legacyStrategyRows = rawRows.filter((row) => isRecord(row) && isLegacyRegimeGuardStrategyId(row.strategyId)).length;
    const legacyDeploymentIdRows = rawRows.filter((row) => {
        if (!isRecord(row)) return false;
        if (hasLegacyRegimeGuardIdInDeployment(row.deploymentId)) return true;
        if (!isRecord(row.leaderboardEntry)) return false;
        return hasLegacyRegimeGuardIdInDeployment(row.leaderboardEntry.deploymentId);
    }).length;

    const beforeCount = rawRows.length;
    const normalized = normalizeRegistrySnapshot(raw);
    const afterCount = normalized.deployments.length;
    const dedupedCount = Math.max(0, beforeCount - afterCount);
    const updatedAt = new Date().toISOString();
    const snapshot: ScalpDeploymentRegistrySnapshot = {
        version: REGISTRY_VERSION,
        updatedAt,
        deployments: normalized.deployments,
    };

    if (!dryRun) {
        await saveScalpDeploymentRegistry(snapshot);
    }

    return {
        dryRun,
        storeMode,
        registryPath,
        registryKvKey,
        beforeCount,
        afterCount,
        dedupedCount,
        legacyStrategyRows,
        legacyDeploymentIdRows,
        wrote: !dryRun,
        updatedAt,
        snapshot,
    };
}

export function filterScalpDeploymentRegistry(
    snapshot: ScalpDeploymentRegistrySnapshot,
    params: { symbol?: unknown; strategyId?: unknown; tuneId?: unknown; enabled?: unknown; promotionEligible?: unknown } = {},
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
    const enabledFilter = params.enabled === undefined ? null : normalizeBool(params.enabled, true);
    const promotionEligibleFilter =
        params.promotionEligible === undefined ? null : normalizeBool(params.promotionEligible, true);
    return snapshot.deployments.filter((entry) => {
        if (symbol && entry.symbol !== symbol) return false;
        if (strategyId && entry.strategyId !== strategyId) return false;
        if (tuneId && entry.tuneId !== tuneId) return false;
        if (enabledFilter !== null && entry.enabled !== enabledFilter) return false;
        if (promotionEligibleFilter !== null && isScalpDeploymentPromotionEligible(entry) !== promotionEligibleFilter) return false;
        return true;
    });
}

export async function listScalpDeploymentRegistryEntries(
    params: { symbol?: unknown; strategyId?: unknown; tuneId?: unknown; enabled?: unknown; promotionEligible?: unknown } = {},
): Promise<ScalpDeploymentRegistryEntry[]> {
    const snapshot = await loadScalpDeploymentRegistry();
    return filterScalpDeploymentRegistry(snapshot, params);
}

function applyUpsertScalpDeploymentRegistryEntry(
    snapshot: ScalpDeploymentRegistrySnapshot,
    params: RegistryWriteParams,
): { snapshot: ScalpDeploymentRegistrySnapshot; entry: ScalpDeploymentRegistryEntry } {
    const deployment = resolveScalpDeployment({
        symbol: params.symbol,
        strategyId: params.strategyId,
        tuneId: params.tuneId,
        deploymentId: params.deploymentId,
    });
    const existing = snapshot.deployments.find((entry) => entry.deploymentId === deployment.deploymentId) || null;
    const nowMs = Date.now();
    const source = normalizeSource(params.source, existing?.source ?? 'manual');
    const thresholds = resolveForwardGateThresholds();
    const explicitPromotionGate = normalizePromotionGate(params.promotionGate);
    const forwardValidation = normalizeForwardValidation(params.forwardValidation);
    let promotionGate: ScalpDeploymentPromotionGate | null = explicitPromotionGate ?? existing?.promotionGate ?? null;

    if (forwardValidation) {
        const evaluated = evaluateForwardValidationAgainstThresholds(forwardValidation, thresholds);
        promotionGate = {
            eligible: evaluated.eligible,
            reason: evaluated.reason,
            source: 'walk_forward',
            evaluatedAtMs: nowMs,
            forwardValidation,
            thresholds,
        };
    } else if (!promotionGate && requiresForwardGate(source)) {
        promotionGate = buildPendingForwardGate(nowMs, thresholds);
    }

    const requestedEnabled = normalizeBool(params.enabled, existing?.enabled ?? true);
    const enabled =
        requiresForwardGate(source) && !allowIneligibleEnable()
            ? requestedEnabled && Boolean(promotionGate?.eligible)
            : requestedEnabled;

    const entry: ScalpDeploymentRegistryEntry = {
        ...deployment,
        enabled,
        source,
        notes: normalizeOptionalText(params.notes, 400) ?? existing?.notes ?? null,
        configOverride: normalizeConfigOverride(params.configOverride) ?? existing?.configOverride ?? null,
        leaderboardEntry: normalizeLeaderboardEntry(params.leaderboardEntry, deployment) ?? existing?.leaderboardEntry ?? null,
        promotionGate,
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
    return { snapshot: next, entry };
}

export async function upsertScalpDeploymentRegistryEntriesBulk(
    paramsList: ScalpDeploymentRegistryWriteParams[],
): Promise<{ snapshot: ScalpDeploymentRegistrySnapshot; entries: ScalpDeploymentRegistryEntry[] }> {
    const rows = Array.isArray(paramsList) ? paramsList : [];
    let snapshot = await loadScalpDeploymentRegistry();
    const entries: ScalpDeploymentRegistryEntry[] = [];

    for (const params of rows) {
        const applied = applyUpsertScalpDeploymentRegistryEntry(snapshot, params);
        snapshot = applied.snapshot;
        entries.push(applied.entry);
    }

    if (entries.length > 0) {
        await saveScalpDeploymentRegistry(snapshot);
    }
    return { snapshot, entries };
}

export async function upsertScalpDeploymentRegistryEntry(
    params: ScalpDeploymentRegistryWriteParams,
): Promise<{ snapshot: ScalpDeploymentRegistrySnapshot; entry: ScalpDeploymentRegistryEntry }> {
    const out = await upsertScalpDeploymentRegistryEntriesBulk([params]);
    const entry = out.entries[0];
    if (!entry) {
        throw new Error('scalp_deployment_registry_bulk_upsert_missing_entry');
    }
    return {
        snapshot: out.snapshot,
        entry,
    };
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
