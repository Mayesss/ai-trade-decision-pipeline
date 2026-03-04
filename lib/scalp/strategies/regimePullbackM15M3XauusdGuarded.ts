import type { ScalpReplayRuntimeConfig } from '../replay/types';
import type { ScalpStrategyConfig } from '../types';
import { buildRegimePullbackM15M3Strategy } from './regimePullbackM15M3';

export const REGIME_PULLBACK_M15_M3_XAUUSD_STRATEGY_ID = 'regime_pullback_m15_m3_xauusd';

type GuardRiskDefaults = {
    tp1ClosePct: number;
    trailAtrMult: number;
    timeStopBars: number;
};

const GOLD_SYMBOLS = new Set(['XAUUSD', 'XAUUSDT']);
const XAUUSD_GUARD_BLOCKED_HOURS_VARIANTS: Record<string, number[]> = {
    xauusd_return: [15],
    xauusd_low_dd: [15, 9],
    xauusd_high_pf: [15, 17],
    off: [],
};
const DEFAULT_BLOCKED_BERLIN_HOURS_VARIANT = 'xauusd_return';
const DEFAULT_BLOCKED_BERLIN_HOURS = XAUUSD_GUARD_BLOCKED_HOURS_VARIANTS[DEFAULT_BLOCKED_BERLIN_HOURS_VARIANT] ?? [15];
const DEFAULT_OPTIMIZED_RISK: GuardRiskDefaults = {
    tp1ClosePct: 20,
    trailAtrMult: 1.6,
    timeStopBars: 18,
};

function normalizeScalpSymbolForGuard(value: unknown): string {
    return String(value || '')
        .trim()
        .toUpperCase()
        .replace(/[^A-Z0-9]/g, '');
}

function parseBoundedNumber(value: string | undefined, fallback: number, min: number, max: number): number {
    const n = Number(value);
    if (!Number.isFinite(n)) return fallback;
    return Math.max(min, Math.min(max, n));
}

function parsePositiveNumber(value: string | undefined, fallback: number): number {
    const n = Number(value);
    if (!Number.isFinite(n) || n <= 0) return fallback;
    return n;
}

function parsePositiveInt(value: string | undefined, fallback: number): number {
    return Math.max(1, Math.floor(parsePositiveNumber(value, fallback)));
}

function parseBlockedBerlinHours(value: string | undefined, fallback: number[]): number[] {
    const raw = String(value ?? '').trim();
    if (!raw) return fallback.slice();
    const out: number[] = [];
    for (const token of raw.split(',')) {
        const hour = Math.floor(Number(token.trim()));
        if (!Number.isFinite(hour) || hour < 0 || hour > 23) continue;
        out.push(hour);
    }
    return Array.from(new Set(out)).sort((a, b) => a - b);
}

function resolveBlockedBerlinHoursFromVariant(value: string | undefined): number[] {
    const normalized = String(value ?? '')
        .trim()
        .toLowerCase();
    const variant = normalized || DEFAULT_BLOCKED_BERLIN_HOURS_VARIANT;
    const selected = XAUUSD_GUARD_BLOCKED_HOURS_VARIANTS[variant];
    if (!Array.isArray(selected)) return DEFAULT_BLOCKED_BERLIN_HOURS.slice();
    return selected.slice();
}

export function isXauusdFamilySymbol(symbol: string): boolean {
    const normalized = normalizeScalpSymbolForGuard(symbol);
    if (!normalized) return false;
    return GOLD_SYMBOLS.has(normalized);
}

export function resolveXauusdGuardBlockedBerlinHours(): number[] {
    const fromVariant = resolveBlockedBerlinHoursFromVariant(process.env.SCALP_XAUUSD_GUARD_BLOCKED_HOURS_VARIANT);
    return parseBlockedBerlinHours(process.env.SCALP_XAUUSD_GUARD_BLOCKED_HOURS_BERLIN, fromVariant);
}

export function resolveXauusdGuardOptimizedRiskDefaults(): GuardRiskDefaults {
    return {
        tp1ClosePct: parseBoundedNumber(
            process.env.SCALP_XAUUSD_GUARD_TP1_CLOSE_PCT,
            DEFAULT_OPTIMIZED_RISK.tp1ClosePct,
            0,
            100,
        ),
        trailAtrMult: parsePositiveNumber(
            process.env.SCALP_XAUUSD_GUARD_TRAIL_ATR_MULT,
            DEFAULT_OPTIMIZED_RISK.trailAtrMult,
        ),
        timeStopBars: parsePositiveInt(
            process.env.SCALP_XAUUSD_GUARD_TIME_STOP_BARS,
            DEFAULT_OPTIMIZED_RISK.timeStopBars,
        ),
    };
}

function applyRiskDefaultsToReplayRuntime(
    runtime: ScalpReplayRuntimeConfig,
    defaults: GuardRiskDefaults,
): ScalpReplayRuntimeConfig {
    if (
        runtime.strategy.tp1ClosePct === defaults.tp1ClosePct &&
        runtime.strategy.trailAtrMult === defaults.trailAtrMult &&
        runtime.strategy.timeStopBars === defaults.timeStopBars
    ) {
        return runtime;
    }
    return {
        ...runtime,
        strategy: {
            ...runtime.strategy,
            tp1ClosePct: defaults.tp1ClosePct,
            trailAtrMult: defaults.trailAtrMult,
            timeStopBars: defaults.timeStopBars,
        },
    };
}

export function applyXauusdGuardRiskDefaultsToReplayRuntime(runtime: ScalpReplayRuntimeConfig): ScalpReplayRuntimeConfig {
    if (!isXauusdFamilySymbol(runtime.symbol)) return runtime;
    if (runtime.strategyId !== REGIME_PULLBACK_M15_M3_XAUUSD_STRATEGY_ID) return runtime;
    return applyRiskDefaultsToReplayRuntime(runtime, resolveXauusdGuardOptimizedRiskDefaults());
}

export function applyXauusdGuardRiskDefaultsToStrategyConfig(params: {
    cfg: ScalpStrategyConfig;
    symbol: string;
    strategyId: string;
}): ScalpStrategyConfig {
    const { cfg, symbol, strategyId } = params;
    if (!isXauusdFamilySymbol(symbol)) return cfg;
    if (strategyId !== REGIME_PULLBACK_M15_M3_XAUUSD_STRATEGY_ID) return cfg;
    const defaults = resolveXauusdGuardOptimizedRiskDefaults();
    if (
        cfg.risk.tp1ClosePct === defaults.tp1ClosePct &&
        cfg.risk.trailAtrMult === defaults.trailAtrMult &&
        cfg.risk.timeStopBars === defaults.timeStopBars
    ) {
        return cfg;
    }
    return {
        ...cfg,
        risk: {
            ...cfg.risk,
            tp1ClosePct: defaults.tp1ClosePct,
            trailAtrMult: defaults.trailAtrMult,
            timeStopBars: defaults.timeStopBars,
        },
    };
}

export const regimePullbackM15M3XauusdGuardedStrategy = buildRegimePullbackM15M3Strategy({
    id: REGIME_PULLBACK_M15_M3_XAUUSD_STRATEGY_ID,
    shortName: 'Regime Pullback XAUUSD',
    longName: 'Regime-Filtered Trend Pullback Continuation (M15/M3, XAUUSD Guarded)',
    allowPullbackSwingBreakTrigger: false,
    blockedBerlinEntryHours: resolveXauusdGuardBlockedBerlinHours(),
});
