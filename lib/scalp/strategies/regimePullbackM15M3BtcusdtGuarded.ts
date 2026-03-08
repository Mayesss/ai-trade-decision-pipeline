import type { ScalpReplayRuntimeConfig } from '../replay/types';
import type { ScalpStrategyConfig } from '../types';

export const REGIME_PULLBACK_M15_M3_BTCUSDT_STRATEGY_ID = 'regime_pullback_m15_m3_btcusdt';

type GuardRiskDefaults = {
    tp1ClosePct: number;
    trailAtrMult: number;
    timeStopBars: number;
};

const BTC_SYMBOLS = new Set(['BTCUSD', 'BTCUSDT']);
const BTCUSDT_GUARD_BLOCKED_HOURS_VARIANTS: Record<string, number[]> = {
    btcusdt_return: [],
    btcusdt_low_dd: [10],
    btcusdt_high_pf: [10, 11],
    off: [],
};
const DEFAULT_BLOCKED_BERLIN_HOURS_VARIANT = 'off';
const EXPERIMENTAL_BLOCKED_BERLIN_HOURS_VARIANT = 'btcusdt_high_pf';
const DEFAULT_BLOCKED_BERLIN_HOURS =
    BTCUSDT_GUARD_BLOCKED_HOURS_VARIANTS[DEFAULT_BLOCKED_BERLIN_HOURS_VARIANT] ?? [];
const DEFAULT_OPTIMIZED_RISK: GuardRiskDefaults = {
    tp1ClosePct: 20,
    trailAtrMult: 1.4,
    timeStopBars: 15,
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

function parseBoolFlag(value: string | undefined, fallback: boolean): boolean {
    const normalized = String(value ?? '')
        .trim()
        .toLowerCase();
    if (!normalized) return fallback;
    if (['1', 'true', 'yes', 'on'].includes(normalized)) return true;
    if (['0', 'false', 'no', 'off'].includes(normalized)) return false;
    return fallback;
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
    const experimentEnabled = parseBoolFlag(process.env.SCALP_BTCUSDT_GUARD_BLOCKED_HOURS_EXPERIMENT, false);
    const fallbackVariant = experimentEnabled ? EXPERIMENTAL_BLOCKED_BERLIN_HOURS_VARIANT : DEFAULT_BLOCKED_BERLIN_HOURS_VARIANT;
    const variant = normalized || fallbackVariant;
    const selected = BTCUSDT_GUARD_BLOCKED_HOURS_VARIANTS[variant];
    if (!Array.isArray(selected)) {
        return (BTCUSDT_GUARD_BLOCKED_HOURS_VARIANTS[fallbackVariant] ?? DEFAULT_BLOCKED_BERLIN_HOURS).slice();
    }
    return selected.slice();
}

export function isBtcusdFamilySymbol(symbol: string): boolean {
    const normalized = normalizeScalpSymbolForGuard(symbol);
    if (!normalized) return false;
    return BTC_SYMBOLS.has(normalized);
}

export function resolveBtcusdtGuardBlockedBerlinHours(): number[] {
    const fromVariant = resolveBlockedBerlinHoursFromVariant(process.env.SCALP_BTCUSDT_GUARD_BLOCKED_HOURS_VARIANT);
    return parseBlockedBerlinHours(process.env.SCALP_BTCUSDT_GUARD_BLOCKED_HOURS_BERLIN, fromVariant);
}

export function resolveBtcusdtGuardOptimizedRiskDefaults(): GuardRiskDefaults {
    return {
        tp1ClosePct: parseBoundedNumber(
            process.env.SCALP_BTCUSDT_GUARD_TP1_CLOSE_PCT,
            DEFAULT_OPTIMIZED_RISK.tp1ClosePct,
            0,
            100,
        ),
        trailAtrMult: parsePositiveNumber(
            process.env.SCALP_BTCUSDT_GUARD_TRAIL_ATR_MULT,
            DEFAULT_OPTIMIZED_RISK.trailAtrMult,
        ),
        timeStopBars: parsePositiveInt(
            process.env.SCALP_BTCUSDT_GUARD_TIME_STOP_BARS,
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

export function applyBtcusdtGuardRiskDefaultsToReplayRuntime(runtime: ScalpReplayRuntimeConfig): ScalpReplayRuntimeConfig {
    if (!isBtcusdFamilySymbol(runtime.symbol)) return runtime;
    if (runtime.strategyId !== REGIME_PULLBACK_M15_M3_BTCUSDT_STRATEGY_ID) return runtime;
    return applyRiskDefaultsToReplayRuntime(runtime, resolveBtcusdtGuardOptimizedRiskDefaults());
}

export function applyBtcusdtGuardRiskDefaultsToStrategyConfig(params: {
    cfg: ScalpStrategyConfig;
    symbol: string;
    strategyId: string;
}): ScalpStrategyConfig {
    const { cfg, symbol, strategyId } = params;
    if (!isBtcusdFamilySymbol(symbol)) return cfg;
    if (strategyId !== REGIME_PULLBACK_M15_M3_BTCUSDT_STRATEGY_ID) return cfg;
    const defaults = resolveBtcusdtGuardOptimizedRiskDefaults();
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
