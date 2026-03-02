import type {
    ScalpBaseTimeframe,
    ScalpClockMode,
    ScalpConfirmTimeframe,
    ScalpFvgEntryMode,
    ScalpStrategyConfig,
} from './types';

const DEFAULT_SCALP_SYMBOL = 'EURUSD';

export type DeepPartial<T> = {
    [K in keyof T]?: T[K] extends any[]
        ? T[K]
        : T[K] extends object
          ? DeepPartial<T[K]>
          : T[K];
};

export type ScalpStrategyConfigOverride = DeepPartial<ScalpStrategyConfig>;

function toPositiveNumber(value: string | undefined, fallback: number): number {
    const n = Number(value);
    if (!Number.isFinite(n) || n <= 0) return fallback;
    return n;
}

function toPositiveInt(value: string | undefined, fallback: number): number {
    return Math.max(1, Math.floor(toPositiveNumber(value, fallback)));
}

function toNonNegativeNumber(value: string | undefined, fallback: number): number {
    const n = Number(value);
    if (!Number.isFinite(n) || n < 0) return fallback;
    return n;
}

function toNonNegativeInt(value: string | undefined, fallback: number): number {
    return Math.max(0, Math.floor(toNonNegativeNumber(value, fallback)));
}

function toBool(value: string | undefined, fallback: boolean): boolean {
    if (value === undefined) return fallback;
    const normalized = String(value).trim().toLowerCase();
    if (['1', 'true', 'yes', 'on'].includes(normalized)) return true;
    if (['0', 'false', 'no', 'off'].includes(normalized)) return false;
    return fallback;
}

function parseClockMode(value: string | undefined): ScalpClockMode {
    const normalized = String(value || '')
        .trim()
        .toUpperCase();
    return normalized === 'UTC_FIXED' ? 'UTC_FIXED' : 'LONDON_TZ';
}

function normalizeClockLabel(value: string | undefined, fallback: string): string {
    const normalized = String(value || '')
        .trim()
        .replace(/\s+/g, '');
    if (!normalized) return fallback;
    const match = normalized.match(/^([01]?\d|2[0-3]):([0-5]\d)$/);
    if (!match) return fallback;
    const hh = String(match[1]).padStart(2, '0');
    const mm = String(match[2]).padStart(2, '0');
    return `${hh}:${mm}`;
}

function parseAsiaTf(value: string | undefined): ScalpBaseTimeframe {
    const normalized = String(value || '')
        .trim()
        .toUpperCase();
    if (normalized === 'M1' || normalized === 'M3' || normalized === 'M5' || normalized === 'M15') {
        return normalized;
    }
    return 'M5';
}

function parseConfirmTf(value: string | undefined): ScalpConfirmTimeframe {
    const normalized = String(value || '')
        .trim()
        .toUpperCase();
    if (normalized === 'M1' || normalized === 'M3') return normalized;
    return 'M3';
}

function parseFvgEntryMode(value: string | undefined): ScalpFvgEntryMode {
    const normalized = String(value || '')
        .trim()
        .toLowerCase();
    if (normalized === 'first_touch' || normalized === 'midline_touch' || normalized === 'full_fill') {
        return normalized;
    }
    return 'midline_touch';
}

export function normalizeScalpSymbol(value: string | undefined): string {
    const normalized = String(value || '')
        .trim()
        .toUpperCase();
    if (!normalized) return DEFAULT_SCALP_SYMBOL;
    return normalized.replace(/[^A-Z0-9._-]/g, '');
}

export function getScalpStrategyConfig(): ScalpStrategyConfig {
    const sessionTtlDays = toPositiveInt(process.env.SCALP_STATE_TTL_DAYS, 3);
    return {
        enabled: toBool(process.env.SCALP_ENABLED, true),
        defaultSymbol: normalizeScalpSymbol(process.env.SCALP_DEFAULT_SYMBOL),
        dryRunDefault: toBool(process.env.SCALP_DRY_RUN_DEFAULT, true),
        cadence: {
            executeMinutes: toPositiveInt(process.env.SCALP_EXECUTE_MINUTES, 3),
        },
        sessions: {
            clockMode: parseClockMode(process.env.SCALP_SESSION_CLOCK_MODE),
            asiaWindowLocal: [
                normalizeClockLabel(process.env.SCALP_ASIA_WINDOW_START_LOCAL, '00:00'),
                normalizeClockLabel(process.env.SCALP_ASIA_WINDOW_END_LOCAL, '06:00'),
            ],
            raidWindowLocal: [
                normalizeClockLabel(process.env.SCALP_RAID_WINDOW_START_LOCAL, '07:00'),
                normalizeClockLabel(process.env.SCALP_RAID_WINDOW_END_LOCAL, '10:00'),
            ],
        },
        timeframes: {
            asiaBase: parseAsiaTf(process.env.SCALP_ASIA_BASE_TF),
            confirm: parseConfirmTf(process.env.SCALP_CONFIRM_TF),
        },
        sweep: {
            bufferPips: toNonNegativeNumber(process.env.SCALP_SWEEP_BUFFER_PIPS, 1.0),
            bufferAtrMult: toNonNegativeNumber(process.env.SCALP_SWEEP_BUFFER_ATR_MULT, 0.08),
            bufferSpreadMult: toNonNegativeNumber(process.env.SCALP_SWEEP_BUFFER_SPREAD_MULT, 1.2),
            rejectInsidePips: toNonNegativeNumber(process.env.SCALP_SWEEP_REJECT_INSIDE_PIPS, 0),
            rejectMaxBars: toPositiveInt(process.env.SCALP_SWEEP_REJECT_MAX_BARS, 3),
            minWickBodyRatio: toNonNegativeNumber(process.env.SCALP_SWEEP_MIN_WICK_BODY_RATIO, 1.2),
        },
        confirm: {
            displacementBodyAtrMult: toNonNegativeNumber(process.env.SCALP_DISPLACEMENT_BODY_ATR_MULT, 1.1),
            displacementRangeAtrMult: toNonNegativeNumber(process.env.SCALP_DISPLACEMENT_RANGE_ATR_MULT, 1.6),
            closeInExtremePct: Math.max(
                0.01,
                Math.min(0.49, toPositiveNumber(process.env.SCALP_DISPLACEMENT_CLOSE_IN_EXTREME_PCT, 0.25)),
            ),
            mssLookbackBars: toPositiveInt(process.env.SCALP_MSS_LOOKBACK_BARS, 8),
            mssBreakBufferPips: toNonNegativeNumber(process.env.SCALP_MSS_BREAK_BUFFER_PIPS, 0.3),
            mssBreakBufferAtrMult: toNonNegativeNumber(process.env.SCALP_MSS_BREAK_BUFFER_ATR_MULT, 0),
            ttlMinutes: toPositiveInt(process.env.SCALP_CONFIRM_TTL_MINUTES, 45),
        },
        ifvg: {
            minAtrMult: toNonNegativeNumber(process.env.SCALP_IFVG_MIN_ATR_MULT, 0.1),
            maxAtrMult: toPositiveNumber(process.env.SCALP_IFVG_MAX_ATR_MULT, 0.8),
            ttlMinutes: toPositiveInt(process.env.SCALP_IFVG_TTL_MINUTES, 90),
            entryMode: parseFvgEntryMode(process.env.SCALP_IFVG_ENTRY_MODE),
        },
        risk: {
            cooldownAfterLossMinutes: toPositiveInt(process.env.SCALP_COOLDOWN_AFTER_LOSS_MINUTES, 90),
            maxTradesPerSymbolPerDay: toPositiveInt(process.env.SCALP_MAX_TRADES_PER_SYMBOL_PER_DAY, 2),
            maxOpenPositionsPerSymbol: toPositiveInt(process.env.SCALP_MAX_OPEN_POSITIONS_PER_SYMBOL, 1),
            killSwitch: toBool(process.env.SCALP_KILL_SWITCH, false),
            riskPerTradePct: toPositiveNumber(process.env.SCALP_RISK_PER_TRADE_PCT, 0.35),
            referenceEquityUsd: toPositiveNumber(process.env.SCALP_REFERENCE_EQUITY_USD, 10_000),
            minNotionalUsd: toPositiveNumber(process.env.SCALP_MIN_NOTIONAL_USD, 100),
            maxNotionalUsd: toPositiveNumber(process.env.SCALP_MAX_NOTIONAL_USD, 2_000),
            takeProfitR: toPositiveNumber(process.env.SCALP_TAKE_PROFIT_R, 2),
            stopBufferPips: toNonNegativeNumber(process.env.SCALP_STOP_BUFFER_PIPS, 0.8),
            stopBufferSpreadMult: toNonNegativeNumber(process.env.SCALP_STOP_BUFFER_SPREAD_MULT, 1.0),
            minStopDistancePips: toPositiveNumber(process.env.SCALP_MIN_STOP_DISTANCE_PIPS, 0.5),
        },
        execution: {
            liveEnabled: toBool(process.env.SCALP_LIVE_ENABLED, false),
            entryOrderType: String(process.env.SCALP_ENTRY_ORDER_TYPE || '')
                .trim()
                .toUpperCase() === 'LIMIT'
                ? 'LIMIT'
                : 'MARKET',
            defaultLeverage: Math.max(1, Math.min(5, toPositiveInt(process.env.SCALP_DEFAULT_LEVERAGE, 1))),
        },
        idempotency: {
            runLockSeconds: toPositiveInt(process.env.SCALP_RUN_LOCK_SECONDS, 90),
        },
        storage: {
            sessionTtlSeconds: sessionTtlDays * 24 * 60 * 60,
            journalMax: toPositiveInt(process.env.SCALP_JOURNAL_MAX, 500),
        },
        data: {
            atrPeriod: toPositiveInt(process.env.SCALP_ATR_PERIOD, 14),
            minAsiaCandles: toPositiveInt(process.env.SCALP_MIN_ASIA_CANDLES, 12),
            minBaseCandles: toPositiveInt(process.env.SCALP_MIN_BASE_CANDLES, 220),
            minConfirmCandles: toPositiveInt(process.env.SCALP_MIN_CONFIRM_CANDLES, 320),
            maxCandlesPerRequest: Math.max(200, Math.min(1000, toPositiveInt(process.env.SCALP_MAX_CANDLES_PER_REQUEST, 1000))),
        },
    };
}

function isPlainObject(value: unknown): value is Record<string, unknown> {
    return Boolean(value) && typeof value === 'object' && !Array.isArray(value);
}

function deepMergeMutable(
    target: Record<string, unknown>,
    source: Record<string, unknown>,
): Record<string, unknown> {
    for (const [key, rawValue] of Object.entries(source)) {
        if (rawValue === undefined) continue;
        const targetValue = target[key];
        if (Array.isArray(rawValue)) {
            target[key] = rawValue.slice();
            continue;
        }
        if (isPlainObject(rawValue) && isPlainObject(targetValue)) {
            target[key] = deepMergeMutable(
                { ...targetValue },
                rawValue,
            );
            continue;
        }
        target[key] = rawValue;
    }
    return target;
}

export function applyScalpStrategyConfigOverride(
    base: ScalpStrategyConfig,
    override?: ScalpStrategyConfigOverride,
): ScalpStrategyConfig {
    if (!override || !isPlainObject(override)) return base;
    const cloned = JSON.parse(JSON.stringify(base)) as ScalpStrategyConfig;
    return deepMergeMutable(
        cloned as unknown as Record<string, unknown>,
        override as unknown as Record<string, unknown>,
    ) as unknown as ScalpStrategyConfig;
}
