import universeConfig from '../../data/forexUniverse.json';

const DEFAULT_NOTIONAL_USD = 850;
const SESSION_TRANSITION_HOURS_UTC = [0, 7, 12, 16, 21] as const;

function toPositiveNumber(value: string | undefined, fallback: number): number {
    const n = Number(value);
    if (!Number.isFinite(n) || n <= 0) return fallback;
    return n;
}

function toNonNegativeNumber(value: string | undefined, fallback: number): number {
    const n = Number(value);
    if (!Number.isFinite(n) || n < 0) return fallback;
    return n;
}

function toPositiveInt(value: string | undefined, fallback: number): number {
    return Math.floor(toPositiveNumber(value, fallback));
}

function toNonNegativeInt(value: string | undefined, fallback: number): number {
    const n = Number(value);
    if (!Number.isFinite(n) || n < 0) return Math.floor(fallback);
    return Math.floor(n);
}

function toBool(value: string | undefined, fallback: boolean): boolean {
    if (value === undefined) return fallback;
    const raw = value.trim().toLowerCase();
    if (['1', 'true', 'yes', 'on'].includes(raw)) return true;
    if (['0', 'false', 'no', 'off'].includes(raw)) return false;
    return fallback;
}

function parseCsvUpper(raw: string | undefined, fallback: string[]): string[] {
    if (!raw) return fallback.slice();
    const values = String(raw)
        .split(',')
        .map((item) => item.trim().toUpperCase())
        .filter((item) => item.length > 0);
    return values.length ? values : fallback.slice();
}

function parsePairNumberMap(raw: string | undefined): Record<string, number> {
    if (!raw) return {};
    try {
        const parsed = JSON.parse(raw);
        if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) return {};
        const out: Record<string, number> = {};
        for (const [key, value] of Object.entries(parsed as Record<string, unknown>)) {
            const n = Number(value);
            if (!Number.isFinite(n) || n <= 0) continue;
            out[key.toUpperCase()] = n;
        }
        return out;
    } catch {
        return {};
    }
}

function toUtcHour(value: string | undefined, fallback: number): number {
    const n = Number(value);
    if (!Number.isFinite(n)) return Math.max(0, Math.min(23, Math.floor(fallback)));
    return Math.max(0, Math.min(23, Math.floor(n)));
}

function parseRolloverForceCloseMode(value: string | undefined): 'close' | 'derisk' {
    const normalized = String(value || '')
        .trim()
        .toLowerCase();
    if (normalized === 'derisk') return 'derisk';
    return 'close';
}

export function getForexUniversePairs(): string[] {
    const fromFile = Array.isArray((universeConfig as any)?.pairs)
        ? (universeConfig as any).pairs.map((p: any) => String(p).trim().toUpperCase()).filter((p: string) => p.length > 0)
        : [];
    const override = String(process.env.FOREX_UNIVERSE_PAIRS || '')
        .split(',')
        .map((p) => p.trim().toUpperCase())
        .filter((p) => p.length > 0);

    const merged = override.length ? override : fromFile;
    return Array.from(new Set(merged));
}

export function pipSizeForPair(pair: string): number {
    const normalized = String(pair || '').toUpperCase();
    if (normalized.includes('JPY')) return 0.01;
    return 0.0001;
}

function minuteDistanceInUtcDay(a: number, b: number): number {
    const delta = Math.abs(a - b) % 1440;
    return Math.min(delta, 1440 - delta);
}

export function isWithinSessionTransitionBuffer(nowMs = Date.now(), bufferMinutes = 0): boolean {
    const buffer = Math.max(0, Math.floor(Number(bufferMinutes) || 0));
    if (buffer <= 0) return false;
    const date = new Date(nowMs);
    const minuteOfDay = date.getUTCHours() * 60 + date.getUTCMinutes();
    return SESSION_TRANSITION_HOURS_UTC.some((hour) => minuteDistanceInUtcDay(minuteOfDay, hour * 60) <= buffer);
}

export function tightenSpreadToAtrCap(baseCap: number, multiplier: number): number {
    const cap = Number(baseCap);
    if (!Number.isFinite(cap) || cap <= 0) return cap;
    const mul = Number(multiplier);
    if (!Number.isFinite(mul) || mul <= 0) return cap;
    return cap * Math.min(1, mul);
}

export function getForexStrategyConfig() {
    const spreadPipsCapByPair = parsePairNumberMap(process.env.FOREX_SPREAD_PIPS_CAP_BY_PAIR);
    const executeMinutes = toPositiveInt(process.env.FOREX_EXECUTE_MINUTES, 5);
    const scanMinutes = toPositiveInt(process.env.FOREX_SCAN_MINUTES, 30);
    const regimeMinutes = toPositiveInt(process.env.FOREX_REGIME_MINUTES, 60);
    const baseReentryLockMinutes = toPositiveInt(process.env.FOREX_REENTRY_LOCK_MINUTES, 5);
    const lockMinutesRegimeFlip = toPositiveInt(
        process.env.FOREX_REENTRY_LOCK_MINUTES_REGIME_FLIP,
        Math.max(baseReentryLockMinutes, executeMinutes * 2),
    );
    const lockMinutesEventRisk = toPositiveInt(
        process.env.FOREX_REENTRY_LOCK_MINUTES_EVENT_RISK,
        Math.max(lockMinutesRegimeFlip, executeMinutes * 4),
    );
    const lockMinutesTimeStop = toPositiveInt(
        process.env.FOREX_REENTRY_LOCK_MINUTES_TIME_STOP,
        Math.max(1, executeMinutes),
    );
    const lockMinutesStopInvalidated = toNonNegativeInt(
        process.env.FOREX_REENTRY_LOCK_MINUTES_STOP_INVALIDATED,
        0,
    );
    const lockMinutesStopInvalidatedStress = toNonNegativeInt(
        process.env.FOREX_REENTRY_LOCK_MINUTES_STOP_INVALIDATED_STRESS,
        lockMinutesStopInvalidated > 0 ? lockMinutesStopInvalidated * 2 : 0,
    );

    return {
        capitalOnly: true,
        defaultNotionalUsd: toPositiveNumber(process.env.FOREX_DEFAULT_NOTIONAL_USD, DEFAULT_NOTIONAL_USD),
        dryRunDefault: toBool(process.env.FOREX_DRY_RUN_DEFAULT, true),
        cadence: {
            executeMinutes,
            scanMinutes,
            regimeMinutes,
        },
        selector: {
            maxSpreadToAtr1h: toPositiveNumber(process.env.FOREX_MAX_SPREAD_TO_ATR1H, 0.12),
            sessionTransitionBufferMinutes: toPositiveInt(process.env.FOREX_SESSION_TRANSITION_BUFFER_MINUTES, 20),
            transitionSpreadToAtrMultiplier: toPositiveNumber(
                process.env.FOREX_SELECTOR_TRANSITION_SPREAD_TO_ATR_MULTIPLIER,
                0.8,
            ),
            minAtr1hPercent: toPositiveNumber(process.env.FOREX_MIN_ATR1H_PERCENT, 0.0004),
            minScore: toPositiveNumber(process.env.FOREX_MIN_SELECTOR_SCORE, 0.1),
            topPercent: Math.max(1, Math.min(100, toPositiveInt(process.env.FOREX_SELECTOR_TOP_PERCENT, 40))),
        },
        packet: {
            staleMinutes: toPositiveInt(process.env.FOREX_PACKET_STALE_MINUTES, 120),
        },
        timeStop: {
            noFollowBars: toPositiveInt(process.env.FOREX_TIME_STOP_NO_FOLLOW_BARS, 18),
            minFollowR: toPositiveNumber(process.env.FOREX_TIME_STOP_MIN_FOLLOW_R, 0.3),
            maxHoldHours: toPositiveNumber(process.env.FOREX_TIME_STOP_MAX_HOLD_HOURS, 10),
        },
        reentry: {
            lockMinutes: baseReentryLockMinutes,
            lockMinutesTimeStop,
            lockMinutesRegimeFlip,
            lockMinutesEventRisk,
            lockMinutesStopInvalidated,
            lockMinutesStopInvalidatedStress,
        },
        events: {
            forceCloseImpacts: parseCsvUpper(process.env.FOREX_EVENT_FORCE_CLOSE_IMPACTS, ['HIGH']),
            blockNewImpacts: parseCsvUpper(process.env.FOREX_EVENT_BLOCK_NEW_IMPACTS, ['HIGH', 'MEDIUM']),
            tightenOnlyImpacts: parseCsvUpper(process.env.FOREX_EVENT_TIGHTEN_ONLY_IMPACTS, ['MEDIUM']),
        },
        risk: {
            spreadPipsCapDefault: toPositiveNumber(process.env.FOREX_SPREAD_PIPS_CAP_DEFAULT, 3.5),
            spreadPipsCapByPair,
            maxSpreadToAtr1h: toPositiveNumber(process.env.FOREX_RISK_MAX_SPREAD_TO_ATR1H, 0.15),
            sessionTransitionBufferMinutes: toPositiveInt(process.env.FOREX_RISK_SESSION_TRANSITION_BUFFER_MINUTES, 20),
            transitionSpreadToAtrMultiplier: toPositiveNumber(
                process.env.FOREX_RISK_TRANSITION_SPREAD_TO_ATR_MULTIPLIER,
                0.75,
            ),
            shockCandleAtr5m: toPositiveNumber(process.env.FOREX_SHOCK_CANDLE_ATR5M, 2.2),
            shockCooldownMinutes: toPositiveInt(process.env.FOREX_SHOCK_COOLDOWN_MINUTES, 30),
            maxCurrencyExposure: toPositiveInt(process.env.FOREX_MAX_CURRENCY_EXPOSURE, 2),
            riskPerTradePct: toPositiveNumber(process.env.FOREX_RISK_PER_TRADE_PCT, 0.5),
            referenceEquityUsd: toPositiveNumber(process.env.FOREX_RISK_REFERENCE_EQUITY_USD, NaN),
            maxPortfolioOpenPct: toPositiveNumber(process.env.FOREX_RISK_MAX_PORTFOLIO_OPEN_PCT, 2.0),
            maxCurrencyOpenPct: toPositiveNumber(process.env.FOREX_RISK_MAX_CURRENCY_OPEN_PCT, 1.0),
            maxLeveragePerPair: toPositiveInt(process.env.FOREX_MAX_LEVERAGE_PER_PAIR, 3),
            rolloverHourUtc: toUtcHour(process.env.FOREX_ROLLOVER_UTC_HOUR, 0),
            rolloverEntryBlockMinutes: toNonNegativeInt(process.env.FOREX_ROLLOVER_ENTRY_BLOCK_MINUTES, 45),
            rolloverForceCloseMinutes: toNonNegativeInt(process.env.FOREX_ROLLOVER_FORCE_CLOSE_MINUTES, 0),
            rolloverForceCloseSpreadToAtr1hMin: toPositiveNumber(
                process.env.FOREX_ROLLOVER_FORCE_CLOSE_SPREAD_TO_ATR1H_MIN,
                0.12,
            ),
            rolloverForceCloseMode: parseRolloverForceCloseMode(process.env.FOREX_ROLLOVER_FORCE_CLOSE_MODE),
            rolloverDeriskWinnerMfeRMin: toNonNegativeNumber(process.env.FOREX_ROLLOVER_DERISK_WINNER_MFE_R_MIN, 0.8),
            rolloverDeriskLoserCloseRMax: Number.isFinite(Number(process.env.FOREX_ROLLOVER_DERISK_LOSER_CLOSE_R_MAX))
                ? Number(process.env.FOREX_ROLLOVER_DERISK_LOSER_CLOSE_R_MAX)
                : 0.2,
            rolloverDeriskPartialClosePct: Math.max(
                0,
                Math.min(100, toNonNegativeNumber(process.env.FOREX_ROLLOVER_DERISK_PARTIAL_CLOSE_PCT, 50)),
            ),
        },
        modules: {
            pullbackAtrBuffer: toPositiveNumber(process.env.FOREX_PULLBACK_ATR5M_BUFFER, 0.4),
            breakoutAtrBuffer: toPositiveNumber(process.env.FOREX_BREAKOUT_ATR5M_BUFFER, 0.35),
            rangeFadeBoundaryAtrBuffer: toPositiveNumber(process.env.FOREX_RANGE_FADE_ATR5M_BUFFER, 0.35),
            rangeFadeMinWidthAtr1h: toPositiveNumber(process.env.FOREX_RANGE_FADE_MIN_WIDTH_ATR1H, 1.5),
            rangeFadeMaxTrendStrength: toPositiveNumber(process.env.FOREX_RANGE_FADE_MAX_TREND_STRENGTH, 1.0),
            rangeFadeMinChopScore: toPositiveNumber(process.env.FOREX_RANGE_FADE_MIN_CHOP_SCORE, 0.3),
            rangeFadeBreakoutAtr5m: toPositiveNumber(process.env.FOREX_RANGE_FADE_BREAKOUT_ATR5M, 2.2),
            rangeFadeKillSwitchCooldownMinutes: toPositiveInt(
                process.env.FOREX_RANGE_FADE_KILL_SWITCH_COOLDOWN_MINUTES,
                regimeMinutes,
            ),
            maxSignalsPerPairPerRun: toPositiveInt(process.env.FOREX_MAX_SIGNALS_PER_PAIR_PER_RUN, 1),
        },
        htf: {
            supportResistanceLookbackBars: toPositiveInt(process.env.FOREX_HTF_SR_LOOKBACK_BARS, 120),
        },
    };
}

export function spreadPipsCapForPair(pair: string): number {
    const cfg = getForexStrategyConfig();
    const key = String(pair || '').toUpperCase();
    return cfg.risk.spreadPipsCapByPair[key] ?? cfg.risk.spreadPipsCapDefault;
}
