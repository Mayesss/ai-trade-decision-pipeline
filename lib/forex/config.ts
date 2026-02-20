import universeConfig from '../../data/forexUniverse.json';

const DEFAULT_NOTIONAL_USD = 100;

function toPositiveNumber(value: string | undefined, fallback: number): number {
    const n = Number(value);
    if (!Number.isFinite(n) || n <= 0) return fallback;
    return n;
}

function toPositiveInt(value: string | undefined, fallback: number): number {
    return Math.floor(toPositiveNumber(value, fallback));
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

export function getForexStrategyConfig() {
    const spreadPipsCapByPair = parsePairNumberMap(process.env.FOREX_SPREAD_PIPS_CAP_BY_PAIR);
    const regimeMinutes = toPositiveInt(process.env.FOREX_REGIME_MINUTES, 60);

    return {
        capitalOnly: true,
        defaultNotionalUsd: toPositiveNumber(process.env.FOREX_DEFAULT_NOTIONAL_USD, DEFAULT_NOTIONAL_USD),
        dryRunDefault: toBool(process.env.FOREX_DRY_RUN_DEFAULT, true),
        cadence: {
            executeMinutes: toPositiveInt(process.env.FOREX_EXECUTE_MINUTES, 5),
            scanMinutes: toPositiveInt(process.env.FOREX_SCAN_MINUTES, 30),
            regimeMinutes,
        },
        selector: {
            maxSpreadToAtr1h: toPositiveNumber(process.env.FOREX_MAX_SPREAD_TO_ATR1H, 0.12),
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
            lockMinutes: toPositiveInt(process.env.FOREX_REENTRY_LOCK_MINUTES, 5),
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
            shockCandleAtr5m: toPositiveNumber(process.env.FOREX_SHOCK_CANDLE_ATR5M, 2.2),
            shockCooldownMinutes: toPositiveInt(process.env.FOREX_SHOCK_COOLDOWN_MINUTES, 30),
            maxCurrencyExposure: toPositiveInt(process.env.FOREX_MAX_CURRENCY_EXPOSURE, 2),
            riskPerTradePct: toPositiveNumber(process.env.FOREX_RISK_PER_TRADE_PCT, 0.5),
            referenceEquityUsd: toPositiveNumber(process.env.FOREX_RISK_REFERENCE_EQUITY_USD, NaN),
            maxPortfolioOpenPct: toPositiveNumber(process.env.FOREX_RISK_MAX_PORTFOLIO_OPEN_PCT, 2.0),
            maxCurrencyOpenPct: toPositiveNumber(process.env.FOREX_RISK_MAX_CURRENCY_OPEN_PCT, 1.0),
            maxLeveragePerPair: toPositiveInt(process.env.FOREX_MAX_LEVERAGE_PER_PAIR, 3),
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
