type ForexMarketHoursConfig = {
    fridayCloseHourUtc: number;
    sundayOpenHourUtc: number;
};

export type ForexMarketGateState = {
    marketClosed: boolean;
    reasonCode: 'MARKET_OPEN' | 'MARKET_CLOSED_WEEKEND';
    reopensAtMs: number | null;
    config: ForexMarketHoursConfig;
};

function toHour(value: string | undefined, fallback: number): number {
    const n = Number(value);
    if (!Number.isFinite(n)) return fallback;
    const rounded = Math.floor(n);
    if (rounded < 0 || rounded > 23) return fallback;
    return rounded;
}

export function getForexMarketHoursConfig(): ForexMarketHoursConfig {
    return {
        fridayCloseHourUtc: toHour(process.env.FOREX_MARKET_CLOSE_FRI_UTC_HOUR, 22),
        sundayOpenHourUtc: toHour(process.env.FOREX_MARKET_OPEN_SUN_UTC_HOUR, 22),
    };
}

function nextSundayAtHourUtc(fromMs: number, hourUtc: number): number {
    const date = new Date(fromMs);
    const day = date.getUTCDay();
    const daysUntilSunday = day === 0 ? 0 : 7 - day;
    const sunday = new Date(
        Date.UTC(
            date.getUTCFullYear(),
            date.getUTCMonth(),
            date.getUTCDate() + daysUntilSunday,
            hourUtc,
            0,
            0,
            0,
        ),
    );
    if (sunday.getTime() <= fromMs) {
        sunday.setUTCDate(sunday.getUTCDate() + 7);
    }
    return sunday.getTime();
}

export function evaluateForexMarketGate(nowMs = Date.now()): ForexMarketGateState {
    const config = getForexMarketHoursConfig();
    const now = new Date(nowMs);
    const day = now.getUTCDay(); // 0 Sun ... 6 Sat
    const hour = now.getUTCHours();

    const fridayClosed = day === 5 && hour >= config.fridayCloseHourUtc;
    const saturdayClosed = day === 6;
    const sundayClosed = day === 0 && hour < config.sundayOpenHourUtc;

    if (fridayClosed || saturdayClosed || sundayClosed) {
        let reopensAtMs: number;
        if (sundayClosed) {
            reopensAtMs = new Date(
                Date.UTC(
                    now.getUTCFullYear(),
                    now.getUTCMonth(),
                    now.getUTCDate(),
                    config.sundayOpenHourUtc,
                    0,
                    0,
                    0,
                ),
            ).getTime();
        } else {
            reopensAtMs = nextSundayAtHourUtc(nowMs, config.sundayOpenHourUtc);
        }

        return {
            marketClosed: true,
            reasonCode: 'MARKET_CLOSED_WEEKEND',
            reopensAtMs,
            config,
        };
    }

    return {
        marketClosed: false,
        reasonCode: 'MARKET_OPEN',
        reopensAtMs: null,
        config,
    };
}
