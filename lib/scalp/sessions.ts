import type { ScalpClockMode, ScalpSessionWindows } from './types';

function parseDayKey(dayKey: string): { y: number; m: number; d: number } {
    const match = String(dayKey || '')
        .trim()
        .match(/^(\d{4})-(\d{2})-(\d{2})$/);
    if (!match) {
        throw new Error(`Invalid day key: ${dayKey}`);
    }
    return {
        y: Number(match[1]),
        m: Number(match[2]),
        d: Number(match[3]),
    };
}

function parseClock(clock: string): { hh: number; mm: number } {
    const match = String(clock || '')
        .trim()
        .match(/^([01]?\d|2[0-3]):([0-5]\d)$/);
    if (!match) throw new Error(`Invalid clock label: ${clock}`);
    return { hh: Number(match[1]), mm: Number(match[2]) };
}

function partsForTimeZone(tsMs: number, timeZone: string): { y: number; m: number; d: number; hh: number; mm: number } {
    const fmt = new Intl.DateTimeFormat('en-CA', {
        timeZone,
        year: 'numeric',
        month: '2-digit',
        day: '2-digit',
        hour: '2-digit',
        minute: '2-digit',
        hour12: false,
    });
    const parts = fmt.formatToParts(new Date(tsMs));
    const read = (type: Intl.DateTimeFormatPartTypes, fallback: number): number => {
        const raw = parts.find((p) => p.type === type)?.value;
        const n = Number(raw);
        return Number.isFinite(n) ? n : fallback;
    };
    return {
        y: read('year', 1970),
        m: read('month', 1),
        d: read('day', 1),
        hh: read('hour', 0),
        mm: read('minute', 0),
    };
}

function utcMsFromZoned(dayKey: string, clock: string, timeZone: string): number {
    const date = parseDayKey(dayKey);
    const t = parseClock(clock);
    let guessMs = Date.UTC(date.y, date.m - 1, date.d, t.hh, t.mm, 0, 0);
    const targetDayInt = date.y * 10_000 + date.m * 100 + date.d;
    const targetMinuteOfDay = t.hh * 60 + t.mm;

    for (let i = 0; i < 6; i += 1) {
        const local = partsForTimeZone(guessMs, timeZone);
        const localDayInt = local.y * 10_000 + local.m * 100 + local.d;
        const dayDelta = localDayInt === targetDayInt ? 0 : localDayInt < targetDayInt ? 1 : -1;
        const localMinuteOfDay = local.hh * 60 + local.mm;
        const deltaMinutes = dayDelta * 1440 + (targetMinuteOfDay - localMinuteOfDay);
        if (deltaMinutes === 0) break;
        guessMs += deltaMinutes * 60_000;
    }

    return guessMs;
}

function utcMsFromFixed(dayKey: string, clock: string): number {
    const date = parseDayKey(dayKey);
    const t = parseClock(clock);
    return Date.UTC(date.y, date.m - 1, date.d, t.hh, t.mm, 0, 0);
}

function normalizeWindow(startMs: number, endMs: number): { startMs: number; endMs: number } {
    if (endMs > startMs) return { startMs, endMs };
    return {
        startMs,
        endMs: endMs + 24 * 60 * 60 * 1000,
    };
}

export function buildScalpSessionWindows(params: {
    dayKey: string;
    clockMode: ScalpClockMode;
    asiaWindowLocal: [string, string];
    raidWindowLocal: [string, string];
}): ScalpSessionWindows {
    const timezone = params.clockMode === 'UTC_FIXED' ? 'UTC' : 'Europe/London';
    const toUtcMs = (clock: string) =>
        params.clockMode === 'UTC_FIXED'
            ? utcMsFromFixed(params.dayKey, clock)
            : utcMsFromZoned(params.dayKey, clock, 'Europe/London');

    const asiaRawStartMs = toUtcMs(params.asiaWindowLocal[0]);
    const asiaRawEndMs = toUtcMs(params.asiaWindowLocal[1]);
    const raidRawStartMs = toUtcMs(params.raidWindowLocal[0]);
    const raidRawEndMs = toUtcMs(params.raidWindowLocal[1]);

    const asia = normalizeWindow(asiaRawStartMs, asiaRawEndMs);
    const raid = normalizeWindow(raidRawStartMs, raidRawEndMs);

    return {
        timezone,
        asiaStartMs: asia.startMs,
        asiaEndMs: asia.endMs,
        raidStartMs: raid.startMs,
        raidEndMs: raid.endMs,
        asiaStartUtcIso: new Date(asia.startMs).toISOString(),
        asiaEndUtcIso: new Date(asia.endMs).toISOString(),
        raidStartUtcIso: new Date(raid.startMs).toISOString(),
        raidEndUtcIso: new Date(raid.endMs).toISOString(),
    };
}
