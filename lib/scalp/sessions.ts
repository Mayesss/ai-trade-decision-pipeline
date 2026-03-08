import type { ScalpClockMode, ScalpEntrySessionProfile, ScalpSessionWindows } from './types';

const SCALP_ENTRY_SESSION_PROFILE_ORDER: ScalpEntrySessionProfile[] = [
    'tokyo',
    'tokyo_london_overlap',
    'berlin',
    'newyork',
];

export const DEFAULT_SCALP_ENTRY_SESSION_PROFILE: ScalpEntrySessionProfile = 'berlin';

type ScalpEntrySessionProfileDefinition = {
    profile: ScalpEntrySessionProfile;
    timeZone: string;
    windowsLocal: Array<[string, string]>;
};

const SCALP_ENTRY_SESSION_PROFILES: Record<ScalpEntrySessionProfile, ScalpEntrySessionProfileDefinition> = {
    tokyo: {
        profile: 'tokyo',
        timeZone: 'Asia/Tokyo',
        windowsLocal: [['09:00', '13:00']],
    },
    tokyo_london_overlap: {
        profile: 'tokyo_london_overlap',
        timeZone: 'Europe/London',
        windowsLocal: [['07:00', '11:00']],
    },
    berlin: {
        profile: 'berlin',
        timeZone: 'Europe/Berlin',
        windowsLocal: [['08:00', '12:00']],
    },
    newyork: {
        profile: 'newyork',
        timeZone: 'America/New_York',
        windowsLocal: [['08:00', '12:00']],
    },
};

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

function minuteOfDayForClock(clock: string): number {
    const parsed = parseClock(clock);
    return parsed.hh * 60 + parsed.mm;
}

function minutesInWindow(startClock: string, endClock: string): number {
    const start = minuteOfDayForClock(startClock);
    const end = minuteOfDayForClock(endClock);
    if (end > start) return end - start;
    return 24 * 60 - start + end;
}

function windowContainsMinute(minuteOfDay: number, startClock: string, endClock: string): boolean {
    const start = minuteOfDayForClock(startClock);
    const end = minuteOfDayForClock(endClock);
    if (end > start) return minuteOfDay >= start && minuteOfDay < end;
    return minuteOfDay >= start || minuteOfDay < end;
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

export function minuteOfDayInTimeZone(tsMs: number, timeZone: string): number {
    const parts = partsForTimeZone(tsMs, timeZone);
    const hh = Number(parts.hh);
    const mm = Number(parts.mm);
    if (!Number.isFinite(hh) || !Number.isFinite(mm)) return -1;
    return hh * 60 + mm;
}

function getEntrySessionProfileDefinition(profile: ScalpEntrySessionProfile): ScalpEntrySessionProfileDefinition {
    return SCALP_ENTRY_SESSION_PROFILES[profile];
}

export function normalizeScalpEntrySessionProfile(
    value: unknown,
    fallback: ScalpEntrySessionProfile = DEFAULT_SCALP_ENTRY_SESSION_PROFILE,
): ScalpEntrySessionProfile {
    const normalized = String(value || '')
        .trim()
        .toLowerCase()
        .replace(/[^a-z0-9_]/g, '') as ScalpEntrySessionProfile;
    if (normalized in SCALP_ENTRY_SESSION_PROFILES) {
        return normalized;
    }
    return fallback;
}

export function listScalpEntrySessionProfiles(): ScalpEntrySessionProfile[] {
    return SCALP_ENTRY_SESSION_PROFILE_ORDER.slice();
}

export function scalpEntrySessionProfileDistance(a: ScalpEntrySessionProfile, b: ScalpEntrySessionProfile): number {
    const idxA = SCALP_ENTRY_SESSION_PROFILE_ORDER.indexOf(a);
    const idxB = SCALP_ENTRY_SESSION_PROFILE_ORDER.indexOf(b);
    if (idxA < 0 || idxB < 0) return Number.MAX_SAFE_INTEGER;
    return Math.abs(idxA - idxB);
}

export function inScalpEntrySessionProfileWindow(nowMs: number, profile: ScalpEntrySessionProfile): boolean {
    const definition = getEntrySessionProfileDefinition(profile);
    const minuteOfDay = minuteOfDayInTimeZone(nowMs, definition.timeZone);
    if (!(minuteOfDay >= 0)) return false;
    return definition.windowsLocal.some(([startClock, endClock]) =>
        windowContainsMinute(minuteOfDay, startClock, endClock),
    );
}

function assertEqualDurationWindows(): void {
    const expected = 4 * 60;
    for (const profile of SCALP_ENTRY_SESSION_PROFILE_ORDER) {
        const definition = getEntrySessionProfileDefinition(profile);
        const total = definition.windowsLocal.reduce((sum, [startClock, endClock]) => {
            return sum + minutesInWindow(startClock, endClock);
        }, 0);
        if (total !== expected) {
            throw new Error(`Invalid session profile ${profile}: expected ${expected} minutes, got ${total}`);
        }
    }
}

assertEqualDurationWindows();

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
