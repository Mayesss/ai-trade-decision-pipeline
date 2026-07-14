// Venue-aware session event clock for session-traded instruments.
//
// buildForexSessionLevelsContext (sessionLevels.ts) answers WHERE the session
// liquidity sits (levels, sweep/reclaim flags). This module answers WHEN the
// venue's liquidity regime changes: cash opens/closes, lunch breaks, Globex
// maintenance halts and the thin weekly reopen — the moments that fill resting
// orders and gap brackets. Emitted as measurements (event name + ISO UTC time
// + minutes away) plus a schedule-derived liquidity_phase; interpretation
// stays in the prompt.
//
// All venue clocks are IANA-timezone based so DST shifts (Xetra open moving
// between 07:00/08:00 UTC etc.) are handled by the runtime, never hardcoded.

export type VenueLiquidityPhase =
    | 'pre_open'
    | 'opening_drive'
    | 'into_close'
    | 'venue_break'
    | 'off_hours'
    | 'thin_reopen'
    | 'normal';

export type VenueSessionEventsContext = {
    venue: string;
    liquidity_phase: VenueLiquidityPhase;
    recent: Array<{ event: string; at_utc: string; minutes_ago: number }>;
    upcoming: Array<{ event: string; at_utc: string; minutes_to: number }>;
};

type EventKind = 'open' | 'close' | 'break_start' | 'break_end' | 'weekly_close' | 'weekly_reopen';

type VenueEventDef = {
    event: string;
    kind: EventKind;
    hour: number;
    minute: number;
    // Venue-local weekdays (0=Sun..6=Sat) the event occurs on. Default Mon–Fri.
    days?: number[];
    // false = cross-venue influence event (e.g. the US cash open moving DAX):
    // counts for pre_open/opening_drive but not for the home venue's
    // off_hours/break bookkeeping.
    home?: boolean;
};

type VenueCalendar = {
    venue: string;
    zones: Array<{ timeZone: string; events: VenueEventDef[] }>;
};

const RECENT_WINDOW_MIN = 90;
const UPCOMING_WINDOW_MIN = 180;
const OPEN_DRIVE_MIN = 30;
const PRE_OPEN_MIN = 30;
const INTO_CLOSE_MIN = 30;
const THIN_REOPEN_MIN = 120;

const WEEKDAYS = [1, 2, 3, 4, 5];

const NY = 'America/New_York';
const LDN = 'Europe/London';
const BER = 'Europe/Berlin';
const HK = 'Asia/Hong_Kong';
const TYO = 'Asia/Tokyo';

// Globex maintenance halt (17:00–18:00 ET Mon–Thu) + CME weekly close/reopen.
// Shared by US index CFDs and COMEX/NYMEX commodities.
const GLOBEX_EVENTS: VenueEventDef[] = [
    { event: 'globex_break_start', kind: 'break_start', hour: 17, minute: 0, days: [1, 2, 3, 4] },
    { event: 'globex_break_end', kind: 'break_end', hour: 18, minute: 0, days: [1, 2, 3, 4] },
    { event: 'globex_weekly_close', kind: 'weekly_close', hour: 17, minute: 0, days: [5] },
    { event: 'globex_weekly_reopen', kind: 'weekly_reopen', hour: 18, minute: 0, days: [0] },
];

const US_CASH_EVENTS: VenueEventDef[] = [
    { event: 'us_cash_open', kind: 'open', hour: 9, minute: 30 },
    { event: 'us_cash_close', kind: 'close', hour: 16, minute: 0 },
];

const US_CASH_INFLUENCE: VenueEventDef[] = US_CASH_EVENTS.map((e) => ({ ...e, home: false }));

const CALENDARS: Record<string, VenueCalendar> = {
    US_INDEX: {
        venue: 'NYSE/Globex',
        zones: [{ timeZone: NY, events: [...US_CASH_EVENTS, ...GLOBEX_EVENTS] }],
    },
    NYSE: {
        venue: 'NYSE',
        zones: [{ timeZone: NY, events: US_CASH_EVENTS }],
    },
    XETRA: {
        venue: 'XETRA',
        zones: [
            {
                timeZone: BER,
                events: [
                    { event: 'xetra_cash_open', kind: 'open', hour: 9, minute: 0 },
                    { event: 'xetra_cash_close', kind: 'close', hour: 17, minute: 30 },
                ],
            },
            { timeZone: NY, events: US_CASH_INFLUENCE },
        ],
    },
    LSE: {
        venue: 'LSE',
        zones: [
            {
                timeZone: LDN,
                events: [
                    { event: 'lse_cash_open', kind: 'open', hour: 8, minute: 0 },
                    { event: 'lse_cash_close', kind: 'close', hour: 16, minute: 30 },
                ],
            },
            { timeZone: NY, events: US_CASH_INFLUENCE },
        ],
    },
    HKEX: {
        venue: 'HKEX',
        zones: [
            {
                timeZone: HK,
                events: [
                    { event: 'hkex_cash_open', kind: 'open', hour: 9, minute: 30 },
                    { event: 'hkex_lunch_start', kind: 'break_start', hour: 12, minute: 0 },
                    { event: 'hkex_lunch_end', kind: 'break_end', hour: 13, minute: 0 },
                    { event: 'hkex_cash_close', kind: 'close', hour: 16, minute: 0 },
                ],
            },
        ],
    },
    TSE: {
        venue: 'TSE',
        zones: [
            {
                timeZone: TYO,
                events: [
                    { event: 'tse_cash_open', kind: 'open', hour: 9, minute: 0 },
                    { event: 'tse_lunch_start', kind: 'break_start', hour: 11, minute: 30 },
                    { event: 'tse_lunch_end', kind: 'break_end', hour: 12, minute: 30 },
                    { event: 'tse_cash_close', kind: 'close', hour: 15, minute: 30 },
                ],
            },
        ],
    },
    METALS: {
        venue: 'COMEX/Globex',
        zones: [
            {
                timeZone: NY,
                events: [{ event: 'comex_floor_open', kind: 'open', hour: 8, minute: 20 }, ...GLOBEX_EVENTS],
            },
            { timeZone: LDN, events: [{ event: 'london_open', kind: 'open', hour: 8, minute: 0, home: false }] },
        ],
    },
    ENERGY: {
        venue: 'NYMEX/Globex',
        zones: [
            {
                timeZone: NY,
                events: [{ event: 'nymex_floor_open', kind: 'open', hour: 9, minute: 0 }, ...GLOBEX_EVENTS],
            },
            { timeZone: LDN, events: [{ event: 'london_open', kind: 'open', hour: 8, minute: 0, home: false }] },
        ],
    },
    FX: {
        venue: 'FX',
        zones: [
            { timeZone: TYO, events: [{ event: 'tokyo_open', kind: 'open', hour: 9, minute: 0, home: false }] },
            {
                timeZone: LDN,
                events: [
                    { event: 'london_open', kind: 'open', hour: 8, minute: 0, home: false },
                    { event: 'london_close', kind: 'close', hour: 16, minute: 30, home: false },
                ],
            },
            {
                timeZone: NY,
                events: [
                    { event: 'new_york_open', kind: 'open', hour: 8, minute: 0, home: false },
                    { event: 'fx_weekly_close', kind: 'weekly_close', hour: 17, minute: 0, days: [5] },
                    { event: 'fx_weekly_reopen', kind: 'weekly_reopen', hour: 17, minute: 0, days: [0] },
                ],
            },
        ],
    },
};

const SYMBOL_CALENDAR: Record<string, string> = {
    US100: 'US_INDEX',
    US500: 'US_INDEX',
    US30: 'US_INDEX',
    RTY: 'US_INDEX',
    TLT: 'NYSE',
    DE40: 'XETRA',
    EU50: 'XETRA',
    UK100: 'LSE',
    HK50: 'HKEX',
    JP225: 'TSE',
    GOLD: 'METALS',
    SILVER: 'METALS',
    COPPER: 'METALS',
    PLATINUM: 'METALS',
    PALLADIUM: 'METALS',
    OIL: 'ENERGY',
    OIL_CRUDE: 'ENERGY',
    BRENT: 'ENERGY',
    NATURALGAS: 'ENERGY',
    NATGAS: 'ENERGY',
};

function calendarFor(symbol: string, category?: string | null): VenueCalendar | null {
    const key = String(symbol || '').toUpperCase().trim();
    const mapped = SYMBOL_CALENDAR[key];
    if (mapped) return CALENDARS[mapped];
    if (category === 'forex') return CALENDARS.FX;
    if (category === 'commodity') return CALENDARS.METALS;
    // Unknown index/other venue: no calendar beats a wrong one — the prompt
    // prose is conditional on this block's presence.
    return null;
}

// ---------------------------------------------------------------------------
// IANA-timezone local→UTC conversion (no dependencies, DST-safe)
// ---------------------------------------------------------------------------

const dtfCache = new Map<string, Intl.DateTimeFormat>();

function dtfFor(timeZone: string): Intl.DateTimeFormat {
    let dtf = dtfCache.get(timeZone);
    if (!dtf) {
        dtf = new Intl.DateTimeFormat('en-US', {
            timeZone,
            year: 'numeric',
            month: '2-digit',
            day: '2-digit',
            hour: '2-digit',
            minute: '2-digit',
            second: '2-digit',
            hour12: false,
        });
        dtfCache.set(timeZone, dtf);
    }
    return dtf;
}

function localParts(timeZone: string, utcMs: number): { y: number; m: number; d: number } {
    const parts: Record<string, string> = {};
    for (const p of dtfFor(timeZone).formatToParts(new Date(utcMs))) parts[p.type] = p.value;
    return { y: Number(parts.year), m: Number(parts.month), d: Number(parts.day) };
}

function zoneOffsetMs(timeZone: string, utcMs: number): number {
    const parts: Record<string, string> = {};
    for (const p of dtfFor(timeZone).formatToParts(new Date(utcMs))) parts[p.type] = p.value;
    const asUtc = Date.UTC(
        Number(parts.year),
        Number(parts.month) - 1,
        Number(parts.day),
        Number(parts.hour) % 24,
        Number(parts.minute),
        Number(parts.second),
    );
    return asUtc - utcMs;
}

// UTC instant at which the venue-local wall clock reads hour:minute on the
// local calendar day given by (y, m, d). Two-pass offset correction handles
// DST transition days.
function utcMsForLocalTime(timeZone: string, y: number, m: number, d: number, hour: number, minute: number): number {
    const guess = Date.UTC(y, m - 1, d, hour, minute);
    const once = guess - zoneOffsetMs(timeZone, guess);
    return guess - zoneOffsetMs(timeZone, once);
}

// ---------------------------------------------------------------------------

type EventInstance = { event: string; kind: EventKind; home: boolean; atMs: number };

function instancesFor(calendar: VenueCalendar, nowMs: number): EventInstance[] {
    const out: EventInstance[] = [];
    for (const zone of calendar.zones) {
        const today = localParts(zone.timeZone, nowMs);
        const todayUtcAnchor = Date.UTC(today.y, today.m - 1, today.d);
        // -3..+1 local days: the weekend lookback (Sunday needs Friday's weekly
        // close to classify off_hours) plus tomorrow's upcoming events.
        for (let delta = -3; delta <= 1; delta++) {
            const day = new Date(todayUtcAnchor + delta * 86_400_000);
            const weekday = day.getUTCDay();
            for (const def of zone.events) {
                const days = def.days ?? WEEKDAYS;
                if (!days.includes(weekday)) continue;
                out.push({
                    event: def.event,
                    kind: def.kind,
                    home: def.home !== false,
                    atMs: utcMsForLocalTime(
                        zone.timeZone,
                        day.getUTCFullYear(),
                        day.getUTCMonth() + 1,
                        day.getUTCDate(),
                        def.hour,
                        def.minute,
                    ),
                });
            }
        }
    }
    return out.sort((a, b) => a.atMs - b.atMs);
}

function derivePhase(instances: EventInstance[], nowMs: number): VenueLiquidityPhase {
    const minutesTo = (e: EventInstance) => (e.atMs - nowMs) / 60_000;
    const past = instances.filter((e) => e.atMs <= nowMs);
    const future = instances.filter((e) => e.atMs > nowMs);

    // Inside a scheduled halt (lunch break / Globex maintenance / weekend)?
    const lastHome = [...past].reverse().find((e) => e.home);
    if (lastHome?.kind === 'break_start') return 'venue_break';
    if (lastHome?.kind === 'weekly_close') return 'off_hours';

    if (past.some((e) => e.kind === 'weekly_reopen' && -minutesTo(e) <= THIN_REOPEN_MIN)) return 'thin_reopen';
    if (past.some((e) => (e.kind === 'open' || e.kind === 'break_end') && -minutesTo(e) <= OPEN_DRIVE_MIN)) {
        return 'opening_drive';
    }
    if (future.some((e) => e.kind === 'open' && minutesTo(e) <= PRE_OPEN_MIN)) return 'pre_open';
    if (future.some((e) => e.home && (e.kind === 'close' || e.kind === 'weekly_close' || e.kind === 'break_start') && minutesTo(e) <= INTO_CLOSE_MIN)) {
        return 'into_close';
    }

    // Between the home cash close and the next home open the CFD may still
    // trade, but the underlying book is dark/thin (Globex overnight included).
    const lastHomeCashBoundary = [...past]
        .reverse()
        .find((e) => e.home && (e.kind === 'open' || e.kind === 'close' || e.kind === 'weekly_close' || e.kind === 'weekly_reopen'));
    if (lastHomeCashBoundary?.kind === 'close' || lastHomeCashBoundary?.kind === 'weekly_close') return 'off_hours';
    return 'normal';
}

export function buildVenueSessionEvents(params: {
    symbol: string;
    category?: string | null;
    nowMs?: number;
}): VenueSessionEventsContext | null {
    const calendar = calendarFor(params.symbol, params.category);
    if (!calendar) return null;
    const nowMs = Number.isFinite(params.nowMs as number) ? Number(params.nowMs) : Date.now();

    const instances = instancesFor(calendar, nowMs);
    const recent = instances
        .filter((e) => e.atMs <= nowMs && nowMs - e.atMs <= RECENT_WINDOW_MIN * 60_000)
        .map((e) => ({
            event: e.event,
            at_utc: new Date(e.atMs).toISOString(),
            minutes_ago: Math.round((nowMs - e.atMs) / 60_000),
        }));
    const upcoming = instances
        .filter((e) => e.atMs > nowMs && e.atMs - nowMs <= UPCOMING_WINDOW_MIN * 60_000)
        .map((e) => ({
            event: e.event,
            at_utc: new Date(e.atMs).toISOString(),
            minutes_to: Math.round((e.atMs - nowMs) / 60_000),
        }));

    return {
        venue: calendar.venue,
        liquidity_phase: derivePhase(instances, nowMs),
        recent,
        upcoming,
    };
}
