// WORLD BUILDER: ForexFactory economic calendar.
//
// Unauthenticated GET; the payload MUST be a top-level JSON array
// (lib/swing/forexEvents.ts rejects anything else). Field aliases accepted by
// normalizeForexFactoryEventRow: title/event/name, country/currency/ccy,
// date/dateUtc/timestamp/time, impact/importance/priority, forecast/estimate.

import { http, HttpResponse } from 'msw';

import type { RequestHandler } from 'msw';

// The default URL in lib/swing/forexEvents.ts — no env override needed.
export const FOREX_FACTORY_CALENDAR_URL = 'https://nfs.faireconomy.media/ff_calendar_thisweek.json';

export interface ForexFactoryEventSeed {
    title: string;
    country: string;
    /** ISO timestamp — must fall within [now-1d, now+7d] to survive filtering. */
    date: string;
    impact: 'High' | 'Medium' | 'Low' | 'Holiday';
    forecast?: string;
    previous?: string;
}

export function forexFactoryCalendar(events: ForexFactoryEventSeed[]): RequestHandler {
    return http.get(FOREX_FACTORY_CALENDAR_URL, () => HttpResponse.json(events));
}
