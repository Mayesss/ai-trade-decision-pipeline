// WORLD BUILDER: market data replayed from captured fixtures — both venues.
//
// Fixtures come from `node --import tsx scripts/capture-analyze-fixtures.ts`,
// which records REAL market-data responses (full body) from one live dryRun
// tick: Bitget /api/v2/mix/market/* (public), Capital /api/v1/prices* and
// /api/v1/markets* (market metadata; account state is never recorded). Replay
// freezes the clock at the fixture's `capturedAtMs` (pass it to
// startBoundary) so age/staleness math reproduces the capture.
//
// Matching ignores the volatile time-window params (startTime/endTime/after/
// from/to) — they are Date.now()-derived and differ per run; everything else
// must match exactly or the handler answers a named error, failing the test
// loudly. Capital's tick calls carry only stable params (max + resolution),
// so its fixture keys are exact.

import { http, HttpResponse } from 'msw';

import { BITGET_HOST } from './bitget';
import { CAPITAL_HOST } from './capital';

import type { RequestHandler } from 'msw';

export interface RecordedMarketFixture {
    symbol: string;
    capturedAtMs: number;
    entries: Array<{
        path: string;
        query: Record<string, string>;
        body: unknown;
    }>;
}

const VOLATILE_PARAMS = new Set(['startTime', 'endTime', 'after', 'idLessThan', 'from', 'to']);

function stableQuery(params: URLSearchParams | Record<string, string>): Record<string, string> {
    const out: Record<string, string> = {};
    const entries = params instanceof URLSearchParams ? params.entries() : Object.entries(params);
    for (const [key, value] of entries) {
        if (VOLATILE_PARAMS.has(key)) continue;
        // lib code mixes productType casings per call site; Bitget accepts both.
        out[key] = key === 'productType' ? value.toLowerCase() : value;
    }
    return out;
}

function sameQuery(a: Record<string, string>, b: Record<string, string>): boolean {
    const aKeys = Object.keys(a).sort();
    const bKeys = Object.keys(b).sort();
    return aKeys.length === bKeys.length && aKeys.every((key, i) => key === bKeys[i] && a[key] === b[key]);
}

function recordedMarketWorld(host: string, fixture: RecordedMarketFixture): RequestHandler[] {
    const byPath = new Map<string, RecordedMarketFixture['entries']>();
    for (const entry of fixture.entries) {
        const list = byPath.get(entry.path) ?? [];
        list.push(entry);
        byPath.set(entry.path, list);
    }

    return [...byPath.entries()].map(([path, entries]) =>
        http.get(`${host}${path}`, ({ request }) => {
            const wanted = stableQuery(new URL(request.url).searchParams);
            const hit = entries.find((entry) => sameQuery(stableQuery(entry.query), wanted));
            if (!hit) {
                return HttpResponse.json(
                    { code: 'FIXTURE_MISS', msg: `no captured response for ${path}?${JSON.stringify(wanted)}` },
                    { status: 500 },
                );
            }
            return HttpResponse.json(hit.body as object);
        }),
    );
}

export function bitgetMarketWorld(fixture: RecordedMarketFixture): RequestHandler[] {
    return recordedMarketWorld(BITGET_HOST, fixture);
}

export function capitalMarketWorld(fixture: RecordedMarketFixture): RequestHandler[] {
    return recordedMarketWorld(CAPITAL_HOST, fixture);
}
