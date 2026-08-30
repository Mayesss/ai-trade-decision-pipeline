// WORLD BUILDER: alternative.me crypto Fear & Greed index.
//
// GET /fng/?limit=10 → { data: [today, yesterday, ...] } newest-first with
// STRING values — lib/swing/fearGreed.ts parses them numerically and reverses
// into daily_oldest_first (see FEAR_GREED_URL).

import { http, HttpResponse } from 'msw';

import type { RequestHandler } from 'msw';

export const ALTERNATIVE_ME_HOST = 'https://api.alternative.me';

export interface FearGreedSeed {
    value: number;
    classification?: string;
    timestampMs?: number;
}

// Seeds NEWEST FIRST (today at index 0), like the real payload; timestamps
// default to one day apart counting back from now.
export function fearGreedIndex(seeds: FearGreedSeed[]): RequestHandler {
    const row = (seed: FearGreedSeed, index: number) => ({
        value: String(seed.value),
        value_classification: seed.classification ?? 'Neutral',
        timestamp: String(Math.floor((seed.timestampMs ?? Date.now() - index * 86_400_000) / 1000)),
        time_until_update: '3600',
    });
    return http.get(`${ALTERNATIVE_ME_HOST}/fng/`, () =>
        HttpResponse.json({
            name: 'Fear and Greed Index',
            data: seeds.map(row),
            metadata: { error: null },
        }),
    );
}
