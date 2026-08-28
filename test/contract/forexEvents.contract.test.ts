// Contract: lib/swing/forexEvents.ts — the ForexFactory calendar fetch plus
// the KV snapshot/meta bookkeeping around it.

import { expect, test } from 'vitest';

import { refreshForexEvents } from '../../lib/swing/forexEvents';
import { conversation, FIXED_NOW_MS, startBoundary } from '../harness';
import { forexFactoryCalendar } from '../harness/worlds/forexFactory';
import { kvWorld } from '../harness/worlds/kv';

startBoundary(() => ({
    http: [
        ...kvWorld(),
        forexFactoryCalendar([
            {
                title: 'Non-Farm Employment Change',
                country: 'USD',
                date: new Date(FIXED_NOW_MS + 26 * 3600_000).toISOString(),
                impact: 'High',
                forecast: '185K',
                previous: '187K',
            },
            {
                // Outside the [now-1d, now+7d] window — must be filtered out.
                title: 'Stale CPI print',
                country: 'EUR',
                date: new Date(FIXED_NOW_MS - 9 * 24 * 3600_000).toISOString(),
                impact: 'High',
            },
        ]),
    ],
}));

test('forced refresh fetches the calendar and persists snapshot + meta in KV', async () => {
    const out = await refreshForexEvents({ force: true });

    expect(out.ok).toBe(true);
    expect(out.state.snapshot?.events).toHaveLength(1);
    expect(out.state.snapshot?.events?.[0]).toMatchObject({
        event_name: 'Non-Farm Employment Change',
        currency: 'USD',
        impact: 'HIGH',
    });

    await expect(await conversation()).toMatchFileSnapshot('./__snapshots__/forex-events-refresh.txt');
});
