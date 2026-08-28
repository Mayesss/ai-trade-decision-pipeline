// Contract: lib/capital.ts — session bootstrap (CST / X-SECURITY-TOKEN from
// response headers, then sent on every authed call), epic verification, and
// the light markets?epics= quote read behind fetchCapitalMidPrice.
//
// lib/capital.ts caches the session and resolved epics in module state for
// the life of the worker, so this file keeps ONE test — a second test would
// see a pre-warmed module and record a shorter conversation.

import { expect, test } from 'vitest';

import { fetchCapitalMidPrice } from '../../lib/capital';
import { conversation, startBoundary } from '../harness';
import { capitalGet, capitalSession } from '../harness/worlds/capital';

startBoundary({
    http: [
        capitalSession(),
        capitalGet('/api/v1/markets/EURUSD', {
            instrument: { epic: 'EURUSD' },
            snapshot: { marketStatus: 'TRADEABLE' },
        }),
        capitalGet('/api/v1/markets', {
            markets: [{ epic: 'EURUSD', snapshot: { marketStatus: 'TRADEABLE', bid: 1.09, offer: 1.091 } }],
        }),
    ],
});

test('mid-price read: login, verify the epic, quote via markets?epics=', async () => {
    const mid = await fetchCapitalMidPrice('EURUSD');
    expect(mid).toBeCloseTo(1.0905, 6);

    await expect(await conversation()).toMatchFileSnapshot('./__snapshots__/capital-mid-price.txt');
});
