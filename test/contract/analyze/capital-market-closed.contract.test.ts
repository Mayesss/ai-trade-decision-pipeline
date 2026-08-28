// Contract: the capital-only market-hours gate. When /api/v1/markets/{epic}
// reports a non-TRADEABLE status (weekend, session close, auction), the tick
// exits before reading positions, prices or anything else — the conversation
// is just the session handshake and the market probe.

import { expect, test } from 'vitest';

import { analyzePg, runAnalyzeTick } from './world';
import { conversation, conversationSummary, startBoundary } from '../../harness';
import { capitalGet, capitalSession } from '../../harness/worlds/capital';

startBoundary(
    {
        http: [
            capitalSession(),
            capitalGet('/api/v1/markets/EURUSD', {
                instrument: { epic: 'EURUSD', openingHours: null },
                snapshot: { marketStatus: 'CLOSED', bid: 1.1579, offer: 1.15797 },
            }),
        ],
        db: analyzePg,
    },
);

test('closed market: the tick exits after the market probe', async () => {
    const out = await runAnalyzeTick({
        symbol: 'EURUSD',
        platform: 'capital',
        category: 'forex',
        dryRun: 'true',
    });

    expect(out.statusCode).toBe(200);
    const body = out.body as Record<string, any>;
    expect(body.promptSkipped).toBe(true);
    expect(body.decision.reason).toContain('capital_market_closed');
    expect(body.execRes.placed).toBe(false);

    const summary = await conversationSummary();
    expect(summary.some((line) => line.includes('/api/v1/prices'))).toBe(false);
    expect(summary.some((line) => line.includes('/api/v1/positions'))).toBe(false);

    await expect(await conversation()).toMatchFileSnapshot('./__snapshots__/capital-market-closed.txt');
});
