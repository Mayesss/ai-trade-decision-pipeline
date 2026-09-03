// Contract: a LIVE (dryRun=false) Capital.com manage tick — the bracket
// amendment that dryRun stops short of. Capital's PUT /positions/{dealId}
// REPLACES the whole bracket (an omitted leg would clear it), so the pinned
// body must always carry BOTH stopLevel and profitLevel even when the model
// amended only the stop. Also live-only: the thread upsert and the
// capital-close bookkeeping path.

import { expect, test } from 'vitest';

import eurusdFixtureJson from '../fixtures/capital-EURUSD.json';
import { analyzePg, capitalInPositionPrivateWorld, decisionBase, runAnalyzeTick } from './world';
import { conversation, startBoundary } from '../../harness';
import { responsesDecides } from '../../harness/worlds/aiGateway';
import { capitalPut } from '../../harness/worlds/capital';
import { forexFactoryCalendar } from '../../harness/worlds/forexFactory';
import { kvWorld } from '../../harness/worlds/kv';
import { marketauxNews } from '../../harness/worlds/news';
import { capitalMarketWorld } from '../../harness/worlds/recordedMarkets';

import type { RecordedMarketFixture } from '../../harness/worlds/recordedMarkets';

const fixture = eurusdFixtureJson as RecordedMarketFixture;

const MANAGE_HOLD = {
    ...decisionBase('HOLD', 'thesis intact, protect gains', 'raise stop under reclaimed level'),
    stop_loss_price: 1.152,
};

startBoundary(
    () => ({
        http: [
            ...capitalMarketWorld(fixture),
            ...capitalInPositionPrivateWorld({
                epic: 'EURUSD',
                entryLevel: 1.15,
                bid: 1.1579,
                offer: 1.15797,
                openedAtMs: fixture.capturedAtMs - 2 * 24 * 3600_000,
                stopLevel: 1.144,
                profitLevel: 1.175,
            }),
            ...kvWorld(),
            marketauxNews([{ title: 'Euro steadies after PMI beat', sentimentScore: 0.2 }]),
            forexFactoryCalendar([]),
            responsesDecides(MANAGE_HOLD),
            capitalPut('/api/v1/positions/deal-test-1', { dealReference: 'ref-amend-1' }),
        ],
        db: analyzePg,
    }),
    { nowMs: fixture.capturedAtMs },
);

test('live capital manage: PUT carries the full bracket, thread persists', async () => {
    const out = await runAnalyzeTick({
        symbol: 'EURUSD',
        platform: 'capital',
        category: 'forex',
        dryRun: 'false',
    });

    expect(out.statusCode).toBe(200);
    const body = out.body as Record<string, any>;
    expect(body.decision.action).toBe('HOLD');
    expect(body.execRes.tpsl).toMatchObject({ updated: true, dealId: 'deal-test-1' });

    const text = await conversation();
    expect(text).toContain('PUT https://api-capital.backend-capital.com/api/v1/positions/deal-test-1');
    // Whole-bracket replacement: the untouched TP leg must ride along.
    expect(text).toContain('"profitLevel": 1.175');
    expect(text).toContain('INSERT INTO swing.ai_threads');

    await expect(text).toMatchFileSnapshot('./__snapshots__/capital-live-amend.txt');
}, 30_000);
