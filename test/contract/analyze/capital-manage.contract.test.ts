// Contract: a Capital.com manage tick on an open EURUSD long — the flat-only
// gates (actionability, extension, warmup, affordability) do not apply
// in-position, so this is the scenario that carries the full CAPITAL prompt
// to the model (and the marketaux news read the capital platform uses).
// Capital keeps the standing bracket ON the position row (stopLevel/
// profitLevel — no plan-order read); the model answers HOLD with a tightened
// stop, and updateCapitalPositionLevels re-reads /api/v1/positions but stops
// before the PUT in dryRun.

import { expect, test } from 'vitest';

import eurusdFixtureJson from '../fixtures/capital-EURUSD.json';
import { analyzePg, capitalInPositionPrivateWorld, decisionBase, runAnalyzeTick } from './world';
import { conversation, startBoundary } from '../../harness';
import { openAiDecides } from '../../harness/worlds/aiGateway';
import { forexFactoryCalendar } from '../../harness/worlds/forexFactory';
import { kvWorld } from '../../harness/worlds/kv';
import { marketauxNews } from '../../harness/worlds/news';
import { capitalMarketWorld } from '../../harness/worlds/recordedMarkets';

import type { RecordedMarketFixture } from '../../harness/worlds/recordedMarkets';

const fixture = eurusdFixtureJson as RecordedMarketFixture;

// Long from 1.15000 (captured mid ~1.1580); model raises the stop to 1.15200
// and leaves the TP untouched.
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
            openAiDecides(MANAGE_HOLD),
        ],
        db: analyzePg,
    }),
    { nowMs: fixture.capturedAtMs },
);

test('in-position manage tick: full capital prompt, HOLD with stop amendment, dryRun stops before PUT', async () => {
    const out = await runAnalyzeTick({
        symbol: 'EURUSD',
        platform: 'capital',
        category: 'forex',
        dryRun: 'true',
    });

    expect(out.statusCode).toBe(200);
    const body = out.body as Record<string, any>;
    expect(body.promptSkipped).toBeUndefined();
    expect(body.decision.action).toBe('HOLD');
    expect(body.decision.response_id).toBe('resp_test-1');
    expect(body.execRes.placed).toBe(false);

    const text = await conversation();
    expect(text).toContain('POST https://ai-gateway.vercel.sh/v1/responses');
    expect(text).toContain('api.marketaux.com');
    expect(text).not.toContain('PUT https://api-capital.backend-capital.com');

    await expect(text).toMatchFileSnapshot('./__snapshots__/capital-manage.txt');
}, 30_000);
