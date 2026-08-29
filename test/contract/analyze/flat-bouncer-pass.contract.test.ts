// Contract: the ai-bouncer lets a flat tick PROCEED — and every bouncer
// failure fails OPEN (the full decision call still runs). The bouncer handler
// varies per test via startBoundary().use; the baseline world carries the full
// happy-path surface (market, news, main model).

import { expect, test, vi } from 'vitest';

import btcFixtureJson from '../fixtures/bitget-BTCUSDT.json';
import { analyzePg, decisionBase, flatPrivateWorld, runAnalyzeTick } from './world';
import { conversationSummary, startBoundary } from '../../harness';
import { bouncerDecides, openAiDecides } from '../../harness/worlds/aiGateway';
import { bitgetMarketWorld } from '../../harness/worlds/recordedMarkets';
import { forexFactoryCalendar } from '../../harness/worlds/forexFactory';
import { kvWorld } from '../../harness/worlds/kv';
import { coindeskNews } from '../../harness/worlds/news';

import type { RecordedMarketFixture } from '../../harness/worlds/recordedMarkets';

const fixture = btcFixtureJson as RecordedMarketFixture;

const HOLD = decisionBase('HOLD', 'boxed between 4H levels', 'no directional edge at current price');

const boundary = startBoundary(
    () => ({
        http: [
            ...bitgetMarketWorld(fixture),
            ...flatPrivateWorld(),
            ...kvWorld(),
            coindeskNews([{ title: 'Bitcoin consolidates below resistance', sentiment: 'NEUTRAL' }]),
            forexFactoryCalendar([]),
            openAiDecides(HOLD),
        ],
        db: analyzePg,
    }),
    { nowMs: fixture.capturedAtMs },
);

test('bouncer proceed=true: the full decision call runs after the triage call', async () => {
    vi.stubEnv('SWING_AI_BOUNCER_ENABLED', 'true');
    boundary.use(bouncerDecides({ proceed: true, confidence: 0.9, reason: 'clean structure' }));

    const out = await runAnalyzeTick({ symbol: 'BTCUSDT', platform: 'bitget', dryRun: 'true' });

    expect(out.statusCode).toBe(200);
    const body = out.body as Record<string, any>;
    expect(body.promptSkipped).toBeUndefined();
    expect(body.decision.action).toBe('HOLD');
    expect(body.decision.response_id).toBe('resp_test-1');

    const summary = await conversationSummary();
    const bouncerCalls = summary.filter((line) => line.includes('/v1/chat/completions'));
    const decisionCalls = summary.filter((line) => line.includes('/v1/responses'));
    expect(bouncerCalls.length).toBe(1);
    expect(decisionCalls.length).toBe(1);
    // Triage strictly before the expensive call.
    expect(summary.indexOf(bouncerCalls[0])).toBeLessThan(summary.indexOf(decisionCalls[0]));
});

test('bouncer HTTP 500 fails open: the full decision call still runs', async () => {
    vi.stubEnv('SWING_AI_BOUNCER_ENABLED', 'true');
    boundary.use(bouncerDecides({ proceed: false, confidence: 1, reason: 'irrelevant' }, { status: 500 }));

    const out = await runAnalyzeTick({ symbol: 'BTCUSDT', platform: 'bitget', dryRun: 'true' });

    expect(out.statusCode).toBe(200);
    const body = out.body as Record<string, any>;
    expect(body.promptSkipped).toBeUndefined();
    expect(body.decision.action).toBe('HOLD');

    const summary = await conversationSummary();
    expect(summary.some((line) => line.includes('/v1/responses'))).toBe(true);
});

test('flag off (default): no triage call goes out, the tick behaves as before', async () => {
    const out = await runAnalyzeTick({ symbol: 'BTCUSDT', platform: 'bitget', dryRun: 'true' });

    expect(out.statusCode).toBe(200);
    expect((out.body as Record<string, any>).decision.action).toBe('HOLD');

    const summary = await conversationSummary();
    expect(summary.some((line) => line.includes('/v1/chat/completions'))).toBe(false);
});
