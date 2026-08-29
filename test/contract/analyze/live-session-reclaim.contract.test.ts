// Contract: session-reclaim look (reclaim-wake phase 2). The watcher left a
// fresh sweep-and-reclaim event of the prior-day low in KV; the analyze tick
// must pick it up (no cooldown row involved), bypass the flat quality gates,
// carry market.session_reclaim in the prompt, keep the look READ-ONLY (the
// AI's HOLD cooldown attempt is ignored), and consume the KV event only
// after the decision is durably recorded.

import { expect, test } from 'vitest';

import btcFixtureJson from '../fixtures/bitget-BTCUSDT.json';
import { kvSetJson } from '../../../lib/kv';
import { sessionSweepEventKey, type SessionSweepEvent } from '../../../lib/swing/wakeWatch';
import { analyzePg, decisionBase, flatPrivateWorld, runAnalyzeTick } from './world';
import { conversation, conversationSummary, startBoundary } from '../../harness';
import { openAiDecides } from '../../harness/worlds/aiGateway';
import { forexFactoryCalendar } from '../../harness/worlds/forexFactory';
import { kvWorld } from '../../harness/worlds/kv';
import { coindeskNews } from '../../harness/worlds/news';
import { bitgetMarketWorld } from '../../harness/worlds/recordedMarkets';

import type { RecordedMarketFixture } from '../../harness/worlds/recordedMarkets';

const fixture = btcFixtureJson as RecordedMarketFixture;

// Captured lastPr ~77,543: the prior-day low at 77,200 was swept to 76,900
// (0.3 ATR deep at atr 1000) and reclaimed 4 minutes ago.
const EVENT: SessionSweepEvent = {
    kind: 'prior_day_low',
    side: 'below',
    level: 77_200,
    extreme: 76_900,
    touchedAtMs: fixture.capturedAtMs - 12 * 60_000,
    reclaimedAtMs: fixture.capturedAtMs - 4 * 60_000,
    atr: 1000,
};

const HOLD_REWRITE = {
    ...decisionBase('HOLD', 'reclaim noted, no confluence', 'sweep reclaimed but micro has not turned'),
    cooldown_minutes: 240,
    cooldown_wake_below: 76_900,
    cooldown_wake_note: 'must be ignored — read-only look',
};

startBoundary(
    () => ({
        http: [
            ...bitgetMarketWorld(fixture),
            ...flatPrivateWorld(),
            ...kvWorld(),
            coindeskNews([{ title: 'Bitcoin sweeps the prior-day low', sentiment: 'NEUTRAL' }]),
            forexFactoryCalendar([]),
            openAiDecides(HOLD_REWRITE),
        ],
        db: analyzePg,
    }),
    { nowMs: fixture.capturedAtMs },
);

test('fresh session-sweep event produces a read-only AI look and is consumed', async () => {
    // Seed the watcher's event into the harness KV before the tick.
    await kvSetJson(sessionSweepEventKey('bitget', 'BTCUSDT'), EVENT, 900);

    const out = await runAnalyzeTick({ symbol: 'BTCUSDT', platform: 'bitget', dryRun: 'false' });

    expect(out.statusCode).toBe(200);
    const body = out.body as Record<string, any>;
    expect(body.decision.action).toBe('HOLD');
    // Read-only: the cooldown rewrite is ignored in code.
    expect(body.decision.cooldown_minutes).toBeNull();
    expect(body.decision.cooldown_wake_below).toBeNull();
    expect(body.decision.cooldown_notes).toContain('reclaim_look_read_only');
    expect(body.execRes.placed).toBe(false);

    const summary = await conversationSummary();
    expect(summary.filter((line) => line.includes('ai-gateway'))).toHaveLength(1);
    expect(summary.some((line) => line.includes('place-order'))).toBe(false);

    const text = await conversation();
    // The prompt carried the pool context; the tick logged its own stage.
    expect(text).toContain('"session_reclaim"');
    expect(text).toContain('prior_day_low');
    // The event was consumed after persistence (KV DEL of the event key).
    expect(text).toMatch(/DEL[^A-Z]*swing%3Asessionsweep%3Aevent|"DEL"/);

    await expect(text).toMatchFileSnapshot('./__snapshots__/live-session-reclaim.txt');
});
