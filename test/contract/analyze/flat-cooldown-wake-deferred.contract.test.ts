// Contract: a DEFERRED cooldown wake — the watcher confirmed a sustained band,
// a schedule gate (session decision window / open warmup) refused the fired
// look and stamped wake_gate_held_at_ms, and the look finally runs later. The
// prompt must report the hold apart from the confirmation window: DE40
// 2026-09-11 (touch 05:31, fire refused 05:41, look 07:31) reached the model
// as "sustained 119 min" and was read as a stronger break, when 110 of those
// minutes were the pre-open gate. Pins sustained_minutes = the window the
// model asked for, gate_held_minutes = the hold, plus the DEFERRED doctrine.

import { expect, test } from 'vitest';

import btcFixtureJson from '../fixtures/bitget-BTCUSDT.json';
import { analyzePgWith, decisionBase, flatPrivateWorld, runAnalyzeTick } from './world';
import { conversation, conversationSummary, startBoundary } from '../../harness';
import { responsesDecides } from '../../harness/worlds/aiGateway';
import { forexFactoryCalendar } from '../../harness/worlds/forexFactory';
import { kvWorld } from '../../harness/worlds/kv';
import { coindeskNews } from '../../harness/worlds/news';
import { bitgetMarketWorld } from '../../harness/worlds/recordedMarkets';

import type { RecordedMarketFixture } from '../../harness/worlds/recordedMarkets';

const fixture = btcFixtureJson as RecordedMarketFixture;

// Captured lastPr ~77,543.7 sits ABOVE wake_above 77,000. The touch started
// 120 min ago, the watcher's fire was refused 110 min ago (10 min after the
// touch = exactly the confirm window), and the row is still fresh.
const COOLDOWN_ROW = {
    until_ms: fixture.capturedAtMs + 90 * 60_000,
    wake_above: 77_000,
    wake_below: 74_000,
    wake_note: 'Acceptance above 77000 = breakout long check',
    set_at_ms: fixture.capturedAtMs - 240 * 60_000,
    wake_sustain_minutes: 10,
    wake_touch_side: 'above',
    wake_touch_started_ms: fixture.capturedAtMs - 120 * 60_000,
    wake_touch_extreme: 77_400,
    wake_atr: 1000,
    wake_sweeps: [],
    reclaim_looked_at_ms: null,
    wake_gate_held_at_ms: fixture.capturedAtMs - 110 * 60_000,
};

const WAKE_HOLD = decisionBase(
    'HOLD',
    'deferred wake, structure has moved on',
    'the break confirmed two hours ago and price has since drifted; nothing to anchor to at the level now',
);

const wakePg = analyzePgWith((text) => {
    if (text.includes('FROM swing.ai_cooldowns') && text.trimStart().startsWith('SELECT')) return [COOLDOWN_ROW];
    // claimSwingAiCooldown: UPDATE ... SET claimed_until_ms ... RETURNING
    if (text.startsWith('UPDATE swing.ai_cooldowns') && text.includes('claimed_until_ms')) return [COOLDOWN_ROW];
    return undefined;
});

startBoundary(
    () => ({
        http: [
            ...bitgetMarketWorld(fixture),
            ...flatPrivateWorld(),
            ...kvWorld(),
            coindeskNews([{ title: 'Bitcoin holds above 77k', sentiment: 'POSITIVE' }]),
            forexFactoryCalendar([]),
            responsesDecides(WAKE_HOLD),
        ],
        db: wakePg,
    }),
    { nowMs: fixture.capturedAtMs },
);

test('a gate-held wake reports the confirm window and the hold separately', async () => {
    const out = await runAnalyzeTick({ symbol: 'BTCUSDT', platform: 'bitget', dryRun: 'false' });

    expect(out.statusCode).toBe(200);
    const body = out.body as Record<string, any>;
    expect(body.decision.action).toBe('HOLD');
    expect(body.promptSkipped).toBeFalsy();

    const summary = await conversationSummary();
    expect(summary.filter((line) => line.includes('ai-gateway'))).toHaveLength(1);
    expect(summary.some((line) => line.includes('place-order'))).toBe(false);

    const text = await conversation();
    // The window the model asked for, not the whole 120 minutes since the cross.
    // (The prompt JSON sits inside a JSON request body, so its quotes arrive
    // backslash-escaped in the conversation dump — match the field loosely.)
    expect(text).toMatch(/sustained_minutes\\*":10[,}]/);
    expect(text).toMatch(/crossed_minutes_ago\\*":120[,}]/);
    expect(text).toMatch(/gate_held_minutes\\*":110[,}]/);
    expect(text).not.toMatch(/sustained_minutes\\*":120[,}]/);
    // The deferred-wake doctrine renders only on this path.
    expect(text).toContain('DEFERRED wakes');

    await expect(text).toMatchFileSnapshot('./__snapshots__/flat-cooldown-wake-deferred.txt');
});
