// Contract: flat cooldown-wake — price crossed the wake band the model attached
// to a previous flat HOLD, so this evaluation exists BECAUSE of that crossing.
// The tick claims the row, bypasses the flat quality gates, and the prompt
// carries market.cooldown_wake plus the wake block of SITUATIONAL DOCTRINE.
//
// This path had no snapshot until 2026-09-02, which is how a false claim about
// the pre-AI extension gate ("beyond ~N ATR a flat tick is not sent to you")
// survived in the prompt: the gate is bypassed on exactly this tick
// (analyze.ts, `&& !cooldownWakeActive`), so the sentence was falsifiable here
// and nowhere else. The snapshot now pins the wake prompt.

import { expect, test } from 'vitest';

import btcFixtureJson from '../fixtures/bitget-BTCUSDT.json';
import { analyzePgWith, decisionBase, flatPrivateWorld, runAnalyzeTick } from './world';
import { conversation, conversationSummary, startBoundary } from '../../harness';
import { openAiDecides } from '../../harness/worlds/aiGateway';
import { forexFactoryCalendar } from '../../harness/worlds/forexFactory';
import { kvWorld } from '../../harness/worlds/kv';
import { coindeskNews } from '../../harness/worlds/news';
import { bitgetMarketWorld } from '../../harness/worlds/recordedMarkets';

import type { RecordedMarketFixture } from '../../harness/worlds/recordedMarkets';

const fixture = btcFixtureJson as RecordedMarketFixture;

// Captured lastPr ~77,543.7 sits ABOVE wake_above 77,000 → the band is crossed.
// until_ms is still in the future, so this is a FRESH wake (not the expired
// path): the caller claims the row and bypasses the flat quality gates.
const COOLDOWN_ROW = {
    until_ms: fixture.capturedAtMs + 90 * 60_000,
    wake_above: 77_000,
    wake_below: 74_000,
    wake_note: 'Acceptance above 77000 = breakout long check',
    set_at_ms: fixture.capturedAtMs - 120 * 60_000,
    wake_sustain_minutes: null,
    wake_touch_side: null,
    wake_touch_started_ms: null,
    wake_touch_extreme: null,
    wake_atr: 1000,
    wake_sweeps: [],
    reclaim_looked_at_ms: null,
};

const WAKE_HOLD = decisionBase(
    'HOLD',
    'band crossed, structure not confirming',
    'price cleared 77000 but the 4H close has not held it; re-arm higher',
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
            coindeskNews([{ title: 'Bitcoin pushes through 77k', sentiment: 'POSITIVE' }]),
            forexFactoryCalendar([]),
            openAiDecides(WAKE_HOLD),
        ],
        db: wakePg,
    }),
    { nowMs: fixture.capturedAtMs },
);

test('a crossed wake band reaches the model with its plan note and wake doctrine', async () => {
    // dryRun=false: the cooldown branch in analyze.ts is gated on `!dryRun`,
    // so a dry tick never reads the row. The decision is a HOLD, so nothing is
    // placed either way, and the HTTP boundary is fully stubbed.
    const out = await runAnalyzeTick({ symbol: 'BTCUSDT', platform: 'bitget', dryRun: 'false' });

    expect(out.statusCode).toBe(200);
    const body = out.body as Record<string, any>;
    expect(body.decision.action).toBe('HOLD');
    expect(body.promptSkipped).toBeFalsy();

    const summary = await conversationSummary();
    expect(summary.filter((line) => line.includes('ai-gateway'))).toHaveLength(1);
    expect(summary.some((line) => line.includes('place-order'))).toBe(false);

    const text = await conversation();
    // The wake payload and the note the model attached when it set the band.
    expect(text).toContain('cooldown_wake');
    expect(text).toContain('Acceptance above 77000');
    // The wake block of SITUATIONAL DOCTRINE renders on this path.
    expect(text).toContain('Wake-band trigger');
    // ...and the prompt must NOT claim the pre-AI extension gate filtered this
    // tick, because on a wake tick it did not run.
    expect(text).not.toContain('not sent to you at all');

    await expect(text).toMatchFileSnapshot('./__snapshots__/flat-cooldown-wake.txt');
});
