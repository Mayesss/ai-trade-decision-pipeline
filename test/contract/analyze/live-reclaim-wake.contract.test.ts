// Contract: reclaim wake — a fresh, deep-enough sweep of the standing
// sustained band (price back INSIDE the bands, no crossing) claims the row's
// one-shot budget and produces a real AI look at the bounce moment, with
// market.reclaim_wake in the prompt. The look is READ-ONLY: the AI's HOLD
// (with attempted cooldown fields) must leave the row fully armed — no
// upsert, no delete — and its cooldown_* outputs are ignored. Only the
// atomic reclaim_looked_at_ms claim touches the row.

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

// Captured lastPr ~77,543 sits INSIDE the bands (above 78,000 / below 76,500 —
// no crossing). The newest sweep poked 78,000 to 78,320 (0.32 ATR deep at
// wake_atr 1000) and reclaimed 3 minutes ago: fresh, deep, budget unspent.
const COOLDOWN_ROW = {
    until_ms: fixture.capturedAtMs + 60 * 60_000,
    wake_above: 78_000,
    wake_below: 76_500,
    wake_note: 'Above 78000 held = breakout long check',
    set_at_ms: fixture.capturedAtMs - 45 * 60_000,
    wake_sustain_minutes: 10,
    wake_touch_side: null,
    wake_touch_started_ms: null,
    wake_touch_extreme: null,
    wake_atr: 1000,
    wake_sweeps: [
        {
            side: 'above',
            level: 78_000,
            touched_at_ms: fixture.capturedAtMs - 8 * 60_000,
            reclaimed_at_ms: fixture.capturedAtMs - 3 * 60_000,
            extreme: 78_320,
        },
    ],
    reclaim_looked_at_ms: null,
};

// The AI declines the bounce but tries to rewrite its plan — the read-only
// rule must ignore every cooldown_* field.
const HOLD_REWRITE = {
    ...decisionBase('HOLD', 'sweep noted, no confluence', 'mid-range poke, no HTF level backing the fade'),
    cooldown_minutes: 480,
    cooldown_wake_above: 79_000,
    cooldown_wake_note: 'this must be ignored',
};

const reclaimPg = analyzePgWith((text) => {
    if (text.includes('FROM swing.ai_cooldowns') && text.trimStart().startsWith('SELECT')) return [COOLDOWN_ROW];
    // claimSwingReclaimLook: UPDATE ... SET reclaim_looked_at_ms ... RETURNING
    if (text.startsWith('UPDATE swing.ai_cooldowns') && text.includes('reclaim_looked_at_ms')) {
        return [{ set_at_ms: COOLDOWN_ROW.set_at_ms }];
    }
    return undefined;
});

startBoundary(
    () => ({
        http: [
            ...bitgetMarketWorld(fixture),
            ...flatPrivateWorld(),
            ...kvWorld(),
            coindeskNews([{ title: 'Bitcoin rejects the breakout zone', sentiment: 'NEUTRAL' }]),
            forexFactoryCalendar([]),
            openAiDecides(HOLD_REWRITE),
        ],
        db: reclaimPg,
    }),
    { nowMs: fixture.capturedAtMs },
);

test('fresh sweep claims the one-shot look; HOLD leaves the band fully armed', async () => {
    const out = await runAnalyzeTick({ symbol: 'BTCUSDT', platform: 'bitget', dryRun: 'false' });

    expect(out.statusCode).toBe(200);
    const body = out.body as Record<string, any>;
    expect(body.decision.action).toBe('HOLD');
    // Read-only rule: the model's cooldown rewrite is ignored in code.
    expect(body.decision.cooldown_minutes).toBeNull();
    expect(body.decision.cooldown_wake_above).toBeNull();
    expect(body.decision.cooldown_notes).toContain('reclaim_look_read_only');
    expect(body.execRes.placed).toBe(false);

    const summary = await conversationSummary();
    // A real AI look happened (this is a judgment event, never mechanical).
    expect(summary.filter((line) => line.includes('ai-gateway'))).toHaveLength(1);
    expect(summary.some((line) => line.includes('place-order'))).toBe(false);

    const text = await conversation();
    // The one-shot claim is the ONLY write to the row: no delete, no re-arm.
    expect(text).toContain('reclaim_looked_at_ms');
    expect(text).not.toContain('DELETE FROM swing.ai_cooldowns');
    expect(text).not.toContain('INSERT INTO swing.ai_cooldowns');
    // The prompt carried the bounce-moment context.
    expect(text).toContain('"reclaim_wake"');

    await expect(text).toMatchFileSnapshot('./__snapshots__/live-reclaim-wake.txt');
});
