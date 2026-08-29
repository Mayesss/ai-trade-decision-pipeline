// Contract: mechanical wake-entry dead-end falls through to the AI. The
// captured BTCUSDT market is trend-down, so a confirmed breakOUT (above) fire
// builds a synthetic BUY that postprocessDecision's trend guard demotes — the
// route must then call the AI for a real decision instead of consuming the
// wake with a silent synthetic HOLD (which would also have enqueued a refusal
// investigation for a refusal the AI never made). The AI answers HOLD and
// tries to re-arm a band on the SIDE that just fired; the ratchet guard drops
// the band AND (since no band survives) the cooldown with it — full fold —
// and the claimed wake row is consumed.

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

// Captured lastPr ~77,543: a sustained breakOUT band at 77,000 confirmed
// (held 15m past its 10m window, extension ~0.5 ATR — inside the chase cap),
// but the market is trend-down: the synthetic BUY gets demoted.
const WAKE_LEVEL = 77_000;
const COOLDOWN_ROW = {
    until_ms: fixture.capturedAtMs + 60 * 60_000,
    wake_above: WAKE_LEVEL,
    wake_below: null,
    wake_note: 'Above 77000 held = breakout long check',
    set_at_ms: fixture.capturedAtMs - 45 * 60_000,
    wake_sustain_minutes: 10,
    wake_touch_side: 'above',
    wake_touch_started_ms: fixture.capturedAtMs - 15 * 60_000,
    wake_touch_extreme: 77_600,
    wake_atr: null,
    wake_sweeps: null,
};

// The fallback AI refuses and tries to keep watching the SAME side — the
// ratchet guard must eat both the band and the attached cooldown.
const HOLD_REARM = {
    ...decisionBase('HOLD', 'breakout into trend-down, pass', 'counter-regime break; wait for retest'),
    cooldown_minutes: 240,
    cooldown_wake_above: 78_500,
    cooldown_wake_note: 'continuation check above 78500',
    cooldown_wake_sustain_minutes: 15,
};

const wakePg = analyzePgWith((text) => {
    if (text.includes('FROM swing.ai_cooldowns') && text.trimStart().startsWith('SELECT')) return [COOLDOWN_ROW];
    if (text.startsWith('UPDATE swing.ai_cooldowns') && text.includes('RETURNING')) return [COOLDOWN_ROW];
    return undefined;
});

startBoundary(
    () => ({
        http: [
            ...bitgetMarketWorld(fixture),
            ...flatPrivateWorld(),
            ...kvWorld(),
            coindeskNews([{ title: 'Bitcoin squeezes against the downtrend', sentiment: 'POSITIVE' }]),
            forexFactoryCalendar([]),
            openAiDecides(HOLD_REARM),
        ],
        db: wakePg,
    }),
    { nowMs: fixture.capturedAtMs },
);

test('demoted mechanical entry falls back to the AI; same-side re-arm folds band + cooldown', async () => {
    const out = await runAnalyzeTick({ symbol: 'BTCUSDT', platform: 'bitget', dryRun: 'false' });

    expect(out.statusCode).toBe(200);
    const body = out.body as Record<string, any>;
    // The AI's decision, not the synthetic one — no mechanical cohort marker.
    expect(body.decision.action).toBe('HOLD');
    expect(body.decision.wake_auto_entry).toBeUndefined();
    expect(body.decision.response_id).not.toMatch(/^wake-auto-/);
    // Ratchet guard: fired-side re-arm dropped, and with no surviving band the
    // cooldown goes too (full fold — no blind quiet period the AI never signed).
    expect(body.decision.cooldown_wake_above).toBeNull();
    expect(body.decision.cooldown_wake_below).toBeNull();
    expect(body.decision.cooldown_minutes).toBeNull();
    expect(body.decision.cooldown_notes).toContain('wake_rearm_same_side_dropped');
    expect(body.execRes.placed).toBe(false);

    const summary = await conversationSummary();
    // Exactly one AI call (the fallback), no order placement.
    expect(summary.filter((line) => line.includes('ai-gateway'))).toHaveLength(1);
    expect(summary.some((line) => line.includes('place-order'))).toBe(false);

    const text = await conversation();
    // The claimed wake row is consumed (no fresh cooldown replaced it).
    expect(text).toContain('DELETE FROM swing.ai_cooldowns');
    expect(text).not.toContain('INSERT INTO swing.ai_cooldowns');

    await expect(text).toMatchFileSnapshot('./__snapshots__/live-wake-auto-fallback.txt');
});
