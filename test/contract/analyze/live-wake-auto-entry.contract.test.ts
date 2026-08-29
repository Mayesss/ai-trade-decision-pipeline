// Contract: wake auto-entry (native) — a LIVE flat tick whose cooldown wake
// band fired CONFIRMED (sustained 15m past its 10m window) executes the
// mechanical entry with NO AI call: the conversation must contain the
// place-order request (standard risk sizing, disaster stop, far TP backstop)
// plus the wake
// bookkeeping — claim lease, thread upsert, failed-break trigger at the level,
// cooldown consume — and must NOT contain any ai-gateway request. The captured
// BTCUSDT market is trend-down, so the confirmed break here is a breakDOWN
// (a mechanical BUY would be demoted by postprocessDecision's trend guard,
// which stays active for wake entries by design).

import { expect, test } from 'vitest';

import btcFixtureJson from '../fixtures/bitget-BTCUSDT.json';
import { analyzePgWith, flatPrivateWorld, runAnalyzeTick } from './world';
import { conversation, conversationSummary, startBoundary } from '../../harness';
import { bitgetPost } from '../../harness/worlds/bitget';
import { forexFactoryCalendar } from '../../harness/worlds/forexFactory';
import { kvWorld } from '../../harness/worlds/kv';
import { coindeskNews } from '../../harness/worlds/news';
import { bitgetMarketWorld } from '../../harness/worlds/recordedMarkets';

import type { RecordedMarketFixture } from '../../harness/worlds/recordedMarkets';

const fixture = btcFixtureJson as RecordedMarketFixture;

// Captured lastPr ~77,543: a sustained breakdown band at 78,000 has been
// crossed, its touch started 15 minutes ago (past the 10-minute window), and
// the plan horizon is still fresh — the wake arrives CONFIRMED by time.
const WAKE_LEVEL = 78_000;
const COOLDOWN_ROW = {
    until_ms: fixture.capturedAtMs + 60 * 60_000,
    wake_above: null,
    wake_below: WAKE_LEVEL,
    wake_note: 'Below 78000 held = breakdown short check',
    set_at_ms: fixture.capturedAtMs - 45 * 60_000,
    wake_sustain_minutes: 10,
    wake_touch_side: 'below',
    wake_touch_started_ms: fixture.capturedAtMs - 15 * 60_000,
    wake_touch_extreme: 77_500,
    wake_atr: null,
    wake_sweeps: null,
};

const wakePg = analyzePgWith((text) => {
    if (text.includes('FROM swing.ai_cooldowns') && text.trimStart().startsWith('SELECT')) return [COOLDOWN_ROW];
    // claimSwingAiCooldown: UPDATE ... RETURNING the row — winning the lease.
    if (text.startsWith('UPDATE swing.ai_cooldowns') && text.includes('RETURNING')) return [COOLDOWN_ROW];
    return undefined;
});

startBoundary(
    () => ({
        http: [
            ...bitgetMarketWorld(fixture),
            ...flatPrivateWorld(),
            ...kvWorld(),
            coindeskNews([{ title: 'Bitcoin loses range support', sentiment: 'NEGATIVE' }]),
            forexFactoryCalendar([]),
            // Deliberately NO ai-gateway handler: an AI call would fail the tick.
            bitgetPost('/api/v2/mix/account/set-margin-mode', {}),
            bitgetPost('/api/v2/mix/account/set-leverage', { symbol: 'BTCUSDT', leverage: '5' }),
            bitgetPost('/api/v2/mix/order/place-order', { orderId: 'order-wake-auto-1', clientOid: 'echo' }),
        ],
        db: wakePg,
    }),
    { nowMs: fixture.capturedAtMs },
);

test('confirmed wake fire: mechanical SELL executes with no AI call', async () => {
    const out = await runAnalyzeTick({ symbol: 'BTCUSDT', platform: 'bitget', dryRun: 'false' });

    expect(out.statusCode).toBe(200);
    const body = out.body as Record<string, any>;
    expect(body.decision.action).toBe('SELL');
    expect(body.dryRun).toBe(false);
    expect(body.execRes.placed).toBe(true);
    expect(body.execRes.orderId).toBe('order-wake-auto-1');

    // The mechanical cohort marker rides the decision for SQL and dashboards.
    expect(body.decision.wake_auto_entry).toMatchObject({
        confirmed_via: 'time',
        sustained_minutes: 15,
        level: WAKE_LEVEL,
    });
    expect(body.decision.response_id).toBe(`wake-auto-${fixture.capturedAtMs}`);
    // Standard sizing: the normal 10%-of-equity risk budget on 10k equity.
    expect(body.decision.risk_sizing.risk_usd).toBe(1000);
    // Failed-break watch armed at the broken level (the policy's real exit).
    expect(body.decision.entry_trigger_price).toBe(WAKE_LEVEL);

    const summary = await conversationSummary();
    expect(summary.filter((line) => line.includes('place-order'))).toHaveLength(1);
    expect(summary.some((line) => line.includes('ai-gateway'))).toBe(false);

    const text = await conversation();
    expect(text).toContain('INSERT INTO swing.ai_threads');
    expect(text).toContain('INSERT INTO swing.break_triggers');
    // The claimed wake row is consumed after the decision is durably recorded.
    expect(text).toContain('DELETE FROM swing.ai_cooldowns');

    await expect(text).toMatchFileSnapshot('./__snapshots__/live-wake-auto-entry.txt');
});
