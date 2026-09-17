// Contract: a flat tick that KEEPS its resting entry keeps its thread.
//
// The thread lifecycle in analyze.ts branches on what this tick DID: placed an
// entry, managed a position, executed a full close. A routine "the limit is
// still good, leave it" HOLD did none of those, so it fell through to the
// catch-all and the thread was DELETED while the order was still live at the
// venue — 2026-09-17 USDJPY, limit resting at 155.34, the 08:00 look said "keep
// the resting BUY limit", and the row went anyway. Losing it drops the symbol
// out of the dashboard's pending-entry pill, stops the portfolio cap counting
// the committed capital, and throws away the conversation that placed the order.
//
// The distinction the branch rests on is `standingEntry` — the VENUE read, which
// analyze nulls whenever the order actually goes away (aged out, superseded,
// withdrawn). So this test pairs a live working order with a HOLD that does not
// touch it, and asserts the row survives as pending_entry.

import { expect, test } from 'vitest';

import eurusdFixtureJson from '../fixtures/capital-EURUSD.json';
import { analyzePgWith, capitalFlatWithRestingEntryWorld, decisionBase, runAnalyzeTick } from './world';
import { conversation, conversationSummary, startBoundary } from '../../harness';
import { responsesDecides } from '../../harness/worlds/aiGateway';
import { forexFactoryCalendar } from '../../harness/worlds/forexFactory';
import { kvWorld } from '../../harness/worlds/kv';
import { marketauxNews } from '../../harness/worlds/news';
import { capitalMarketWorld } from '../../harness/worlds/recordedMarkets';

import type { RecordedMarketFixture } from '../../harness/worlds/recordedMarkets';

const fixture = eurusdFixtureJson as RecordedMarketFixture;

// A plain flat HOLD: no entry, no withdraw_resting_entry. The order stands.
const KEEP_THE_LIMIT = decisionBase(
    'HOLD',
    'keep the resting limit',
    'the pullback thesis is unchanged; the BUY limit at 1.16 stays where it is',
);

// The thread the entry tick opened. platform/symbol are set so the portfolio-cap
// query reads this as the symbol's OWN occupancy (self never blocks itself)
// rather than as a foreign occupant of the forex class.
const PENDING_ENTRY_THREAD = {
    platform: 'capital',
    symbol: 'EURUSD',
    status: 'pending_entry',
    last_response_id: 'resp_entry-1',
    turns: 1,
    provider: 'openai',
    transcript: [
        { role: 'user', content: 'STATE: {"position":{"open":false,"status":"none"}}' },
        {
            role: 'assistant',
            content: '{"action":"BUY","summary":"pullback buy","reason":"resting limit into the 1.16 shelf"}',
        },
    ],
    wake_above: null,
    wake_below: null,
    wake_note: null,
    wake_set_at_ms: null,
};

startBoundary(
    () => ({
        http: [
            ...capitalMarketWorld(fixture),
            // Resting 40 minutes — well inside RESTING_ENTRY_MAX_AGE_MINUTES (48h),
            // so the age backstop does not cancel it and standingEntry stays set.
            ...capitalFlatWithRestingEntryWorld({
                epic: 'EURUSD',
                level: 1.16,
                createdAtMs: fixture.capturedAtMs - 40 * 60_000,
            }),
            ...kvWorld(),
            marketauxNews([{ title: 'Euro steady ahead of ECB' }]),
            forexFactoryCalendar([]),
            responsesDecides(KEEP_THE_LIMIT),
        ],
        db: analyzePgWith((text) => (text.includes('FROM swing.ai_threads') ? [PENDING_ENTRY_THREAD] : undefined)),
    }),
    { nowMs: fixture.capturedAtMs },
);

test('a flat HOLD over a live resting entry keeps the thread on pending_entry', async () => {
    const out = await runAnalyzeTick({
        symbol: 'EURUSD',
        platform: 'capital',
        category: 'forex',
        dryRun: 'false',
    });

    expect(out.statusCode).toBe(200);
    const body = out.body as Record<string, any>;
    expect(body.decision.action).toBe('HOLD');
    expect(body.execRes.placed).toBe(false);

    const summary = await conversationSummary();
    // The order was never touched: no withdraw, so it is still standing when the
    // thread bookkeeping runs. (If this ever flips, the assertions below stop
    // meaning what they say — the thread SHOULD end once nothing stands.)
    expect(summary.some((line) => line.includes('/api/v1/workingorders/wo-test-1'))).toBe(false);

    const text = await conversation();

    // The row is rewritten, not removed…
    expect(text).toContain('INSERT INTO swing.ai_threads');
    expect(text).not.toContain('DELETE FROM swing.ai_threads');

    // …and it is rewritten as pending_entry. Bounded to the thread statement's
    // own params: 'pending_entry' also appears in the prompt text
    // (state.position.cancelled_pending_entry) and would otherwise match there.
    const threadStart = text.indexOf('INSERT INTO swing.ai_threads');
    const nextStatement = text.indexOf('"text":', threadStart + 1);
    const threadWrite = text.slice(threadStart, nextStatement > 0 ? nextStatement : undefined);
    expect(threadWrite).toContain('pending_entry');
    expect(threadWrite).not.toContain('in_position');

    await expect(text).toMatchFileSnapshot('./__snapshots__/capital-resting-entry-kept.txt');
}, 60_000);
