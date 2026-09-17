// Contract: the CONSUMING half of the session decision-window deferral.
//
// capital-session-window covers the arming side — a flat tick inside a window
// takes no decision and leaves an owed marker. This is what that marker buys:
// the first off-boundary cron tick after the window has ended spends a real AI
// call even though the primary-close cadence would otherwise have skipped it.
//
// The stage it records is the point. Owed looks used to log as 'decision',
// identical to the once-a-day scheduled look, which made the cadence
// unauditable — USDJPY on 2026-09-17 logged three indistinguishable 'decision'
// calls when only one was scheduled and two were owed looks released at the
// Tokyo and London opening-drive window ends. They now log as
// 'session_window_owed'.

import { expect, test, vi } from 'vitest';

import eurusdFixtureJson from '../fixtures/capital-EURUSD.json';
import { analyzePg, capitalFlatPrivateWorld, decisionBase, runAnalyzeTick } from './world';
import { conversation, conversationSummary, startBoundary } from '../../harness';
import { responsesDecides } from '../../harness/worlds/aiGateway';
import { forexFactoryCalendar } from '../../harness/worlds/forexFactory';
import { kvWorld } from '../../harness/worlds/kv';
import { marketauxNews } from '../../harness/worlds/news';
import { capitalMarketWorld } from '../../harness/worlds/recordedMarkets';

import type { RecordedMarketFixture } from '../../harness/worlds/recordedMarkets';

const fixture = eurusdFixtureJson as RecordedMarketFixture;

const OWED_LOOK_HOLD = decisionBase(
    'HOLD',
    'post-window look',
    'the window has passed and the level no longer justifies an entry',
);

startBoundary(
    () => ({
        http: [
            ...capitalMarketWorld(fixture),
            ...capitalFlatPrivateWorld(),
            // The marker the window gate left behind, with its window already
            // ended — this is what releases the look.
            ...kvWorld({
                'swing:sessionwindow:owed:capital:EURUSD': JSON.stringify({
                    windowEndMs: fixture.capturedAtMs - 5 * 60_000,
                }),
            }),
            marketauxNews([{ title: 'Euro steady ahead of ECB' }]),
            forexFactoryCalendar([]),
            responsesDecides(OWED_LOOK_HOLD),
        ],
        db: analyzePg,
    }),
    { nowMs: fixture.capturedAtMs },
);

test('the owed post-window look runs off-boundary and records its own stage', async () => {
    // Windows on, but the post-close span left at its default so THIS tick sits
    // outside one — an owed look is only read when no window is currently active.
    vi.stubEnv('SWING_SESSION_WINDOW_ENABLED', '1');

    const out = await runAnalyzeTick(
        {
            symbol: 'EURUSD',
            platform: 'capital',
            category: 'forex',
            dryRun: 'false',
            decisionPolicy: 'balanced',
        },
        { 'x-vercel-cron': '1' },
    );

    expect(out.statusCode).toBe(200);
    const body = out.body as Record<string, any>;
    // The cadence gate did NOT end this tick: the model was actually consulted.
    expect(body.promptSkipped).toBeUndefined();
    expect(body.decision.action).toBe('HOLD');

    const summary = await conversationSummary();
    expect(summary.some((line) => line.includes('ai-gateway.vercel.sh'))).toBe(true);

    const text = await conversation();
    // The marker is consumed, so the look is granted exactly once.
    expect(text).toContain('swing:sessionwindow:owed:capital:EURUSD');
    // …and the tick log names it for what it was, not a scheduled 'decision'.
    expect(text).toContain('session_window_owed');

    await expect(text).toMatchFileSnapshot('./__snapshots__/capital-session-window-owed.txt');
}, 60_000);
