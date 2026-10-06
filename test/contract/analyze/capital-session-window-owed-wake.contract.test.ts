// Contract: the owed session-window look released by the WAKE-WATCHER
// (2026-10-06). Since the analyze crons fire only inside the daily decision
// windows, no off-boundary cron tick follows most session windows any more —
// the watcher fires the owed look itself (reason session_window_owed, a plain
// wake=1 call) once the window is over. The analyze run must treat that wake
// fire as the owed look: consume the marker (so it is granted once, not again
// by a later tick) and record the 'session_window_owed' stage. Same world as
// capital-session-window-owed; only the request differs.

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

test('a wake fire after the window takes the owed look and consumes the marker', async () => {
    // Windows on, but the post-close span left at its default so THIS tick sits
    // outside one — an owed look is only read when no window is currently active.
    vi.stubEnv('SWING_SESSION_WINDOW_ENABLED', '1');

    // Exactly what wake-watch sends: no cron header, wake=1.
    const out = await runAnalyzeTick({
        symbol: 'EURUSD',
        platform: 'capital',
        decisionPolicy: 'balanced',
        wake: '1',
        dryRun: 'false',
    });

    expect(out.statusCode).toBe(200);
    const body = out.body as Record<string, any>;
    // The model was consulted.
    expect(body.promptSkipped).toBeUndefined();
    expect(body.decision.action).toBe('HOLD');

    const summary = await conversationSummary();
    expect(summary.some((line) => line.includes('ai-gateway.vercel.sh'))).toBe(true);

    const text = await conversation();
    // The marker is read AND deleted, so the look is granted exactly once.
    expect(text).toMatch(/"DEL",\s*"swing:sessionwindow:owed:capital:EURUSD"/);
    // …and the tick log names it for what it was, not a scheduled 'decision'.
    expect(text).toContain('session_window_owed');

    await expect(text).toMatchFileSnapshot('./__snapshots__/capital-session-window-owed-wake.txt');
}, 60_000);
