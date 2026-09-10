// Contract: the session decision-window gate. A FLAT Capital tick that lands
// inside a venue window takes no decision — no news, no prompt, no AI — and
// withdraws the resting entry still standing (DELETE on the working order),
// then records the skip and the owed post-window look.
//
// The EURUSD fixture is frozen at 18:56 UTC on a Friday; the FX calendar's
// London / European cash close is 15:30 UTC, so a 240-minute post-close window
// (env, read at call time) puts this tick inside it. Windows are pinned OFF for
// every other scenario in test/harness/setup-env.ts.
import { expect, test, vi } from 'vitest';

import eurusdFixtureJson from '../fixtures/capital-EURUSD.json';
import { analyzePg, capitalFlatWithRestingEntryWorld, runAnalyzeTick } from './world';
import { conversation, conversationSummary, startBoundary } from '../../harness';
import { forexFactoryCalendar } from '../../harness/worlds/forexFactory';
import { kvWorld } from '../../harness/worlds/kv';
import { capitalMarketWorld } from '../../harness/worlds/recordedMarkets';

import type { RecordedMarketFixture } from '../../harness/worlds/recordedMarkets';

const fixture = eurusdFixtureJson as RecordedMarketFixture;

startBoundary(
    () => ({
        http: [
            ...capitalMarketWorld(fixture),
            // A BUY limit resting 40 minutes — the order the window must withdraw.
            ...capitalFlatWithRestingEntryWorld({ epic: 'EURUSD', level: 1.16, createdAtMs: fixture.capturedAtMs - 40 * 60_000 }),
            ...kvWorld(),
            forexFactoryCalendar([]),
        ],
        db: analyzePg,
    }),
    { nowMs: fixture.capturedAtMs },
);

test('flat tick inside a post-close window: resting entry withdrawn, no decision, owed look armed', async () => {
    vi.stubEnv('SWING_SESSION_WINDOW_ENABLED', '1');
    vi.stubEnv('SWING_SESSION_WINDOW_POST_CLOSE_MIN', '240');
    const out = await runAnalyzeTick({ symbol: 'EURUSD', platform: 'capital', category: 'forex', dryRun: 'false' });
    expect(out.statusCode).toBe(200);
    const body = out.body as Record<string, any>;
    expect(body.promptSkipped).toBe(true);
    expect(body.decision.summary).toBe('session_window');
    expect(body.decision.reason).toMatch(/^flat_skip_session_window_post_close_/);
    expect(body.decision.reason).toContain('_resting_entry_withdrawn');
    expect(body.sessionWindow).toMatchObject({ active: true, kind: 'post_close' });

    const summary = await conversationSummary();
    // The withdraw went to the venue…
    expect(summary.some((line) => line.includes('/api/v1/workingorders/wo-test-1'))).toBe(true);
    // …and nothing was asked of the model or the news feed.
    expect(summary.some((line) => line.includes('ai-gateway.vercel.sh'))).toBe(false);
    expect(summary.some((line) => line.includes('api.marketaux.com'))).toBe(false);

    const text = await conversation();
    expect(text).toContain('swing:sessionwindow:owed:capital:EURUSD');
    expect(text).toContain('session_window_gate');
    await expect(text).toMatchFileSnapshot('./__snapshots__/capital-session-window.txt');
}, 60_000);
