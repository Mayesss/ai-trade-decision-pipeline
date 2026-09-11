// Contract: with the session decision-window gate ON, a FLAT Capital tick that
// lands OUTSIDE any window hands the model the windows AHEAD
// (market.session_windows_upcoming) and the sentence that tells it to size
// cooldown_minutes / cooldown_wake_confirm_minutes against them. DE40
// 2026-09-11 armed a 10-minute confirm at 04:02 UTC for a level that was
// unwatchable from 05:00 to 07:30 because the prompt only stated the rule in
// prose; this pins the concrete spans reaching the prompt.
//
// The EURUSD fixture is frozen at 18:56 UTC on a Friday: the London close
// window ended at 16:30 and no weekday event falls inside the next 24h, so the
// payload is the EMPTY list — the "none ahead" shape, which must still render
// (an absent key would read as "no gate").

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

const HOLD = decisionBase('HOLD', 'weekend ahead', 'no plan into the weekly close');

startBoundary(
    () => ({
        http: [
            ...capitalMarketWorld(fixture),
            ...capitalFlatPrivateWorld(),
            ...kvWorld(),
            marketauxNews([{ title: 'Euro steady ahead of ECB' }]),
            forexFactoryCalendar([]),
            responsesDecides(HOLD),
        ],
        db: analyzePg,
    }),
    { nowMs: fixture.capturedAtMs },
);

test('flat Capital tick outside a window: the windows ahead reach the prompt', async () => {
    vi.stubEnv('SWING_SESSION_WINDOW_ENABLED', '1');
    const out = await runAnalyzeTick({
        symbol: 'EURUSD',
        platform: 'capital',
        category: 'forex',
        dryRun: 'true',
    });

    expect(out.statusCode).toBe(200);
    const body = out.body as Record<string, any>;
    expect(body.promptSkipped).toBeUndefined();
    expect(body.decision.action).toBe('HOLD');

    const summary = await conversationSummary();
    expect(summary.filter((line) => line.includes('ai-gateway'))).toHaveLength(1);

    const text = await conversation();
    // The concrete spans (none inside the 24h horizon on a Friday evening)...
    // (Escaped quotes: the prompt JSON travels inside a JSON request body.)
    expect(text).toMatch(/session_windows_upcoming\\*":\[\]/);
    // ...and the sizing instruction that only renders with them.
    expect(text).toContain('market.session_windows_upcoming lists the windows');
    // The gate's own rule still renders alongside.
    expect(text).toContain('Session decision windows (enforced in code)');

    await expect(text).toMatchFileSnapshot('./__snapshots__/capital-flat-windows-ahead.txt');
}, 60_000);
