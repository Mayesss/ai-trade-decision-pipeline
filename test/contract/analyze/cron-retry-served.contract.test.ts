// Contract: a production-shaped cron tick under the schedule that has run
// since 2026-10-06 — the analyze crons fire only on the hour, inside each
// venue's decision retry window (vercel.json: Bitget "0 0-5 * * *", Capital
// "0 8-13 * * 1-5"; DECISION_RETRY_WINDOW_MIN = 6h from DECISION_HOUR_UTC).
// Replaces cron-quarter: under that schedule a quarter tick never fires.
//
// The scenario is the one most window ticks are: the day's look was already
// served (KV served marker for today), so this tick is due for nothing and
// must end at the scheduled-look gate (not_primary_close) after the upkeep
// surface. Same production request shape as the crons send (x-vercel-cron,
// dryRun=false, decisionPolicy=balanced).
//
// The harness pins the 4H cadence (setup-env.ts); this file restores the
// production '1D' cadence and moves Bitget's decision hour to 18 UTC so the
// recorded fixture's own clock (18:24 UTC) sits inside the retry window —
// the tick is frozen at 19:00:05, the 19:00 retry tick, 36 minutes after
// capture. Also under test: the kill-switch read, the last-scan marker and
// its on-the-hour housekeeping, and the per-venue warm latch (finisher 1 of
// Bitget's 2 crons, so the summary warm must NOT run).

import { expect, test, vi } from 'vitest';

import ethFixtureJson from '../fixtures/bitget-ETHUSDT.json';
import { analyzePg, flatPrivateWorld, runAnalyzeTick } from './world';
import { conversation, conversationSummary, startBoundary } from '../../harness';
import { forexFactoryCalendar } from '../../harness/worlds/forexFactory';
import { kvWorld } from '../../harness/worlds/kv';
import { bitgetMarketWorld } from '../../harness/worlds/recordedMarkets';

import type { RecordedMarketFixture } from '../../harness/worlds/recordedMarkets';

// Before any import evaluates decisionConfig.ts (it reads these at load).
// stubEnv, not assignment: the harness unstubs after the test, so nothing
// leaks into the next file this worker runs.
vi.hoisted(() => {
    vi.stubEnv('SWING_DECISION_CADENCE', '1D');
    vi.stubEnv('SWING_DECISION_HOUR_UTC_BITGET', '18');
});

const fixture = ethFixtureJson as RecordedMarketFixture;
const capturedAt = new Date(fixture.capturedAtMs);
const RETRY_TICK_MS = Date.UTC(capturedAt.getUTCFullYear(), capturedAt.getUTCMonth(), capturedAt.getUTCDate(), 19, 0, 5);
const DAY_KEY = new Date(RETRY_TICK_MS).toISOString().slice(0, 10);

startBoundary(
    () => ({
        http: [
            ...bitgetMarketWorld(fixture),
            ...flatPrivateWorld(),
            // Today's look was served by the 18:00 tick.
            ...kvWorld({
                'swing:decision:served:v1:bitget:ETHUSDT': JSON.stringify({
                    day: DAY_KEY,
                    claimedAtMs: RETRY_TICK_MS - 3600_000,
                }),
            }),
            forexFactoryCalendar([]),
        ],
        db: analyzePg,
    }),
    { nowMs: RETRY_TICK_MS },
);

test('retry-window cron tick after the look was served: upkeep, gated skip, per-venue warm latch', async () => {
    const out = await runAnalyzeTick(
        {
            symbol: 'ETHUSDT',
            platform: 'bitget',
            newsSource: 'coindesk',
            category: 'crypto',
            dryRun: 'false',
            decisionPolicy: 'balanced',
        },
        { 'x-vercel-cron': '1' },
    );

    expect(out.statusCode).toBe(200);
    const body = out.body as Record<string, any>;
    expect(body.promptSkipped).toBe(true);
    expect(body.decision.summary).toBe('not_primary_close');

    const transcript = await conversation();
    const summary = await conversationSummary();
    // The served marker was read, and NOT re-claimed (no SETEX on it).
    expect(transcript).toContain('swing:decision:served:v1:bitget:ETHUSDT');
    expect(summary.some((line) => line.includes('ai-gateway.vercel.sh'))).toBe(false);
    // An on-the-hour tick is an 'hourly' tick, never a quarter one.
    expect(transcript).toContain('"hourly"');
    expect(transcript.includes('"quarter"')).toBe(false);
    // Latch counted per venue; finisher 1 of Bitget's 2 — no summary warm.
    expect(transcript).toContain(`swing:warm:latch:bitget:`);
    expect(transcript.includes('swing:warm:done:')).toBe(false);

    await expect(transcript).toMatchFileSnapshot('./__snapshots__/cron-retry-served.txt');
});
