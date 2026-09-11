// Contract: the portfolio cap is a PRE-market, pre-AI gate. A flat BTCUSDT
// tick finds a crypto sibling (ETHUSDT) already in a position and stops
// before any market read, news fetch or model call — the conversation is the
// kill-switch read, the position/private reads, ONE ai_threads query, and the
// skip's persistence trail. No AI handler is registered: a model call here
// fails the test via msw error-on-unhandled.

import { expect, test } from 'vitest';

import btcFixtureJson from '../fixtures/bitget-BTCUSDT.json';
import { analyzePgWith, flatPrivateWorld, runAnalyzeTick } from './world';
import { conversation, startBoundary } from '../../harness';
import { kvWorld } from '../../harness/worlds/kv';

import type { RecordedMarketFixture } from '../../harness/worlds/recordedMarkets';

const fixture = btcFixtureJson as RecordedMarketFixture;

const ETH_IN_POSITION = { platform: 'bitget', symbol: 'ETHUSDT', status: 'in_position' };

startBoundary(
    () => ({
        http: [...flatPrivateWorld(), ...kvWorld()],
        db: analyzePgWith((text) =>
            text.includes('FROM swing.ai_threads') && text.includes("status IN ('pending_entry', 'in_position')")
                ? [ETH_IN_POSITION]
                : undefined,
        ),
    }),
    { nowMs: fixture.capturedAtMs },
);

test('flat tick with a same-class position standing: skipped before market data and the model', async () => {
    const out = await runAnalyzeTick({ symbol: 'BTCUSDT', platform: 'bitget', dryRun: 'true' });

    expect(out.statusCode).toBe(200);
    const body = out.body as Record<string, any>;
    expect(body.promptSkipped).toBe(true);
    expect(body.decision.action).toBe('HOLD');
    expect(body.decision.summary).toBe('asset_class_occupied');
    expect(body.decision.reason).toBe('asset_class_occupied:crypto:ETHUSDT');
    expect(body.execRes.placed).toBe(false);
    expect(body.execRes.reason).toBe('asset_class_occupied');

    const convo = await conversation();
    // Skip persisted with its stage; nothing market-side was read.
    expect(convo).toContain('INSERT INTO swing.decisions');
    expect(convo).toContain('asset_class_occupied');
    expect(convo).not.toContain('/api/v2/mix/market/candles');
    expect(convo).not.toContain('/v1/responses');

    await expect(convo).toMatchFileSnapshot('./__snapshots__/flat-position-cap.txt');
});
