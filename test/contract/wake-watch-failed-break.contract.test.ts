// Contract: the wake-watcher's failed-break check. It only runs in the
// 10-minute window after a primary bar close, so this file freezes the clock
// at 08:05 UTC — five minutes after a 4H boundary. An armed break trigger on
// an OPEN long whose last closed 4H bar finished back below the trigger fires
// the analyze route with reason failed_break.

import { http, HttpResponse } from 'msw';
import { expect, test } from 'vitest';

import handler from '../../pages/api/swing/wake-watch';
import { getSwingAiThread } from '../../lib/swing/pg';
import { conversation, startBoundary } from '../harness';
import { createApiRequest, createApiResponse } from '../harness/next';
import { resetEntries } from '../harness/recorder';
import { bitgetGet } from '../harness/worlds/bitget';
import { capitalGet, capitalSession } from '../harness/worlds/capital';
import { kvWorld } from '../harness/worlds/kv';

import type { PgResponder } from '../harness/pg';

const SELF_HOST = 'wake-watch.boundary.test';

// Five minutes after the 08:00 UTC 4H close — inside the post-close window.
const NOW_MS = Date.UTC(2026, 7, 12, 8, 5, 0);
const H4 = 4 * 3600_000;

const TRIGGER_ROW = {
    platform: 'bitget',
    symbol: 'BTCUSDT',
    side: 'long',
    trigger_price: '77000',
    time_frame: '4H',
    entry_at_ms: String(NOW_MS - 12 * 3600_000),
};

// Ascending 4H bars: the last CLOSED bar opened 04:00 and closed 08:00 at
// 76,500 — back below the 77,000 trigger the long entered on.
const CANDLES = [
    [String(NOW_MS - 5 * 60_000 - 3 * H4), '77200', '77800', '77000', '77500', '100', '7000000'],
    [String(NOW_MS - 5 * 60_000 - 2 * H4), '77500', '77900', '77100', '77300', '100', '7000000'],
    [String(NOW_MS - 5 * 60_000 - H4), '77300', '77400', '76400', '76500', '100', '7000000'],
];

const wakePg: PgResponder = (text) => {
    const kind = text.split(' ')[0].toUpperCase();
    if (!['SELECT', 'INSERT', 'UPDATE', 'DELETE', 'WITH'].includes(kind)) return 0;
    if (text.includes('FROM swing.ai_cooldowns')) return [];
    if (text.includes('FROM swing.ai_threads')) return [];
    if (text.includes('FROM swing.break_triggers')) return [TRIGGER_ROW];
    if (kind === 'UPDATE' || kind === 'DELETE') return 1;
    throw new Error(`wake-watch pg world: unexpected query: ${text}`);
};

startBoundary(
    () => ({
        http: [
            ...kvWorld(),
            capitalSession(),
            capitalGet('/api/v1/positions', { positions: [] }),
            // The position the trigger belongs to is still open on the venue.
            bitgetGet('/api/v2/mix/position/all-position', [
                { symbol: 'BTCUSDT', markPrice: '76500', total: '0.05' },
            ]),
            bitgetGet('/api/v2/mix/market/candles', CANDLES),
            http.get(`https://${SELF_HOST}/api/swing/analyze`, () =>
                HttpResponse.json({ ok: true, decision: { action: 'HOLD' } }),
            ),
        ],
        db: wakePg,
    }),
    { nowMs: NOW_MS },
);

test('bar closed back through the trigger: failed_break fires the analyze route', async () => {
    await getSwingAiThread('bitget', 'SCHEMA-WARMUP');
    resetEntries();

    const req = createApiRequest({ path: '/api/swing/wake-watch', headers: { host: SELF_HOST } });
    const { res, state } = createApiResponse();
    await handler(req as never, res as never);

    expect(state.statusCode).toBe(200);
    const body = state.body as Record<string, any>;
    expect(body.breakTriggersChecked).toBe(1);
    expect(body.fired).toEqual([
        { platform: 'bitget', symbol: 'BTCUSDT', reason: 'failed_break', invoked: true, error: null },
    ]);

    await expect(await conversation()).toMatchFileSnapshot('./__snapshots__/wake-watch-failed-break.txt');
});
