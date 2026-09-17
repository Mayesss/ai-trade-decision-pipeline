// Contract: the UNAUTHENTICATED cron drain (vercel.json, every 15 min) when
// the analyst is off — which is the default since 2026-09-17.
//
// The mode gates enqueue, not the queue. Rows written before the switch
// flipped stay mature and claimable forever, so an ungated drain would keep
// making the exact AI calls the switch exists to stop — quietly, on a cron,
// hours after the change. This asserts the drain claims nothing, reads
// nothing, and calls nothing: any outbound host or DB query here is an
// unhandled-request error by harness policy.

import { expect, test } from 'vitest';

import handler from '../../pages/api/swing/postmortem-drain';
import { createApiRequest, createApiResponse } from '../harness/next';
import { startBoundary } from '../harness';

import type { PgResponder } from '../harness/pg';

// Any query at all is a failure: the gate sits ahead of the claim UPDATE and
// ahead of the ai-health KV read.
const noPgTraffic: PgResponder = (text: string) => {
    throw new Error(`drain must not touch the database while the analyst is off: ${text.slice(0, 80)}`);
};

startBoundary({ http: [], db: noPgTraffic });

test('analyst off: the cron drain claims nothing and calls nothing', async () => {
    const req = createApiRequest({ path: '/api/swing/postmortem-drain' });
    const { res, state } = createApiResponse();
    await handler(req as never, res as never);

    expect(state.statusCode).toBe(200);
    expect(state.body).toMatchObject({ ok: true, processed: 0, note: 'postmortem_mode_off' });
});
