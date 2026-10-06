// Contract: /api/swing/postmortem-drain after its retirement as a cron
// (2026-10-06). It is no longer in vercel.json nor in
// UNAUTHENTICATED_CRON_ROUTES — it is an ordinary admin route that drains the
// post-mortem queue on demand once SWING_POSTMORTEM_MODE is switched back on
// (docs/neon-compute-cost.md, "Postmortem drain").
//
// With the analyst off (the default since 2026-09-17) a manual call is still a
// no-op. The mode gates enqueue, not the queue: rows written before the switch
// flipped stay mature and claimable forever, so an ungated drain would make
// the exact AI calls the switch exists to stop. This asserts the drain claims
// nothing, reads nothing, and calls nothing: any outbound host or DB query
// here is an unhandled-request error by harness policy.

import { expect, test, vi } from 'vitest';

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

test('analyst off: a manual drain claims nothing and calls nothing', async () => {
    const req = createApiRequest({ path: '/api/swing/postmortem-drain' });
    const { res, state } = createApiResponse();
    await handler(req as never, res as never);

    expect(state.statusCode).toBe(200);
    expect(state.body).toMatchObject({ ok: true, processed: 0, note: 'postmortem_mode_off' });
});

// Off the unauthenticated-cron allow-list: with an admin secret configured, an
// unauthenticated call (what a Vercel cron sends) is refused before anything
// runs; the admin header gets through.
test('no longer an unauthenticated cron route: needs the admin secret', async () => {
    vi.stubEnv('ADMIN_ACCESS_SECRET', 'drain-test-secret');

    const anon = createApiResponse();
    await handler(createApiRequest({ path: '/api/swing/postmortem-drain' }) as never, anon.res as never);
    expect(anon.state.statusCode).toBe(401);

    const admin = createApiResponse();
    await handler(
        createApiRequest({
            path: '/api/swing/postmortem-drain',
            headers: { 'x-admin-access-secret': 'drain-test-secret' },
        }) as never,
        admin.res as never,
    );
    expect(admin.state.statusCode).toBe(200);
    expect(admin.state.body).toMatchObject({ ok: true, processed: 0, note: 'postmortem_mode_off' });
});
