// Contract: lib/swing/pg.ts reaches Postgres only through the fake client the
// harness plants on global.__pgClient — no socket, no Neon HTTP. The world's
// db responder answers the handful of queries under test; the first access in
// a worker also runs the idempotent schema bootstrap (recorded like any other
// outgoing effect, then memoized for the process).

import { expect, test } from 'vitest';

import { getSwingAiThread, upsertSwingAiThread } from '../../lib/swing/pg';
import { conversation, startBoundary } from '../harness';

import type { PgResponder } from '../harness/pg';

const THREAD_ROW = {
    status: 'in_position',
    last_response_id: 'resp_prev-9',
    turns: 3,
    provider: 'openai',
    transcript: [{ role: 'user', content: 'prior tick state' }],
    wake_above: null,
    wake_below: null,
    wake_note: null,
    wake_set_at_ms: null,
};

const swingDb: PgResponder = (text) => {
    const kind = text.split(' ')[0].toUpperCase();
    // Schema bootstrap: everything that is not DML answers as a no-op DDL.
    if (!['SELECT', 'INSERT', 'UPDATE', 'DELETE', 'WITH'].includes(kind)) return 0;
    if (text.includes('FROM swing.ai_threads')) return [THREAD_ROW];
    if (text.startsWith('INSERT INTO swing.ai_threads')) return 1;
    throw new Error(`pg world: unexpected query: ${text}`);
};

startBoundary({ db: swingDb });

// Runs first in this file on purpose: it absorbs the one-time schema
// bootstrap so the following test records only its own queries.
test('first access bootstraps the swing schema, then reads the thread', async () => {
    const thread = await getSwingAiThread('capital', 'eurusd');

    expect(thread).toEqual({
        status: 'in_position',
        lastResponseId: 'resp_prev-9',
        turns: 3,
        provider: 'openai',
        transcript: [{ role: 'user', content: 'prior tick state' }],
        wakeAbove: null,
        wakeBelow: null,
        wakeNote: null,
        wakeSetAtMs: null,
    });

    const text = await conversation();
    expect(text).toContain('CREATE SCHEMA IF NOT EXISTS swing');
    expect(text).toContain('FROM swing.ai_threads');
});

test('a warmed process pays only the queries themselves', async () => {
    await upsertSwingAiThread({
        platform: 'capital',
        symbol: 'EURUSD',
        status: 'in_position',
        lastResponseId: 'resp_test-1',
        provider: 'openai',
        transcript: [{ role: 'user', content: 'this tick state' }],
    });
    await getSwingAiThread('capital', 'EURUSD');

    await expect(await conversation()).toMatchFileSnapshot('./__snapshots__/pg-ai-thread.txt');
});
