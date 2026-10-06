// Contract: the wake-watcher's session-window upkeep (steps 5 and 6) — duties
// the 15-minute analyze cron performed as a side effect of ticking until the
// analyze schedule went daily (2026-10-06):
//   - a Capital resting entry still standing when a session decision window
//     opens fires the analyze run whose session gate withdraws it, once per
//     window;
//   - an owed session-window look (marker written by that gate) is fired once
//     its window is over, once per marker.
//
// Frozen at 2026-08-12 12:35 UTC (a watcher tick): US500 is inside its US pre-open window
// (11:30-13:30), EURUSD is in none. Tests run in file order: lib/capital.ts
// caches the session per worker.

import { http, HttpResponse } from 'msw';
import { beforeEach, expect, test, vi } from 'vitest';

import handler from '../../pages/api/swing/wake-watch';
import { getSwingAiThread } from '../../lib/swing/pg';
import { conversation, conversationSummary, startBoundary } from '../harness';
import { createApiRequest, createApiResponse } from '../harness/next';
import { installFakePg } from '../harness/pg';
import { resetEntries } from '../harness/recorder';
import { bitgetGet } from '../harness/worlds/bitget';
import { capitalGet, capitalSession } from '../harness/worlds/capital';
import { kvWorld } from '../harness/worlds/kv';

import type { PgResponder } from '../harness/pg';

const SELF_HOST = 'wake-watch.boundary.test';
const NOW_MS = Date.UTC(2026, 7, 12, 12, 35, 0);
const US_PRE_OPEN_START_MS = Date.UTC(2026, 7, 12, 11, 30, 0);

function wakePg(pendingEntries: Record<string, unknown>[]): PgResponder {
    return (text) => {
        const kind = text.split(' ')[0].toUpperCase();
        if (!['SELECT', 'INSERT', 'UPDATE', 'DELETE', 'WITH'].includes(kind)) return 0;
        if (text.includes('FROM swing.ai_cooldowns')) return [];
        if (text.includes("FROM swing.ai_threads WHERE status = 'pending_entry'")) return pendingEntries;
        if (text.includes('FROM swing.ai_threads')) return [];
        if (text.includes('FROM swing.break_triggers')) return [];
        throw new Error(`wake-watch pg world: unexpected query: ${text}`);
    };
}

const boundary = startBoundary(
    () => ({
        http: [
            ...kvWorld(),
            capitalSession(),
            capitalGet('/api/v1/positions', { positions: [] }),
            bitgetGet('/api/v2/mix/position/all-position', []),
            http.get(`https://${SELF_HOST}/api/swing/analyze`, () =>
                HttpResponse.json({ ok: true, decision: { action: 'HOLD' } }),
            ),
        ],
        db: wakePg([]),
    }),
    { nowMs: NOW_MS },
);

// Off in the harness by default (setup-env.ts); this file is about the window.
// Per test: the contract setup unstubs env after each one.
beforeEach(() => {
    vi.stubEnv('SWING_SESSION_WINDOW_ENABLED', '1');
});

async function runWakeWatch() {
    await getSwingAiThread('bitget', 'SCHEMA-WARMUP');
    resetEntries();
    const req = createApiRequest({ path: '/api/swing/wake-watch', headers: { host: SELF_HOST } });
    const { res, state } = createApiResponse();
    await handler(req as never, res as never);
    return state;
}

test('Capital resting entry inside a session window: one withdraw fire, budget spent for the window', async () => {
    installFakePg(wakePg([{ platform: 'capital', symbol: 'US500' }]));

    const out = await runWakeWatch();

    expect(out.statusCode).toBe(200);
    const body = out.body as Record<string, any>;
    expect(body.pendingEntriesChecked).toBe(1);
    expect(body.fired).toEqual([
        { platform: 'capital', symbol: 'US500', reason: 'session_window_pre_open', invoked: true, error: null },
    ]);
    const summary = await conversationSummary();
    const fire = summary.find((line) => line.includes(`${SELF_HOST}/api/swing/analyze`));
    // A plain wake fire: the analyze session gate does the withdraw.
    expect(fire).toContain('wake=1');
    expect(fire).not.toContain('reconcileOnly');
    // Inside a window the order book is not read — the gate decides.
    expect(summary.some((line) => line.includes('/api/v1/workingorders'))).toBe(false);
    const transcript = await conversation();
    expect(transcript).toContain(`swing:wakewatch:swwithdraw:capital:US500:${US_PRE_OPEN_START_MS}`);

    await expect(transcript).toMatchFileSnapshot('./__snapshots__/wake-watch-session-window.txt');
});

test('owed session-window look: fired once its window is over, never while a window is still active', async () => {
    boundary.use(
        ...kvWorld({
            // EURUSD's window ended at 12:00 — owed and releasable.
            'swing:sessionwindow:owed:capital:EURUSD': JSON.stringify({
                windowEndMs: Date.UTC(2026, 7, 12, 12, 0, 0),
                kind: 'pre_open',
                event: 'new_york_open',
                setAtMs: Date.UTC(2026, 7, 12, 10, 30, 0),
            }),
            // US500's marker is past its end too, but the US pre-open window is
            // active right now: the analyze gate would only re-park the look.
            'swing:sessionwindow:owed:capital:US500': JSON.stringify({
                windowEndMs: Date.UTC(2026, 7, 12, 11, 0, 0),
                kind: 'post_close',
                event: 'europe_cash_close',
                setAtMs: Date.UTC(2026, 7, 12, 10, 0, 0),
            }),
        }),
    );
    installFakePg(wakePg([]));

    const out = await runWakeWatch();

    const body = out.body as Record<string, any>;
    expect(body.owedLooksFired).toBe(1);
    expect(body.fired).toEqual([
        { platform: 'capital', symbol: 'EURUSD', reason: 'session_window_owed', invoked: true, error: null },
    ]);
    const transcript = await conversation();
    expect(transcript).toContain(`swing:wakewatch:swowed:capital:EURUSD:${Date.UTC(2026, 7, 12, 12, 0, 0)}`);
});
