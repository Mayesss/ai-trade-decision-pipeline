// Contract: the 1-minute wake-watcher (/api/swing/wake-watch). It does no AI
// work — it reads the armed wake state (cooldown bands, break triggers,
// in-position threads) from Postgres, compares against live venue prices, and
// FIRES the analyze route for crossed events by self-invoking
// {host}/api/swing/analyze?wake=1 through invokeCronEndpoint. With the
// base-URL envs scrubbed, that resolves from req.headers.host — so the fire
// itself is intercepted by msw and pinned in the snapshot.
//
// Tests run in file order on purpose: lib/capital.ts caches the session per
// worker, so only the first test's conversation carries POST /session.

import { http, HttpResponse } from 'msw';
import { expect, test } from 'vitest';

import handler from '../../pages/api/swing/wake-watch';
import { getSwingAiThread } from '../../lib/swing/pg';
import { conversation, conversationSummary, FIXED_NOW_MS, startBoundary } from '../harness';
import { createApiRequest, createApiResponse } from '../harness/next';
import { installFakePg } from '../harness/pg';
import { resetEntries } from '../harness/recorder';
import { bitgetGet } from '../harness/worlds/bitget';
import { capitalGet, capitalSession } from '../harness/worlds/capital';
import { kvWorld } from '../harness/worlds/kv';

import type { PgResponder } from '../harness/pg';

// The self-invocation target — resolveCronBaseUrl falls back to req.headers.host.
const SELF_HOST = 'wake-watch.boundary.test';

function analyzeFireEndpoint() {
    return http.get(`https://${SELF_HOST}/api/swing/analyze`, () =>
        HttpResponse.json({ ok: true, decision: { action: 'HOLD' } }),
    );
}

// One armed flat wake band: wake above 77,000 (instant, no sustain window).
const BAND_ROW = {
    platform: 'bitget',
    symbol: 'BTCUSDT',
    until_ms: String(FIXED_NOW_MS + 3600_000),
    wake_above: '77000',
    wake_below: null,
    wake_note: 'wake on range-high reclaim',
    set_at_ms: String(FIXED_NOW_MS - 30 * 60_000),
    wake_sustain_minutes: null,
    wake_touch_side: null,
    wake_touch_started_ms: null,
    wake_touch_extreme: null,
    wake_atr: '1200',
    wake_sweeps: null,
};

interface WakeState {
    cooldowns?: Record<string, unknown>[];
    threads?: Record<string, unknown>[];
    triggers?: Record<string, unknown>[];
}

function wakePg(state: WakeState): PgResponder {
    return (text) => {
        const kind = text.split(' ')[0].toUpperCase();
        if (!['SELECT', 'INSERT', 'UPDATE', 'DELETE', 'WITH'].includes(kind)) return 0; // schema bootstrap
        if (text.includes('FROM swing.ai_cooldowns')) return state.cooldowns ?? [];
        if (text.includes('FROM swing.ai_threads')) return state.threads ?? [];
        if (text.includes('FROM swing.break_triggers')) return state.triggers ?? [];
        if (kind === 'UPDATE' || kind === 'DELETE') return 1; // touch/sweep persistence, trigger cleanup
        throw new Error(`wake-watch pg world: unexpected query: ${text}`);
    };
}

// The baseline db responder is fixed at startBoundary time; scenarios swap the
// armed wake state by re-installing the pg fake for this test only.
function installWakeState(state: WakeState): void {
    installFakePg(wakePg(state));
}

// Baseline world: only what every scenario needs — the per-test wake state and
// venue answers are layered on with boundary.use().
const boundary = startBoundary(() => ({
    http: [
        ...kvWorld(),
        capitalSession(),
        capitalGet('/api/v1/positions', { positions: [] }),
        analyzeFireEndpoint(),
    ],
    db: wakePg({}),
}));

async function runWakeWatch(query: Record<string, string> = {}) {
    await getSwingAiThread('bitget', 'SCHEMA-WARMUP');
    resetEntries();
    const req = createApiRequest({ path: '/api/swing/wake-watch', query, headers: { host: SELF_HOST } });
    const { res, state } = createApiResponse();
    await handler(req as never, res as never);
    return state;
}

test('flat wake band crossed: live price read, guards, analyze self-fire', async () => {
    boundary.use(
        bitgetGet('/api/v2/mix/position/all-position', []),
        bitgetGet('/api/v2/mix/market/ticker', [{ symbol: 'BTCUSDT', lastPr: '77543.7' }]),
    );
    installWakeState({ cooldowns: [BAND_ROW] });

    const out = await runWakeWatch({ dryRun: '1' });

    expect(out.statusCode).toBe(200);
    const body = out.body as Record<string, any>;
    expect(body.ok).toBe(true);
    expect(body.bandsChecked).toBe(1);
    expect(body.fired).toEqual([
        { platform: 'bitget', symbol: 'BTCUSDT', reason: 'wake_band_above', invoked: true, error: null },
    ]);

    const summary = await conversationSummary();
    const fire = summary.find((line) => line.includes(`${SELF_HOST}/api/swing/analyze`));
    expect(fire).toContain('wake=1');
    expect(fire).toContain('dryRun=1');

    await expect(await conversation()).toMatchFileSnapshot('./__snapshots__/wake-watch-band-fire.txt');
});

test('kill switch on: the crossing is detected but the fire is suppressed', async () => {
    boundary.use(
        bitgetGet('/api/v2/mix/position/all-position', []),
        bitgetGet('/api/v2/mix/market/ticker', [{ symbol: 'BTCUSDT', lastPr: '77543.7' }]),
        ...kvWorld({
            'swing:cron:control:v1': JSON.stringify({
                hardDeactivated: true,
                reason: 'ops halt',
                updatedAtMs: FIXED_NOW_MS - 3600_000,
                updatedBy: 'test',
            }),
        }),
    );
    installWakeState({ cooldowns: [BAND_ROW] });

    const out = await runWakeWatch();

    const body = out.body as Record<string, any>;
    expect(body.fired).toEqual([
        {
            platform: 'bitget',
            symbol: 'BTCUSDT',
            reason: 'wake_band_above',
            invoked: false,
            error: 'blocked:swing_cron_hard_deactivated',
        },
    ]);
    const summary = await conversationSummary();
    expect(summary.some((line) => line.includes('/api/swing/analyze'))).toBe(false);
});

test('venue-side close: in_position thread with a flat venue fires the reconcile', async () => {
    boundary.use(bitgetGet('/api/v2/mix/position/all-position', []));
    installWakeState({
        threads: [{ platform: 'bitget', symbol: 'ETHUSDT', wake_above: null, wake_below: null }],
    });

    const out = await runWakeWatch();

    const body = out.body as Record<string, any>;
    expect(body.closesDetected).toBe(1);
    expect(body.fired).toEqual([
        { platform: 'bitget', symbol: 'ETHUSDT', reason: 'position_closed', invoked: true, error: null },
    ]);
});

test('in-position emergency move beyond the ATR threshold fires an early look', async () => {
    boundary.use(
        bitgetGet('/api/v2/mix/position/all-position', [
            { symbol: 'BTCUSDT', markPrice: '80000', total: '0.05' },
        ]),
        // Fresh AI-look reference: |80000 - 77543.7| / 1200 ≈ 2.05 ATR ≥ 1.5.
        ...kvWorld({
            'swing:wakewatch:ref:bitget:BTCUSDT': JSON.stringify({
                price: 77543.7,
                atr: 1200,
                ts: FIXED_NOW_MS - 10 * 60_000,
            }),
        }),
    );
    installWakeState({
        threads: [{ platform: 'bitget', symbol: 'BTCUSDT', wake_above: null, wake_below: null }],
    });

    const out = await runWakeWatch();

    const body = out.body as Record<string, any>;
    expect(body.positionsChecked).toBe(1);
    expect(body.fired).toHaveLength(1);
    expect(body.fired[0]).toMatchObject({
        platform: 'bitget',
        symbol: 'BTCUSDT',
        reason: 'emergency_move_2.05atr',
        invoked: true,
    });
});
