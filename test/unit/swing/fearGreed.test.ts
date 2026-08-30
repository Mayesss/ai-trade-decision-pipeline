// lib/swing/fearGreed.ts — flag gating (default ON, kill switch), payload
// parsing, fail-open paths. KV cache behavior is covered by the contract
// suite (the unit project runs with KV unconfigured, so the module's cache
// try/catch fails open to a fetch here — itself a property worth the
// coverage). setup-env pins SWING_FEAR_GREED_ENABLED=false for every test,
// so each test states the flag it wants explicitly.

import assert from 'node:assert/strict';
import { afterEach, test, vi } from 'vitest';

import { loadFearGreedContext, swingFearGreedEnabled, FEAR_GREED_URL } from '../../../lib/swing/fearGreed';

afterEach(() => {
    vi.unstubAllEnvs();
});

type FetchCall = { url: string; init: RequestInit };

function stubFetch(responder: (call: FetchCall) => Response): { calls: FetchCall[]; restore: () => void } {
    const originalFetch = globalThis.fetch;
    const calls: FetchCall[] = [];
    globalThis.fetch = (async (input: RequestInfo | URL, init?: RequestInit) => {
        const call = { url: String(input), init: init ?? {} };
        calls.push(call);
        return responder(call);
    }) as typeof fetch;
    return { calls, restore: () => (globalThis.fetch = originalFetch) };
}

function fngResponse(data: unknown, status = 200): Response {
    return new Response(JSON.stringify({ name: 'Fear and Greed Index', data, metadata: { error: null } }), {
        status,
    });
}

test('flag semantics: default ON, explicit off values disable', () => {
    vi.stubEnv('SWING_FEAR_GREED_ENABLED', '');
    assert.equal(swingFearGreedEnabled(), true);
    for (const off of ['false', '0', 'no', 'off', ' OFF ']) {
        vi.stubEnv('SWING_FEAR_GREED_ENABLED', off);
        assert.equal(swingFearGreedEnabled(), false, `expected '${off}' to disable`);
    }
});

test('flag off: returns null without any fetch', async () => {
    vi.stubEnv('SWING_FEAR_GREED_ENABLED', 'false');
    const { calls, restore } = stubFetch(() => fngResponse([]));
    try {
        assert.equal(await loadFearGreedContext(), null);
        assert.equal(calls.length, 0);
    } finally {
        restore();
    }
});

test('parses newest-first rows (string values) into the context shape, history reversed oldest-first', async () => {
    vi.stubEnv('SWING_FEAR_GREED_ENABLED', 'true');
    const { calls, restore } = stubFetch(() =>
        fngResponse([
            { value: '72', value_classification: 'Greed', timestamp: '1788048000', time_until_update: '3600' },
            { value: '65', value_classification: 'Greed', timestamp: '1787961600' },
            { value: '48', value_classification: 'Neutral', timestamp: '1787875200' },
        ]),
    );
    try {
        const ctx = await loadFearGreedContext();
        assert.ok(ctx);
        assert.equal(ctx.value, 72);
        assert.equal(ctx.label, 'Greed');
        assert.deepEqual(ctx.daily_oldest_first, [48, 65, 72]);
        // 1788048000s = 2026-08-30T00:00:00Z
        assert.equal(ctx.updated_utc, '2026-08-30');
        assert.equal(calls.length, 1);
        assert.equal(calls[0].url, FEAR_GREED_URL);
    } finally {
        restore();
    }
});

test('single-row payload yields a one-element history', async () => {
    vi.stubEnv('SWING_FEAR_GREED_ENABLED', 'true');
    const { restore } = stubFetch(() =>
        fngResponse([{ value: '18', value_classification: 'Extreme Fear', timestamp: '1788048000' }]),
    );
    try {
        const ctx = await loadFearGreedContext();
        assert.ok(ctx);
        assert.equal(ctx.value, 18);
        assert.equal(ctx.label, 'Extreme Fear');
        assert.deepEqual(ctx.daily_oldest_first, [18]);
    } finally {
        restore();
    }
});

test('a malformed OLDER row is skipped, not fatal', async () => {
    vi.stubEnv('SWING_FEAR_GREED_ENABLED', 'true');
    const { restore } = stubFetch(() =>
        fngResponse([
            { value: '72', value_classification: 'Greed', timestamp: '1788048000' },
            { value: 'garbage', value_classification: 'Greed', timestamp: '1787961600' },
            { value: '48', value_classification: 'Neutral', timestamp: '1787875200' },
        ]),
    );
    try {
        const ctx = await loadFearGreedContext();
        assert.ok(ctx);
        assert.deepEqual(ctx.daily_oldest_first, [48, 72]);
    } finally {
        restore();
    }
});

test('HTTP failure fails open to null', async () => {
    vi.stubEnv('SWING_FEAR_GREED_ENABLED', 'true');
    const { restore } = stubFetch(() => fngResponse([], 500));
    try {
        assert.equal(await loadFearGreedContext(), null);
    } finally {
        restore();
    }
});

test('malformed rows fail open to null (out-of-range value, missing label)', async () => {
    vi.stubEnv('SWING_FEAR_GREED_ENABLED', 'true');
    for (const data of [
        [{ value: '250', value_classification: 'Greed', timestamp: '1788048000' }],
        [{ value: '50', value_classification: '', timestamp: '1788048000' }],
        [{ value: 'NaN', value_classification: 'Fear', timestamp: '1788048000' }],
        [],
        'not-an-array',
    ]) {
        const { restore } = stubFetch(() => fngResponse(data));
        try {
            assert.equal(await loadFearGreedContext(), null);
        } finally {
            restore();
        }
    }
});
