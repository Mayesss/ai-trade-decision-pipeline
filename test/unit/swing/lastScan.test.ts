// lib/swing/lastScan.ts — the KV command sequence each scan stamp issues.
// Upstash bills per command, so what matters here is not just the stored value
// but HOW MANY commands the tick spends: the tick-start stamp never does the
// ring-buffer housekeeping, and the outcome stamp does it about once an hour.
// The contract snapshots only ever capture fixtures frozen past :15, so this is
// the only place the housekeeping branch is exercised.

import assert from 'node:assert/strict';
import { afterEach, beforeEach, test, vi } from 'vitest';

const KV_URL = 'https://kv.unit.test';

type Sent = { command: string; args: unknown[] };

let sent: Sent[] = [];

// lib/kv.ts freezes its URL/token at module import, so the env has to be in
// place before the dynamic import below.
async function loadLastScan() {
    vi.stubEnv('upstash_payasyougo_KV_REST_API_URL', KV_URL);
    vi.stubEnv('upstash_payasyougo_KV_REST_API_TOKEN', 'unit-token');
    vi.resetModules();
    return import('../../../lib/swing/lastScan');
}

beforeEach(() => {
    sent = [];
    vi.stubGlobal('fetch', async (_url: string, init?: { body?: string }) => {
        const body = JSON.parse(String(init?.body || '[]')) as unknown[];
        sent.push({ command: String(body[0]), args: body.slice(1) });
        return new Response(JSON.stringify({ result: null }), { status: 200 });
    });
});

afterEach(() => {
    vi.unstubAllGlobals();
    vi.unstubAllEnvs();
    vi.useRealTimers();
});

const commands = () => sent.map((row) => row.command);

test('the tick-start stamp writes the marker and the log entry, never the housekeeping', async () => {
    const { stampSwingScanStarted } = await loadLastScan();
    // Minute 02 — inside the housekeeping window, which the start stamp still
    // must not act on: its outcome call, seconds later, owns the trim.
    vi.setSystemTime(new Date('2026-09-02T14:02:00Z'));
    await stampSwingScanStarted('bitget', 'ethusdt');
    assert.deepEqual(commands(), ['SETEX', 'LPUSH']);
    assert.equal(sent[0].args[0], 'swing:lastScan:v1:bitget:ETHUSDT');
    assert.equal(sent[1].args[0], 'swing:scanTicks:v1:bitget:ETHUSDT');
});

test('the outcome stamp trims and refreshes the TTL on the hourly tick', async () => {
    const { recordSwingLastScan } = await loadLastScan();
    vi.setSystemTime(new Date('2026-09-02T14:00:07Z'));
    await recordSwingLastScan('bitget', 'ETHUSDT', { stage: 'primary_close_gate', reason: 'flat' });
    assert.deepEqual(commands(), ['SETEX', 'LPUSH', 'LTRIM', 'EXPIRE']);
    // The stage rides on both the marker and the log entry, as the timeline reads it.
    assert.match(String(sent[0].args[2]), /primary_close_gate/);
    assert.match(String(sent[1].args[1]), /primary_close_gate/);
});

test('the outcome stamp skips housekeeping on the quarter ticks', async () => {
    const { recordSwingLastScan } = await loadLastScan();
    for (const minute of ['15', '30', '45']) {
        sent = [];
        vi.setSystemTime(new Date(`2026-09-02T14:${minute}:03Z`));
        await recordSwingLastScan('capital', 'GOLD', { stage: 'capital_market_closed' });
        assert.deepEqual(commands(), ['SETEX', 'LPUSH'], `minute ${minute} must not housekeep`);
    }
});

test('markers for the whole universe come back in one MGET', async () => {
    const { readSwingLastScanMany } = await loadLastScan();
    const rows = await readSwingLastScanMany([
        { platform: 'bitget', symbol: 'BTCUSDT' },
        { platform: 'capital', symbol: 'gold' },
    ]);
    assert.deepEqual(commands(), ['MGET']);
    assert.deepEqual(sent[0].args, ['swing:lastScan:v1:bitget:BTCUSDT', 'swing:lastScan:v1:capital:GOLD']);
    assert.deepEqual(rows, [null, null]);
});

test('an empty universe costs no command at all', async () => {
    const { readSwingLastScanMany } = await loadLastScan();
    assert.deepEqual(await readSwingLastScanMany([]), []);
    assert.deepEqual(commands(), []);
});
