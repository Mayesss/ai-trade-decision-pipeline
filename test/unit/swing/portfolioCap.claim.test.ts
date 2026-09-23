import assert from 'node:assert/strict';
import { test } from 'vitest';

import { claimEntryCapacity, type EntryClaimStore, type PortfolioOccupant } from '../../../lib/swing/portfolioCap';

import type { SwingEntryClaim } from '../../../lib/swing/pg';

// In-memory swing.entry_claims with trySwingEntryClaim's semantics: a key is
// won when free, already held by the taker, or held by a dead claim (stale and
// holder has no committed thread). `threads` stands in for swing.ai_threads.
function memoryClaims(threads: Set<string> = new Set()) {
    const rows = new Map<string, SwingEntryClaim>();
    const STALE_MS = 5 * 60_000;
    let nowMs = 1_000_000;
    const holder = (c: SwingEntryClaim) => `${c.platform}:${c.symbol}`;
    const storeFor = (platform: string, symbol: string): EntryClaimStore => ({
        list: async () => [...rows.values()],
        tryClaim: async (claimKey) => {
            const row = rows.get(claimKey);
            const self = `${platform}:${symbol}`;
            const dead = row && row.claimedAtMs < nowMs - STALE_MS && !threads.has(holder(row));
            if (row && holder(row) !== self && !dead) return false;
            rows.set(claimKey, { claimKey, platform, symbol, claimedAtMs: nowMs });
            return true;
        },
        release: async (claimKey) => {
            const row = rows.get(claimKey);
            if (row && holder(row) === `${platform}:${symbol}` && !threads.has(holder(row))) rows.delete(claimKey);
        },
    });
    return { rows, storeFor, advance: (ms: number) => (nowMs += ms) };
}

const occ = (symbol: string, category: string, platform = 'bitget'): PortfolioOccupant => ({
    platform,
    symbol,
    category,
    status: 'pending_entry',
});

test('same-minute race: BTC and ETH both passed the pre-AI gate, only one may enter', async () => {
    const db = memoryClaims();
    // Both ticks read an empty book (no threads yet) — the 2026-09-23 case.
    const [btc, eth] = await Promise.all([
        claimEntryCapacity({ self: { platform: 'bitget', symbol: 'BTCUSDT', category: 'crypto' }, occupants: [], store: db.storeFor('bitget', 'BTCUSDT'), maxOpen: 4, onePerClass: true }),
        claimEntryCapacity({ self: { platform: 'bitget', symbol: 'ETHUSDT', category: 'crypto' }, occupants: [], store: db.storeFor('bitget', 'ETHUSDT'), maxOpen: 4, onePerClass: true }),
    ]);
    assert.equal([btc, eth].filter((v) => v.granted).length, 1);
    const loser = btc.granted ? eth : btc;
    assert.equal(!loser.granted && loser.stage, 'asset_class_claimed');
    // The loser must not keep a slot it cannot use.
    assert.equal([...db.rows.keys()].filter((k) => k.startsWith('slot:')).length, 1);
});

test('slots: concurrent entries across classes never exceed maxOpen', async () => {
    const db = memoryClaims();
    const symbols: Array<[string, string, string]> = [
        ['bitget', 'BTCUSDT', 'crypto'],
        ['capital', 'EURUSD', 'forex'],
        ['capital', 'US500', 'index'],
        ['capital', 'GOLD', 'commodity'],
        ['capital', 'TLT', 'bond'],
    ];
    const verdicts = await Promise.all(
        symbols.map(([platform, symbol, category]) =>
            claimEntryCapacity({ self: { platform, symbol, category }, occupants: [], store: db.storeFor(platform, symbol), maxOpen: 3, onePerClass: true }),
        ),
    );
    assert.equal(verdicts.filter((v) => v.granted).length, 3);
    const lost = verdicts.filter((v) => !v.granted);
    assert.ok(lost.every((v) => !v.granted && v.stage === 'position_cap_claimed'));
    // Losers released the class claims they took on the way.
    assert.equal([...db.rows.keys()].filter((k) => k.startsWith('class:')).length, 3);
});

test('threads without claims (pre-claim positions) block their class and shrink the slot range', async () => {
    const db = memoryClaims();
    const legacy = [occ('ETHUSDT', 'crypto'), occ('EURUSD', 'forex', 'capital')];
    const sibling = await claimEntryCapacity({ self: { platform: 'bitget', symbol: 'BTCUSDT', category: 'crypto' }, occupants: legacy, store: db.storeFor('bitget', 'BTCUSDT'), maxOpen: 3, onePerClass: true });
    assert.equal(!sibling.granted && sibling.reason, 'asset_class_claimed:crypto:ETHUSDT');

    // Two legacy threads under a cap of 3 leave exactly one claimable slot.
    const first = await claimEntryCapacity({ self: { platform: 'capital', symbol: 'US500', category: 'index' }, occupants: legacy, store: db.storeFor('capital', 'US500'), maxOpen: 3, onePerClass: true });
    assert.deepEqual(first.granted && first.claimKeys, ['class:index', 'slot:1']);
    const second = await claimEntryCapacity({ self: { platform: 'capital', symbol: 'GOLD', category: 'commodity' }, occupants: legacy, store: db.storeFor('capital', 'GOLD'), maxOpen: 3, onePerClass: true });
    assert.equal(!second.granted && second.stage, 'position_cap_claimed');
});

test('a symbol superseding its own resting entry re-takes its own claims', async () => {
    const threads = new Set(['bitget:BTCUSDT']);
    const db = memoryClaims(threads);
    const self = { platform: 'bitget', symbol: 'BTCUSDT', category: 'crypto' };
    await claimEntryCapacity({ self, occupants: [], store: db.storeFor('bitget', 'BTCUSDT'), maxOpen: 1, onePerClass: true });
    const again = await claimEntryCapacity({ self, occupants: [occ('BTCUSDT', 'crypto')], store: db.storeFor('bitget', 'BTCUSDT'), maxOpen: 1, onePerClass: true });
    assert.deepEqual(again.granted && again.claimKeys, ['class:crypto', 'slot:1']);
});

test('claims die only when stale AND their holder has no committed thread', async () => {
    const threads = new Set<string>();
    const db = memoryClaims(threads);
    const btc = { platform: 'bitget', symbol: 'BTCUSDT', category: 'crypto' };
    const eth = { platform: 'bitget', symbol: 'ETHUSDT', category: 'crypto' };
    assert.equal((await claimEntryCapacity({ self: btc, occupants: [], store: db.storeFor('bitget', 'BTCUSDT'), maxOpen: 4, onePerClass: true })).granted, true);
    threads.add('bitget:BTCUSDT');

    // Holder still committed, long after the grace: still held.
    db.advance(60 * 60_000);
    const held = await claimEntryCapacity({ self: eth, occupants: [occ('BTCUSDT', 'crypto')], store: db.storeFor('bitget', 'ETHUSDT'), maxOpen: 4, onePerClass: true });
    assert.equal(!held.granted && held.reason, 'asset_class_claimed:crypto:BTCUSDT');

    // Thread ended (position closed / entry dropped): the class frees up.
    threads.delete('bitget:BTCUSDT');
    const freed = await claimEntryCapacity({ self: eth, occupants: [], store: db.storeFor('bitget', 'ETHUSDT'), maxOpen: 4, onePerClass: true });
    assert.equal(freed.granted, true);
});

test('fresh claim without a thread yet (order in flight) still blocks', async () => {
    const db = memoryClaims();
    const btc = { platform: 'bitget', symbol: 'BTCUSDT', category: 'crypto' };
    const eth = { platform: 'bitget', symbol: 'ETHUSDT', category: 'crypto' };
    await claimEntryCapacity({ self: btc, occupants: [], store: db.storeFor('bitget', 'BTCUSDT'), maxOpen: 4, onePerClass: true });
    db.advance(60_000);
    assert.equal((await claimEntryCapacity({ self: eth, occupants: [], store: db.storeFor('bitget', 'ETHUSDT'), maxOpen: 4, onePerClass: true })).granted, false);
});

test('flags off: nothing is claimed', async () => {
    const db = memoryClaims();
    const v = await claimEntryCapacity({ self: { platform: 'bitget', symbol: 'BTCUSDT', category: 'crypto' }, occupants: [], store: db.storeFor('bitget', 'BTCUSDT'), maxOpen: 0, onePerClass: false });
    assert.deepEqual(v.granted && v.claimKeys, []);
    assert.equal(db.rows.size, 0);
});
