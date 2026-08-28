// Contract: lib/kv.ts speaks Upstash REST — JSON command arrays against the
// bare base URL. The snapshot holds the full outgoing command stream.

import { expect, test } from 'vitest';

import { kvGetJson, kvIncr, kvListPushJson, kvListRangeJson, kvSetJson } from '../../lib/kv';
import { conversation, startBoundary } from '../harness';
import { kvWorld } from '../harness/worlds/kv';

startBoundary(() => ({ http: kvWorld() }));

test('JSON round-trip, atomic counter and capped list', async () => {
    await kvSetJson('swing:test:blob', { a: 1, b: 'x' }, 3600);
    expect(await kvGetJson('swing:test:blob')).toEqual({ a: 1, b: 'x' });

    expect(await kvIncr('swing:test:counter')).toBe(1);
    expect(await kvIncr('swing:test:counter')).toBe(2);

    await kvListPushJson('swing:test:list', { id: 1 });
    await kvListPushJson('swing:test:list', { id: 2 });
    expect(await kvListRangeJson('swing:test:list', 0, 9)).toEqual([{ id: 2 }, { id: 1 }]);

    await expect(await conversation()).toMatchFileSnapshot('./__snapshots__/kv-roundtrip.txt');
});
