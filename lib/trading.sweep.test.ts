import assert from 'node:assert/strict';
import test from 'node:test';

import { classifyPendingEntrySweep } from './trading';

// Fail-closed contract: a new entry order may only be placed after a sweep
// that CONFIRMS no previous entry order is still resting on the venue.

test('clean sweep: nothing resting', () => {
    assert.equal(classifyPendingEntrySweep({ found: 0, cancelled: 0, errors: [] }), null);
});

test('clean sweep: everything found was cancelled', () => {
    assert.equal(classifyPendingEntrySweep({ found: 1, cancelled: 1, errors: [] }), null);
    assert.equal(classifyPendingEntrySweep({ found: 3, cancelled: 3, errors: [] }), null);
});

test('sweep helper threw → exception, block entry', () => {
    assert.equal(classifyPendingEntrySweep(null), 'sweep_exception');
});

test('pending-orders fetch failed → cannot enumerate resting orders, block entry', () => {
    assert.equal(classifyPendingEntrySweep({ found: 0, cancelled: 0, errors: ['fetch timeout'] }), 'sweep_fetch_failed');
});

test('cancel failed without a fill → order may still rest, block entry', () => {
    assert.equal(
        classifyPendingEntrySweep({ found: 1, cancelled: 0, errors: ['43001 order not found'] }),
        'sweep_cancel_failed',
    );
    assert.equal(classifyPendingEntrySweep({ found: 2, cancelled: 1, errors: ['x'] }), 'sweep_cancel_failed');
});
