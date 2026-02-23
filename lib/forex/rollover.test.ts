import assert from 'node:assert/strict';
import test from 'node:test';

import { isWithinPreRolloverWindow, minutesUntilNextUtcRollover } from './rollover';

test('minutesUntilNextUtcRollover computes remaining minutes to UTC rollover', () => {
    const at2350 = Date.UTC(2026, 1, 23, 23, 50, 0);
    const at0005 = Date.UTC(2026, 1, 24, 0, 5, 0);

    assert.equal(minutesUntilNextUtcRollover(at2350, 0), 10);
    assert.equal(minutesUntilNextUtcRollover(at0005, 0), 23 * 60 + 55);
});

test('isWithinPreRolloverWindow respects configured window size', () => {
    const at2330 = Date.UTC(2026, 1, 23, 23, 30, 0);
    const at2210 = Date.UTC(2026, 1, 23, 22, 10, 0);

    assert.equal(isWithinPreRolloverWindow(at2330, 45, 0), true);
    assert.equal(isWithinPreRolloverWindow(at2210, 45, 0), false);
});
