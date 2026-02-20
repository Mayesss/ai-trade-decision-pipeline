import assert from 'node:assert/strict';
import test from 'node:test';

import { regimeExclusiveModules } from './ai';

test('regimeExclusiveModules enforces deterministic module mapping', () => {
    assert.deepEqual(regimeExclusiveModules('trend_up'), ['pullback']);
    assert.deepEqual(regimeExclusiveModules('trend_down'), ['pullback']);
    assert.deepEqual(regimeExclusiveModules('high_vol'), ['breakout_retest']);
    assert.deepEqual(regimeExclusiveModules('range'), ['range_fade']);
    assert.deepEqual(regimeExclusiveModules('event_risk'), ['none']);
});
