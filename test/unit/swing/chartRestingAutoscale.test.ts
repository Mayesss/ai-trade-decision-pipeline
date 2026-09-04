// components/ChartPanel PositionOverlayPrimitive.autoscaleInfo — resting-entry
// levels must widen the series' price range. The series autoscales off the
// candles alone, and a STOP entry rests BEYOND the traversed range by
// construction (a SELL stop under the low, a BUY stop over the high), so its
// dotted line and dashed window were painted outside the pane and never showed.
// A LIMIT rests inside the range, which is why only stop entries looked broken.

import assert from 'node:assert/strict';
import { test } from 'vitest';

import { PositionOverlayPrimitive } from '../../../components/ChartPanel';

const theme = {} as never;
const primitive = () => new PositionOverlayPrimitive(theme);

test('no resting anything leaves the price scale alone', () => {
    assert.equal(primitive().autoscaleInfo(), null);
});

test('a live SELL stop below the candles pulls the scale down to it', () => {
    const p = primitive();
    p.setRestingLevels([2426]);
    assert.deepEqual(p.autoscaleInfo()?.priceRange, { minValue: 2426, maxValue: 2426 });
});

test('window levels and live levels are both covered', () => {
    const p = primitive();
    p.setLimitOrders([{ fromTime: 1, toTime: 2, price: 2426, side: 'sell', filled: false }]);
    p.setRestingLevels([2610]);
    assert.deepEqual(p.autoscaleInfo()?.priceRange, { minValue: 2426, maxValue: 2610 });
});

test('cleared levels hand the scale back to the candles', () => {
    const p = primitive();
    p.setRestingLevels([2426]);
    p.setRestingLevels([]);
    assert.equal(p.autoscaleInfo(), null);
});

test('unusable levels are ignored', () => {
    const p = primitive();
    p.setRestingLevels([0, Number.NaN, -5]);
    assert.equal(p.autoscaleInfo(), null);
});
