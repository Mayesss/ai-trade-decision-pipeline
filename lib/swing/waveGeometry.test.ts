import assert from 'node:assert/strict';
import test from 'node:test';

import { computeWaveGeometry, computeNanoContext, findPivots } from './waveGeometry';

// Synthetic candles in the venues' array shape: [ts, open, high, low, close].
// Rising channel: +1/bar drift with a 16-bar sine wave (amplitude 6) on top.
function wave(bars: number, opts: { drift?: number; amp?: number; period?: number; phase?: number } = {}) {
    const { drift = 1, amp = 6, period = 16, phase = 0 } = opts;
    const out: any[] = [];
    for (let i = 0; i < bars; i++) {
        const close = 500 + drift * i + amp * Math.sin(((i + phase) * 2 * Math.PI) / period);
        const open = close - drift / 2;
        out.push([i * 60_000, open, Math.max(open, close) + 1, Math.min(open, close) - 1, close]);
    }
    return out;
}

test('too-short series returns null', () => {
    assert.equal(computeWaveGeometry(wave(20)), null);
    assert.equal(computeNanoContext(wave(10)), null);
    assert.equal(computeWaveGeometry(undefined), null);
});

test('rising channel: positive slope, crest near channel top, trough near bottom', () => {
    // phase chosen so the LAST bar sits at the sine crest / trough respectively
    const crest = computeWaveGeometry(wave(80, { phase: 4 - 79 + 16 * 5 }));
    const trough = computeWaveGeometry(wave(80, { phase: 12 - 79 + 16 * 5 }));
    assert.ok(crest && trough, 'geometry computed');
    assert.ok(crest!.slope_atr > 0, `slope_atr positive (${crest!.slope_atr})`);
    assert.ok(crest!.channel_pos > 0.7, `crest near channel top (${crest!.channel_pos})`);
    assert.ok(trough!.channel_pos < 0.3, `trough near channel bottom (${trough!.channel_pos})`);
});

test('pivots and trendlines exist on a wavy series and slope with the drift', () => {
    const g = computeWaveGeometry(wave(96));
    assert.ok(g);
    assert.ok(g!.support_trendline && g!.support_trendline.touches >= 2, 'support trendline fitted');
    assert.ok(g!.resistance_trendline && g!.resistance_trendline.touches >= 2, 'resistance trendline fitted');
    assert.ok(g!.support_trendline!.slope_atr > 0, 'rising support trendline');
    assert.ok(g!.last_swing_high && g!.last_swing_low, 'swing points present');
    assert.ok(g!.last_swing_high!.bars_ago >= 2 && g!.last_swing_low!.bars_ago >= 2, 'pivots need confirmation bars');
});

test('nano context: uptrend classifies UP bias and HH_HL structure', () => {
    const nano = computeNanoContext(wave(96, { drift: 1.2, amp: 4 }));
    assert.ok(nano);
    assert.equal(nano!.bias, 'UP');
    assert.equal(nano!.structure, 'HH_HL');
    assert.ok(Number.isFinite(nano!.extension_atr));
});

test('nano context: downtrend classifies DOWN bias and LH_LL structure', () => {
    const nano = computeNanoContext(wave(96, { drift: -1.2, amp: 4 }));
    assert.ok(nano);
    assert.equal(nano!.bias, 'DOWN');
    assert.equal(nano!.structure, 'LH_LL');
});

test('findPivots marks fractal highs and lows', () => {
    const candles = wave(64).map((c: any[]) => ({ high: c[2], low: c[3], close: c[4] }));
    const pivots = findPivots(candles as any);
    assert.ok(pivots.some((p) => p.kind === 'high'));
    assert.ok(pivots.some((p) => p.kind === 'low'));
});
