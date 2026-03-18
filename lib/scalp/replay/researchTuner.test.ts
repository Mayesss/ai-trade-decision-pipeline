import assert from 'node:assert/strict';
import test from 'node:test';

import { buildScalpResearchTuneVariants } from '../researchTuner';

test('buildScalpResearchTuneVariants includes baseline and respects max variant cap', () => {
    const variants = buildScalpResearchTuneVariants({
        symbol: 'BTCUSDT',
        strategyId: 'compression_breakout_pullback_m15_m3',
        maxVariantsPerStrategy: 4,
    });

    assert.equal(variants.length, 4);
    assert.equal(variants[0]?.tuneId, 'default');
    assert.ok(variants.some((row) => row.tuneId.startsWith('auto_')));
});

test('buildScalpResearchTuneVariants emits session-profile and risk variants for regime pullback', () => {
    const variants = buildScalpResearchTuneVariants({
        symbol: 'BTCUSDT',
        strategyId: 'regime_pullback_m15_m3',
        maxVariantsPerStrategy: 24,
    });

    const hasSessionProfileVariant = variants.some((row) => row.configOverride?.sessions?.entrySessionProfile !== undefined);
    const hasRiskVariant = variants.some(
        (row) =>
            row.configOverride?.risk?.trailAtrMult !== undefined ||
            row.configOverride?.risk?.timeStopBars !== undefined ||
            row.configOverride?.risk?.tp1ClosePct !== undefined,
    );

    assert.ok(hasSessionProfileVariant);
    assert.ok(hasRiskVariant);
});

test('buildScalpResearchTuneVariants keeps one session-profile variant when cap is tight', () => {
    const variants = buildScalpResearchTuneVariants({
        symbol: 'BTCUSDT',
        strategyId: 'regime_pullback_m15_m3',
        maxVariantsPerStrategy: 4,
    });

    assert.equal(variants.length, 4);
    assert.equal(variants[0]?.tuneId, 'default');
    assert.ok(variants.some((row) => row.configOverride?.sessions?.entrySessionProfile !== undefined));
});

test('buildScalpResearchTuneVariants can expand beyond legacy 20-variant cap', () => {
    const variants = buildScalpResearchTuneVariants({
        symbol: 'BTCUSDT',
        strategyId: 'compression_breakout_pullback_m15_m3',
        maxVariantsPerStrategy: 48,
    });

    assert.ok(variants.length > 20);
    assert.ok(variants.length <= 48);
});
