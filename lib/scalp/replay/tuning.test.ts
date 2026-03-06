import assert from 'node:assert/strict';
import test from 'node:test';

import { buildScalpConfigOverrideFromEffectiveConfig } from '../tuning';

test('buildScalpConfigOverrideFromEffectiveConfig extracts replay-tunable fields including optional timeframes', () => {
    const override = buildScalpConfigOverrideFromEffectiveConfig(
        {
            strategy: {
                asiaBaseTf: 'M5',
                confirmTf: 'M1',
                maxTradesPerDay: 3,
                riskPerTradePct: 0.4,
                takeProfitR: 1.7,
                tp1R: 0.8,
                tp1ClosePct: 30,
                trailStartR: 1.2,
                trailAtrMult: 1.6,
                timeStopBars: 12,
                sweepBufferPips: 0.2,
                mssLookbackBars: 4,
                ifvgEntryMode: 'first_touch',
            },
        },
        { includeTimeframes: true },
    );

    assert.deepEqual(override, {
        risk: {
            maxTradesPerSymbolPerDay: 3,
            riskPerTradePct: 0.4,
            takeProfitR: 1.7,
            tp1R: 0.8,
            tp1ClosePct: 30,
            trailStartR: 1.2,
            trailAtrMult: 1.6,
            timeStopBars: 12,
        },
        sweep: {
            bufferPips: 0.2,
        },
        confirm: {
            mssLookbackBars: 4,
        },
        ifvg: {
            entryMode: 'first_touch',
        },
        timeframes: {
            asiaBase: 'M5',
            confirm: 'M1',
        },
    });
});
