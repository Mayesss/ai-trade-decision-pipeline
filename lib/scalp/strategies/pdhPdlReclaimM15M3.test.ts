import assert from 'node:assert/strict';
import test from 'node:test';

import { getScalpStrategyConfig } from '../config';
import { buildScalpSessionWindows } from '../sessions';
import { createInitialScalpSessionState, deriveScalpDayKey } from '../stateMachine';
import type { ScalpCandle } from '../types';
import { pdhPdlReclaimM15M3Strategy } from './pdhPdlReclaimM15M3';

test('pdh/pdl reclaim strategy builds previous-day reference and stays idle before a sweep', () => {
    const cfg = getScalpStrategyConfig();
    const nowMs = Date.UTC(2026, 0, 6, 10, 0, 0, 0);
    const dayKey = deriveScalpDayKey(nowMs, cfg.sessions.clockMode);
    const windows = buildScalpSessionWindows({
        dayKey,
        clockMode: cfg.sessions.clockMode,
        asiaWindowLocal: cfg.sessions.asiaWindowLocal,
        raidWindowLocal: cfg.sessions.raidWindowLocal,
    });
    const state = createInitialScalpSessionState({
        symbol: 'EURUSD',
        dayKey,
        nowMs,
        killSwitchActive: false,
    });

    const previousDayBaseCandles: ScalpCandle[] = Array.from({ length: 12 }, (_, index) => {
        const candleTs = Date.UTC(2026, 0, 5, 6, 0, 0, 0) + index * 15 * 60_000;
        return [candleTs, 1.1, 1.101 + index * 0.00002, 1.099 - index * 0.00002, 1.1, 100];
    });
    const currentDayBaseCandles: ScalpCandle[] = [
        [Date.UTC(2026, 0, 6, 9, 0, 0, 0), 1.1, 1.1004, 1.0998, 1.1002, 100],
        [Date.UTC(2026, 0, 6, 9, 15, 0, 0), 1.1002, 1.1005, 1.0999, 1.1001, 100],
    ];
    const confirmCandles: ScalpCandle[] = [
        [Date.UTC(2026, 0, 6, 9, 54, 0, 0), 1.1001, 1.1002, 1.1, 1.10015, 50],
        [Date.UTC(2026, 0, 6, 9, 57, 0, 0), 1.10015, 1.10025, 1.10005, 1.1002, 50],
        [Date.UTC(2026, 0, 6, 10, 0, 0, 0), 1.1002, 1.1003, 1.1001, 1.10025, 50],
    ];

    const phase = pdhPdlReclaimM15M3Strategy.applyPhaseDetectors({
        state,
        market: {
            symbol: 'EURUSD',
            epic: 'REPLAY:EURUSD',
            nowMs,
            quote: {
                price: 1.10025,
                bid: 1.10015,
                offer: 1.10035,
                spreadAbs: 0.0002,
                spreadPips: 2,
                tsMs: nowMs,
            },
            baseTf: 'M15',
            confirmTf: 'M3',
            baseCandles: [...previousDayBaseCandles, ...currentDayBaseCandles],
            confirmCandles,
        },
        windows,
        nowMs,
        cfg,
    });

    assert.equal(phase.state.state, 'ASIA_RANGE_READY');
    assert.equal(phase.entryIntent, null);
    assert.ok(phase.state.asiaRange, 'expected previous-day reference to be cached');
    assert.ok(phase.reasonCodes.includes('PDH_PDL_REFERENCE_READY'));
    assert.ok(phase.reasonCodes.includes('NO_SWEEP_DETECTED'));
});
