import assert from 'node:assert/strict';
import test from 'node:test';

import { getScalpStrategyConfig } from '../config';
import { buildScalpSessionWindows } from '../sessions';
import { createInitialScalpSessionState, deriveScalpDayKey } from '../stateMachine';
import type { ScalpCandle } from '../types';
import { openingRangeBreakoutRetestM5M1Strategy } from './openingRangeBreakoutRetestM5M1';

test('opening-range breakout strategy emits entry signal on M5 breakout and M1 retest reclaim', () => {
    const cfg = getScalpStrategyConfig();
    const nowMs = Date.UTC(2026, 0, 6, 7, 32, 0, 0);
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

    const baseCandles: ScalpCandle[] = [
        [Date.UTC(2026, 0, 6, 5, 55, 0, 0), 1.0989, 1.0993, 1.0987, 1.0991, 100],
        [Date.UTC(2026, 0, 6, 6, 0, 0, 0), 1.0991, 1.0995, 1.0989, 1.0992, 100],
        [Date.UTC(2026, 0, 6, 6, 5, 0, 0), 1.0992, 1.0996, 1.099, 1.0993, 100],
        [Date.UTC(2026, 0, 6, 6, 10, 0, 0), 1.0993, 1.0997, 1.0991, 1.09935, 100],
        [Date.UTC(2026, 0, 6, 6, 15, 0, 0), 1.09935, 1.09975, 1.09915, 1.0994, 100],
        [Date.UTC(2026, 0, 6, 6, 20, 0, 0), 1.0994, 1.0998, 1.0992, 1.09945, 100],
        [Date.UTC(2026, 0, 6, 6, 25, 0, 0), 1.09945, 1.09985, 1.09925, 1.0995, 100],
        [Date.UTC(2026, 0, 6, 6, 30, 0, 0), 1.0995, 1.0999, 1.0993, 1.09955, 100],
        [Date.UTC(2026, 0, 6, 6, 35, 0, 0), 1.09955, 1.09995, 1.09935, 1.0996, 100],
        [Date.UTC(2026, 0, 6, 6, 40, 0, 0), 1.0996, 1.1, 1.0994, 1.09965, 100],
        [Date.UTC(2026, 0, 6, 6, 45, 0, 0), 1.09965, 1.10005, 1.09945, 1.0997, 100],
        [Date.UTC(2026, 0, 6, 6, 50, 0, 0), 1.0997, 1.1001, 1.0995, 1.09975, 100],
        [Date.UTC(2026, 0, 6, 6, 55, 0, 0), 1.09975, 1.10015, 1.09955, 1.0998, 100],
        [Date.UTC(2026, 0, 6, 7, 0, 0, 0), 1.09995, 1.10025, 1.09985, 1.10005, 100],
        [Date.UTC(2026, 0, 6, 7, 5, 0, 0), 1.10005, 1.1003, 1.09995, 1.10015, 100],
        [Date.UTC(2026, 0, 6, 7, 10, 0, 0), 1.10015, 1.10035, 1.1, 1.1002, 100],
        [Date.UTC(2026, 0, 6, 7, 15, 0, 0), 1.1002, 1.1003, 1.10008, 1.10022, 100],
        [Date.UTC(2026, 0, 6, 7, 20, 0, 0), 1.10022, 1.10135, 1.10018, 1.10122, 100],
        [Date.UTC(2026, 0, 6, 7, 25, 0, 0), 1.10122, 1.10128, 1.101, 1.10112, 100],
        [Date.UTC(2026, 0, 6, 7, 30, 0, 0), 1.10112, 1.1013, 1.10102, 1.1012, 100],
    ];
    const confirmCandles: ScalpCandle[] = [
        [Date.UTC(2026, 0, 6, 7, 21, 0, 0), 1.10122, 1.10125, 1.10105, 1.1011, 50],
        [Date.UTC(2026, 0, 6, 7, 22, 0, 0), 1.1011, 1.10112, 1.10065, 1.10078, 50],
        [Date.UTC(2026, 0, 6, 7, 23, 0, 0), 1.10078, 1.10092, 1.10028, 1.10042, 50],
        [Date.UTC(2026, 0, 6, 7, 24, 0, 0), 1.10042, 1.10058, 1.10018, 1.10048, 50],
        [Date.UTC(2026, 0, 6, 7, 25, 0, 0), 1.10048, 1.10088, 1.10036, 1.10072, 50],
        [Date.UTC(2026, 0, 6, 7, 26, 0, 0), 1.10072, 1.10102, 1.1006, 1.10092, 50],
        [Date.UTC(2026, 0, 6, 7, 27, 0, 0), 1.10092, 1.10112, 1.10082, 1.101, 50],
        [Date.UTC(2026, 0, 6, 7, 28, 0, 0), 1.101, 1.10116, 1.10088, 1.10105, 50],
        [Date.UTC(2026, 0, 6, 7, 29, 0, 0), 1.10105, 1.10118, 1.10092, 1.10108, 50],
        [Date.UTC(2026, 0, 6, 7, 30, 0, 0), 1.10108, 1.10122, 1.10098, 1.10112, 50],
        [Date.UTC(2026, 0, 6, 7, 31, 0, 0), 1.10112, 1.10124, 1.101, 1.10116, 50],
        [Date.UTC(2026, 0, 6, 7, 32, 0, 0), 1.10116, 1.10128, 1.10104, 1.1012, 50],
    ];

    const phase = openingRangeBreakoutRetestM5M1Strategy.applyPhaseDetectors({
        state,
        market: {
            symbol: 'EURUSD',
            epic: 'REPLAY:EURUSD',
            nowMs,
            quote: {
                price: 1.1012,
                bid: 1.1011,
                offer: 1.1013,
                spreadAbs: 0.0002,
                spreadPips: 2,
                tsMs: nowMs,
            },
            baseTf: 'M5',
            confirmTf: 'M1',
            baseCandles,
            confirmCandles,
        },
        windows,
        nowMs,
        cfg,
    });

    assert.equal(phase.state.state, 'WAITING_RETRACE');
    assert.deepEqual(phase.entryIntent, { model: 'ifvg_touch' });
    assert.ok(phase.reasonCodes.includes('OPENING_RANGE_READY'));
    assert.ok(phase.reasonCodes.includes('OPENING_RANGE_BREAKOUT_CONFIRMED'));
    assert.ok(phase.reasonCodes.includes('OPENING_RANGE_RETEST_RECLAIM_CONFIRMED'));
    assert.ok(phase.reasonCodes.includes('ENTRY_SIGNAL_READY'));
});
