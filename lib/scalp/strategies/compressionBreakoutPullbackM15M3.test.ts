import assert from 'node:assert/strict';
import test from 'node:test';

import { getScalpStrategyConfig } from '../config';
import { buildScalpSessionWindows } from '../sessions';
import { createInitialScalpSessionState, deriveScalpDayKey } from '../stateMachine';
import type { ScalpCandle } from '../types';
import { compressionBreakoutPullbackM15M3Strategy } from './compressionBreakoutPullbackM15M3';

test('compression breakout strategy emits entry signal after breakout retest reclaim', () => {
    const cfg = getScalpStrategyConfig();
    const nowMs = Date.UTC(2026, 0, 6, 10, 9, 0, 0);
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
        [Date.UTC(2026, 0, 6, 5, 15, 0, 0), 1.0992, 1.0996, 1.0989, 1.0994, 100],
        [Date.UTC(2026, 0, 6, 5, 30, 0, 0), 1.0994, 1.0998, 1.0991, 1.0996, 100],
        [Date.UTC(2026, 0, 6, 5, 45, 0, 0), 1.0996, 1.1, 1.0993, 1.0998, 100],
        [Date.UTC(2026, 0, 6, 6, 0, 0, 0), 1.0998, 1.1001, 1.0994, 1.0997, 100],
        [Date.UTC(2026, 0, 6, 6, 15, 0, 0), 1.0997, 1.1002, 1.0995, 1.0999, 100],
        [Date.UTC(2026, 0, 6, 6, 30, 0, 0), 1.0999, 1.1003, 1.0996, 1.1001, 100],
        [Date.UTC(2026, 0, 6, 6, 45, 0, 0), 1.1001, 1.1005, 1.0998, 1.1002, 100],
        [Date.UTC(2026, 0, 6, 7, 0, 0, 0), 1.1002, 1.1006, 1.0999, 1.1003, 100],
        [Date.UTC(2026, 0, 6, 7, 15, 0, 0), 1.1003, 1.1007, 1.1, 1.1004, 100],
        [Date.UTC(2026, 0, 6, 7, 30, 0, 0), 1.1004, 1.1008, 1.1001, 1.1005, 100],
        [Date.UTC(2026, 0, 6, 7, 45, 0, 0), 1.1005, 1.1009, 1.1002, 1.1006, 100],
        [Date.UTC(2026, 0, 6, 8, 0, 0, 0), 1.1006, 1.101, 1.1003, 1.1007, 100],
        [Date.UTC(2026, 0, 6, 8, 15, 0, 0), 1.1007, 1.10095, 1.10045, 1.10075, 100],
        [Date.UTC(2026, 0, 6, 8, 30, 0, 0), 1.10075, 1.10098, 1.1005, 1.10078, 100],
        [Date.UTC(2026, 0, 6, 8, 45, 0, 0), 1.10078, 1.10099, 1.10052, 1.1008, 100],
        [Date.UTC(2026, 0, 6, 9, 0, 0, 0), 1.1008, 1.10102, 1.10055, 1.10082, 100],
        [Date.UTC(2026, 0, 6, 9, 15, 0, 0), 1.10082, 1.10103, 1.10058, 1.10084, 100],
        [Date.UTC(2026, 0, 6, 9, 30, 0, 0), 1.10084, 1.10185, 1.10074, 1.10175, 100],
    ];
    const confirmCandles: ScalpCandle[] = [
        [Date.UTC(2026, 0, 6, 9, 33, 0, 0), 1.10175, 1.10182, 1.1013, 1.10145, 50],
        [Date.UTC(2026, 0, 6, 9, 36, 0, 0), 1.10145, 1.1015, 1.10102, 1.10112, 50],
        [Date.UTC(2026, 0, 6, 9, 39, 0, 0), 1.10112, 1.10145, 1.10092, 1.10128, 50],
        [Date.UTC(2026, 0, 6, 9, 42, 0, 0), 1.10128, 1.10162, 1.10112, 1.1015, 50],
        [Date.UTC(2026, 0, 6, 9, 45, 0, 0), 1.1015, 1.10178, 1.1013, 1.1017, 50],
        [Date.UTC(2026, 0, 6, 9, 48, 0, 0), 1.1017, 1.1019, 1.10145, 1.10178, 50],
        [Date.UTC(2026, 0, 6, 9, 51, 0, 0), 1.10178, 1.10195, 1.10155, 1.10186, 50],
        [Date.UTC(2026, 0, 6, 9, 54, 0, 0), 1.10186, 1.102, 1.10165, 1.10192, 50],
        [Date.UTC(2026, 0, 6, 9, 57, 0, 0), 1.10192, 1.10205, 1.10172, 1.10196, 50],
        [Date.UTC(2026, 0, 6, 10, 0, 0, 0), 1.10196, 1.10208, 1.1018, 1.102, 50],
        [Date.UTC(2026, 0, 6, 10, 3, 0, 0), 1.102, 1.1021, 1.10185, 1.10202, 50],
        [Date.UTC(2026, 0, 6, 10, 6, 0, 0), 1.10202, 1.10214, 1.1019, 1.10205, 50],
        [Date.UTC(2026, 0, 6, 10, 9, 0, 0), 1.10205, 1.10218, 1.10195, 1.10208, 50],
    ];

    const phase = compressionBreakoutPullbackM15M3Strategy.applyPhaseDetectors({
        state,
        market: {
            symbol: 'EURUSD',
            epic: 'REPLAY:EURUSD',
            nowMs,
            quote: {
                price: 1.10208,
                bid: 1.10198,
                offer: 1.10218,
                spreadAbs: 0.0002,
                spreadPips: 2,
                tsMs: nowMs,
            },
            baseTf: 'M15',
            confirmTf: 'M3',
            baseCandles,
            confirmCandles,
        },
        windows,
        nowMs,
        cfg,
    });

    assert.equal(phase.state.state, 'WAITING_RETRACE');
    assert.deepEqual(phase.entryIntent, { model: 'ifvg_touch' });
    assert.ok(phase.reasonCodes.includes('COMPRESSION_WINDOW_DETECTED'));
    assert.ok(phase.reasonCodes.includes('PULLBACK_RETEST_CONFIRMED'));
    assert.ok(phase.reasonCodes.includes('ENTRY_SIGNAL_READY'));
});
