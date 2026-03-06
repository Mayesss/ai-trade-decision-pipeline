import assert from 'node:assert/strict';
import test from 'node:test';

import { getScalpStrategyConfig } from '../config';
import { buildScalpSessionWindows } from '../sessions';
import { createInitialScalpSessionState, deriveScalpDayKey } from '../stateMachine';
import type { ScalpCandle } from '../types';
import { trendDayReaccelerationM15M3Strategy } from './trendDayReaccelerationM15M3';

test('trend-day reacceleration strategy emits entry signal after impulse follow-through and shallow reclaim', () => {
    const cfg = getScalpStrategyConfig();
    const nowMs = Date.UTC(2026, 0, 6, 9, 57, 0, 0);
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
        [Date.UTC(2026, 0, 6, 5, 45, 0, 0), 1.0992, 1.0998, 1.099, 1.0995, 100],
        [Date.UTC(2026, 0, 6, 6, 0, 0, 0), 1.0995, 1.1, 1.0993, 1.0997, 100],
        [Date.UTC(2026, 0, 6, 6, 15, 0, 0), 1.0997, 1.1001, 1.0994, 1.0998, 100],
        [Date.UTC(2026, 0, 6, 6, 30, 0, 0), 1.0998, 1.1002, 1.0995, 1.0999, 100],
        [Date.UTC(2026, 0, 6, 6, 45, 0, 0), 1.0999, 1.1003, 1.0996, 1.1, 100],
        [Date.UTC(2026, 0, 6, 7, 0, 0, 0), 1.1, 1.10035, 1.0997, 1.1001, 100],
        [Date.UTC(2026, 0, 6, 7, 15, 0, 0), 1.1001, 1.1004, 1.0998, 1.10015, 100],
        [Date.UTC(2026, 0, 6, 7, 30, 0, 0), 1.10015, 1.10045, 1.09985, 1.1002, 100],
        [Date.UTC(2026, 0, 6, 7, 45, 0, 0), 1.1002, 1.1005, 1.0999, 1.10025, 100],
        [Date.UTC(2026, 0, 6, 8, 0, 0, 0), 1.10025, 1.10055, 1.09995, 1.1003, 100],
        [Date.UTC(2026, 0, 6, 8, 15, 0, 0), 1.1003, 1.1006, 1.1, 1.10035, 100],
        [Date.UTC(2026, 0, 6, 8, 30, 0, 0), 1.10035, 1.10065, 1.10005, 1.1004, 100],
        [Date.UTC(2026, 0, 6, 8, 45, 0, 0), 1.1004, 1.10095, 1.10015, 1.1006, 100],
        [Date.UTC(2026, 0, 6, 9, 0, 0, 0), 1.1006, 1.10098, 1.1003, 1.10072, 100],
        [Date.UTC(2026, 0, 6, 9, 15, 0, 0), 1.10072, 1.10205, 1.10064, 1.10195, 100],
        [Date.UTC(2026, 0, 6, 9, 30, 0, 0), 1.10195, 1.10212, 1.10152, 1.10188, 100],
    ];
    const confirmCandles: ScalpCandle[] = [
        [Date.UTC(2026, 0, 6, 9, 18, 0, 0), 1.10195, 1.102, 1.10172, 1.1019, 50],
        [Date.UTC(2026, 0, 6, 9, 21, 0, 0), 1.1019, 1.10195, 1.10158, 1.10172, 50],
        [Date.UTC(2026, 0, 6, 9, 24, 0, 0), 1.10172, 1.10178, 1.10138, 1.10145, 50],
        [Date.UTC(2026, 0, 6, 9, 27, 0, 0), 1.10145, 1.10162, 1.10128, 1.10134, 50],
        [Date.UTC(2026, 0, 6, 9, 30, 0, 0), 1.10134, 1.10158, 1.10124, 1.10148, 50],
        [Date.UTC(2026, 0, 6, 9, 33, 0, 0), 1.10148, 1.10182, 1.10142, 1.10172, 50],
        [Date.UTC(2026, 0, 6, 9, 36, 0, 0), 1.10172, 1.10198, 1.10166, 1.10188, 50],
        [Date.UTC(2026, 0, 6, 9, 39, 0, 0), 1.10188, 1.10204, 1.1018, 1.10196, 50],
        [Date.UTC(2026, 0, 6, 9, 42, 0, 0), 1.10196, 1.10208, 1.10188, 1.102, 50],
        [Date.UTC(2026, 0, 6, 9, 45, 0, 0), 1.102, 1.1021, 1.10192, 1.10202, 50],
        [Date.UTC(2026, 0, 6, 9, 48, 0, 0), 1.10202, 1.10214, 1.10194, 1.10206, 50],
        [Date.UTC(2026, 0, 6, 9, 51, 0, 0), 1.10206, 1.10218, 1.10198, 1.1021, 50],
        [Date.UTC(2026, 0, 6, 9, 54, 0, 0), 1.1021, 1.10222, 1.10202, 1.10212, 50],
        [Date.UTC(2026, 0, 6, 9, 57, 0, 0), 1.10212, 1.10224, 1.10204, 1.10214, 50],
    ];

    const phase = trendDayReaccelerationM15M3Strategy.applyPhaseDetectors({
        state,
        market: {
            symbol: 'EURUSD',
            epic: 'REPLAY:EURUSD',
            nowMs,
            quote: {
                price: 1.10214,
                bid: 1.10204,
                offer: 1.10224,
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
    assert.ok(phase.reasonCodes.includes('TREND_DAY_BULL_IMPULSE_CONFIRMED'));
    assert.ok(phase.reasonCodes.includes('TREND_DAY_FOLLOW_THROUGH_CONFIRMED'));
    assert.ok(phase.reasonCodes.includes('TREND_DAY_PULLBACK_RECLAIM_CONFIRMED'));
    assert.ok(phase.reasonCodes.includes('ENTRY_SIGNAL_READY'));
});
