import assert from 'node:assert/strict';
import test from 'node:test';

import { getScalpStrategyConfig } from '../config';
import { buildScalpSessionWindows } from '../sessions';
import { createInitialScalpSessionState, deriveScalpDayKey } from '../stateMachine';
import type { ScalpCandle } from '../types';
import { failedAuctionExtremeReversalM15M1Strategy } from './failedAuctionExtremeReversalM15M1';

test('failed-auction strategy emits entry signal on rejected high sweep and M1 retest failure', () => {
    const cfg = getScalpStrategyConfig();
    const nowMs = Date.UTC(2026, 0, 6, 7, 36, 0, 0);
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
        [Date.UTC(2026, 0, 6, 4, 0, 0, 0), 1.0992, 1.0997, 1.099, 1.0995, 100],
        [Date.UTC(2026, 0, 6, 4, 15, 0, 0), 1.0995, 1.0999, 1.0992, 1.0996, 100],
        [Date.UTC(2026, 0, 6, 4, 30, 0, 0), 1.0996, 1.1, 1.0994, 1.0998, 100],
        [Date.UTC(2026, 0, 6, 4, 45, 0, 0), 1.0998, 1.1001, 1.0995, 1.0999, 100],
        [Date.UTC(2026, 0, 6, 5, 0, 0, 0), 1.0999, 1.1002, 1.0996, 1.1, 100],
        [Date.UTC(2026, 0, 6, 5, 15, 0, 0), 1.1, 1.10025, 1.0997, 1.10005, 100],
        [Date.UTC(2026, 0, 6, 5, 30, 0, 0), 1.10005, 1.1003, 1.09975, 1.1001, 100],
        [Date.UTC(2026, 0, 6, 5, 45, 0, 0), 1.1001, 1.10035, 1.0998, 1.10012, 100],
        [Date.UTC(2026, 0, 6, 6, 0, 0, 0), 1.10012, 1.1004, 1.09985, 1.10018, 100],
        [Date.UTC(2026, 0, 6, 6, 15, 0, 0), 1.10018, 1.10045, 1.0999, 1.1002, 100],
        [Date.UTC(2026, 0, 6, 6, 30, 0, 0), 1.1002, 1.1005, 1.09995, 1.10024, 100],
        [Date.UTC(2026, 0, 6, 6, 45, 0, 0), 1.10024, 1.10055, 1.1, 1.10028, 100],
        [Date.UTC(2026, 0, 6, 7, 0, 0, 0), 1.10028, 1.10058, 1.10005, 1.1003, 100],
        [Date.UTC(2026, 0, 6, 7, 15, 0, 0), 1.1003, 1.1006, 1.10008, 1.10034, 100],
        [Date.UTC(2026, 0, 6, 7, 30, 0, 0), 1.10048, 1.10155, 1.1001, 1.10022, 100],
    ];
    const confirmCandles: ScalpCandle[] = [
        [Date.UTC(2026, 0, 6, 7, 31, 0, 0), 1.10022, 1.10038, 1.10016, 1.1003, 50],
        [Date.UTC(2026, 0, 6, 7, 32, 0, 0), 1.1003, 1.10048, 1.10022, 1.10042, 50],
        [Date.UTC(2026, 0, 6, 7, 33, 0, 0), 1.10042, 1.10058, 1.10028, 1.10033, 50],
        [Date.UTC(2026, 0, 6, 7, 34, 0, 0), 1.10033, 1.10045, 1.10016, 1.1002, 50],
        [Date.UTC(2026, 0, 6, 7, 35, 0, 0), 1.1002, 1.10028, 1.10008, 1.10016, 50],
        [Date.UTC(2026, 0, 6, 7, 36, 0, 0), 1.10016, 1.10022, 1.10002, 1.10012, 50],
    ];

    const phase = failedAuctionExtremeReversalM15M1Strategy.applyPhaseDetectors({
        state,
        market: {
            symbol: 'EURUSD',
            epic: 'REPLAY:EURUSD',
            nowMs,
            quote: {
                price: 1.10012,
                bid: 1.10002,
                offer: 1.10022,
                spreadAbs: 0.0002,
                spreadPips: 2,
                tsMs: nowMs,
            },
            baseTf: 'M15',
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
    assert.ok(phase.reasonCodes.includes('FAILED_AUCTION_HIGH_CONFIRMED'));
    assert.ok(phase.reasonCodes.includes('FAILED_AUCTION_RETEST_CONFIRMED'));
    assert.ok(phase.reasonCodes.includes('ENTRY_SIGNAL_READY'));
});
