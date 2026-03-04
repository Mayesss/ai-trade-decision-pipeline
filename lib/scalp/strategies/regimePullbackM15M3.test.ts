import assert from 'node:assert/strict';
import test from 'node:test';

import { getScalpStrategyConfig } from '../config';
import { buildScalpSessionWindows } from '../sessions';
import { createInitialScalpSessionState, deriveScalpDayKey } from '../stateMachine';
import { regimePullbackM15M3Strategy } from './regimePullbackM15M3';
import { regimePullbackM15M3XauusdGuardedStrategy } from './regimePullbackM15M3XauusdGuarded';

test('regime pullback strategy returns deterministic idle output on insufficient history', () => {
    const cfg = getScalpStrategyConfig();
    const nowMs = Date.UTC(2026, 0, 5, 10, 0, 0, 0);
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

    const phase = regimePullbackM15M3Strategy.applyPhaseDetectors({
        state,
        market: {
            symbol: 'EURUSD',
            epic: 'REPLAY:EURUSD',
            nowMs,
            quote: {
                price: 1.1,
                bid: 1.0999,
                offer: 1.1001,
                spreadAbs: 0.0002,
                spreadPips: 2,
                tsMs: nowMs,
            },
            baseTf: 'M15',
            confirmTf: 'M3',
            baseCandles: [
                [nowMs - 15 * 60_000, 1.1, 1.1002, 1.0998, 1.1001, 100],
                [nowMs, 1.1001, 1.1003, 1.0999, 1.1002, 100],
            ],
            confirmCandles: [
                [nowMs - 3 * 60_000, 1.1, 1.1001, 1.0999, 1.1, 50],
                [nowMs, 1.1, 1.1002, 1.0998, 1.1001, 50],
            ],
        },
        windows,
        nowMs,
        cfg,
    });

    assert.equal(phase.state.state, 'IDLE');
    assert.equal(phase.entryIntent, null);
    assert.ok(
        phase.reasonCodes.includes('REGIME_INSUFFICIENT_M15_CANDLES') || phase.reasonCodes.includes('SESSION_FILTER_OUTSIDE_BERLIN_WINDOWS'),
    );
});

test('xauusd guarded strategy blocks configured Berlin hour', () => {
    const cfg = getScalpStrategyConfig();
    const nowMs = Date.UTC(2026, 0, 5, 14, 0, 0, 0); // 15:00 Berlin (CET)
    const dayKey = deriveScalpDayKey(nowMs, cfg.sessions.clockMode);
    const windows = buildScalpSessionWindows({
        dayKey,
        clockMode: cfg.sessions.clockMode,
        asiaWindowLocal: cfg.sessions.asiaWindowLocal,
        raidWindowLocal: cfg.sessions.raidWindowLocal,
    });
    const state = createInitialScalpSessionState({
        symbol: 'XAUUSD',
        dayKey,
        nowMs,
        killSwitchActive: false,
    });

    const phase = regimePullbackM15M3XauusdGuardedStrategy.applyPhaseDetectors({
        state,
        market: {
            symbol: 'XAUUSD',
            epic: 'REPLAY:XAUUSD',
            nowMs,
            quote: {
                price: 2900,
                bid: 2899.9,
                offer: 2900.1,
                spreadAbs: 0.2,
                spreadPips: 2,
                tsMs: nowMs,
            },
            baseTf: 'M15',
            confirmTf: 'M3',
            baseCandles: [
                [nowMs - 15 * 60_000, 2900, 2902, 2898, 2901, 100],
                [nowMs, 2901, 2903, 2899, 2902, 100],
            ],
            confirmCandles: [
                [nowMs - 3 * 60_000, 2900, 2901, 2899, 2900.5, 50],
                [nowMs, 2900.5, 2901.5, 2899.5, 2901, 50],
            ],
        },
        windows,
        nowMs,
        cfg,
    });

    assert.equal(phase.state.state, 'IDLE');
    assert.equal(phase.entryIntent, null);
    assert.ok(phase.reasonCodes.includes('SESSION_FILTER_BLOCKED_BERLIN_HOUR'));
    assert.ok(phase.reasonCodes.includes('SESSION_FILTER_BLOCKED_BERLIN_HOUR_15'));
});
