import assert from 'node:assert/strict';
import test from 'node:test';

import { getScalpStrategyConfig } from '../config';
import { buildScalpSessionWindows } from '../sessions';
import { createInitialScalpSessionState, deriveScalpDayKey } from '../stateMachine';
import type { ScalpCandle } from '../types';
import { anchoredVwapReversionM15M3Strategy } from './anchoredVwapReversionM15M3';
import { basisDislocationReversionProxyM15M3Strategy } from './basisDislocationReversionProxyM15M3';
import { fundingOiExhaustionProxyM15M3Strategy } from './fundingOiExhaustionProxyM15M3';
import { relativeValueSpreadProxyM15M3Strategy } from './relativeValueSpreadProxyM15M3';
import { sessionSeasonalityBiasM15M3Strategy } from './sessionSeasonalityBiasM15M3';

function buildLinearCandles(params: {
    startTsMs: number;
    count: number;
    stepMs: number;
    startPrice: number;
    driftPerBar: number;
    rangeAbs: number;
    volume: number;
}): ScalpCandle[] {
    const out: ScalpCandle[] = [];
    let price = params.startPrice;
    for (let i = 0; i < params.count; i += 1) {
        const ts = params.startTsMs + i * params.stepMs;
        const open = price;
        const close = open + params.driftPerBar;
        const high = Math.max(open, close) + params.rangeAbs;
        const low = Math.min(open, close) - params.rangeAbs;
        out.push([ts, open, high, low, close, params.volume]);
        price = close;
    }
    return out;
}

function testContext(nowMs: number) {
    const cfg = getScalpStrategyConfig();
    const dayKey = deriveScalpDayKey(nowMs, cfg.sessions.clockMode);
    const windows = buildScalpSessionWindows({
        dayKey,
        clockMode: cfg.sessions.clockMode,
        asiaWindowLocal: cfg.sessions.asiaWindowLocal,
        raidWindowLocal: cfg.sessions.raidWindowLocal,
    });
    const state = createInitialScalpSessionState({
        symbol: 'BTCUSDT',
        dayKey,
        nowMs,
        killSwitchActive: false,
    });
    return { cfg, windows, state };
}

test('anchored VWAP reversion strategy can emit an entry signal', () => {
    const nowMs = Date.UTC(2026, 2, 10, 9, 57, 0, 0);
    const { cfg, windows, state } = testContext(nowMs);
    const base = buildLinearCandles({
        startTsMs: nowMs - 220 * 15 * 60_000,
        count: 220,
        stepMs: 15 * 60_000,
        startPrice: 100,
        driftPerBar: 0.02,
        rangeAbs: 0.05,
        volume: 120,
    });
    const confirm = buildLinearCandles({
        startTsMs: nowMs - 220 * 3 * 60_000,
        count: 220,
        stepMs: 3 * 60_000,
        startPrice: 100,
        driftPerBar: 0.001,
        rangeAbs: 0.015,
        volume: 80,
    });
    confirm[confirm.length - 2] = [nowMs - 3 * 60_000, 100.2, 100.24, 98.8, 98.95, 190];
    confirm[confirm.length - 1] = [nowMs, 98.95, 100.15, 98.9, 100.05, 210];

    const phase = anchoredVwapReversionM15M3Strategy.applyPhaseDetectors({
        state,
        market: {
            symbol: 'BTCUSDT',
            epic: 'REPLAY:BTCUSDT',
            nowMs,
            quote: {
                price: 99.96,
                bid: 99.95,
                offer: 99.97,
                spreadAbs: 0.02,
                spreadPips: 2,
                tsMs: nowMs,
            },
            baseTf: 'M15',
            confirmTf: 'M3',
            baseCandles: base,
            confirmCandles: confirm,
        },
        windows,
        nowMs,
        cfg,
    });

    assert.equal(phase.state.state, 'WAITING_RETRACE');
    assert.deepEqual(phase.entryIntent, { model: 'ifvg_touch' });
    assert.ok(phase.reasonCodes.includes('ENTRY_SIGNAL_READY'));
});

test('funding/OI proxy strategy can emit an entry signal', () => {
    const nowMs = Date.UTC(2026, 2, 10, 9, 57, 0, 0);
    const { cfg, windows, state } = testContext(nowMs);
    const base = buildLinearCandles({
        startTsMs: nowMs - 120 * 15 * 60_000,
        count: 120,
        stepMs: 15 * 60_000,
        startPrice: 200,
        driftPerBar: 0.01,
        rangeAbs: 0.06,
        volume: 100,
    });
    const confirm = buildLinearCandles({
        startTsMs: nowMs - 180 * 3 * 60_000,
        count: 180,
        stepMs: 3 * 60_000,
        startPrice: 200,
        driftPerBar: 0.001,
        rangeAbs: 0.02,
        volume: 90,
    });
    base[base.length - 2] = [nowMs - 15 * 60_000, 201.6, 201.7, 199.6, 199.7, 420];
    base[base.length - 1] = [nowMs, 199.7, 200.8, 199.68, 200.75, 200];
    confirm[confirm.length - 1] = [nowMs, 200.4, 200.9, 200.35, 200.82, 160];

    const phase = fundingOiExhaustionProxyM15M3Strategy.applyPhaseDetectors({
        state,
        market: {
            symbol: 'BTCUSDT',
            epic: 'REPLAY:BTCUSDT',
            nowMs,
            quote: {
                price: 200.82,
                bid: 200.8,
                offer: 200.84,
                spreadAbs: 0.04,
                spreadPips: 2,
                tsMs: nowMs,
            },
            baseTf: 'M15',
            confirmTf: 'M3',
            baseCandles: base,
            confirmCandles: confirm,
        },
        windows,
        nowMs,
        cfg,
    });

    assert.equal(phase.state.state, 'WAITING_RETRACE');
    assert.deepEqual(phase.entryIntent, { model: 'ifvg_touch' });
    assert.ok(phase.reasonCodes.includes('ENTRY_SIGNAL_READY'));
});

test('basis dislocation proxy strategy can emit an entry signal', () => {
    const nowMs = Date.UTC(2026, 2, 10, 9, 57, 0, 0);
    const { cfg, windows, state } = testContext(nowMs);
    const base = buildLinearCandles({
        startTsMs: nowMs - 180 * 15 * 60_000,
        count: 180,
        stepMs: 15 * 60_000,
        startPrice: 300,
        driftPerBar: 0.015,
        rangeAbs: 0.08,
        volume: 100,
    });
    const confirm = buildLinearCandles({
        startTsMs: nowMs - 200 * 3 * 60_000,
        count: 200,
        stepMs: 3 * 60_000,
        startPrice: 300,
        driftPerBar: 0.0005,
        rangeAbs: 0.03,
        volume: 80,
    });
    base[base.length - 1] = [nowMs, 302.5, 302.7, 296.4, 296.6, 140];
    confirm[confirm.length - 2] = [nowMs - 3 * 60_000, 296.6, 296.8, 296.45, 296.55, 110];
    confirm[confirm.length - 1] = [nowMs, 296.55, 297.3, 296.52, 297.2, 130];

    const phase = basisDislocationReversionProxyM15M3Strategy.applyPhaseDetectors({
        state,
        market: {
            symbol: 'BTCUSDT',
            epic: 'REPLAY:BTCUSDT',
            nowMs,
            quote: {
                price: 297.2,
                bid: 297.18,
                offer: 297.22,
                spreadAbs: 0.04,
                spreadPips: 2,
                tsMs: nowMs,
            },
            baseTf: 'M15',
            confirmTf: 'M3',
            baseCandles: base,
            confirmCandles: confirm,
        },
        windows,
        nowMs,
        cfg,
    });

    assert.equal(phase.state.state, 'WAITING_RETRACE');
    assert.deepEqual(phase.entryIntent, { model: 'ifvg_touch' });
    assert.ok(phase.reasonCodes.includes('ENTRY_SIGNAL_READY'));
});

test('relative-value proxy strategy can emit an entry signal', () => {
    const nowMs = Date.UTC(2026, 2, 10, 9, 57, 0, 0);
    const { cfg, windows, state } = testContext(nowMs);
    const base = buildLinearCandles({
        startTsMs: nowMs - 180 * 15 * 60_000,
        count: 180,
        stepMs: 15 * 60_000,
        startPrice: 50,
        driftPerBar: 0.05,
        rangeAbs: 0.08,
        volume: 100,
    });
    const confirm = buildLinearCandles({
        startTsMs: nowMs - 220 * 3 * 60_000,
        count: 220,
        stepMs: 3 * 60_000,
        startPrice: 50,
        driftPerBar: -0.01,
        rangeAbs: 0.04,
        volume: 80,
    });
    confirm[confirm.length - 9] = [nowMs - 24 * 60_000, 52.3, 52.4, 52.1, 52.25, 70];
    confirm[confirm.length - 2] = [nowMs - 3 * 60_000, 50.8, 50.92, 50.7, 50.78, 90];
    confirm[confirm.length - 1] = [nowMs, 50.78, 51.05, 50.75, 51.02, 120];

    const phase = relativeValueSpreadProxyM15M3Strategy.applyPhaseDetectors({
        state,
        market: {
            symbol: 'BTCUSDT',
            epic: 'REPLAY:BTCUSDT',
            nowMs,
            quote: {
                price: 51.02,
                bid: 51,
                offer: 51.04,
                spreadAbs: 0.04,
                spreadPips: 2,
                tsMs: nowMs,
            },
            baseTf: 'M15',
            confirmTf: 'M3',
            baseCandles: base,
            confirmCandles: confirm,
        },
        windows,
        nowMs,
        cfg,
    });

    assert.equal(phase.state.state, 'WAITING_RETRACE');
    assert.deepEqual(phase.entryIntent, { model: 'ifvg_touch' });
    assert.ok(phase.reasonCodes.includes('ENTRY_SIGNAL_READY'));
});

test('session seasonality strategy can emit an entry signal', () => {
    const nowMs = Date.UTC(2026, 2, 10, 9, 57, 0, 0);
    const { cfg, windows, state } = testContext(nowMs);
    const base = buildLinearCandles({
        startTsMs: nowMs - 220 * 15 * 60_000,
        count: 220,
        stepMs: 15 * 60_000,
        startPrice: 80,
        driftPerBar: 0.02,
        rangeAbs: 0.05,
        volume: 100,
    });
    const confirm = buildLinearCandles({
        startTsMs: nowMs - 320 * 3 * 60_000,
        count: 320,
        stepMs: 3 * 60_000,
        startPrice: 80,
        driftPerBar: 0.01,
        rangeAbs: 0.03,
        volume: 80,
    });
    confirm[confirm.length - 2] = [nowMs - 3 * 60_000, 83.9, 84.0, 83.84, 83.86, 95];
    confirm[confirm.length - 1] = [nowMs, 83.86, 84.28, 83.85, 84.24, 130];

    const phase = sessionSeasonalityBiasM15M3Strategy.applyPhaseDetectors({
        state,
        market: {
            symbol: 'BTCUSDT',
            epic: 'REPLAY:BTCUSDT',
            nowMs,
            quote: {
                price: 84.15,
                bid: 84.13,
                offer: 84.17,
                spreadAbs: 0.04,
                spreadPips: 2,
                tsMs: nowMs,
            },
            baseTf: 'M15',
            confirmTf: 'M3',
            baseCandles: base,
            confirmCandles: confirm,
        },
        windows,
        nowMs,
        cfg,
    });

    assert.equal(phase.state.state, 'WAITING_RETRACE');
    assert.deepEqual(phase.entryIntent, { model: 'ifvg_touch' });
    assert.ok(phase.reasonCodes.includes('ENTRY_SIGNAL_READY'));
});
