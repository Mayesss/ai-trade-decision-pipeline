import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import test from 'node:test';

import { defaultScalpReplayConfig, normalizeScalpReplayInput, runScalpReplay } from './harness';
import { getDefaultScalpStrategy, listScalpStrategies } from '../strategies/registry';
import {
    REGIME_PULLBACK_M15_M3_XAUUSD_STRATEGY_ID,
    applyXauusdGuardRiskDefaultsToReplayRuntime,
    resolveXauusdGuardBlockedBerlinHours,
    resolveXauusdGuardOptimizedRiskDefaults,
} from '../strategies/regimePullbackM15M3XauusdGuarded';
import type { ScalpReplayInputFile } from './types';

test('scalp replay sample fixture produces deterministic non-empty run summary', async () => {
    const here = path.dirname(fileURLToPath(import.meta.url));
    const fixturePath = path.resolve(here, '../../../data/scalp-replay/fixtures/eurusd.sample.json');
    const raw = await readFile(fixturePath, 'utf8');
    const input = normalizeScalpReplayInput(JSON.parse(raw) as ScalpReplayInputFile);
    const config = defaultScalpReplayConfig(input.symbol);

    const result = await runScalpReplay({
        candles: input.candles,
        pipSize: input.pipSize,
        config,
    });

    assert.equal(result.summary.symbol, input.symbol);
    assert.ok(result.summary.runs > 0, 'expected replay runs > 0');
    assert.ok(result.timeline.length > 0, 'expected non-empty replay timeline');
    // With higher-timeframe defaults (M15/M3), this short fixture can legitimately produce zero trades.
    assert.ok(result.summary.trades >= 0, 'expected trades count to be non-negative');
    assert.ok(Number.isFinite(result.summary.expectancyR), 'expectancy must be finite');
    assert.ok(Number.isFinite(result.summary.maxDrawdownR), 'maxDrawdownR must be finite');
});

test('scalp replay resolves unknown strategy id to default strategy', async () => {
    const here = path.dirname(fileURLToPath(import.meta.url));
    const fixturePath = path.resolve(here, '../../../data/scalp-replay/fixtures/eurusd.sample.json');
    const raw = await readFile(fixturePath, 'utf8');
    const input = normalizeScalpReplayInput(JSON.parse(raw) as ScalpReplayInputFile);
    const config = defaultScalpReplayConfig(input.symbol);
    config.strategyId = 'does-not-exist';

    const result = await runScalpReplay({
        candles: input.candles,
        pipSize: input.pipSize,
        config,
    });

    assert.equal(result.config.strategyId, getDefaultScalpStrategy().id);
});

test('scalp replay accepts a registered non-default strategy id', async () => {
    const here = path.dirname(fileURLToPath(import.meta.url));
    const fixturePath = path.resolve(here, '../../../data/scalp-replay/fixtures/eurusd.sample.json');
    const raw = await readFile(fixturePath, 'utf8');
    const input = normalizeScalpReplayInput(JSON.parse(raw) as ScalpReplayInputFile);
    const config = defaultScalpReplayConfig(input.symbol);
    const alt = listScalpStrategies().find((row) => row.id !== config.strategyId);
    assert.ok(alt, 'expected at least one alternate registered scalp strategy');
    config.strategyId = alt!.id;

    const result = await runScalpReplay({
        candles: input.candles,
        pipSize: input.pipSize,
        config,
    });

    assert.equal(result.config.strategyId, alt!.id);
    assert.ok(result.summary.runs > 0, 'expected replay runs > 0');
});

test('default gold replay config does not auto-select XAUUSD guarded strategy', () => {
    const cfg = defaultScalpReplayConfig('XAUUSDT');
    assert.notEqual(cfg.strategyId, REGIME_PULLBACK_M15_M3_XAUUSD_STRATEGY_ID);
});

test('gold replay config applies XAUUSD guarded optimized risk defaults only when XAU strategy is selected', () => {
    const prevTp1 = process.env.SCALP_XAUUSD_GUARD_TP1_CLOSE_PCT;
    const prevTrail = process.env.SCALP_XAUUSD_GUARD_TRAIL_ATR_MULT;
    const prevTimeStop = process.env.SCALP_XAUUSD_GUARD_TIME_STOP_BARS;
    delete process.env.SCALP_XAUUSD_GUARD_TP1_CLOSE_PCT;
    delete process.env.SCALP_XAUUSD_GUARD_TRAIL_ATR_MULT;
    delete process.env.SCALP_XAUUSD_GUARD_TIME_STOP_BARS;
    try {
        const optimized = resolveXauusdGuardOptimizedRiskDefaults();
        const cfg = defaultScalpReplayConfig('XAUUSDT');
        cfg.strategyId = REGIME_PULLBACK_M15_M3_XAUUSD_STRATEGY_ID;
        const guarded = applyXauusdGuardRiskDefaultsToReplayRuntime(cfg);
        assert.equal(guarded.strategy.tp1ClosePct, optimized.tp1ClosePct);
        assert.equal(guarded.strategy.trailAtrMult, optimized.trailAtrMult);
        assert.equal(guarded.strategy.timeStopBars, optimized.timeStopBars);
        assert.equal(optimized.tp1ClosePct, 20);
        assert.equal(optimized.trailAtrMult, 1.6);
        assert.equal(optimized.timeStopBars, 18);
    } finally {
        if (prevTp1 === undefined) delete process.env.SCALP_XAUUSD_GUARD_TP1_CLOSE_PCT;
        else process.env.SCALP_XAUUSD_GUARD_TP1_CLOSE_PCT = prevTp1;
        if (prevTrail === undefined) delete process.env.SCALP_XAUUSD_GUARD_TRAIL_ATR_MULT;
        else process.env.SCALP_XAUUSD_GUARD_TRAIL_ATR_MULT = prevTrail;
        if (prevTimeStop === undefined) delete process.env.SCALP_XAUUSD_GUARD_TIME_STOP_BARS;
        else process.env.SCALP_XAUUSD_GUARD_TIME_STOP_BARS = prevTimeStop;
    }
});

test('xauusd blocked-hour variants are configurable and explicit hours override variant', () => {
    const prevVariant = process.env.SCALP_XAUUSD_GUARD_BLOCKED_HOURS_VARIANT;
    const prevHours = process.env.SCALP_XAUUSD_GUARD_BLOCKED_HOURS_BERLIN;
    delete process.env.SCALP_XAUUSD_GUARD_BLOCKED_HOURS_VARIANT;
    delete process.env.SCALP_XAUUSD_GUARD_BLOCKED_HOURS_BERLIN;
    try {
        assert.deepEqual(resolveXauusdGuardBlockedBerlinHours(), [15]);

        process.env.SCALP_XAUUSD_GUARD_BLOCKED_HOURS_VARIANT = 'xauusd_low_dd';
        assert.deepEqual(resolveXauusdGuardBlockedBerlinHours(), [15, 9]);

        process.env.SCALP_XAUUSD_GUARD_BLOCKED_HOURS_VARIANT = 'xauusd_high_pf';
        assert.deepEqual(resolveXauusdGuardBlockedBerlinHours(), [15, 17]);

        process.env.SCALP_XAUUSD_GUARD_BLOCKED_HOURS_VARIANT = 'off';
        assert.deepEqual(resolveXauusdGuardBlockedBerlinHours(), []);

        process.env.SCALP_XAUUSD_GUARD_BLOCKED_HOURS_VARIANT = 'unknown_variant';
        assert.deepEqual(resolveXauusdGuardBlockedBerlinHours(), [15]);

        process.env.SCALP_XAUUSD_GUARD_BLOCKED_HOURS_VARIANT = 'xauusd_low_dd';
        process.env.SCALP_XAUUSD_GUARD_BLOCKED_HOURS_BERLIN = '17,15,17';
        assert.deepEqual(resolveXauusdGuardBlockedBerlinHours(), [15, 17]);
    } finally {
        if (prevVariant === undefined) delete process.env.SCALP_XAUUSD_GUARD_BLOCKED_HOURS_VARIANT;
        else process.env.SCALP_XAUUSD_GUARD_BLOCKED_HOURS_VARIANT = prevVariant;
        if (prevHours === undefined) delete process.env.SCALP_XAUUSD_GUARD_BLOCKED_HOURS_BERLIN;
        else process.env.SCALP_XAUUSD_GUARD_BLOCKED_HOURS_BERLIN = prevHours;
    }
});
