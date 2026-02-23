import assert from 'node:assert/strict';
import test from 'node:test';

import { resolveReentryLockMinutes } from '../engine';
import { evaluateBreakoutRetestModule } from '../modules/breakoutRetest';
import { defaultReplayConfig, mergeReentryLockUntil, runReplay } from './harness';
import type { ReplayEntrySignal, ReplayQuote } from './types';
import type { ForexPairMetrics, ForexRegimePacket } from '../types';

function ts(iso: string): number {
    return Date.parse(iso);
}

function withNoSlippage(base = defaultReplayConfig('EURUSD')) {
    const cfg = { ...base };
    cfg.atr1hAbs = 0.003;
    cfg.slippage = {
        ...cfg.slippage,
        entryBaseBps: 0,
        exitBaseBps: 0,
        randomBps: 0,
        shockBps: 0,
        mediumEventBps: 0,
        highEventBps: 0,
    };
    return cfg;
}

test('replay stop checks use long stop side correctly and trigger only when bid crosses stop', () => {
    const cfg = withNoSlippage();
    const quotes: ReplayQuote[] = [
        { ts: ts('2026-02-23T10:00:00.000Z'), bid: 1.1, ask: 1.1002 },
        { ts: ts('2026-02-23T10:01:00.000Z'), bid: 1.0996, ask: 1.0999 },
        { ts: ts('2026-02-23T10:02:00.000Z'), bid: 1.0994, ask: 1.0997 },
    ];
    const entries: ReplayEntrySignal[] = [
        { ts: quotes[0]!.ts, side: 'BUY', stopPrice: 1.0995, notionalUsd: 1000 },
    ];

    const result = runReplay({ quotes, entries, config: cfg });
    const exits = result.ledger.filter((row) => row.kind === 'EXIT');
    assert.equal(exits.length, 1);
    assert.equal(exits[0]?.ts, quotes[2]?.ts);
    assert.ok(exits[0]?.reasonCodes.includes('STOP_INVALIDATED_LONG'));
});

test('replay can delay stop invalidation until minimum hold window elapses', () => {
    const cfg = withNoSlippage();
    cfg.management.minHoldMinutesBeforeStopInvalidation = 5;
    const quotes: ReplayQuote[] = [
        { ts: ts('2026-02-23T10:00:00.000Z'), bid: 1.1, ask: 1.1002 },
        { ts: ts('2026-02-23T10:02:00.000Z'), bid: 1.0994, ask: 1.0997 },
        { ts: ts('2026-02-23T10:03:00.000Z'), bid: 1.0993, ask: 1.0996 },
        { ts: ts('2026-02-23T10:05:00.000Z'), bid: 1.0992, ask: 1.0995 },
    ];
    const entries: ReplayEntrySignal[] = [
        { ts: quotes[0]!.ts, side: 'BUY', stopPrice: 1.0995, notionalUsd: 1000 },
    ];

    const result = runReplay({ quotes, entries, config: cfg });
    const exits = result.ledger.filter((row) => row.kind === 'EXIT');
    assert.equal(exits.length, 1);
    assert.equal(exits[0]?.ts, quotes[3]?.ts);
    assert.ok(exits[0]?.reasonCodes.includes('STOP_INVALIDATED_LONG'));
    const minHoldEvents = result.timeline.filter(
        (event) => event.type === 'POSITION_HELD' && event.reasonCodes.includes('STOP_INVALIDATION_MIN_HOLD_ACTIVE'),
    );
    assert.ok(minHoldEvents.length >= 1);
});

test('replay entry gate blocks transition-window spread stress using tightened spread-to-ATR cap', () => {
    const cfg = withNoSlippage();
    cfg.atr1hAbs = 0.0012; // spread_to_atr1h = 0.10 -> below base cap 0.12, above tightened 0.096
    const quotes: ReplayQuote[] = [
        { ts: ts('2026-02-23T07:00:00.000Z'), bid: 1.1, ask: 1.10012 },
    ];
    const entries: ReplayEntrySignal[] = [
        { ts: quotes[0]!.ts, side: 'BUY', stopPrice: 1.099, notionalUsd: 1000 },
    ];

    const result = runReplay({ quotes, entries, config: cfg });
    const blocked = result.timeline.find((event) => event.type === 'ENTRY_BLOCKED');
    assert.ok(blocked);
    assert.ok(blocked?.reasonCodes.includes('SESSION_TRANSITION_SPREAD_STRESS'));
    assert.ok(blocked?.reasonCodes.includes('SPREAD_TO_ATR_TRANSITION_CAP_EXCEEDED'));
});

test('replay blocks new entries inside configured pre-rollover window', () => {
    const cfg = withNoSlippage();
    cfg.rollover.entryBlockMinutes = 45;
    cfg.rollover.rolloverHourUtc = 0;

    const quotes: ReplayQuote[] = [
        { ts: ts('2026-02-23T23:30:00.000Z'), bid: 1.1, ask: 1.10008 },
    ];
    const entries: ReplayEntrySignal[] = [
        { ts: quotes[0]!.ts, side: 'BUY', stopPrice: 1.099, notionalUsd: 1000 },
    ];

    const result = runReplay({ quotes, entries, config: cfg });
    const blocked = result.timeline.find((event) => event.type === 'ENTRY_BLOCKED');
    assert.ok(blocked);
    assert.ok(blocked?.reasonCodes.includes('ROLLOVER_ENTRY_BLOCK_WINDOW'));
});

test('breakout-retest module requires breakout + retest + continuation confirmation', () => {
    const packet: ForexRegimePacket = {
        pair: 'EURUSD',
        generatedAtMs: Date.now(),
        regime: 'trend_up',
        permission: 'both',
        allowed_modules: ['breakout_retest'],
        risk_state: 'normal',
        confidence: 0.75,
        htf_context: {
            nearest_support: null,
            nearest_resistance: null,
            distance_to_support_atr1h: null,
            distance_to_resistance_atr1h: null,
        },
        notes_codes: [],
    };
    const metrics: ForexPairMetrics = {
        pair: 'EURUSD',
        epic: 'EURUSD',
        sessionTag: 'LONDON',
        price: 1.1,
        spreadAbs: 0.0001,
        spreadPips: 1,
        spreadToAtr1h: 0.05,
        atr1h: 0.001,
        atr4h: 0.002,
        atr1hPercent: 0.001,
        trendStrength: 0.8,
        chopScore: 0.25,
        shockFlag: false,
        timestampMs: Date.now(),
    };
    const candles: any[] = Array.from({ length: 70 }).map((_, i) => [i, 1.1000, 1.1010, 1.0990, 1.1001, 1000]);
    candles[candles.length - 3] = [67, 1.1009, 1.10145, 1.10095, 1.1014, 1000];
    candles[candles.length - 2] = [68, 1.10135, 1.10138, 1.10102, 1.10122, 1000];
    candles[candles.length - 1] = [69, 1.1012, 1.10155, 1.10108, 1.1014, 1000];
    const market: any = {
        pair: 'EURUSD',
        epic: 'EURUSD',
        nowMs: Date.now(),
        sessionTag: 'LONDON',
        price: 1.1,
        bid: 1.0999,
        offer: 1.1,
        spreadAbs: 0.0001,
        spreadPips: 1,
        atr5m: 0.0002,
        atr1h: 0.001,
        atr4h: 0.002,
        atr1hPercent: 0.001,
        spreadToAtr1h: 0.05,
        trendDirection1h: 'up',
        trendStrength1h: 0.8,
        chopScore1h: 0.25,
        shockFlag: false,
        nearestSupport: null,
        nearestResistance: null,
        distanceToSupportAtr1h: null,
        distanceToResistanceAtr1h: null,
        candles: { m5: candles, m15: candles, h1: candles, h4: candles, d1: candles },
    };

    const signal = evaluateBreakoutRetestModule({
        pair: 'EURUSD',
        packet,
        market,
        metrics,
    });
    assert.ok(signal);
    assert.equal(signal?.side, 'BUY');
});

test('reentry lock merge keeps longer lock when a shorter lock is applied later', () => {
    const baseTs = ts('2026-02-23T12:00:00.000Z');
    const reentry = {
        lockMinutes: 5,
        lockMinutesTimeStop: 5,
        lockMinutesRegimeFlip: 10,
        lockMinutesEventRisk: 20,
    };
    const eventLockMin = resolveReentryLockMinutes({
        reasonCodes: ['EVENT_HIGH_FORCE_CLOSE'],
        reentry,
        executeMinutes: 5,
    });
    const timeStopLockMin = resolveReentryLockMinutes({
        reasonCodes: ['CLOSE_TIME_STOP_MAX_HOLD'],
        reentry,
        executeMinutes: 5,
    });

    const eventLockUntil = baseTs + Number(eventLockMin) * 60_000;
    const timeStopLockUntil = baseTs + Number(timeStopLockMin) * 60_000;
    const merged = mergeReentryLockUntil(eventLockUntil, timeStopLockUntil);
    assert.equal(merged, eventLockUntil);
});

test('replay handles partial -> BE/trailing -> stop close sequencing under spread widening', () => {
    const cfg = withNoSlippage();
    cfg.management.partialAtR = 1;
    cfg.management.partialClosePct = 50;
    cfg.management.trailingDistanceR = 1.0;

    const quotes: ReplayQuote[] = [
        { ts: ts('2026-02-23T09:00:00.000Z'), bid: 1.1, ask: 1.1002 },
        { ts: ts('2026-02-23T09:05:00.000Z'), bid: 1.1014, ask: 1.1016 },
        { ts: ts('2026-02-23T09:06:00.000Z'), bid: 1.1004, ask: 1.1012, spreadMultiplier: 2.5 },
    ];
    const entries: ReplayEntrySignal[] = [
        { ts: quotes[0]!.ts, side: 'BUY', stopPrice: 1.0992, notionalUsd: 1000 },
    ];

    const result = runReplay({ quotes, entries, config: cfg });
    const partialIdx = result.ledger.findIndex((row) => row.kind === 'PARTIAL_EXIT');
    const exitIdx = result.ledger.findIndex((row) => row.kind === 'EXIT');
    assert.ok(partialIdx >= 0);
    assert.ok(exitIdx > partialIdx);
    assert.ok(result.ledger[exitIdx]?.reasonCodes.includes('STOP_INVALIDATED_LONG'));
});

test('replay applies rollover fee when holding across rollover boundary', () => {
    const cfg = withNoSlippage();
    cfg.rollover.dailyFeeBps = 1.2;
    cfg.rollover.entryBlockMinutes = 0;
    cfg.rollover.forceCloseMinutes = 0;
    cfg.spreadStress.transitionBufferMinutes = 0;

    const quotes: ReplayQuote[] = [
        { ts: ts('2026-02-23T23:10:00.000Z'), bid: 1.1, ask: 1.1002 },
        { ts: ts('2026-02-24T00:01:00.000Z'), bid: 1.1, ask: 1.1003, rollover: true },
        { ts: ts('2026-02-24T00:02:00.000Z'), bid: 1.1001, ask: 1.1003 },
    ];
    const entries: ReplayEntrySignal[] = [
        { ts: quotes[0]!.ts, side: 'BUY', stopPrice: 1.09, notionalUsd: 1000 },
    ];

    const result = runReplay({ quotes, entries, config: cfg });
    assert.ok(result.summary.rolloverFeesUsd > 0);
    assert.ok(result.ledger.some((row) => row.kind === 'ROLLOVER_FEE'));
});

test('replay force-closes positions before rollover when spread stress is elevated', () => {
    const cfg = withNoSlippage();
    cfg.rollover.entryBlockMinutes = 0;
    cfg.rollover.forceCloseMinutes = 20;
    cfg.rollover.forceCloseSpreadToAtr1hMin = 0.12;
    cfg.rollover.rolloverHourUtc = 0;

    const quotes: ReplayQuote[] = [
        { ts: ts('2026-02-23T22:50:00.000Z'), bid: 1.2, ask: 1.20008 },
        { ts: ts('2026-02-23T23:50:00.000Z'), bid: 1.2, ask: 1.2015 },
        { ts: ts('2026-02-24T00:01:00.000Z'), bid: 1.2, ask: 1.2004, rollover: true },
    ];
    const entries: ReplayEntrySignal[] = [
        { ts: quotes[0]!.ts, side: 'BUY', stopPrice: 1.19, notionalUsd: 1000 },
    ];

    const result = runReplay({ quotes, entries, config: cfg });
    const exit = result.ledger.find((row) => row.kind === 'EXIT');
    assert.ok(exit);
    assert.ok(exit?.reasonCodes.includes('ROLLOVER_PREEMPTIVE_FORCE_CLOSE'));
    assert.ok(!result.ledger.some((row) => row.kind === 'ROLLOVER_FEE'));
});

test('replay derisks pre-rollover winners with partial close instead of full close', () => {
    const cfg = withNoSlippage();
    cfg.management.partialAtR = 99;
    cfg.rollover.entryBlockMinutes = 0;
    cfg.rollover.forceCloseMinutes = 20;
    cfg.rollover.forceCloseSpreadToAtr1hMin = 0.12;
    cfg.rollover.rolloverHourUtc = 0;
    cfg.rollover.forceCloseMode = 'derisk';
    cfg.rollover.deriskWinnerMfeRMin = 0.8;
    cfg.rollover.deriskLoserCloseRMax = 0.2;
    cfg.rollover.deriskPartialClosePct = 50;

    const quotes: ReplayQuote[] = [
        { ts: ts('2026-02-23T22:50:00.000Z'), bid: 1.2, ask: 1.20008 },
        { ts: ts('2026-02-23T23:20:00.000Z'), bid: 1.209, ask: 1.20908 },
        { ts: ts('2026-02-23T23:50:00.000Z'), bid: 1.2065, ask: 1.2077 },
        { ts: ts('2026-02-23T23:59:00.000Z'), bid: 1.206, ask: 1.2064 },
    ];
    const entries: ReplayEntrySignal[] = [
        { ts: quotes[0]!.ts, side: 'BUY', stopPrice: 1.19, notionalUsd: 1000 },
    ];

    const result = runReplay({ quotes, entries, config: cfg });
    const partial = result.ledger.find((row) => row.kind === 'PARTIAL_EXIT');
    assert.ok(partial);
    assert.ok(partial?.reasonCodes.includes('ROLLOVER_PREEMPTIVE_DERISK_PARTIAL'));
    assert.ok(
        !result.ledger.some(
            (row) => row.kind === 'EXIT' && row.reasonCodes.includes('ROLLOVER_PREEMPTIVE_FORCE_CLOSE'),
        ),
    );
});

test('replay derisk mode still force-closes weak pre-rollover positions', () => {
    const cfg = withNoSlippage();
    cfg.rollover.entryBlockMinutes = 0;
    cfg.rollover.forceCloseMinutes = 20;
    cfg.rollover.forceCloseSpreadToAtr1hMin = 0.12;
    cfg.rollover.rolloverHourUtc = 0;
    cfg.rollover.forceCloseMode = 'derisk';
    cfg.rollover.deriskWinnerMfeRMin = 0.8;
    cfg.rollover.deriskLoserCloseRMax = 0.2;
    cfg.rollover.deriskPartialClosePct = 50;

    const quotes: ReplayQuote[] = [
        { ts: ts('2026-02-23T22:50:00.000Z'), bid: 1.2, ask: 1.20008 },
        { ts: ts('2026-02-23T23:50:00.000Z'), bid: 1.1998, ask: 1.2013 },
        { ts: ts('2026-02-24T00:01:00.000Z'), bid: 1.2, ask: 1.2004, rollover: true },
    ];
    const entries: ReplayEntrySignal[] = [
        { ts: quotes[0]!.ts, side: 'BUY', stopPrice: 1.19, notionalUsd: 1000 },
    ];

    const result = runReplay({ quotes, entries, config: cfg });
    const exit = result.ledger.find((row) => row.kind === 'EXIT');
    assert.ok(exit);
    assert.ok(exit?.reasonCodes.includes('ROLLOVER_PREEMPTIVE_FORCE_CLOSE'));
    assert.ok(exit?.reasonCodes.includes('ROLLOVER_PREEMPTIVE_DERISK_CLOSE'));
    assert.ok(!result.ledger.some((row) => row.kind === 'ROLLOVER_FEE'));
});
