import assert from 'node:assert/strict';
import test from 'node:test';

import type { ScalpReplayTrade } from '../scalp/replay/types';
import {
    computeScalpV2V3Drift,
    computeScalpV2V3EdgeScore,
    computeScalpV2V3Holdout,
    evaluateScalpV2V3TemporalFilter,
    resolveScalpV2V3StaleNewsBlackout,
    scalpV2V3EntryWindowsOverlap,
    synthesizeScalpV2V3HoldoutFromStages,
} from './index';

function trade(rMultiple: number, index: number, exitTs = Date.UTC(2026, 0, 5) + index * 60_000): ScalpReplayTrade {
    return {
        id: `t${index}`,
        dayKey: '2026-01-05',
        side: 'BUY',
        entryTs: exitTs - 60_000,
        exitTs,
        holdMinutes: 1,
        entryPrice: 1,
        stopPrice: 0.99,
        takeProfitPrice: 1.01,
        exitPrice: 1 + rMultiple * 0.01,
        exitReason: rMultiple >= 0 ? 'TP' : 'STOP',
        riskAbs: 0.01,
        riskUsd: 1,
        notionalUsd: 100,
        rMultiple,
        pnlUsd: rMultiple,
    };
}

test('V3 temporal filter blocks session gaps and allows configured 30m slot', () => {
    const mondayBeforeBerlin = Date.UTC(2026, 0, 5, 6, 30); // 07:30 Europe/Berlin.
    const before = evaluateScalpV2V3TemporalFilter({
        nowMs: mondayBeforeBerlin,
        session: 'berlin',
        filter: {
            sessionSlotMinutes: 30,
            allowedSessionWindowSlots: [0],
        },
    });
    assert.equal(before.allowed, false);
    assert.ok(before.reasonCodes.includes('V3_TEMPORAL_SLOT_BLOCKED'));

    const mondayBerlinOpen = Date.UTC(2026, 0, 5, 7, 0); // 08:00 Europe/Berlin.
    const open = evaluateScalpV2V3TemporalFilter({
        nowMs: mondayBerlinOpen,
        session: 'berlin',
        filter: {
            sessionSlotMinutes: 30,
            allowedSessionWindowSlots: [0],
            allowedWeekdaysLocal: [1],
            allowedUtcHours: [7],
        },
    });
    assert.equal(open.allowed, true);
    assert.equal(open.slotIndex, 0);
    assert.equal(open.weekdayLocal, 1);
    assert.equal(open.utcHour, 7);
});

test('V3 temporal filter blocks unmatched local weekday and UTC hour', () => {
    const mondayBerlinOpen = Date.UTC(2026, 0, 5, 7, 0);
    const result = evaluateScalpV2V3TemporalFilter({
        nowMs: mondayBerlinOpen,
        session: 'berlin',
        filter: {
            allowedWeekdaysLocal: [2],
            allowedUtcHours: [8],
        },
    });
    assert.equal(result.allowed, false);
    assert.ok(result.reasonCodes.includes('V3_TEMPORAL_WEEKDAY_BLOCKED'));
    assert.ok(result.reasonCodes.includes('V3_TEMPORAL_UTC_HOUR_BLOCKED'));
});

test('V3 edge score blocks low-N temporal variants and caps worst-week penalty', () => {
    const lowN = computeScalpV2V3EdgeScore({
        trades: [0.7, 0.4, -0.2].map((r, i) => trade(r, i)),
        weeklyNetR: { '1': 0.9 },
        minVariantTrades: 8,
        isTemporalVariant: true,
    });
    assert.equal(lowN.variantTradeFloorPassed, false);
    assert.equal(lowN.edgeScore, Number.NEGATIVE_INFINITY);

    const scored = computeScalpV2V3EdgeScore({
        trades: [0.5, 0.5, 0.5, -0.5].map((r, i) => trade(r, i)),
        weeklyNetR: { '1': 1.5, '2': -10 },
        minVariantTrades: 1,
        isTemporalVariant: false,
    });
    assert.equal(scored.stats?.worstWeekPenalty, 1);
});

test('V3 edge score prefers stable lower-bound quality over lucky high mean', () => {
    const weeklyNetR = { '1': 1, '2': 1, '3': 1, '4': 1 };
    const stable = computeScalpV2V3EdgeScore({
        trades: Array.from({ length: 24 }, (_, i) => trade(0.2, i)),
        weeklyNetR,
    });
    const lucky = computeScalpV2V3EdgeScore({
        trades: [2, 2, 2, ...Array.from({ length: 21 }, () => -0.05)].map((r, i) => trade(r, i)),
        weeklyNetR,
    });
    assert.ok(Number(stable.edgeScore) > Number(lucky.edgeScore));
});

test('V3 holdout catches train-only curve fit', () => {
    const windowToTs = Date.UTC(2026, 4, 4);
    const holdoutStart = windowToTs - 6 * 7 * 24 * 60 * 60 * 1000;
    const trades = [
        ...Array.from({ length: 16 }, (_, i) => trade(0.7, i, holdoutStart - (i + 1) * 24 * 60 * 60 * 1000)),
        ...Array.from({ length: 8 }, (_, i) => trade(-0.4, i + 100, holdoutStart + i * 24 * 60 * 60 * 1000)),
    ];
    const holdout = computeScalpV2V3Holdout({
        trades,
        windowToTs,
        holdoutWeeks: 6,
        trainingNetR: 11.2,
        trainingExpectancyR: 0.7,
        minTrades: 8,
    });
    assert.equal(holdout.passed, false);
    assert.equal(holdout.reason, 'holdout_expectancy_ratio_below_threshold');
});

test('V3 stale news blackout fail-closes recurring tier-1 window and otherwise fails open', () => {
    const firstFridayNfp = Date.UTC(2026, 4, 1, 13, 30);
    const tier1 = resolveScalpV2V3StaleNewsBlackout(firstFridayNfp);
    assert.equal(tier1.blocked, true);
    assert.equal(tier1.tier, 'tier1');
    assert.ok(tier1.reasonCodes.includes('V3_NEWS_BLACKOUT_TIER1_STALE_FAIL_CLOSED'));

    const quietTime = Date.UTC(2026, 4, 6, 10, 0);
    const tier2Fallback = resolveScalpV2V3StaleNewsBlackout(quietTime);
    assert.equal(tier2Fallback.blocked, false);
    assert.equal(tier2Fallback.staleData, true);
});

test('V3 drift monitor flags live expectancy below 50 percent after sample threshold', () => {
    const previous = {
        trades: process.env.SCALP_V2_V3_DRIFT_MIN_TRADES,
        weeks: process.env.SCALP_V2_V3_DRIFT_MIN_WEEKS,
    };
    process.env.SCALP_V2_V3_DRIFT_MIN_TRADES = '20';
    process.env.SCALP_V2_V3_DRIFT_MIN_WEEKS = '2';
    try {
        const nowMs = Date.UTC(2026, 4, 6);
        const drift = computeScalpV2V3Drift({
            deployment: {
                deploymentId: 'd1',
                candidateId: 1,
                venue: 'capital',
                symbol: 'EURUSD',
                strategyId: 's',
                tuneId: 't',
                entrySessionProfile: 'berlin',
                enabled: true,
                liveMode: 'live',
                promotionGate: {
                    worker: {
                        stageC: {
                            expectancyR: 0.3,
                            maxDrawdownR: 3,
                        },
                    },
                },
                riskProfile: {
                    riskPerTradePct: 1,
                    maxOpenPositionsPerSymbol: 1,
                    autoPauseDailyR: -3,
                    autoPause30dR: -6,
                },
                createdAtMs: nowMs,
                updatedAtMs: nowMs,
            },
            ledgerRows: Array.from({ length: 20 }, (_, i) => ({
                tsExitMs: nowMs - (i < 10 ? i : i + 7) * 24 * 60 * 60 * 1000,
                rMultiple: 0.1,
            })),
            nowMs,
        });
        assert.equal(drift.status, 'drifting');
        assert.equal(drift.reason, 'drift_expectancy_ratio_below_threshold');
        assert.ok(Number(drift.expectancyRatio) < 0.5);
    } finally {
        if (previous.trades === undefined) delete process.env.SCALP_V2_V3_DRIFT_MIN_TRADES;
        else process.env.SCALP_V2_V3_DRIFT_MIN_TRADES = previous.trades;
        if (previous.weeks === undefined) delete process.env.SCALP_V2_V3_DRIFT_MIN_WEEKS;
        else process.env.SCALP_V2_V3_DRIFT_MIN_WEEKS = previous.weeks;
    }
});

test('V3 broker entry-window overlap detects same-venue temporal conflicts', () => {
    const nowMs = Date.UTC(2026, 0, 5);
    assert.equal(
        scalpV2V3EntryWindowsOverlap({
            nowMs,
            a: {
                session: 'berlin',
                filter: {
                    sessionSlotMinutes: 30,
                    allowedSessionWindowSlots: [0],
                },
            },
            b: {
                session: 'berlin',
                filter: {
                    sessionSlotMinutes: 30,
                    allowedSessionWindowSlots: [0],
                },
            },
        }),
        true,
    );
    assert.equal(
        scalpV2V3EntryWindowsOverlap({
            nowMs,
            a: {
                session: 'berlin',
                filter: {
                    sessionSlotMinutes: 30,
                    allowedSessionWindowSlots: [0],
                },
            },
            b: {
                session: 'berlin',
                filter: {
                    sessionSlotMinutes: 30,
                    allowedSessionWindowSlots: [3],
                },
            },
        }),
        false,
    );
    assert.equal(
        scalpV2V3EntryWindowsOverlap({
            nowMs,
            a: {
                session: 'berlin',
                filter: {
                    allowedWeekdaysLocal: [1],
                },
            },
            b: {
                session: 'berlin',
                filter: {
                    allowedWeekdaysLocal: [2],
                },
            },
        }),
        false,
    );
});

test('V3 broker entry-window overlap treats full-session candidates as overlapping slots', () => {
    assert.equal(
        scalpV2V3EntryWindowsOverlap({
            nowMs: Date.UTC(2026, 0, 5),
            a: {
                session: 'berlin',
                filter: null,
            },
            b: {
                session: 'berlin',
                filter: {
                    sessionSlotMinutes: 30,
                    allowedSessionWindowSlots: [2],
                },
            },
        }),
        true,
    );
});

test('synthesizeScalpV2V3HoldoutFromStages splits stage-C minus stage-B and applies pass criteria', () => {
    const passing = synthesizeScalpV2V3HoldoutFromStages({
        stageB: { netR: 14.9, trades: 20, fromTs: 1, toTs: 2, weeks: 6, maxDrawdownR: 1, profitFactor: 8.4 },
        stageC: { netR: 23.0, trades: 30 },
    });
    assert.ok(passing);
    assert.equal(passing!.trades, 20);
    assert.equal(passing!.trainingTrades, 10);
    assert.ok(Math.abs(passing!.trainingNetR - 8.1) < 1e-6);
    assert.equal(passing!.passed, true);
    assert.equal(passing!.source, 'v2_backfill');

    const failing = synthesizeScalpV2V3HoldoutFromStages({
        stageB: { netR: 5.7, trades: 47, fromTs: 1, toTs: 2, weeks: 6, maxDrawdownR: 4.6, profitFactor: 1.3 },
        stageC: { netR: 20.5, trades: 100 },
    });
    assert.ok(failing);
    assert.equal(failing!.passed, false);
    assert.equal(failing!.reason, 'holdout_expectancy_ratio_below_threshold');

    assert.equal(
        synthesizeScalpV2V3HoldoutFromStages({
            stageB: { netR: 5, trades: 10 },
            stageC: { netR: 5, trades: 10 },
        }),
        null,
    );
});
