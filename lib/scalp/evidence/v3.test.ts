import assert from 'node:assert/strict';
import test from 'node:test';

import type { ScalpReplayTrade } from '../replay/types';
import {
    computeScalpComposerV3EdgeScore,
    evaluateScalpComposerV3TemporalFilter,
    resolveScalpComposerV3StaleNewsBlackout,
    scalpComposerV3EntryWindowsOverlap,
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
    const before = evaluateScalpComposerV3TemporalFilter({
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
    const open = evaluateScalpComposerV3TemporalFilter({
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
    const result = evaluateScalpComposerV3TemporalFilter({
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
    const lowN = computeScalpComposerV3EdgeScore({
        trades: [0.7, 0.4, -0.2].map((r, i) => trade(r, i)),
        weeklyNetR: { '1': 0.9 },
        minVariantTrades: 8,
        isTemporalVariant: true,
    });
    assert.equal(lowN.variantTradeFloorPassed, false);
    assert.equal(lowN.edgeScore, Number.NEGATIVE_INFINITY);

    const scored = computeScalpComposerV3EdgeScore({
        trades: [0.5, 0.5, 0.5, -0.5].map((r, i) => trade(r, i)),
        weeklyNetR: { '1': 1.5, '2': -10 },
        minVariantTrades: 1,
        isTemporalVariant: false,
    });
    assert.equal(scored.stats?.worstWeekPenalty, 1);
});

test('V3 edge score prefers stable lower-bound quality over lucky high mean', () => {
    const weeklyNetR = { '1': 1, '2': 1, '3': 1, '4': 1 };
    const stable = computeScalpComposerV3EdgeScore({
        trades: Array.from({ length: 24 }, (_, i) => trade(0.2, i)),
        weeklyNetR,
    });
    const lucky = computeScalpComposerV3EdgeScore({
        trades: [2, 2, 2, ...Array.from({ length: 21 }, () => -0.05)].map((r, i) => trade(r, i)),
        weeklyNetR,
    });
    assert.ok(Number(stable.edgeScore) > Number(lucky.edgeScore));
});

test('V3 stale news blackout fail-closes recurring tier-1 window and otherwise fails open', () => {
    const firstFridayNfp = Date.UTC(2026, 4, 1, 13, 30);
    const tier1 = resolveScalpComposerV3StaleNewsBlackout(firstFridayNfp);
    assert.equal(tier1.blocked, true);
    assert.equal(tier1.tier, 'tier1');
    assert.ok(tier1.reasonCodes.includes('V3_NEWS_BLACKOUT_TIER1_STALE_FAIL_CLOSED'));

    const quietTime = Date.UTC(2026, 4, 6, 10, 0);
    const tier2Fallback = resolveScalpComposerV3StaleNewsBlackout(quietTime);
    assert.equal(tier2Fallback.blocked, false);
    assert.equal(tier2Fallback.staleData, true);
});

test('V3 broker entry-window overlap detects same-venue temporal conflicts', () => {
    const nowMs = Date.UTC(2026, 0, 5);
    assert.equal(
        scalpComposerV3EntryWindowsOverlap({
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
        scalpComposerV3EntryWindowsOverlap({
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
        scalpComposerV3EntryWindowsOverlap({
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
        scalpComposerV3EntryWindowsOverlap({
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
