import assert from 'node:assert/strict';
import test from 'node:test';

import { resolveReentryLockMinutes } from '../engine';
import type { ForexSide } from '../types';
import { defaultReplayConfig, mergeReentryLockUntil, runReplay } from './harness';
import { createSeededRng } from './random';
import type { ReplayEntrySignal, ReplayQuote, ReplayRuntimeConfig } from './types';

const EPS = 1e-12;

function baseTs(iso = '2026-02-23T10:00:00.000Z'): number {
    return Date.parse(iso);
}

function minuteTs(startTs: number, step: number): number {
    return startTs + step * 60_000;
}

function noFrictionConfig(pair = 'EURUSD'): ReplayRuntimeConfig {
    const cfg = defaultReplayConfig(pair);
    cfg.atr1hAbs = 0.02;
    cfg.forceCloseOnHighEvent = true;

    cfg.spreadStress.transitionBufferMinutes = 0;
    cfg.spreadStress.transitionMultiplier = 1;
    cfg.spreadStress.rolloverMultiplier = 1;
    cfg.spreadStress.mediumEventMultiplier = 1;
    cfg.spreadStress.highEventMultiplier = 1;

    cfg.slippage.entryBaseBps = 0;
    cfg.slippage.exitBaseBps = 0;
    cfg.slippage.randomBps = 0;
    cfg.slippage.shockBps = 0;
    cfg.slippage.mediumEventBps = 0;
    cfg.slippage.highEventBps = 0;

    cfg.management.partialClosePct = 0;
    cfg.management.enableTrailing = false;
    return cfg;
}

function randomWalkQuotes(params: {
    seed: number;
    steps: number;
    startMid?: number;
    startSpread?: number;
    drift?: number;
    vol?: number;
}): ReplayQuote[] {
    const rng = createSeededRng(params.seed);
    const steps = Math.max(4, Math.floor(params.steps));
    const startMid = params.startMid ?? 1.1;
    const startSpread = params.startSpread ?? 0.00012;
    const drift = params.drift ?? 0;
    const vol = params.vol ?? 0.00016;
    const out: ReplayQuote[] = [];
    const startTs = baseTs();
    let mid = startMid;
    let spread = startSpread;

    for (let i = 0; i < steps; i += 1) {
        if (i > 0) {
            mid = Math.max(0.5, mid + drift + rng.nextSigned() * vol);
            spread = Math.max(0.00002, startSpread * (1 + rng.nextSigned() * 0.25));
        }
        out.push({
            ts: minuteTs(startTs, i),
            bid: mid - spread / 2,
            ask: mid + spread / 2,
        });
    }
    return out;
}

function expectedExitForPath(params: {
    side: ForexSide;
    quotes: ReplayQuote[];
    stopPrice: number;
    takeProfitPrice: number;
}): { reasonCode: string; ts: number } {
    const { side, quotes, stopPrice, takeProfitPrice } = params;
    let firstStopTs: number | null = null;
    let firstTpTs: number | null = null;

    for (const quote of quotes) {
        if (side === 'BUY') {
            if (firstTpTs === null && quote.bid >= takeProfitPrice - EPS) firstTpTs = quote.ts;
            if (firstStopTs === null && quote.bid <= stopPrice + EPS) firstStopTs = quote.ts;
        } else {
            if (firstTpTs === null && quote.ask <= takeProfitPrice + EPS) firstTpTs = quote.ts;
            if (firstStopTs === null && quote.ask >= stopPrice - EPS) firstStopTs = quote.ts;
        }
    }

    if (firstTpTs !== null && (firstStopTs === null || firstTpTs <= firstStopTs)) {
        return { reasonCode: 'TAKE_PROFIT_HIT', ts: firstTpTs };
    }
    if (firstStopTs !== null) {
        return { reasonCode: side === 'BUY' ? 'STOP_INVALIDATED_LONG' : 'STOP_INVALIDATED_SHORT', ts: firstStopTs };
    }
    return { reasonCode: 'END_OF_REPLAY_FLAT', ts: quotes[quotes.length - 1]!.ts };
}

test('property: stop/TP trigger side correctness and ordering under deterministic random paths', () => {
    const cfg = noFrictionConfig();
    cfg.forceCloseOnHighEvent = false;

    const sides: ForexSide[] = ['BUY', 'SELL'];
    let casesChecked = 0;
    for (const side of sides) {
        for (let trial = 0; trial < 80; trial += 1) {
            const seed = 12000 + trial + (side === 'BUY' ? 0 : 1000);
            const quotes = randomWalkQuotes({
                seed,
                steps: 35,
                startMid: 1.09 + trial * 0.00001,
                drift: side === 'BUY' ? 0.00001 : -0.00001,
                vol: 0.00018,
            });
            const first = quotes[0]!;
            const entryPrice = side === 'BUY' ? first.ask : first.bid;
            const stopDistance = 0.00035 + (trial % 7) * 0.00003;
            const tpDistance = 0.00045 + (trial % 5) * 0.00004;
            const stopPrice = side === 'BUY' ? entryPrice - stopDistance : entryPrice + stopDistance;
            const takeProfitPrice = side === 'BUY' ? entryPrice + tpDistance : entryPrice - tpDistance;

            const entries: ReplayEntrySignal[] = [
                {
                    ts: first.ts,
                    side,
                    stopPrice,
                    takeProfitPrice,
                    notionalUsd: 1000,
                },
            ];
            const result = runReplay({ quotes, entries, config: cfg });
            const entryRows = result.ledger.filter((row) => row.kind === 'ENTRY');
            const exits = result.ledger.filter((row) => row.kind === 'EXIT');
            assert.equal(entryRows.length, 1, `entry should open (side=${side}, trial=${trial})`);
            assert.equal(exits.length, 1, `single terminal exit expected (side=${side}, trial=${trial})`);

            const expected = expectedExitForPath({
                side,
                quotes,
                stopPrice,
                takeProfitPrice,
            });
            const actual = exits[0]!;
            const actualReason = String(actual.reasonCodes[0] || '');
            assert.equal(actualReason, expected.reasonCode, `reason mismatch (side=${side}, trial=${trial})`);
            assert.equal(actual.ts, expected.ts, `ts mismatch (side=${side}, trial=${trial})`);

            const quoteByTs = new Map(quotes.map((q) => [q.ts, q]));
            const exitQuote = quoteByTs.get(actual.ts);
            assert.ok(exitQuote, 'exit quote should exist');
            if (actualReason === 'STOP_INVALIDATED_LONG') {
                assert.ok((exitQuote?.bid || Number.POSITIVE_INFINITY) <= stopPrice + EPS, 'long stop must trigger on bid');
            } else if (actualReason === 'STOP_INVALIDATED_SHORT') {
                assert.ok((exitQuote?.ask || Number.NEGATIVE_INFINITY) >= stopPrice - EPS, 'short stop must trigger on ask');
            } else if (actualReason === 'TAKE_PROFIT_HIT') {
                if (side === 'BUY') {
                    assert.ok((exitQuote?.bid || Number.NEGATIVE_INFINITY) >= takeProfitPrice - EPS, 'long TP must trigger on bid');
                } else {
                    assert.ok((exitQuote?.ask || Number.POSITIVE_INFINITY) <= takeProfitPrice + EPS, 'short TP must trigger on ask');
                }
            }
            casesChecked += 1;
        }
    }
    assert.equal(casesChecked, 160);
});

test('property: trailing stop tightening is monotonic after activation', () => {
    const sides: ForexSide[] = ['BUY', 'SELL'];
    for (const side of sides) {
        let tightenEventsObserved = 0;
        for (let trial = 0; trial < 50; trial += 1) {
            const cfg = noFrictionConfig();
            cfg.forceCloseOnHighEvent = false;
            cfg.management.partialAtR = 0.2;
            cfg.management.partialClosePct = 40;
            cfg.management.trailingDistanceR = 0.9;
            cfg.management.enableTrailing = true;

            const quotes = randomWalkQuotes({
                seed: 51000 + trial + (side === 'BUY' ? 0 : 5000),
                steps: 70,
                startMid: 1.1,
                drift: side === 'BUY' ? 0.00012 : -0.00012,
                vol: 0.00005,
            });
            const first = quotes[0]!;
            const entryPrice = side === 'BUY' ? first.ask : first.bid;
            const stopPrice = side === 'BUY' ? entryPrice - 0.0006 : entryPrice + 0.0006;

            const result = runReplay({
                quotes,
                entries: [{ ts: first.ts, side, stopPrice, notionalUsd: 1000 }],
                config: cfg,
            });
            const tightenStops = result.timeline
                .filter((event) => event.type === 'STOP_TIGHTENED')
                .map((event) => Number(event.details?.nextStop))
                .filter((value) => Number.isFinite(value));

            if (!tightenStops.length) continue;
            tightenEventsObserved += tightenStops.length;
            for (let i = 1; i < tightenStops.length; i += 1) {
                const prev = tightenStops[i - 1]!;
                const next = tightenStops[i]!;
                if (side === 'BUY') {
                    assert.ok(next >= prev - EPS, `long trailing stop cannot loosen (trial=${trial})`);
                } else {
                    assert.ok(next <= prev + EPS, `short trailing stop cannot loosen (trial=${trial})`);
                }
            }
        }
        assert.ok(tightenEventsObserved > 0, `expected trailing tighten events for side ${side}`);
    }
});

test('property: re-entry lock merge is non-shrinking across mixed lock reasons', () => {
    const rng = createSeededRng(73123);
    const reentry = {
        lockMinutes: 5,
        lockMinutesTimeStop: 5,
        lockMinutesRegimeFlip: 10,
        lockMinutesEventRisk: 20,
    };
    const reasonPools = [
        ['EVENT_HIGH_FORCE_CLOSE'],
        ['REGIME_FLIP_CLOSE'],
        ['CLOSE_TIME_STOP_MAX_HOLD'],
        ['CLOSE_TIME_STOP_NO_PROGRESS'],
        ['SOME_OTHER_REASON'],
    ];

    for (let trial = 0; trial < 120; trial += 1) {
        const start = baseTs('2026-02-23T12:00:00.000Z') + trial * 10_000;
        let current: number | null = null;
        let expectedMax: number | null = null;
        for (let step = 0; step < 40; step += 1) {
            const ts = start + Math.floor(step * 2 + rng.next() * 2) * 60_000;
            const reasons = reasonPools[Math.floor(rng.next() * reasonPools.length)]!;
            const lockMinutes = resolveReentryLockMinutes({
                reasonCodes: reasons,
                reentry,
                executeMinutes: 5,
            });
            const candidate = Number.isFinite(lockMinutes as number) && Number(lockMinutes) > 0
                ? ts + Number(lockMinutes) * 60_000
                : null;
            const merged = mergeReentryLockUntil(current, candidate);

            if (expectedMax === null) {
                expectedMax = candidate;
            } else if (candidate !== null) {
                expectedMax = Math.max(expectedMax, candidate);
            }
            assert.equal(merged, expectedMax, `lock merge mismatch at trial=${trial}, step=${step}`);

            if (current !== null && merged !== null) {
                assert.ok(merged >= current, `lock regressed at trial=${trial}, step=${step}`);
            }
            current = merged;
        }
    }
});

test('integration: lock update timeline is monotonic across event/time-stop/regime closes', () => {
    const cfg = noFrictionConfig();
    const t0 = baseTs('2026-02-23T10:00:00.000Z');
    const quotes: ReplayQuote[] = [
        { ts: t0 - 40 * 60_000, bid: 1.1, ask: 1.1001 },
        { ts: t0 + 5 * 60_000, bid: 1.1, ask: 1.1001, eventRisk: 'high' },
        { ts: t0 + 25 * 60_000, bid: 1.1, ask: 1.1001 },
        { ts: t0 + 30 * 60_000, bid: 1.1, ask: 1.1001, forceCloseReasonCode: 'CLOSE_TIME_STOP_MAX_HOLD' },
        { ts: t0 + 36 * 60_000, bid: 1.1, ask: 1.1001 },
        { ts: t0 + 40 * 60_000, bid: 1.1, ask: 1.1001, forceCloseReasonCode: 'REGIME_FLIP_CLOSE' },
    ];
    const entries: ReplayEntrySignal[] = [
        { ts: t0 - 40 * 60_000, side: 'BUY', stopPrice: 1.095, notionalUsd: 1000 },
        { ts: t0 + 25 * 60_000, side: 'BUY', stopPrice: 1.095, notionalUsd: 1000 },
        { ts: t0 + 36 * 60_000, side: 'BUY', stopPrice: 1.095, notionalUsd: 1000 },
    ];
    const result = runReplay({ quotes, entries, config: cfg });

    const lockUpdates = result.timeline
        .filter((event) => event.type === 'REENTRY_LOCK_UPDATED')
        .map((event) => Number(event.details?.lockUntilMs))
        .filter((value) => Number.isFinite(value));

    assert.equal(lockUpdates.length, 3);
    for (let i = 1; i < lockUpdates.length; i += 1) {
        assert.ok(lockUpdates[i]! >= lockUpdates[i - 1]!, 'lockUntil timeline should be non-decreasing');
    }
});
