import assert from 'node:assert/strict';
import test from 'node:test';

import {
    computeHybridRiskSize,
    computeOpenRiskUsage,
    evaluateForexRiskCheck,
    evaluateRiskCapBudget,
} from './risk';
import type { ForexPositionContext, ForexRegimePacket } from './types';

const packet: ForexRegimePacket = {
    pair: 'EURUSD',
    generatedAtMs: 1,
    regime: 'trend_up',
    permission: 'long_only',
    allowed_modules: ['pullback'],
    risk_state: 'normal',
    confidence: 0.8,
    htf_context: {
        nearest_support: null,
        nearest_resistance: null,
        distance_to_support_atr1h: null,
        distance_to_resistance_atr1h: null,
    },
    notes_codes: [],
};

const context: ForexPositionContext = {
    pair: 'EURUSD',
    side: 'BUY',
    entryModule: 'pullback',
    entryPrice: 1.1,
    initialStopPrice: 1.095,
    currentStopPrice: 1.097,
    initialRiskPrice: 0.005,
    partialTakenPct: 50,
    trailingActive: true,
    trailingMode: 'structure',
    tp1Price: 1.105,
    tp2Price: null,
    openedAtMs: 1,
    lastManagedAtMs: 1,
    lastCloseAtMs: null,
    packet,
};

test('computeHybridRiskSize sizes by stop distance when inputs are valid', () => {
    const out = computeHybridRiskSize({
        entryPrice: 1.1,
        stopPrice: 1.095,
        confidence: 0.85,
        fallbackNotionalUsd: 100,
        maxLeverage: 3,
        riskPerTradePct: 0.5,
        referenceEquityUsd: 10_000,
    });

    assert.equal(out.usedFallback, false);
    assert.ok((out.riskUsd ?? 0) > 0);
    assert.ok(out.sideSizeUsd > 0);
});

test('computeHybridRiskSize falls back explicitly when equity input missing', () => {
    const out = computeHybridRiskSize({
        entryPrice: 1.1,
        stopPrice: 1.095,
        confidence: 0.85,
        fallbackNotionalUsd: 100,
        maxLeverage: 3,
        riskPerTradePct: 0.5,
        referenceEquityUsd: NaN,
    });

    assert.equal(out.usedFallback, true);
    assert.ok(out.reasonCodes.includes('SIZE_FALLBACK_NOTIONAL'));
});

test('evaluateRiskCapBudget blocks when portfolio risk would breach cap', () => {
    const usage = computeOpenRiskUsage({
        openByPair: new Map([
            [
                'EURUSD',
                {
                    epic: 'CS.D.EURUSD.MINI.IP',
                    dealId: '1',
                    side: 'long',
                    entryPrice: 1.1,
                    leverage: 2,
                    size: 10_000,
                    pnlPct: 0,
                    bid: 1.101,
                    offer: 1.102,
                    updatedAtMs: Date.now(),
                },
            ],
        ]),
        contextsByPair: new Map([['EURUSD', context]]),
        equityUsd: 10_000,
        fallbackRiskPctForUnknown: 0.5,
    });

    const cap = evaluateRiskCapBudget({
        pair: 'GBPUSD',
        candidateRiskPct: 0.5,
        usage,
        maxPortfolioOpenPct: 0.6,
        maxCurrencyOpenPct: 1.0,
    });

    assert.equal(cap.allow, false);
    assert.ok(cap.reasonCodes.includes('NO_TRADE_RISK_CAP_PORTFOLIO'));
});

test('evaluateForexRiskCheck blocks entries on session-transition spread stress', async () => {
    const out = await evaluateForexRiskCheck({
        pair: 'EURUSD',
        nowMs: Date.UTC(2026, 1, 23, 7, 0, 0),
        metrics: {
            pair: 'EURUSD',
            epic: 'EURUSD',
            sessionTag: 'LONDON',
            price: 1.09,
            spreadAbs: 0.0001,
            spreadPips: 1,
            spreadToAtr1h: 0.13,
            atr1h: 0.002,
            atr4h: 0.004,
            atr1hPercent: 0.002,
            trendStrength: 0.8,
            chopScore: 0.3,
            shockFlag: false,
            timestampMs: Date.now(),
        },
        eventGate: {
            pair: 'EURUSD',
            blockNewEntries: false,
            allowNewEntries: true,
            allowRiskReduction: true,
            staleData: false,
            reasonCodes: ['EVENT_WINDOW_CLEAR'],
            matchedEvents: [],
            riskStateApplied: 'normal',
            activeImpactLevels: [],
        },
        exposure: {},
    });

    assert.equal(out.allowEntry, false);
    assert.ok(out.reasonCodes.includes('SESSION_TRANSITION_SPREAD_STRESS'));
    assert.ok(out.reasonCodes.includes('SPREAD_TO_ATR_TRANSITION_RISK_CAP_EXCEEDED'));
});

test('evaluateForexRiskCheck blocks entries in pre-rollover window', async () => {
    const out = await evaluateForexRiskCheck({
        pair: 'EURUSD',
        nowMs: Date.UTC(2026, 1, 23, 23, 30, 0),
        metrics: {
            pair: 'EURUSD',
            epic: 'EURUSD',
            sessionTag: 'LONDON',
            price: 1.09,
            spreadAbs: 0.00008,
            spreadPips: 0.8,
            spreadToAtr1h: 0.04,
            atr1h: 0.002,
            atr4h: 0.004,
            atr1hPercent: 0.002,
            trendStrength: 0.8,
            chopScore: 0.3,
            shockFlag: false,
            timestampMs: Date.now(),
        },
        eventGate: {
            pair: 'EURUSD',
            blockNewEntries: false,
            allowNewEntries: true,
            allowRiskReduction: true,
            staleData: false,
            reasonCodes: ['EVENT_WINDOW_CLEAR'],
            matchedEvents: [],
            riskStateApplied: 'normal',
            activeImpactLevels: [],
        },
        exposure: {},
    });

    assert.equal(out.allowEntry, false);
    assert.ok(out.reasonCodes.includes('ROLLOVER_ENTRY_BLOCK_WINDOW'));
    assert.ok(out.reasonCodes.includes('NO_TRADE_ROLLOVER_WINDOW'));
});
