import assert from 'node:assert/strict';
import test from 'node:test';

import { buildScalpDealReference, matchesScalpDeploymentDealReference, reconcileScalpBrokerPosition } from '../execution';
import { createInitialScalpSessionState } from '../stateMachine';
import type { ScalpCandle, ScalpMarketSnapshot } from '../types';

function candle(ts: number, open: number, high: number, low: number, close: number): ScalpCandle {
    return [ts, open, high, low, close, 100];
}

function marketSnapshot(params: { nowMs: number; epic: string; price: number }): ScalpMarketSnapshot {
    const confirmCandles = [
        candle(params.nowMs - 6 * 60_000, params.price, params.price + 0.0005, params.price - 0.0005, params.price),
        candle(params.nowMs - 3 * 60_000, params.price, params.price + 0.0006, params.price - 0.0004, params.price),
    ];
    return {
        symbol: 'EURUSD',
        epic: params.epic,
        nowMs: params.nowMs,
        quote: {
            price: params.price,
            bid: params.price - 0.00005,
            offer: params.price + 0.00005,
            spreadAbs: 0.0001,
            spreadPips: 1,
            tsMs: params.nowMs,
        },
        baseTf: 'M15',
        confirmTf: 'M3',
        baseCandles: confirmCandles,
        confirmCandles,
    };
}

test('deployment-owned deal references stay distinct across strategies on the same symbol', () => {
    const setupId = 'scalp:setup-1';
    const dayKey = '2026-01-05';
    const deploymentA = 'EURUSD~regime_pullback_m15_m3~default';
    const deploymentB = 'EURUSD~pdh_pdl_reclaim_m15_m3~default';

    const refA = buildScalpDealReference({ deploymentId: deploymentA, setupId, dayKey });
    const refB = buildScalpDealReference({ deploymentId: deploymentB, setupId, dayKey });

    assert.notEqual(refA, refB);
    assert.ok(matchesScalpDeploymentDealReference(refA, deploymentA));
    assert.equal(matchesScalpDeploymentDealReference(refA, deploymentB), false);
});

test('reconcileScalpBrokerPosition recovers only the owned deployment position when multiple positions share an epic', async () => {
    const nowMs = Date.UTC(2026, 0, 5, 10, 0, 0, 0);
    const epic = 'CS.D.EURUSD.CFD.IP';
    const ownedDeploymentId = 'EURUSD~regime_pullback_m15_m3~default';
    const foreignDeploymentId = 'EURUSD~pdh_pdl_reclaim_m15_m3~default';
    const state = createInitialScalpSessionState({
        symbol: 'EURUSD',
        strategyId: 'regime_pullback_m15_m3',
        tuneId: 'default',
        deploymentId: ownedDeploymentId,
        dayKey: '2026-01-05',
        nowMs,
        killSwitchActive: false,
    });

    const reconciled = await reconcileScalpBrokerPosition({
        state,
        market: marketSnapshot({ nowMs, epic, price: 1.1045 }),
        dryRun: false,
        maxOpenPositionsPerSymbol: 1,
        snapshots: [
            {
                epic,
                dealId: 'deal-foreign',
                dealReference: buildScalpDealReference({
                    deploymentId: foreignDeploymentId,
                    setupId: 'scalp:foreign',
                    dayKey: '2026-01-05',
                }),
                side: 'long',
                entryPrice: 1.103,
                leverage: 10,
                size: 1,
                pnlPct: 0.5,
                bid: 1.1044,
                offer: 1.1046,
                createdAtMs: nowMs - 120_000,
                updatedAtMs: nowMs,
            },
            {
                epic,
                dealId: 'deal-owned',
                dealReference: buildScalpDealReference({
                    deploymentId: ownedDeploymentId,
                    setupId: 'scalp:owned',
                    dayKey: '2026-01-05',
                }),
                side: 'short',
                entryPrice: 1.105,
                leverage: 10,
                size: 1,
                pnlPct: 0.2,
                bid: 1.1044,
                offer: 1.1046,
                createdAtMs: nowMs - 60_000,
                updatedAtMs: nowMs,
            },
        ],
    });

    assert.equal(reconciled.state.state, 'IN_TRADE');
    assert.ok(reconciled.state.trade);
    assert.equal(reconciled.state.trade!.brokerPositionId, 'deal-owned');
    assert.equal(reconciled.state.trade!.dealReference.includes('sclp-'), true);
    assert.equal(reconciled.state.trade!.side, 'SELL');
    assert.ok(reconciled.reasonCodes.includes('BROKER_POSITION_RECOVERED'));
});

test('reconcileScalpBrokerPosition confirms an existing trade by exact owned deal reference even with foreign same-symbol positions', async () => {
    const nowMs = Date.UTC(2026, 0, 5, 11, 0, 0, 0);
    const epic = 'CS.D.EURUSD.CFD.IP';
    const ownedDeploymentId = 'EURUSD~regime_pullback_m15_m3~default';
    const ownedDealReference = buildScalpDealReference({
        deploymentId: ownedDeploymentId,
        setupId: 'scalp:confirm',
        dayKey: '2026-01-05',
    });
    const state = createInitialScalpSessionState({
        symbol: 'EURUSD',
        strategyId: 'regime_pullback_m15_m3',
        tuneId: 'default',
        deploymentId: ownedDeploymentId,
        dayKey: '2026-01-05',
        nowMs,
        killSwitchActive: false,
    });
    state.state = 'IN_TRADE';
    state.trade = {
        setupId: 'setup-confirm',
        dealReference: ownedDealReference,
        side: 'BUY',
        entryPrice: 1.1,
        stopPrice: 1.09,
        takeProfitPrice: 1.12,
        riskR: 1,
        riskAbs: 0.01,
        initialStopPrice: 1.09,
        remainingSizePct: 1,
        realizedR: 0,
        tp1Done: false,
        tp1Price: null,
        trailActive: false,
        trailStopPrice: null,
        favorableExtremePrice: 1.1,
        barsHeld: 0,
        openedAtMs: nowMs - 180_000,
        brokerOrderId: null,
        brokerPositionId: null,
        dryRun: false,
    };

    const reconciled = await reconcileScalpBrokerPosition({
        state,
        market: marketSnapshot({ nowMs, epic, price: 1.101 }),
        dryRun: false,
        maxOpenPositionsPerSymbol: 1,
        snapshots: [
            {
                epic,
                dealId: 'deal-foreign',
                dealReference: buildScalpDealReference({
                    deploymentId: 'EURUSD~trend_day_reacceleration_m15_m3~default',
                    setupId: 'scalp:foreign',
                    dayKey: '2026-01-05',
                }),
                side: 'short',
                entryPrice: 1.102,
                leverage: 10,
                size: 1,
                pnlPct: 0.1,
                bid: 1.1009,
                offer: 1.1011,
                createdAtMs: nowMs - 240_000,
                updatedAtMs: nowMs,
            },
            {
                epic,
                dealId: 'deal-owned',
                dealReference: ownedDealReference,
                side: 'long',
                entryPrice: 1.1,
                leverage: 10,
                size: 1,
                pnlPct: 0.3,
                bid: 1.1009,
                offer: 1.1011,
                createdAtMs: nowMs - 180_000,
                updatedAtMs: nowMs,
            },
        ],
    });

    assert.equal(reconciled.state.state, 'IN_TRADE');
    assert.ok(reconciled.state.trade);
    assert.equal(reconciled.state.trade!.brokerPositionId, 'deal-owned');
    assert.ok(reconciled.reasonCodes.includes('BROKER_POSITION_CONFIRMED_BY_DEALREFERENCE'));
});
