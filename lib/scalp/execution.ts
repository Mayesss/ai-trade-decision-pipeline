import crypto from 'crypto';

import { executeCapitalScalpEntry, fetchCapitalOpenPositionSnapshots } from '../capital';
import type { ScalpEntryPlan, ScalpMarketSnapshot, ScalpSessionState, ScalpStrategyConfig } from './types';
import { pipSizeForScalpSymbol } from './marketData';

function toFinite(value: unknown, fallback = NaN): number {
    const n = Number(value);
    return Number.isFinite(n) ? n : fallback;
}

function clamp(value: number, min: number, max: number): number {
    return Math.max(min, Math.min(max, value));
}

function hashForSetup(raw: string): string {
    return crypto.createHash('sha1').update(raw).digest('hex').slice(0, 20);
}

function sideFromDirection(direction: 'BULLISH' | 'BEARISH'): 'BUY' | 'SELL' {
    return direction === 'BULLISH' ? 'BUY' : 'SELL';
}

function entryPriceForLimit(params: {
    side: 'BUY' | 'SELL';
    ifvgLow: number;
    ifvgHigh: number;
    entryMode: ScalpStrategyConfig['ifvg']['entryMode'];
}): number {
    const mid = (params.ifvgLow + params.ifvgHigh) / 2;
    if (params.side === 'BUY') {
        if (params.entryMode === 'first_touch') return params.ifvgHigh;
        if (params.entryMode === 'full_fill') return params.ifvgLow;
        return mid;
    }
    if (params.entryMode === 'first_touch') return params.ifvgLow;
    if (params.entryMode === 'full_fill') return params.ifvgHigh;
    return mid;
}

function buildSetupId(state: ScalpSessionState): string | null {
    if (!state.sweep || !state.ifvg) return null;
    const seed = [
        state.symbol,
        state.dayKey,
        String(state.sweep.side),
        String(state.sweep.sweepTsMs),
        String(state.ifvg.direction),
        String(state.ifvg.createdTsMs),
    ].join(':');
    return `scalp:${hashForSetup(seed)}`;
}

export function buildScalpEntryPlan(params: {
    state: ScalpSessionState;
    market: ScalpMarketSnapshot;
    cfg: ScalpStrategyConfig;
}): { plan: ScalpEntryPlan | null; reasonCodes: string[] } {
    const state = params.state;
    const ifvg = state.ifvg;
    const sweep = state.sweep;
    if (!ifvg || !sweep) return { plan: null, reasonCodes: ['ENTRY_PLAN_MISSING_SETUP'] };
    if (!ifvg.touched) return { plan: null, reasonCodes: ['ENTRY_PLAN_IFVG_NOT_TOUCHED'] };

    const setupId = buildSetupId(state);
    if (!setupId) return { plan: null, reasonCodes: ['ENTRY_PLAN_SETUP_ID_FAILED'] };
    const dealReference = `scalp-${hashForSetup(`${setupId}:${state.dayKey}`)}`;
    const side = sideFromDirection(ifvg.direction);
    const pipSize = pipSizeForScalpSymbol(state.symbol);
    const price = toFinite(params.market.quote.price);

    const orderType = params.cfg.execution.entryOrderType;
    const limitLevel =
        orderType === 'LIMIT'
            ? entryPriceForLimit({
                  side,
                  ifvgLow: ifvg.low,
                  ifvgHigh: ifvg.high,
                  entryMode: ifvg.entryMode,
              })
            : null;
    const entryReferencePrice = orderType === 'LIMIT' ? toFinite(limitLevel) : price;
    if (!(Number.isFinite(entryReferencePrice) && entryReferencePrice > 0)) {
        return { plan: null, reasonCodes: ['ENTRY_PLAN_INVALID_ENTRY_PRICE'] };
    }

    const stopBufferAbs = Math.max(
        params.cfg.risk.stopBufferPips * pipSize,
        params.cfg.risk.stopBufferSpreadMult * Math.max(0, params.market.quote.spreadAbs),
    );
    const stopPrice = side === 'BUY' ? sweep.sweepPrice - stopBufferAbs : sweep.sweepPrice + stopBufferAbs;
    if (!(Number.isFinite(stopPrice) && stopPrice > 0)) {
        return { plan: null, reasonCodes: ['ENTRY_PLAN_INVALID_STOP'] };
    }

    const riskAbs = Math.abs(entryReferencePrice - stopPrice);
    const minStopDistanceAbs = params.cfg.risk.minStopDistancePips * pipSize;
    if (!(Number.isFinite(riskAbs) && riskAbs >= minStopDistanceAbs)) {
        return { plan: null, reasonCodes: ['ENTRY_PLAN_STOP_DISTANCE_TOO_TIGHT'] };
    }

    const riskUsd = params.cfg.risk.referenceEquityUsd * (params.cfg.risk.riskPerTradePct / 100);
    const rawNotionalUsd = (riskUsd * entryReferencePrice) / riskAbs;
    const notionalUsd = clamp(rawNotionalUsd, params.cfg.risk.minNotionalUsd, params.cfg.risk.maxNotionalUsd);
    if (!(Number.isFinite(notionalUsd) && notionalUsd > 0)) {
        return { plan: null, reasonCodes: ['ENTRY_PLAN_INVALID_NOTIONAL'] };
    }

    const takeProfitPrice =
        side === 'BUY'
            ? entryReferencePrice + riskAbs * params.cfg.risk.takeProfitR
            : entryReferencePrice - riskAbs * params.cfg.risk.takeProfitR;

    if (!(Number.isFinite(takeProfitPrice) && takeProfitPrice > 0)) {
        return { plan: null, reasonCodes: ['ENTRY_PLAN_INVALID_TP'] };
    }

    return {
        plan: {
            setupId,
            dealReference,
            side,
            orderType,
            limitLevel: orderType === 'LIMIT' ? limitLevel : null,
            entryReferencePrice,
            stopPrice,
            takeProfitPrice,
            riskAbs,
            riskUsd,
            notionalUsd,
            leverage: params.cfg.execution.defaultLeverage,
        },
        reasonCodes: ['ENTRY_PLAN_READY'],
    };
}

export async function reconcileScalpBrokerPosition(params: {
    state: ScalpSessionState;
    market: ScalpMarketSnapshot;
    dryRun: boolean;
    maxOpenPositionsPerSymbol: number;
}): Promise<{ state: ScalpSessionState; reasonCodes: string[] }> {
    if (params.dryRun) return { state: params.state, reasonCodes: ['BROKER_RECONCILE_SKIPPED_DRY_RUN'] };

    let snapshots: Awaited<ReturnType<typeof fetchCapitalOpenPositionSnapshots>> = [];
    try {
        snapshots = await fetchCapitalOpenPositionSnapshots();
    } catch {
        return { state: params.state, reasonCodes: ['BROKER_RECONCILE_UNAVAILABLE'] };
    }

    const matching = snapshots.filter((row) => String(row.epic || '').trim() === params.market.epic);
    const byEpic = matching[0] || null;
    const next: ScalpSessionState = { ...params.state, trade: params.state.trade ? { ...params.state.trade } : null };

    if (matching.length > Math.max(1, params.maxOpenPositionsPerSymbol)) {
        next.state = 'DONE';
        return { state: next, reasonCodes: ['BROKER_OPEN_POSITION_LIMIT_EXCEEDED'] };
    }

    if (!byEpic) {
        if (next.state === 'IN_TRADE' && next.trade && !next.trade.dryRun) {
            next.state = 'DONE';
            return { state: next, reasonCodes: ['BROKER_POSITION_NOT_FOUND_MARK_DONE'] };
        }
        return { state: next, reasonCodes: ['BROKER_POSITION_NONE'] };
    }

    const side = byEpic.side === 'long' ? 'BUY' : byEpic.side === 'short' ? 'SELL' : null;
    const entryPrice = toFinite(byEpic.entryPrice);
    if (!side || !(Number.isFinite(entryPrice) && entryPrice > 0)) {
        return { state: next, reasonCodes: ['BROKER_POSITION_INVALID_PAYLOAD'] };
    }

    if (next.trade && !next.trade.dryRun) {
        next.state = 'IN_TRADE';
        return { state: next, reasonCodes: ['BROKER_POSITION_CONFIRMED'] };
    }

    next.trade = {
        setupId: `recovered:${params.market.epic}`,
        dealReference: String(byEpic.dealId || `recovered-${params.market.epic}`),
        side,
        entryPrice,
        stopPrice: next.trade?.stopPrice ?? entryPrice,
        takeProfitPrice: next.trade?.takeProfitPrice ?? null,
        riskR: 1,
        openedAtMs: params.market.nowMs,
        brokerOrderId: byEpic.dealId || null,
        dryRun: false,
    };
    next.state = 'IN_TRADE';
    return { state: next, reasonCodes: ['BROKER_POSITION_RECOVERED'] };
}

export async function executeScalpEntryPlan(params: {
    state: ScalpSessionState;
    plan: ScalpEntryPlan;
    cfg: ScalpStrategyConfig;
    dryRun: boolean;
    nowMs: number;
}): Promise<{ state: ScalpSessionState; reasonCodes: string[] }> {
    const next: ScalpSessionState = {
        ...params.state,
        trade: params.state.trade ? { ...params.state.trade } : null,
        stats: { ...params.state.stats },
    };

    if (next.stats.tradesPlaced >= params.cfg.risk.maxTradesPerSymbolPerDay) {
        next.state = 'DONE';
        return { state: next, reasonCodes: ['TRADE_LIMIT_REACHED'] };
    }

    if (!params.dryRun && !params.cfg.execution.liveEnabled) {
        next.state = 'DONE';
        return { state: next, reasonCodes: ['LIVE_EXECUTION_DISABLED'] };
    }

    const exec = await executeCapitalScalpEntry({
        symbol: next.symbol,
        direction: params.plan.side,
        notionalUsd: params.plan.notionalUsd,
        leverage: params.plan.leverage,
        dryRun: params.dryRun,
        clientOid: params.plan.dealReference,
        orderType: params.plan.orderType,
        limitLevel: params.plan.limitLevel,
        stopLevel: params.plan.stopPrice,
        profitLevel: params.plan.takeProfitPrice,
    });

    if (!exec.placed && !params.dryRun) {
        return { state: next, reasonCodes: ['ENTRY_NOT_PLACED'] };
    }

    next.trade = {
        setupId: params.plan.setupId,
        dealReference: params.plan.dealReference,
        side: params.plan.side,
        entryPrice: params.plan.entryReferencePrice,
        stopPrice: params.plan.stopPrice,
        takeProfitPrice: params.plan.takeProfitPrice,
        riskR: 1,
        openedAtMs: params.nowMs,
        brokerOrderId: exec.orderId || null,
        dryRun: params.dryRun,
    };
    next.stats.tradesPlaced += 1;
    next.stats.lastTradeAtMs = params.nowMs;

    if (params.dryRun) {
        next.state = 'DONE';
        return { state: next, reasonCodes: ['ENTRY_DRYRUN_SIMULATED'] };
    }
    next.state = 'IN_TRADE';
    return { state: next, reasonCodes: ['ENTRY_PLACED'] };
}
