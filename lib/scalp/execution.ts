import crypto from 'crypto';

import { executeCapitalDecision, executeCapitalScalpEntry, fetchCapitalOpenPositionSnapshots } from '../capital';
import { computeAtr } from './detectors';
import type { ScalpStrategyEntryIntent } from './strategies/types';
import type { ScalpEntryPlan, ScalpMarketSnapshot, ScalpSessionState, ScalpStrategyConfig } from './types';
import { pipSizeForScalpSymbol, timeframeMinutes } from './marketData';

function toFinite(value: unknown, fallback = NaN): number {
    const n = Number(value);
    return Number.isFinite(n) ? n : fallback;
}

function clamp(value: number, min: number, max: number): number {
    return Math.max(min, Math.min(max, value));
}

function high(candle: [number, number, number, number, number, number]): number {
    return candle[2];
}

function low(candle: [number, number, number, number, number, number]): number {
    return candle[3];
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

export function resolveLegacyIfvgEntryIntent(state: ScalpSessionState): ScalpStrategyEntryIntent | null {
    if (state.trade) return null;
    if (state.state !== 'WAITING_RETRACE') return null;
    if (!state.ifvg?.touched) return null;
    return { model: 'ifvg_touch' };
}

function closeFractionFromPct(remainingSizePct: number, closePct: number): number {
    return clamp(remainingSizePct, 0, 1) * clamp(closePct, 0, 100) / 100;
}

function currentRForTrade(trade: NonNullable<ScalpSessionState['trade']>, price: number, riskAbs: number): number {
    if (!(Number.isFinite(price) && Number.isFinite(riskAbs) && riskAbs > 0)) return 0;
    const signedMove = trade.side === 'BUY' ? price - trade.entryPrice : trade.entryPrice - price;
    return signedMove / riskAbs;
}

async function closeScalpTradePortion(params: {
    symbol: string;
    closePct: number;
    dryRun: boolean;
    reason: string;
}): Promise<{ closed: boolean; reasonCodes: string[] }> {
    const closePct = clamp(params.closePct, 0, 100);
    if (!(closePct > 0)) {
        return { closed: false, reasonCodes: ['TRADE_CLOSE_SKIPPED_ZERO_PCT'] };
    }
    if (params.dryRun) {
        return { closed: true, reasonCodes: ['TRADE_CLOSE_SIMULATED_DRYRUN'] };
    }
    try {
        const closeRes = await executeCapitalDecision(
            params.symbol,
            0,
            {
                action: 'CLOSE',
                summary: `scalp-manage:${params.reason}`,
                reason: params.reason,
                close_size_pct: closePct,
            },
            false,
        );
        const closed = Boolean((closeRes as any)?.closed || (closeRes as any)?.placed);
        return {
            closed,
            reasonCodes: [closed ? 'TRADE_CLOSE_CONFIRMED' : 'TRADE_CLOSE_NOT_CONFIRMED'],
        };
    } catch (err) {
        return {
            closed: false,
            reasonCodes: ['TRADE_CLOSE_ERROR', err instanceof Error ? err.message.toUpperCase().slice(0, 80) : 'TRADE_CLOSE_UNKNOWN_ERROR'],
        };
    }
}

export function buildScalpEntryPlan(params: {
    state: ScalpSessionState;
    market: ScalpMarketSnapshot;
    cfg: ScalpStrategyConfig;
    entryIntent?: ScalpStrategyEntryIntent | null;
}): { plan: ScalpEntryPlan | null; reasonCodes: string[] } {
    const state = params.state;
    const entryIntent = params.entryIntent ?? resolveLegacyIfvgEntryIntent(state);
    if (!entryIntent) return { plan: null, reasonCodes: ['ENTRY_PLAN_NOT_READY'] };
    if (entryIntent.model !== 'ifvg_touch') {
        return { plan: null, reasonCodes: ['ENTRY_PLAN_UNSUPPORTED_INTENT'] };
    }
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
        reasonCodes: ['ENTRY_INTENT_IFVG_TOUCH', 'ENTRY_PLAN_READY'],
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
        riskAbs: next.trade?.riskAbs,
        riskUsd: next.trade?.riskUsd,
        notionalUsd: next.trade?.notionalUsd,
        initialStopPrice: next.trade?.initialStopPrice ?? next.trade?.stopPrice ?? entryPrice,
        remainingSizePct: next.trade?.remainingSizePct ?? 1,
        realizedR: next.trade?.realizedR ?? 0,
        tp1Done: Boolean(next.trade?.tp1Done),
        tp1Price: next.trade?.tp1Price ?? null,
        trailActive: Boolean(next.trade?.trailActive),
        trailStopPrice: next.trade?.trailStopPrice ?? null,
        favorableExtremePrice: next.trade?.favorableExtremePrice ?? entryPrice,
        barsHeld: next.trade?.barsHeld ?? 0,
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
        riskAbs: params.plan.riskAbs,
        riskUsd: params.plan.riskUsd,
        notionalUsd: params.plan.notionalUsd,
        initialStopPrice: params.plan.stopPrice,
        remainingSizePct: 1,
        realizedR: 0,
        tp1Done: false,
        tp1Price: null,
        trailActive: false,
        trailStopPrice: null,
        favorableExtremePrice: params.plan.entryReferencePrice,
        barsHeld: 0,
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

export async function manageScalpOpenTrade(params: {
    state: ScalpSessionState;
    market: ScalpMarketSnapshot;
    cfg: ScalpStrategyConfig;
    dryRun: boolean;
    nowMs: number;
}): Promise<{ state: ScalpSessionState; reasonCodes: string[] }> {
    const next: ScalpSessionState = {
        ...params.state,
        stats: { ...params.state.stats },
        trade: params.state.trade ? { ...params.state.trade } : null,
    };
    if (!next.trade) {
        return { state: next, reasonCodes: ['TRADE_MANAGE_SKIPPED_NO_TRADE'] };
    }

    const trade = next.trade;
    if (trade.dryRun && next.state !== 'IN_TRADE') {
        return { state: next, reasonCodes: ['TRADE_MANAGE_SKIPPED_DRYRUN_NON_LIVE_STATE'] };
    }
    const reasonCodes: string[] = [];
    const entry = toFinite(trade.entryPrice);
    const price = toFinite(params.market.quote.price);
    if (!(Number.isFinite(entry) && entry > 0 && Number.isFinite(price) && price > 0)) {
        return { state: next, reasonCodes: ['TRADE_MANAGE_INVALID_PRICING'] };
    }

    const inferredRiskAbs = Math.abs(entry - toFinite(trade.initialStopPrice ?? trade.stopPrice));
    const riskAbs = toFinite(trade.riskAbs, inferredRiskAbs);
    if (!(Number.isFinite(riskAbs) && riskAbs > 0)) {
        return { state: next, reasonCodes: ['TRADE_MANAGE_INVALID_RISKABS'] };
    }
    trade.riskAbs = riskAbs;
    trade.initialStopPrice = toFinite(trade.initialStopPrice, trade.stopPrice);
    trade.remainingSizePct = clamp(toFinite(trade.remainingSizePct, 1), 0, 1);
    trade.realizedR = toFinite(trade.realizedR, 0);
    trade.tp1Done = Boolean(trade.tp1Done);
    trade.trailActive = Boolean(trade.trailActive);
    trade.tp1Price = Number.isFinite(toFinite(trade.tp1Price, NaN)) ? Number(trade.tp1Price) : null;
    trade.trailStopPrice = Number.isFinite(toFinite(trade.trailStopPrice, NaN)) ? Number(trade.trailStopPrice) : null;
    trade.favorableExtremePrice = Number.isFinite(toFinite(trade.favorableExtremePrice, NaN))
        ? Number(trade.favorableExtremePrice)
        : entry;

    const tfMs = timeframeMinutes(params.market.confirmTf) * 60_000;
    const barsHeld = Math.max(0, Math.floor((params.nowMs - trade.openedAtMs) / Math.max(60_000, tfMs)));
    trade.barsHeld = barsHeld;
    const lastCandle = params.market.confirmCandles.at(-1);
    if (lastCandle) {
        const extreme = trade.side === 'BUY' ? high(lastCandle) : low(lastCandle);
        if (Number.isFinite(extreme)) {
            trade.favorableExtremePrice =
                trade.side === 'BUY'
                    ? Math.max(toFinite(trade.favorableExtremePrice, entry), extreme)
                    : Math.min(toFinite(trade.favorableExtremePrice, entry), extreme);
        }
    }
    const currentR = currentRForTrade(trade, price, riskAbs);
    const atrAbs = computeAtr(params.market.confirmCandles, params.cfg.data.atrPeriod);

    if (!trade.tp1Done && currentR >= params.cfg.risk.tp1R && trade.remainingSizePct > 0) {
        const tp1Pct = clamp(params.cfg.risk.tp1ClosePct, 0, 100);
        if (tp1Pct > 0) {
            const tp1Close = await closeScalpTradePortion({
                symbol: next.symbol,
                closePct: tp1Pct,
                dryRun: params.dryRun,
                reason: 'tp1_partial',
            });
            reasonCodes.push(...tp1Close.reasonCodes);
            if (tp1Close.closed) {
                const closedFraction = closeFractionFromPct(trade.remainingSizePct, tp1Pct);
                trade.remainingSizePct = Math.max(0, trade.remainingSizePct - closedFraction);
                trade.realizedR += closedFraction * params.cfg.risk.tp1R;
                trade.tp1Done = true;
                trade.tp1Price = price;
                const beOffsetAbs = Math.max(0, params.cfg.risk.breakEvenOffsetR) * riskAbs;
                const beStop = trade.side === 'BUY' ? entry + beOffsetAbs : entry - beOffsetAbs;
                trade.stopPrice = trade.side === 'BUY' ? Math.max(trade.stopPrice, beStop) : Math.min(trade.stopPrice, beStop);
                reasonCodes.push('TP1_PARTIAL_EXECUTED');
            }
        } else {
            trade.tp1Done = true;
            reasonCodes.push('TP1_DISABLED_BY_CONFIG');
        }
    }

    if (!trade.trailActive && currentR >= params.cfg.risk.trailStartR) {
        trade.trailActive = true;
        reasonCodes.push('TRAIL_ACTIVATED');
    }

    if (trade.trailActive && Number.isFinite(atrAbs) && atrAbs > 0) {
        const trailAnchor = toFinite(trade.favorableExtremePrice, entry);
        const candidateStop =
            trade.side === 'BUY'
                ? trailAnchor - params.cfg.risk.trailAtrMult * atrAbs
                : trailAnchor + params.cfg.risk.trailAtrMult * atrAbs;
        if (Number.isFinite(candidateStop) && candidateStop > 0) {
            if (trade.side === 'BUY' && candidateStop > trade.stopPrice) {
                trade.stopPrice = candidateStop;
                trade.trailStopPrice = candidateStop;
                reasonCodes.push('TRAIL_STOP_UPDATED');
            }
            if (trade.side === 'SELL' && candidateStop < trade.stopPrice) {
                trade.stopPrice = candidateStop;
                trade.trailStopPrice = candidateStop;
                reasonCodes.push('TRAIL_STOP_UPDATED');
            }
        }
    }

    const stopHit = trade.side === 'BUY' ? price <= trade.stopPrice : price >= trade.stopPrice;
    const timeStopHit = barsHeld >= Math.max(1, params.cfg.risk.timeStopBars);
    if (!stopHit && !timeStopHit) {
        return { state: next, reasonCodes: reasonCodes.length ? reasonCodes : ['TRADE_MANAGE_ACTIVE'] };
    }

    const closeRes = await closeScalpTradePortion({
        symbol: next.symbol,
        closePct: 100,
        dryRun: params.dryRun,
        reason: stopHit ? 'stop_exit' : 'time_stop_exit',
    });
    reasonCodes.push(...closeRes.reasonCodes);
    if (!closeRes.closed) {
        return { state: next, reasonCodes: [...reasonCodes, 'TRADE_EXIT_NOT_CONFIRMED'] };
    }

    const remaining = clamp(toFinite(trade.remainingSizePct, 1), 0, 1);
    const totalTradeR = toFinite(trade.realizedR, 0) + remaining * currentR;
    next.stats.realizedR = toFinite(next.stats.realizedR, 0) + totalTradeR;
    next.stats.lastExitAtMs = params.nowMs;
    if (totalTradeR > 0) {
        next.stats.wins += 1;
        next.stats.consecutiveLosses = 0;
    } else if (totalTradeR < 0) {
        next.stats.losses += 1;
        next.stats.consecutiveLosses = Math.max(0, next.stats.consecutiveLosses) + 1;
    } else {
        next.stats.consecutiveLosses = 0;
    }

    next.trade = null;
    if (next.stats.realizedR <= params.cfg.risk.dailyLossLimitR) {
        next.state = 'DONE';
        reasonCodes.push('DAILY_LOSS_LIMIT_REACHED');
    } else if (
        totalTradeR < 0 &&
        next.stats.consecutiveLosses >= Math.max(1, params.cfg.risk.consecutiveLossPauseThreshold)
    ) {
        const cooldownBars = Math.max(0, params.cfg.risk.consecutiveLossCooldownBars);
        if (cooldownBars > 0) {
            next.cooldownUntilMs = params.nowMs + cooldownBars * Math.max(60_000, tfMs);
            next.state = 'COOLDOWN';
            reasonCodes.push('CONSECUTIVE_LOSS_COOLDOWN_SET');
        } else {
            next.state = 'IDLE';
            reasonCodes.push('CONSECUTIVE_LOSS_PAUSE_TRIGGERED');
        }
    } else {
        next.state = 'IDLE';
        reasonCodes.push('TRADE_EXITED_READY_NEXT_SETUP');
    }

    reasonCodes.push(stopHit ? 'TRADE_EXIT_STOP_HIT' : 'TRADE_EXIT_TIME_STOP');
    return { state: next, reasonCodes };
}
