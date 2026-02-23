import { evaluatePairEligibility } from '../selector';
import { resolveReentryLockMinutes, shouldInvalidateByStop } from '../engine';
import type { ForexPositionContext, ForexRegimePacket, ForexSide } from '../types';
import { createSeededRng } from './random';
import { applyExecutionPrice, applySpreadStress, executionSlippageBps } from './models';
import type { StressedQuote } from './models';
import type {
    ReplayEntrySignal,
    ReplayEquityPoint,
    ReplayInputFile,
    ReplayLedgerRow,
    ReplayQuote,
    ReplayResult,
    ReplayRuntimeConfig,
    ReplaySummary,
    ReplayTimelineEvent,
} from './types';

type PositionState = {
    side: ForexSide;
    entryPrice: number;
    initialStopPrice: number;
    currentStopPrice: number;
    takeProfitPrice: number | null;
    units: number;
    initialUnits: number;
    initialRiskAbs: number;
    partialTakenPct: number;
    trailingActive: boolean;
    openedAtMs: number;
    entryNotionalUsd: number;
};

function utcDayKey(ts: number): string {
    const d = new Date(ts);
    const y = d.getUTCFullYear();
    const m = `${d.getUTCMonth() + 1}`.padStart(2, '0');
    const day = `${d.getUTCDate()}`.padStart(2, '0');
    return `${y}-${m}-${day}`;
}

function toTs(value: number | string): number {
    const n = Number(value);
    if (Number.isFinite(n) && n > 0) return n;
    const parsed = Date.parse(String(value));
    if (!Number.isFinite(parsed) || parsed <= 0) {
        throw new Error(`Invalid timestamp: ${String(value)}`);
    }
    return parsed;
}

function clamp(value: number, min: number, max: number): number {
    return Math.max(min, Math.min(max, value));
}

function sideSign(side: ForexSide): number {
    return side === 'BUY' ? 1 : -1;
}

function closeSide(side: ForexSide): ForexSide {
    return side === 'BUY' ? 'SELL' : 'BUY';
}

function buildPacket(pair: string): ForexRegimePacket {
    return {
        pair,
        generatedAtMs: 0,
        regime: 'trend_up',
        permission: 'both',
        allowed_modules: ['pullback'],
        risk_state: 'normal',
        confidence: 0.7,
        htf_context: {
            nearest_support: null,
            nearest_resistance: null,
            distance_to_support_atr1h: null,
            distance_to_resistance_atr1h: null,
        },
        notes_codes: ['REPLAY_PACKET_PLACEHOLDER'],
    };
}

function toPositionContext(pair: string, packet: ForexRegimePacket, position: PositionState): ForexPositionContext {
    return {
        pair,
        side: position.side,
        entryModule: 'pullback',
        module: 'pullback',
        entryPrice: position.entryPrice,
        initialStopPrice: position.initialStopPrice,
        currentStopPrice: position.currentStopPrice,
        initialRiskPrice: position.initialRiskAbs,
        partialTakenPct: position.partialTakenPct,
        trailingActive: position.trailingActive,
        trailingMode: position.trailingActive ? 'structure' : 'none',
        tp1Price: null,
        tp2Price: null,
        openedAtMs: position.openedAtMs,
        lastManagedAtMs: position.openedAtMs,
        lastCloseAtMs: null,
        stopPrice: position.currentStopPrice,
        updatedAtMs: position.openedAtMs,
        entryNotionalUsd: position.entryNotionalUsd,
        entryLeverage: 1,
        packet,
    };
}

function maxDrawdownPct(curve: ReplayEquityPoint[]): number {
    let peak = Number.NEGATIVE_INFINITY;
    let maxDd = 0;
    for (const point of curve) {
        peak = Math.max(peak, point.equityUsd);
        if (!(peak > 0)) continue;
        const dd = ((peak - point.equityUsd) / peak) * 100;
        if (dd > maxDd) maxDd = dd;
    }
    return maxDd;
}

export function mergeReentryLockUntil(currentUntilMs: number | null, nextUntilMs: number | null): number | null {
    if (!(Number.isFinite(nextUntilMs as number) && Number(nextUntilMs) > 0)) return currentUntilMs;
    if (!(Number.isFinite(currentUntilMs as number) && Number(currentUntilMs) > 0)) return Number(nextUntilMs);
    return Math.max(Number(currentUntilMs), Number(nextUntilMs));
}

export function defaultReplayConfig(pair = 'EURUSD'): ReplayRuntimeConfig {
    return {
        pair: pair.toUpperCase(),
        startingEquityUsd: 10_000,
        defaultNotionalUsd: 850,
        atr1hAbs: 0.0012,
        executeMinutes: 5,
        forceCloseOnHighEvent: true,
        reentry: {
            lockMinutes: 5,
            lockMinutesTimeStop: 5,
            lockMinutesRegimeFlip: 10,
            lockMinutesEventRisk: 20,
        },
        spreadStress: {
            transitionBufferMinutes: 20,
            transitionMultiplier: 1.6,
            rolloverMultiplier: 1.8,
            mediumEventMultiplier: 1.4,
            highEventMultiplier: 2.0,
        },
        slippage: {
            seed: 7,
            entryBaseBps: 0.2,
            exitBaseBps: 0.3,
            randomBps: 0.15,
            shockBps: 0.6,
            mediumEventBps: 0.4,
            highEventBps: 0.9,
        },
        management: {
            partialAtR: 1,
            partialClosePct: 50,
            trailingDistanceR: 0.9,
            enableTrailing: true,
        },
        rollover: {
            dailyFeeBps: 0.8,
        },
    };
}

export function normalizeReplayInput(input: ReplayInputFile): {
    pair: string;
    quotes: ReplayQuote[];
    entries: ReplayEntrySignal[];
} {
    const pair = String(input.pair || 'EURUSD').trim().toUpperCase();
    if (!Array.isArray(input.quotes) || input.quotes.length === 0) {
        throw new Error('Replay input requires a non-empty quotes array');
    }
    const quotes = input.quotes
        .map((q) => ({
            ts: toTs(q.ts),
            bid: Number(q.bid),
            ask: Number(q.ask),
            eventRisk: q.eventRisk,
            forceCloseReasonCode: q.forceCloseReasonCode,
            shock: Boolean(q.shock),
            rollover: Boolean(q.rollover),
            spreadMultiplier: Number.isFinite(Number(q.spreadMultiplier)) ? Number(q.spreadMultiplier) : undefined,
            note: q.note,
        }))
        .sort((a, b) => a.ts - b.ts);

    const entries = (Array.isArray(input.entries) ? input.entries : [])
        .map((e) => ({
            ts: toTs(e.ts),
            side: e.side,
            stopPrice: Number(e.stopPrice),
            takeProfitPrice:
                Number.isFinite(Number(e.takeProfitPrice)) && Number(e.takeProfitPrice) > 0 ? Number(e.takeProfitPrice) : null,
            notionalUsd: Number.isFinite(Number(e.notionalUsd)) && Number(e.notionalUsd) > 0 ? Number(e.notionalUsd) : undefined,
            label: e.label,
        }))
        .sort((a, b) => a.ts - b.ts);

    return { pair, quotes, entries };
}

export function runReplay(params: {
    quotes: ReplayQuote[];
    entries: ReplayEntrySignal[];
    config: ReplayRuntimeConfig;
}): ReplayResult {
    const quotes = params.quotes
        .slice()
        .sort((a, b) => a.ts - b.ts);
    const entries = params.entries
        .slice()
        .sort((a, b) => a.ts - b.ts);
    if (!quotes.length) throw new Error('Cannot run replay with empty quotes');

    const cfg = params.config;
    const rng = createSeededRng(cfg.slippage.seed);
    const packet = buildPacket(cfg.pair);

    let equityUsd = cfg.startingEquityUsd;
    let realizedPnlUsd = 0;
    let rolloverFeesUsd = 0;
    let lockUntilMs: number | null = null;
    let position: PositionState | null = null;
    let lastDayKey: string | null = null;
    let entryIndex = 0;
    let rowId = 1;

    const ledger: ReplayLedgerRow[] = [];
    const timeline: ReplayTimelineEvent[] = [];
    const equityCurve: ReplayEquityPoint[] = [];

    const addLedgerRow = (row: Omit<ReplayLedgerRow, 'id'>) => {
        ledger.push({ ...row, id: rowId++ });
    };

    const unrealizedPnl = (quoteBid: number, quoteAsk: number): number => {
        if (!position) return 0;
        if (position.side === 'BUY') return (quoteBid - position.entryPrice) * position.units;
        return (position.entryPrice - quoteAsk) * position.units;
    };

    const markEquity = (ts: number, quoteBid: number, quoteAsk: number) => {
        equityCurve.push({
            ts,
            equityUsd,
            realizedPnlUsd,
            unrealizedPnlUsd: unrealizedPnl(quoteBid, quoteAsk),
        });
    };

    const updateLock = (ts: number, reasonCodes: string[]) => {
        const lockMinutes = resolveReentryLockMinutes({
            reasonCodes,
            reentry: cfg.reentry,
            executeMinutes: cfg.executeMinutes,
        });
        if (!(typeof lockMinutes === 'number' && Number.isFinite(lockMinutes) && lockMinutes > 0)) return;
        const nextUntil = ts + lockMinutes * 60_000;
        const mergedUntil = mergeReentryLockUntil(lockUntilMs, nextUntil);
        if (mergedUntil !== lockUntilMs) {
            lockUntilMs = mergedUntil;
            timeline.push({
                ts,
                type: 'REENTRY_LOCK_UPDATED',
                reasonCodes,
                details: { lockUntilMs, lockMinutes },
            });
        }
    };

    const closePosition = (reasonCodes: string[], quote: StressedQuote) => {
        if (!position) return;
        const exitSide = closeSide(position.side);
        const reference = exitSide === 'BUY' ? quote.ask : quote.bid;
        const slippageBps = executionSlippageBps({
            quote,
            cfg: cfg.slippage,
            rng,
            isEntry: false,
        });
        const exitPrice = applyExecutionPrice({
            side: exitSide,
            referencePrice: reference,
            slippageBps,
        });
        const pnlUsd = (exitPrice - position.entryPrice) * sideSign(position.side) * position.units;
        realizedPnlUsd += pnlUsd;
        equityUsd += pnlUsd;

        addLedgerRow({
            ts: quote.ts,
            kind: 'EXIT',
            side: exitSide,
            price: exitPrice,
            units: position.units,
            notionalUsd: position.units * exitPrice,
            pnlUsd,
            feeUsd: 0,
            reasonCodes,
            positionUnitsAfter: 0,
            equityUsdAfter: equityUsd,
        });
        timeline.push({
            ts: quote.ts,
            type: 'POSITION_CLOSED',
            reasonCodes,
            details: {
                side: position.side,
                exitSide,
                exitPrice,
                pnlUsd,
            },
        });

        updateLock(quote.ts, reasonCodes);
        position = null;
    };

    for (const quote of quotes) {
        const stressed = applySpreadStress(quote, cfg.spreadStress);
        const dayKey = utcDayKey(stressed.ts);

        if (position && (quote.rollover || (lastDayKey !== null && dayKey !== lastDayKey))) {
            const markMid = (stressed.bid + stressed.ask) / 2;
            const positionNotional = position.units * markMid;
            const feeUsd = Math.max(0, (positionNotional * cfg.rollover.dailyFeeBps) / 10_000);
            if (feeUsd > 0) {
                rolloverFeesUsd += feeUsd;
                equityUsd -= feeUsd;
                addLedgerRow({
                    ts: stressed.ts,
                    kind: 'ROLLOVER_FEE',
                    side: null,
                    price: null,
                    units: null,
                    notionalUsd: positionNotional,
                    pnlUsd: 0,
                    feeUsd,
                    reasonCodes: ['ROLLOVER_FEE_APPLIED'],
                    positionUnitsAfter: position.units,
                    equityUsdAfter: equityUsd,
                });
                timeline.push({
                    ts: stressed.ts,
                    type: 'ROLLOVER_FEE_APPLIED',
                    reasonCodes: ['ROLLOVER_FEE_APPLIED'],
                    details: { feeUsd, positionNotional },
                });
            }
        }
        lastDayKey = dayKey;

        if (position) {
            if (cfg.forceCloseOnHighEvent && stressed.eventRisk === 'high') {
                closePosition(['EVENT_HIGH_FORCE_CLOSE'], stressed);
            } else if (quote.forceCloseReasonCode) {
                closePosition([String(quote.forceCloseReasonCode).toUpperCase()], stressed);
            } else {
                const rMultiple =
                    position.side === 'BUY'
                        ? (stressed.bid - position.entryPrice) / position.initialRiskAbs
                        : (position.entryPrice - stressed.ask) / position.initialRiskAbs;

                if (
                    position.partialTakenPct < cfg.management.partialClosePct &&
                    cfg.management.partialClosePct > 0 &&
                    rMultiple >= cfg.management.partialAtR
                ) {
                    const fraction = clamp(cfg.management.partialClosePct / 100, 0, 1);
                    const closeUnits = position.units * fraction;
                    const exitSide = closeSide(position.side);
                    const reference = exitSide === 'BUY' ? stressed.ask : stressed.bid;
                    const slippageBps = executionSlippageBps({
                        quote: stressed,
                        cfg: cfg.slippage,
                        rng,
                        isEntry: false,
                    });
                    const partialExitPrice = applyExecutionPrice({
                        side: exitSide,
                        referencePrice: reference,
                        slippageBps,
                    });
                    const pnlUsd = (partialExitPrice - position.entryPrice) * sideSign(position.side) * closeUnits;
                    realizedPnlUsd += pnlUsd;
                    equityUsd += pnlUsd;
                    position.units -= closeUnits;
                    position.partialTakenPct = cfg.management.partialClosePct;
                    position.currentStopPrice = position.entryPrice;
                    position.trailingActive = cfg.management.enableTrailing;

                    addLedgerRow({
                        ts: stressed.ts,
                        kind: 'PARTIAL_EXIT',
                        side: exitSide,
                        price: partialExitPrice,
                        units: closeUnits,
                        notionalUsd: closeUnits * partialExitPrice,
                        pnlUsd,
                        feeUsd: 0,
                        reasonCodes: ['PARTIAL_AT_TARGET_R'],
                        positionUnitsAfter: position.units,
                        equityUsdAfter: equityUsd,
                    });
                    timeline.push({
                        ts: stressed.ts,
                        type: 'PARTIAL_TAKEN',
                        reasonCodes: ['PARTIAL_AT_TARGET_R'],
                        details: { closeUnits, partialExitPrice, rMultiple },
                    });
                }

                if (position && position.trailingActive) {
                    const trailDistance = position.initialRiskAbs * Math.max(0.1, cfg.management.trailingDistanceR);
                    if (position.side === 'BUY') {
                        const nextStop = stressed.bid - trailDistance;
                        if (nextStop > position.currentStopPrice) {
                            position.currentStopPrice = nextStop;
                            timeline.push({
                                ts: stressed.ts,
                                type: 'STOP_TIGHTENED',
                                reasonCodes: ['TRAILING_STOP_TIGHTENED'],
                                details: { nextStop },
                            });
                        }
                    } else {
                        const nextStop = stressed.ask + trailDistance;
                        if (nextStop < position.currentStopPrice) {
                            position.currentStopPrice = nextStop;
                            timeline.push({
                                ts: stressed.ts,
                                type: 'STOP_TIGHTENED',
                                reasonCodes: ['TRAILING_STOP_TIGHTENED'],
                                details: { nextStop },
                            });
                        }
                    }
                }

                if (position && Number.isFinite(position.takeProfitPrice as number) && (position.takeProfitPrice as number) > 0) {
                    const tp = Number(position.takeProfitPrice);
                    const tpHit = position.side === 'BUY' ? stressed.bid >= tp : stressed.ask <= tp;
                    if (tpHit) {
                        closePosition(['TAKE_PROFIT_HIT'], stressed);
                    }
                }

                if (position) {
                    const context = toPositionContext(cfg.pair, packet, position);
                    const stopCheck = shouldInvalidateByStop({
                        context,
                        openSide: position.side,
                        bidPrice: stressed.bid,
                        offerPrice: stressed.ask,
                        midPrice: stressed.mid,
                    });
                    if (stopCheck.invalidated) {
                        closePosition([stopCheck.reasonCode || 'STOP_INVALIDATED'], stressed);
                    }
                }
            }
        }

        while (!position && entryIndex < entries.length && entries[entryIndex]!.ts <= stressed.ts) {
            const signal = entries[entryIndex]!;
            entryIndex += 1;

            if (lockUntilMs !== null && stressed.ts < lockUntilMs) {
                timeline.push({
                    ts: stressed.ts,
                    type: 'ENTRY_BLOCKED',
                    reasonCodes: ['REENTRY_NEXT_BAR_LOCK'],
                    details: { lockUntilMs, signalTs: signal.ts },
                });
                continue;
            }

            const spreadToAtr1h = cfg.atr1hAbs > 0 ? stressed.spreadAbs / cfg.atr1hAbs : Number.POSITIVE_INFINITY;
            const eligibility = evaluatePairEligibility({
                pair: cfg.pair,
                nowMs: stressed.ts,
                staleEvents: false,
                events: [],
                metrics: {
                    pair: cfg.pair,
                    epic: cfg.pair,
                    sessionTag: 'LONDON',
                    price: stressed.mid,
                    spreadAbs: stressed.spreadAbs,
                    spreadPips: 0,
                    spreadToAtr1h,
                    atr1h: cfg.atr1hAbs,
                    atr4h: cfg.atr1hAbs * 2,
                    atr1hPercent: stressed.mid > 0 ? cfg.atr1hAbs / stressed.mid : 0,
                    trendStrength: 0.8,
                    chopScore: 0.3,
                    shockFlag: Boolean(stressed.shock),
                    timestampMs: stressed.ts,
                },
            });
            if (!eligibility.eligible) {
                timeline.push({
                    ts: stressed.ts,
                    type: 'ENTRY_BLOCKED',
                    reasonCodes: eligibility.reasons,
                    details: { spreadToAtr1h },
                });
                continue;
            }

            const entrySide = signal.side;
            const reference = entrySide === 'BUY' ? stressed.ask : stressed.bid;
            const slippageBps = executionSlippageBps({
                quote: stressed,
                cfg: cfg.slippage,
                rng,
                isEntry: true,
            });
            const entryPrice = applyExecutionPrice({
                side: entrySide,
                referencePrice: reference,
                slippageBps,
            });
            const notionalUsd = Number.isFinite(signal.notionalUsd as number) && (signal.notionalUsd as number) > 0
                ? Number(signal.notionalUsd)
                : cfg.defaultNotionalUsd;
            const units = notionalUsd / entryPrice;
            const stop = Number(signal.stopPrice);
            const riskAbs = Math.abs(entryPrice - stop);
            if (!(Number.isFinite(units) && units > 0 && Number.isFinite(riskAbs) && riskAbs > 0)) {
                timeline.push({
                    ts: stressed.ts,
                    type: 'ENTRY_BLOCKED',
                    reasonCodes: ['INVALID_ENTRY_PARAMETERS'],
                    details: { entryPrice, stop, units, riskAbs },
                });
                continue;
            }

            position = {
                side: entrySide,
                entryPrice,
                initialStopPrice: stop,
                currentStopPrice: stop,
                takeProfitPrice: Number.isFinite(Number(signal.takeProfitPrice)) ? Number(signal.takeProfitPrice) : null,
                units,
                initialUnits: units,
                initialRiskAbs: riskAbs,
                partialTakenPct: 0,
                trailingActive: false,
                openedAtMs: stressed.ts,
                entryNotionalUsd: notionalUsd,
            };

            addLedgerRow({
                ts: stressed.ts,
                kind: 'ENTRY',
                side: entrySide,
                price: entryPrice,
                units,
                notionalUsd,
                pnlUsd: 0,
                feeUsd: 0,
                reasonCodes: ['ENTRY_OPENED'],
                positionUnitsAfter: units,
                equityUsdAfter: equityUsd,
            });
            timeline.push({
                ts: stressed.ts,
                type: 'ENTRY_OPENED',
                reasonCodes: ['ENTRY_OPENED'],
                details: { side: entrySide, entryPrice, stopPrice: stop, notionalUsd },
            });
        }

        markEquity(stressed.ts, stressed.bid, stressed.ask);
    }

    if (position) {
        const last = quotes[quotes.length - 1]!;
        const stressedLast = applySpreadStress(last, cfg.spreadStress);
        closePosition(['END_OF_REPLAY_FLAT'], stressedLast);
        markEquity(stressedLast.ts, stressedLast.bid, stressedLast.ask);
    }

    const closedLegs = ledger.filter((row) => row.kind === 'EXIT' || row.kind === 'PARTIAL_EXIT');
    const winningLegs = closedLegs.filter((row) => row.pnlUsd > 0).length;
    const winRatePct = closedLegs.length > 0 ? (winningLegs / closedLegs.length) * 100 : 0;

    const summary: ReplaySummary = {
        pair: cfg.pair,
        startTs: quotes[0]?.ts ?? null,
        endTs: quotes[quotes.length - 1]?.ts ?? null,
        startingEquityUsd: cfg.startingEquityUsd,
        endingEquityUsd: equityUsd,
        realizedPnlUsd,
        rolloverFeesUsd,
        returnPct: cfg.startingEquityUsd > 0 ? ((equityUsd - cfg.startingEquityUsd) / cfg.startingEquityUsd) * 100 : 0,
        closedLegs: closedLegs.length,
        winningLegs,
        winRatePct,
        maxDrawdownPct: maxDrawdownPct(equityCurve),
        finalPositionOpen: Boolean(position),
    };

    return {
        summary,
        ledger,
        timeline,
        equityCurve,
    };
}
