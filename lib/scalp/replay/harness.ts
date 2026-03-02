import { buildAsiaRangeSnapshot, computeAtr, detectConfirmation, detectIfvg, detectIfvgTouch, detectSweepLifecycle } from '../detectors';
import { buildScalpEntryPlan } from '../execution';
import { pipSizeForScalpSymbol, timeframeMinutes } from '../marketData';
import { buildScalpSessionWindows } from '../sessions';
import { getScalpStrategyConfig } from '../config';
import { advanceScalpStateMachine, createInitialScalpSessionState, deriveScalpDayKey } from '../stateMachine';
import type { ScalpCandle, ScalpMarketSnapshot, ScalpSessionState, ScalpStrategyConfig } from '../types';
import type {
    ScalpReplayCandle,
    ScalpReplayInputFile,
    ScalpReplayResult,
    ScalpReplayRuntimeConfig,
    ScalpReplaySummary,
    ScalpReplayTimelineEvent,
    ScalpReplayTrade,
} from './types';

type ReplayPosition = {
    tradeId: string;
    dayKey: string;
    side: 'BUY' | 'SELL';
    entryTs: number;
    entryPrice: number;
    stopPrice: number;
    takeProfitPrice: number;
    riskAbs: number;
    riskUsd: number;
    notionalUsd: number;
    activeFromIndex: number;
};

function toTs(value: number | string): number {
    const n = Number(value);
    if (Number.isFinite(n) && n > 0) return n;
    const parsed = Date.parse(String(value));
    if (!Number.isFinite(parsed) || parsed <= 0) {
        throw new Error(`Invalid candle timestamp: ${String(value)}`);
    }
    return parsed;
}

function toFinite(value: unknown, fallback = NaN): number {
    const n = Number(value);
    return Number.isFinite(n) ? n : fallback;
}

function dedupeReasonCodes(codes: string[]): string[] {
    return Array.from(new Set(codes.map((c) => String(c || '').trim().toUpperCase()).filter((c) => c.length > 0)));
}

function buildStrategyConfig(runtime: ScalpReplayRuntimeConfig): ScalpStrategyConfig {
    return {
        enabled: true,
        defaultSymbol: runtime.symbol,
        dryRunDefault: true,
        cadence: { executeMinutes: runtime.executeMinutes },
        sessions: {
            clockMode: runtime.strategy.sessionClockMode,
            asiaWindowLocal: runtime.strategy.asiaWindowLocal,
            raidWindowLocal: runtime.strategy.raidWindowLocal,
        },
        timeframes: {
            asiaBase: runtime.strategy.asiaBaseTf,
            confirm: runtime.strategy.confirmTf,
        },
        sweep: {
            bufferPips: runtime.strategy.sweepBufferPips,
            bufferAtrMult: runtime.strategy.sweepBufferAtrMult,
            bufferSpreadMult: runtime.strategy.sweepBufferSpreadMult,
            rejectInsidePips: runtime.strategy.sweepRejectInsidePips,
            rejectMaxBars: runtime.strategy.sweepRejectMaxBars,
            minWickBodyRatio: runtime.strategy.sweepMinWickBodyRatio,
        },
        confirm: {
            displacementBodyAtrMult: runtime.strategy.displacementBodyAtrMult,
            displacementRangeAtrMult: runtime.strategy.displacementRangeAtrMult,
            closeInExtremePct: runtime.strategy.displacementCloseInExtremePct,
            mssLookbackBars: runtime.strategy.mssLookbackBars,
            mssBreakBufferPips: runtime.strategy.mssBreakBufferPips,
            mssBreakBufferAtrMult: runtime.strategy.mssBreakBufferAtrMult,
            ttlMinutes: runtime.strategy.confirmTtlMinutes,
        },
        ifvg: {
            minAtrMult: runtime.strategy.ifvgMinAtrMult,
            maxAtrMult: runtime.strategy.ifvgMaxAtrMult,
            ttlMinutes: runtime.strategy.ifvgTtlMinutes,
            entryMode: runtime.strategy.ifvgEntryMode,
        },
        risk: {
            cooldownAfterLossMinutes: 0,
            maxTradesPerSymbolPerDay: runtime.strategy.maxTradesPerDay,
            maxOpenPositionsPerSymbol: 1,
            killSwitch: false,
            riskPerTradePct: runtime.strategy.riskPerTradePct,
            referenceEquityUsd: runtime.strategy.referenceEquityUsd,
            minNotionalUsd: runtime.strategy.minNotionalUsd,
            maxNotionalUsd: runtime.strategy.maxNotionalUsd,
            takeProfitR: runtime.strategy.takeProfitR,
            stopBufferPips: runtime.strategy.stopBufferPips,
            stopBufferSpreadMult: runtime.strategy.stopBufferSpreadMult,
            minStopDistancePips: runtime.strategy.minStopDistancePips,
        },
        execution: {
            liveEnabled: false,
            entryOrderType: 'MARKET',
            defaultLeverage: 1,
        },
        idempotency: { runLockSeconds: 1 },
        storage: { sessionTtlSeconds: 24 * 60 * 60, journalMax: 2000 },
        data: {
            atrPeriod: runtime.strategy.atrPeriod,
            minAsiaCandles: runtime.strategy.minAsiaCandles,
            minBaseCandles: runtime.strategy.minBaseCandles,
            minConfirmCandles: runtime.strategy.minConfirmCandles,
            maxCandlesPerRequest: 1000,
        },
    };
}

export function defaultScalpReplayConfig(symbol = 'EURUSD'): ScalpReplayRuntimeConfig {
    const cfg = getScalpStrategyConfig();
    return {
        symbol: symbol.toUpperCase(),
        executeMinutes: cfg.cadence.executeMinutes,
        defaultSpreadPips: 1.1,
        spreadFactor: 1,
        slippagePips: 0.15,
        preferStopWhenBothHit: true,
        forceCloseAtEnd: true,
        strategy: {
            sessionClockMode: cfg.sessions.clockMode,
            asiaWindowLocal: cfg.sessions.asiaWindowLocal,
            raidWindowLocal: cfg.sessions.raidWindowLocal,
            asiaBaseTf: 'M1',
            confirmTf: 'M1',
            maxTradesPerDay: cfg.risk.maxTradesPerSymbolPerDay,
            riskPerTradePct: cfg.risk.riskPerTradePct,
            referenceEquityUsd: cfg.risk.referenceEquityUsd,
            minNotionalUsd: cfg.risk.minNotionalUsd,
            maxNotionalUsd: cfg.risk.maxNotionalUsd,
            takeProfitR: Math.min(cfg.risk.takeProfitR, 1.2),
            stopBufferPips: cfg.risk.stopBufferPips,
            stopBufferSpreadMult: cfg.risk.stopBufferSpreadMult,
            minStopDistancePips: cfg.risk.minStopDistancePips,
            // Aggressive defaults: prioritize setup frequency for parameter exploration.
            sweepBufferPips: Math.max(0.05, Math.min(cfg.sweep.bufferPips, 0.25)),
            sweepBufferAtrMult: cfg.sweep.bufferAtrMult,
            sweepBufferSpreadMult: cfg.sweep.bufferSpreadMult,
            sweepRejectInsidePips: Math.min(cfg.sweep.rejectInsidePips, 0.05),
            sweepRejectMaxBars: Math.max(20, cfg.sweep.rejectMaxBars),
            sweepMinWickBodyRatio: Math.min(cfg.sweep.minWickBodyRatio, 0.8),
            displacementBodyAtrMult: Math.min(cfg.confirm.displacementBodyAtrMult, 0.08),
            displacementRangeAtrMult: Math.min(cfg.confirm.displacementRangeAtrMult, 0.15),
            displacementCloseInExtremePct: cfg.confirm.closeInExtremePct,
            mssLookbackBars: 1,
            mssBreakBufferPips: cfg.confirm.mssBreakBufferPips,
            mssBreakBufferAtrMult: cfg.confirm.mssBreakBufferAtrMult,
            confirmTtlMinutes: cfg.confirm.ttlMinutes,
            ifvgMinAtrMult: 0,
            ifvgMaxAtrMult: Math.max(cfg.ifvg.maxAtrMult, 3),
            ifvgTtlMinutes: cfg.ifvg.ttlMinutes,
            ifvgEntryMode: 'first_touch',
            atrPeriod: cfg.data.atrPeriod,
            minAsiaCandles: cfg.data.minAsiaCandles,
            minBaseCandles: cfg.data.minBaseCandles,
            minConfirmCandles: cfg.data.minConfirmCandles,
        },
    };
}

export function normalizeScalpReplayInput(input: ScalpReplayInputFile): {
    symbol: string;
    candles: ScalpReplayCandle[];
    pipSize: number;
} {
    const symbol = String(input.symbol || 'EURUSD').trim().toUpperCase();
    if (!Array.isArray(input.candles) || input.candles.length === 0) {
        throw new Error('Replay input requires non-empty candles');
    }
    const normalized: ScalpReplayCandle[] = input.candles
        .map((c) => {
            const ts = toTs(c.ts);
            const open = toFinite(c.open);
            const high = toFinite(c.high);
            const low = toFinite(c.low);
            const close = toFinite(c.close);
            const volume = Number.isFinite(toFinite(c.volume)) ? toFinite(c.volume) : 0;
            const spreadPips = Number.isFinite(toFinite(c.spreadPips)) ? toFinite(c.spreadPips) : NaN;
            if (![open, high, low, close].every((v) => Number.isFinite(v) && v > 0)) {
                throw new Error(`Invalid candle at ${ts}`);
            }
            return {
                ts,
                open,
                high,
                low,
                close,
                volume,
                spreadPips: Number.isFinite(spreadPips) && spreadPips >= 0 ? spreadPips : NaN,
            };
        })
        .sort((a, b) => a.ts - b.ts);
    const pipSize = Number.isFinite(toFinite(input.pipSize)) && toFinite(input.pipSize) > 0 ? toFinite(input.pipSize) : pipSizeForScalpSymbol(symbol);
    return { symbol, candles: normalized, pipSize };
}

function aggregateCandles(candles: ScalpReplayCandle[], tfMinutes: number): ScalpCandle[] {
    const tfMs = Math.max(1, Math.floor(tfMinutes)) * 60_000;
    const buckets = new Map<number, ScalpReplayCandle[]>();
    for (const candle of candles) {
        const start = Math.floor(candle.ts / tfMs) * tfMs;
        if (!buckets.has(start)) buckets.set(start, []);
        buckets.get(start)!.push(candle);
    }
    const out: ScalpCandle[] = [];
    const keys = Array.from(buckets.keys()).sort((a, b) => a - b);
    for (const key of keys) {
        const rows = buckets.get(key)!;
        rows.sort((a, b) => a.ts - b.ts);
        const first = rows[0]!;
        const last = rows[rows.length - 1]!;
        const high = Math.max(...rows.map((r) => r.high));
        const low = Math.min(...rows.map((r) => r.low));
        const volume = rows.reduce((acc, row) => acc + row.volume, 0);
        out.push([key, first.open, high, low, last.close, volume]);
    }
    return out;
}

function latestTs(candles: ScalpCandle[]): number | null {
    const ts = candles.at(-1)?.[0];
    return Number.isFinite(Number(ts)) ? Number(ts) : null;
}

function buildReplayMarketSnapshot(params: {
    symbol: string;
    nowMs: number;
    sourceCandles: ScalpReplayCandle[];
    pipSize: number;
    runtime: ScalpReplayRuntimeConfig;
}): ScalpMarketSnapshot {
    const upto = params.sourceCandles.filter((c) => c.ts <= params.nowMs);
    const baseTfMinutes = timeframeMinutes(params.runtime.strategy.asiaBaseTf);
    const confirmTfMinutes = timeframeMinutes(params.runtime.strategy.confirmTf);
    const baseTfMs = baseTfMinutes * 60_000;
    const confirmTfMs = confirmTfMinutes * 60_000;
    const baseCandles = aggregateCandles(upto, baseTfMinutes).filter((c) => c[0] + baseTfMs <= params.nowMs);
    const confirmCandles = aggregateCandles(upto, confirmTfMinutes).filter((c) => c[0] + confirmTfMs <= params.nowMs);
    const last = upto[upto.length - 1];
    if (!last) {
        throw new Error('No candle available at replay nowMs');
    }
    const spreadPipsRaw = Number.isFinite(last.spreadPips) ? last.spreadPips : params.runtime.defaultSpreadPips;
    const spreadPips = Math.max(0, spreadPipsRaw * params.runtime.spreadFactor);
    const spreadAbs = spreadPips * params.pipSize;
    const bid = last.close - spreadAbs / 2;
    const offer = last.close + spreadAbs / 2;

    return {
        symbol: params.symbol,
        epic: `REPLAY:${params.symbol}`,
        nowMs: params.nowMs,
        quote: {
            price: last.close,
            bid: Number.isFinite(bid) ? bid : null,
            offer: Number.isFinite(offer) ? offer : null,
            spreadAbs,
            spreadPips,
            tsMs: last.ts,
        },
        baseTf: params.runtime.strategy.asiaBaseTf,
        confirmTf: params.runtime.strategy.confirmTf,
        baseCandles,
        confirmCandles,
    };
}

function appendTimeline(timeline: ScalpReplayTimelineEvent[], event: ScalpReplayTimelineEvent) {
    timeline.push({
        ts: event.ts,
        type: event.type,
        state: event.state,
        reasonCodes: dedupeReasonCodes(event.reasonCodes),
        payload: event.payload,
    });
}

function resolveExitFromCandle(params: {
    position: ReplayPosition;
    candle: ScalpReplayCandle;
    slippageAbs: number;
    preferStopWhenBothHit: boolean;
}): { hit: boolean; exitPrice: number; reason: 'STOP' | 'TP' } | null {
    const p = params.position;
    const c = params.candle;
    const slip = Math.max(0, params.slippageAbs);

    if (p.side === 'BUY') {
        const stopHit = c.low <= p.stopPrice;
        const tpHit = c.high >= p.takeProfitPrice;
        if (!stopHit && !tpHit) return null;
        if (stopHit && tpHit && params.preferStopWhenBothHit) {
            return { hit: true, exitPrice: p.stopPrice - slip, reason: 'STOP' };
        }
        if (stopHit && tpHit && !params.preferStopWhenBothHit) {
            return { hit: true, exitPrice: p.takeProfitPrice - slip, reason: 'TP' };
        }
        if (stopHit) return { hit: true, exitPrice: p.stopPrice - slip, reason: 'STOP' };
        return { hit: true, exitPrice: p.takeProfitPrice - slip, reason: 'TP' };
    }

    const stopHit = c.high >= p.stopPrice;
    const tpHit = c.low <= p.takeProfitPrice;
    if (!stopHit && !tpHit) return null;
    if (stopHit && tpHit && params.preferStopWhenBothHit) {
        return { hit: true, exitPrice: p.stopPrice + slip, reason: 'STOP' };
    }
    if (stopHit && tpHit && !params.preferStopWhenBothHit) {
        return { hit: true, exitPrice: p.takeProfitPrice + slip, reason: 'TP' };
    }
    if (stopHit) return { hit: true, exitPrice: p.stopPrice + slip, reason: 'STOP' };
    return { hit: true, exitPrice: p.takeProfitPrice + slip, reason: 'TP' };
}

function closePositionAsTrade(params: {
    position: ReplayPosition;
    exitTs: number;
    exitPrice: number;
    exitReason: 'STOP' | 'TP' | 'FORCE_CLOSE';
}): ScalpReplayTrade {
    const holdMinutes = Math.max(0, (params.exitTs - params.position.entryTs) / 60_000);
    const pnlAbs =
        params.position.side === 'BUY'
            ? params.exitPrice - params.position.entryPrice
            : params.position.entryPrice - params.exitPrice;
    const rMultiple = params.position.riskAbs > 0 ? pnlAbs / params.position.riskAbs : 0;
    const pnlUsd = rMultiple * params.position.riskUsd;

    return {
        id: params.position.tradeId,
        dayKey: params.position.dayKey,
        side: params.position.side,
        entryTs: params.position.entryTs,
        exitTs: params.exitTs,
        holdMinutes,
        entryPrice: params.position.entryPrice,
        stopPrice: params.position.stopPrice,
        takeProfitPrice: params.position.takeProfitPrice,
        exitPrice: params.exitPrice,
        exitReason: params.exitReason,
        riskAbs: params.position.riskAbs,
        riskUsd: params.position.riskUsd,
        notionalUsd: params.position.notionalUsd,
        rMultiple,
        pnlUsd,
    };
}

function applyPhaseDetectors(params: {
    state: ScalpSessionState;
    market: ScalpMarketSnapshot;
    nowMs: number;
    cfg: ScalpStrategyConfig;
}) {
    const reasonCodes: string[] = [];
    let next = {
        ...params.state,
        lastProcessed: { ...params.state.lastProcessed },
    };
    const baseTs = latestTs(params.market.baseCandles);
    const confirmTs = latestTs(params.market.confirmCandles);
    if (params.market.baseTf === 'M1') next.lastProcessed.m1ClosedTsMs = baseTs;
    if (params.market.baseTf === 'M3') next.lastProcessed.m3ClosedTsMs = baseTs;
    if (params.market.baseTf === 'M5') next.lastProcessed.m5ClosedTsMs = baseTs;
    if (params.market.baseTf === 'M15') next.lastProcessed.m15ClosedTsMs = baseTs;
    if (params.market.confirmTf === 'M1') next.lastProcessed.m1ClosedTsMs = confirmTs;
    if (params.market.confirmTf === 'M3') next.lastProcessed.m3ClosedTsMs = confirmTs;

    const windows = buildScalpSessionWindows({
        dayKey: next.dayKey,
        clockMode: params.cfg.sessions.clockMode,
        asiaWindowLocal: params.cfg.sessions.asiaWindowLocal,
        raidWindowLocal: params.cfg.sessions.raidWindowLocal,
    });

    if (!next.asiaRange) {
        const asia = buildAsiaRangeSnapshot({
            nowMs: params.nowMs,
            windows,
            candles: params.market.baseCandles,
            minCandles: params.cfg.data.minAsiaCandles,
            sourceTf: params.market.baseTf,
        });
        reasonCodes.push(...asia.reasonCodes);
        if (!asia.snapshot) return { state: next, reasonCodes: dedupeReasonCodes(reasonCodes) };
        next.asiaRange = asia.snapshot;
        if (next.state === 'IDLE') next.state = 'ASIA_RANGE_READY';
    }

    if (!next.sweep && params.nowMs > windows.raidEndMs && next.state === 'ASIA_RANGE_READY') {
        next.state = 'DONE';
        reasonCodes.push('RAID_WINDOW_CLOSED_NO_SWEEP');
        return { state: next, reasonCodes: dedupeReasonCodes(reasonCodes) };
    }

    if (next.state === 'ASIA_RANGE_READY' || next.state === 'SWEEP_DETECTED') {
        const atrBase = computeAtr(params.market.baseCandles, params.cfg.data.atrPeriod);
        const sweep = detectSweepLifecycle({
            existingSweep: next.sweep,
            candles: params.market.baseCandles,
            windows,
            nowMs: params.nowMs,
            asiaHigh: next.asiaRange.high,
            asiaLow: next.asiaRange.low,
            atrAbs: atrBase,
            spreadAbs: params.market.quote.spreadAbs,
            pipSize: pipSizeForScalpSymbol(next.symbol),
            cfg: params.cfg.sweep,
        });
        reasonCodes.push(...sweep.reasonCodes);
        if (sweep.sweep) next.sweep = sweep.sweep;
        if (sweep.status === 'pending') {
            next.state = 'SWEEP_DETECTED';
            return { state: next, reasonCodes: dedupeReasonCodes(reasonCodes) };
        }
        if (sweep.status === 'expired') {
            next.state = 'DONE';
            return { state: next, reasonCodes: dedupeReasonCodes(reasonCodes) };
        }
        if (sweep.status === 'none') return { state: next, reasonCodes: dedupeReasonCodes(reasonCodes) };
        next.state = 'CONFIRMING';
    }

    if (next.state === 'CONFIRMING') {
        const rejectionTsMs = Number(next.sweep?.rejectedTsMs);
        const direction = next.sweep?.side === 'BUY_SIDE' ? 'BEARISH' : next.sweep?.side === 'SELL_SIDE' ? 'BULLISH' : null;
        if (!(Number.isFinite(rejectionTsMs) && rejectionTsMs > 0 && direction)) {
            next.state = 'DONE';
            reasonCodes.push('CONFIRM_REQUIRES_REJECTED_SWEEP');
            return { state: next, reasonCodes: dedupeReasonCodes(reasonCodes) };
        }
        const confirmation = detectConfirmation({
            candles: params.market.confirmCandles,
            nowMs: params.nowMs,
            rejectionTsMs,
            pipSize: pipSizeForScalpSymbol(next.symbol),
            atrPeriod: params.cfg.data.atrPeriod,
            direction,
            cfg: params.cfg.confirm,
        });
        next.confirmation = confirmation.snapshot;
        reasonCodes.push(...confirmation.reasonCodes);
        if (confirmation.status === 'pending') return { state: next, reasonCodes: dedupeReasonCodes(reasonCodes) };
        if (confirmation.status === 'expired') {
            next.state = 'DONE';
            return { state: next, reasonCodes: dedupeReasonCodes(reasonCodes) };
        }
        const ifvg = detectIfvg({
            candles: params.market.confirmCandles,
            direction,
            displacementTsMs: confirmation.displacementTsMs!,
            structureShiftTsMs: confirmation.structureShiftTsMs!,
            nowMs: params.nowMs,
            atrPeriod: params.cfg.data.atrPeriod,
            cfg: params.cfg.ifvg,
        });
        reasonCodes.push(...ifvg.reasonCodes);
        if (!ifvg.zone) return { state: next, reasonCodes: dedupeReasonCodes(reasonCodes) };
        next.ifvg = ifvg.zone;
        next.state = 'WAITING_RETRACE';
    }

    if (next.state === 'WAITING_RETRACE' && next.ifvg) {
        const touch = detectIfvgTouch({
            candles: params.market.confirmCandles,
            ifvg: next.ifvg,
            nowMs: params.nowMs,
        });
        reasonCodes.push(...touch.reasonCodes);
        if (touch.touched) {
            next.ifvg = { ...next.ifvg, touched: true };
            reasonCodes.push('ENTRY_SIGNAL_READY');
            return { state: next, reasonCodes: dedupeReasonCodes(reasonCodes) };
        }
        if (touch.expired) {
            next.state = 'DONE';
            return { state: next, reasonCodes: dedupeReasonCodes(reasonCodes) };
        }
    }

    return { state: next, reasonCodes: dedupeReasonCodes(reasonCodes) };
}

function summarize(params: {
    symbol: string;
    runs: number;
    trades: ScalpReplayTrade[];
    startTs: number | null;
    endTs: number | null;
}): ScalpReplaySummary {
    const trades = params.trades;
    const wins = trades.filter((t) => t.rMultiple > 0).length;
    const losses = trades.filter((t) => t.rMultiple <= 0).length;
    const netR = trades.reduce((acc, t) => acc + t.rMultiple, 0);
    const avgR = trades.length ? netR / trades.length : 0;
    const expectancyR = avgR;
    const netPnlUsd = trades.reduce((acc, t) => acc + t.pnlUsd, 0);
    const avgHoldMinutes = trades.length ? trades.reduce((acc, t) => acc + t.holdMinutes, 0) / trades.length : 0;
    const exitsByReason: Record<string, number> = {};
    let equityR = 0;
    let peakR = 0;
    let maxDd = 0;
    for (const t of trades) {
        exitsByReason[t.exitReason] = (exitsByReason[t.exitReason] || 0) + 1;
        equityR += t.rMultiple;
        peakR = Math.max(peakR, equityR);
        maxDd = Math.max(maxDd, peakR - equityR);
    }

    return {
        symbol: params.symbol,
        startTs: params.startTs,
        endTs: params.endTs,
        runs: params.runs,
        trades: trades.length,
        wins,
        losses,
        winRatePct: trades.length ? (wins / trades.length) * 100 : 0,
        avgR,
        expectancyR,
        netR,
        netPnlUsd,
        maxDrawdownR: maxDd,
        avgHoldMinutes,
        exitsByReason,
    };
}

export function runScalpReplay(params: {
    candles: ScalpReplayCandle[];
    pipSize: number;
    config: ScalpReplayRuntimeConfig;
}): ScalpReplayResult {
    const candles = params.candles.slice().sort((a, b) => a.ts - b.ts);
    if (!candles.length) throw new Error('Replay requires candles');
    const runtime = params.config;
    const strategyCfg = buildStrategyConfig(runtime);
    const intervalMs = Math.max(1, Math.floor(runtime.executeMinutes)) * 60_000;
    const slippageAbs = runtime.slippagePips * params.pipSize;
    const timeline: ScalpReplayTimelineEvent[] = [];
    const trades: ScalpReplayTrade[] = [];
    let runs = 0;
    let state: ScalpSessionState | null = null;
    let position: ReplayPosition | null = null;
    let nextRunTs = candles[0]!.ts;

    for (let i = 0; i < candles.length; i += 1) {
        const candle = candles[i]!;
        if (position && i >= position.activeFromIndex) {
            const exit = resolveExitFromCandle({
                position,
                candle,
                slippageAbs,
                preferStopWhenBothHit: runtime.preferStopWhenBothHit,
            });
            if (exit?.hit) {
                const trade = closePositionAsTrade({
                    position,
                    exitTs: candle.ts,
                    exitPrice: exit.exitPrice,
                    exitReason: exit.reason,
                });
                trades.push(trade);
                appendTimeline(timeline, {
                    ts: candle.ts,
                    type: 'exit',
                    state: state?.state,
                    reasonCodes: [exit.reason === 'TP' ? 'EXIT_TP' : 'EXIT_STOP'],
                    payload: { tradeId: trade.id, r: trade.rMultiple, pnlUsd: trade.pnlUsd },
                });
                position = null;
                if (state) {
                    state.trade = null;
                    state.state = 'DONE';
                }
            }
        }

        if (candle.ts < nextRunTs) continue;
        while (nextRunTs <= candle.ts) {
            runs += 1;
            const nowMs = nextRunTs;
            const dayKey = deriveScalpDayKey(nowMs, strategyCfg.sessions.clockMode);
            if (!state) {
                state = createInitialScalpSessionState({
                    symbol: runtime.symbol,
                    dayKey,
                    nowMs,
                    killSwitchActive: false,
                });
            }
            const transitioned = advanceScalpStateMachine(state, { nowMs, dayKey });
            state = transitioned.nextState;
            const market = buildReplayMarketSnapshot({
                symbol: runtime.symbol,
                nowMs,
                sourceCandles: candles,
                pipSize: params.pipSize,
                runtime,
            });
            const phase = applyPhaseDetectors({
                state,
                market,
                nowMs,
                cfg: strategyCfg,
            });
            state = phase.state;

            const runReasons = dedupeReasonCodes(['SCALP_REPLAY_RUN', ...transitioned.reasonCodes, ...phase.reasonCodes]);
            appendTimeline(timeline, {
                ts: nowMs,
                type: 'state',
                state: state.state,
                reasonCodes: runReasons,
                payload:
                    state.ifvg && (runReasons.includes('IFVG_QUALIFIED') || runReasons.includes('IFVG_WAITING_RETRACE'))
                        ? {
                              ifvgDirection: state.ifvg.direction,
                              ifvgLow: state.ifvg.low,
                              ifvgHigh: state.ifvg.high,
                              ifvgCreatedTs: state.ifvg.createdTsMs,
                              ifvgTouched: state.ifvg.touched,
                          }
                        : undefined,
            });

            if (!position && state.state === 'WAITING_RETRACE' && state.ifvg?.touched && !state.trade) {
                const planRes = buildScalpEntryPlan({
                    state,
                    market,
                    cfg: strategyCfg,
                });
                const reasons = dedupeReasonCodes(planRes.reasonCodes);
                appendTimeline(timeline, {
                    ts: nowMs,
                    type: 'note',
                    state: state.state,
                    reasonCodes: reasons,
                });
                if (planRes.plan && state.stats.tradesPlaced < strategyCfg.risk.maxTradesPerSymbolPerDay) {
                    const adverse = planRes.plan.side === 'BUY' ? slippageAbs : -slippageAbs;
                    const fillPrice = planRes.plan.entryReferencePrice + adverse;
                    const riskAbs = Math.abs(fillPrice - planRes.plan.stopPrice);
                    if (riskAbs > 0) {
                        position = {
                            tradeId: planRes.plan.setupId,
                            dayKey: state.dayKey,
                            side: planRes.plan.side,
                            entryTs: nowMs,
                            entryPrice: fillPrice,
                            stopPrice: planRes.plan.stopPrice,
                            takeProfitPrice: planRes.plan.takeProfitPrice,
                            riskAbs,
                            riskUsd: planRes.plan.riskUsd,
                            notionalUsd: planRes.plan.notionalUsd,
                            activeFromIndex: i + 1,
                        };
                        state.trade = {
                            setupId: planRes.plan.setupId,
                            dealReference: planRes.plan.dealReference,
                            side: planRes.plan.side,
                            entryPrice: fillPrice,
                            stopPrice: planRes.plan.stopPrice,
                            takeProfitPrice: planRes.plan.takeProfitPrice,
                            riskR: 1,
                            openedAtMs: nowMs,
                            brokerOrderId: null,
                            dryRun: true,
                        };
                        state.stats = {
                            ...state.stats,
                            tradesPlaced: state.stats.tradesPlaced + 1,
                            lastTradeAtMs: nowMs,
                        };
                        state.state = 'IN_TRADE';
                        appendTimeline(timeline, {
                            ts: nowMs,
                            type: 'entry',
                            state: state.state,
                            reasonCodes: ['ENTRY_SIMULATED'],
                            payload: {
                                setupId: planRes.plan.setupId,
                                side: planRes.plan.side,
                                entry: fillPrice,
                                stop: planRes.plan.stopPrice,
                                tp: planRes.plan.takeProfitPrice,
                            },
                        });
                    }
                }
            }
            nextRunTs += intervalMs;
        }
    }

    if (position && runtime.forceCloseAtEnd) {
        const last = candles[candles.length - 1]!;
        const exitPrice = position.side === 'BUY' ? last.close - slippageAbs : last.close + slippageAbs;
        const trade = closePositionAsTrade({
            position,
            exitTs: last.ts,
            exitPrice,
            exitReason: 'FORCE_CLOSE',
        });
        trades.push(trade);
        appendTimeline(timeline, {
            ts: last.ts,
            type: 'exit',
            state: state?.state,
            reasonCodes: ['EXIT_FORCE_CLOSE'],
            payload: { tradeId: trade.id, r: trade.rMultiple, pnlUsd: trade.pnlUsd },
        });
        if (state) {
            state.trade = null;
            state.state = 'DONE';
        }
        position = null;
    }

    const summary = summarize({
        symbol: runtime.symbol,
        runs,
        trades,
        startTs: candles[0]?.ts ?? null,
        endTs: candles[candles.length - 1]?.ts ?? null,
    });

    return {
        config: runtime,
        summary,
        trades,
        timeline,
    };
}
