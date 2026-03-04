import { getDefaultScalpStrategy, getScalpStrategyById, resolveScalpStrategyIdForSymbol } from '../strategies/registry';
import { applyXauusdGuardRiskDefaultsToReplayRuntime } from '../strategies/regimePullbackM15M3XauusdGuarded';
import { buildScalpEntryPlan, manageScalpOpenTrade, resolveLegacyIfvgEntryIntent } from '../execution';
import { pipSizeForScalpSymbol, timeframeMinutes } from '../marketData';
import { buildScalpSessionWindows } from '../sessions';
import { getScalpStrategyConfig } from '../config';
import { advanceScalpStateMachine, createInitialScalpSessionState, deriveScalpDayKey } from '../stateMachine';
import type { ScalpCandle, ScalpMarketSnapshot, ScalpSessionState, ScalpStrategyConfig } from '../types';
import type {
    ScalpReplayCandle,
    ScalpReplayInputFile,
    ScalpReplayProgressEvent,
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
    initialStopPrice: number;
    stopPrice: number;
    takeProfitPrice: number;
    riskAbs: number;
    riskUsd: number;
    notionalUsd: number;
};

type ScalpReplayProgressOptions = {
    everyRuns?: number;
    minIntervalMs?: number;
    onProgress?: (event: ScalpReplayProgressEvent) => void;
};

type ScalpReplayMarketDataSources = {
    baseCandles?: ScalpReplayCandle[];
    confirmCandles?: ScalpReplayCandle[];
    priceCandles?: ScalpReplayCandle[];
};

type PreparedReplaySeries = {
    driverCandles: ScalpReplayCandle[];
    baseCandlesAll: ScalpCandle[];
    confirmCandlesAll: ScalpCandle[];
    baseTfMs: number;
    confirmTfMs: number;
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
            dailyLossLimitR: runtime.strategy.dailyLossLimitR,
            consecutiveLossPauseThreshold: runtime.strategy.consecutiveLossPauseThreshold,
            consecutiveLossCooldownBars: runtime.strategy.consecutiveLossCooldownBars,
            killSwitch: false,
            riskPerTradePct: runtime.strategy.riskPerTradePct,
            referenceEquityUsd: runtime.strategy.referenceEquityUsd,
            minNotionalUsd: runtime.strategy.minNotionalUsd,
            maxNotionalUsd: runtime.strategy.maxNotionalUsd,
            takeProfitR: runtime.strategy.takeProfitR,
            stopBufferPips: runtime.strategy.stopBufferPips,
            stopBufferSpreadMult: runtime.strategy.stopBufferSpreadMult,
            breakEvenOffsetR: runtime.strategy.breakEvenOffsetR,
            tp1R: runtime.strategy.tp1R,
            tp1ClosePct: runtime.strategy.tp1ClosePct,
            trailStartR: runtime.strategy.trailStartR,
            trailAtrMult: runtime.strategy.trailAtrMult,
            timeStopBars: runtime.strategy.timeStopBars,
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
    const defaultStrategy = getDefaultScalpStrategy();
    const normalizedSymbol = symbol.toUpperCase();
    const base: ScalpReplayRuntimeConfig = {
        symbol: normalizedSymbol,
        strategyId: resolveScalpStrategyIdForSymbol({ symbol: normalizedSymbol, fallbackStrategyId: defaultStrategy.id }),
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
            asiaBaseTf: 'M15',
            confirmTf: 'M3',
            maxTradesPerDay: cfg.risk.maxTradesPerSymbolPerDay,
            riskPerTradePct: cfg.risk.riskPerTradePct,
            referenceEquityUsd: cfg.risk.referenceEquityUsd,
            minNotionalUsd: cfg.risk.minNotionalUsd,
            maxNotionalUsd: cfg.risk.maxNotionalUsd,
            takeProfitR: Math.min(cfg.risk.takeProfitR, 1.2),
            stopBufferPips: cfg.risk.stopBufferPips,
            stopBufferSpreadMult: cfg.risk.stopBufferSpreadMult,
            breakEvenOffsetR: cfg.risk.breakEvenOffsetR,
            tp1R: cfg.risk.tp1R,
            tp1ClosePct: cfg.risk.tp1ClosePct,
            trailStartR: cfg.risk.trailStartR,
            trailAtrMult: cfg.risk.trailAtrMult,
            timeStopBars: cfg.risk.timeStopBars,
            dailyLossLimitR: cfg.risk.dailyLossLimitR,
            consecutiveLossPauseThreshold: cfg.risk.consecutiveLossPauseThreshold,
            consecutiveLossCooldownBars: cfg.risk.consecutiveLossCooldownBars,
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
    return applyXauusdGuardRiskDefaultsToReplayRuntime(base);
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

function sortReplayCandles(candles: ScalpReplayCandle[]): ScalpReplayCandle[] {
    return candles.slice().sort((a, b) => a.ts - b.ts);
}

function prepareReplaySeries(params: {
    candles: ScalpReplayCandle[];
    runtime: ScalpReplayRuntimeConfig;
    marketData?: ScalpReplayMarketDataSources;
}): PreparedReplaySeries {
    const baseTfMinutes = timeframeMinutes(params.runtime.strategy.asiaBaseTf);
    const confirmTfMinutes = timeframeMinutes(params.runtime.strategy.confirmTf);
    const baseSource = Array.isArray(params.marketData?.baseCandles) && params.marketData!.baseCandles!.length
        ? sortReplayCandles(params.marketData!.baseCandles!)
        : params.candles;
    const confirmSource = Array.isArray(params.marketData?.confirmCandles) && params.marketData!.confirmCandles!.length
        ? sortReplayCandles(params.marketData!.confirmCandles!)
        : params.candles;
    const priceSource = Array.isArray(params.marketData?.priceCandles) && params.marketData!.priceCandles!.length
        ? sortReplayCandles(params.marketData!.priceCandles!)
        : params.candles;
    if (!priceSource.length) {
        throw new Error('Replay requires at least one driver candle');
    }

    return {
        driverCandles: priceSource,
        baseCandlesAll: aggregateCandles(baseSource, baseTfMinutes),
        confirmCandlesAll: aggregateCandles(confirmSource, confirmTfMinutes),
        baseTfMs: baseTfMinutes * 60_000,
        confirmTfMs: confirmTfMinutes * 60_000,
    };
}

function buildReplayMarketSnapshot(params: {
    symbol: string;
    nowMs: number;
    priceCandle: ScalpReplayCandle;
    baseCandles: ScalpCandle[];
    confirmCandles: ScalpCandle[];
    pipSize: number;
    runtime: ScalpReplayRuntimeConfig;
}): ScalpMarketSnapshot {
    const last = params.priceCandle;
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
        baseCandles: params.baseCandles,
        confirmCandles: params.confirmCandles,
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

function inferExitReasonFromManageCodes(codes: string[]): 'STOP' | 'TIME_STOP' {
    const normalized = dedupeReasonCodes(codes);
    if (normalized.includes('TRADE_EXIT_TIME_STOP')) return 'TIME_STOP';
    return 'STOP';
}

function closePositionAsTrade(params: {
    position: ReplayPosition;
    exitTs: number;
    exitPrice: number;
    exitReason: 'STOP' | 'TIME_STOP' | 'FORCE_CLOSE';
    totalTradeR: number;
    tradeBeforeExit: NonNullable<ScalpSessionState['trade']> | null;
}): ScalpReplayTrade {
    const holdMinutes = Math.max(0, (params.exitTs - params.position.entryTs) / 60_000);
    const rMultiple = Number.isFinite(params.totalTradeR) ? params.totalTradeR : 0;
    const pnlUsd = rMultiple * params.position.riskUsd;
    const realizedRBeforeFinalExit = params.tradeBeforeExit ? toFinite(params.tradeBeforeExit.realizedR, 0) : 0;
    const remainingSizePctAtExit = params.tradeBeforeExit ? toFinite(params.tradeBeforeExit.remainingSizePct, 1) : 1;
    const tp1Taken = Boolean(params.tradeBeforeExit?.tp1Done);
    const trailingActiveAtExit = Boolean(params.tradeBeforeExit?.trailActive);

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
        realizedRBeforeFinalExit,
        remainingSizePctAtExit,
        tp1Taken,
        trailingActiveAtExit,
    };
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
    const losses = trades.filter((t) => t.rMultiple < 0).length;
    const netR = trades.reduce((acc, t) => acc + t.rMultiple, 0);
    const grossProfitR = trades.reduce((acc, t) => acc + Math.max(0, t.rMultiple), 0);
    const grossLossAbsR = trades.reduce((acc, t) => acc + Math.max(0, -t.rMultiple), 0);
    const grossLossR = -grossLossAbsR;
    const profitFactor = grossLossAbsR > 0 ? grossProfitR / grossLossAbsR : null;
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
        grossProfitR,
        grossLossR,
        profitFactor,
        netPnlUsd,
        maxDrawdownR: maxDd,
        avgHoldMinutes,
        exitsByReason,
    };
}

export async function runScalpReplay(params: {
    candles: ScalpReplayCandle[];
    pipSize: number;
    config: ScalpReplayRuntimeConfig;
    progress?: ScalpReplayProgressOptions;
    marketData?: ScalpReplayMarketDataSources;
}): Promise<ScalpReplayResult> {
    const candles = params.candles.slice().sort((a, b) => a.ts - b.ts);
    if (!candles.length) throw new Error('Replay requires candles');
    const strategyDef = getScalpStrategyById(params.config.strategyId) || getDefaultScalpStrategy();
    const runtime: ScalpReplayRuntimeConfig = {
        ...params.config,
        strategyId: strategyDef.id,
    };
    const strategyCfg = buildStrategyConfig(runtime);
    const prepared = prepareReplaySeries({
        candles,
        runtime,
        marketData: params.marketData,
    });
    const driverCandles = prepared.driverCandles;
    const intervalMs = Math.max(1, Math.floor(runtime.executeMinutes)) * 60_000;
    const estimatedTotalRuns = Math.max(
        1,
        Math.floor((driverCandles[driverCandles.length - 1]!.ts - driverCandles[0]!.ts) / intervalMs) + 1,
    );
    const progressEveryRuns = Math.max(1, Math.floor(params.progress?.everyRuns ?? 2000));
    const progressMinIntervalMs = Math.max(0, Math.floor(params.progress?.minIntervalMs ?? 5000));
    const onProgress = params.progress?.onProgress;
    const replayStartedAtMs = Date.now();
    let lastProgressEmittedAtMs = replayStartedAtMs;
    let lastProgressRuns = 0;

    const emitProgress = (nowTs: number, force = false) => {
        if (!onProgress) return;
        const now = Date.now();
        if (force && runs === lastProgressRuns) return;
        const dueByRuns = runs - lastProgressRuns >= progressEveryRuns;
        const dueByTime = progressMinIntervalMs > 0 && now - lastProgressEmittedAtMs >= progressMinIntervalMs;
        if (!force && !dueByRuns && !dueByTime) return;

        onProgress({
            runs,
            estimatedTotalRuns,
            completedPct: Math.max(0, Math.min(100, (runs / estimatedTotalRuns) * 100)),
            trades: trades.length,
            nowTs,
            elapsedMs: now - replayStartedAtMs,
        });
        lastProgressRuns = runs;
        lastProgressEmittedAtMs = now;
    };

    const slippageAbs = runtime.slippagePips * params.pipSize;
    const timeline: ScalpReplayTimelineEvent[] = [];
    const trades: ScalpReplayTrade[] = [];
    let runs = 0;
    let state: ScalpSessionState | null = null;
    let position: ReplayPosition | null = null;
    let nextRunTs = driverCandles[0]!.ts;
    let priceCursor = 0;
    let baseCursor = 0;
    let confirmCursor = 0;
    const baseClosedCandles: ScalpCandle[] = [];
    const confirmClosedCandles: ScalpCandle[] = [];

    for (let i = 0; i < driverCandles.length; i += 1) {
        const candle = driverCandles[i]!;
        if (candle.ts < nextRunTs) continue;
        while (nextRunTs <= candle.ts) {
            runs += 1;
            const nowMs = nextRunTs;
            while (baseCursor < prepared.baseCandlesAll.length && prepared.baseCandlesAll[baseCursor]![0] + prepared.baseTfMs <= nowMs) {
                baseClosedCandles.push(prepared.baseCandlesAll[baseCursor]!);
                baseCursor += 1;
            }
            while (
                confirmCursor < prepared.confirmCandlesAll.length &&
                prepared.confirmCandlesAll[confirmCursor]![0] + prepared.confirmTfMs <= nowMs
            ) {
                confirmClosedCandles.push(prepared.confirmCandlesAll[confirmCursor]!);
                confirmCursor += 1;
            }
            while (priceCursor + 1 < driverCandles.length && driverCandles[priceCursor + 1]!.ts <= nowMs) {
                priceCursor += 1;
            }
            const priceCandle = driverCandles[priceCursor];
            if (!priceCandle) {
                throw new Error('No candle available at replay nowMs');
            }
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
                priceCandle,
                baseCandles: baseClosedCandles,
                confirmCandles: confirmClosedCandles,
                pipSize: params.pipSize,
                runtime,
            });
            const windows = buildScalpSessionWindows({
                dayKey: state.dayKey,
                clockMode: strategyCfg.sessions.clockMode,
                asiaWindowLocal: strategyCfg.sessions.asiaWindowLocal,
                raidWindowLocal: strategyCfg.sessions.raidWindowLocal,
            });
            const phase = strategyDef.applyPhaseDetectors({
                state,
                market,
                windows,
                nowMs,
                cfg: strategyCfg,
            });
            state = phase.state;
            const manageReasonCodes: string[] = [];
            if (position && !state.trade) {
                state.trade = {
                    setupId: position.tradeId,
                    dealReference: `replay:${position.tradeId}`,
                    side: position.side,
                    entryPrice: position.entryPrice,
                    stopPrice: position.stopPrice,
                    takeProfitPrice: position.takeProfitPrice,
                    riskR: 1,
                    riskAbs: position.riskAbs,
                    riskUsd: position.riskUsd,
                    notionalUsd: position.notionalUsd,
                    initialStopPrice: position.initialStopPrice,
                    remainingSizePct: 1,
                    realizedR: 0,
                    tp1Done: false,
                    tp1Price: null,
                    trailActive: false,
                    trailStopPrice: null,
                    favorableExtremePrice: position.entryPrice,
                    barsHeld: 0,
                    openedAtMs: position.entryTs,
                    brokerOrderId: null,
                    dryRun: true,
                };
                state.state = 'IN_TRADE';
                manageReasonCodes.push('REPLAY_TRADE_CONTEXT_RESTORED');
            }
            const hadOpenTradeAtStartOfManage = Boolean(position && state.trade);
            if (position && state.trade) {
                const priorRealizedR = toFinite(state.stats.realizedR, 0);
                const tradeBeforeManage: NonNullable<ScalpSessionState['trade']> = { ...state.trade };
                const managed = await manageScalpOpenTrade({
                    state,
                    market,
                    cfg: strategyCfg,
                    dryRun: true,
                    nowMs,
                });
                state = managed.state;
                manageReasonCodes.push(...managed.reasonCodes);

                const managedCodes = dedupeReasonCodes(managed.reasonCodes);
                const significantManagedCodes = managedCodes.filter((code) => code !== 'TRADE_MANAGE_ACTIVE');
                if (significantManagedCodes.length) {
                    appendTimeline(timeline, {
                        ts: nowMs,
                        type: 'note',
                        state: state.state,
                        reasonCodes: significantManagedCodes,
                    });
                }

                if (!state.trade) {
                    const exitReason = inferExitReasonFromManageCodes(managedCodes);
                    const totalTradeR = toFinite(state.stats.realizedR, 0) - priorRealizedR;
                    const exitPrice = toFinite(market.quote.price, tradeBeforeManage.stopPrice);
                    const trade = closePositionAsTrade({
                        position,
                        exitTs: nowMs,
                        exitPrice,
                        exitReason,
                        totalTradeR,
                        tradeBeforeExit: tradeBeforeManage,
                    });
                    trades.push(trade);
                    appendTimeline(timeline, {
                        ts: nowMs,
                        type: 'exit',
                        state: state.state,
                        reasonCodes: [...managedCodes, exitReason === 'TIME_STOP' ? 'EXIT_TIME_STOP' : 'EXIT_STOP'],
                        payload: { tradeId: trade.id, r: trade.rMultiple, pnlUsd: trade.pnlUsd },
                    });
                    position = null;
                } else {
                    position.stopPrice = toFinite(state.trade.stopPrice, position.stopPrice);
                }
            }
            const strategyEntryIntent = phase.entryIntent ?? null;
            const legacyEntryIntent = strategyEntryIntent ? null : resolveLegacyIfvgEntryIntent(state);
            const entryIntent = strategyEntryIntent || legacyEntryIntent;
            if (toFinite(state.stats.realizedR, 0) <= strategyCfg.risk.dailyLossLimitR) {
                state.state = 'DONE';
                manageReasonCodes.push('DAILY_LOSS_LIMIT_BLOCKED_NEW_ENTRY');
            }

            const runReasons = dedupeReasonCodes([
                'SCALP_REPLAY_RUN',
                ...transitioned.reasonCodes,
                ...phase.reasonCodes,
                ...manageReasonCodes,
                ...(legacyEntryIntent ? ['ENTRY_INTENT_LEGACY_FALLBACK'] : []),
            ]);
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

            if (!hadOpenTradeAtStartOfManage && !position && !state.trade && entryIntent && state.state !== 'DONE' && state.state !== 'COOLDOWN') {
                const planRes = buildScalpEntryPlan({
                    state,
                    market,
                    cfg: strategyCfg,
                    entryIntent,
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
                            initialStopPrice: planRes.plan.stopPrice,
                            stopPrice: planRes.plan.stopPrice,
                            takeProfitPrice: planRes.plan.takeProfitPrice,
                            riskAbs,
                            riskUsd: planRes.plan.riskUsd,
                            notionalUsd: planRes.plan.notionalUsd,
                        };
                        state.trade = {
                            setupId: planRes.plan.setupId,
                            dealReference: planRes.plan.dealReference,
                            side: planRes.plan.side,
                            entryPrice: fillPrice,
                            stopPrice: planRes.plan.stopPrice,
                            takeProfitPrice: planRes.plan.takeProfitPrice,
                            riskR: 1,
                            riskAbs,
                            riskUsd: planRes.plan.riskUsd,
                            notionalUsd: planRes.plan.notionalUsd,
                            initialStopPrice: planRes.plan.stopPrice,
                            remainingSizePct: 1,
                            realizedR: 0,
                            tp1Done: false,
                            tp1Price: null,
                            trailActive: false,
                            trailStopPrice: null,
                            favorableExtremePrice: fillPrice,
                            barsHeld: 0,
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
                                riskAbs,
                            },
                        });
                    }
                }
            }
            emitProgress(nowMs);
            nextRunTs += intervalMs;
        }
    }

    if (position && state?.trade && runtime.forceCloseAtEnd) {
        const last = driverCandles[driverCandles.length - 1]!;
        const exitPrice = position.side === 'BUY' ? last.close - slippageAbs : last.close + slippageAbs;
        const signedMove = position.side === 'BUY' ? exitPrice - position.entryPrice : position.entryPrice - exitPrice;
        const currentR = position.riskAbs > 0 ? signedMove / position.riskAbs : 0;
        const remainingSizePct = Math.max(0, Math.min(1, toFinite(state.trade.remainingSizePct, 1)));
        const realizedRBeforeFinalExit = toFinite(state.trade.realizedR, 0);
        const totalTradeR = realizedRBeforeFinalExit + remainingSizePct * currentR;
        const trade = closePositionAsTrade({
            position,
            exitTs: last.ts,
            exitPrice,
            exitReason: 'FORCE_CLOSE',
            totalTradeR,
            tradeBeforeExit: { ...state.trade },
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

    emitProgress(driverCandles[driverCandles.length - 1]!.ts, true);

    const summary = summarize({
        symbol: runtime.symbol,
        runs,
        trades,
        startTs: driverCandles[0]?.ts ?? null,
        endTs: driverCandles[driverCandles.length - 1]?.ts ?? null,
    });

    return {
        config: runtime,
        summary,
        trades,
        timeline,
    };
}
