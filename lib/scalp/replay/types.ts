import type { ScalpEntrySessionProfile } from '../types';
import type { ScalpStrategyConfigOverride } from '../config';

export interface ScalpReplayInputCandle {
    ts: number | string;
    open: number;
    high: number;
    low: number;
    close: number;
    volume?: number;
    spreadPips?: number;
}

export interface ScalpReplayInputFile {
    symbol: string;
    candles: ScalpReplayInputCandle[];
    pipSize?: number;
}

export interface ScalpReplayCandle {
    ts: number;
    open: number;
    high: number;
    low: number;
    close: number;
    volume: number;
    spreadPips: number;
}

export interface ScalpReplayRuntimeConfig {
    symbol: string;
    strategyId: string;
    tuneId: string;
    deploymentId: string;
    tuneLabel: string;
    configOverride?: ScalpStrategyConfigOverride | null;
    executeMinutes: number;
    defaultSpreadPips: number;
    spreadFactor: number;
    slippagePips: number;
    preferStopWhenBothHit: boolean;
    forceCloseAtEnd: boolean;
    strategy: {
        sessionClockMode: 'LONDON_TZ' | 'UTC_FIXED';
        entrySessionProfile: ScalpEntrySessionProfile;
        asiaWindowLocal: [string, string];
        raidWindowLocal: [string, string];
        blockedBerlinEntryHours: number[];
        asiaBaseTf: 'M1' | 'M3' | 'M5' | 'M15';
        confirmTf: 'M1' | 'M3';
        maxTradesPerDay: number;
        riskPerTradePct: number;
        referenceEquityUsd: number;
        minNotionalUsd: number;
        maxNotionalUsd: number;
        takeProfitR: number;
        stopBufferPips: number;
        stopBufferSpreadMult: number;
        breakEvenOffsetR: number;
        tp1R: number;
        tp1ClosePct: number;
        trailStartR: number;
        trailAtrMult: number;
        timeStopBars: number;
        dailyLossLimitR: number;
        consecutiveLossPauseThreshold: number;
        consecutiveLossCooldownBars: number;
        minStopDistancePips: number;
        sweepBufferPips: number;
        sweepBufferAtrMult: number;
        sweepBufferSpreadMult: number;
        sweepRejectInsidePips: number;
        sweepRejectMaxBars: number;
        sweepMinWickBodyRatio: number;
        displacementBodyAtrMult: number;
        displacementRangeAtrMult: number;
        displacementCloseInExtremePct: number;
        mssLookbackBars: number;
        mssBreakBufferPips: number;
        mssBreakBufferAtrMult: number;
        confirmTtlMinutes: number;
        allowPullbackSwingBreakTrigger: boolean;
        ifvgMinAtrMult: number;
        ifvgMaxAtrMult: number;
        ifvgTtlMinutes: number;
        ifvgEntryMode: 'first_touch' | 'midline_touch' | 'full_fill';
        atrPeriod: number;
        minAsiaCandles: number;
        minBaseCandles: number;
        minConfirmCandles: number;
    };
}

export interface ScalpReplayTrade {
    id: string;
    dayKey: string;
    side: 'BUY' | 'SELL';
    entryTs: number;
    exitTs: number;
    holdMinutes: number;
    entryPrice: number;
    stopPrice: number;
    takeProfitPrice: number;
    exitPrice: number;
    exitReason: 'STOP' | 'STOP_LOSS' | 'STOP_BE' | 'STOP_TRAIL' | 'TP' | 'TIME_STOP' | 'FORCE_CLOSE';
    riskAbs: number;
    riskUsd: number;
    notionalUsd: number;
    rMultiple: number;
    pnlUsd: number;
    realizedRBeforeFinalExit?: number;
    remainingSizePctAtExit?: number;
    tp1Taken?: boolean;
    trailingActiveAtExit?: boolean;
}

export interface ScalpReplayTimelineEvent {
    ts: number;
    type: 'state' | 'entry' | 'exit' | 'note';
    state?: string;
    reasonCodes: string[];
    payload?: Record<string, unknown>;
}

export interface ScalpReplaySummary {
    symbol: string;
    strategyId: string;
    tuneId: string;
    deploymentId: string;
    tuneLabel: string;
    startTs: number | null;
    endTs: number | null;
    runs: number;
    trades: number;
    wins: number;
    losses: number;
    winRatePct: number;
    avgR: number;
    expectancyR: number;
    netR: number;
    grossProfitR: number;
    grossLossR: number;
    profitFactor: number | null;
    netPnlUsd: number;
    maxDrawdownR: number;
    avgHoldMinutes: number;
    exitsByReason: Record<string, number>;
}

export interface ScalpBacktestLeaderboardEntry {
    symbol: string;
    strategyId: string;
    tuneId: string;
    deploymentId: string;
    tuneLabel: string;
    netR: number;
    profitFactor: number | null;
    maxDrawdownR: number;
    trades: number;
    winRatePct: number;
    avgHoldMinutes: number;
    expectancyR: number;
}

export interface ScalpReplayResult {
    config: ScalpReplayRuntimeConfig;
    summary: ScalpReplaySummary;
    trades: ScalpReplayTrade[];
    timeline: ScalpReplayTimelineEvent[];
}

export interface ScalpReplayProgressEvent {
    runs: number;
    estimatedTotalRuns: number;
    completedPct: number;
    trades: number;
    nowTs: number;
    elapsedMs: number;
}
