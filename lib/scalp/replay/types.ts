import type { ScalpCandle, ScalpEntrySessionProfile, ScalpSessionState } from '../types';
import type { ScalpStrategyConfigOverride } from '../config';

// Open-position bookkeeping inside the replay loop. Defined here (not in
// harness.ts) so the incremental checkpoint type can name it. Plain numbers
// + strings — fully JSON-serializable.
export interface ScalpReplayPosition {
    tradeId: string;
    dayKey: string;
    side: "BUY" | "SELL";
    entryTs: number;
    entryPrice: number;
    initialStopPrice: number;
    stopPrice: number;
    takeProfitPrice: number;
    riskAbs: number;
    riskUsd: number;
    notionalUsd: number;
}

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
        allowedSessionWindowSlots?: number[];
        sessionSlotMinutes?: number;
        allowedWeekdaysLocal?: number[];
        allowedUtcHours?: number[];
        entryBlockReasonCodes?: string[];
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
    earlyAborted?: boolean;
    // Snapshot of strategy + position state at the LAST candle processed,
    // plus the closed-candle tails the strategy needs as indicator lookback
    // when resuming. Produced by every real runScalpReplay call; optional
    // on the type so test fixtures and manually-constructed result literals
    // don't have to fabricate one.
    finalCheckpoint?: ScalpReplayCheckpoint;
}

// Resumable replay state. Pass as `initialCheckpoint` to runScalpReplay to
// continue a previous replay against new candles instead of starting fresh.
//
// `configHash` is stamped at write time; callers MUST compare it against
// the configHash of their current run before re-using a checkpoint. A
// mismatch (deployment DSL changed, classifier version bumped, etc.)
// invalidates the checkpoint — fall back to a full replay.
export interface ScalpReplayCheckpoint {
    version: 1;
    // Last candle timestamp processed (in ms). Used by callers to verify the
    // checkpoint's coverage and decide whether incremental is applicable.
    endTs: number;
    // The next scheduled run timestamp at the moment the previous replay
    // ended. Restoring this lets the resumed replay pick up the cadence
    // exactly where it left off.
    nextRunTs: number;
    // Strategy phase state (asia range, sweep, confirmation, ifvg, daily
    // stats, etc.). Already fully JSON-serializable by design.
    state: ScalpSessionState;
    // Open position carried into the next replay, or null when flat.
    position: ScalpReplayPosition | null;
    // Closed-candle tails the strategy reads for indicator lookbacks (ATR,
    // SMA, MSS, etc.). Capped at MAX_CHECKPOINT_CANDLE_TAIL on each side so
    // the JSONB payload stays bounded.
    baseClosedCandles: ScalpCandle[];
    confirmClosedCandles: ScalpCandle[];
    // SHA-256/16 of the stable-stringified strategy config that produced
    // the checkpoint. Caller compares against the config of the new replay
    // and discards the checkpoint if it differs.
    configHash: string;
}

export interface ScalpReplayProgressEvent {
    runs: number;
    estimatedTotalRuns: number;
    completedPct: number;
    trades: number;
    nowTs: number;
    elapsedMs: number;
}
