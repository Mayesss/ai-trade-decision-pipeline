import type { ForexSide } from '../types';

export type ReplayEventRisk = 'none' | 'medium' | 'high';

export interface ReplayQuote {
    ts: number;
    bid: number;
    ask: number;
    eventRisk?: ReplayEventRisk;
    forceCloseReasonCode?: string;
    shock?: boolean;
    rollover?: boolean;
    spreadMultiplier?: number;
    note?: string;
}

export interface ReplayEntrySignal {
    ts: number;
    side: ForexSide;
    stopPrice: number;
    takeProfitPrice?: number | null;
    notionalUsd?: number;
    label?: string;
}

export interface ReplayReentryConfig {
    lockMinutes: number;
    lockMinutesTimeStop: number;
    lockMinutesRegimeFlip: number;
    lockMinutesEventRisk: number;
    lockMinutesStopInvalidated?: number;
    lockMinutesStopInvalidatedStress?: number;
}

export interface ReplaySpreadStressConfig {
    transitionBufferMinutes: number;
    transitionMultiplier: number;
    rolloverMultiplier: number;
    mediumEventMultiplier: number;
    highEventMultiplier: number;
}

export interface ReplaySlippageConfig {
    seed: number;
    entryBaseBps: number;
    exitBaseBps: number;
    randomBps: number;
    shockBps: number;
    mediumEventBps: number;
    highEventBps: number;
}

export interface ReplayManagementConfig {
    partialAtR: number;
    partialClosePct: number;
    trailingDistanceR: number;
    enableTrailing: boolean;
}

export type ReplayRolloverForceCloseMode = 'close' | 'derisk';

export interface ReplayRolloverConfig {
    dailyFeeBps: number;
    rolloverHourUtc: number;
    entryBlockMinutes: number;
    forceCloseMinutes: number;
    forceCloseSpreadToAtr1hMin: number;
    forceCloseMode: ReplayRolloverForceCloseMode;
    deriskWinnerMfeRMin: number;
    deriskLoserCloseRMax: number;
    deriskPartialClosePct: number;
}

export interface ReplayRuntimeConfig {
    pair: string;
    startingEquityUsd: number;
    defaultNotionalUsd: number;
    atr1hAbs: number;
    executeMinutes: number;
    forceCloseOnHighEvent: boolean;
    reentry: ReplayReentryConfig;
    spreadStress: ReplaySpreadStressConfig;
    slippage: ReplaySlippageConfig;
    management: ReplayManagementConfig;
    rollover: ReplayRolloverConfig;
}

export type ReplayLedgerKind = 'ENTRY' | 'EXIT' | 'PARTIAL_EXIT' | 'ROLLOVER_FEE';

export interface ReplayLedgerRow {
    id: number;
    ts: number;
    kind: ReplayLedgerKind;
    side: ForexSide | null;
    price: number | null;
    units: number | null;
    notionalUsd: number | null;
    pnlUsd: number;
    feeUsd: number;
    reasonCodes: string[];
    positionUnitsAfter: number;
    equityUsdAfter: number;
}

export interface ReplayTimelineEvent {
    ts: number;
    type:
        | 'ENTRY_OPENED'
        | 'ENTRY_BLOCKED'
        | 'PARTIAL_TAKEN'
        | 'STOP_TIGHTENED'
        | 'POSITION_CLOSED'
        | 'ROLLOVER_FEE_APPLIED'
        | 'REENTRY_LOCK_UPDATED';
    reasonCodes: string[];
    details?: Record<string, unknown>;
}

export interface ReplayEquityPoint {
    ts: number;
    equityUsd: number;
    realizedPnlUsd: number;
    unrealizedPnlUsd: number;
}

export interface ReplaySummary {
    pair: string;
    startTs: number | null;
    endTs: number | null;
    startingEquityUsd: number;
    endingEquityUsd: number;
    realizedPnlUsd: number;
    rolloverFeesUsd: number;
    returnPct: number;
    closedLegs: number;
    winningLegs: number;
    winRatePct: number;
    maxDrawdownPct: number;
    finalPositionOpen: boolean;
}

export interface ReplayResult {
    summary: ReplaySummary;
    ledger: ReplayLedgerRow[];
    timeline: ReplayTimelineEvent[];
    equityCurve: ReplayEquityPoint[];
}

export interface ReplayInputFile {
    pair?: string;
    quotes: Array<{
        ts: number | string;
        bid: number;
        ask: number;
        eventRisk?: ReplayEventRisk;
        forceCloseReasonCode?: string;
        shock?: boolean;
        rollover?: boolean;
        spreadMultiplier?: number;
        note?: string;
    }>;
    entries?: Array<{
        ts: number | string;
        side: ForexSide;
        stopPrice: number;
        takeProfitPrice?: number | null;
        notionalUsd?: number;
        label?: string;
    }>;
}
