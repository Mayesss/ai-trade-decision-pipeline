export type ScalpClockMode = 'LONDON_TZ' | 'UTC_FIXED';
export type ScalpBaseTimeframe = 'M1' | 'M3' | 'M5' | 'M15';
export type ScalpConfirmTimeframe = 'M1' | 'M3';
export type ScalpFvgEntryMode = 'first_touch' | 'midline_touch' | 'full_fill';
export type ScalpDirectionalBias = 'BULLISH' | 'BEARISH';
export type ScalpCandle = [number, number, number, number, number, number];

export type ScalpState =
    | 'IDLE'
    | 'ASIA_RANGE_READY'
    | 'SWEEP_DETECTED'
    | 'CONFIRMING'
    | 'WAITING_RETRACE'
    | 'IN_TRADE'
    | 'DONE'
    | 'COOLDOWN';

export interface ScalpAsiaRangeSnapshot {
    timezone: 'Europe/London' | 'UTC';
    sourceTf: ScalpBaseTimeframe;
    startUtcIso: string;
    endUtcIso: string;
    high: number;
    low: number;
    candleCount: number;
    builtAtMs: number;
}

export interface ScalpSweepSnapshot {
    side: 'BUY_SIDE' | 'SELL_SIDE';
    sweepTsMs: number;
    sweepPrice: number;
    bufferAbs: number;
    rejected: boolean;
    rejectedTsMs: number | null;
    reasonCodes: string[];
}

export interface ScalpConfirmationSnapshot {
    displacementDetected: boolean;
    displacementTsMs: number | null;
    structureShiftDetected: boolean;
    structureShiftTsMs: number | null;
    reasonCodes: string[];
}

export interface ScalpIfvgZoneSnapshot {
    direction: ScalpDirectionalBias;
    low: number;
    high: number;
    createdTsMs: number;
    expiresAtMs: number;
    entryMode: ScalpFvgEntryMode;
    touched: boolean;
}

export interface ScalpTradeSnapshot {
    setupId: string;
    dealReference: string;
    side: 'BUY' | 'SELL';
    entryPrice: number;
    stopPrice: number;
    takeProfitPrice: number | null;
    riskR: number;
    openedAtMs: number;
    brokerOrderId: string | null;
    dryRun: boolean;
}

export interface ScalpTimeCursor {
    m1ClosedTsMs: number | null;
    m3ClosedTsMs: number | null;
    m5ClosedTsMs: number | null;
    m15ClosedTsMs: number | null;
}

export interface ScalpDailyStats {
    tradesPlaced: number;
    wins: number;
    losses: number;
    lastTradeAtMs: number | null;
}

export interface ScalpRunContext {
    lastRunAtMs: number | null;
    lastRunId: string | null;
    dryRunLast: boolean;
    lastReasonCodes: string[];
}

export interface ScalpSessionState {
    version: 1;
    symbol: string;
    dayKey: string;
    state: ScalpState;
    createdAtMs: number;
    updatedAtMs: number;
    cooldownUntilMs: number | null;
    killSwitchActive: boolean;
    asiaRange: ScalpAsiaRangeSnapshot | null;
    sweep: ScalpSweepSnapshot | null;
    confirmation: ScalpConfirmationSnapshot | null;
    ifvg: ScalpIfvgZoneSnapshot | null;
    trade: ScalpTradeSnapshot | null;
    lastProcessed: ScalpTimeCursor;
    stats: ScalpDailyStats;
    run: ScalpRunContext;
}

export type ScalpJournalEntryType = 'execution' | 'state' | 'risk' | 'error';

export interface ScalpJournalEntry {
    id: string;
    timestampMs: number;
    type: ScalpJournalEntryType;
    symbol: string | null;
    dayKey: string | null;
    level: 'info' | 'warn' | 'error';
    reasonCodes: string[];
    payload: Record<string, unknown>;
}

export interface ScalpStrategyConfig {
    enabled: boolean;
    defaultSymbol: string;
    dryRunDefault: boolean;
    cadence: {
        executeMinutes: number;
    };
    sessions: {
        clockMode: ScalpClockMode;
        asiaWindowLocal: [string, string];
        raidWindowLocal: [string, string];
    };
    timeframes: {
        asiaBase: ScalpBaseTimeframe;
        confirm: ScalpConfirmTimeframe;
    };
    sweep: {
        bufferPips: number;
        bufferAtrMult: number;
        bufferSpreadMult: number;
        rejectInsidePips: number;
        rejectMaxBars: number;
        minWickBodyRatio: number;
    };
    confirm: {
        displacementBodyAtrMult: number;
        displacementRangeAtrMult: number;
        closeInExtremePct: number;
        mssLookbackBars: number;
        mssBreakBufferPips: number;
        mssBreakBufferAtrMult: number;
        ttlMinutes: number;
    };
    ifvg: {
        minAtrMult: number;
        maxAtrMult: number;
        ttlMinutes: number;
        entryMode: ScalpFvgEntryMode;
    };
    risk: {
        cooldownAfterLossMinutes: number;
        maxTradesPerSymbolPerDay: number;
        maxOpenPositionsPerSymbol: number;
        killSwitch: boolean;
        riskPerTradePct: number;
        referenceEquityUsd: number;
        minNotionalUsd: number;
        maxNotionalUsd: number;
        takeProfitR: number;
        stopBufferPips: number;
        stopBufferSpreadMult: number;
        minStopDistancePips: number;
    };
    execution: {
        liveEnabled: boolean;
        entryOrderType: 'MARKET' | 'LIMIT';
        defaultLeverage: number;
    };
    idempotency: {
        runLockSeconds: number;
    };
    storage: {
        sessionTtlSeconds: number;
        journalMax: number;
    };
    data: {
        atrPeriod: number;
        minAsiaCandles: number;
        minBaseCandles: number;
        minConfirmCandles: number;
        maxCandlesPerRequest: number;
    };
}

export interface ScalpStateMachineInput {
    nowMs: number;
    dayKey: string;
}

export interface ScalpStateMachineResult {
    nextState: ScalpSessionState;
    transitioned: boolean;
    reasonCodes: string[];
}

export interface ScalpSessionWindows {
    timezone: 'Europe/London' | 'UTC';
    asiaStartMs: number;
    asiaEndMs: number;
    raidStartMs: number;
    raidEndMs: number;
    asiaStartUtcIso: string;
    asiaEndUtcIso: string;
    raidStartUtcIso: string;
    raidEndUtcIso: string;
}

export interface ScalpLiveQuote {
    price: number;
    bid: number | null;
    offer: number | null;
    spreadAbs: number;
    spreadPips: number;
    tsMs: number;
}

export interface ScalpMarketSnapshot {
    symbol: string;
    epic: string;
    nowMs: number;
    quote: ScalpLiveQuote;
    baseTf: ScalpBaseTimeframe;
    confirmTf: ScalpConfirmTimeframe;
    baseCandles: ScalpCandle[];
    confirmCandles: ScalpCandle[];
}

export interface ScalpExecuteCycleResult {
    generatedAtMs: number;
    symbol: string;
    dayKey: string;
    dryRun: boolean;
    runLockAcquired: boolean;
    state: ScalpState;
    reasonCodes: string[];
}

export interface ScalpEntryPlan {
    setupId: string;
    dealReference: string;
    side: 'BUY' | 'SELL';
    orderType: 'MARKET' | 'LIMIT';
    limitLevel: number | null;
    entryReferencePrice: number;
    stopPrice: number;
    takeProfitPrice: number;
    riskAbs: number;
    riskUsd: number;
    notionalUsd: number;
    leverage: number;
}
