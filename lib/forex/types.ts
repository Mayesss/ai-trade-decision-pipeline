export type ForexRiskState = 'normal' | 'elevated' | 'extreme';

export type ForexEventImpact = 'LOW' | 'MEDIUM' | 'HIGH' | 'UNKNOWN';

export type ForexEventSource = 'forexfactory';

export type ForexSessionTag = 'ASIA' | 'LONDON' | 'NEW_YORK' | 'OVERLAP' | 'DEAD_HOURS';
export type ForexRegime = 'trend_up' | 'trend_down' | 'range' | 'high_vol' | 'event_risk';
export type ForexPermission = 'long_only' | 'short_only' | 'both' | 'flat';
export type ForexModuleName = 'pullback' | 'breakout_retest' | 'range_fade' | 'none';
export type ForexSide = 'BUY' | 'SELL';
export type ForexExecutionAction = ForexSide | 'CLOSE' | 'REVERSE' | 'NONE';
export type ForexTrailingMode = 'none' | 'structure' | 'atr' | 'range_protective';
export type ForexManageDecisionType = 'CLOSE_FULL' | 'CLOSE_PARTIAL' | 'TIGHTEN_STOP' | 'HOLD';

export interface NormalizedForexEconomicEvent {
    id: string;
    timestamp_utc: string;
    currency: string;
    impact: ForexEventImpact;
    event_name: string;
    actual: string | number | null;
    forecast: string | number | null;
    previous: string | number | null;
    source: ForexEventSource;
}

export interface ForexEventSnapshot {
    source: ForexEventSource;
    fetchedAtMs: number;
    fromDate: string;
    toDate: string;
    events: NormalizedForexEconomicEvent[];
}

export interface ForexEventStoreMeta {
    lastFetchAttemptAtMs: number | null;
    lastSuccessAtMs: number | null;
    lastFailureAtMs: number | null;
    lastError: string | null;
    callCounterDay: string;
    callCounter: number;
}

export interface ForexEventState {
    snapshot: ForexEventSnapshot | null;
    meta: ForexEventStoreMeta;
    stale: boolean;
    staleMinutes: number;
    refreshMinutes: number;
}

export interface ForexEventGateDecision {
    pair: string;
    blockNewEntries: boolean;
    allowNewEntries: boolean;
    allowRiskReduction: boolean;
    staleData: boolean;
    reasonCodes: string[];
    matchedEvents: NormalizedForexEconomicEvent[];
    riskStateApplied: ForexRiskState;
    activeImpactLevels?: ForexEventImpact[];
}

export interface ForexPairMetrics {
    pair: string;
    epic: string | null;
    sessionTag: ForexSessionTag;
    price: number;
    spreadAbs: number;
    spreadPips: number;
    spreadToAtr1h: number;
    atr1h: number;
    atr4h: number;
    atr1hPercent: number;
    trendStrength: number;
    chopScore: number;
    shockFlag: boolean;
    timestampMs: number;
}

export interface ForexPairEligibility {
    pair: string;
    eligible: boolean;
    rank: number;
    score: number;
    reasons: string[];
    metrics: ForexPairMetrics;
}

export interface ForexScanSnapshot {
    generatedAtMs: number;
    staleEvents: boolean;
    pairs: ForexPairEligibility[];
}

export interface ForexRegimePacket {
    pair: string;
    generatedAtMs: number;
    regime: ForexRegime;
    permission: ForexPermission;
    allowed_modules: ForexModuleName[];
    risk_state: ForexRiskState;
    confidence: number;
    htf_context: {
        nearest_support: number | null;
        nearest_resistance: number | null;
        distance_to_support_atr1h: number | null;
        distance_to_resistance_atr1h: number | null;
    };
    notes_codes: string[];
}

export interface ForexPacketSnapshot {
    generatedAtMs: number;
    packets: ForexRegimePacket[];
}

export interface ForexRiskCheck {
    pair: string;
    allowEntry: boolean;
    allowRiskReduction: boolean;
    reasonCodes: string[];
    cooldownUntilMs: number | null;
}

export interface ForexModuleSignal {
    pair: string;
    module: Exclude<ForexModuleName, 'none'>;
    side: ForexSide;
    entryPrice: number;
    stopPrice: number;
    confidence: number;
    reasonCodes: string[];
    tp1Price?: number | null;
    tp2Price?: number | null;
    rangeLowerBoundary?: number | null;
    rangeUpperBoundary?: number | null;
}

export interface ForexPositionContext {
    pair: string;
    side: ForexSide;
    entryModule: Exclude<ForexModuleName, 'none'>;
    entryPrice: number;
    initialStopPrice: number;
    currentStopPrice: number;
    initialRiskPrice: number;
    partialTakenPct: number;
    trailingActive: boolean;
    trailingMode: ForexTrailingMode;
    tp1Price: number | null;
    tp2Price: number | null;
    rangeLowerBoundary?: number | null;
    rangeUpperBoundary?: number | null;
    openedAtMs: number;
    lastManagedAtMs: number;
    lastCloseAtMs: number | null;
    // Legacy/backward-compatible aliases, still accepted on load.
    module?: Exclude<ForexModuleName, 'none'>;
    stopPrice?: number;
    updatedAtMs?: number;
    entryNotionalUsd?: number | null;
    entryLeverage?: number | null;
    packet: ForexRegimePacket;
}

export interface ForexManageDecision {
    action: ForexManageDecisionType;
    closePct?: number;
    nextStopPrice?: number | null;
    reasonCodes: string[];
}

export interface ForexExecutionResultSummary {
    pair: string;
    attempted: boolean;
    placed: boolean;
    dryRun: boolean;
    action: ForexExecutionAction;
    module: ForexModuleName;
    reasonCodes: string[];
    orderId: string | null;
    clientOid: string | null;
    packet: ForexRegimePacket | null;
    managementAction?: ForexManageDecisionType | null;
    packetAgeMinutes?: number | null;
}

export type ForexJournalEntryType = 'scan' | 'regime' | 'execution' | 'risk' | 'event_refresh';

export interface ForexJournalEntry {
    id: string;
    timestampMs: number;
    type: ForexJournalEntryType;
    pair: string | null;
    level: 'info' | 'warn' | 'error';
    reasonCodes: string[];
    payload: Record<string, any>;
}
