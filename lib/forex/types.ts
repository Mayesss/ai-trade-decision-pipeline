export type ForexRiskState = 'normal' | 'elevated' | 'extreme';

export type ForexEventImpact = 'LOW' | 'MEDIUM' | 'HIGH' | 'UNKNOWN';

export type ForexEventSource = 'fmp';

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
}
