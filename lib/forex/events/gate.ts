import type {
    ForexEventGateDecision,
    ForexRiskState,
    NormalizedForexEconomicEvent,
} from '../types';
import { getForexEventConfig } from './config';

const PRE_EVENT_BLOCK_MINUTES = 30;
const POST_EVENT_BLOCK_MINUTES = 15;

function normalizePair(pair: string): string {
    return String(pair || '')
        .trim()
        .toUpperCase();
}

export function pairCurrencies(pair: string): string[] {
    const normalized = normalizePair(pair).replace(/[^A-Z]/g, '');
    if (normalized.length < 6) return [];
    return [normalized.slice(0, 3), normalized.slice(3, 6)];
}

function resolveRiskState(value?: ForexRiskState | null): ForexRiskState {
    const normalized = String(value ?? '')
        .trim()
        .toLowerCase();
    if (normalized === 'normal' || normalized === 'elevated' || normalized === 'extreme') {
        return normalized;
    }
    // Safety default chosen in the plan.
    return 'elevated';
}

function timestampMs(isoTimestamp: string): number {
    const ts = Date.parse(isoTimestamp);
    return Number.isFinite(ts) ? ts : NaN;
}

function isBlockedImpact(impact: string, blockedImpacts: string[]): boolean {
    return blockedImpacts.includes(String(impact || '').toUpperCase());
}

export function isWithinEventWindow(eventIsoTimestamp: string, nowMs: number): boolean {
    const eventMs = timestampMs(eventIsoTimestamp);
    if (!Number.isFinite(eventMs)) return false;
    const startMs = eventMs - PRE_EVENT_BLOCK_MINUTES * 60_000;
    const endMs = eventMs + POST_EVENT_BLOCK_MINUTES * 60_000;
    return nowMs >= startMs && nowMs <= endMs;
}

export type PairEventMatch = {
    event: NormalizedForexEconomicEvent;
    activeWindow: boolean;
    msToEvent: number;
};

export function listPairEventMatches(params: {
    pair: string;
    events: NormalizedForexEconomicEvent[];
    nowMs?: number;
    blockedImpacts?: string[];
}): PairEventMatch[] {
    const pair = normalizePair(params.pair);
    const nowMs = Number.isFinite(params.nowMs as number) ? Number(params.nowMs) : Date.now();
    const configuredBlockedImpacts = (params.blockedImpacts ?? getForexEventConfig().blockImpacts).map((impact) =>
        impact.toUpperCase(),
    );

    const currencies = new Set(pairCurrencies(pair));
    if (!currencies.size) return [];

    return (params.events || [])
        .filter((event) => currencies.has(String(event.currency || '').toUpperCase()))
        .filter((event) => isBlockedImpact(event.impact, configuredBlockedImpacts))
        .map((event) => {
            const eventMs = timestampMs(event.timestamp_utc);
            return {
                event,
                activeWindow: isWithinEventWindow(event.timestamp_utc, nowMs),
                msToEvent: Number.isFinite(eventMs) ? eventMs - nowMs : Number.POSITIVE_INFINITY,
            };
        })
        .sort((a, b) => Math.abs(a.msToEvent) - Math.abs(b.msToEvent));
}

export function evaluateForexEventGate(params: {
    pair: string;
    events: NormalizedForexEconomicEvent[];
    staleData: boolean;
    riskState?: ForexRiskState | null;
    nowMs?: number;
    blockedImpacts?: string[];
}): ForexEventGateDecision {
    const pair = normalizePair(params.pair);
    const riskStateApplied = resolveRiskState(params.riskState);
    const reasonCodes: string[] = [];

    if (params.staleData) {
        if (riskStateApplied === 'normal') {
            reasonCodes.push('EVENT_DATA_STALE_ALLOW_NORMAL_RISK');
            return {
                pair,
                blockNewEntries: false,
                allowNewEntries: true,
                allowRiskReduction: true,
                staleData: true,
                reasonCodes,
                matchedEvents: [],
                riskStateApplied,
            };
        }

        reasonCodes.push('EVENT_DATA_STALE_BLOCK_NON_NORMAL_RISK');
        return {
            pair,
            blockNewEntries: true,
            allowNewEntries: false,
            allowRiskReduction: true,
            staleData: true,
            reasonCodes,
            matchedEvents: [],
            riskStateApplied,
        };
    }

    const matches = listPairEventMatches({
        pair,
        events: params.events,
        nowMs: params.nowMs,
        blockedImpacts: params.blockedImpacts,
    });

    const activeEvents = matches.filter((match) => match.activeWindow).map((match) => match.event);
    if (activeEvents.length) {
        reasonCodes.push('EVENT_WINDOW_ACTIVE_BLOCK');
        return {
            pair,
            blockNewEntries: true,
            allowNewEntries: false,
            allowRiskReduction: true,
            staleData: false,
            reasonCodes,
            matchedEvents: activeEvents,
            riskStateApplied,
        };
    }

    reasonCodes.push('EVENT_WINDOW_CLEAR');
    return {
        pair,
        blockNewEntries: false,
        allowNewEntries: true,
        allowRiskReduction: true,
        staleData: false,
        reasonCodes,
        matchedEvents: [],
        riskStateApplied,
    };
}
