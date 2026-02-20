export const config = { runtime: 'nodejs' };

import type { NextApiRequest, NextApiResponse } from 'next';

import { requireAdminAccess } from '../../../../lib/admin';
import { getForexEventConfig } from '../../../../lib/forex/events/config';
import { getForexEventsState } from '../../../../lib/forex/events/forexFactory';
import { evaluateForexEventGate, listPairEventMatches } from '../../../../lib/forex/events/gate';
import type { ForexRiskState } from '../../../../lib/forex/types';

function parseRiskState(value: string | string[] | undefined): ForexRiskState | undefined {
    const raw = (Array.isArray(value) ? value[0] : value)?.trim().toLowerCase();
    if (raw === 'normal' || raw === 'elevated' || raw === 'extreme') return raw;
    return undefined;
}

function parseIntParam(value: string | string[] | undefined, fallback: number) {
    const raw = Array.isArray(value) ? value[0] : value;
    const n = Number(raw);
    if (!Number.isFinite(n)) return fallback;
    return Math.floor(n);
}

function setNoStoreHeaders(res: NextApiResponse) {
    res.setHeader('Cache-Control', 'no-store, no-cache, must-revalidate, proxy-revalidate');
    res.setHeader('Pragma', 'no-cache');
    res.setHeader('Expires', '0');
}

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
    if (req.method !== 'GET') {
        return res.status(405).json({ error: 'Method Not Allowed', message: 'Use GET' });
    }
    if (!requireAdminAccess(req, res)) return;

    setNoStoreHeaders(res);

    const pair = String(req.query.pair || '')
        .trim()
        .toUpperCase();
    const riskState = parseRiskState(req.query.riskState as string | string[] | undefined);
    const limit = Math.min(300, Math.max(1, parseIntParam(req.query.limit as string | string[] | undefined, 80)));

    try {
        const config = getForexEventConfig();
        const nowMs = Date.now();
        const state = await getForexEventsState(nowMs);
        const events = state.snapshot?.events ?? [];
        const sortedEvents = events
            .slice()
            .sort((a, b) => Date.parse(a.timestamp_utc) - Date.parse(b.timestamp_utc))
            .slice(0, limit);

        const payload: Record<string, unknown> = {
            source: state.snapshot?.source ?? 'forexfactory',
            nowMs,
            config: {
                refreshMinutes: config.refreshMinutes,
                staleMinutes: config.staleMinutes,
                blockImpacts: config.blockImpacts,
                callWarnThreshold: config.callWarnThreshold,
            },
            freshness: {
                stale: state.stale,
                lastFetchAttemptAtMs: state.meta.lastFetchAttemptAtMs,
                lastSuccessAtMs: state.meta.lastSuccessAtMs,
                lastFailureAtMs: state.meta.lastFailureAtMs,
                lastError: state.meta.lastError,
            },
            callBudget: {
                day: state.meta.callCounterDay,
                used: state.meta.callCounter,
                softLimit: config.callWarnThreshold,
            },
            counts: {
                totalEvents: events.length,
                returnedEvents: sortedEvents.length,
            },
            events: sortedEvents,
        };

        if (pair) {
            const matches = listPairEventMatches({
                pair,
                events,
                nowMs,
                blockedImpacts: config.blockImpacts,
            });
            const gate = evaluateForexEventGate({
                pair,
                events,
                staleData: state.stale,
                riskState,
                nowMs,
                blockedImpacts: config.blockImpacts,
            });

            payload.pair = pair;
            payload.riskState = riskState ?? null;
            payload.gate = gate;
            payload.pairMatches = matches.slice(0, limit).map((match) => ({
                ...match.event,
                activeWindow: match.activeWindow,
                msToEvent: match.msToEvent,
            }));
        }

        return res.status(200).json(payload);
    } catch (err: any) {
        console.error('Error in /api/forex/dashboard/events:', err);
        return res.status(500).json({ error: err?.message || String(err) });
    }
}
