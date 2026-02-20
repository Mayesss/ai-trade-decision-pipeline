export const config = { runtime: 'nodejs' };

import type { NextApiRequest, NextApiResponse } from 'next';

import { requireAdminAccess } from '../../../../lib/admin';
import { fetchCapitalOpenPositionSnapshots } from '../../../../lib/capital';
import { evaluateForexEventGate } from '../../../../lib/forex/events/gate';
import { getForexEventsState } from '../../../../lib/forex/events/forexFactory';
import { loadForexPacketSnapshot, loadForexScanSnapshot, loadForexJournal } from '../../../../lib/forex/store';
import type { ForexJournalEntry } from '../../../../lib/forex/types';

function setNoStoreHeaders(res: NextApiResponse) {
    res.setHeader('Cache-Control', 'no-store, no-cache, must-revalidate, proxy-revalidate');
    res.setHeader('Pragma', 'no-cache');
    res.setHeader('Expires', '0');
}

function safeRecord(value: unknown): Record<string, any> {
    if (!value || typeof value !== 'object' || Array.isArray(value)) return {};
    return value as Record<string, any>;
}

function normalizeComparable(value: string): string {
    return String(value || '')
        .trim()
        .toUpperCase()
        .replace(/[^A-Z0-9]/g, '');
}

function findOpenPositionForPair(
    pair: string,
    openPositions: Array<{
        epic: string;
        dealId: string | null;
        side: 'long' | 'short' | null;
        entryPrice: number | null;
        leverage: number | null;
        size: number | null;
        pnlPct: number | null;
        updatedAtMs: number;
    }>,
) {
    const pairKey = normalizeComparable(pair);
    if (!pairKey) return null;

    const scored = openPositions
        .map((row) => {
            const epicKey = normalizeComparable(row.epic);
            if (!epicKey) return null;
            if (epicKey === pairKey) return { row, score: 3 };
            if (epicKey.includes(pairKey)) return { row, score: 2 };
            if (pairKey.includes(epicKey)) return { row, score: 1 };
            return null;
        })
        .filter((x): x is { row: (typeof openPositions)[number]; score: number } => x !== null)
        .sort((a, b) => b.score - a.score);

    return scored[0]?.row ?? null;
}

function buildLatestExecution(entry: ForexJournalEntry | null) {
    if (!entry) return null;
    const payload = safeRecord(entry.payload);
    const signal = safeRecord(payload.signal);
    const decision = safeRecord(payload.decision);
    const execution = safeRecord(payload.execution);

    const module = typeof signal.module === 'string' ? signal.module : null;
    const action =
        typeof signal.side === 'string'
            ? signal.side
            : typeof decision.action === 'string'
            ? decision.action
            : null;
    const placed = Boolean(execution.placed);
    const orderId = typeof execution.orderId === 'string' ? execution.orderId : null;
    const clientOid = typeof execution.clientOid === 'string' ? execution.clientOid : null;
    const dryRun = payload.dryRun === true;
    const hasExecutionObject = Boolean(payload.execution && typeof payload.execution === 'object' && !Array.isArray(payload.execution));
    const hasDecisionObject = Boolean(payload.decision && typeof payload.decision === 'object' && !Array.isArray(payload.decision));

    const attempted = Boolean(module || action || orderId || clientOid || hasExecutionObject || hasDecisionObject);
    const status = placed
        ? 'placed'
        : attempted
        ? dryRun
            ? 'dry_run_attempt'
            : 'attempted_not_placed'
        : payload.risk || payload.gate
        ? 'blocked_or_no_signal'
        : 'info';

    return {
        pair: entry.pair,
        timestampMs: entry.timestampMs,
        status,
        attempted,
        placed,
        dryRun,
        module,
        action,
        summary: typeof decision.summary === 'string' ? decision.summary : null,
        reason: typeof decision.reason === 'string' ? decision.reason : null,
        orderId,
        clientOid,
        reasonCodes: Array.isArray(entry.reasonCodes) ? entry.reasonCodes : [],
    };
}

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
    if (req.method !== 'GET') {
        return res.status(405).json({ error: 'Method Not Allowed', message: 'Use GET' });
    }
    if (!requireAdminAccess(req, res)) return;
    setNoStoreHeaders(res);

    try {
        const nowMs = Date.now();
        const [scan, packets, journal, eventState] = await Promise.all([
            loadForexScanSnapshot(),
            loadForexPacketSnapshot(),
            loadForexJournal(600),
            getForexEventsState(),
        ]);

        const packetByPair = new Map((packets?.packets || []).map((p) => [p.pair, p]));
        const events = eventState.snapshot?.events ?? [];
        const last24h = nowMs - 24 * 60 * 60 * 1000;
        const latestExecutionEntry = journal.find((entry) => entry.type === 'execution' && entry.pair) ?? null;
        const latestExecution = buildLatestExecution(latestExecutionEntry);
        let openPositions: Awaited<ReturnType<typeof fetchCapitalOpenPositionSnapshots>> = [];
        try {
            openPositions = await fetchCapitalOpenPositionSnapshots();
        } catch (err) {
            console.warn('Could not fetch open Capital positions for forex summary:', err);
            openPositions = [];
        }

        const data = (scan?.pairs || []).map((row) => {
            const packet = packetByPair.get(row.pair) ?? null;
            const gate = evaluateForexEventGate({
                pair: row.pair,
                events,
                staleData: eventState.stale,
                riskState: packet?.risk_state,
                nowMs,
            });

            const pairJournal = journal.filter((entry) => entry.pair === row.pair);
            const recentPairJournal = pairJournal.filter((entry) => entry.timestampMs >= last24h);
            const lastExec = pairJournal.find((entry) => entry.type === 'execution') ?? null;
            const openPosition = findOpenPositionForPair(row.pair, openPositions);

            return {
                pair: row.pair,
                eligible: row.eligible,
                rank: row.rank,
                score: row.score,
                reasons: row.reasons,
                metrics: row.metrics,
                packet,
                gate,
                journalCount24h: recentPairJournal.length,
                lastExecutionAtMs: lastExec?.timestampMs ?? null,
                lastExecutionReasonCodes: lastExec?.reasonCodes ?? [],
                latestExecution: buildLatestExecution(lastExec),
                openPosition: openPosition
                    ? {
                          isOpen: true,
                          epic: openPosition.epic,
                          dealId: openPosition.dealId,
                          side: openPosition.side,
                          entryPrice: openPosition.entryPrice,
                          leverage: openPosition.leverage,
                          size: openPosition.size,
                          pnlPct: openPosition.pnlPct,
                          updatedAtMs: openPosition.updatedAtMs,
                      }
                    : { isOpen: false },
            };
        });

        return res.status(200).json({
            mode: 'forex',
            generatedAtMs: nowMs,
            scanGeneratedAtMs: scan?.generatedAtMs ?? null,
            packetsGeneratedAtMs: packets?.generatedAtMs ?? null,
            staleEvents: eventState.stale,
            latestExecution,
            pairs: data,
        });
    } catch (err: any) {
        console.error('Error in /api/forex/dashboard/summary:', err);
        return res.status(500).json({ error: err?.message || String(err) });
    }
}
