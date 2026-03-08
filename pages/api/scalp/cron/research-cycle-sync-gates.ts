export const config = { runtime: 'nodejs' };

import type { NextApiRequest, NextApiResponse } from 'next';

import { requireAdminAccess } from '../../../../lib/admin';
import { syncResearchCyclePromotionGates } from '../../../../lib/scalp/researchPromotion';

function parseBoolParam(value: string | string[] | undefined, fallback: boolean): boolean {
    if (value === undefined) return fallback;
    const first = Array.isArray(value) ? value[0] : value;
    if (first === undefined) return fallback;
    const normalized = String(first).trim().toLowerCase();
    if (['false', '0', 'no', 'off'].includes(normalized)) return false;
    if (['true', '1', 'yes', 'on'].includes(normalized)) return true;
    return fallback;
}

function firstQueryValue(value: string | string[] | undefined): string | undefined {
    if (typeof value === 'string') return value.trim() || undefined;
    if (Array.isArray(value) && value.length > 0) return String(value[0] || '').trim() || undefined;
    return undefined;
}

function parseSourceList(value: string | undefined): Array<'manual' | 'backtest' | 'matrix'> | undefined {
    if (!value) return undefined;
    const rows = value
        .split(',')
        .map((row) => String(row || '').trim().toLowerCase())
        .filter((row): row is 'manual' | 'backtest' | 'matrix' => row === 'manual' || row === 'backtest' || row === 'matrix');
    return rows.length ? Array.from(new Set(rows)) : undefined;
}

function parsePositiveInt(value: string | undefined): number | undefined {
    const n = Math.floor(Number(value));
    if (!Number.isFinite(n) || n <= 0) return undefined;
    return n;
}

function parseFiniteNumber(value: string | undefined): number | undefined {
    const n = Number(value);
    if (!Number.isFinite(n)) return undefined;
    return n;
}

function setNoStoreHeaders(res: NextApiResponse): void {
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

    const cycleId = firstQueryValue(req.query.cycleId);
    const dryRun = parseBoolParam(req.query.dryRun, false);
    const requireCompletedCycle = parseBoolParam(req.query.requireCompletedCycle, true);
    const sources = parseSourceList(firstQueryValue(req.query.sources));
    const weeklyRobustnessEnabled = parseBoolParam(req.query.weeklyRobustnessEnabled, true);
    const weeklyRobustnessRequireWinnerShortlist = parseBoolParam(req.query.weeklyRobustnessRequireWinnerShortlist, true);
    const weeklyRobustnessTopKPerSymbol = parsePositiveInt(firstQueryValue(req.query.weeklyRobustnessTopKPerSymbol));
    const weeklyRobustnessLookbackDays = parsePositiveInt(firstQueryValue(req.query.weeklyRobustnessLookbackDays));
    const weeklyRobustnessMinCandlesPerSlice = parsePositiveInt(firstQueryValue(req.query.weeklyRobustnessMinCandlesPerSlice));
    const weeklyRobustnessMinSlices = parsePositiveInt(firstQueryValue(req.query.weeklyRobustnessMinSlices));
    const weeklyRobustnessMinProfitablePct = parseFiniteNumber(firstQueryValue(req.query.weeklyRobustnessMinProfitablePct));
    const weeklyRobustnessMinMedianExpectancyR = parseFiniteNumber(firstQueryValue(req.query.weeklyRobustnessMinMedianExpectancyR));
    const weeklyRobustnessMaxTopWeekPnlConcentrationPct = parseFiniteNumber(
        firstQueryValue(req.query.weeklyRobustnessMaxTopWeekPnlConcentrationPct),
    );

    try {
        const out = await syncResearchCyclePromotionGates({
            cycleId,
            dryRun,
            requireCompletedCycle,
            sources,
            updatedBy: firstQueryValue(req.query.updatedBy) || 'cron:research-cycle-sync-gates',
            weeklyRobustnessEnabled,
            weeklyRobustnessRequireWinnerShortlist,
            weeklyRobustnessTopKPerSymbol,
            weeklyRobustnessLookbackDays,
            weeklyRobustnessMinCandlesPerSlice,
            weeklyRobustnessMinSlices,
            weeklyRobustnessMinProfitablePct,
            weeklyRobustnessMinMedianExpectancyR,
            weeklyRobustnessMaxTopWeekPnlConcentrationPct,
        });

        return res.status(200).json({
            ok: out.ok,
            cycleId: out.cycleId,
            cycleStatus: out.cycleStatus,
            reason: out.reason,
            dryRun: out.dryRun,
            requireCompletedCycle: out.requireCompletedCycle,
            weeklyPolicy: out.weeklyPolicy,
            deploymentsConsidered: out.deploymentsConsidered,
            deploymentsMatched: out.deploymentsMatched,
            deploymentsUpdated: out.deploymentsUpdated,
            candidates: out.candidates,
            rows: out.rows,
        });
    } catch (err: any) {
        return res.status(500).json({
            error: 'research_cycle_sync_gates_failed',
            message: err?.message || String(err),
        });
    }
}
