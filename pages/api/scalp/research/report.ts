export const config = { runtime: 'nodejs' };

import type { NextApiRequest, NextApiResponse } from 'next';

import { requireAdminAccess } from '../../../../lib/admin';
import {
    loadScalpResearchPortfolioReportSnapshot,
    refreshScalpResearchPortfolioReport,
} from '../../../../lib/scalp/researchReporting';

function firstQueryValue(value: string | string[] | undefined): string | undefined {
    if (typeof value === 'string') return value.trim() || undefined;
    if (Array.isArray(value) && value.length > 0) return String(value[0] || '').trim() || undefined;
    return undefined;
}

function parseBoolParam(value: string | string[] | undefined, fallback: boolean): boolean {
    if (value === undefined) return fallback;
    const first = Array.isArray(value) ? value[0] : value;
    if (first === undefined) return fallback;
    const normalized = String(first).trim().toLowerCase();
    if (['false', '0', 'no', 'off'].includes(normalized)) return false;
    if (['true', '1', 'yes', 'on'].includes(normalized)) return true;
    return fallback;
}

function parsePositiveInt(value: string | undefined): number | undefined {
    const n = Number(value);
    if (!Number.isFinite(n) || n <= 0) return undefined;
    return Math.floor(n);
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

    const refresh = parseBoolParam(req.query.refresh, false);
    const cycleId = firstQueryValue(req.query.cycleId);
    const tradeLimit = parsePositiveInt(firstQueryValue(req.query.tradeLimit));
    const monthlyMonths = parsePositiveInt(firstQueryValue(req.query.monthlyMonths));

    try {
        const snapshot = refresh
            ? await refreshScalpResearchPortfolioReport({
                  cycleId,
                  tradeLimit,
                  monthlyMonths,
                  persist: false,
              })
            : await loadScalpResearchPortfolioReportSnapshot();

        if (!snapshot) {
            return res.status(404).json({
                error: 'research_report_not_found',
                message: 'No stored research report snapshot was found. Run /api/scalp/cron/research-report first.',
            });
        }

        return res.status(200).json({
            ok: true,
            snapshot,
        });
    } catch (err: any) {
        return res.status(500).json({
            error: 'research_report_read_failed',
            message: err?.message || String(err),
        });
    }
}
