export const config = { runtime: 'nodejs' };

import type { NextApiRequest, NextApiResponse } from 'next';

import { requireAdminAccess } from '../../../../lib/admin';
import { loadSwingCronControlState, setSwingCronControlState } from '../../../../lib/swing/cronControl';

function firstQueryValue(value: string | string[] | undefined): string | undefined {
    if (typeof value === 'string') return value.trim() || undefined;
    if (Array.isArray(value) && value.length > 0) return String(value[0] || '').trim() || undefined;
    return undefined;
}

function parseBool(value: string | string[] | undefined, fallback: boolean): boolean {
    const first = firstQueryValue(value);
    if (!first) return fallback;
    const normalized = first.toLowerCase();
    if (['1', 'true', 'yes', 'on'].includes(normalized)) return true;
    if (['0', 'false', 'no', 'off'].includes(normalized)) return false;
    return fallback;
}

function setNoStoreHeaders(res: NextApiResponse): void {
    res.setHeader('Cache-Control', 'no-store, no-cache, must-revalidate, proxy-revalidate');
    res.setHeader('Pragma', 'no-cache');
    res.setHeader('Expires', '0');
}

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
    if (!requireAdminAccess(req, res)) return;
    setNoStoreHeaders(res);

    if (req.method === 'GET') {
        const state = await loadSwingCronControlState();
        return res.status(200).json({ ok: true, cronControl: state });
    }

    if (req.method === 'POST') {
        const hardDeactivated = parseBool(req.query.hardDeactivated, true);
        const reason = firstQueryValue(req.query.reason) || null;
        const updatedBy = firstQueryValue(req.query.updatedBy) || 'ui:swing-cron-control';
        const state = await setSwingCronControlState({
            hardDeactivated,
            reason,
            updatedBy,
        });
        return res.status(200).json({ ok: true, cronControl: state });
    }

    return res.status(405).json({ error: 'Method Not Allowed', message: 'Use GET or POST' });
}

