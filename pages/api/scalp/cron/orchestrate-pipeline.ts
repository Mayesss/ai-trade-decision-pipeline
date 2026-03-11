export const config = { runtime: 'nodejs', maxDuration: 600 };

import type { NextApiRequest, NextApiResponse } from 'next';

import { requireAdminAccess } from '../../../../lib/admin';
import { runScalpPipelineOrchestrator } from '../../../../lib/scalp/orchestrator';

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

    try {
        const out = await runScalpPipelineOrchestrator({
            maxDurationMs: parsePositiveInt(firstQueryValue(req.query.maxDurationMs)),
            selfInvokeOnContinue: parseBoolParam(req.query.selfInvokeOnContinue, true),
            continueRun: parseBoolParam(req.query.continue, false),
            debug: parseBoolParam(req.query.debug, false),
        });
        const statusCode = out.status === 'blocked' ? 409 : out.status === 'error' ? 500 : 200;
        return res.status(statusCode).json(out);
    } catch (err: any) {
        return res.status(500).json({
            error: 'scalp_orchestrator_failed',
            message: err?.message || String(err),
        });
    }
}

