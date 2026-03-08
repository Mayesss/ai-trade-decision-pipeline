export const config = { runtime: 'nodejs' };

import type { NextApiRequest, NextApiResponse } from 'next';

import { requireAdminAccess } from '../../../../lib/admin';
import { canonicalizeScalpDeploymentRegistry } from '../../../../lib/scalp/deploymentRegistry';

function parseBoolParam(value: string | string[] | undefined, fallback: boolean): boolean {
    if (value === undefined) return fallback;
    const first = Array.isArray(value) ? value[0] : value;
    if (first === undefined) return fallback;
    const normalized = String(first).trim().toLowerCase();
    if (['false', '0', 'no', 'off'].includes(normalized)) return false;
    if (['true', '1', 'yes', 'on'].includes(normalized)) return true;
    return fallback;
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

    const dryRun = parseBoolParam(req.query.dryRun, true);

    try {
        const out = await canonicalizeScalpDeploymentRegistry({ dryRun });
        return res.status(200).json({
            ok: true,
            dryRun: out.dryRun,
            wrote: out.wrote,
            storeMode: out.storeMode,
            registryPath: out.registryPath,
            registryKvKey: out.registryKvKey,
            updatedAt: out.updatedAt,
            beforeCount: out.beforeCount,
            afterCount: out.afterCount,
            dedupedCount: out.dedupedCount,
            legacyStrategyRows: out.legacyStrategyRows,
            legacyDeploymentIdRows: out.legacyDeploymentIdRows,
            deployments: out.snapshot.deployments,
        });
    } catch (err: any) {
        return res.status(500).json({
            error: 'canonicalize_scalp_deployments_failed',
            message: err?.message || String(err),
        });
    }
}
