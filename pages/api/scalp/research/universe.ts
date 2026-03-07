export const config = { runtime: 'nodejs' };

import type { NextApiRequest, NextApiResponse } from 'next';

import { requireAdminAccess } from '../../../../lib/admin';
import { loadScalpSymbolUniverseSnapshot } from '../../../../lib/scalp/symbolDiscovery';

function setNoStoreHeaders(res: NextApiResponse): void {
    res.setHeader('Cache-Control', 'no-store, no-cache, must-revalidate, proxy-revalidate');
    res.setHeader('Pragma', 'no-cache');
    res.setHeader('Expires', '0');
}

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
    if (req.method !== 'GET') {
        return res.status(405).json({ error: 'method_not_allowed', message: 'Use GET' });
    }
    if (!requireAdminAccess(req, res)) return;
    setNoStoreHeaders(res);

    try {
        const snapshot = await loadScalpSymbolUniverseSnapshot();
        if (!snapshot) {
            return res.status(404).json({
                error: 'symbol_universe_not_found',
                message: 'No symbol discovery snapshot found yet.',
            });
        }

        return res.status(200).json({
            ok: true,
            selectedCount: snapshot.selectedSymbols.length,
            candidatesEvaluated: snapshot.candidatesEvaluated,
            generatedAtIso: snapshot.generatedAtIso,
            snapshot,
        });
    } catch (err: any) {
        return res.status(500).json({
            error: 'symbol_universe_read_failed',
            message: err?.message || String(err),
        });
    }
}
