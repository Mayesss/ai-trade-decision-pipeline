export const config = { runtime: 'nodejs' };
// 1-minute wake-watcher (cron * * * * *): closes the gap between "price crossed
// a level the AI asked to be woken at" and "the next 15-min tick happens to
// notice" — worst-case wake latency drops to ~a minute. It does NO AI work:
//
//   1. Flat wake bands: every swing.ai_cooldowns row carrying a band is
//      compared against a live price (Bitget public ticker / Capital markets
//      quote). A crossing fires the normal analyze route for that symbol; the
//      analyze cooldown handler re-detects the crossing itself, sets
//      market.cooldown_wake (which bypasses the flat quality gates) and
//      consumes the row — the watcher adds no new decision semantics.
//   2. In-position emergency: for every open position (one all-position call
//      per venue), live price is compared against the last-AI-look reference
//      (price + primary ATR) stamped by analyze; a move ≥
//      SWING_INPOS_EMERGENCY_MOVE_ATR fires the analyze route early. The
//      exchange-side bracket remains the actual guard — this only gets the
//      model's eyes on a violent move sooner.
//
// Firing = invokeCronEndpoint (the scalp cron-chaining pattern): a short-
// timeout GET that kicks the analyze invocation and returns — the analyze run
// completes server-side. A fired KV marker (TTL 4 min) prevents consecutive
// watcher ticks from double-firing the same event while that run is in flight;
// the durable dedupe is the analyze handler itself (it consumes the cooldown
// row / re-stamps the AI-look ref, so the trigger condition disappears).
import type { NextApiRequest, NextApiResponse } from 'next';

import { requireAdminAccess } from '../../../lib/admin';
import { bitgetFetch } from '../../../lib/bitget';
import { fetchCapitalMidPrice, fetchCapitalOpenPositionMarkers } from '../../../lib/capital';
import { kvGetJson, kvSetJson } from '../../../lib/kv';
import { invokeCronEndpoint } from '../../../lib/scalp/cronChaining';
import { listSwingAiCooldownsWithWakeBands } from '../../../lib/swing/pg';
import {
    emergencyMoveAtr,
    wakeBandCrossed,
    wakeWatchFiredKey,
    wakeWatchRefKey,
    WAKE_WATCH_FIRED_TTL_SECONDS,
    type WakeWatchRef,
} from '../../../lib/swing/wakeWatch';
import { getTradeProductType } from '../../../lib/trading';

// Same knob the analyze route uses for its own off-boundary in-position look.
const EMERGENCY_MOVE_ATR = (() => {
    const n = Number(process.env.SWING_INPOS_EMERGENCY_MOVE_ATR);
    return Number.isFinite(n) && n > 0 ? n : 1.5;
})();

type FiredEntry = {
    platform: string;
    symbol: string;
    reason: string;
    invoked: boolean;
    error?: string | null;
};

async function fetchBitgetLastPrice(symbol: string): Promise<number | null> {
    try {
        const data = await bitgetFetch('GET', '/api/v2/mix/market/ticker', {
            symbol,
            productType: getTradeProductType() as string,
        });
        const t = Array.isArray(data) ? data[0] : data;
        const p = Number(t?.lastPr ?? t?.last ?? t?.close);
        return Number.isFinite(p) && p > 0 ? p : null;
    } catch (err) {
        console.warn(`[wake-watch] bitget ticker failed for ${symbol}:`, err);
        return null;
    }
}

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
    if (req.method !== 'GET') {
        return res.status(405).json({ error: 'Method Not Allowed', message: 'Use GET' });
    }
    if (!requireAdminAccess(req, res)) return;

    // Test seam: ?dryRun=1 forwards dryRun to the fired analyze calls so the
    // whole watcher path can be exercised without live orders. Note a dry-run
    // analyze deliberately skips cooldown-row consumption, so repeated dry
    // tests re-fire (only the KV marker dedupes them). Production crons omit it.
    const dryRunRaw = String(Array.isArray(req.query.dryRun) ? req.query.dryRun[0] : req.query.dryRun || '')
        .trim()
        .toLowerCase();
    const forwardDryRun = ['1', 'true', 'yes', 'on'].includes(dryRunRaw);

    const fired: FiredEntry[] = [];
    const maybeFire = async (platform: string, symbol: string, reason: string) => {
        const firedKey = wakeWatchFiredKey(platform, symbol);
        try {
            const already = await kvGetJson<{ ts: number }>(firedKey);
            if (already) return; // an analyze run for this event is (or just was) in flight
            await kvSetJson(firedKey, { ts: Date.now(), reason }, WAKE_WATCH_FIRED_TTL_SECONDS);
        } catch (err) {
            // KV down → fire anyway: a rare duplicate AI call beats a missed wake.
            console.warn(`[wake-watch] fired-marker failed for ${platform}:${symbol}:`, err);
        }
        const result = await invokeCronEndpoint(
            req,
            '/api/swing/analyze',
            { symbol, platform, decisionPolicy: 'balanced', ...(forwardDryRun ? { dryRun: true } : {}) },
            5_000,
        );
        // A timeout abort means the analyze invocation was kicked and keeps
        // running server-side — that counts as fired (scalp chaining pattern).
        const invoked = result.invoked || String(result.error || '').toLowerCase().includes('abort');
        fired.push({ platform, symbol, reason, invoked, error: invoked ? null : result.error });
        console.log(
            `[wake-watch] fired ${platform}:${symbol} (${reason}) invoked=${invoked}${invoked ? '' : ` error=${result.error}`}`,
        );
    };

    const [bandRows, bitgetPositionsRaw, capitalMarkers] = await Promise.all([
        listSwingAiCooldownsWithWakeBands().catch((err) => {
            console.warn('[wake-watch] cooldown list failed:', err);
            return [];
        }),
        bitgetFetch('GET', '/api/v2/mix/position/all-position', {
            productType: getTradeProductType() as string,
        }).catch((err: unknown) => {
            console.warn('[wake-watch] bitget all-position failed:', err);
            return [] as unknown[];
        }),
        fetchCapitalOpenPositionMarkers(),
    ]);

    const bitgetPositions = (Array.isArray(bitgetPositionsRaw) ? bitgetPositionsRaw : [])
        .map((row: any) => ({
            symbol: String(row?.symbol || '').toUpperCase(),
            price: Number(row?.markPrice),
            size: Number(row?.total ?? row?.available),
        }))
        .filter((p) => p.symbol && Number.isFinite(p.size) && p.size > 0);
    const openBySymbol = new Set<string>([
        ...bitgetPositions.map((p) => `bitget:${p.symbol}`),
        ...capitalMarkers.filter((m) => m.epic).map((m) => `capital:${m.epic}`),
    ]);

    // 1) Flat wake bands. A band row for a symbol that meanwhile has an open
    // position is stale (cooldowns are flat-only) — skip it; the in-position
    // path below owns that symbol.
    let bandsChecked = 0;
    for (const row of bandRows) {
        if (openBySymbol.has(`${row.platform}:${row.symbol}`)) continue;
        bandsChecked++;
        const price =
            row.platform === 'capital'
                ? await fetchCapitalMidPrice(row.symbol)
                : await fetchBitgetLastPrice(row.symbol);
        const crossed = wakeBandCrossed(price, row.wakeAbove, row.wakeBelow);
        if (crossed) await maybeFire(row.platform, row.symbol, `wake_band_${crossed}`);
    }

    // 2) In-position emergency moves vs the last-AI-look reference.
    const positionMarkers: Array<{ platform: string; symbol: string; price: number | null }> = [
        ...bitgetPositions.map((p) => ({
            platform: 'bitget',
            symbol: p.symbol,
            price: Number.isFinite(p.price) && p.price > 0 ? p.price : null,
        })),
        ...capitalMarkers
            .filter((m) => m.epic)
            .map((m) => ({ platform: 'capital', symbol: m.epic as string, price: m.mid })),
    ];
    for (const marker of positionMarkers) {
        const ref = await kvGetJson<WakeWatchRef>(wakeWatchRefKey(marker.platform, marker.symbol)).catch(
            () => null,
        );
        const moveAtr = emergencyMoveAtr(marker.price, ref);
        if (moveAtr != null && moveAtr >= EMERGENCY_MOVE_ATR) {
            await maybeFire(marker.platform, marker.symbol, `emergency_move_${moveAtr.toFixed(2)}atr`);
        }
    }

    return res.status(200).json({
        ok: true,
        bandsChecked,
        positionsChecked: positionMarkers.length,
        emergencyThresholdAtr: EMERGENCY_MOVE_ATR,
        fired,
    });
}
