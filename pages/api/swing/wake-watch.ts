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
//   3. Failed-break triggers: shortly after each primary bar close, armed
//      break-trigger rows are checked against the last closed bar.
//   4. Venue-side closes: in_position AI threads whose symbol is flat on the
//      venue (TP/SL bracket fill, manual close, liquidation) fire analyze so
//      the close is reconciled/persisted within ~a minute instead of at the
//      next 15-min tick.
//
// Firing = invokeCronEndpoint (the scalp cron-chaining pattern): a short-
// timeout GET that kicks the analyze invocation and returns — the analyze run
// completes server-side. A fired KV marker (TTL 4 min) prevents consecutive
// watcher ticks from double-firing the same event while that run is in flight;
// the durable dedupe is the analyze handler itself (it claims the cooldown row
// with a lease and deletes it only once the decision is recorded / re-stamps
// the AI-look ref). Claimed rows are excluded from the band work list, so a
// run that dies mid-AI puts its wake back on the list when the lease expires
// instead of losing it until the next primary close.
import type { NextApiRequest, NextApiResponse } from 'next';

import { requireAdminAccess } from '../../../lib/admin';
import { bitgetFetch } from '../../../lib/bitget';
import {
    fetchCapitalCandlesByEpic,
    fetchCapitalMidPrice,
    fetchCapitalOpenPositionMarkers,
    resolveCapitalEpic,
} from '../../../lib/capital';
import { kvGetJson, kvSetJson } from '../../../lib/kv';
import { invokeCronEndpoint } from '../../../lib/scalp/cronChaining';
import {
    clearSwingBreakTrigger,
    listSwingAiCooldownsWithWakeBands,
    listSwingBreakTriggers,
    listSwingInPositionThreads,
} from '../../../lib/swing/pg';
import {
    breakTriggerFailed,
    emergencyMoveAtr,
    lastClosedBar,
    minutesSinceBarBoundary,
    timeframeToMs,
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

// Failed-break checks only make sense right after a primary bar close (the
// condition can't change mid-bar), so candle fetches are throttled to this
// window after each boundary instead of running every minute all day.
const FAILED_BREAK_POST_CLOSE_WINDOW_MIN = 10;

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

    const [bandRows, breakTriggerRows, inPositionThreads, bitgetPositionsRaw, capitalMarkers] = await Promise.all([
        listSwingAiCooldownsWithWakeBands().catch((err) => {
            console.warn('[wake-watch] cooldown list failed:', err);
            return [];
        }),
        listSwingBreakTriggers().catch((err) => {
            console.warn('[wake-watch] break-trigger list failed:', err);
            return [];
        }),
        listSwingInPositionThreads().catch((err) => {
            console.warn('[wake-watch] in-position thread list failed:', err);
            return [];
        }),
        bitgetFetch('GET', '/api/v2/mix/position/all-position', {
            productType: getTradeProductType() as string,
        }).catch((err: unknown) => {
            // null (not []) so close-detection can tell "venue unreachable"
            // apart from "venue says flat" — see step 4.
            console.warn('[wake-watch] bitget all-position failed:', err);
            return null;
        }),
        fetchCapitalOpenPositionMarkers(),
    ]);

    const bitgetFetchOk = Array.isArray(bitgetPositionsRaw);
    const capitalFetchOk = Array.isArray(capitalMarkers);
    const capitalMarkerRows = capitalMarkers ?? [];
    const bitgetPositions = (bitgetFetchOk ? (bitgetPositionsRaw as unknown[]) : [])
        .map((row: any) => ({
            symbol: String(row?.symbol || '').toUpperCase(),
            price: Number(row?.markPrice),
            size: Number(row?.total ?? row?.available),
        }))
        .filter((p) => p.symbol && Number.isFinite(p.size) && p.size > 0);
    const openBySymbol = new Set<string>([
        ...bitgetPositions.map((p) => `bitget:${p.symbol}`),
        ...capitalMarkerRows.filter((m) => m.epic).map((m) => `capital:${m.epic}`),
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
        ...capitalMarkerRows
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

    // 3) Failed-break triggers (swing.break_triggers, armed at entry on
    // breakout/breakdown-thesis trades): shortly after each primary bar close,
    // fetch the last CLOSED bar and fire the analyze route if it closed back
    // through the trigger — the analyze run re-detects the condition itself,
    // surfaces market.failed_break to the model and consumes the row. Rows
    // whose position meanwhile closed (bracket exits happen with no analyze
    // tick) are cleaned up here.
    let breakTriggersChecked = 0;
    for (const row of breakTriggerRows) {
        if (!openBySymbol.has(`${row.platform}:${row.symbol}`)) {
            await clearSwingBreakTrigger(row.platform, row.symbol).catch((err) =>
                console.warn(`[wake-watch] break-trigger cleanup failed for ${row.platform}:${row.symbol}:`, err),
            );
            continue;
        }
        const tfMs = timeframeToMs(row.timeFrame);
        if (!tfMs) continue;
        const sinceClose = minutesSinceBarBoundary(tfMs, Date.now());
        if (sinceClose === null || sinceClose > FAILED_BREAK_POST_CLOSE_WINDOW_MIN) continue;
        breakTriggersChecked++;
        let candles: unknown[] = [];
        try {
            if (row.platform === 'capital') {
                candles = await fetchCapitalCandlesByEpic(resolveCapitalEpic(row.symbol).epic, row.timeFrame, 20);
            } else {
                const cs = await bitgetFetch('GET', '/api/v2/mix/market/candles', {
                    symbol: row.symbol,
                    productType: getTradeProductType() as string,
                    granularity: row.timeFrame,
                    limit: 5,
                });
                candles = Array.isArray(cs) ? cs : [];
            }
        } catch (err) {
            console.warn(`[wake-watch] candle fetch failed for ${row.platform}:${row.symbol}:`, err);
            continue;
        }
        // Venue feed ordering isn't guaranteed here — sort ascending by bar ts
        // so lastClosedBar's from-the-end scan sees the newest bars last.
        const ascending = [...candles].sort((a: any, b: any) => Number(a?.[0]) - Number(b?.[0]));
        const bar = lastClosedBar(ascending, tfMs, Date.now());
        if (!bar || bar.closeTs <= row.entryAtMs) continue;
        if (breakTriggerFailed(row.side, row.triggerPrice, bar.close)) {
            await maybeFire(row.platform, row.symbol, 'failed_break');
        }
    }

    // 4) Venue-side closes (TP/SL bracket fills, manual closes, liquidations):
    // an in_position AI thread whose symbol is flat on the venue means the
    // position closed since the last analyze tick — an executed AI CLOSE ends
    // its thread in the same tick, so it never appears here. Fire analyze so
    // its existing close reconcile runs now (thread end, Capital close
    // persistence, overlay cache invalidation) instead of up to 15 minutes
    // later. Gated per venue on a SUCCESSFUL position fetch: a failed fetch
    // must not read as "every position closed at once". The fired analyze
    // re-checks broker reality itself, so a race against a mid-tick thread
    // update costs at most one redundant invocation (deduped by the KV marker).
    let closesDetected = 0;
    for (const row of inPositionThreads) {
        const platform = String(row.platform || '').toLowerCase();
        const symbol = String(row.symbol || '').toUpperCase();
        if (platform === 'capital') {
            if (!capitalFetchOk) continue;
            let epic = symbol;
            try {
                epic = String(resolveCapitalEpic(symbol).epic || symbol).toUpperCase();
            } catch {
                /* unresolvable → compare on the raw symbol */
            }
            if (openBySymbol.has(`capital:${epic}`)) continue;
        } else {
            if (!bitgetFetchOk) continue;
            if (openBySymbol.has(`${platform}:${symbol}`)) continue;
        }
        closesDetected++;
        await maybeFire(platform, symbol, 'position_closed');
    }

    return res.status(200).json({
        ok: true,
        bandsChecked,
        positionsChecked: positionMarkers.length,
        breakTriggersChecked,
        inPositionThreads: inPositionThreads.length,
        closesDetected,
        emergencyThresholdAtr: EMERGENCY_MOVE_ATR,
        fired,
    });
}
