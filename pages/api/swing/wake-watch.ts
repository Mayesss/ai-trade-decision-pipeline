// maxDuration: a firing run WAITS for its analyze invocations to complete
// (see maybeFire) — detection itself takes seconds, but a fire adds the full
// analyze runtime (~60-120s of AI latency, capped at 240s per fire).
export const config = { runtime: 'nodejs', maxDuration: 300 };
// Wake-watcher (cron */10, WAKE_WATCH_TICK_MINUTES): closes the gap between
// "price crossed a level the AI asked to be woken at" and the next scheduled
// look, which since 2026-10-06 is up to a day away — worst-case wake latency
// is one watcher tick. It was a one-minute cron until then; see
// docs/neon-compute-cost.md for why it is ten. It does NO AI work:
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
//   3. Failed-break triggers: in the first ticks after each primary bar
//      close, armed break-trigger rows are checked against the last closed bar.
//   4. Venue-side closes: in_position AI threads whose symbol is flat on the
//      venue (TP/SL bracket fill, manual close, liquidation) fire analyze so
//      the close is reconciled/persisted within a tick. This fire carries
//      enforcePrimaryCloseGate + postCloseReconcile: it wants the reconcile,
//      not a fresh look at a symbol that just went flat.
//   5. Resting entries (pending_entry threads) — upkeep the analyze cron did
//      on every 15-minute tick until the schedule went daily: a fill (venue
//      shows the position) or a vanished order (venue purge, manual cancel)
//      fires a reconcile-only analyze run; a Capital entry still resting when
//      a session decision window opens fires the run whose session gate
//      withdraws it, so nothing of ours fills into the open.
//   6. Owed session-window looks: a look the session gate deferred (marker
//      written by analyze) is fired once its window is over.
//
// Firing = invokeCronEndpoint held OPEN until analyze responds (240s cap).
// The old scalp kick-and-detach (5s abort, "the analyze run completes
// server-side") was a lie on Vercel: a function whose client disconnected is
// only kept alive by luck, and most detached wake runs were hard-killed
// mid-AI-call AFTER claiming the cooldown row — each death locked the wake
// behind the claim lease and pushed the real look 15-50 min out (BGBUSDT
// 2026-07-29). Detection stays fast: fires are collected and awaited together
// (Promise.all) just before the response, so one slow analyze never delays
// checking the other symbols. A fired KV marker (WAKE_WATCH_FIRED_TTL_SECONDS,
// under one tick) stops one event firing twice while its run is in flight;
// the durable dedupe is the analyze handler itself (it claims the cooldown
// row with a lease and deletes it only once the decision is recorded /
// re-stamps the AI-look ref). Claimed rows are excluded from the band work
// list, so a run that dies mid-AI puts its wake back on the list when the
// lease expires instead of losing it until the next scheduled look.
import type { NextApiRequest, NextApiResponse } from 'next';

import {
    IN_POSITION_EMERGENCY_MOVE_ATR,
    POSITION_WAKE_ENABLED,
    resolveSessionWindowConfig,
} from '../../../lib/swing/decisionConfig';
import { requireAdminAccess } from '../../../lib/admin';
import { bitgetFetch } from '../../../lib/bitget';
import {
    fetchCapitalCandlesByEpic,
    fetchCapitalMidPrice,
    fetchCapitalOpenPositionMarkers,
    listCapitalPendingEntryOrders,
    resolveCapitalEpic,
} from '../../../lib/capital';
import { kvDel, kvGetJson, kvMGetJson, kvSetJson } from '../../../lib/kv';
import { invokeCronEndpoint } from '../../../lib/cronChaining';
import { getCronSymbolConfigs } from '../../../lib/symbolRegistry';
import {
    clearSwingBreakTrigger,
    replaceSwingWakeSweeps,
    setSwingWakeTouch,
} from '../../../lib/swing/pg';
import { loadWakeWork } from '../../../lib/swing/wakeWorkCache';
import { loadSwingAiHealth } from '../../../lib/swing/aiHealth';
import { loadSwingCronControlState } from '../../../lib/swing/cronControl';
import { evaluateSessionDecisionWindow } from '../../../lib/swing/sessionEvents';
import {
    breakTriggerFailed,
    emergencyMoveAtr,
    failedBreakCheckDue,
    flatWakePlanStale,
    lastClosedBar,
    reclaimWakeEligible,
    sessionLevelsRefKey,
    sessionSweepEventKey,
    sessionSweepLookedKey,
    sessionSweepStateKey,
    sessionSweepStep,
    SESSION_SWEEP_EVENT_TTL_SECONDS,
    SESSION_SWEEP_LOOKED_TTL_SECONDS,
    SESSION_SWEEP_STATE_TTL_SECONDS,
    SESSION_WINDOW_OWED_FIRED_TTL_SECONDS,
    sessionWindowOwedFiredKey,
    sessionWindowOwedKey,
    sessionWindowWithdrawFiredKey,
    sustainedWakeStep,
    timeframeToMs,
    wakeBandCrossed,
    wakeWatchFiredKey,
    wakeWatchRefKey,
    WAKE_WATCH_FIRED_TTL_SECONDS,
    type SessionLevelsRef,
    type SessionSweepState,
    type WakeWatchRef,
} from '../../../lib/swing/wakeWatch';

// Session-sweep detection runs for the venue classes that carry session
// structure (matches analyze's SESSION_LEVEL_CATEGORIES).
const SESSION_SWEEP_CATEGORIES = new Set(['forex', 'commodity', 'index']);
import { fetchPendingEntryOrders, getTradeProductType } from '../../../lib/trading';

// Same knob the analyze route uses for its own off-boundary in-position look.
const EMERGENCY_MOVE_ATR = IN_POSITION_EMERGENCY_MOVE_ATR;

// Fires that exist for the reconcile above the analyze gates, not for a look.
const RECONCILE_ONLY_REASONS = new Set(['entry_filled', 'pending_entry_gone']);

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
    // Set when this run wrote to a work-list table itself (sustained-band
    // touch/sweep, break-trigger cleanup) — every such write bumps the
    // work-list version, same as the analyze runs this watcher fires.
    let wroteWorkList = false;
    // Long analyze invocations, started inside maybeFire but awaited together
    // right before the response — the detection loops stay quick while every
    // fired run keeps a connected client until it completes (or the 240s cap).
    const pendingFires: Promise<void>[] = [];
    // Resolves true when a fire was actually queued — false when another
    // event already holds this symbol's fired marker or a guard blocked it —
    // so callers with a once-per-event budget only spend it on a real fire.
    const maybeFire = async (platform: string, symbol: string, reason: string): Promise<boolean> => {
        const firedKey = wakeWatchFiredKey(platform, symbol);
        try {
            const already = await kvGetJson<{ ts: number }>(firedKey);
            if (already) return false; // an analyze run for this event is (or just was) in flight
            await kvSetJson(firedKey, { ts: Date.now(), reason }, WAKE_WATCH_FIRED_TTL_SECONDS);
        } catch (err) {
            // KV down → fire anyway: a rare duplicate AI call beats a missed wake.
            console.warn(`[wake-watch] fired-marker failed for ${platform}:${symbol}:`, err);
        }
        // Fire-time guards — checked HERE (after the fired-marker dedupe, so a
        // blocked event re-checks at most once a tick per symbol, and only on
        // actual fire attempts, never per watcher tick — KV cost ≈ zero):
        // - kill switch: the fired analyze call carries wake=1 and is blocked
        //   server-side too, but not firing at all spares the invocation;
        // - AI health: a billing/config outage doesn't self-heal — every fire
        //   would 500 until a human acts, so stay quiet (transient degradation
        //   still fires: the next call may well succeed). Fail open on KV
        //   errors — a wrongly-suppressed wake is worse than a wasted call.
        try {
            const [cronControl, aiHealth] = await Promise.all([
                loadSwingCronControlState(),
                loadSwingAiHealth(),
            ]);
            const blocked = cronControl.hardDeactivated
                ? 'swing_cron_hard_deactivated'
                : aiHealth.degraded && (aiHealth.kind === 'billing' || aiHealth.kind === 'config')
                  ? `ai_unavailable_${aiHealth.kind}`
                  : null;
            if (blocked) {
                fired.push({ platform, symbol, reason, invoked: false, error: `blocked:${blocked}` });
                console.log(`[wake-watch] suppressed ${platform}:${symbol} (${reason}): ${blocked}`);
                return false;
            }
        } catch (err) {
            console.warn(`[wake-watch] fire guards unreadable for ${platform}:${symbol}; firing anyway:`, err);
        }
        pendingFires.push(
            invokeCronEndpoint(
                req,
                '/api/swing/analyze',
                // wake=1: lets analyze apply the swing-cron kill switch to this
                // call — wake fires carry no Vercel cron headers, so without the
                // marker they'd be classified as manual operator ticks and bypass
                // the hard-deactivation gate entirely.
                //
                // enforcePrimaryCloseGate on a position_closed fire: that fire
                // exists for the close RECONCILE, not for a fresh decision, and
                // the reconcile surface (thread end, Capital close persistence
                // + postmortem enqueue, overlay invalidation) all sits ABOVE the
                // 4H-close gate in analyze. Without the flag the fire is
                // classified manual, so the cadence gate never applies and every
                // TP/SL fill bought an off-boundary flat AI call on a symbol
                // that had just gone flat. The gate still yields to a genuinely
                // crossed wake band (analyze peeks the band before skipping),
                // which matters because the per-symbol fired marker lets a close
                // win the marker over a same-minute band crossing; and a close
                // landing on a real 4H boundary evaluates as it normally would.
                {
                    symbol,
                    platform,
                    decisionPolicy: 'balanced',
                    wake: true,
                    // postCloseReconcile makes analyze skip the AI outright
                    // after the close upkeep — even inside the 4H boundary
                    // tolerance window, where the cadence gate alone let a
                    // fresh flat look through one minute after the close.
                    ...(reason === 'position_closed' ? { enforcePrimaryCloseGate: true, postCloseReconcile: true } : {}),
                    // Pending-entry upkeep (step 5): reconcile, then stop.
                    ...(RECONCILE_ONLY_REASONS.has(reason) ? { reconcileOnly: true } : {}),
                    ...(forwardDryRun ? { dryRun: true } : {}),
                },
                // Held open for the whole analyze run (~60-120s of AI latency):
                // a detached run gets hard-killed by the platform more often
                // than not, and it dies AFTER claiming the cooldown row.
                240_000,
            ).then((result) => {
                // An abort here means analyze outlived even the 240s cap — the
                // run may still finish server-side, so count it as fired (the
                // claim lease + fired marker cover the retry either way).
                const invoked = result.invoked || String(result.error || '').toLowerCase().includes('abort');
                fired.push({ platform, symbol, reason, invoked, error: invoked ? null : result.error });
                console.log(
                    `[wake-watch] fired ${platform}:${symbol} (${reason}) invoked=${invoked}${invoked ? '' : ` error=${result.error}`}`,
                );
            }),
        );
        return true;
    };

    // One KV-cached read of all the work lists instead of Postgres round
    // trips per watcher tick — the cron that kept the Neon compute from
    // ever scaling to zero. Falls back to Postgres on any cache miss or KV
    // failure; see lib/swing/wakeWorkCache.ts for the invalidation contract.
    const [wakeWork, bitgetPositionsRaw, capitalMarkers] = await Promise.all([
        loadWakeWork().catch((err) => {
            console.warn('[wake-watch] work list failed:', err);
            return { bands: [], triggers: [], threads: [], pendingEntries: [], source: 'pg' as const };
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

    const bandRows = wakeWork.bands;
    const breakTriggerRows = wakeWork.triggers;
    const inPositionThreads = wakeWork.threads;

    const bitgetFetchOk = Array.isArray(bitgetPositionsRaw);
    const capitalFetchOk = Array.isArray(capitalMarkers);
    const capitalMarkerRows = capitalMarkers ?? [];
    const bitgetPositions = (bitgetFetchOk ? (bitgetPositionsRaw as unknown[]) : [])
        .map((row) => {
            const r = row as { symbol?: unknown; markPrice?: unknown; total?: unknown; available?: unknown } | null;
            return {
                symbol: String(r?.symbol || '').toUpperCase(),
                price: Number(r?.markPrice),
                size: Number(r?.total ?? r?.available),
            };
        })
        .filter((p) => p.symbol && Number.isFinite(p.size) && p.size > 0);
    const openBySymbol = new Set<string>([
        ...bitgetPositions.map((p) => `bitget:${p.symbol}`),
        ...capitalMarkerRows.filter((m) => m.epic).map((m) => `capital:${m.epic}`),
    ]);

    // 1) Flat wake bands. A band row for a symbol that meanwhile has an open
    // position is stale (cooldowns are flat-only) — skip it; the in-position
    // path below owns that symbol. A band past its plan horizon (cooldown end
    // + grace) no longer earns an off-cadence fire either — the next regular
    // tick consumes the row and hands the note over as an expired idea.
    // Bands carrying wake_sustain_minutes fire only after price has HELD
    // beyond the band that long (sustainedWakeStep); a touch that reclaims
    // earlier is recorded on the row as sweep evidence instead of waking.
    let bandsChecked = 0;
    for (const row of bandRows) {
        if (openBySymbol.has(`${row.platform}:${row.symbol}`)) continue;
        if (flatWakePlanStale(row.untilMs, row.setAtMs, Date.now())) continue;
        bandsChecked++;
        const price =
            row.platform === 'capital'
                ? await fetchCapitalMidPrice(row.symbol)
                : await fetchBitgetLastPrice(row.symbol);
        if (!row.confirmMinutes) {
            const crossed = wakeBandCrossed(price, row.wakeAbove, row.wakeBelow);
            if (crossed) await maybeFire(row.platform, row.symbol, `wake_band_${crossed}`);
            continue;
        }
        const step = sustainedWakeStep({
            price: Number.isFinite(Number(price)) && Number(price) > 0 ? Number(price) : null,
            wakeAbove: row.wakeAbove,
            wakeBelow: row.wakeBelow,
            confirmMinutes: row.confirmMinutes,
            touchSide: row.touchSide,
            touchStartedMs: row.touchStartedMs,
            touchExtreme: row.touchExtreme,
            nowMs: Date.now(),
            atr: row.atr,
        });
        // Persistence is best-effort: a failed write means this tick's state
        // transition is retried next tick from the re-read row (arm re-arms
        // one tick later, a lost sweep costs evidence, never a wake).
        try {
            if (step.kind !== 'fire' && step.kind !== 'hold' && step.kind !== 'idle') wroteWorkList = true;
            if (step.kind === 'fire') {
                const reason =
                    step.via === 'extension'
                        ? `wake_band_${step.side}_break_${(step.extensionAtr ?? 0).toFixed(1)}atr`
                        : `wake_band_${step.side}_sustained_${step.heldMinutes}m`;
                await maybeFire(row.platform, row.symbol, reason);
            } else if (step.kind === 'arm') {
                if (step.sweep) {
                    await replaceSwingWakeSweeps(row.platform, row.symbol, [...row.sweeps, step.sweep]);
                }
                await setSwingWakeTouch(row.platform, row.symbol, {
                    side: step.side,
                    startedMs: Date.now(),
                    extreme: Number(price),
                });
            } else if (step.kind === 'extend') {
                await setSwingWakeTouch(row.platform, row.symbol, {
                    side: step.side,
                    startedMs: row.touchStartedMs as number,
                    extreme: step.extreme,
                });
            } else if (step.kind === 'sweep') {
                await replaceSwingWakeSweeps(row.platform, row.symbol, [...row.sweeps, step.sweep]);
                // Reclaim wake: a deep-enough sweep of the band earns ONE
                // immediate AI look at the bounce moment (the analyze route
                // re-detects the fresh sweep, claims the row's one-shot
                // budget and builds market.reclaim_wake). Only the pure sweep
                // transition fires — an 'arm' side-flip means price is already
                // beyond the OTHER band, not at a reclaimed level.
                if (
                    reclaimWakeEligible({
                        sweep: step.sweep,
                        atr: row.atr,
                        reclaimLookedAtMs: row.reclaimLookedAtMs,
                        nowMs: Date.now(),
                    })
                ) {
                    await maybeFire(row.platform, row.symbol, `reclaim_wake_${step.sweep.side}`);
                }
            }
        } catch (err) {
            console.warn(`[wake-watch] sustained-band persistence failed for ${row.platform}:${row.symbol}:`, err);
        }
    }

    // 1b) Session-level sweeps (reclaim-wake phase 2): per-tick touch state
    // machine against the last-session / prior-day highs and lows stamped by
    // analyze (sessionLevelsRefKey). A deep-enough touch-and-reclaim writes an
    // event the analyze route consumes as market.session_reclaim and fires the
    // look. Batched KV reads (two MGETs per tick) keep the round-trip cost
    // flat regardless of universe size; prices are fetched only for symbols
    // that actually carry a ref or an in-flight touch.
    let sessionSweepsChecked = 0;
    try {
        const sessionCfgs = getCronSymbolConfigs().filter((c) =>
            SESSION_SWEEP_CATEGORIES.has(String(c.category || '').toLowerCase()),
        );
        if (sessionCfgs.length) {
            const [refs, states] = await Promise.all([
                kvMGetJson<SessionLevelsRef>(sessionCfgs.map((c) => sessionLevelsRefKey(c.platform, c.symbol))),
                kvMGetJson<SessionSweepState>(sessionCfgs.map((c) => sessionSweepStateKey(c.platform, c.symbol))),
            ]);
            for (let i = 0; i < sessionCfgs.length; i++) {
                const cfg = sessionCfgs[i];
                const platform = String(cfg.platform || '').toLowerCase();
                const symbol = String(cfg.symbol || '').toUpperCase();
                const ref = refs[i] ?? null;
                const state = states[i] ?? null;
                if (!ref && !state) continue;
                // Flat symbols only — the position paths below own open ones.
                let openKeySymbol = symbol;
                if (platform === 'capital') {
                    try {
                        openKeySymbol = String(resolveCapitalEpic(symbol).epic || symbol).toUpperCase();
                    } catch {
                        /* unresolvable → raw-symbol key */
                    }
                }
                if (openBySymbol.has(`${platform}:${openKeySymbol}`)) continue;
                sessionSweepsChecked++;
                const price =
                    platform === 'capital'
                        ? await fetchCapitalMidPrice(symbol)
                        : await fetchBitgetLastPrice(symbol);
                const step = sessionSweepStep({
                    price: Number.isFinite(Number(price)) && Number(price) > 0 ? Number(price) : null,
                    ref,
                    state,
                    nowMs: Date.now(),
                });
                try {
                    if (step.kind === 'arm' || step.kind === 'extend') {
                        await kvSetJson(sessionSweepStateKey(platform, symbol), step.state, SESSION_SWEEP_STATE_TTL_SECONDS);
                    } else if (step.kind === 'abandon') {
                        await kvDel(sessionSweepStateKey(platform, symbol));
                    } else if (step.kind === 'reclaim') {
                        await kvDel(sessionSweepStateKey(platform, symbol));
                        // Same depth/freshness floor as band sweeps, plus a
                        // per-pool one-shot budget (the TLT failure mode is
                        // one pool poked many times per session).
                        const eligible = reclaimWakeEligible({
                            sweep: {
                                level: step.event.level,
                                extreme: step.event.extreme,
                                reclaimedAtMs: step.event.reclaimedAtMs,
                            },
                            atr: step.event.atr,
                            reclaimLookedAtMs: null,
                            nowMs: Date.now(),
                        });
                        const lookedKey = sessionSweepLookedKey(platform, symbol, step.event.kind);
                        const looked = eligible ? await kvGetJson<{ ts: number }>(lookedKey).catch(() => null) : null;
                        if (eligible && !looked) {
                            await kvSetJson(lookedKey, { ts: Date.now() }, SESSION_SWEEP_LOOKED_TTL_SECONDS);
                            await kvSetJson(
                                sessionSweepEventKey(platform, symbol),
                                step.event,
                                SESSION_SWEEP_EVENT_TTL_SECONDS,
                            );
                            await maybeFire(platform, symbol, `session_reclaim_${step.event.kind}`);
                        }
                    }
                } catch (err) {
                    console.warn(`[wake-watch] session-sweep persistence failed for ${platform}:${symbol}:`, err);
                }
            }
        }
    } catch (err) {
        console.warn('[wake-watch] session-sweep pass failed:', err);
    }

    // 2) In-position wake bands + emergency moves. Bands are AI-declared levels
    // inside the bracket, stored on the in_position thread (zero extra queries —
    // they ride the close-detection list) and checked against the same live
    // price the emergency check uses. Band first: the more specific fire
    // reason wins, and the shared per-symbol fired marker dedupes the pair
    // anyway. For Capital the thread stores the raw symbol while markers carry
    // the epic — key the band map under both so the lookup can't miss.
    const wakeByKey = new Map<string, { wakeAbove: number | null; wakeBelow: number | null }>();
    if (POSITION_WAKE_ENABLED) {
        for (const row of inPositionThreads) {
            if (row.wakeAbove === null && row.wakeBelow === null) continue;
            const platform = String(row.platform || '').toLowerCase();
            const symbol = String(row.symbol || '').toUpperCase();
            const bands = { wakeAbove: row.wakeAbove, wakeBelow: row.wakeBelow };
            wakeByKey.set(`${platform}:${symbol}`, bands);
            if (platform === 'capital') {
                try {
                    const epic = String(resolveCapitalEpic(symbol).epic || '').toUpperCase();
                    if (epic) wakeByKey.set(`capital:${epic}`, bands);
                } catch {
                    /* unresolvable → raw-symbol key only */
                }
            }
        }
    }
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
        const bands = wakeByKey.get(`${marker.platform}:${marker.symbol}`);
        const bandCrossed = bands ? wakeBandCrossed(marker.price, bands.wakeAbove, bands.wakeBelow) : null;
        if (bandCrossed) {
            await maybeFire(marker.platform, marker.symbol, `position_wake_${bandCrossed}`);
            continue;
        }
        const ref = await kvGetJson<WakeWatchRef>(wakeWatchRefKey(marker.platform, marker.symbol)).catch(
            () => null,
        );
        // nowMs enables the ref-staleness guard: a frozen anchor (no AI look
        // for > SWING_WAKE_REF_MAX_AGE_MINUTES) reads as quiet, not emergency.
        const moveAtr = emergencyMoveAtr(marker.price, ref, Date.now());
        if (moveAtr != null && moveAtr >= EMERGENCY_MOVE_ATR) {
            await maybeFire(marker.platform, marker.symbol, `emergency_move_${moveAtr.toFixed(2)}atr`);
        }
    }

    // 3) Failed-break triggers (swing.break_triggers, armed at entry on
    // breakout/breakdown-thesis trades): in the first ticks after each primary
    // bar close (failedBreakCheckDue — the :00 and :10 ticks),
    // fetch the last CLOSED bar and fire the analyze route if it closed back
    // through the trigger — the analyze run re-detects the condition itself,
    // surfaces market.failed_break to the model and consumes the row. Rows
    // whose position meanwhile closed (bracket exits happen with no analyze
    // tick) are cleaned up here.
    let breakTriggersChecked = 0;
    for (const row of breakTriggerRows) {
        if (!openBySymbol.has(`${row.platform}:${row.symbol}`)) {
            wroteWorkList = true;
            await clearSwingBreakTrigger(row.platform, row.symbol).catch((err) =>
                console.warn(`[wake-watch] break-trigger cleanup failed for ${row.platform}:${row.symbol}:`, err),
            );
            continue;
        }
        const tfMs = timeframeToMs(row.timeFrame);
        if (!tfMs) continue;
        if (!failedBreakCheckDue(tfMs, Date.now())) continue;
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
        const ascending = [...candles].sort(
            (a, b) => Number((a as ArrayLike<unknown> | null)?.[0]) - Number((b as ArrayLike<unknown> | null)?.[0]),
        );
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
    // persistence, overlay cache invalidation) instead of at the next
    // scheduled look. Reconcile-only: the fire carries enforcePrimaryCloseGate so it
    // does the upkeep and stops at the 4H-close gate rather than spending an
    // AI call (see maybeFire). The trade-off is that no flat HOLD runs here to
    // arm a fresh wake band — the symbol waits for its next primary close, the
    // same as any other off-boundary flat tick. Gated per venue on a SUCCESSFUL position fetch: a failed fetch
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

    // 5) Resting entries. Until 2026-10-06 the 15-minute analyze cron kept
    // pending_entry threads honest as a side effect of ticking; with analyze
    // running only inside the daily decision windows, that upkeep lives here.
    // Each check is venue-only; Postgres is touched only when a fire runs.
    //   filled  — the venue shows the position: reconcile-only fire, so the
    //             thread turns in_position and step 4 can see its close;
    //   window  — a Capital entry still resting as a session decision window
    //             opens: a normal wake fire, whose analyze session gate
    //             withdraws it (once per window — the analyze run holds this
    //             symbol's fired marker to the window's end itself);
    //   gone    — flat and nothing resting (venue purge, manual cancel):
    //             reconcile-only fire, which ends the stale thread. Left
    //             alone it would hold an asset-class slot in the portfolio
    //             cap and the dashboard's pendingEntry flag until the next
    //             scheduled look. A failed order read fires nothing.
    let pendingEntriesChecked = 0;
    const sessionWindowConfig = resolveSessionWindowConfig();
    const categoryByKey = new Map(
        getCronSymbolConfigs().map((c) => [
            `${String(c.platform || '').toLowerCase()}:${String(c.symbol || '').toUpperCase()}`,
            c.category ?? null,
        ]),
    );
    for (const row of wakeWork.pendingEntries) {
        const platform = String(row.platform || '').toLowerCase();
        const symbol = String(row.symbol || '').toUpperCase();
        let openKey = `${platform}:${symbol}`;
        if (platform === 'capital') {
            if (!capitalFetchOk) continue;
            try {
                openKey = `capital:${String(resolveCapitalEpic(symbol).epic || symbol).toUpperCase()}`;
            } catch {
                /* unresolvable → raw-symbol key */
            }
        } else if (!bitgetFetchOk) {
            continue;
        }
        pendingEntriesChecked++;
        if (openBySymbol.has(openKey)) {
            await maybeFire(platform, symbol, 'entry_filled');
            continue;
        }
        if (sessionWindowConfig.enabled) {
            const window = evaluateSessionDecisionWindow({
                symbol,
                category: categoryByKey.get(`${platform}:${symbol}`) ?? null,
                preOpenMin: sessionWindowConfig.preOpenMin,
                postOpenMin: sessionWindowConfig.postOpenMin,
                postCloseMin: sessionWindowConfig.postCloseMin,
            });
            if (window?.active && window.startMs !== null && window.endMs !== null) {
                const budgetKey = sessionWindowWithdrawFiredKey(platform, symbol, window.startMs);
                const spent = await kvGetJson<{ ts: number }>(budgetKey).catch(() => null);
                if (!spent && (await maybeFire(platform, symbol, `session_window_${window.kind}`))) {
                    const remainingSec = Math.max(60, Math.ceil((window.endMs - Date.now()) / 1000));
                    await kvSetJson(budgetKey, { ts: Date.now() }, remainingSec).catch(() => undefined);
                }
                continue;
            }
        }
        let resting: unknown[] | null = null;
        try {
            resting =
                platform === 'capital'
                    ? await listCapitalPendingEntryOrders(symbol)
                    : await fetchPendingEntryOrders(symbol, getTradeProductType());
        } catch (err) {
            console.warn(`[wake-watch] resting-entry read failed for ${platform}:${symbol}:`, err);
        }
        if (Array.isArray(resting) && resting.length === 0) {
            await maybeFire(platform, symbol, 'pending_entry_gone');
        }
    }

    // 6) Owed session-window looks. The analyze session gate parks a flat
    // look it refuses (a wake fire, the scheduled look, a step-5 withdraw) by
    // writing an owed marker with the window's end. The 15-minute cron used
    // to release it on its first tick after the window; now this does, once
    // per marker. One MGET per tick covers every calendar symbol. Skipped
    // while another window is active (adjacent pre-open → opening drive: the
    // analyze gate re-parks the look under the later window) and while the
    // symbol holds a position (the marker is a flat look).
    let owedLooksFired = 0;
    if (sessionWindowConfig.enabled) {
        try {
            const owedCfgs = getCronSymbolConfigs().filter((c) => String(c.platform || '').toLowerCase() === 'capital');
            if (owedCfgs.length) {
                const markers = await kvMGetJson<{ windowEndMs?: number }>(
                    owedCfgs.map((c) => sessionWindowOwedKey(c.platform, c.symbol)),
                );
                for (let i = 0; i < owedCfgs.length; i++) {
                    const windowEndMs = Number(markers[i]?.windowEndMs);
                    if (!(Number.isFinite(windowEndMs) && windowEndMs > 0 && windowEndMs <= Date.now())) continue;
                    const platform = 'capital';
                    const symbol = String(owedCfgs[i].symbol || '').toUpperCase();
                    let epic = symbol;
                    try {
                        epic = String(resolveCapitalEpic(symbol).epic || symbol).toUpperCase();
                    } catch {
                        /* unresolvable → raw-symbol key */
                    }
                    if (!capitalFetchOk || openBySymbol.has(`capital:${epic}`)) continue;
                    const activeNow = evaluateSessionDecisionWindow({
                        symbol,
                        category: owedCfgs[i].category ?? null,
                        preOpenMin: sessionWindowConfig.preOpenMin,
                        postOpenMin: sessionWindowConfig.postOpenMin,
                        postCloseMin: sessionWindowConfig.postCloseMin,
                    });
                    if (activeNow?.active) continue;
                    const budgetKey = sessionWindowOwedFiredKey(platform, symbol, windowEndMs);
                    const spent = await kvGetJson<{ ts: number }>(budgetKey).catch(() => null);
                    if (spent) continue;
                    if (await maybeFire(platform, symbol, 'session_window_owed')) {
                        owedLooksFired++;
                        await kvSetJson(budgetKey, { ts: Date.now() }, SESSION_WINDOW_OWED_FIRED_TTL_SECONDS).catch(
                            () => undefined,
                        );
                    }
                }
            }
        } catch (err) {
            console.warn('[wake-watch] owed session-window pass failed:', err);
        }
    }

    // Wait for every fired analyze run to finish before responding: the open
    // request is what keeps the fired run alive on Vercel (invokeCronEndpoint
    // never rejects, so this cannot throw). Concurrent fires overlap; the
    // per-symbol fired marker + cooldown claim lease dedupe the next watcher
    // tick that starts while these are still in flight.
    await Promise.all(pendingFires);

    // Rebuild the work-list snapshot NOW if this run invalidated it (fired
    // analyze runs and the watcher's own writes bump the version). The compute
    // is awake from those writes this very minute; left to the next tick, the
    // rebuild would land ten minutes later on a suspended compute and buy a
    // cold start of its own. A no-op KV read when nothing actually bumped.
    let workRefreshed = false;
    if (wroteWorkList || pendingFires.length > 0) {
        const refreshed = await loadWakeWork().catch((err) => {
            console.warn('[wake-watch] post-run work-list refresh failed:', err);
            return null;
        });
        workRefreshed = refreshed?.source === 'pg';
    }

    return res.status(200).json({
        ok: true,
        bandsChecked,
        sessionSweepsChecked,
        positionsChecked: positionMarkers.length,
        breakTriggersChecked,
        inPositionThreads: inPositionThreads.length,
        // 'kv' = this tick cost zero Postgres round trips. Watching this
        // field is how you confirm the work-list cache is actually serving.
        workSource: wakeWork.source,
        // true = this run rebuilt the snapshot after its own fires/writes.
        workRefreshed,
        closesDetected,
        pendingEntriesChecked,
        owedLooksFired,
        emergencyThresholdAtr: EMERGENCY_MOVE_ATR,
        fired,
    });
}
