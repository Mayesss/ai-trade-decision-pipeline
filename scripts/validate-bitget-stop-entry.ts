// Bitget resting STOP-entry validation — runs against DEMO trading only.
//
// The one unverified step in the resting-entry work (docs/resting-entry-separation.md):
// `place-plan-order` with planType=normal_plan. Bitget's API docs render
// client-side and could not be read, so the request body in executeDecision has
// never touched the venue. Until this passes, RESTING_ENTRY_VENUE_SUPPORT.bitget
// stays ['limit'] and a stop-entry on Bitget is DROPPED by the sanitizer rather
// than mis-placed as a limit (which would be the opposite trade).
//
// The read and cancel halves are already proven in production shape — the sweep
// reads orders-plan-pending and cancels via cancel-plan-order — but they are
// re-checked here against a REAL resting stop, because the two-book sweep is the
// safety-critical part: a plan order the TTL sweep misses survives into the next
// tick and the fresh entry stacks on top of it (the DE40 double fill, 2026-07-13).
//
// Phases:
//   A. executeDecision places a resting STOP entry (BUY, trigger ABOVE market so
//      it cannot fire) → accepted, returns pendingEntry + restingEntryKind 'stop'.
//   B. it appears in orders-plan-pending under planType=normal_plan with the
//      right triggerPrice / side / size, and the preset bracket rode along.
//   C. fetchPendingEntryOrders (REAL) returns it with planOrder=true and price
//      mapped from triggerPrice — the mapping the sweep depends on.
//   D. a resting LIMIT is placed alongside; ONE fetchPendingEntryOrders call
//      returns BOTH books. This is the check that matters most.
//   E. cancelPendingEntryOrders (REAL) clears both, found===cancelled, and both
//      books read empty afterwards.
//   F. OPT-IN (BITGET_VALIDATE_TRIGGER=1): a stop placed just above market
//      actually triggers, opens the position, and carries its bracket. Opens a
//      real demo position and closes it again. Skipped by default.
//
// Run:  npx tsx scripts/validate-bitget-stop-entry.ts
// Env:  BITGET_DEMO_API_KEY / BITGET_DEMO_API_SECRET / BITGET_DEMO_API_PASSPHRASE
//       (demo-trading keys — the LIVE keys are rejected by paptrading with 40099,
//       so this cannot be run without them). Optional: BITGET_DEMO_SYMBOL
//       (default BTCUSDT), BITGET_VALIDATE_TRIGGER=1 for phase F.
//
// Uses the REAL executeDecision / fetchPendingEntryOrders / cancelPendingEntryOrders
// from lib/trading.ts, so a pass validates the shipped code rather than a copy.
// Never touches live: every authed request carries the paptrading header
// (BITGET_PAPTRADING=1). Per the 2026-07-08 probe, the authed demo env uses the
// NORMAL names (USDT-FUTURES / BTCUSDT / USDT); the S-prefixed ones exist only in
// the public market-data API. The demo account must hold demo USDT.
import nextEnv from '@next/env';

import { bitgetFetch } from '../lib/bitget';
import type { ProductType } from '../lib/bitget';
import { fetchSymbolMeta, fetchPositionInfo } from '../lib/analytics';
import { executeDecision, fetchPendingEntryOrders, cancelPendingEntryOrders } from '../lib/trading';
import type { TradeDecision } from '../lib/trading';

const DEMO_PT = 'USDT-FUTURES' as unknown as ProductType;
const LEVERAGE = 5;

let failures = 0;
function check(name: string, ok: boolean, detail?: unknown) {
    const flag = ok ? 'PASS' : 'FAIL';
    if (!ok) failures++;
    console.log(`[${flag}] ${name}${detail !== undefined ? ` :: ${JSON.stringify(detail)}` : ''}`);
}
function info(msg: string, detail?: unknown) {
    console.log(`[info] ${msg}${detail !== undefined ? ` :: ${JSON.stringify(detail)}` : ''}`);
}
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

async function getLastPrice(symbol: string): Promise<number> {
    const data = await bitgetFetch('GET', '/api/v2/mix/market/ticker', { symbol, productType: DEMO_PT as string });
    const t = Array.isArray(data) ? data[0] : data;
    const p = Number(t?.lastPr ?? t?.last);
    if (!(p > 0)) throw new Error(`no demo ticker price for ${symbol}`);
    return p;
}

// The raw plan book, read directly rather than through our mapper — phase B has
// to see what the VENUE stored, not what our parser makes of it.
async function rawPlanBook(symbol: string): Promise<Array<Record<string, unknown>>> {
    const res = await bitgetFetch('GET', '/api/v2/mix/order/orders-plan-pending', {
        symbol,
        productType: DEMO_PT as string,
        planType: 'normal_plan',
    });
    return Array.isArray(res?.entrustedList) ? res.entrustedList : [];
}

async function rawPlainBook(symbol: string): Promise<Array<Record<string, unknown>>> {
    const res = await bitgetFetch('GET', '/api/v2/mix/order/orders-pending', {
        symbol,
        productType: DEMO_PT as string,
    });
    return Array.isArray(res?.entrustedList) ? res.entrustedList : [];
}

function restingDecision(kind: 'limit' | 'stop', price: number): TradeDecision {
    return {
        action: 'BUY',
        summary: `stop-entry validation (${kind})`,
        reason: `resting ${kind} entry endpoint validation`,
        leverage: LEVERAGE,
        resting_entry_kind: kind,
        ...(kind === 'stop' ? { entry_stop_price: price } : { entry_limit_price: price }),
    } as TradeDecision;
}

// Leave nothing resting on the demo account, whatever happened above.
async function cleanup(symbol: string) {
    try {
        const res = await cancelPendingEntryOrders(symbol, DEMO_PT);
        info('cleanup: cancelled resting orders', { found: res.found, cancelled: res.cancelled, errors: res.errors });
        const [plan, plain] = await Promise.all([rawPlanBook(symbol), rawPlainBook(symbol)]);
        if (plan.length || plain.length) {
            info('LEFTOVER resting orders — cancel manually in the demo UI', { plan: plan.length, plain: plain.length });
        }
    } catch (err) {
        info('cleanup failed', err instanceof Error ? err.message : String(err));
    }
}

async function main() {
    nextEnv.loadEnvConfig(process.cwd());
    // Demo routing + key override — AFTER env load, BEFORE any Bitget call.
    process.env.BITGET_PAPTRADING = '1';
    if (!process.env.BITGET_DEMO_API_KEY) {
        console.error(
            '\nBITGET_DEMO_API_KEY is not set. Bitget rejects LIVE keys on paptrading (40099), so this\n' +
                'validation cannot run without demo-trading keys. Create them in the Bitget demo UI and\n' +
                'export BITGET_DEMO_API_KEY / BITGET_DEMO_API_SECRET / BITGET_DEMO_API_PASSPHRASE.',
        );
        process.exit(2);
    }
    process.env.BITGET_API_KEY = process.env.BITGET_DEMO_API_KEY;
    process.env.BITGET_API_SECRET = process.env.BITGET_DEMO_API_SECRET || '';
    process.env.BITGET_API_PASSPHRASE = process.env.BITGET_DEMO_API_PASSPHRASE || '';
    info('using BITGET_DEMO_* API keys');

    const symbol = String(process.env.BITGET_DEMO_SYMBOL || 'BTCUSDT').toUpperCase();
    info(`symbol=${symbol} productType=${DEMO_PT} leverage=${LEVERAGE}`);

    const pos = await fetchPositionInfo(symbol);
    if (pos.status === 'open') {
        console.error(`\n${symbol} already has an OPEN demo position — close it first; this validation needs flat.`);
        process.exit(2);
    }
    await cleanup(symbol);

    const meta = await fetchSymbolMeta(symbol, DEMO_PT);
    const minQty = Number(meta.minTradeNum ?? 0.001);
    const price = await getLastPrice(symbol);
    // Smallest notional that still clears the venue minimum, then back out the
    // margin executeDecision expects (it multiplies by leverage internally).
    const notional = Math.max(minQty * price * 1.3, 5);
    const sideSizeUSDT = notional / LEVERAGE;
    info('sizing', { price, minQty, notional: Number(notional.toFixed(2)), sideSizeUSDT: Number(sideSizeUSDT.toFixed(2)) });

    // 2% away: far enough that neither resting order can fire during the run.
    const stopTrigger = price * 1.02;
    const limitLevel = price * 0.98;
    const stopSl = stopTrigger * 0.97;
    const stopTp = stopTrigger * 1.06;

    try {
        // ---- A: place the resting stop -------------------------------------
        const placed = await executeDecision(
            symbol,
            sideSizeUSDT,
            restingDecision('stop', stopTrigger),
            DEMO_PT,
            false,
            stopSl,
            stopTp,
        );
        check('A1 place-plan-order accepted', Boolean(placed?.placed), placed);
        check('A2 reported as a resting STOP entry', (placed as Record<string, unknown>)?.restingEntryKind === 'stop', {
            restingEntryKind: (placed as Record<string, unknown>)?.restingEntryKind,
            pendingEntry: (placed as Record<string, unknown>)?.pendingEntry,
        });
        await sleep(1_500);

        // ---- B: the venue actually stored what we meant ---------------------
        const plan = await rawPlanBook(symbol);
        check('B1 exactly one normal_plan order rests', plan.length === 1, { count: plan.length });
        const row = plan[0] ?? {};
        const triggerSeen = Number(row.triggerPrice);
        check(
            'B2 triggerPrice matches the requested level',
            Number.isFinite(triggerSeen) && Math.abs(triggerSeen - stopTrigger) / stopTrigger < 0.001,
            { requested: stopTrigger, stored: row.triggerPrice },
        );
        check('B3 side is buy', String(row.side || '').toLowerCase() === 'buy', { side: row.side });
        check('B4 size is non-zero', Number(row.size) > 0, { size: row.size });
        // The bracket is the reason a stop entry is safe to leave resting: if it
        // does not ride along, a triggered entry opens NAKED.
        //
        // Read ONLY the plan-book names. The place-order names
        // (presetStopLossPrice / presetStopSurplusPrice) are accepted by
        // place-plan-order and silently dropped, so accepting them here as a
        // fallback would hide exactly the bug this check exists to catch — it
        // did, on the first run, until the raw row showed which names stuck.
        const slSeen = Number(row.stopLossTriggerPrice);
        const tpSeen = Number(row.stopSurplusTriggerPrice);
        check('B5 stop-loss leg rode along', Number.isFinite(slSeen) && slSeen > 0, { stored: row.stopLossTriggerPrice, requested: stopSl });
        check('B6 take-profit leg rode along', Number.isFinite(tpSeen) && tpSeen > 0, {
            stored: row.stopSurplusTriggerPrice,
            requested: stopTp,
        });
        check('B7 both legs trigger on mark price', row.stopLossTriggerType === 'mark_price' && row.stopSurplusTriggerType === 'mark_price', {
            sl: row.stopLossTriggerType,
            tp: row.stopSurplusTriggerType,
        });

        // ---- C: our mapper reads it correctly -------------------------------
        const mapped = await fetchPendingEntryOrders(symbol, DEMO_PT);
        const mappedStop = mapped.find((o) => o.planOrder);
        check('C1 fetchPendingEntryOrders sees the plan order', Boolean(mappedStop), { rows: mapped.length });
        check(
            'C2 planOrder price is mapped from triggerPrice',
            Boolean(mappedStop && mappedStop.price && Math.abs(mappedStop.price - stopTrigger) / stopTrigger < 0.001),
            { mapped: mappedStop?.price, expected: stopTrigger },
        );
        check('C3 planOrder carries an orderId for the cancel path', Boolean(mappedStop?.orderId), {
            orderId: mappedStop?.orderId,
        });

        // ---- D: BOTH books in one sweep (the safety-critical check) ---------
        const placedLimit = await executeDecision(
            symbol,
            sideSizeUSDT,
            restingDecision('limit', limitLevel),
            DEMO_PT,
            false,
            limitLevel * 0.97,
            limitLevel * 1.06,
        );
        check('D1 resting LIMIT accepted alongside the stop', Boolean(placedLimit?.placed), placedLimit);
        await sleep(1_500);
        const both = await fetchPendingEntryOrders(symbol, DEMO_PT);
        check('D2 one sweep returns BOTH books', both.length === 2, {
            total: both.length,
            plan: both.filter((o) => o.planOrder).length,
            plain: both.filter((o) => !o.planOrder).length,
        });
        check(
            'D3 the two rows are distinguished by planOrder',
            both.filter((o) => o.planOrder).length === 1 && both.filter((o) => !o.planOrder).length === 1,
            both.map((o) => ({ planOrder: o.planOrder, price: o.price })),
        );

        // ---- E: the TTL sweep clears both -----------------------------------
        const swept = await cancelPendingEntryOrders(symbol, DEMO_PT);
        check('E1 sweep found both resting orders', swept.found === 2, { found: swept.found });
        check('E2 sweep cancelled everything it found', swept.cancelled === swept.found, {
            found: swept.found,
            cancelled: swept.cancelled,
            errors: swept.errors,
        });
        check('E3 no cancel errors', swept.errors.length === 0, swept.errors);
        await sleep(1_500);
        const [planAfter, plainAfter] = await Promise.all([rawPlanBook(symbol), rawPlainBook(symbol)]);
        check('E4 plan book empty after the sweep', planAfter.length === 0, { remaining: planAfter.length });
        check('E5 plain book empty after the sweep', plainAfter.length === 0, { remaining: plainAfter.length });

        // ---- F: does a trigger actually open the position? ------------------
        if (process.env.BITGET_VALIDATE_TRIGGER === '1') {
            // Bitget fires a plan order when the mark price CROSSES the trigger,
            // so a trigger parked above market only fires if price happens to
            // rise — a directional bet, and two earlier runs lost it (price
            // drifted down and the order simply rested). Measured on demo: mark
            // tracks last and moves ~$12 per 30s on BTC, oscillating both ways.
            //
            // So: sit the trigger ~2 USD above spot, where ordinary tick noise
            // crosses it in seconds, and RE-ANCHOR on each attempt — cancel and
            // re-place at the new spot rather than waiting out a trend. That
            // removes the direction bet instead of betting bigger on it.
            let opened = false;
            for (let attempt = 1; attempt <= 5 && !opened; attempt++) {
                const live = await getLastPrice(symbol);
                const nearTrigger = live * 1.000025;
                info(`phase F attempt ${attempt}: placing a triggerable stop (opens a real demo position)`, {
                    live,
                    nearTrigger: Number(nearTrigger.toFixed(2)),
                });
                await executeDecision(
                    symbol,
                    sideSizeUSDT,
                    restingDecision('stop', nearTrigger),
                    DEMO_PT,
                    false,
                    nearTrigger * 0.97,
                    nearTrigger * 1.06,
                );
                const startedAt = Date.now();
                while (Date.now() - startedAt < 45_000) {
                    await sleep(2_000);
                    const p = await fetchPositionInfo(symbol);
                    if (p.status === 'open') {
                        opened = true;
                        check('F1 triggered stop opened the position', true, {
                            entryPrice: p.entryPrice,
                            total: p.total,
                            attempt,
                            waitedSec: Math.round((Date.now() - startedAt) / 1000),
                        });
                        break;
                    }
                }
                if (!opened) {
                    info(`attempt ${attempt}: no cross in 45s — cancelling and re-anchoring to current price`);
                    await cancelPendingEntryOrders(symbol, DEMO_PT);
                    await sleep(1_000);
                }
            }
            if (!opened) {
                // INCONCLUSIVE, not a code failure: the order rested correctly
                // every time, the market just never crossed. Re-run rather than
                // treating this as a verdict on the code.
                check(
                    'F1 triggered stop opened the position',
                    false,
                    'INCONCLUSIVE: no cross across 5 re-anchored attempts — re-run; this is not evidence the code is wrong',
                );
            } else {
                const tpsl = await bitgetFetch('GET', '/api/v2/mix/order/orders-plan-pending', {
                    symbol,
                    productType: DEMO_PT as string,
                    planType: 'profit_loss',
                });
                const legs = Array.isArray(tpsl?.entrustedList) ? tpsl.entrustedList : [];
                check('F2 the triggered entry carries its bracket', legs.length >= 1, { legs: legs.length });
                await bitgetFetch('POST', '/api/v2/mix/order/close-positions', {}, { symbol, productType: DEMO_PT });
                info('phase F: demo position closed');
            }
        } else {
            info('phase F skipped (set BITGET_VALIDATE_TRIGGER=1 to open and close a real demo position)');
        }
    } finally {
        await cleanup(symbol);
    }

    console.log(failures === 0 ? '\nALL CHECKS PASSED' : `\n${failures} CHECK(S) FAILED`);
    if (failures === 0) {
        console.log(
            "Next: add 'stop' to RESTING_ENTRY_VENUE_SUPPORT.bitget in lib/swing/decisionConfig.ts\n" +
                'and update docs/resting-entry-separation.md.',
        );
    }
    process.exit(failures === 0 ? 0 : 1);
}

main().catch((err) => {
    console.error('\nvalidation aborted:', err instanceof Error ? err.message : err);
    process.exit(2);
});
