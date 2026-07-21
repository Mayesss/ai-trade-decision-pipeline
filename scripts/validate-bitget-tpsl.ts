// Bitget position-TPSL endpoint validation — runs against DEMO trading only.
//
// Validates the semantics the swing pipeline relies on but which were never
// verified on this account (see lib/trading.ts):
//   A. place-order presetStopLossPrice/presetStopSurplusPrice materialize as
//      position TPSL plans visible via orders-plan-pending (planType profit_loss)
//      → fetchPositionTpsl parses them.
//   B. modify-tpsl-order re-points an existing pos_profit/pos_loss plan
//      → updatePositionTpsl's modify path.
//   C. closing the position auto-cancels the TPSL plans (no orphans).
//   D. place-tpsl-order creates position TPSL on a bare position
//      → updatePositionTpsl's place path, then a second amend takes modify.
//   E. set-leverage on an OPEN isolated position (postSetLeverage): leverage
//      changes, size unchanged, resting TPSL plans survive → the margin-recycle
//      raise's endpoint semantics.
//   F. full profit-recycle sequence via the REAL maybeManagePosition:
//      BE stop (modify path) → leverage raise → reduceOnly partial trim →
//      post-trim stop amend with the fresh position size (the 43023 case).
//
// Run:  ENABLE_CRYPTO_MARGIN_RECYCLE=true \
//       CRYPTO_MARGIN_RECYCLE_MIN_PROFIT_BPS=-100000 \
//       CRYPTO_BE_STOP_FEE_BUFFER_BPS=-100 \
//       npx tsx scripts/validate-bitget-tpsl.ts
//       (the flag enables the maneuver for phase F; the two overrides let it run
//       on a just-opened demo position that has no profit cushion — the guard
//       always passes and the "breakeven" trigger sits ~1% below entry so the
//       pos_loss plan lands on the valid side of the mark price. Production
//       keeps the real defaults; endpoint semantics are identical.)
// Env:  BITGET_DEMO_API_KEY / BITGET_DEMO_API_SECRET / BITGET_DEMO_API_PASSPHRASE
//       (demo-trading keys; falls back to the live keys, which Bitget rejects
//       for paptrading with 40099). Optional: BITGET_DEMO_SYMBOL (default BTCUSDT).
//
// Uses the REAL fetchPositionTpsl / updatePositionTpsl from lib/trading.ts so a
// pass validates the shipped code, not a copy. Never touches live: every
// authed request carries the paptrading header (BITGET_PAPTRADING=1), which is
// what isolates the demo environment. Empirically (probed 2026-07-08): the
// authed demo env uses the NORMAL product type / symbol / margin coin names
// (USDT-FUTURES / BTCUSDT / USDT) — the S-prefixed names (SUSDT-FUTURES /
// SBTCSUSDT) exist only in the PUBLIC market-data API and get 40034/40778 on
// authed endpoints. The demo account must hold demo USDT (claim demo assets in
// the Bitget demo-trading UI) or orders fail on balance.
import nextEnv from '@next/env';

import { bitgetFetch } from '../lib/bitget';
import type { ProductType } from '../lib/bitget';
import { fetchSymbolMeta } from '../lib/analytics';
import type { PositionInfo } from '../lib/analytics';
import { fetchPositionTpsl, updatePositionTpsl, postSetLeverage, maybeManagePosition, pickTighterStop } from '../lib/trading';

const DEMO_PT = 'USDT-FUTURES' as unknown as ProductType;
const MARGIN_COIN = 'USDT';

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

async function getOpenPosition(symbol: string): Promise<PositionInfo> {
    const data = await bitgetFetch('GET', '/api/v2/mix/position/single-position', {
        symbol,
        productType: DEMO_PT as string,
        marginCoin: MARGIN_COIN,
    });
    const rows = Array.isArray(data) ? data : [];
    const row = rows.find((r: any) => Number(r?.total) > 0);
    if (!row) return { status: 'none' };
    const levRaw = Number(row.leverage ?? row.marginLeverage ?? row.lever);
    const markRaw = Number(row.markPrice);
    return {
        status: 'open',
        symbol,
        holdSide: String(row.holdSide || '').toLowerCase() as 'long' | 'short',
        entryPrice: String(row.openPriceAvg ?? row.averageOpenPrice ?? '0'),
        marginCoin: MARGIN_COIN,
        total: String(row.total),
        posMode: row.posMode === 'hedge_mode' ? 'hedge_mode' : 'one_way_mode',
        // Needed by the phase E/F management checks (maybeManagePosition wants
        // entry, mark and leverage).
        leverage: Number.isFinite(levRaw) && levRaw > 0 ? levRaw : null,
        markPrice: Number.isFinite(markRaw) && markRaw > 0 ? markRaw : null,
    };
}

async function waitForPosition(symbol: string, want: 'open' | 'none', timeoutMs = 15_000): Promise<PositionInfo> {
    const startedAt = Date.now();
    for (;;) {
        const pos = await getOpenPosition(symbol);
        if (pos.status === want) return pos;
        if (Date.now() - startedAt > timeoutMs) {
            throw new Error(`timeout waiting for position to be ${want} on ${symbol}`);
        }
        await sleep(1_500);
    }
}

async function openDemoPosition(symbol: string, presets: { sl?: number; tp?: number } | null, posMode: string) {
    const meta = await fetchSymbolMeta(symbol, DEMO_PT);
    const pricePlace = Number.isFinite(Number(meta.pricePlace)) ? Number(meta.pricePlace) : 1;
    const minTradeNum = String(meta.minTradeNum ?? '0.001');
    const body: any = {
        symbol,
        productType: DEMO_PT,
        marginCoin: MARGIN_COIN,
        marginMode: 'isolated',
        side: 'buy',
        orderType: 'market',
        size: minTradeNum,
        clientOid: `tpsl-val-${Date.now()}`,
        force: 'gtc',
    };
    if (posMode === 'hedge_mode') body.tradeSide = 'open';
    if (presets?.sl) body.presetStopLossPrice = presets.sl.toFixed(Math.max(0, pricePlace));
    if (presets?.tp) body.presetStopSurplusPrice = presets.tp.toFixed(Math.max(0, pricePlace));
    const res = await bitgetFetch('POST', '/api/v2/mix/order/place-order', {}, body);
    info(`entry order placed (${presets ? 'with presets' : 'bare'})`, { orderId: res?.orderId, size: minTradeNum });
    return { pricePlace };
}

async function flashClose(symbol: string) {
    try {
        const res = await bitgetFetch('POST', '/api/v2/mix/order/close-positions', {}, {
            symbol,
            productType: DEMO_PT,
        });
        info('flash close requested', { success: res?.successList?.length ?? 0 });
    } catch (err) {
        info('flash close failed (may already be flat)', err instanceof Error ? err.message : String(err));
    }
}

async function detectPosMode(symbol: string): Promise<string> {
    try {
        const data = await bitgetFetch('GET', '/api/v2/mix/account/account', {
            symbol,
            productType: DEMO_PT as string,
            marginCoin: MARGIN_COIN,
        });
        return String((data as any)?.posMode || 'one_way_mode');
    } catch {
        return 'one_way_mode';
    }
}

async function main() {
    nextEnv.loadEnvConfig(process.cwd());
    // Demo routing + key override — AFTER env load, BEFORE any Bitget call.
    process.env.BITGET_PAPTRADING = '1';
    if (process.env.BITGET_DEMO_API_KEY) {
        process.env.BITGET_API_KEY = process.env.BITGET_DEMO_API_KEY;
        process.env.BITGET_API_SECRET = process.env.BITGET_DEMO_API_SECRET || '';
        process.env.BITGET_API_PASSPHRASE = process.env.BITGET_DEMO_API_PASSPHRASE || '';
        info('using BITGET_DEMO_* API keys');
    } else {
        info('no BITGET_DEMO_* keys found — attempting live keys against paptrading (Bitget may reject them)');
    }

    const symbol = String(process.env.BITGET_DEMO_SYMBOL || 'BTCUSDT').toUpperCase();
    info(`symbol=${symbol} productType=${DEMO_PT}`);

    // Optional: flip the demo account's position mode before validating, so both
    // one-way (live production reality — holdSide buy/sell on TPSL plans) and
    // hedge (holdSide long/short) semantics can be certified. Requires flat.
    const wantPosMode = String(process.env.BITGET_DEMO_SET_POSMODE || '').trim();
    if (wantPosMode === 'one_way_mode' || wantPosMode === 'hedge_mode') {
        try {
            await bitgetFetch('POST', '/api/v2/mix/account/set-position-mode', {}, {
                productType: DEMO_PT,
                posMode: wantPosMode,
            });
            info(`position mode set to ${wantPosMode}`);
        } catch (err) {
            info('set-position-mode failed (continuing with current mode)', err instanceof Error ? err.message : String(err));
        }
    }

    const price = await getLastPrice(symbol);
    info(`demo last price = ${price}`);
    const posMode = await detectPosMode(symbol);
    info(`account posMode = ${posMode}`);

    try {
        // ---- Phase A: entry presets materialize as position TPSL plans ----
        await openDemoPosition(symbol, { sl: price * 0.9, tp: price * 1.1 }, posMode);
        const posA = await waitForPosition(symbol, 'open');
        info('position open', { holdSide: (posA as any).holdSide, total: (posA as any).total });
        await sleep(2_000);
        const plansA = await fetchPositionTpsl(symbol, DEMO_PT);
        check('A1 entry preset TP appears in orders-plan-pending (fetchPositionTpsl)', plansA.takeProfit != null, plansA.takeProfit);
        check('A2 entry preset SL appears in orders-plan-pending (fetchPositionTpsl)', plansA.stopLoss != null, plansA.stopLoss);

        // ---- Phase B: modify path re-points existing plans ----
        const upB = await updatePositionTpsl({
            symbol,
            productType: DEMO_PT,
            takeProfitPrice: price * 1.12,
            stopLossPrice: price * 0.88,
            pos: posA,
        });
        check('B1 TP amend applied', upB.takeProfit?.applied === true, upB.takeProfit);
        check('B2 SL amend applied', upB.stopLoss?.applied === true, upB.stopLoss);
        check(
            'B3 amend used modify path when plans existed',
            (plansA.takeProfit == null || upB.takeProfit?.mode === 'modify') &&
                (plansA.stopLoss == null || upB.stopLoss?.mode === 'modify'),
            { tpMode: upB.takeProfit?.mode, slMode: upB.stopLoss?.mode },
        );
        await sleep(2_000);
        const plansB = await fetchPositionTpsl(symbol, DEMO_PT);
        const closeEnough = (a?: number | null, b?: number | null) =>
            a != null && b != null && Math.abs(a - b) / b < 0.001;
        check('B4 TP trigger price actually moved', closeEnough(plansB.takeProfit?.price, price * 1.12), plansB.takeProfit);
        check('B5 SL trigger price actually moved', closeEnough(plansB.stopLoss?.price, price * 0.88), plansB.stopLoss);

        // ---- Phase C: closing the position cleans up its TPSL plans ----
        await flashClose(symbol);
        await waitForPosition(symbol, 'none');
        await sleep(2_000);
        const plansC = await fetchPositionTpsl(symbol, DEMO_PT);
        check('C1 TPSL plans auto-cancel on position close (no orphans)', plansC.takeProfit == null && plansC.stopLoss == null, plansC);

        // ---- Phase D: place path on a bare position, then modify ----
        await openDemoPosition(symbol, null, posMode);
        const posD = await waitForPosition(symbol, 'open');
        await sleep(2_000);
        const plansD0 = await fetchPositionTpsl(symbol, DEMO_PT);
        check('D1 bare entry has no TPSL plans', plansD0.takeProfit == null && plansD0.stopLoss == null, plansD0);
        const upD = await updatePositionTpsl({
            symbol,
            productType: DEMO_PT,
            takeProfitPrice: price * 1.08,
            stopLossPrice: price * 0.92,
            pos: posD,
        });
        check('D2 TP placed on bare position', upD.takeProfit?.applied === true && upD.takeProfit?.mode === 'place', upD.takeProfit);
        check('D3 SL placed on bare position', upD.stopLoss?.applied === true && upD.stopLoss?.mode === 'place', upD.stopLoss);
        await sleep(2_000);
        const plansD1 = await fetchPositionTpsl(symbol, DEMO_PT);
        info('D4 inputs', { pos: posD, plans: plansD1 });
        const upD2 = await updatePositionTpsl({
            symbol,
            productType: DEMO_PT,
            takeProfitPrice: price * 1.09,
            pos: posD,
        });
        check('D4 second amend takes the modify path', upD2.takeProfit?.applied === true && upD2.takeProfit?.mode === 'modify', upD2.takeProfit);

        // ---- Phase E: set-leverage on the OPEN isolated position ----
        // Continues from phase D: position open with placed TPSL plans resting.
        await sleep(2_000);
        const posE0 = await getOpenPosition(symbol);
        if (posE0.status !== 'open' || !posE0.holdSide) throw new Error('phase E expected an open position');
        const levBefore = Number(posE0.leverage);
        const sizeBefore = Number(posE0.total);
        const plansE0 = await fetchPositionTpsl(symbol, DEMO_PT);
        check('E0 preconditions: leverage+size readable, plans resting', levBefore > 0 && sizeBefore > 0 && plansE0.stopLoss != null, {
            levBefore,
            sizeBefore,
        });
        const levTargetE = Math.round(levBefore + 5);
        await postSetLeverage(symbol, DEMO_PT, levTargetE, posE0.holdSide);
        await sleep(2_000);
        const posE1 = await getOpenPosition(symbol);
        check('E1 leverage raised on the open position', Number((posE1 as any).leverage) === levTargetE, {
            before: levBefore,
            after: (posE1 as any).leverage,
        });
        check('E2 position size unchanged by the raise', Number((posE1 as any).total) === sizeBefore, {
            before: sizeBefore,
            after: (posE1 as any).total,
        });
        const plansE1 = await fetchPositionTpsl(symbol, DEMO_PT);
        check('E3 resting TPSL plans survive the leverage change', plansE1.takeProfit != null && plansE1.stopLoss != null, plansE1);

        // ---- Phase F: full profit-recycle sequence (real maybeManagePosition) ----
        // Double the position first so a 50% trim stays >= minTradeNum.
        const metaF = await fetchSymbolMeta(symbol, DEMO_PT);
        const minTradeNum = String(metaF.minTradeNum ?? '0.001');
        const addBody: any = {
            symbol,
            productType: DEMO_PT,
            marginCoin: MARGIN_COIN,
            marginMode: 'isolated',
            side: 'buy',
            orderType: 'market',
            size: minTradeNum,
            clientOid: `tpsl-val-add-${Date.now()}`,
            force: 'gtc',
        };
        if (posMode === 'hedge_mode') addBody.tradeSide = 'open';
        await bitgetFetch('POST', '/api/v2/mix/order/place-order', {}, addBody);
        await sleep(2_500);
        const posF0 = await getOpenPosition(symbol);
        if (posF0.status !== 'open' || !posF0.holdSide) throw new Error('phase F expected an open position');
        const sizeF0 = Number(posF0.total);
        info('phase F position', { size: sizeF0, leverage: posF0.leverage, entry: posF0.entryPrice });

        // 1) Management: BE stop (modify path — plans exist) then leverage raise.
        //    Requires ENABLE_CRYPTO_MARGIN_RECYCLE=true and the demo-friendly
        //    profit/buffer overrides from the header (guard passes, trigger lands
        //    on the valid side of the mark price).
        const levTargetF = Math.round(Number(posF0.leverage ?? levTargetE) + 5);
        const mgmtF = await maybeManagePosition({
            symbol,
            productType: DEMO_PT,
            decision: { action: 'CLOSE', exit_size_pct: 50, raise_leverage_to: levTargetF, move_stop_to_be: true } as any,
            dryRun: false,
            pos: posF0,
        });
        check('F1 maneuver ran and BE stop rested (modify path)', Boolean((mgmtF as any)?.managed && (mgmtF as any)?.beStop?.ok), mgmtF);
        check('F2 leverage raise applied after the BE stop', (mgmtF as any)?.leverageRaised === true, {
            leverage: (mgmtF as any)?.leverage,
            error: (mgmtF as any)?.leverageError,
        });
        const beTriggerF = Number((mgmtF as any)?.beTriggerPrice);
        check('F3 maneuver surfaced its BE trigger for the stop-amend guard', Number.isFinite(beTriggerF) && beTriggerF > 0, {
            beTriggerF,
        });

        // 2) reduceOnly 50% trim (production body shape).
        const stepF = parseFloat(String(metaF.sizeMultiplier ?? minTradeNum));
        const rawHalf = sizeF0 / 2;
        const trimSize = Math.max(Math.floor(rawHalf / stepF) * stepF, parseFloat(minTradeNum));
        const trimBody: any = {
            symbol,
            productType: DEMO_PT,
            marginCoin: MARGIN_COIN,
            marginMode: 'isolated',
            orderType: 'market',
            size: trimSize.toString(),
            clientOid: `tpsl-val-trim-${Date.now()}`,
            force: 'gtc',
        };
        if (posMode === 'hedge_mode') {
            trimBody.side = posF0.holdSide === 'long' ? 'buy' : 'sell';
            trimBody.tradeSide = 'close';
            trimBody.holdSide = posF0.holdSide;
        } else {
            trimBody.side = posF0.holdSide === 'long' ? 'sell' : 'buy';
            trimBody.reduceOnly = 'YES';
        }
        await bitgetFetch('POST', '/api/v2/mix/order/place-order', {}, trimBody);
        await sleep(2_500);
        const posF1 = await getOpenPosition(symbol);
        check('F4 reduceOnly trim reduced the position', posF1.status === 'open' && Number((posF1 as any).total) < sizeF0, {
            before: sizeF0,
            after: (posF1 as any).total,
        });

        // 3) Post-trim stop amend with the FRESH position (modify must send the
        //    current size — stale size is the 43023 failure this certifies against).
        const aiStopTighter = posF0.holdSide === 'long' ? beTriggerF * 1.002 : beTriggerF * 0.998;
        const guardedStop = pickTighterStop(posF0.holdSide, beTriggerF, aiStopTighter);
        check('F5 pickTighterStop keeps a tighter stop / drops a looser one', guardedStop === aiStopTighter &&
            pickTighterStop(posF0.holdSide, beTriggerF, posF0.holdSide === 'long' ? beTriggerF * 0.99 : beTriggerF * 1.01) === null, {
            beTriggerF,
            aiStopTighter,
        });
        const upF = await updatePositionTpsl({
            symbol,
            productType: DEMO_PT,
            stopLossPrice: guardedStop,
            pos: posF1.status === 'open' ? posF1 : undefined,
        });
        check('F6 post-trim stop amend applied with fresh size (no 43023)', upF.stopLoss?.applied === true && upF.stopLoss?.mode === 'modify', upF.stopLoss);
    } finally {
        // ---- Cleanup: never leave a demo position or stray plans behind ----
        await flashClose(symbol);
        try {
            await waitForPosition(symbol, 'none', 20_000);
            const leftovers = await fetchPositionTpsl(symbol, DEMO_PT);
            if (leftovers.takeProfit || leftovers.stopLoss) {
                info('leftover TPSL plans after cleanup — cancel manually in demo UI', leftovers);
            } else {
                info('cleanup complete: flat, no leftover plans');
            }
        } catch (err) {
            info('cleanup verification failed', err instanceof Error ? err.message : String(err));
        }
    }

    console.log(failures === 0 ? '\nALL CHECKS PASSED' : `\n${failures} CHECK(S) FAILED`);
    process.exit(failures === 0 ? 0 : 1);
}

main().catch((err) => {
    console.error('\nvalidation aborted:', err instanceof Error ? err.message : err);
    process.exit(2);
});
