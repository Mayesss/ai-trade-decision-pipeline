// lib/trading.ts

import crypto from 'crypto';
import { bitgetFetch, resolveProductType } from './bitget';
import type { ProductType } from './bitget';

import { computeOrderSize, fetchPositionInfo, fetchSymbolMeta } from './analytics';
import type { PositionInfo } from './analytics';

// ------------------------------
// Types
// ------------------------------

export interface TradeDecision {
    action: 'BUY' | 'SELL' | 'HOLD' | 'CLOSE' | 'REVERSE';
    summary: string;
    reason: string;
    timestamp?: number;
    signal_strength?: 'LOW' | 'MEDIUM' | 'HIGH' | string;
    leverage?: number | null;
    exit_size_pct?: number | null;
    close_size_pct?: number | null;
    target_position_pct?: number | null;
    // Profit-lock margin-recycle maneuver (crypto/Bitget only, HOLD or partial
    // CLOSE): move the stop to (just past) breakeven and raise leverage toward the
    // symbol's max. On isolated margin the raise releases locked margin without
    // cutting notional; the breakeven stop keeps the higher-leverage remainder
    // risk-bounded. Honored only when ENABLE_CRYPTO_MARGIN_RECYCLE is set.
    move_stop_to_be?: boolean | null;
    raise_leverage_to?: number | null;
    // Exchange-side bracket, sanitized upstream (sanitizeExchangeTpSl): on entry
    // take_profit_price is attached with the order; on HOLD / partial CLOSE both
    // fields amend the position's standing TP/SL plan orders (null = leave as-is).
    take_profit_price?: number | null;
    stop_loss_price?: number | null;
    // Pullback limit entry (flat BUY/SELL, sanitized upstream via
    // sanitizeEntryLimit): rest a LIMIT at this price instead of a market
    // entry. The caller owns the one-tick TTL (cancelPendingEntryOrders).
    entry_limit_price?: number | null;
}

// Profit-lock margin-recycle feature flags (crypto only). The full sequence —
// BE stop (modify path) → set-leverage raise on the open isolated position →
// reduceOnly trim → post-trim stop amend — was validated end-to-end on Bitget
// DEMO 2026-07-21 (scripts/validate-bitget-tpsl.ts, phases A–F all green).
// Enable via env; production needs ENABLE_CRYPTO_MARGIN_RECYCLE=true on Vercel.
const MARGIN_RECYCLE_ENABLED = process.env.ENABLE_CRYPTO_MARGIN_RECYCLE === 'true';
// Breakeven stop sits this many bps past entry so the locked exit is net
// non-negative after round-trip venue fees (~0.30R).
const MARGIN_RECYCLE_FEE_BUFFER_BPS = Number(process.env.CRYPTO_BE_STOP_FEE_BUFFER_BPS ?? 8);
// Only manage once price has moved at least this far past entry in the favorable
// direction — avoids setting a breakeven stop while price still hugs entry
// (instant/whipsaw stop-out).
const MARGIN_RECYCLE_MIN_PROFIT_BPS = Number(process.env.CRYPTO_MARGIN_RECYCLE_MIN_PROFIT_BPS ?? 40);
// Auto-recycle: with the master flag on, the maneuver is code-enforced on any
// position whose profit cushion exceeds the auto guard — it no longer depends
// on the AI opting in via raise_leverage_to/move_stop_to_be (observed live:
// the model almost never opts in, even on textbook profitable trims). Opt out
// with CRYPTO_MARGIN_RECYCLE_AUTO=false. The auto guard is deliberately wider
// than MIN_PROFIT_BPS: a breakeven stop rested on a 40bps cushion converts
// most winners into scratches on noise; auto waits for a real cushion.
const MARGIN_RECYCLE_AUTO = process.env.CRYPTO_MARGIN_RECYCLE_AUTO !== 'false';
const MARGIN_RECYCLE_AUTO_MIN_PROFIT_BPS = Number(process.env.CRYPTO_MARGIN_RECYCLE_AUTO_MIN_PROFIT_BPS ?? 250);
// Liquidation-safety ceiling for any recycle raise. Isolated liq distance from
// entry ≈ 1/leverage − maintenance margin rate; the raise must keep that
// distance at least LIQ_BUFFER so a gap through the breakeven stop hits the
// stop, not liquidation (the BE stop is a trigger order — at extreme leverage
// the liq price sits inside normal noise and fires first). 400+100 bps → 20x.
const MARGIN_RECYCLE_LIQ_BUFFER_BPS = Number(process.env.CRYPTO_MARGIN_RECYCLE_LIQ_BUFFER_BPS ?? 400);
const MARGIN_RECYCLE_ASSUMED_MMR_BPS = Number(process.env.CRYPTO_MARGIN_RECYCLE_ASSUMED_MMR_BPS ?? 100);

// Highest leverage the recycle maneuver may set: liq distance (≈ 1/lev − mmr)
// stays ≥ the configured buffer. Exported for tests.
export function recycleLeverageCeiling(): number {
    const denomBps = Math.max(50, MARGIN_RECYCLE_LIQ_BUFFER_BPS + MARGIN_RECYCLE_ASSUMED_MMR_BPS);
    return Math.max(1, Math.floor(10000 / denomBps));
}

function normalizeClosePct(pct: unknown) {
    const n = Number(pct);
    if (!Number.isFinite(n)) return null;
    const clamped = Math.max(0, Math.min(100, n));
    return clamped > 0 ? clamped : null;
}

// Crypto entry leverage range (the model is prompted for 5–10; anything
// outside clamps into it).
function clampLeverage(value: unknown): number | null {
    const n = Number(value);
    if (!Number.isFinite(n)) return null;
    const rounded = Math.round(n);
    const clamped = Math.max(5, Math.min(10, rounded));
    return clamped;
}

function deriveLeverage(decision: TradeDecision): number | null {
    const explicit = clampLeverage((decision as any)?.leverage);
    if (explicit) return explicit;

    const strengthRaw = (decision as any)?.signal_strength;
    const strengthNum = Number(strengthRaw);
    if (Number.isFinite(strengthNum)) {
        const mapped = clampLeverage(strengthNum);
        if (mapped) return mapped;
    }
    const strength = String(strengthRaw || '').toUpperCase();
    if (decision.action === 'BUY' || decision.action === 'SELL' || decision.action === 'REVERSE') {
        if (strength === 'HIGH') return 8;
        if (strength === 'MEDIUM') return 6;
        if (strength === 'LOW') return 5;
    }
    return null;
}

function deriveOrderNotional(sideSizeUSDT: number, leverage: number | null): number {
    const lev = clampLeverage(leverage) ?? 1;
    return sideSizeUSDT * lev;
}

// Raw set-leverage POST — no clamping. Callers own the clamp (1–5 at entry via
// applyLeverage; up to symbol max for the profit-lock raise via
// clampManagementLeverage), so this helper stays policy-free. Exported for
// scripts/validate-bitget-tpsl.ts (demo phase E certifies set-leverage on an
// OPEN isolated position via this exact function).
export async function postSetLeverage(symbol: string, productType: ProductType, leverage: number, holdSide?: 'long' | 'short') {
    const pt = (productType as string).toUpperCase();
    const body: any = {
        symbol,
        productType: pt,
        marginCoin: 'USDT',
        marginMode: 'isolated',
        leverage: Math.round(leverage).toString(),
    };
    if (holdSide) body.holdSide = holdSide;
    return bitgetFetch('POST', '/api/v2/mix/account/set-leverage', {}, body);
}

// Bitget's set-leverage writes to the slot of the symbol's CURRENT margin mode
// (the marginMode field in its body is not a documented parameter and is
// ignored), so a symbol still in crossed mode takes the leverage on the crossed
// slot and the isolated order then opens at the stale isolated setting
// (observed live: BGBUSDT opened at 1x after a "successful" 5x set, consuming
// 5x the intended margin). Force the symbol to isolated before setting
// leverage. Rejected while a position is open — fine: an open position means
// the isolated slot is already the effective one.
async function ensureIsolatedMarginMode(
    symbol: string,
    productType: ProductType,
): Promise<{ ok: boolean; error?: string }> {
    try {
        await bitgetFetch('POST', '/api/v2/mix/account/set-margin-mode', {}, {
            symbol,
            productType: (productType as string).toUpperCase(),
            marginCoin: 'USDT',
            marginMode: 'isolated',
        });
        return { ok: true };
    } catch (err) {
        return { ok: false, error: err instanceof Error ? err.message : String(err) };
    }
}

async function applyLeverage(params: {
    symbol: string;
    productType: ProductType;
    leverage: number | null;
    holdSide?: 'long' | 'short';
    dryRun?: boolean;
}) {
    const { symbol, productType, leverage, holdSide, dryRun } = params;
    const target = clampLeverage(leverage);
    if (!target) return { applied: false, leverage: null, skipped: true };
    if (dryRun) return { applied: false, leverage: target, dryRun: true };

    const marginMode = await ensureIsolatedMarginMode(symbol, productType);
    try {
        const res = await postSetLeverage(symbol, productType, target, holdSide);
        return {
            applied: true,
            leverage: target,
            raw: res,
            ...(marginMode.ok ? {} : { marginModeError: marginMode.error }),
        };
    } catch (err) {
        console.warn('Failed to set leverage:', err);
        return {
            applied: false,
            leverage: target,
            error: err instanceof Error ? err.message : String(err),
        };
    }
}

// Management-time leverage clamp: unlike the 1–5 entry clamp, a profit-lock raise
// may climb to the symbol's exchange maximum. Never below current leverage, never
// above the symbol max; returns null when the target isn't a genuine raise.
function clampManagementLeverage(target: unknown, current: number, symbolMax: number): number | null {
    const n = Math.round(Number(target));
    if (!Number.isFinite(n)) return null;
    const lo = Math.max(1, Math.floor(current));
    const hi = Math.max(lo, Math.floor(symbolMax));
    const clamped = Math.max(lo, Math.min(hi, n));
    return clamped > current ? clamped : null;
}

// After the margin-recycle maneuver rests a breakeven stop, the AI's own stop
// amend is applied only when it TIGHTENS protection past that BE trigger
// (long: higher, short: lower). Otherwise the BE stop stands and the amend leg
// is dropped — the maneuver's floor must never be loosened by a same-tick
// amend. No BE trigger (maneuver didn't move the stop) → the AI stop passes
// through untouched (the route's sanitize already enforced tighten-only vs the
// pre-tick standing stop).
export function pickTighterStop(
    side: 'long' | 'short',
    beTriggerPrice: number | null | undefined,
    aiStopPrice: number | null | undefined,
): number | null {
    const aiStop = Number(aiStopPrice);
    if (!(Number.isFinite(aiStop) && aiStop > 0)) return null;
    const beTrigger = Number(beTriggerPrice);
    if (!(Number.isFinite(beTrigger) && beTrigger > 0)) return aiStop;
    if (side === 'long') return aiStop > beTrigger ? aiStop : null;
    return aiStop < beTrigger ? aiStop : null;
}

// Move a position's stop-loss to (just past) breakeven via the position-level
// TP/SL plan. Delegates to updatePositionTpsl, which MODIFIES the existing
// pos_loss plan when one is pending (the entry presetStopLossPrice materializes
// as exactly one such plan — validated on demo, scripts/validate-bitget-tpsl.ts)
// and places a fresh plan otherwise — so the BE move can never stack a second
// stop on top of the entry one.
async function setPositionBreakevenStop(params: {
    symbol: string;
    productType: ProductType;
    triggerPrice: number;
    pos: PositionInfo;
}): Promise<{ ok: boolean; triggerPrice: number; mode?: 'modify' | 'place' | 'dryRun'; error?: string }> {
    const { symbol, productType, triggerPrice, pos } = params;
    const res = await updatePositionTpsl({
        symbol,
        productType,
        stopLossPrice: triggerPrice,
        pos,
    });
    const leg = res.stopLoss;
    if (!leg) return { ok: false, triggerPrice, error: res.note || 'stop_leg_missing' };
    return {
        ok: leg.applied,
        triggerPrice: leg.requested,
        mode: leg.mode,
        ...(leg.error ? { error: leg.error } : {}),
    };
}

// ------------------------------
// Position-level TP/SL (resting exchange bracket)
// ------------------------------

export type PositionTpsl = {
    takeProfit: { price: number; orderId: string | null; size: string | null } | null;
    stopLoss: { price: number; orderId: string | null; size: string | null } | null;
};

// Read the position's standing TP/SL plan orders (including the presets attached
// at entry, which Bitget materializes as pos_profit/pos_loss plans once filled).
// Used both to show the current bracket to the model and to decide
// modify-vs-place when amending.
export async function fetchPositionTpsl(symbol: string, productType: ProductType): Promise<PositionTpsl> {
    const res = await bitgetFetch('GET', '/api/v2/mix/order/orders-plan-pending', {
        symbol,
        productType: (productType as string).toUpperCase(),
        planType: 'profit_loss',
    });
    const list = Array.isArray(res?.entrustedList) ? res.entrustedList : [];
    let takeProfit: PositionTpsl['takeProfit'] = null;
    let stopLoss: PositionTpsl['stopLoss'] = null;
    for (const o of list) {
        const planType = String(o?.planType || '').toLowerCase();
        const price = Number(o?.triggerPrice);
        if (!(Number.isFinite(price) && price > 0)) continue;
        const entry = {
            price,
            orderId: o?.orderId ? String(o.orderId) : null,
            size: o?.size != null ? String(o.size) : null,
        };
        if (planType === 'pos_profit' || planType === 'profit_plan') takeProfit = entry;
        else if (planType === 'pos_loss' || planType === 'loss_plan') stopLoss = entry;
    }
    return { takeProfit, stopLoss };
}

type TpslLegResult = {
    requested: number;
    applied: boolean;
    mode: 'modify' | 'place' | 'dryRun';
    error?: string;
};

// Amend the position's standing TP/SL: modify the existing plan order when one
// is pending, otherwise place a new position-level (pos_profit/pos_loss) plan.
// Per-leg failures are surfaced, never thrown — a failed amend must not break
// the decision path (the catastrophe stop from entry still bounds the position).
export async function updatePositionTpsl(params: {
    symbol: string;
    productType: ProductType;
    takeProfitPrice?: number | null;
    stopLossPrice?: number | null;
    dryRun?: boolean;
    pos?: PositionInfo;
}): Promise<{ takeProfit?: TpslLegResult; stopLoss?: TpslLegResult; note?: string }> {
    const { symbol, productType, dryRun } = params;
    const tpTarget =
        Number.isFinite(params.takeProfitPrice as number) && (params.takeProfitPrice as number) > 0
            ? Number(params.takeProfitPrice)
            : null;
    const slTarget =
        Number.isFinite(params.stopLossPrice as number) && (params.stopLossPrice as number) > 0
            ? Number(params.stopLossPrice)
            : null;
    if (tpTarget == null && slTarget == null) return { note: 'no_tpsl_targets' };

    const pos = params.pos ?? (await fetchPositionInfo(symbol));
    if (pos.status !== 'open' || !pos.holdSide) return { note: 'no_open_position' };
    const holdSide = pos.holdSide;
    const marginCoin = pos.marginCoin ?? 'USDT';
    // Bitget's place-tpsl-order expects holdSide long/short in HEDGE mode but
    // buy/sell in ONE-WAY mode (live accounts run one-way; sending long/short
    // there is rejected with 43011 "holdSide error"). Default to one-way when
    // posMode is unknown — that matches production reality.
    const planHoldSide =
        pos.posMode === 'hedge_mode' ? holdSide : holdSide === 'long' ? 'buy' : 'sell';

    const meta = await fetchSymbolMeta(symbol, productType);
    const pricePlace = Number.isFinite(Number(meta.pricePlace)) ? Number(meta.pricePlace) : 2;
    const quantize = (p: number) => Number(p).toFixed(Math.max(0, pricePlace));

    let existing: PositionTpsl = { takeProfit: null, stopLoss: null };
    try {
        existing = await fetchPositionTpsl(symbol, productType);
    } catch (err) {
        console.warn(`Could not read pending TP/SL plans for ${symbol}:`, err);
    }

    const applyLeg = async (
        target: number,
        planType: 'pos_profit' | 'pos_loss',
        current: PositionTpsl['takeProfit'],
    ): Promise<TpslLegResult> => {
        const trigger = quantize(target);
        if (dryRun) return { requested: Number(trigger), applied: false, mode: 'dryRun' };
        try {
            if (current?.orderId) {
                // modify-tpsl-order REQUIRES a size (400172 without one), and it
                // must match the plan's own size semantics (both validated on
                // demo): plans created by place-tpsl-order carry size "0"
                // ("whole position") and must be modified with size "0"; plans
                // created by entry presets carry a concrete size and must be
                // modified with the CURRENT position size — echoing the plan's
                // recorded size goes stale after trims (43023 "Insufficient
                // position").
                const wholePositionPlan = current.size != null && Number(current.size) === 0;
                const modifySize = wholePositionPlan
                    ? '0'
                    : ((pos.status === 'open' ? pos.total ?? pos.available : null) ?? current.size);
                const body: any = {
                    orderId: current.orderId,
                    symbol,
                    productType: (productType as string).toUpperCase(),
                    marginCoin,
                    triggerPrice: trigger,
                    triggerType: 'mark_price',
                    executePrice: '0',
                };
                if (modifySize != null) body.size = String(modifySize);
                await bitgetFetch('POST', '/api/v2/mix/order/modify-tpsl-order', {}, body);
                return { requested: Number(trigger), applied: true, mode: 'modify' };
            }
            await bitgetFetch('POST', '/api/v2/mix/order/place-tpsl-order', {}, {
                symbol,
                productType: (productType as string).toUpperCase(),
                marginCoin,
                planType,
                triggerPrice: trigger,
                triggerType: 'mark_price',
                executePrice: '0',
                holdSide: planHoldSide,
            });
            return { requested: Number(trigger), applied: true, mode: 'place' };
        } catch (err) {
            return {
                requested: Number(trigger),
                applied: false,
                mode: current?.orderId ? 'modify' : 'place',
                error: err instanceof Error ? err.message : String(err),
            };
        }
    };

    const out: { takeProfit?: TpslLegResult; stopLoss?: TpslLegResult } = {};
    if (tpTarget != null) out.takeProfit = await applyLeg(tpTarget, 'pos_profit', existing.takeProfit);
    if (slTarget != null) out.stopLoss = await applyLeg(slTarget, 'pos_loss', existing.stopLoss);
    return out;
}

// ------------------------------
// Pending entry orders (pullback limit entries, one-tick TTL)
// ------------------------------

export type PendingEntryOrder = {
    orderId: string;
    clientOid: string | null;
    side: string | null;
    price: number | null;
    size: string | null;
    createdAtMs: number | null;
};

// The pipeline is the only writer on this account, so every pending NORMAL
// order on a symbol is one of our resting pullback entries (TP/SL live as
// plan orders, listed separately).
export async function fetchPendingEntryOrders(symbol: string, productType: ProductType): Promise<PendingEntryOrder[]> {
    const res = await bitgetFetch('GET', '/api/v2/mix/order/orders-pending', {
        symbol,
        productType: (productType as string).toUpperCase(),
    });
    const list = Array.isArray(res?.entrustedList) ? res.entrustedList : [];
    return list
        .map((o: any): PendingEntryOrder | null => {
            const orderId = o?.orderId ? String(o.orderId) : null;
            if (!orderId) return null;
            const price = Number(o?.price ?? o?.priceAvg);
            const createdAt = Number(o?.cTime ?? o?.ctime);
            return {
                orderId,
                clientOid: o?.clientOid ? String(o.clientOid) : null,
                side: o?.side ? String(o.side) : null,
                price: Number.isFinite(price) && price > 0 ? price : null,
                size: o?.size != null ? String(o.size) : null,
                createdAtMs: Number.isFinite(createdAt) && createdAt > 0 ? createdAt : null,
            };
        })
        .filter((o: PendingEntryOrder | null): o is PendingEntryOrder => o !== null);
}

// Cancel all resting entry orders for a symbol (one-tick TTL / supersede-on-
// new-evaluation). Never throws; per-order failures are surfaced so the caller
// can detect the filled-while-cancelling race via the error text.
export async function cancelPendingEntryOrders(
    symbol: string,
    productType: ProductType,
): Promise<{ found: number; cancelled: number; errors: string[]; orders: PendingEntryOrder[] }> {
    let orders: PendingEntryOrder[] = [];
    try {
        orders = await fetchPendingEntryOrders(symbol, productType);
    } catch (err) {
        return { found: 0, cancelled: 0, errors: [err instanceof Error ? err.message : String(err)], orders: [] };
    }
    let cancelled = 0;
    const errors: string[] = [];
    for (const order of orders) {
        try {
            await bitgetFetch('POST', '/api/v2/mix/order/cancel-order', {}, {
                symbol,
                productType: (productType as string).toUpperCase(),
                orderId: order.orderId,
            });
            cancelled++;
        } catch (err) {
            errors.push(err instanceof Error ? err.message : String(err));
        }
    }
    // Orders returned so the caller can tell the AI what was resting (it decides
    // fresh each evaluation: re-issue, chase with market, or drop).
    return { found: orders.length, cancelled, errors, orders };
}

// A sweep is clean only when every resting entry order is confirmed gone:
// nothing was found (and the venue actually answered), or everything found was
// cancelled. Anything else — the sweep helper threw, the pending-orders fetch
// failed (we can't even enumerate what's resting), or a cancel failed without
// the order having filled — and the previous entry order may still be live on
// the venue. Placing a NEW entry order on top of it stacks exposure, so the
// caller must fail CLOSED and skip this tick's entry (observed live: DE40
// double fill 2026-07-13). Venue-agnostic: both cancel helpers return this shape.
export type PendingEntrySweepFailure = 'sweep_exception' | 'sweep_fetch_failed' | 'sweep_cancel_failed';

export function classifyPendingEntrySweep(
    sweep: { found: number; cancelled: number; errors: string[] } | null,
): PendingEntrySweepFailure | null {
    if (!sweep) return 'sweep_exception';
    if (sweep.found === 0) return sweep.errors.length > 0 ? 'sweep_fetch_failed' : null;
    if (sweep.cancelled < sweep.found) return 'sweep_cancel_failed';
    return null;
}

type PlaceOrderBody = {
    symbol: string;
    productType: ProductType;
    marginCoin: string;
    marginMode: string;
    side: string;
    orderType: string;
    size: string;
    clientOid: string;
    force: string;
    price?: string;
    holdSide?: 'long' | 'short';
    // Bitget mix v2 expects the string 'YES'/'NO' here, not a boolean.
    reduceOnly?: 'YES' | 'NO';
    // Preset stop-loss / take-profit trigger prices (strings) attached at entry.
    // When hit, Bitget closes the position at market. Optional.
    presetStopLossPrice?: string;
    presetStopSurplusPrice?: string;
};

async function quantizePositionSize(symbol: string, productType: ProductType, rawSize: number) {
    if (!(rawSize > 0)) return 0;
    const meta = await fetchSymbolMeta(symbol, productType);
    const decimals = Number(meta.volumePlace ?? 3);
    const minTradeNum = parseFloat(meta.minTradeNum ?? '0');
    const step = parseFloat(meta.sizeMultiplier ?? `1e-${decimals}`);
    const quantizeDown = (x: number, s: number) => Math.floor(x / s) * s;
    const rounded = quantizeDown(rawSize, step);
    const finalSize = Math.max(rounded, minTradeNum);
    return Number(finalSize.toFixed(decimals));
}

// ------------------------------
// Flash Close (Bitget API)
// ------------------------------

export async function flashClosePosition(symbol: string, productType: ProductType, holdSide?: 'long' | 'short') {
    const body: any = {
        productType,
        symbol,
    };
    if (holdSide) body.holdSide = holdSide;
    const res = await bitgetFetch('POST', '/api/v2/mix/order/close-positions', {}, body);
    return res; // successList / failureList
}

// ------------------------------
// Execute Trade Decision
// ------------------------------

export function getTargetLeverage(decision: TradeDecision): number | null {
    return deriveLeverage(decision);
}

// Profit-lock margin-recycle maneuver (crypto only). Runs on HOLD and partial
// CLOSE before the normal action handling: move the stop to breakeven, then raise
// leverage (freeing isolated margin without cutting size). Two triggers:
//   - AI-requested (move_stop_to_be / raise_leverage_to), behind MIN_PROFIT_BPS;
//   - AUTO (default on): past AUTO_MIN_PROFIT_BPS the sequence is code-enforced
//     with a computed leverage target — the model proved unwilling to opt in.
// Every raise (AI or auto) is capped at recycleLeverageCeiling(): the liq price
// must stay clear of the breakeven trigger, so max margin is freed WITHOUT the
// gap-through-the-stop liquidation risk of literal exchange-max leverage.
// Returns null when the feature is off or there is nothing to manage.
// Exported for scripts/validate-bitget-tpsl.ts (demo phase F runs the real
// maneuver → trim → post-trim amend sequence end-to-end).
export async function maybeManagePosition(args: {
    symbol: string;
    productType: ProductType;
    decision: TradeDecision;
    dryRun: boolean;
    pos?: PositionInfo;
}) {
    const { symbol, productType, decision, dryRun } = args;
    if (!MARGIN_RECYCLE_ENABLED) return null;

    const wantLevRaise =
        decision.raise_leverage_to != null && Number.isFinite(Number(decision.raise_leverage_to));
    // A leverage raise ALWAYS forces a breakeven stop first — never sit at higher
    // leverage without protection. A bare move_stop_to_be is honored on its own.
    const aiRequested = decision.move_stop_to_be === true || wantLevRaise;
    if (!aiRequested && !MARGIN_RECYCLE_AUTO) return null;

    const pos = args.pos ?? (await fetchPositionInfo(symbol));
    if (pos.status !== 'open' || !pos.holdSide) {
        // Auto probes every tick — stay quiet (null) when there is nothing to
        // manage; only an explicit AI request deserves a surfaced no-op.
        return aiRequested ? { managed: false, note: 'no_open_position' } : null;
    }

    const entry = Number(pos.entryPrice);
    const mark = Number(pos.markPrice);
    const currentLev = Number(pos.leverage);
    if (!(entry > 0) || !(mark > 0) || !(currentLev > 0)) {
        return aiRequested ? { managed: false, note: 'management_missing_price_or_leverage' } : null;
    }
    const side = pos.holdSide;

    // Profit guards: an AI request needs MIN_PROFIT_BPS (a breakeven stop rested
    // on top of entry noise stops out immediately); auto engages only past the
    // wider AUTO_MIN_PROFIT_BPS cushion.
    const favMoveBps = (side === 'long' ? (mark - entry) / entry : (entry - mark) / entry) * 10000;
    const autoEngaged = MARGIN_RECYCLE_AUTO && favMoveBps >= MARGIN_RECYCLE_AUTO_MIN_PROFIT_BPS;
    if (aiRequested && !(favMoveBps >= MARGIN_RECYCLE_MIN_PROFIT_BPS)) {
        return { managed: false, note: 'profit_guard_not_met', favMoveBps: Number(favMoveBps.toFixed(2)) };
    }
    if (!aiRequested && !autoEngaged) return null;

    const meta = await fetchSymbolMeta(symbol, productType);
    const pricePlace = Number.isFinite(Number(meta.pricePlace)) ? Number(meta.pricePlace) : 2;
    const symbolMax = Number(meta.maxLever);
    const marginCoin = pos.marginCoin ?? 'USDT';

    // Breakeven trigger offset past entry by the fee buffer, so the locked exit is
    // net non-negative after round-trip venue fees.
    const feeMult = MARGIN_RECYCLE_FEE_BUFFER_BPS / 10000;
    const beTrigger = side === 'long' ? entry * (1 + feeMult) : entry * (1 - feeMult);
    const symbolCap = Number.isFinite(symbolMax) && symbolMax > 0 ? symbolMax : currentLev;
    const clampMax = Math.min(symbolCap, recycleLeverageCeiling());
    // Auto raises to the liq-safe ceiling (max margin freed); an AI target is
    // honored but never above that ceiling.
    const desiredLev = autoEngaged
        ? Math.max(wantLevRaise ? Number(decision.raise_leverage_to) : 0, recycleLeverageCeiling())
        : wantLevRaise
          ? Number(decision.raise_leverage_to)
          : null;

    // Never loosen protection: auto re-runs every tick, so once the stop has been
    // trailed TIGHTER than breakeven (by the AI or a previous maneuver), resting
    // the BE trigger again would move it backwards. Skip the BE leg then — the
    // tighter standing stop already satisfies the raise-needs-protection rule.
    const beTriggerQ = Number(beTrigger.toFixed(Math.max(0, pricePlace)));
    let existingStop: number | null = null;
    try {
        existingStop = (await fetchPositionTpsl(symbol, productType)).stopLoss?.price ?? null;
    } catch (err) {
        console.warn(`Could not read standing stop for ${symbol} before BE move:`, err);
    }
    const stopAlreadyTighter =
        existingStop != null &&
        (side === 'long' ? existingStop >= beTriggerQ : existingStop <= beTriggerQ);
    const wantBE = (aiRequested || autoEngaged) && !stopAlreadyTighter;

    if (dryRun) {
        return {
            managed: true,
            dryRun: true,
            side,
            entry,
            mark,
            auto: autoEngaged,
            beStop: { plannedTrigger: beTriggerQ },
            // Normalized BE trigger for the post-maneuver stop-amend guard
            // (pickTighterStop) — present whenever the maneuver owns the stop.
            beTriggerPrice: wantBE ? beTriggerQ : null,
            plannedLeverage:
                desiredLev != null ? clampManagementLeverage(desiredLev, currentLev, clampMax) : null,
            currentLeverage: currentLev,
            ...(stopAlreadyTighter ? { note: 'stop_already_tighter_than_be' } : {}),
        };
    }

    // 1) Breakeven stop FIRST. If it fails, abort the leverage raise entirely.
    let beResult: Awaited<ReturnType<typeof setPositionBreakevenStop>> | null = null;
    if (wantBE) {
        beResult = await setPositionBreakevenStop({
            symbol,
            productType,
            triggerPrice: beTrigger,
            pos,
        });
        if (!beResult.ok) {
            return {
                managed: false,
                auto: autoEngaged,
                beStop: beResult,
                leverageRaised: false,
                note: 'be_stop_failed_leverage_skipped',
            };
        }
    }

    // 2) Only now raise leverage — on isolated margin this releases freed margin
    // back to the available balance for future positions, keeping notional intact.
    let leverageRaised = false;
    let newLeverage = currentLev;
    let leverageError: string | undefined;
    if (desiredLev != null) {
        const targetLev = clampManagementLeverage(desiredLev, currentLev, clampMax);
        if (targetLev) {
            try {
                await postSetLeverage(symbol, productType, targetLev, side);
                leverageRaised = true;
                newLeverage = targetLev;
            } catch (err) {
                leverageError = err instanceof Error ? err.message : String(err);
            }
        }
    }

    return {
        managed: true,
        side,
        auto: autoEngaged,
        beStop: beResult,
        // Normalized BE trigger for the post-maneuver stop-amend guard
        // (pickTighterStop) — set only when the BE stop actually rests. When the
        // BE leg was skipped for an already-tighter stop this stays null and the
        // route's tighten-only sanitize keeps guarding the standing stop.
        beTriggerPrice: beResult?.ok ? beResult.triggerPrice : null,
        leverageRaised,
        // Surfaced so extractCapturedLeverages records the post-raise leverage on
        // this decision's history entry, keeping the captured-leverage timeline true.
        leverage: newLeverage,
        currentLeverage: currentLev,
        ...(stopAlreadyTighter ? { note: 'stop_already_tighter_than_be' } : {}),
        ...(leverageError ? { leverageError } : {}),
    };
}

export async function executeDecision(
    symbol: string,
    sideSizeUSDT: number,
    decision: TradeDecision,
    productType: ProductType,
    dryRun = true,
    stopLossPrice: number | null = null,
    takeProfitPrice: number | null = null,
) {
    const clientOid = `cfw-${crypto.randomUUID()}`;
    const partialClosePct =
        normalizeClosePct(decision.exit_size_pct) ??
        normalizeClosePct((decision as any).close_size_pct) ??
        normalizeClosePct((decision as any).partial_close_pct);
    const targetLeverage = deriveLeverage(decision);

    // BUY / SELL
    if (decision.action === 'BUY' || decision.action === 'SELL') {
        const holdSide: 'long' | 'short' = decision.action === 'BUY' ? 'long' : 'short';
        const entryLimitPrice =
            Number.isFinite(decision.entry_limit_price as number) && (decision.entry_limit_price as number) > 0
                ? Number(decision.entry_limit_price)
                : null;
        const leverageResult = await applyLeverage({ symbol, productType, leverage: targetLeverage, holdSide, dryRun });
        if (dryRun) return { placed: false, orderId: null, clientOid, leverage: leverageResult.leverage };
        const orderNotionalUSDT = deriveOrderNotional(sideSizeUSDT, targetLeverage);
        const size = await computeOrderSize(symbol, orderNotionalUSDT, productType);

        const body: PlaceOrderBody = {
            symbol,
            productType,
            marginCoin: 'USDT',
            marginMode: 'isolated',
            side: decision.action.toLowerCase(),
            // Pullback limit entry: rest at the sanitized limit instead of
            // taking the market. The caller cancels it at the next evaluation
            // if unfilled (one-tick TTL).
            orderType: entryLimitPrice != null ? 'limit' : 'market',
            size: size.toString(),
            clientOid,
            force: 'gtc',
        };
        // In hedge mode Bitget expects holdSide to distinguish open vs reduce
        if (decision.action === 'BUY') body['holdSide'] = 'long';
        if (decision.action === 'SELL') body['holdSide'] = 'short';
        // Attach the exchange-side bracket at entry: a protective (catastrophe)
        // stop plus a resting take-profit, so the position is bounded — and the
        // upside captured — during the gap between AI evaluations. Both are
        // sized and placed on the correct side of entry by the caller (anchored
        // at the limit price for pullback entries); here we just quantize them
        // to the symbol's price precision.
        const hasSl = Number.isFinite(stopLossPrice as number) && (stopLossPrice as number) > 0;
        const hasTp = Number.isFinite(takeProfitPrice as number) && (takeProfitPrice as number) > 0;
        if (hasSl || hasTp || entryLimitPrice != null) {
            const meta = await fetchSymbolMeta(symbol, productType);
            const pricePlace = Number.isFinite(Number(meta.pricePlace)) ? Number(meta.pricePlace) : 2;
            if (hasSl) body['presetStopLossPrice'] = Number(stopLossPrice).toFixed(Math.max(0, pricePlace));
            if (hasTp) body['presetStopSurplusPrice'] = Number(takeProfitPrice).toFixed(Math.max(0, pricePlace));
            if (entryLimitPrice != null) body['price'] = entryLimitPrice.toFixed(Math.max(0, pricePlace));
        }
        const res = await bitgetFetch('POST', '/api/v2/mix/order/place-order', {}, body);
        // Verify the fill's actual leverage (market entries only — a resting
        // limit has no position yet). A mismatch means set-leverage landed on
        // the wrong slot (see ensureIsolatedMarginMode); surfaced, not fatal,
        // and the actual value wins so the captured-leverage timeline stays true.
        let actualLeverage: number | null = null;
        if (entryLimitPrice == null) {
            try {
                const posAfter = await fetchPositionInfo(symbol);
                if (posAfter.status === 'open') actualLeverage = Number(posAfter.leverage) || null;
            } catch {
                // best-effort — the entry itself succeeded
            }
        }
        return {
            placed: true,
            orderId: res?.orderId || res?.order_id || null,
            clientOid,
            leverage: actualLeverage ?? leverageResult.leverage,
            leverageApplied: leverageResult.applied,
            leverageError: leverageResult.error,
            ...(actualLeverage != null && targetLeverage != null && actualLeverage !== targetLeverage
                ? { leverageMismatch: true, targetLeverage }
                : {}),
            ...(entryLimitPrice != null ? { pendingEntry: true, entryLimitPrice } : {}),
        };
    }

    if (decision.action === 'REVERSE') {
        if (dryRun) return { placed: false, orderId: null, clientOid };
        const pos = await fetchPositionInfo(symbol);
        if (pos.status === 'none') {
            return { placed: false, orderId: null, clientOid, note: 'no open position to reverse' };
        }
        const oppositeSide: 'long' | 'short' = pos.holdSide === 'long' ? 'short' : 'long';

        // Partial reverse support (one-way mode): trim a percent, then flip that trimmed size
        if (partialClosePct !== null && partialClosePct < 100 && pos.posMode !== 'hedge_mode') {
            try {
                const posSize = Number(pos.total ?? pos.available);
                if (Number.isFinite(posSize) && posSize > 0) {
                    const closeSize = await quantizePositionSize(symbol, productType, posSize * (partialClosePct / 100));
                    if (closeSize > 0) {
                        const leverageResult = await applyLeverage({
                            symbol,
                            productType,
                            leverage: targetLeverage,
                            holdSide: oppositeSide,
                            dryRun,
                        });
                        const closeSide = pos.holdSide === 'long' ? 'sell' : 'buy';
                        const closeBody = {
                            symbol,
                            productType,
                            marginCoin: pos.marginCoin ?? 'USDT',
                            marginMode: 'isolated',
                            side: closeSide,
                            orderType: 'market',
                            size: closeSize.toString(),
                            clientOid: `${clientOid}-close`,
                            force: 'gtc',
                            // Bitget expects the string 'YES'/'NO', not a boolean.
                            reduceOnly: 'YES',
                        };
                        const closeRes = await bitgetFetch('POST', '/api/v2/mix/order/place-order', {}, closeBody);

                        // Open opposite side with the same size; in one-way mode this nets out/overturns
                        const openBody = {
                            symbol,
                            productType,
                            marginCoin: pos.marginCoin ?? 'USDT',
                            marginMode: 'isolated',
                            side: closeSide,
                            orderType: 'market',
                            size: closeSize.toString(),
                            clientOid: `${clientOid}-open`,
                            force: 'gtc',
                        };
                        const openRes = await bitgetFetch('POST', '/api/v2/mix/order/place-order', {}, openBody);

                        return {
                            placed: true,
                            orderId: openRes?.orderId || openRes?.order_id || null,
                            clientOid,
                            reversed: true,
                            partial: true,
                            partialClosePct,
                            closeSize,
                            openSize: closeSize,
                            targetSide: oppositeSide,
                            leverage: leverageResult.leverage,
                            leverageApplied: leverageResult.applied,
                            leverageError: leverageResult.error,
                            raw: { close: closeRes, open: openRes },
                        };
                    }
                }
            } catch (err) {
                console.warn('Partial reverse failed, falling back to full reverse:', err);
            }
        }

        const targetSide: 'long' | 'short' = pos.holdSide === 'long' ? 'short' : 'long';
        const leverageResult = await applyLeverage({
            symbol,
            productType,
            leverage: targetLeverage,
            holdSide: targetSide,
            dryRun,
        });

        const closeRes = await flashClosePosition(
            symbol,
            productType,
            pos.posMode === 'hedge_mode' ? pos.holdSide : undefined,
        );
        const closeOk = Array.isArray(closeRes?.successList) && closeRes.successList.length > 0;
        if (!closeOk) {
            return { placed: false, orderId: null, clientOid, note: 'failed to close before reverse' };
        }
        const orderNotionalUSDT = deriveOrderNotional(sideSizeUSDT, targetLeverage);
        const size = await computeOrderSize(symbol, orderNotionalUSDT, productType);
        const body: PlaceOrderBody = {
            symbol,
            productType,
            marginCoin: 'USDT',
            marginMode: 'isolated',
            side: oppositeSide === 'long' ? 'buy' : 'sell',
            orderType: 'market',
            size: size.toString(),
            clientOid,
            force: 'gtc',
        };
        body['holdSide'] = oppositeSide;
        // The reversed position gets the same entry bracket as BUY/SELL — the
        // caller computes stop/TP for the NEW side. (The partial-reverse path
        // above intentionally does not attach presets: its netting open order
        // has murkier semantics, and the prompt mandates full reverses anyway.)
        const revHasSl = Number.isFinite(stopLossPrice as number) && (stopLossPrice as number) > 0;
        const revHasTp = Number.isFinite(takeProfitPrice as number) && (takeProfitPrice as number) > 0;
        if (revHasSl || revHasTp) {
            const meta = await fetchSymbolMeta(symbol, productType);
            const pricePlace = Number.isFinite(Number(meta.pricePlace)) ? Number(meta.pricePlace) : 2;
            if (revHasSl) body['presetStopLossPrice'] = Number(stopLossPrice).toFixed(Math.max(0, pricePlace));
            if (revHasTp) body['presetStopSurplusPrice'] = Number(takeProfitPrice).toFixed(Math.max(0, pricePlace));
        }
        const res = await bitgetFetch('POST', '/api/v2/mix/order/place-order', {}, body);
        return {
            placed: true,
            orderId: res?.orderId || res?.order_id || null,
            clientOid,
            reversed: true,
            size,
            targetSide: oppositeSide,
            leverage: leverageResult.leverage,
            leverageApplied: leverageResult.applied,
            leverageError: leverageResult.error,
        };
    }

    // CLOSE
    if (decision.action === 'CLOSE') {
        if (dryRun) {
            return { placed: false, orderId: null, clientOid, closed: true, partialClosePct };
        }

        const pos = await fetchPositionInfo(symbol);
        if (pos.status === 'none') {
            return { placed: false, orderId: null, clientOid, closed: false, note: 'no open position' };
        }

        // Partial close (reduce-only) when requested. A partial close must NEVER
        // escalate to a full close — that would defeat "trim X% and let the rest
        // run". On any failure we surface it and leave the position intact rather
        // than flash-closing everything.
        //
        // Order of operations (profit-recycle sequence): 1) management — BE stop
        // then leverage raise (maybeManagePosition owns that internal order and
        // aborts the raise if the BE stop fails) → 2) reduceOnly trim → 3) bracket
        // amend against the POST-TRIM position (modify-tpsl needs the current
        // size; amending before the trim leaves a stale-size plan → 43023). The
        // AI's stop amend is applied only when it tightens past the maneuver's
        // BE trigger (pickTighterStop) — never loosening the just-rested floor.
        if (partialClosePct !== null && partialClosePct < 100) {
            const trimMgmt = await maybeManagePosition({ symbol, productType, decision, dryRun, pos });
            const trimMgmtLev = trimMgmt && (trimMgmt as any).managed ? (trimMgmt as any).leverage : undefined;
            const beTriggerPrice =
                trimMgmt && (trimMgmt as any).managed ? ((trimMgmt as any).beTriggerPrice ?? null) : null;
            // Bracket amend targets the remainder that keeps running, so it runs
            // even when the trim itself fails or is skipped (protective either
            // way) — with the freshest position snapshot available.
            const amendBracket = async (posForAmend: PositionInfo) => {
                const guardedStop = pos.holdSide
                    ? pickTighterStop(pos.holdSide, beTriggerPrice, decision.stop_loss_price ?? null)
                    : (decision.stop_loss_price ?? null);
                if (decision.take_profit_price == null && guardedStop == null) return null;
                return updatePositionTpsl({
                    symbol,
                    productType,
                    takeProfitPrice: decision.take_profit_price ?? null,
                    stopLossPrice: guardedStop,
                    dryRun,
                    pos: posForAmend,
                });
            };
            const mgmtFields = (tpsl: Awaited<ReturnType<typeof amendBracket>>) => ({
                ...(trimMgmt ? { management: trimMgmt } : {}),
                ...(tpsl ? { tpsl } : {}),
                ...(Number.isFinite(Number(trimMgmtLev)) && Number(trimMgmtLev) > 0
                    ? { leverage: Number(trimMgmtLev) }
                    : {}),
            });
            const posSize = Number(pos.total ?? pos.available);
            if (!(Number.isFinite(posSize) && posSize > 0 && pos.holdSide)) {
                return {
                    placed: false,
                    orderId: null,
                    clientOid,
                    closed: false,
                    note: 'partial_close_unknown_position_size',
                    ...mgmtFields(await amendBracket(pos)),
                };
            }
            const targetSize = await quantizePositionSize(symbol, productType, posSize * (partialClosePct / 100));
            if (!(targetSize > 0 && targetSize < posSize)) {
                return {
                    placed: false,
                    orderId: null,
                    clientOid,
                    closed: false,
                    note: 'partial_close_size_out_of_range',
                    partialClosePct,
                    targetSize,
                    posSize,
                    ...mgmtFields(await amendBracket(pos)),
                };
            }
            const partialIsHedge = pos.posMode === 'hedge_mode';
            const body: any = {
                symbol,
                productType,
                marginCoin: pos.marginCoin ?? 'USDT',
                marginMode: 'isolated',
                orderType: 'market',
                size: targetSize.toString(),
                clientOid,
                force: 'gtc',
            };
            if (partialIsHedge) {
                // Hedge mode reduces via tradeSide=close (no reduceOnly flag).
                body.side = pos.holdSide === 'long' ? 'buy' : 'sell';
                body.tradeSide = 'close';
                body.holdSide = pos.holdSide;
            } else {
                // One-way mode: opposite side + reduceOnly. Bitget expects the
                // string 'YES'/'NO' here, NOT a boolean (a boolean is rejected
                // with error 40017 REDUCEONLY).
                body.side = pos.holdSide === 'long' ? 'sell' : 'buy';
                body.reduceOnly = 'YES';
            }
            try {
                const res = await bitgetFetch('POST', '/api/v2/mix/order/place-order', {}, body);
                // Trim placed: amend against the post-trim position so the
                // modify-tpsl size is fresh. If the refetch shows the position
                // gone (stop/TP filled mid-trim), skip the amend — there is
                // nothing left to bracket.
                const postTrimPos = await fetchPositionInfo(symbol).catch(() => null);
                const trimTpsl =
                    postTrimPos && postTrimPos.status === 'open' ? await amendBracket(postTrimPos) : null;
                return {
                    placed: true,
                    orderId: res?.orderId || res?.order_id || null,
                    clientOid,
                    closed: true,
                    partial: true,
                    partialClosePct,
                    size: targetSize,
                    raw: res,
                    ...mgmtFields(trimTpsl),
                };
            } catch (err) {
                // Leave the position intact: the request was to keep the
                // remainder open, so a failed trim must not become a full close.
                console.warn('Partial close order failed (position left intact):', err);
                return {
                    placed: false,
                    orderId: null,
                    clientOid,
                    closed: false,
                    partial: true,
                    partialClosePct,
                    size: targetSize,
                    note: 'partial_close_failed',
                    error: err instanceof Error ? err.message : String(err),
                    ...mgmtFields(await amendBracket(pos)),
                };
            }
        }

        const isHedge = pos.posMode === 'hedge_mode';
        const res = await flashClosePosition(symbol, productType, isHedge ? pos.holdSide : undefined);
        const ok = Array.isArray(res?.successList) && res.successList.length > 0;
        const orderId = ok ? res.successList[0]?.orderId ?? null : null;
        return { placed: ok, orderId, clientOid, closed: ok, raw: res };
    }

    // HOLD (+ optional in-place profit-lock / margin-recycle management for crypto,
    // + exchange-side TP/SL bracket amend when the decision carries new levels).
    // When the maneuver just rested a BE stop, the AI's stop amend applies only
    // if it tightens past that trigger (pickTighterStop) — a tighter structural
    // stop is welcome, a looser one must not undo the floor.
    const holdMgmt = await maybeManagePosition({ symbol, productType, decision, dryRun });
    const holdMgmtLev = holdMgmt && (holdMgmt as any).managed ? (holdMgmt as any).leverage : undefined;
    const holdBeTrigger =
        holdMgmt && (holdMgmt as any).managed ? ((holdMgmt as any).beTriggerPrice ?? null) : null;
    const holdMgmtSide = holdMgmt && (holdMgmt as any).managed ? ((holdMgmt as any).side ?? null) : null;
    const holdGuardedStop =
        holdBeTrigger != null && (holdMgmtSide === 'long' || holdMgmtSide === 'short')
            ? pickTighterStop(holdMgmtSide, holdBeTrigger, decision.stop_loss_price ?? null)
            : (decision.stop_loss_price ?? null);
    const holdTpsl =
        decision.take_profit_price != null || holdGuardedStop != null
            ? await updatePositionTpsl({
                  symbol,
                  productType,
                  takeProfitPrice: decision.take_profit_price ?? null,
                  stopLossPrice: holdGuardedStop,
                  dryRun,
              })
            : null;
    return {
        placed: false,
        orderId: null,
        clientOid,
        ...(holdMgmt ? { management: holdMgmt } : {}),
        ...(holdTpsl ? { tpsl: holdTpsl } : {}),
        ...(Number.isFinite(Number(holdMgmtLev)) && Number(holdMgmtLev) > 0 ? { leverage: Number(holdMgmtLev) } : {}),
    };
}

// ------------------------------
// Product Type Resolver
// ------------------------------

export function getTradeProductType(): ProductType {
    return resolveProductType();
}
