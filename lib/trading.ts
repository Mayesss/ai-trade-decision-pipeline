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
}

// Profit-lock margin-recycle feature flags (crypto only). Ships OFF; the Bitget
// TP/SL endpoint used to move the stop must be validated on testnet before enabling.
const MARGIN_RECYCLE_ENABLED = process.env.ENABLE_CRYPTO_MARGIN_RECYCLE === 'true';
// Breakeven stop sits this many bps past entry so the locked exit is net
// non-negative after round-trip venue fees (~0.30R).
const MARGIN_RECYCLE_FEE_BUFFER_BPS = Number(process.env.CRYPTO_BE_STOP_FEE_BUFFER_BPS ?? 8);
// Only manage once price has moved at least this far past entry in the favorable
// direction — avoids setting a breakeven stop while price still hugs entry
// (instant/whipsaw stop-out).
const MARGIN_RECYCLE_MIN_PROFIT_BPS = Number(process.env.CRYPTO_MARGIN_RECYCLE_MIN_PROFIT_BPS ?? 40);

function normalizeClosePct(pct: unknown) {
    const n = Number(pct);
    if (!Number.isFinite(n)) return null;
    const clamped = Math.max(0, Math.min(100, n));
    return clamped > 0 ? clamped : null;
}

function clampLeverage(value: unknown): number | null {
    const n = Number(value);
    if (!Number.isFinite(n)) return null;
    const rounded = Math.round(n);
    const clamped = Math.max(1, Math.min(5, rounded));
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
        if (strength === 'HIGH') return 4;
        if (strength === 'MEDIUM') return 3;
        if (strength === 'LOW') return 1;
    }
    return null;
}

function deriveOrderNotional(sideSizeUSDT: number, leverage: number | null): number {
    const lev = clampLeverage(leverage) ?? 1;
    return sideSizeUSDT * lev;
}

// Raw set-leverage POST — no clamping. Callers own the clamp (1–5 at entry via
// applyLeverage; up to symbol max for the profit-lock raise via
// clampManagementLeverage), so this helper stays policy-free.
async function postSetLeverage(symbol: string, productType: ProductType, leverage: number, holdSide?: 'long' | 'short') {
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

    try {
        const res = await postSetLeverage(symbol, productType, target, holdSide);
        return { applied: true, leverage: target, raw: res };
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

// Move a position's stop-loss to (just past) breakeven via Bitget's position-level
// TP/SL loss plan. This supersedes the entry presetStopLossPrice for the position.
// planType 'pos_loss' targets the whole position; executePrice '0' = close at market.
// NOTE: exact place-vs-modify semantics of this endpoint must be verified on
// testnet before enabling in production (see ENABLE_CRYPTO_MARGIN_RECYCLE).
async function setPositionBreakevenStop(params: {
    symbol: string;
    productType: ProductType;
    holdSide: 'long' | 'short';
    triggerPrice: number;
    marginCoin: string;
    pricePlace: number;
}): Promise<{ ok: boolean; triggerPrice: number; raw?: any; error?: string }> {
    const { symbol, productType, holdSide, triggerPrice, marginCoin, pricePlace } = params;
    const trigger = Number(triggerPrice.toFixed(Math.max(0, pricePlace)));
    const body: any = {
        symbol,
        productType: (productType as string).toUpperCase(),
        marginCoin,
        planType: 'pos_loss',
        triggerPrice: trigger.toString(),
        triggerType: 'mark_price',
        executePrice: '0',
        holdSide,
    };
    try {
        const raw = await bitgetFetch('POST', '/api/v2/mix/order/place-tpsl-order', {}, body);
        return { ok: true, triggerPrice: trigger, raw };
    } catch (err) {
        return { ok: false, triggerPrice: trigger, error: err instanceof Error ? err.message : String(err) };
    }
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
                const body: any = {
                    orderId: current.orderId,
                    symbol,
                    productType: (productType as string).toUpperCase(),
                    marginCoin,
                    triggerPrice: trigger,
                    triggerType: 'mark_price',
                    executePrice: '0',
                };
                if (current.size != null) body.size = current.size;
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
                holdSide,
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
// leverage toward the symbol max (freeing isolated margin without cutting size).
// Returns null when the feature is off or the decision requests no management.
async function maybeManagePosition(args: {
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
    const wantBE = decision.move_stop_to_be === true || wantLevRaise;
    if (!wantBE && !wantLevRaise) return null;

    const pos = args.pos ?? (await fetchPositionInfo(symbol));
    if (pos.status !== 'open' || !pos.holdSide) return { managed: false, note: 'no_open_position' };

    const entry = Number(pos.entryPrice);
    const mark = Number(pos.markPrice);
    const currentLev = Number(pos.leverage);
    if (!(entry > 0) || !(mark > 0) || !(currentLev > 0)) {
        return { managed: false, note: 'management_missing_price_or_leverage' };
    }
    const side = pos.holdSide;

    // Profit guard: only manage once price has moved past entry by at least
    // MIN_PROFIT_BPS in the favorable direction, so a breakeven stop won't trigger
    // immediately (or whipsaw) on noise around entry.
    const favMoveBps = (side === 'long' ? (mark - entry) / entry : (entry - mark) / entry) * 10000;
    if (!(favMoveBps >= MARGIN_RECYCLE_MIN_PROFIT_BPS)) {
        return { managed: false, note: 'profit_guard_not_met', favMoveBps: Number(favMoveBps.toFixed(2)) };
    }

    const meta = await fetchSymbolMeta(symbol, productType);
    const pricePlace = Number.isFinite(Number(meta.pricePlace)) ? Number(meta.pricePlace) : 2;
    const symbolMax = Number(meta.maxLever);
    const marginCoin = pos.marginCoin ?? 'USDT';

    // Breakeven trigger offset past entry by the fee buffer, so the locked exit is
    // net non-negative after round-trip venue fees.
    const feeMult = MARGIN_RECYCLE_FEE_BUFFER_BPS / 10000;
    const beTrigger = side === 'long' ? entry * (1 + feeMult) : entry * (1 - feeMult);
    const clampMax = Number.isFinite(symbolMax) && symbolMax > 0 ? symbolMax : currentLev;

    if (dryRun) {
        return {
            managed: true,
            dryRun: true,
            side,
            entry,
            mark,
            beStop: { plannedTrigger: Number(beTrigger.toFixed(Math.max(0, pricePlace))) },
            plannedLeverage: wantLevRaise
                ? clampManagementLeverage(decision.raise_leverage_to, currentLev, clampMax)
                : null,
            currentLeverage: currentLev,
        };
    }

    // 1) Breakeven stop FIRST. If it fails, abort the leverage raise entirely.
    let beResult: Awaited<ReturnType<typeof setPositionBreakevenStop>> | null = null;
    if (wantBE) {
        beResult = await setPositionBreakevenStop({
            symbol,
            productType,
            holdSide: side,
            triggerPrice: beTrigger,
            marginCoin,
            pricePlace,
        });
        if (!beResult.ok) {
            return { managed: false, beStop: beResult, leverageRaised: false, note: 'be_stop_failed_leverage_skipped' };
        }
    }

    // 2) Only now raise leverage — on isolated margin this releases freed margin
    // back to the available balance for future positions, keeping notional intact.
    let leverageRaised = false;
    let newLeverage = currentLev;
    let leverageError: string | undefined;
    if (wantLevRaise) {
        const targetLev = clampManagementLeverage(decision.raise_leverage_to, currentLev, clampMax);
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
        beStop: beResult,
        leverageRaised,
        // Surfaced so extractCapturedLeverages records the post-raise leverage on
        // this decision's history entry, keeping the captured-leverage timeline true.
        leverage: newLeverage,
        currentLeverage: currentLev,
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
            orderType: 'market',
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
        // sized and placed on the correct side of entry by the caller; here we
        // just quantize them to the symbol's price precision.
        const hasSl = Number.isFinite(stopLossPrice as number) && (stopLossPrice as number) > 0;
        const hasTp = Number.isFinite(takeProfitPrice as number) && (takeProfitPrice as number) > 0;
        if (hasSl || hasTp) {
            const meta = await fetchSymbolMeta(symbol, productType);
            const pricePlace = Number.isFinite(Number(meta.pricePlace)) ? Number(meta.pricePlace) : 2;
            if (hasSl) body['presetStopLossPrice'] = Number(stopLossPrice).toFixed(Math.max(0, pricePlace));
            if (hasTp) body['presetStopSurplusPrice'] = Number(takeProfitPrice).toFixed(Math.max(0, pricePlace));
        }
        const res = await bitgetFetch('POST', '/api/v2/mix/order/place-order', {}, body);
        return {
            placed: true,
            orderId: res?.orderId || res?.order_id || null,
            clientOid,
            leverage: leverageResult.leverage,
            leverageApplied: leverageResult.applied,
            leverageError: leverageResult.error,
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
        if (partialClosePct !== null && partialClosePct < 100) {
            // Profit-lock management runs BEFORE the trim: raise the stop to
            // breakeven and (optionally) leverage first, so the remainder that
            // keeps running is protected. Reuses the already-fetched position.
            const trimMgmt = await maybeManagePosition({ symbol, productType, decision, dryRun, pos });
            const trimMgmtLev = trimMgmt && (trimMgmt as any).managed ? (trimMgmt as any).leverage : undefined;
            // Bracket amend targets the remainder that keeps running, so it runs
            // even if the trim order itself fails below (protective either way).
            // When the margin-recycle BE stop just moved the stop this tick, the
            // AI's stop amend is skipped rather than fighting it.
            const trimBeMoved = Boolean(trimMgmt && (trimMgmt as any).managed && decision.move_stop_to_be === true);
            const trimTpsl =
                decision.take_profit_price != null || decision.stop_loss_price != null
                    ? await updatePositionTpsl({
                          symbol,
                          productType,
                          takeProfitPrice: decision.take_profit_price ?? null,
                          stopLossPrice: trimBeMoved ? null : decision.stop_loss_price ?? null,
                          dryRun,
                          pos,
                      })
                    : null;
            const mgmtFields = {
                ...(trimMgmt ? { management: trimMgmt } : {}),
                ...(trimTpsl ? { tpsl: trimTpsl } : {}),
                ...(Number.isFinite(Number(trimMgmtLev)) && Number(trimMgmtLev) > 0
                    ? { leverage: Number(trimMgmtLev) }
                    : {}),
            };
            const posSize = Number(pos.total ?? pos.available);
            if (!(Number.isFinite(posSize) && posSize > 0 && pos.holdSide)) {
                return {
                    placed: false,
                    orderId: null,
                    clientOid,
                    closed: false,
                    note: 'partial_close_unknown_position_size',
                    ...mgmtFields,
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
                    ...mgmtFields,
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
                return {
                    placed: true,
                    orderId: res?.orderId || res?.order_id || null,
                    clientOid,
                    closed: true,
                    partial: true,
                    partialClosePct,
                    size: targetSize,
                    raw: res,
                    ...mgmtFields,
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
                    ...mgmtFields,
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
    // + exchange-side TP/SL bracket amend when the decision carries new levels)
    const holdMgmt = await maybeManagePosition({ symbol, productType, decision, dryRun });
    const holdMgmtLev = holdMgmt && (holdMgmt as any).managed ? (holdMgmt as any).leverage : undefined;
    const holdBeMoved = Boolean(holdMgmt && (holdMgmt as any).managed && decision.move_stop_to_be === true);
    const holdTpsl =
        decision.take_profit_price != null || decision.stop_loss_price != null
            ? await updatePositionTpsl({
                  symbol,
                  productType,
                  takeProfitPrice: decision.take_profit_price ?? null,
                  stopLossPrice: holdBeMoved ? null : decision.stop_loss_price ?? null,
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
