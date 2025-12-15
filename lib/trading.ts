// lib/trading.ts

import crypto from 'crypto';
import { bitgetFetch, resolveProductType } from './bitget';
import type { ProductType } from './bitget';

import { computeOrderSize, fetchPositionInfo, fetchSymbolMeta } from './analytics';

// ------------------------------
// Types
// ------------------------------

export interface TradeDecision {
    action: 'BUY' | 'SELL' | 'HOLD' | 'CLOSE' | 'REVERSE';
    summary: string;
    reason: string;
    timestamp?: number;
    exit_size_pct?: number | null;
    close_size_pct?: number | null;
    target_position_pct?: number | null;
}

function normalizeClosePct(pct: unknown) {
    const n = Number(pct);
    if (!Number.isFinite(n)) return null;
    const clamped = Math.max(0, Math.min(100, n));
    return clamped > 0 ? clamped : null;
}

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

export async function executeDecision(
    symbol: string,
    sideSizeUSDT: number,
    decision: TradeDecision,
    productType: ProductType,
    dryRun = true,
) {
    const clientOid = `cfw-${crypto.randomUUID()}`;
    const partialClosePct =
        normalizeClosePct(decision.exit_size_pct) ??
        normalizeClosePct((decision as any).close_size_pct) ??
        normalizeClosePct((decision as any).partial_close_pct);

    // BUY / SELL
    if (decision.action === 'BUY' || decision.action === 'SELL') {
        if (dryRun) return { placed: false, orderId: null, clientOid };
        const size = await computeOrderSize(symbol, sideSizeUSDT, productType);

        const body = {
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
        const res = await bitgetFetch('POST', '/api/v2/mix/order/place-order', {}, body);
        return { placed: true, orderId: res?.orderId || res?.order_id || null, clientOid };
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
                            reduceOnly: true,
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
                            raw: { close: closeRes, open: openRes },
                        };
                    }
                }
            } catch (err) {
                console.warn('Partial reverse failed, falling back to full reverse:', err);
            }
        }

        const closeRes = await flashClosePosition(
            symbol,
            productType,
            pos.posMode === 'hedge_mode' ? pos.holdSide : undefined,
        );
        const closeOk = Array.isArray(closeRes?.successList) && closeRes.successList.length > 0;
        if (!closeOk) {
            return { placed: false, orderId: null, clientOid, note: 'failed to close before reverse' };
        }
        const size = await computeOrderSize(symbol, sideSizeUSDT, productType);
        const body = {
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
        const res = await bitgetFetch('POST', '/api/v2/mix/order/place-order', {}, body);
        return {
            placed: true,
            orderId: res?.orderId || res?.order_id || null,
            clientOid,
            reversed: true,
            size,
            targetSide: oppositeSide,
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

        // Allow partial close (reduce-only) when requested and we know the position size
        if (partialClosePct !== null && partialClosePct < 100) {
            try {
                const posSize = Number(pos.total ?? pos.available);
                if (Number.isFinite(posSize) && posSize > 0 && pos.holdSide) {
                    const targetSize = await quantizePositionSize(symbol, productType, posSize * (partialClosePct / 100));
                    if (targetSize > 0 && targetSize < posSize) {
                        const side = pos.holdSide === 'long' ? 'sell' : 'buy';
                        const body: any = {
                            symbol,
                            productType,
                            marginCoin: pos.marginCoin ?? 'USDT',
                            marginMode: 'isolated',
                            side,
                            orderType: 'market',
                            size: targetSize.toString(),
                            clientOid,
                            force: 'gtc',
                            reduceOnly: true,
                        };
                        if (pos.posMode === 'hedge_mode') {
                            body.holdSide = pos.holdSide;
                        }
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
                        };
                    }
                }
            } catch (err) {
                // fall through to full flash close if reduce-only order fails
                console.warn('Partial close failed, falling back to full flash close:', err);
            }
        }

        const isHedge = pos.posMode === 'hedge_mode';
        const res = await flashClosePosition(symbol, productType, isHedge ? pos.holdSide : undefined);
        const ok = Array.isArray(res?.successList) && res.successList.length > 0;
        const orderId = ok ? res.successList[0]?.orderId ?? null : null;
        return { placed: ok, orderId, clientOid, closed: ok, raw: res };
    }

    // HOLD
    return { placed: false, orderId: null, clientOid };
}

// ------------------------------
// Product Type Resolver
// ------------------------------

export function getTradeProductType(): ProductType {
    return resolveProductType();
}
