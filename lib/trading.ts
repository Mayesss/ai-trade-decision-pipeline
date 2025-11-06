// lib/trading.ts

import { bitgetFetch, resolveProductType } from './bitget';
import type { ProductType } from './bitget';

import { computeOrderSize, fetchPositionInfo } from './analytics';

// ------------------------------
// Types
// ------------------------------

export interface TradeDecision {
    action: 'BUY' | 'SELL' | 'HOLD' | 'CLOSE';
    summary: string;
    reason: string;
    timestamp?: number;
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
        const res = await bitgetFetch('POST', '/api/v2/mix/order/place-order', {}, body);
        return { placed: true, orderId: res?.orderId || res?.order_id || null, clientOid };
    }

    // CLOSE
    if (decision.action === 'CLOSE') {
        if (dryRun) return { placed: false, orderId: null, clientOid, closed: true };

        const pos = await fetchPositionInfo(symbol);
        if (pos.status === 'none') {
            return { placed: false, orderId: null, clientOid, closed: false, note: 'no open position' };
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
