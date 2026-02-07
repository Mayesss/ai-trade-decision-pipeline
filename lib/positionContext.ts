// lib/positionContext.ts

import type { PositionContext } from './ai';
import type { PositionInfo } from './analytics';
import { DEFAULT_TAKER_FEE_RATE } from './constants';

const toFixedNumber = (value: number | null | undefined, digits: number) => {
    const n = Number(value);
    return Number.isFinite(n) ? Number(n.toFixed(digits)) : undefined;
};

export function composePositionContext(params: {
    position: PositionInfo;
    pnlPct: number;
    maxDrawdownPct?: number;
    maxProfitPct?: number;
    enteredAt?: number;
}): PositionContext | null {
    if (params.position.status !== 'open') return null;

    const entryPriceNum = Number(params.position.entryPrice);
    const side = params.position.holdSide;
    const derivedEntryTs = params.enteredAt ?? params.position.entryTimestamp;
    const entryTsIso = derivedEntryTs ? new Date(derivedEntryTs).toISOString() : undefined;
    const holdMinutes = derivedEntryTs ? Math.max((Date.now() - derivedEntryTs) / 60_000, 0) : 0;
    const takerFeeRate = Number.isFinite(DEFAULT_TAKER_FEE_RATE) ? DEFAULT_TAKER_FEE_RATE : 0.0006;
    const positionSize = Math.abs(Number(params.position.total ?? 0));
    const direction = side === 'long' ? 1 : -1;

    const totalTransactionFee = entryPriceNum > 0 && positionSize > 0 ? Math.abs(entryPriceNum * positionSize * takerFeeRate) : 0;
    const fundingFee = 0; // not tracked yet
    const realizedPnl = 0; // not tracked yet
    const numerator =
        totalTransactionFee -
        fundingFee -
        realizedPnl +
        direction * entryPriceNum * positionSize;
    const denominator = positionSize * (direction - takerFeeRate);
    const breakevenPrice = positionSize > 0 && denominator !== 0 ? numerator / denominator : undefined;

    return {
        side,
        entry_price: toFixedNumber(entryPriceNum, 6),
        entry_ts: entryTsIso,
        hold_minutes: toFixedNumber(holdMinutes, 1),
        unrealized_pnl_pct: toFixedNumber(params.pnlPct, 2),
        max_drawdown_pct: toFixedNumber(params.maxDrawdownPct, 2),
        max_profit_pct: toFixedNumber(params.maxProfitPct, 2),
        breakeven_price: toFixedNumber(breakevenPrice ?? entryPriceNum, 6),
        taker_fee_rate: toFixedNumber(takerFeeRate, 6),
    };
}
