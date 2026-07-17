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
    // Standing exchange-side bracket (resting TP/SL orders) on the position, so
    // the model amends against the actual current levels. null = none resting.
    takeProfitPrice?: number | null;
    stopLossPrice?: number | null;
    // Per-side taker fee. Capital CFDs are commission-free — pass 0 there so
    // breakeven_price isn't shifted by a fee the venue never charges. Omitted =
    // the Bitget default.
    takerFeeRate?: number;
}): PositionContext | null {
    if (params.position.status !== 'open') return null;

    const entryPriceNum = Number(params.position.entryPrice);
    const side = params.position.holdSide;
    // Both venues report PnL% on MARGIN (return on equity, leverage-multiplied):
    // Bitget = uPnl/initialMargin, Capital = price-move × leverage. Divide the
    // leverage back out to also expose the unleveraged price-scale move, so the
    // prompt can distinguish "position up 6% on margin" from "price moved 0.6%".
    const leverageNum = Number(params.position.leverage);
    const leverage = Number.isFinite(leverageNum) && leverageNum >= 1 ? leverageNum : null;
    const priceMovePct = leverage !== null ? params.pnlPct / leverage : params.pnlPct;
    const derivedEntryTs = params.enteredAt ?? params.position.entryTimestamp;
    const entryTsIso = derivedEntryTs ? new Date(derivedEntryTs).toISOString() : undefined;
    const holdMinutes = derivedEntryTs ? Math.max((Date.now() - derivedEntryTs) / 60_000, 0) : 0;
    const takerFeeRate =
        Number.isFinite(params.takerFeeRate as number) && (params.takerFeeRate as number) >= 0
            ? (params.takerFeeRate as number)
            : Number.isFinite(DEFAULT_TAKER_FEE_RATE)
              ? DEFAULT_TAKER_FEE_RATE
              : 0.0006;
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
        unrealized_pnl_pct_on_margin: toFixedNumber(params.pnlPct, 2),
        price_move_pct: toFixedNumber(priceMovePct, 3),
        leverage,
        max_drawdown_pct: toFixedNumber(params.maxDrawdownPct, 2),
        max_profit_pct: toFixedNumber(params.maxProfitPct, 2),
        breakeven_price: toFixedNumber(breakevenPrice ?? entryPriceNum, 6),
        taker_fee_rate: toFixedNumber(takerFeeRate, 6),
        take_profit_price: toFixedNumber(params.takeProfitPrice, 6) ?? null,
        stop_loss_price: toFixedNumber(params.stopLossPrice, 6) ?? null,
    };
}
