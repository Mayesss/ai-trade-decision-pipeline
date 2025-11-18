// lib/positionContext.ts

import type { PositionContext } from './ai';
import type { PositionInfo } from './analytics';
import { DEFAULT_TAKER_FEE_RATE } from './constants';

const PRESSURE_SCALE = 50; // governs how aggressively we squash CVD into [-1, 1]
const ALIGNMENT_THRESHOLD = 0.2;

const toFixedNumber = (value: number, digits: number) => {
    const n = Number(value);
    return Number.isFinite(n) ? Number(n.toFixed(digits)) : undefined;
};

export function composePositionContext(params: {
    position: PositionInfo;
    pnlPct: number;
    cvd: number;
    obImb: number;
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

    const cvdScaled = Math.tanh(params.cvd / PRESSURE_SCALE);
    const pressureDelta = params.obImb - cvdScaled;
    const alignmentScore = params.obImb + cvdScaled;
    const alignment =
        alignmentScore >= ALIGNMENT_THRESHOLD
            ? 'bullish'
            : alignmentScore <= -ALIGNMENT_THRESHOLD
            ? 'bearish'
            : 'neutral';
    const againstPosition =
        alignment === 'neutral'
            ? false
            : side === 'long'
            ? alignment === 'bearish'
            : alignment === 'bullish';

    return {
        side,
        entry_price: toFixedNumber(entryPriceNum, 6),
        entry_ts: entryTsIso,
        hold_minutes: toFixedNumber(holdMinutes, 1),
        unrealized_pnl_pct: toFixedNumber(params.pnlPct, 2),
        breakeven_price: toFixedNumber(breakevenPrice ?? entryPriceNum, 6),
        taker_fee_rate: toFixedNumber(takerFeeRate, 6),
        flow: {
            cvd: toFixedNumber(params.cvd, 3),
            ob_imbalance: toFixedNumber(params.obImb, 3),
            pressure_delta: toFixedNumber(pressureDelta, 3),
            alignment,
            against_position: againstPosition,
        },
    };
}
