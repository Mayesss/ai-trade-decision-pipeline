// lib/positionContext.ts

import type { PositionContext } from './ai';
import type { PositionInfo } from './analytics';

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
        flow: {
            cvd: toFixedNumber(params.cvd, 3),
            ob_imbalance: toFixedNumber(params.obImb, 3),
            pressure_delta: toFixedNumber(pressureDelta, 3),
            alignment,
            against_position: againstPosition,
        },
    };
}
