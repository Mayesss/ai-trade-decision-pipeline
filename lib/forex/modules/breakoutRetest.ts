import { getForexStrategyConfig } from '../config';
import type { ForexModuleSignal, ForexPairMetrics, ForexRegimePacket } from '../types';
import type { ForexPairMarketState } from '../marketData';

function permissionAllowsLong(permission: ForexRegimePacket['permission']) {
    return permission === 'long_only' || permission === 'both';
}

function permissionAllowsShort(permission: ForexRegimePacket['permission']) {
    return permission === 'short_only' || permission === 'both';
}

function num(value: unknown): number {
    const n = Number(value);
    return Number.isFinite(n) ? n : 0;
}

export function evaluateBreakoutRetestModule(params: {
    pair: string;
    packet: ForexRegimePacket;
    market: ForexPairMarketState;
    metrics: ForexPairMetrics;
}): ForexModuleSignal | null {
    const { pair, packet, market } = params;
    const cfg = getForexStrategyConfig();

    const candles = market.candles.m15;
    if (!Array.isArray(candles) || candles.length < 45) return null;

    // Two-step confirmation flow:
    // 1) breakout candle closes outside range,
    // 2) retest candle accepts back in breakout direction,
    // 3) confirmation candle continues in breakout direction.
    const recent = candles.slice(-30, -5);
    const rangeHigh = Math.max(...recent.map((c) => num(c?.[2])));
    const rangeLow = Math.min(...recent.map((c) => num(c?.[3])));

    const breakoutCandle = candles[candles.length - 3];
    const retestCandle = candles[candles.length - 2];
    const confirmCandle = candles[candles.length - 1];

    const breakoutClose = num(breakoutCandle?.[4]);
    const breakoutLow = num(breakoutCandle?.[3]);
    const breakoutHigh = num(breakoutCandle?.[2]);
    const retestClose = num(retestCandle?.[4]);
    const retestLow = num(retestCandle?.[3]);
    const retestHigh = num(retestCandle?.[2]);
    const confirmClose = num(confirmCandle?.[4]);
    const confirmLow = num(confirmCandle?.[3]);
    const confirmHigh = num(confirmCandle?.[2]);

    const buffer = Math.max(1e-9, market.atr5m * cfg.modules.breakoutAtrBuffer);

    const longBreak = breakoutClose > rangeHigh + buffer;
    const longRetest = retestLow <= rangeHigh + buffer && retestClose > rangeHigh;
    const longConfirm = confirmClose > Math.max(rangeHigh, retestClose) && confirmLow > rangeHigh - buffer;

    if (
        (packet.regime === 'trend_up' || packet.regime === 'high_vol') &&
        permissionAllowsLong(packet.permission) &&
        longBreak &&
        longRetest &&
        longConfirm
    ) {
        const stop = Math.min(rangeHigh - buffer, retestLow - buffer, breakoutLow - buffer);
        return {
            pair,
            module: 'breakout_retest',
            side: 'BUY',
            entryPrice: confirmClose,
            stopPrice: stop,
            confidence: Math.max(0.6, packet.confidence),
            reasonCodes: ['MODULE_BREAKOUT_RETEST_LONG_TWO_STEP_CONFIRMED'],
        };
    }

    const shortBreak = breakoutClose < rangeLow - buffer;
    const shortRetest = retestHigh >= rangeLow - buffer && retestClose < rangeLow;
    const shortConfirm = confirmClose < Math.min(rangeLow, retestClose) && confirmHigh < rangeLow + buffer;

    if (
        (packet.regime === 'trend_down' || packet.regime === 'high_vol') &&
        permissionAllowsShort(packet.permission) &&
        shortBreak &&
        shortRetest &&
        shortConfirm
    ) {
        const stop = Math.max(rangeLow + buffer, retestHigh + buffer, breakoutHigh + buffer);
        return {
            pair,
            module: 'breakout_retest',
            side: 'SELL',
            entryPrice: confirmClose,
            stopPrice: stop,
            confidence: Math.max(0.6, packet.confidence),
            reasonCodes: ['MODULE_BREAKOUT_RETEST_SHORT_TWO_STEP_CONFIRMED'],
        };
    }

    return null;
}
