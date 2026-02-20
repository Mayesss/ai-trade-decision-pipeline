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
    if (!Array.isArray(candles) || candles.length < 40) return null;

    const recent = candles.slice(-30, -5);
    const rangeHigh = Math.max(...recent.map((c) => num(c?.[2])));
    const rangeLow = Math.min(...recent.map((c) => num(c?.[3])));

    const last = candles[candles.length - 1];
    const prev = candles[candles.length - 2];
    const prevClose = num(prev?.[4]);
    const lastClose = num(last?.[4]);
    const lastLow = num(last?.[3]);
    const lastHigh = num(last?.[2]);

    const buffer = market.atr5m * cfg.modules.breakoutAtrBuffer;

    const longBreak = prevClose > rangeHigh + buffer;
    const longRetest = lastLow <= rangeHigh + buffer && lastClose > rangeHigh;

    if (
        (packet.regime === 'trend_up' || packet.regime === 'high_vol') &&
        permissionAllowsLong(packet.permission) &&
        longBreak &&
        longRetest
    ) {
        const stop = Math.min(rangeHigh - buffer, lastLow - buffer);
        return {
            pair,
            module: 'breakout_retest',
            side: 'BUY',
            entryPrice: lastClose,
            stopPrice: stop,
            confidence: Math.max(0.6, packet.confidence),
            reasonCodes: ['MODULE_BREAKOUT_RETEST_LONG_TRIGGER'],
        };
    }

    const shortBreak = prevClose < rangeLow - buffer;
    const shortRetest = lastHigh >= rangeLow - buffer && lastClose < rangeLow;

    if (
        (packet.regime === 'trend_down' || packet.regime === 'high_vol') &&
        permissionAllowsShort(packet.permission) &&
        shortBreak &&
        shortRetest
    ) {
        const stop = Math.max(rangeLow + buffer, lastHigh + buffer);
        return {
            pair,
            module: 'breakout_retest',
            side: 'SELL',
            entryPrice: lastClose,
            stopPrice: stop,
            confidence: Math.max(0.6, packet.confidence),
            reasonCodes: ['MODULE_BREAKOUT_RETEST_SHORT_TRIGGER'],
        };
    }

    return null;
}
