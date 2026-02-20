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

export interface RangeFadeEvaluation {
    signal: ForexModuleSignal | null;
    killSwitchTriggered: boolean;
    reasonCodes: string[];
}

export function evaluateRangeFadeModule(params: {
    pair: string;
    packet: ForexRegimePacket;
    market: ForexPairMarketState;
    metrics: ForexPairMetrics;
}): RangeFadeEvaluation {
    const { pair, packet, market, metrics } = params;
    const cfg = getForexStrategyConfig();

    if (packet.regime !== 'range') {
        return {
            signal: null,
            killSwitchTriggered: false,
            reasonCodes: ['MODULE_RANGE_FADE_REGIME_NOT_RANGE'],
        };
    }

    const candles = market.candles.m15;
    if (!Array.isArray(candles) || candles.length < 50) {
        return {
            signal: null,
            killSwitchTriggered: false,
            reasonCodes: ['MODULE_RANGE_FADE_NOT_ENOUGH_DATA'],
        };
    }

    const lookback = candles.slice(-45, -1);
    const highs = lookback.map((c) => num(c?.[2])).filter((v) => v > 0);
    const lows = lookback.map((c) => num(c?.[3])).filter((v) => v > 0);
    if (!highs.length || !lows.length) {
        return {
            signal: null,
            killSwitchTriggered: false,
            reasonCodes: ['MODULE_RANGE_FADE_LEVELS_UNAVAILABLE'],
        };
    }

    const upperBoundary = Math.max(...highs);
    const lowerBoundary = Math.min(...lows);
    const rangeWidth = upperBoundary - lowerBoundary;
    const minRangeWidth = metrics.atr1h * cfg.modules.rangeFadeMinWidthAtr1h;
    if (!(rangeWidth > minRangeWidth && minRangeWidth > 0)) {
        return {
            signal: null,
            killSwitchTriggered: false,
            reasonCodes: ['MODULE_RANGE_FADE_RANGE_TOO_NARROW'],
        };
    }

    if (metrics.trendStrength > cfg.modules.rangeFadeMaxTrendStrength) {
        return {
            signal: null,
            killSwitchTriggered: false,
            reasonCodes: ['MODULE_RANGE_FADE_TREND_TOO_STRONG'],
        };
    }

    if (metrics.chopScore < cfg.modules.rangeFadeMinChopScore) {
        return {
            signal: null,
            killSwitchTriggered: false,
            reasonCodes: ['MODULE_RANGE_FADE_NOT_CHOPPY_ENOUGH'],
        };
    }

    const last = candles[candles.length - 1];
    const prev = candles[candles.length - 2];

    const prevClose = num(prev?.[4]);
    const lastClose = num(last?.[4]);
    const lastLow = num(last?.[3]);
    const lastHigh = num(last?.[2]);

    const boundaryBuffer = Math.max(market.atr5m * cfg.modules.rangeFadeBoundaryAtrBuffer, 1e-9);
    const breakoutRange = Math.max(0, lastHigh - lastLow);
    const breakoutAtrThreshold = market.atr5m * cfg.modules.rangeFadeBreakoutAtr5m;
    const breakoutUp = lastClose > upperBoundary + boundaryBuffer;
    const breakoutDown = lastClose < lowerBoundary - boundaryBuffer;

    if ((breakoutAtrThreshold > 0 && breakoutRange >= breakoutAtrThreshold) || breakoutUp || breakoutDown) {
        return {
            signal: null,
            killSwitchTriggered: true,
            reasonCodes: ['MODULE_RANGE_FADE_BREAKOUT_KILL_SWITCH'],
        };
    }

    const upperTouch = lastHigh >= upperBoundary - boundaryBuffer;
    const lowerTouch = lastLow <= lowerBoundary + boundaryBuffer;

    const shortRejection = upperTouch && prevClose >= upperBoundary - boundaryBuffer && lastClose < upperBoundary;
    if (shortRejection && permissionAllowsShort(packet.permission)) {
        const stopPrice = upperBoundary + boundaryBuffer;
        return {
            signal: {
                pair,
                module: 'range_fade',
                side: 'SELL',
                entryPrice: lastClose,
                stopPrice,
                confidence: Math.max(0.55, packet.confidence),
                reasonCodes: ['MODULE_RANGE_FADE_SHORT_REJECTION'],
            },
            killSwitchTriggered: false,
            reasonCodes: ['MODULE_RANGE_FADE_SIGNAL_SHORT'],
        };
    }

    const longRejection = lowerTouch && prevClose <= lowerBoundary + boundaryBuffer && lastClose > lowerBoundary;
    if (longRejection && permissionAllowsLong(packet.permission)) {
        const stopPrice = lowerBoundary - boundaryBuffer;
        return {
            signal: {
                pair,
                module: 'range_fade',
                side: 'BUY',
                entryPrice: lastClose,
                stopPrice,
                confidence: Math.max(0.55, packet.confidence),
                reasonCodes: ['MODULE_RANGE_FADE_LONG_REJECTION'],
            },
            killSwitchTriggered: false,
            reasonCodes: ['MODULE_RANGE_FADE_SIGNAL_LONG'],
        };
    }

    return {
        signal: null,
        killSwitchTriggered: false,
        reasonCodes: ['MODULE_RANGE_FADE_NO_REJECTION_SIGNAL'],
    };
}
