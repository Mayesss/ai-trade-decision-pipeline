import { computeEMA } from '../../indicators';
import { getForexStrategyConfig } from '../config';
import type { ForexModuleSignal, ForexPairMetrics, ForexRegimePacket } from '../types';
import type { ForexPairMarketState } from '../marketData';

function permissionAllowsLong(permission: ForexRegimePacket['permission']) {
    return permission === 'long_only' || permission === 'both';
}

function permissionAllowsShort(permission: ForexRegimePacket['permission']) {
    return permission === 'short_only' || permission === 'both';
}

function closes(candles: any[]): number[] {
    return candles.map((c) => Number(c?.[4])).filter((v) => Number.isFinite(v));
}

function lows(candles: any[]): number[] {
    return candles.map((c) => Number(c?.[3])).filter((v) => Number.isFinite(v));
}

function highs(candles: any[]): number[] {
    return candles.map((c) => Number(c?.[2])).filter((v) => Number.isFinite(v));
}

export function evaluatePullbackModule(params: {
    pair: string;
    packet: ForexRegimePacket;
    market: ForexPairMarketState;
    metrics: ForexPairMetrics;
}): ForexModuleSignal | null {
    const { pair, packet, market } = params;
    const cfg = getForexStrategyConfig();

    const c5 = closes(market.candles.m5);
    if (c5.length < 30) return null;

    const ema20 = computeEMA(c5, 20);
    const ema50 = computeEMA(c5, 50);
    const lastClose = c5[c5.length - 1]!;
    const prevClose = c5[c5.length - 2]!;
    const e20 = ema20[ema20.length - 1] ?? lastClose;
    const e50 = ema50[ema50.length - 1] ?? lastClose;
    const zoneLow = Math.min(e20, e50);
    const zoneHigh = Math.max(e20, e50);

    const inZoneNow = lastClose >= zoneLow && lastClose <= zoneHigh;
    const touchedZoneRecently = c5.slice(-6).some((value) => value >= zoneLow && value <= zoneHigh);

    if (packet.regime === 'trend_up' && market.trendDirection1h === 'up' && permissionAllowsLong(packet.permission)) {
        const trigger = prevClose <= e20 && lastClose > e20;
        if (!trigger && !(inZoneNow && touchedZoneRecently)) return null;

        const recentLows = lows(market.candles.m5.slice(-8));
        const swingLow = Math.min(...recentLows);
        const stop = swingLow - market.atr5m * cfg.modules.pullbackAtrBuffer;

        return {
            pair,
            module: 'pullback',
            side: 'BUY',
            entryPrice: lastClose,
            stopPrice: stop,
            confidence: Math.max(0.55, packet.confidence),
            reasonCodes: ['MODULE_PULLBACK_LONG_TRIGGER'],
        };
    }

    if (packet.regime === 'trend_down' && market.trendDirection1h === 'down' && permissionAllowsShort(packet.permission)) {
        const trigger = prevClose >= e20 && lastClose < e20;
        if (!trigger && !(inZoneNow && touchedZoneRecently)) return null;

        const recentHighs = highs(market.candles.m5.slice(-8));
        const swingHigh = Math.max(...recentHighs);
        const stop = swingHigh + market.atr5m * cfg.modules.pullbackAtrBuffer;

        return {
            pair,
            module: 'pullback',
            side: 'SELL',
            entryPrice: lastClose,
            stopPrice: stop,
            confidence: Math.max(0.55, packet.confidence),
            reasonCodes: ['MODULE_PULLBACK_SHORT_TRIGGER'],
        };
    }

    return null;
}
