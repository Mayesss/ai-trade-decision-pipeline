import crypto from 'crypto';

import { executeCapitalDecision, fetchCapitalOpenPositionSnapshots } from '../capital';
import { getForexStrategyConfig, getForexUniversePairs } from './config';
import { buildForexPacketSnapshot } from './ai';
import { refreshForexEvents, getForexEventsState } from './events/forexFactory';
import { evaluateForexEventGate } from './events/gate';
import { evaluateForexMarketGate } from './marketHours';
import { loadForexPairMarketState } from './marketData';
import { evaluateBreakoutRetestModule } from './modules/breakoutRetest';
import { evaluatePullbackModule } from './modules/pullback';
import { evaluateRangeFadeModule } from './modules/rangeFade';
import {
    buildOpenCurrencyExposure,
    computeHybridRiskSize,
    computeOpenRiskUsage,
    confidenceToLeverageCapped,
    evaluateForexRiskCheck,
    evaluateRiskCapBudget,
} from './risk';
import {
    appendForexJournal,
    clearForexReentryLock,
    deleteForexPositionContext,
    getForexReentryLockUntil,
    getForexRangeFadeCooldownUntil,
    loadForexPositionContext,
    loadForexPacketSnapshot,
    loadForexScanSnapshot,
    saveForexPositionContext,
    setForexReentryLock,
    setForexRangeFadeCooldown,
    saveForexPacketSnapshot,
    saveForexScanSnapshot,
} from './store';
import { isWithinSelectorTopPercentile, runForexUniverseScan } from './selector';
import type {
    ForexExecutionResultSummary,
    ForexJournalEntry,
    ForexModuleSignal,
    ForexPairEligibility,
    ForexPacketSnapshot,
    ForexPositionContext,
    ForexRegimePacket,
    ForexSide,
    ForexScanSnapshot,
    NormalizedForexEconomicEvent,
} from './types';

function journalEntry(params: {
    type: ForexJournalEntry['type'];
    pair?: string | null;
    level?: ForexJournalEntry['level'];
    reasonCodes?: string[];
    payload?: Record<string, any>;
}): ForexJournalEntry {
    return {
        id: crypto.randomUUID(),
        timestampMs: Date.now(),
        type: params.type,
        pair: params.pair ?? null,
        level: params.level ?? 'info',
        reasonCodes: params.reasonCodes ?? [],
        payload: params.payload ?? {},
    };
}

async function safeAppendJournal(entry: ForexJournalEntry) {
    try {
        await appendForexJournal(entry);
    } catch (err) {
        console.warn('Failed to append forex journal:', err);
    }
}

async function ensureEventState(nowMs: number) {
    let state = await getForexEventsState(nowMs);
    const refreshIntervalMs = Math.max(1, state.refreshMinutes) * 60_000;
    const shouldRefresh =
        !state.meta.lastSuccessAtMs || nowMs - Number(state.meta.lastSuccessAtMs) >= refreshIntervalMs;

    if (shouldRefresh) {
        const refreshRes = await refreshForexEvents({ force: false, nowMs });
        await safeAppendJournal(
            journalEntry({
                type: 'event_refresh',
                level: refreshRes.ok ? 'info' : 'warn',
                reasonCodes: [refreshRes.ok ? 'EVENT_REFRESH_OK' : 'EVENT_REFRESH_FAILED'],
                payload: {
                    refreshed: refreshRes.refreshed,
                    skipped: refreshRes.skipped,
                    reason: refreshRes.reason,
                    stale: refreshRes.state.stale,
                },
            }),
        );
        state = refreshRes.state;
    }

    return state;
}

export async function runForexScanCycle(opts: { nowMs?: number } = {}): Promise<ForexScanSnapshot> {
    const nowMs = Number.isFinite(opts.nowMs as number) ? Number(opts.nowMs) : Date.now();
    const marketGate = evaluateForexMarketGate(nowMs);
    if (marketGate.marketClosed) {
        await safeAppendJournal(
            journalEntry({
                type: 'scan',
                level: 'info',
                reasonCodes: ['MARKET_CLOSED_HARD_GATE', marketGate.reasonCode],
                payload: {
                    nowMs,
                    reopensAtMs: marketGate.reopensAtMs,
                    config: marketGate.config,
                },
            }),
        );
        const existing = await loadForexScanSnapshot();
        if (existing) {
            return {
                ...existing,
                marketClosed: true,
                reasonCodes: ['MARKET_CLOSED_HARD_GATE', marketGate.reasonCode],
                reopensAtMs: marketGate.reopensAtMs,
            } as ForexScanSnapshot;
        }
        return {
            generatedAtMs: nowMs,
            staleEvents: false,
            pairs: [],
            marketClosed: true,
            reasonCodes: ['MARKET_CLOSED_HARD_GATE', marketGate.reasonCode],
            reopensAtMs: marketGate.reopensAtMs,
        } as ForexScanSnapshot;
    }

    const eventState = await ensureEventState(nowMs);
    const events = eventState.snapshot?.events ?? [];

    const snapshot = await runForexUniverseScan({
        nowMs,
        events,
        staleEvents: eventState.stale,
    });

    await saveForexScanSnapshot(snapshot);

    await safeAppendJournal(
        journalEntry({
            type: 'scan',
            reasonCodes: ['FOREX_SCAN_COMPLETED'],
            payload: {
                generatedAtMs: snapshot.generatedAtMs,
                staleEvents: snapshot.staleEvents,
                eligiblePairs: snapshot.pairs.filter((p) => p.eligible).map((p) => p.pair),
            },
        }),
    );

    return snapshot;
}

function packetMap(snapshot: ForexPacketSnapshot | null): Map<string, ForexRegimePacket> {
    const map = new Map<string, ForexRegimePacket>();
    for (const packet of snapshot?.packets || []) {
        map.set(packet.pair, packet);
    }
    return map;
}

export async function runForexRegimeCycle(opts: { nowMs?: number } = {}): Promise<ForexPacketSnapshot> {
    const nowMs = Number.isFinite(opts.nowMs as number) ? Number(opts.nowMs) : Date.now();
    const marketGate = evaluateForexMarketGate(nowMs);
    if (marketGate.marketClosed) {
        await safeAppendJournal(
            journalEntry({
                type: 'regime',
                level: 'info',
                reasonCodes: ['MARKET_CLOSED_HARD_GATE', marketGate.reasonCode],
                payload: {
                    nowMs,
                    reopensAtMs: marketGate.reopensAtMs,
                    config: marketGate.config,
                },
            }),
        );
        const existing = await loadForexPacketSnapshot();
        if (existing) {
            return {
                ...existing,
                marketClosed: true,
                reasonCodes: ['MARKET_CLOSED_HARD_GATE', marketGate.reasonCode],
                reopensAtMs: marketGate.reopensAtMs,
            } as ForexPacketSnapshot;
        }
        return {
            generatedAtMs: nowMs,
            packets: [],
            marketClosed: true,
            reasonCodes: ['MARKET_CLOSED_HARD_GATE', marketGate.reasonCode],
            reopensAtMs: marketGate.reopensAtMs,
        } as ForexPacketSnapshot;
    }

    const scan = (await loadForexScanSnapshot()) ?? (await runForexScanCycle({ nowMs }));
    const eventState = await ensureEventState(nowMs);
    const events = eventState.snapshot?.events ?? [];

    const blockedPairs = new Set<string>();
    for (const row of scan.pairs) {
        const gate = evaluateForexEventGate({
            pair: row.pair,
            events,
            staleData: eventState.stale,
            riskState: 'normal',
            nowMs,
        });
        if (gate.blockNewEntries) blockedPairs.add(row.pair);
    }

    const packetSnapshot = await buildForexPacketSnapshot({
        entries: scan.pairs,
        eventBlockedPairs: blockedPairs,
        nowMs,
    });

    await saveForexPacketSnapshot(packetSnapshot);

    await safeAppendJournal(
        journalEntry({
            type: 'regime',
            reasonCodes: ['FOREX_REGIME_COMPLETED'],
            payload: {
                generatedAtMs: packetSnapshot.generatedAtMs,
                packetCount: packetSnapshot.packets.length,
                blockedPairs: Array.from(blockedPairs),
            },
        }),
    );

    return packetSnapshot;
}

function confidenceToLeverage(confidence: number, maxLeverage = 3): number {
    return confidenceToLeverageCapped(confidence, maxLeverage);
}

function confidenceToSignalStrength(confidence: number): 'LOW' | 'MEDIUM' | 'HIGH' {
    if (confidence >= 0.75) return 'HIGH';
    if (confidence >= 0.6) return 'MEDIUM';
    return 'LOW';
}

function normalizeComparable(value: string): string {
    return String(value || '')
        .trim()
        .toUpperCase()
        .replace(/[^A-Z0-9]/g, '');
}

function sideFromOpenPosition(value: 'long' | 'short' | null): ForexSide | null {
    if (value === 'long') return 'BUY';
    if (value === 'short') return 'SELL';
    return null;
}

function midPriceFromSnapshot(position: Awaited<ReturnType<typeof fetchCapitalOpenPositionSnapshots>>[number]): number | null {
    const bid = Number(position.bid);
    const offer = Number(position.offer);
    if (Number.isFinite(bid) && Number.isFinite(offer) && bid > 0 && offer > 0) {
        return (bid + offer) / 2;
    }
    if (Number.isFinite(bid) && bid > 0) return bid;
    if (Number.isFinite(offer) && offer > 0) return offer;
    return null;
}

function currentStopFromContext(context: ForexPositionContext | null): number | null {
    if (!context) return null;
    const current = Number((context as any).currentStopPrice ?? context.stopPrice);
    if (Number.isFinite(current) && current > 0) return current;
    const initial = Number((context as any).initialStopPrice);
    return Number.isFinite(initial) && initial > 0 ? initial : null;
}

function entryModuleFromContext(context: ForexPositionContext | null): ForexPositionContext['entryModule'] | null {
    if (!context) return null;
    const raw = String((context as any).entryModule || context.module || '')
        .trim()
        .toLowerCase();
    if (raw === 'pullback' || raw === 'breakout_retest' || raw === 'range_fade') return raw;
    return null;
}

export function packetAgeMinutes(packet: ForexRegimePacket | null, nowMs: number): number | null {
    if (!packet) return null;
    const generatedAtMs = Number(packet.generatedAtMs);
    if (!(Number.isFinite(generatedAtMs) && generatedAtMs > 0)) return null;
    return Math.max(0, (nowMs - generatedAtMs) / 60_000);
}

export function isPacketStale(packet: ForexRegimePacket | null, nowMs: number, staleMinutes: number): boolean {
    const ageMin = packetAgeMinutes(packet, nowMs);
    if (ageMin === null) return true;
    return ageMin > Math.max(1, staleMinutes);
}

function eventHasImpact(events: NormalizedForexEconomicEvent[] | undefined, impact: string): boolean {
    const wanted = String(impact || '').trim().toUpperCase();
    if (!wanted) return false;
    return Boolean(events?.some((event) => String(event?.impact || '').toUpperCase() === wanted));
}

function isTrendAlignedForHold(openSide: ForexSide | null, packet: ForexRegimePacket | null): boolean {
    if (!openSide || !packet) return false;
    if (openSide === 'BUY') {
        return packet.regime === 'trend_up' && (packet.permission === 'long_only' || packet.permission === 'both');
    }
    return packet.regime === 'trend_down' && (packet.permission === 'short_only' || packet.permission === 'both');
}

type PositionProgress = {
    rValue: number;
    mfeR: number;
    ageBars5m: number;
    ageHours: number;
    maxFavorablePrice: number | null;
    minFavorablePrice: number | null;
};

function safeNumber(value: unknown): number {
    const n = Number(value);
    return Number.isFinite(n) ? n : NaN;
}

function candlesSince(candles: any[], sinceMs: number): any[] {
    if (!Array.isArray(candles) || !candles.length) return [];
    return candles.filter((c) => {
        const ts = Number(c?.[0]);
        return Number.isFinite(ts) && ts >= sinceMs;
    });
}

export function computePositionProgress(params: {
    side: ForexSide;
    entryPrice: number;
    initialStopPrice: number;
    openedAtMs: number;
    nowMs: number;
    candles5m: any[];
}): PositionProgress {
    const entryPrice = safeNumber(params.entryPrice);
    const stop = safeNumber(params.initialStopPrice);
    const nowMs = safeNumber(params.nowMs);
    const openedAtMs = safeNumber(params.openedAtMs);
    const rValue = Number.isFinite(entryPrice) && Number.isFinite(stop) ? Math.abs(entryPrice - stop) : NaN;
    const ageMinutes = Number.isFinite(nowMs) && Number.isFinite(openedAtMs) && nowMs > openedAtMs ? (nowMs - openedAtMs) / 60_000 : 0;
    const ageBars5m = Math.floor(ageMinutes / 5);
    const ageHours = ageMinutes / 60;

    if (!(Number.isFinite(rValue) && rValue > 0)) {
        return {
            rValue: NaN,
            mfeR: NaN,
            ageBars5m,
            ageHours,
            maxFavorablePrice: null,
            minFavorablePrice: null,
        };
    }

    const since = candlesSince(params.candles5m, openedAtMs);
    const highs = since.map((c) => safeNumber(c?.[2])).filter((v) => Number.isFinite(v));
    const lows = since.map((c) => safeNumber(c?.[3])).filter((v) => Number.isFinite(v));
    const maxHigh = highs.length ? Math.max(...highs) : entryPrice;
    const minLow = lows.length ? Math.min(...lows) : entryPrice;

    const mfeAbs = params.side === 'BUY' ? maxHigh - entryPrice : entryPrice - minLow;
    const mfeR = mfeAbs / rValue;

    return {
        rValue,
        mfeR: Number.isFinite(mfeR) ? mfeR : NaN,
        ageBars5m,
        ageHours,
        maxFavorablePrice: Number.isFinite(maxHigh) ? maxHigh : null,
        minFavorablePrice: Number.isFinite(minLow) ? minLow : null,
    };
}

export function shouldTimeStopNoFollowThrough(params: {
    ageBars5m: number;
    mfeR: number;
    thresholdBars: number;
    minFollowR: number;
}): boolean {
    return params.ageBars5m >= params.thresholdBars && params.mfeR < params.minFollowR;
}

export function shouldTimeStopMaxHold(params: {
    ageHours: number;
    maxHoldHours: number;
    trendAligned: boolean;
    trailingActive: boolean;
}): boolean {
    if (params.ageHours < params.maxHoldHours) return false;
    if (params.trendAligned && params.trailingActive) return false;
    return true;
}

function computeTrendTrailStop(params: {
    side: ForexSide;
    currentStop: number;
    candles5m: any[];
    atr5m: number;
}): { nextStop: number | null; reasonCode: string | null } {
    const currentStop = safeNumber(params.currentStop);
    const atr5m = safeNumber(params.atr5m);
    const closes = params.candles5m || [];
    if (!Number.isFinite(currentStop)) return { nextStop: null, reasonCode: null };
    if (!Array.isArray(closes) || closes.length < 4) return { nextStop: null, reasonCode: null };

    const recent = closes.slice(-4, -1);
    const lows = recent.map((c) => safeNumber(c?.[3])).filter((v) => Number.isFinite(v));
    const highs = recent.map((c) => safeNumber(c?.[2])).filter((v) => Number.isFinite(v));
    if (params.side === 'BUY') {
        const swingLow = lows.length ? Math.min(...lows) : NaN;
        const atrTrail = Number.isFinite(atr5m) && atr5m > 0 ? swingLow - atr5m * 0.5 : swingLow;
        if (Number.isFinite(atrTrail) && atrTrail > currentStop) {
            return { nextStop: atrTrail, reasonCode: 'STOP_TRAIL_STRUCTURE' };
        }
        return { nextStop: null, reasonCode: null };
    }

    const swingHigh = highs.length ? Math.max(...highs) : NaN;
    const atrTrail = Number.isFinite(atr5m) && atr5m > 0 ? swingHigh + atr5m * 0.5 : swingHigh;
    if (Number.isFinite(atrTrail) && atrTrail < currentStop) {
        return { nextStop: atrTrail, reasonCode: 'STOP_TRAIL_STRUCTURE' };
    }
    return { nextStop: null, reasonCode: null };
}

export function hasActiveEventWindow(reasonCodes: string[]): boolean {
    return reasonCodes.includes('EVENT_WINDOW_ACTIVE_BLOCK');
}

export function isOppositePermission(side: ForexSide, permission: ForexRegimePacket['permission']): boolean {
    if (side === 'BUY') return permission === 'short_only';
    return permission === 'long_only';
}

function packetForContext(packet: ForexRegimePacket): ForexRegimePacket {
    return {
        pair: packet.pair,
        generatedAtMs: packet.generatedAtMs,
        regime: packet.regime,
        permission: packet.permission,
        allowed_modules: packet.allowed_modules.slice(),
        risk_state: packet.risk_state,
        confidence: packet.confidence,
        htf_context: { ...packet.htf_context },
        notes_codes: packet.notes_codes.slice(),
    };
}

export function shouldInvalidateByStop(params: {
    context: ForexPositionContext | null;
    openSide: ForexSide | null;
    midPrice: number | null;
}): { invalidated: boolean; reasonCode?: string } {
    const { context, openSide, midPrice } = params;
    if (!context || !openSide || context.side !== openSide) {
        return { invalidated: false };
    }
    const stopPrice = currentStopFromContext(context);
    if (!(Number.isFinite(stopPrice as number) && (stopPrice as number) > 0)) {
        return { invalidated: false };
    }
    if (!(Number.isFinite(midPrice as number) && (midPrice as number) > 0)) {
        return { invalidated: false };
    }
    if (openSide === 'BUY' && Number(midPrice) <= Number(stopPrice)) {
        return { invalidated: true, reasonCode: 'STOP_INVALIDATED_LONG' };
    }
    if (openSide === 'SELL' && Number(midPrice) >= Number(stopPrice)) {
        return { invalidated: true, reasonCode: 'STOP_INVALIDATED_SHORT' };
    }
    return { invalidated: false };
}

export function shouldInvalidateFallback(params: {
    openSide: ForexSide | null;
    packet: ForexRegimePacket | null;
    trendDirection1h: 'up' | 'down' | 'neutral';
}): { invalidated: boolean; reasonCode?: string } {
    const { openSide, packet, trendDirection1h } = params;
    if (!packet || !openSide) return { invalidated: false };
    if (openSide === 'BUY' && trendDirection1h === 'down' && (packet.permission === 'short_only' || packet.permission === 'flat')) {
        return { invalidated: true, reasonCode: 'STRUCTURE_INVALIDATION_FALLBACK_LONG' };
    }
    if (openSide === 'SELL' && trendDirection1h === 'up' && (packet.permission === 'long_only' || packet.permission === 'flat')) {
        return { invalidated: true, reasonCode: 'STRUCTURE_INVALIDATION_FALLBACK_SHORT' };
    }
    return { invalidated: false };
}

export async function runForexExecuteCycle(opts: { nowMs?: number; dryRun?: boolean; notionalUsd?: number } = {}) {
    const nowMs = Number.isFinite(opts.nowMs as number) ? Number(opts.nowMs) : Date.now();
    const cfg = getForexStrategyConfig();
    const dryRun = opts.dryRun ?? cfg.dryRunDefault;
    const notionalUsd = Number.isFinite(opts.notionalUsd as number) ? Number(opts.notionalUsd) : cfg.defaultNotionalUsd;
    const marketGate = evaluateForexMarketGate(nowMs);
    if (marketGate.marketClosed) {
        await safeAppendJournal(
            journalEntry({
                type: 'execution',
                level: 'info',
                reasonCodes: ['MARKET_CLOSED_HARD_GATE', marketGate.reasonCode],
                payload: {
                    nowMs,
                    dryRun,
                    notionalUsd,
                    reopensAtMs: marketGate.reopensAtMs,
                    config: marketGate.config,
                },
            }),
        );
        return {
            generatedAtMs: nowMs,
            dryRun,
            notionalUsd,
            marketClosed: true,
            reasonCodes: ['MARKET_CLOSED_HARD_GATE', marketGate.reasonCode],
            reopensAtMs: marketGate.reopensAtMs,
            results: [],
        };
    }

    const scan = (await loadForexScanSnapshot()) ?? (await runForexScanCycle({ nowMs }));
    const packets = (await loadForexPacketSnapshot()) ?? (await runForexRegimeCycle({ nowMs }));
    const packetByPair = packetMap(packets);
    const eventState = await ensureEventState(nowMs);
    const events = eventState.snapshot?.events ?? [];

    const universe = getForexUniversePairs();
    const exposure = await buildOpenCurrencyExposure(universe);
    const rowsByPair = new Map(scan.pairs.map((row) => [row.pair, row]));
    const results: ForexExecutionResultSummary[] = [];

    const referenceEquityUsd = Number(cfg.risk.referenceEquityUsd);
    const stalePacketMinutes = cfg.packet.staleMinutes;

    let openPositions: Awaited<ReturnType<typeof fetchCapitalOpenPositionSnapshots>> = [];
    try {
        openPositions = await fetchCapitalOpenPositionSnapshots();
    } catch (err) {
        console.warn('Unable to load open Capital positions for forex exit manager:', err);
        openPositions = [];
    }

    const pairByEpic = new Map<string, string>();
    for (const row of scan.pairs) {
        const epicKey = normalizeComparable(String(row.metrics.epic || ''));
        if (!epicKey) continue;
        pairByEpic.set(epicKey, row.pair);
    }

    const openByPair = new Map<string, (typeof openPositions)[number]>();
    for (const position of openPositions) {
        const epicKey = normalizeComparable(String(position.epic || ''));
        const pair = pairByEpic.get(epicKey);
        if (!pair) continue;
        openByPair.set(pair, position);
    }

    const contextsByPair = new Map<string, ForexPositionContext>();
    await Promise.all(
        Array.from(openByPair.keys()).map(async (pair) => {
            const context = await loadForexPositionContext(pair);
            if (context) contextsByPair.set(pair, context);
        }),
    );

    const openRiskUsage = computeOpenRiskUsage({
        openByPair,
        contextsByPair,
        equityUsd: referenceEquityUsd,
        fallbackRiskPctForUnknown: cfg.risk.riskPerTradePct,
    });

    const marketStateCache = new Map<string, Awaited<ReturnType<typeof loadForexPairMarketState>>>();
    const getMarketState = async (pair: string) => {
        if (marketStateCache.has(pair)) return marketStateCache.get(pair)!;
        const market = await loadForexPairMarketState(pair, nowMs);
        marketStateCache.set(pair, market);
        return market;
    };

    const selectModuleSignal = async (row: ForexPairEligibility, packet: ForexRegimePacket) => {
        const market = await getMarketState(row.pair);
        const moduleOrder = packet.allowed_modules.filter((m) => m === 'pullback' || m === 'breakout_retest' || m === 'range_fade');
        const moduleReasons: string[] = [];

        const rangeFadeCooldownUntil = await getForexRangeFadeCooldownUntil(row.pair);
        const filteredModuleOrder = moduleOrder.filter((moduleName) => {
            if (moduleName !== 'range_fade') return true;
            if (Number.isFinite(rangeFadeCooldownUntil as number) && Number(rangeFadeCooldownUntil) > nowMs) {
                moduleReasons.push('RANGE_FADE_DISABLED_UNTIL_NEXT_REEVAL');
                return false;
            }
            return true;
        });

        let signal: ForexModuleSignal | null = null;
        for (const moduleName of filteredModuleOrder) {
            if (moduleName === 'pullback') {
                signal = evaluatePullbackModule({ pair: row.pair, packet, market, metrics: row.metrics });
            } else if (moduleName === 'breakout_retest') {
                signal = evaluateBreakoutRetestModule({ pair: row.pair, packet, market, metrics: row.metrics });
            } else if (moduleName === 'range_fade') {
                const rangeEval = evaluateRangeFadeModule({ pair: row.pair, packet, market, metrics: row.metrics });
                moduleReasons.push(...rangeEval.reasonCodes);
                if (rangeEval.killSwitchTriggered) {
                    const untilMs = nowMs + cfg.modules.rangeFadeKillSwitchCooldownMinutes * 60_000;
                    await setForexRangeFadeCooldown(row.pair, untilMs);
                    moduleReasons.push('RANGE_FADE_DISABLED_UNTIL_NEXT_REEVAL');
                    continue;
                }
                signal = rangeEval.signal;
            }
            if (signal) break;
        }

        return {
            signal,
            reasonCodes: Array.from(new Set(moduleReasons)),
        };
    };

    const eventTierState = (pair: string, riskState: ForexRegimePacket['risk_state'] | undefined) => {
        const blockGate = evaluateForexEventGate({
            pair,
            events,
            staleData: eventState.stale,
            riskState,
            nowMs,
            blockedImpacts: cfg.events.blockNewImpacts,
        });
        const forceCloseGate = evaluateForexEventGate({
            pair,
            events,
            staleData: eventState.stale,
            riskState,
            nowMs,
            blockedImpacts: cfg.events.forceCloseImpacts,
        });
        const tightenGate = evaluateForexEventGate({
            pair,
            events,
            staleData: eventState.stale,
            riskState,
            nowMs,
            blockedImpacts: cfg.events.tightenOnlyImpacts,
        });

        const forceCloseActive = hasActiveEventWindow(forceCloseGate.reasonCodes);
        const tightenOnlyActive = hasActiveEventWindow(tightenGate.reasonCodes);
        const mediumActive = tightenOnlyActive || eventHasImpact(blockGate.matchedEvents, 'MEDIUM');
        return {
            blockGate,
            forceCloseGate,
            tightenGate,
            forceCloseActive,
            mediumActive,
        };
    };

    const buildPositionContext = (params: {
        pair: string;
        packet: ForexRegimePacket;
        signal: ForexModuleSignal;
        sideSizeUsd: number;
        leverage: number;
    }): ForexPositionContext => {
        const riskDistance = Math.abs(params.signal.entryPrice - params.signal.stopPrice);
        const tp1Price =
            Number.isFinite(Number(params.signal.tp1Price)) && Number(params.signal.tp1Price) > 0
                ? Number(params.signal.tp1Price)
                : params.signal.module === 'range_fade'
                  ? null
                  : params.signal.side === 'BUY'
                    ? params.signal.entryPrice + riskDistance
                    : params.signal.entryPrice - riskDistance;
        const tp2Price =
            Number.isFinite(Number(params.signal.tp2Price)) && Number(params.signal.tp2Price) > 0
                ? Number(params.signal.tp2Price)
                : null;

        return {
            pair: params.pair,
            side: params.signal.side,
            entryModule: params.signal.module,
            entryPrice: params.signal.entryPrice,
            initialStopPrice: params.signal.stopPrice,
            currentStopPrice: params.signal.stopPrice,
            initialRiskPrice: riskDistance,
            partialTakenPct: 0,
            trailingActive: false,
            trailingMode: 'none',
            tp1Price,
            tp2Price,
            rangeLowerBoundary:
                Number.isFinite(Number(params.signal.rangeLowerBoundary)) && Number(params.signal.rangeLowerBoundary) > 0
                    ? Number(params.signal.rangeLowerBoundary)
                    : null,
            rangeUpperBoundary:
                Number.isFinite(Number(params.signal.rangeUpperBoundary)) && Number(params.signal.rangeUpperBoundary) > 0
                    ? Number(params.signal.rangeUpperBoundary)
                    : null,
            openedAtMs: nowMs,
            lastManagedAtMs: nowMs,
            lastCloseAtMs: null,
            module: params.signal.module,
            stopPrice: params.signal.stopPrice,
            updatedAtMs: nowMs,
            entryNotionalUsd: params.sideSizeUsd,
            entryLeverage: params.leverage,
            packet: packetForContext(params.packet),
        };
    };

    const persistContextIfChanged = async (pair: string, context: ForexPositionContext | null, changed: boolean) => {
        if (!changed || !context || dryRun) return;
        context.lastManagedAtMs = nowMs;
        context.updatedAtMs = nowMs;
        context.stopPrice = context.currentStopPrice;
        context.module = context.entryModule;
        await saveForexPositionContext(context);
    };

    const adjustUsageForClose = (pair: string, fraction: number) => {
        const clampedFraction = Math.max(0, Math.min(1, fraction));
        const pairRisk = openRiskUsage.pairOpenRiskPct[pair] || 0;
        if (!(pairRisk > 0)) return;
        const delta = pairRisk * clampedFraction;
        openRiskUsage.portfolioOpenRiskPct = Math.max(0, openRiskUsage.portfolioOpenRiskPct - delta);
        openRiskUsage.pairOpenRiskPct[pair] = Math.max(0, pairRisk - delta);
        const currencies = pair.slice(0, 6);
        for (const ccy of [currencies.slice(0, 3), currencies.slice(3, 6)]) {
            if (!ccy) continue;
            openRiskUsage.currencyOpenRiskPct[ccy] = Math.max(0, (openRiskUsage.currencyOpenRiskPct[ccy] || 0) - delta);
        }
    };

    for (const [pair, openPosition] of openByPair.entries()) {
        const row = rowsByPair.get(pair);
        const packet = packetByPair.get(pair) || null;
        const packetAge = packetAgeMinutes(packet, nowMs);
        const positionContext = contextsByPair.get(pair) ?? null;
        const openSide = sideFromOpenPosition(openPosition.side);
        const module = entryModuleFromContext(positionContext);

        if (!row) {
            results.push({
                pair,
                attempted: false,
                placed: false,
                dryRun,
                action: 'NONE',
                module: 'none',
                reasonCodes: ['OPEN_POSITION_PAIR_NOT_IN_SCAN'],
                orderId: null,
                clientOid: null,
                packet,
                managementAction: 'HOLD',
                packetAgeMinutes: packetAge,
            });
            continue;
        }

        const market = await getMarketState(pair);
        const midPrice =
            midPriceFromSnapshot(openPosition) ??
            (Number.isFinite(market.price) ? market.price : null) ??
            (Number.isFinite(row.metrics.price) ? row.metrics.price : null);
        const eventTier = eventTierState(pair, packet?.risk_state);
        const gate = eventTier.blockGate;

        let contextMutated = false;
        let exitAction: 'CLOSE' | null = null;
        let partialClosePct: number | null = null;
        let reentryLockNeeded = false;
        const exitReasonCodes: string[] = [];
        let managementAction: ForexExecutionResultSummary['managementAction'] = null;

        if (eventTier.forceCloseActive) {
            exitAction = 'CLOSE';
            exitReasonCodes.push('EVENT_HIGH_FORCE_CLOSE');
        }

        const stopBefore = currentStopFromContext(positionContext);
        const progress =
            positionContext && openSide
                ? computePositionProgress({
                      side: openSide,
                      entryPrice: positionContext.entryPrice,
                      initialStopPrice: Number(positionContext.initialStopPrice || stopBefore || positionContext.entryPrice),
                      openedAtMs: positionContext.openedAtMs,
                      nowMs,
                      candles5m: market.candles.m5,
                  })
                : {
                      rValue: NaN,
                      mfeR: NaN,
                      ageBars5m: 0,
                      ageHours: 0,
                      maxFavorablePrice: null,
                      minFavorablePrice: null,
                  };

        if (!exitAction && eventTier.mediumActive && positionContext && openSide) {
            const atr5m = Number(market.atr5m);
            const currentStop = currentStopFromContext(positionContext);
            if (Number.isFinite(midPrice as number) && Number.isFinite(atr5m) && atr5m > 0 && Number.isFinite(currentStop as number)) {
                if (openSide === 'BUY') {
                    const tightened = Number(midPrice) - atr5m * 0.6;
                    if (tightened > Number(currentStop)) {
                        positionContext.currentStopPrice = tightened;
                        contextMutated = true;
                    }
                } else {
                    const tightened = Number(midPrice) + atr5m * 0.6;
                    if (tightened < Number(currentStop)) {
                        positionContext.currentStopPrice = tightened;
                        contextMutated = true;
                    }
                }
            }
            if (contextMutated) {
                exitReasonCodes.push('EVENT_MEDIUM_TIGHTEN_STOP');
                managementAction = 'TIGHTEN_STOP';
            }
        }

        if (!exitAction && positionContext && openSide) {
            const noFollow = shouldTimeStopNoFollowThrough({
                ageBars5m: progress.ageBars5m,
                mfeR: Number(progress.mfeR),
                thresholdBars: cfg.timeStop.noFollowBars,
                minFollowR: cfg.timeStop.minFollowR,
            });
            if (noFollow) {
                exitAction = 'CLOSE';
                exitReasonCodes.push('CLOSE_TIME_STOP_NO_PROGRESS');
            }
        }

        if (!exitAction && positionContext && openSide) {
            const maxHold = shouldTimeStopMaxHold({
                ageHours: progress.ageHours,
                maxHoldHours: cfg.timeStop.maxHoldHours,
                trendAligned: isTrendAlignedForHold(openSide, packet),
                trailingActive: Boolean(positionContext.trailingActive),
            });
            if (maxHold) {
                exitAction = 'CLOSE';
                exitReasonCodes.push('CLOSE_TIME_STOP_MAX_HOLD');
            }
        }

        if (!exitAction && positionContext && openSide && module === 'range_fade' && Number.isFinite(midPrice as number)) {
            const mid = Number(midPrice);
            const tp1 = Number(positionContext.tp1Price);
            const tp2 = Number(positionContext.tp2Price);

            if (positionContext.partialTakenPct < 50 && Number.isFinite(tp1) && ((openSide === 'BUY' && mid >= tp1) || (openSide === 'SELL' && mid <= tp1))) {
                partialClosePct = 50;
                positionContext.partialTakenPct = 50;
                positionContext.currentStopPrice = positionContext.entryPrice;
                positionContext.trailingMode = 'range_protective';
                positionContext.trailingActive = false;
                contextMutated = true;
                exitReasonCodes.push('CLOSE_RANGE_TP1_MID');
                managementAction = 'CLOSE_PARTIAL';
            } else if (
                positionContext.partialTakenPct >= 50 &&
                Number.isFinite(tp2) &&
                ((openSide === 'BUY' && mid >= tp2) || (openSide === 'SELL' && mid <= tp2))
            ) {
                exitAction = 'CLOSE';
                exitReasonCodes.push('CLOSE_RANGE_TP2_BOUNDARY');
            }

            if (!exitAction) {
                const lastClose = safeNumber(market.candles.m5.at(-1)?.[4]);
                const lower = safeNumber(positionContext.rangeLowerBoundary);
                const upper = safeNumber(positionContext.rangeUpperBoundary);
                const boundaryBuffer = Number(market.atr5m) * cfg.modules.rangeFadeBoundaryAtrBuffer;
                const breakDown = Number.isFinite(lower) && Number.isFinite(lastClose) && lastClose < lower - boundaryBuffer;
                const breakUp = Number.isFinite(upper) && Number.isFinite(lastClose) && lastClose > upper + boundaryBuffer;
                if ((openSide === 'BUY' && breakDown) || (openSide === 'SELL' && breakUp)) {
                    exitAction = 'CLOSE';
                    exitReasonCodes.push('CLOSE_RANGE_BREAK_INVALIDATION');
                }
            }
        }

        if (!exitAction && positionContext && openSide && (module === 'pullback' || module === 'breakout_retest')) {
            if (positionContext.partialTakenPct < 50 && Number(progress.mfeR) >= 1) {
                partialClosePct = 50;
                positionContext.partialTakenPct = 50;
                exitReasonCodes.push('CLOSE_PARTIAL_1R');
                managementAction = 'CLOSE_PARTIAL';

                const currentStop = currentStopFromContext(positionContext);
                const breakeven = positionContext.entryPrice;
                if (Number.isFinite(currentStop as number)) {
                    if (openSide === 'BUY' && breakeven > Number(currentStop)) {
                        positionContext.currentStopPrice = breakeven;
                        exitReasonCodes.push('STOP_MOVE_BREAKEVEN');
                    } else if (openSide === 'SELL' && breakeven < Number(currentStop)) {
                        positionContext.currentStopPrice = breakeven;
                        exitReasonCodes.push('STOP_MOVE_BREAKEVEN');
                    }
                }
                positionContext.trailingActive = true;
                positionContext.trailingMode = 'structure';
                contextMutated = true;
            }

            if (positionContext.trailingActive) {
                const trail = computeTrendTrailStop({
                    side: openSide,
                    currentStop: Number(positionContext.currentStopPrice),
                    candles5m: market.candles.m5,
                    atr5m: market.atr5m,
                });
                if (Number.isFinite(Number(trail.nextStop))) {
                    positionContext.currentStopPrice = Number(trail.nextStop);
                    contextMutated = true;
                    if (trail.reasonCode) exitReasonCodes.push(trail.reasonCode);
                    managementAction = 'TIGHTEN_STOP';
                }
            }

            if (packet?.htf_context) {
                const nearResistance = Number(packet.htf_context.distance_to_resistance_atr1h);
                const nearSupport = Number(packet.htf_context.distance_to_support_atr1h);
                const currentStop = Number(positionContext.currentStopPrice);
                if (openSide === 'BUY' && Number.isFinite(nearResistance) && nearResistance <= 0.35 && Number.isFinite(midPrice as number)) {
                    const tightened = Number(midPrice) - Math.max(1e-9, Number(market.atr5m)) * 0.4;
                    if (Number.isFinite(tightened) && tightened > currentStop) {
                        positionContext.currentStopPrice = tightened;
                        contextMutated = true;
                        exitReasonCodes.push('STOP_TIGHTEN_HTF_ZONE');
                        managementAction = 'TIGHTEN_STOP';
                    }
                }
                if (openSide === 'SELL' && Number.isFinite(nearSupport) && nearSupport <= 0.35 && Number.isFinite(midPrice as number)) {
                    const tightened = Number(midPrice) + Math.max(1e-9, Number(market.atr5m)) * 0.4;
                    if (Number.isFinite(tightened) && tightened < currentStop) {
                        positionContext.currentStopPrice = tightened;
                        contextMutated = true;
                        exitReasonCodes.push('STOP_TIGHTEN_HTF_ZONE');
                        managementAction = 'TIGHTEN_STOP';
                    }
                }
            }
        }

        if (!exitAction) {
            const stopInvalidation = shouldInvalidateByStop({
                context: positionContext,
                openSide,
                midPrice,
            });
            if (stopInvalidation.invalidated) {
                exitAction = 'CLOSE';
                exitReasonCodes.push(stopInvalidation.reasonCode || 'STOP_INVALIDATED_CLOSE');
            }
        }

        if (!exitAction) {
            const fallbackInvalidation = shouldInvalidateFallback({
                openSide,
                packet,
                trendDirection1h: market?.trendDirection1h ?? 'neutral',
            });
            if (fallbackInvalidation.invalidated) {
                exitAction = 'CLOSE';
                exitReasonCodes.push(fallbackInvalidation.reasonCode || 'STRUCTURE_INVALIDATION_CLOSE');
            }
        }

        if (!exitAction && packet) {
            const flatPermission = packet.permission === 'flat' || packet.allowed_modules.includes('none');
            if (flatPermission) {
                exitAction = 'CLOSE';
                exitReasonCodes.push('REGIME_FLAT_CLOSE');
            } else if (openSide && isOppositePermission(openSide, packet.permission)) {
                exitAction = 'CLOSE';
                reentryLockNeeded = true;
                exitReasonCodes.push('REGIME_FLIP_CLOSE');
            }
        }

        if (exitAction) {
            partialClosePct = null;
        }

        const reasonCodesBase = Array.from(new Set([...exitReasonCodes, ...gate.reasonCodes]));

        if (!exitAction && !partialClosePct) {
            const reasonCodes = Array.from(new Set(['OPEN_POSITION_HELD_NO_EXIT_TRIGGER', ...reasonCodesBase]));
            results.push({
                pair,
                attempted: false,
                placed: false,
                dryRun,
                action: 'NONE',
                module: 'none',
                reasonCodes,
                orderId: null,
                clientOid: null,
                packet,
                managementAction: managementAction ?? 'HOLD',
                packetAgeMinutes: packetAge,
            });
            await persistContextIfChanged(pair, positionContext, contextMutated);
            await safeAppendJournal(
                journalEntry({
                    type: 'execution',
                    pair,
                    level: 'info',
                    reasonCodes,
                    payload: {
                        gate,
                        eventTier: {
                            forceCloseActive: eventTier.forceCloseActive,
                            mediumActive: eventTier.mediumActive,
                        },
                        packet,
                        packetAgeMinutes: packetAge,
                        openPosition,
                        positionContext,
                        progress,
                        dryRun,
                        phase: 'exit',
                    },
                }),
            );
            continue;
        }

        const shouldExecuteClose = exitAction === 'CLOSE' || (Number.isFinite(partialClosePct as number) && Number(partialClosePct) > 0);
        const decision =
            shouldExecuteClose
                ? {
                      action: 'CLOSE',
                      summary: partialClosePct ? 'managed_partial_close' : 'managed_close',
                      reason: reasonCodesBase.join('|') || 'managed_close',
                      leverage: null,
                      signal_strength: 'LOW' as const,
                      partial_close_pct: partialClosePct ?? undefined,
                  }
                : null;

        const exec = decision ? await executeCapitalDecision(pair, notionalUsd, decision as any, dryRun) : { placed: false };
        const placed = Boolean((exec as any)?.placed);
        const reasonCodes = Array.from(new Set(reasonCodesBase));
        const action = decision ? 'CLOSE' : 'NONE';
        results.push({
            pair,
            attempted: Boolean(decision),
            placed,
            dryRun,
            action,
            module: 'none',
            reasonCodes,
            orderId: (exec as any)?.orderId ?? null,
            clientOid: (exec as any)?.clientOid ?? null,
            packet,
            managementAction: partialClosePct ? 'CLOSE_PARTIAL' : 'CLOSE_FULL',
            packetAgeMinutes: packetAge,
        });

        await safeAppendJournal(
            journalEntry({
                type: 'execution',
                pair,
                level: placed ? 'info' : 'warn',
                reasonCodes,
                payload: {
                    gate,
                    eventTier: {
                        forceCloseActive: eventTier.forceCloseActive,
                        mediumActive: eventTier.mediumActive,
                    },
                    packet,
                    packetAgeMinutes: packetAge,
                    openPosition,
                    positionContext,
                    progress,
                    decision,
                    execution: exec,
                    dryRun,
                    phase: 'exit',
                },
            }),
        );

        if (!dryRun && placed) {
            if (partialClosePct && positionContext) {
                positionContext.lastCloseAtMs = nowMs;
                positionContext.lastManagedAtMs = nowMs;
                positionContext.updatedAtMs = nowMs;
                positionContext.stopPrice = positionContext.currentStopPrice;
                await saveForexPositionContext(positionContext);
                adjustUsageForClose(pair, partialClosePct / 100);
            } else if (exitAction === 'CLOSE') {
                await deleteForexPositionContext(pair);
                contextsByPair.delete(pair);
                adjustUsageForClose(pair, 1);
                if (reentryLockNeeded) {
                    const lockMinutes = Math.max(cfg.reentry.lockMinutes, cfg.cadence.executeMinutes);
                    await setForexReentryLock(pair, nowMs + lockMinutes * 60_000);
                } else {
                    await clearForexReentryLock(pair);
                }
                const currencies = pair.slice(0, 6);
                const base = currencies.slice(0, 3);
                const quote = currencies.slice(3, 6);
                if (base) exposure[base] = Math.max(0, (exposure[base] || 0) - 1);
                if (quote) exposure[quote] = Math.max(0, (exposure[quote] || 0) - 1);
            }
        } else {
            await persistContextIfChanged(pair, positionContext, contextMutated);
        }
    }

    const eligibleRows = scan.pairs.filter((item) => item.eligible);
    const topPercent = cfg.selector.topPercent;
    const totalEligibleRows = eligibleRows.length;

    for (const row of eligibleRows) {
        const packet = packetByPair.get(row.pair) || null;
        const packetAge = packetAgeMinutes(packet, nowMs);
        if (!packet) {
            results.push({
                pair: row.pair,
                attempted: false,
                placed: false,
                dryRun,
                action: 'NONE',
                module: 'none',
                reasonCodes: ['NO_PACKET_AVAILABLE'],
                orderId: null,
                clientOid: null,
                packet: null,
                managementAction: null,
                packetAgeMinutes: null,
            });
            continue;
        }

        if (openByPair.has(row.pair)) {
            continue;
        }

        const selectorAllowed = isWithinSelectorTopPercentile({
            rank: row.rank,
            totalRows: totalEligibleRows,
            topPercent,
        });
        if (!selectorAllowed) {
            const reasonCodes = ['NO_TRADE_SELECTOR_PERCENTILE'];
            results.push({
                pair: row.pair,
                attempted: false,
                placed: false,
                dryRun,
                action: 'NONE',
                module: 'none',
                reasonCodes,
                orderId: null,
                clientOid: null,
                packet,
                managementAction: null,
                packetAgeMinutes: packetAge,
            });
            await safeAppendJournal(
                journalEntry({
                    type: 'execution',
                    pair: row.pair,
                    level: 'info',
                    reasonCodes,
                    payload: { packet, packetAgeMinutes: packetAge, row, dryRun, phase: 'entry' },
                }),
            );
            continue;
        }

        const reentryLockUntil = await getForexReentryLockUntil(row.pair);
        if (Number.isFinite(reentryLockUntil as number) && Number(reentryLockUntil) > nowMs) {
            const reasonCodes = ['REENTRY_NEXT_BAR_LOCK'];
            results.push({
                pair: row.pair,
                attempted: false,
                placed: false,
                dryRun,
                action: 'NONE',
                module: 'none',
                reasonCodes,
                orderId: null,
                clientOid: null,
                packet,
                managementAction: null,
                packetAgeMinutes: packetAge,
            });
            await safeAppendJournal(
                journalEntry({
                    type: 'execution',
                    pair: row.pair,
                    level: 'info',
                    reasonCodes,
                    payload: {
                        packet,
                        packetAgeMinutes: packetAge,
                        reentryLockUntil,
                        dryRun,
                        phase: 'entry',
                    },
                }),
            );
            continue;
        } else if (!dryRun && Number.isFinite(reentryLockUntil as number) && Number(reentryLockUntil) <= nowMs) {
            await clearForexReentryLock(row.pair);
        }

        if (isPacketStale(packet, nowMs, stalePacketMinutes)) {
            const reasonCodes = ['NO_TRADE_PACKET_STALE'];
            results.push({
                pair: row.pair,
                attempted: false,
                placed: false,
                dryRun,
                action: 'NONE',
                module: 'none',
                reasonCodes,
                orderId: null,
                clientOid: null,
                packet,
                managementAction: null,
                packetAgeMinutes: packetAge,
            });
            await safeAppendJournal(
                journalEntry({
                    type: 'execution',
                    pair: row.pair,
                    level: 'info',
                    reasonCodes,
                    payload: { packet, packetAgeMinutes: packetAge, staleThresholdMinutes: stalePacketMinutes, dryRun, phase: 'entry' },
                }),
            );
            continue;
        }

        const gate = evaluateForexEventGate({
            pair: row.pair,
            events,
            staleData: eventState.stale,
            riskState: packet.risk_state,
            nowMs,
            blockedImpacts: cfg.events.blockNewImpacts,
        });
        const eventReasons: string[] = [];
        if (gate.blockNewEntries && eventHasImpact(gate.matchedEvents, 'MEDIUM')) {
            eventReasons.push('EVENT_MEDIUM_BLOCK_NEW');
        }

        const risk = await evaluateForexRiskCheck({
            pair: row.pair,
            metrics: row.metrics,
            eventGate: gate,
            nowMs,
            exposure,
        });

        if (!risk.allowEntry || packet.permission === 'flat' || packet.allowed_modules.includes('none')) {
            const reasonCodes = Array.from(new Set([...risk.reasonCodes, ...gate.reasonCodes, ...eventReasons, 'ENTRY_BLOCKED']));
            results.push({
                pair: row.pair,
                attempted: false,
                placed: false,
                dryRun,
                action: 'NONE',
                module: 'none',
                reasonCodes,
                orderId: null,
                clientOid: null,
                packet,
                managementAction: null,
                packetAgeMinutes: packetAge,
            });
            await safeAppendJournal(
                journalEntry({
                    type: 'execution',
                    pair: row.pair,
                    level: 'info',
                    reasonCodes,
                    payload: { gate, risk, packet, packetAgeMinutes: packetAge, dryRun, phase: 'entry' },
                }),
            );
            continue;
        }

        const moduleEval = await selectModuleSignal(row, packet);
        const signal = moduleEval.signal;

        if (!signal) {
            const reasonCodes = Array.from(new Set([...moduleEval.reasonCodes, 'NO_MODULE_SIGNAL']));
            results.push({
                pair: row.pair,
                attempted: false,
                placed: false,
                dryRun,
                action: 'NONE',
                module: 'none',
                reasonCodes,
                orderId: null,
                clientOid: null,
                packet,
                managementAction: null,
                packetAgeMinutes: packetAge,
            });
            await safeAppendJournal(
                journalEntry({
                    type: 'execution',
                    pair: row.pair,
                    level: 'info',
                    reasonCodes,
                    payload: { gate, risk, packet, packetAgeMinutes: packetAge, dryRun, phase: 'entry' },
                }),
            );
            continue;
        }

        const sizing = computeHybridRiskSize({
            entryPrice: signal.entryPrice,
            stopPrice: signal.stopPrice,
            confidence: packet.confidence,
            fallbackNotionalUsd: notionalUsd,
            maxLeverage: cfg.risk.maxLeveragePerPair,
            riskPerTradePct: cfg.risk.riskPerTradePct,
            referenceEquityUsd,
        });
        const candidateRiskPct =
            Number.isFinite(Number(sizing.riskPctUsed)) && Number(sizing.riskPctUsed) > 0
                ? Number(sizing.riskPctUsed)
                : cfg.risk.riskPerTradePct;
        const riskCap = evaluateRiskCapBudget({
            pair: row.pair,
            candidateRiskPct,
            usage: openRiskUsage,
            maxPortfolioOpenPct: cfg.risk.maxPortfolioOpenPct,
            maxCurrencyOpenPct: cfg.risk.maxCurrencyOpenPct,
        });
        if (!riskCap.allow) {
            const reasonCodes = Array.from(new Set([...riskCap.reasonCodes, ...sizing.reasonCodes, 'ENTRY_BLOCKED']));
            results.push({
                pair: row.pair,
                attempted: false,
                placed: false,
                dryRun,
                action: 'NONE',
                module: 'none',
                reasonCodes,
                orderId: null,
                clientOid: null,
                packet,
                managementAction: null,
                packetAgeMinutes: packetAge,
            });
            await safeAppendJournal(
                journalEntry({
                    type: 'execution',
                    pair: row.pair,
                    level: 'warn',
                    reasonCodes,
                    payload: {
                        packet,
                        packetAgeMinutes: packetAge,
                        riskCap,
                        sizing,
                        openRiskUsage,
                        dryRun,
                        phase: 'entry',
                    },
                }),
            );
            continue;
        }

        const leverage = confidenceToLeverage(packet.confidence, cfg.risk.maxLeveragePerPair);
        const decision = {
            action: signal.side,
            summary: `${signal.module}_${signal.side.toLowerCase()}`,
            reason: signal.reasonCodes.join('|'),
            leverage,
            signal_strength: confidenceToSignalStrength(packet.confidence),
        } as const;

        const orderNotionalUsd = Number.isFinite(Number(sizing.sideSizeUsd)) && Number(sizing.sideSizeUsd) > 0 ? Number(sizing.sideSizeUsd) : notionalUsd;
        const exec = await executeCapitalDecision(row.pair, orderNotionalUsd, decision as any, dryRun);

        const result: ForexExecutionResultSummary = {
            pair: row.pair,
            attempted: true,
            placed: Boolean((exec as any)?.placed),
            dryRun,
            action: signal.side,
            module: signal.module,
            reasonCodes: Array.from(
                new Set([
                    ...signal.reasonCodes,
                    ...risk.reasonCodes,
                    ...gate.reasonCodes,
                    ...eventReasons,
                    ...moduleEval.reasonCodes,
                    ...sizing.reasonCodes,
                ]),
            ),
            orderId: (exec as any)?.orderId ?? null,
            clientOid: (exec as any)?.clientOid ?? null,
            packet,
            managementAction: null,
            packetAgeMinutes: packetAge,
        };
        results.push(result);

        await safeAppendJournal(
            journalEntry({
                type: 'execution',
                pair: row.pair,
                level: result.placed ? 'info' : 'warn',
                reasonCodes: result.reasonCodes,
                payload: {
                    signal,
                    gate,
                    risk,
                    packet,
                    packetAgeMinutes: packetAge,
                    sizing,
                    riskCap,
                    decision,
                    orderNotionalUsd,
                    execution: exec,
                    dryRun,
                    phase: 'entry',
                },
            }),
        );

        if (!dryRun && result.placed) {
            const context = buildPositionContext({
                pair: row.pair,
                packet,
                signal,
                sideSizeUsd: orderNotionalUsd,
                leverage,
            });
            await saveForexPositionContext(context);
            contextsByPair.set(row.pair, context);
            await clearForexReentryLock(row.pair);
            openRiskUsage.portfolioOpenRiskPct += candidateRiskPct;
            openRiskUsage.pairOpenRiskPct[row.pair] = (openRiskUsage.pairOpenRiskPct[row.pair] || 0) + candidateRiskPct;
            const pairClean = row.pair.replace(/[^A-Z]/g, '').slice(0, 6);
            const base = pairClean.slice(0, 3);
            const quote = pairClean.slice(3, 6);
            if (base) {
                openRiskUsage.currencyOpenRiskPct[base] = (openRiskUsage.currencyOpenRiskPct[base] || 0) + candidateRiskPct;
                exposure[base] = (exposure[base] || 0) + 1;
            }
            if (quote) {
                openRiskUsage.currencyOpenRiskPct[quote] = (openRiskUsage.currencyOpenRiskPct[quote] || 0) + candidateRiskPct;
                exposure[quote] = (exposure[quote] || 0) + 1;
            }
        }
    }

    await safeAppendJournal(
        journalEntry({
            type: 'execution',
            reasonCodes: ['FOREX_EXECUTION_CYCLE_COMPLETED'],
            payload: {
                generatedAtMs: nowMs,
                dryRun,
                notionalUsd,
                attemptedPairs: results.filter((r) => r.attempted).map((r) => r.pair),
                placedPairs: results.filter((r) => r.placed).map((r) => r.pair),
                openRiskUsage,
            },
        }),
    );

    return {
        generatedAtMs: nowMs,
        dryRun,
        notionalUsd,
        results,
    };
}
