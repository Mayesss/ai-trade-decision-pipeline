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
import { buildOpenCurrencyExposure, evaluateForexRiskCheck } from './risk';
import {
    appendForexJournal,
    deleteForexPositionContext,
    getForexRangeFadeCooldownUntil,
    loadForexPositionContext,
    loadForexPacketSnapshot,
    loadForexScanSnapshot,
    saveForexPositionContext,
    setForexRangeFadeCooldown,
    saveForexPacketSnapshot,
    saveForexScanSnapshot,
} from './store';
import { runForexUniverseScan } from './selector';
import type {
    ForexExecutionResultSummary,
    ForexJournalEntry,
    ForexModuleSignal,
    ForexPairEligibility,
    ForexPacketSnapshot,
    ForexPositionContext,
    ForexRegimePacket,
    ForexRiskCheck,
    ForexSide,
    ForexScanSnapshot,
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

function confidenceToLeverage(confidence: number): number {
    if (confidence >= 0.85) return 3;
    if (confidence >= 0.68) return 2;
    return 1;
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
    if (!(Number.isFinite(context.stopPrice) && context.stopPrice > 0)) {
        return { invalidated: false };
    }
    if (!(Number.isFinite(midPrice as number) && (midPrice as number) > 0)) {
        return { invalidated: false };
    }
    if (openSide === 'BUY' && Number(midPrice) <= context.stopPrice) {
        return { invalidated: true, reasonCode: 'STOP_INVALIDATED_LONG' };
    }
    if (openSide === 'SELL' && Number(midPrice) >= context.stopPrice) {
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
    const notionalUsd = Number.isFinite(opts.notionalUsd as number)
        ? Number(opts.notionalUsd)
        : cfg.defaultNotionalUsd;
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

    const marketStateCache = new Map<string, Awaited<ReturnType<typeof loadForexPairMarketState>>>();
    const getMarketState = async (pair: string) => {
        if (marketStateCache.has(pair)) return marketStateCache.get(pair)!;
        const market = await loadForexPairMarketState(pair, nowMs);
        marketStateCache.set(pair, market);
        return market;
    };

    const selectModuleSignal = async (row: ForexPairEligibility, packet: ForexRegimePacket) => {
        const market = await getMarketState(row.pair);
        const moduleOrder = packet.allowed_modules.filter(
            (m) => m === 'pullback' || m === 'breakout_retest' || m === 'range_fade',
        );
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
            reasonCodes: moduleReasons,
        };
    };

    const savePositionContext = async (params: { pair: string; packet: ForexRegimePacket; signal: ForexModuleSignal }) => {
        if (dryRun) return;
        const context: ForexPositionContext = {
            pair: params.pair,
            side: params.signal.side,
            module: params.signal.module,
            entryPrice: params.signal.entryPrice,
            stopPrice: params.signal.stopPrice,
            openedAtMs: nowMs,
            updatedAtMs: nowMs,
            packet: packetForContext(params.packet),
        };
        await saveForexPositionContext(context);
    };

    for (const [pair, openPosition] of openByPair.entries()) {
        const row = rowsByPair.get(pair);
        const packet = packetByPair.get(pair) || null;
        const positionContext = await loadForexPositionContext(pair);
        const openSide = sideFromOpenPosition(openPosition.side);

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
            });
            continue;
        }

        const gate = evaluateForexEventGate({
            pair,
            events,
            staleData: eventState.stale,
            riskState: packet?.risk_state,
            nowMs,
        });

        let exitAction: 'CLOSE' | 'REVERSE' | null = null;
        let reverseSignal: ForexModuleSignal | null = null;
        let reverseRisk: ForexRiskCheck | null = null;
        const exitReasonCodes: string[] = [];

        if (hasActiveEventWindow(gate.reasonCodes)) {
            exitAction = 'CLOSE';
            exitReasonCodes.push('EVENT_RISK_FLATTEN_CLOSE');
        }

        const market = exitAction ? null : await getMarketState(pair);
        const midPrice =
            midPriceFromSnapshot(openPosition) ??
            (market && Number.isFinite(market.price) ? market.price : null) ??
            (Number.isFinite(row.metrics.price) ? row.metrics.price : null);

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
                const reverseRejects: string[] = [];
                if (packet.risk_state === 'extreme') reverseRejects.push('REVERSE_BLOCKED_RISK_EXTREME');
                if (packet.confidence < 0.68) reverseRejects.push('REVERSE_BLOCKED_CONFIDENCE_LT_068');
                if (gate.blockNewEntries || !gate.allowNewEntries) reverseRejects.push('REVERSE_BLOCKED_EVENT_GATE');

                if (!reverseRejects.length) {
                    reverseRisk = await evaluateForexRiskCheck({
                        pair,
                        metrics: row.metrics,
                        eventGate: gate,
                        nowMs,
                        exposure,
                    });
                    if (!reverseRisk.allowEntry) {
                        reverseRejects.push(...reverseRisk.reasonCodes, 'REVERSE_BLOCKED_RISK_CHECK');
                    }
                }

                if (!reverseRejects.length) {
                    const moduleEval = await selectModuleSignal(row, packet);
                    const expectedSide: ForexSide = openSide === 'BUY' ? 'SELL' : 'BUY';
                    reverseSignal = moduleEval.signal;
                    if (!reverseSignal) {
                        reverseRejects.push(...moduleEval.reasonCodes, 'REVERSE_BLOCKED_NO_MODULE_SIGNAL');
                    } else if (reverseSignal.side !== expectedSide) {
                        reverseRejects.push(...moduleEval.reasonCodes, 'REVERSE_BLOCKED_SIGNAL_NOT_OPPOSITE');
                    }
                }

                if (!reverseRejects.length && reverseSignal) {
                    exitAction = 'REVERSE';
                    exitReasonCodes.push('REGIME_FLIP_REVERSE');
                } else {
                    exitAction = 'CLOSE';
                    exitReasonCodes.push('REGIME_FLIP_CLOSE', ...reverseRejects);
                }
            }
        }

        if (!exitAction) {
            const reasonCodes = Array.from(new Set(['OPEN_POSITION_HELD_NO_EXIT_TRIGGER', ...gate.reasonCodes]));
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
            });
            await safeAppendJournal(
                journalEntry({
                    type: 'execution',
                    pair,
                    level: 'info',
                    reasonCodes,
                    payload: {
                        gate,
                        packet,
                        openPosition,
                        positionContext,
                        dryRun,
                        phase: 'exit',
                    },
                }),
            );
            continue;
        }

        const decision =
            exitAction === 'REVERSE'
                ? {
                      action: 'REVERSE',
                      summary: 'regime_flip_reverse',
                      reason: exitReasonCodes.join('|'),
                      leverage: confidenceToLeverage(packet?.confidence ?? 0.68),
                      signal_strength: confidenceToSignalStrength(packet?.confidence ?? 0.68),
                  }
                : {
                      action: 'CLOSE',
                      summary: 'risk_exit_close',
                      reason: exitReasonCodes.join('|'),
                      leverage: null,
                      signal_strength: 'LOW' as const,
                  };

        const exec = await executeCapitalDecision(pair, notionalUsd, decision as any, dryRun);
        const reasonCodes = Array.from(
            new Set([
                ...exitReasonCodes,
                ...gate.reasonCodes,
                ...(reverseRisk?.reasonCodes || []),
                ...(reverseSignal?.reasonCodes || []),
            ]),
        );
        const placed = Boolean((exec as any)?.placed);
        results.push({
            pair,
            attempted: true,
            placed,
            dryRun,
            action: exitAction,
            module: exitAction === 'REVERSE' ? reverseSignal?.module ?? 'none' : 'none',
            reasonCodes,
            orderId: (exec as any)?.orderId ?? null,
            clientOid: (exec as any)?.clientOid ?? null,
            packet,
        });

        await safeAppendJournal(
            journalEntry({
                type: 'execution',
                pair,
                level: placed ? 'info' : 'warn',
                reasonCodes,
                payload: {
                    gate,
                    packet,
                    openPosition,
                    positionContext,
                    reverseRisk,
                    reverseSignal,
                    decision,
                    execution: exec,
                    dryRun,
                    phase: 'exit',
                },
            }),
        );

        if (!dryRun && placed) {
            if (exitAction === 'CLOSE') {
                await deleteForexPositionContext(pair);
            } else if (exitAction === 'REVERSE' && reverseSignal && packet) {
                await savePositionContext({ pair, packet, signal: reverseSignal });
            }
        }
    }

    for (const row of scan.pairs.filter((item) => item.eligible)) {
        const packet = packetByPair.get(row.pair) || null;
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
            });
            continue;
        }

        if (openByPair.has(row.pair)) {
            continue;
        }

        const gate = evaluateForexEventGate({
            pair: row.pair,
            events,
            staleData: eventState.stale,
            riskState: packet.risk_state,
            nowMs,
        });

        const risk = await evaluateForexRiskCheck({
            pair: row.pair,
            metrics: row.metrics,
            eventGate: gate,
            nowMs,
            exposure,
        });

        if (!risk.allowEntry || packet.permission === 'flat' || packet.allowed_modules.includes('none')) {
            const reasonCodes = Array.from(new Set([...risk.reasonCodes, ...gate.reasonCodes, 'ENTRY_BLOCKED']));
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
            });
            await safeAppendJournal(
                journalEntry({
                    type: 'execution',
                    pair: row.pair,
                    level: 'info',
                    reasonCodes,
                    payload: { gate, risk, packet, dryRun, phase: 'entry' },
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
            });
            await safeAppendJournal(
                journalEntry({
                    type: 'execution',
                    pair: row.pair,
                    level: 'info',
                    reasonCodes,
                    payload: { gate, risk, packet, dryRun, phase: 'entry' },
                }),
            );
            continue;
        }

        const decision = {
            action: signal.side,
            summary: `${signal.module}_${signal.side.toLowerCase()}`,
            reason: signal.reasonCodes.join('|'),
            leverage: confidenceToLeverage(packet.confidence),
            signal_strength: confidenceToSignalStrength(packet.confidence),
        } as const;

        const exec = await executeCapitalDecision(row.pair, notionalUsd, decision as any, dryRun);

        const result: ForexExecutionResultSummary = {
            pair: row.pair,
            attempted: true,
            placed: Boolean((exec as any)?.placed),
            dryRun,
            action: signal.side,
            module: signal.module,
            reasonCodes: Array.from(new Set([...signal.reasonCodes, ...risk.reasonCodes, ...gate.reasonCodes])),
            orderId: (exec as any)?.orderId ?? null,
            clientOid: (exec as any)?.clientOid ?? null,
            packet,
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
                    decision,
                    execution: exec,
                    dryRun,
                    phase: 'entry',
                },
            }),
        );

        if (result.placed) {
            await savePositionContext({
                pair: row.pair,
                packet,
                signal,
            });
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
