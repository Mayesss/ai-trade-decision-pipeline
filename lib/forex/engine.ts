import crypto from 'crypto';

import { executeCapitalDecision } from '../capital';
import { getForexStrategyConfig, getForexUniversePairs } from './config';
import { buildForexPacketSnapshot } from './ai';
import { refreshForexEvents, getForexEventsState } from './events/forexFactory';
import { evaluateForexEventGate } from './events/gate';
import { loadForexPairMarketState } from './marketData';
import { evaluateBreakoutRetestModule } from './modules/breakoutRetest';
import { evaluatePullbackModule } from './modules/pullback';
import { evaluateRangeFadeModule } from './modules/rangeFade';
import { buildOpenCurrencyExposure, evaluateForexRiskCheck } from './risk';
import {
    appendForexJournal,
    getForexRangeFadeCooldownUntil,
    loadForexPacketSnapshot,
    loadForexScanSnapshot,
    setForexRangeFadeCooldown,
    saveForexPacketSnapshot,
    saveForexScanSnapshot,
} from './store';
import { runForexUniverseScan } from './selector';
import type {
    ForexExecutionResultSummary,
    ForexJournalEntry,
    ForexModuleSignal,
    ForexPacketSnapshot,
    ForexRegimePacket,
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

export async function runForexExecuteCycle(opts: { nowMs?: number; dryRun?: boolean; notionalUsd?: number } = {}) {
    const nowMs = Number.isFinite(opts.nowMs as number) ? Number(opts.nowMs) : Date.now();
    const cfg = getForexStrategyConfig();
    const dryRun = opts.dryRun ?? cfg.dryRunDefault;
    const notionalUsd = Number.isFinite(opts.notionalUsd as number)
        ? Number(opts.notionalUsd)
        : cfg.defaultNotionalUsd;

    const scan = (await loadForexScanSnapshot()) ?? (await runForexScanCycle({ nowMs }));
    const packets = (await loadForexPacketSnapshot()) ?? (await runForexRegimeCycle({ nowMs }));
    const packetByPair = packetMap(packets);
    const eventState = await ensureEventState(nowMs);
    const events = eventState.snapshot?.events ?? [];

    const universe = getForexUniversePairs();
    const exposure = await buildOpenCurrencyExposure(universe);

    const results: ForexExecutionResultSummary[] = [];

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
                    payload: { gate, risk, packet, dryRun },
                }),
            );
            continue;
        }

        const market = await loadForexPairMarketState(row.pair, nowMs);

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

        if (!signal) {
            const reasonCodes = Array.from(new Set([...moduleReasons, 'NO_MODULE_SIGNAL']));
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
                    payload: { gate, risk, packet, dryRun },
                }),
            );
            continue;
        }

        const decision = {
            action: signal.side,
            summary: `${signal.module}_${signal.side.toLowerCase()}`,
            reason: signal.reasonCodes.join('|'),
            leverage: confidenceToLeverage(packet.confidence),
            signal_strength: packet.confidence >= 0.75 ? 'HIGH' : packet.confidence >= 0.6 ? 'MEDIUM' : 'LOW',
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
                },
            }),
        );
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
