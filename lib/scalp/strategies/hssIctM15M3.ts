import { buildAsiaRangeSnapshot, computeAtr, detectConfirmation, detectIfvg, detectIfvgTouch, detectSweepLifecycle } from '../detectors';
import { pipSizeForScalpSymbol } from '../marketData';
import type { ScalpCandle, ScalpDirectionalBias, ScalpSessionState } from '../types';
import type { ScalpStrategyDefinition, ScalpStrategyEntryIntent, ScalpStrategyPhaseInput, ScalpStrategyPhaseOutput } from './types';

function latestTs(candles: ScalpCandle[]): number | null {
    const ts = candles.at(-1)?.[0];
    return Number.isFinite(Number(ts)) ? Number(ts) : null;
}

function dedupeReasonCodes(codes: string[]): string[] {
    return Array.from(new Set(codes.map((code) => String(code || '').trim().toUpperCase()).filter((code) => code.length > 0)));
}

function withLastProcessed(state: ScalpSessionState, input: ScalpStrategyPhaseInput['market']): ScalpSessionState {
    const next = {
        ...state,
        lastProcessed: {
            ...state.lastProcessed,
        },
    };
    const baseTs = latestTs(input.baseCandles);
    const confirmTs = latestTs(input.confirmCandles);

    if (input.baseTf === 'M1') next.lastProcessed.m1ClosedTsMs = baseTs;
    if (input.baseTf === 'M3') next.lastProcessed.m3ClosedTsMs = baseTs;
    if (input.baseTf === 'M5') next.lastProcessed.m5ClosedTsMs = baseTs;
    if (input.baseTf === 'M15') next.lastProcessed.m15ClosedTsMs = baseTs;

    if (input.confirmTf === 'M1') next.lastProcessed.m1ClosedTsMs = confirmTs;
    if (input.confirmTf === 'M3') next.lastProcessed.m3ClosedTsMs = confirmTs;
    return next;
}

function expectedDirectionFromSweep(state: ScalpSessionState): ScalpDirectionalBias | null {
    if (!state.sweep) return null;
    return state.sweep.side === 'BUY_SIDE' ? 'BEARISH' : 'BULLISH';
}

function finalizePhase(params: {
    state: ScalpSessionState;
    reasonCodes: string[];
    entryIntent?: ScalpStrategyEntryIntent | null;
}): ScalpStrategyPhaseOutput {
    return {
        state: params.state,
        reasonCodes: dedupeReasonCodes(params.reasonCodes),
        entryIntent: params.entryIntent ?? null,
    };
}

function applyPhaseDetectors(params: ScalpStrategyPhaseInput): ScalpStrategyPhaseOutput {
    const reasonCodes: string[] = [];
    let next = withLastProcessed(params.state, params.market);

    if (next.state === 'IN_TRADE' || next.state === 'COOLDOWN') {
        return finalizePhase({
            state: next,
            reasonCodes: ['STATE_SKIPPED_MANAGED_EXTERNALLY'],
        });
    }
    if (next.state === 'DONE') {
        return finalizePhase({ state: next, reasonCodes: ['DAY_ALREADY_DONE'] });
    }

    if (!next.asiaRange) {
        const asia = buildAsiaRangeSnapshot({
            nowMs: params.nowMs,
            windows: params.windows,
            candles: params.market.baseCandles,
            minCandles: params.cfg.data.minAsiaCandles,
            sourceTf: params.market.baseTf,
        });
        reasonCodes.push(...asia.reasonCodes);
        if (asia.snapshot) {
            next.asiaRange = asia.snapshot;
            if (next.state === 'IDLE' || next.state === 'ASIA_RANGE_READY') {
                next.state = 'ASIA_RANGE_READY';
            }
        } else {
            return finalizePhase({ state: next, reasonCodes });
        }
    }

    if (!next.sweep && params.nowMs > params.windows.raidEndMs && next.state === 'ASIA_RANGE_READY') {
        next.state = 'DONE';
        reasonCodes.push('RAID_WINDOW_CLOSED_NO_SWEEP');
        return finalizePhase({ state: next, reasonCodes });
    }

    if (next.state === 'IDLE') {
        next.state = 'ASIA_RANGE_READY';
    }

    if (next.state === 'ASIA_RANGE_READY' || next.state === 'SWEEP_DETECTED') {
        const atrBase = computeAtr(params.market.baseCandles, params.cfg.data.atrPeriod);
        const sweep = detectSweepLifecycle({
            existingSweep: next.sweep,
            candles: params.market.baseCandles,
            windows: params.windows,
            nowMs: params.nowMs,
            asiaHigh: next.asiaRange.high,
            asiaLow: next.asiaRange.low,
            atrAbs: atrBase,
            spreadAbs: params.market.quote.spreadAbs,
            pipSize: pipSizeForScalpSymbol(next.symbol),
            cfg: params.cfg.sweep,
        });
        reasonCodes.push(...sweep.reasonCodes);
        if (sweep.sweep) next.sweep = sweep.sweep;
        if (sweep.status === 'rejected') {
            next.state = 'CONFIRMING';
        } else if (sweep.status === 'pending') {
            next.state = 'SWEEP_DETECTED';
            return finalizePhase({ state: next, reasonCodes });
        } else if (sweep.status === 'expired') {
            next.state = 'DONE';
            return finalizePhase({ state: next, reasonCodes });
        } else if (sweep.status === 'none') {
            return finalizePhase({ state: next, reasonCodes });
        }
    }

    if (next.state === 'CONFIRMING') {
        const rejectionTsMs = Number(next.sweep?.rejectedTsMs);
        const direction = expectedDirectionFromSweep(next);
        if (!(Number.isFinite(rejectionTsMs) && rejectionTsMs > 0 && direction)) {
            reasonCodes.push('CONFIRM_REQUIRES_REJECTED_SWEEP');
            next.state = 'DONE';
            return finalizePhase({ state: next, reasonCodes });
        }

        const confirmation = detectConfirmation({
            candles: params.market.confirmCandles,
            nowMs: params.nowMs,
            rejectionTsMs,
            pipSize: pipSizeForScalpSymbol(next.symbol),
            atrPeriod: params.cfg.data.atrPeriod,
            direction,
            cfg: params.cfg.confirm,
        });
        next.confirmation = confirmation.snapshot;
        reasonCodes.push(...confirmation.reasonCodes);
        if (confirmation.status === 'pending') {
            return finalizePhase({ state: next, reasonCodes });
        }
        if (confirmation.status === 'expired') {
            next.state = 'DONE';
            return finalizePhase({ state: next, reasonCodes });
        }

        if (confirmation.status === 'confirmed' && confirmation.displacementTsMs && confirmation.structureShiftTsMs) {
            const ifvg = detectIfvg({
                candles: params.market.confirmCandles,
                direction,
                displacementTsMs: confirmation.displacementTsMs,
                structureShiftTsMs: confirmation.structureShiftTsMs,
                nowMs: params.nowMs,
                atrPeriod: params.cfg.data.atrPeriod,
                cfg: params.cfg.ifvg,
            });
            reasonCodes.push(...ifvg.reasonCodes);
            if (!ifvg.zone) {
                if (params.nowMs > rejectionTsMs + params.cfg.confirm.ttlMinutes * 60_000) {
                    next.state = 'DONE';
                    reasonCodes.push('IFVG_NOT_FOUND_BEFORE_CONFIRM_TTL');
                }
                return finalizePhase({ state: next, reasonCodes });
            }
            next.ifvg = ifvg.zone;
            next.state = 'WAITING_RETRACE';
        }
    }

    if (next.state === 'WAITING_RETRACE' && next.ifvg) {
        const touch = detectIfvgTouch({
            candles: params.market.confirmCandles,
            ifvg: next.ifvg,
            nowMs: params.nowMs,
        });
        reasonCodes.push(...touch.reasonCodes);
        if (touch.touched) {
            next.ifvg = {
                ...next.ifvg,
                touched: true,
            };
            next.state = 'WAITING_RETRACE';
            reasonCodes.push('ENTRY_SIGNAL_READY');
            return finalizePhase({
                state: next,
                reasonCodes,
                entryIntent: { model: 'ifvg_touch' },
            });
        }
        if (touch.expired) {
            next.state = 'DONE';
            return finalizePhase({ state: next, reasonCodes });
        }
    }

    return finalizePhase({ state: next, reasonCodes });
}

export const HSS_ICT_M15_M3_STRATEGY_ID = 'hss_ict_m15_m3';

export const hssIctM15M3Strategy: ScalpStrategyDefinition = {
    id: HSS_ICT_M15_M3_STRATEGY_ID,
    shortName: 'HSS-ICT M15/M3',
    longName: 'Hybrid Session-Scoped ICT Scalp (M15/M3)',
    applyPhaseDetectors,
};
