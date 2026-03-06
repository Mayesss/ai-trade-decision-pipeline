import { computeAtr, detectConfirmation, detectIfvg, detectIfvgTouch, detectSweepLifecycle } from '../detectors';
import { pipSizeForScalpSymbol } from '../marketData';
import type { ScalpCandle, ScalpDirectionalBias, ScalpSessionState, ScalpSessionWindows } from '../types';
import type { ScalpStrategyDefinition, ScalpStrategyEntryIntent, ScalpStrategyPhaseInput, ScalpStrategyPhaseOutput } from './types';

type PdhPdlReclaimStrategyOptions = {
    id: string;
    shortName: string;
    longName: string;
};

const STRATEGY_CONST = {
    minReferenceCandles: 12,
} as const;

function ts(candle: ScalpCandle): number {
    return candle[0];
}

function high(candle: ScalpCandle): number {
    return candle[2];
}

function low(candle: ScalpCandle): number {
    return candle[3];
}

function latestTs(candles: ScalpCandle[]): number | null {
    const latest = candles.at(-1)?.[0];
    return Number.isFinite(Number(latest)) ? Number(latest) : null;
}

function dedupeReasonCodes(codes: string[]): string[] {
    return Array.from(new Set(codes.map((code) => String(code || '').trim().toUpperCase()).filter((code) => code.length > 0)));
}

function withLastProcessed(state: ScalpSessionState, input: ScalpStrategyPhaseInput['market']): ScalpSessionState {
    const next: ScalpSessionState = {
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

function dayKeyInTimeZone(tsMs: number, timeZone: string): string {
    const fmt = new Intl.DateTimeFormat('en-CA', {
        timeZone,
        year: 'numeric',
        month: '2-digit',
        day: '2-digit',
    });
    const parts = fmt.formatToParts(new Date(tsMs));
    const year = parts.find((part) => part.type === 'year')?.value || '1970';
    const month = parts.find((part) => part.type === 'month')?.value || '01';
    const day = parts.find((part) => part.type === 'day')?.value || '01';
    return `${year}-${month}-${day}`;
}

function bucketCandlesByDay(candles: ScalpCandle[], timeZone: string): Map<string, ScalpCandle[]> {
    const buckets = new Map<string, ScalpCandle[]>();
    for (const candle of candles) {
        const dayKey = dayKeyInTimeZone(ts(candle), timeZone);
        if (!buckets.has(dayKey)) buckets.set(dayKey, []);
        buckets.get(dayKey)!.push(candle);
    }
    return buckets;
}

function expectedDirectionFromSweep(state: ScalpSessionState): ScalpDirectionalBias | null {
    if (!state.sweep) return null;
    return state.sweep.side === 'BUY_SIDE' ? 'BEARISH' : 'BULLISH';
}

function buildReferenceRange(params: {
    state: ScalpSessionState;
    market: ScalpStrategyPhaseInput['market'];
    windows: ScalpSessionWindows;
}): {
    snapshot: ScalpSessionState['asiaRange'];
    currentDayCandles: ScalpCandle[];
    reasonCodes: string[];
} {
    const timeZone = params.windows.timezone === 'UTC' ? 'UTC' : 'Europe/London';
    const buckets = bucketCandlesByDay(params.market.baseCandles, timeZone);
    const currentDayCandles = (buckets.get(params.state.dayKey) || []).slice().sort((a, b) => ts(a) - ts(b));
    if (!currentDayCandles.length) {
        return {
            snapshot: null,
            currentDayCandles,
            reasonCodes: ['PDH_PDL_CURRENT_DAY_CANDLES_MISSING'],
        };
    }

    const dayKeys = Array.from(buckets.keys()).sort();
    const currentIndex = dayKeys.indexOf(params.state.dayKey);
    if (currentIndex <= 0) {
        return {
            snapshot: null,
            currentDayCandles,
            reasonCodes: ['PDH_PDL_REFERENCE_DAY_NOT_FOUND'],
        };
    }

    const referenceDayKey = dayKeys[currentIndex - 1]!;
    const referenceCandles = (buckets.get(referenceDayKey) || []).slice().sort((a, b) => ts(a) - ts(b));
    if (referenceCandles.length < STRATEGY_CONST.minReferenceCandles) {
        return {
            snapshot: null,
            currentDayCandles,
            reasonCodes: ['PDH_PDL_REFERENCE_INSUFFICIENT_CANDLES'],
        };
    }

    const referenceHigh = Math.max(...referenceCandles.map(high));
    const referenceLow = Math.min(...referenceCandles.map(low));
    if (!(Number.isFinite(referenceHigh) && Number.isFinite(referenceLow) && referenceHigh > referenceLow)) {
        return {
            snapshot: null,
            currentDayCandles,
            reasonCodes: ['PDH_PDL_REFERENCE_INVALID'],
        };
    }

    return {
        snapshot: {
            timezone: params.windows.timezone,
            sourceTf: params.market.baseTf,
            startUtcIso: new Date(ts(referenceCandles[0]!)).toISOString(),
            endUtcIso: new Date(ts(referenceCandles.at(-1)!)).toISOString(),
            high: referenceHigh,
            low: referenceLow,
            candleCount: referenceCandles.length,
            builtAtMs: params.market.nowMs,
        },
        currentDayCandles,
        reasonCodes: ['PDH_PDL_REFERENCE_READY'],
    };
}

function buildCurrentDaySweepWindow(windows: ScalpSessionWindows, currentDayCandles: ScalpCandle[]): ScalpSessionWindows {
    const startMs = ts(currentDayCandles[0]!);
    const endMs = ts(currentDayCandles.at(-1)!) + 1;
    return {
        ...windows,
        raidStartMs: startMs,
        raidEndMs: endMs,
        raidStartUtcIso: new Date(startMs).toISOString(),
        raidEndUtcIso: new Date(endMs).toISOString(),
    };
}

function applyPhaseDetectorsWithOptions(
    input: ScalpStrategyPhaseInput,
    _options: PdhPdlReclaimStrategyOptions,
): ScalpStrategyPhaseOutput {
    const reasonCodes: string[] = [];
    let next = withLastProcessed(input.state, input.market);

    if (next.state === 'IN_TRADE' || next.state === 'COOLDOWN') {
        return finalizePhase({
            state: next,
            reasonCodes: ['STATE_SKIPPED_MANAGED_EXTERNALLY'],
        });
    }
    if (next.state === 'DONE') {
        return finalizePhase({ state: next, reasonCodes: ['DAY_ALREADY_DONE'] });
    }

    const reference = buildReferenceRange({
        state: next,
        market: input.market,
        windows: input.windows,
    });
    reasonCodes.push(...reference.reasonCodes);

    if (!next.asiaRange && reference.snapshot) {
        next.asiaRange = reference.snapshot;
    }
    if (!next.asiaRange) {
        return finalizePhase({ state: next, reasonCodes });
    }

    if (next.state === 'IDLE') {
        next.state = 'ASIA_RANGE_READY';
    }

    const currentDayCandles = reference.currentDayCandles;
    if (!currentDayCandles.length) {
        return finalizePhase({ state: next, reasonCodes });
    }

    if (next.state === 'ASIA_RANGE_READY' || next.state === 'SWEEP_DETECTED') {
        const atrBase = computeAtr(currentDayCandles, input.cfg.data.atrPeriod);
        const sweep = detectSweepLifecycle({
            existingSweep: next.sweep,
            candles: currentDayCandles,
            windows: buildCurrentDaySweepWindow(input.windows, currentDayCandles),
            nowMs: input.nowMs,
            asiaHigh: next.asiaRange.high,
            asiaLow: next.asiaRange.low,
            atrAbs: atrBase,
            spreadAbs: input.market.quote.spreadAbs,
            pipSize: pipSizeForScalpSymbol(next.symbol),
            cfg: input.cfg.sweep,
        });
        reasonCodes.push(...sweep.reasonCodes);
        if (sweep.sweep) next.sweep = sweep.sweep;
        if (sweep.status === 'pending') {
            next.state = 'SWEEP_DETECTED';
            return finalizePhase({ state: next, reasonCodes });
        }
        if (sweep.status === 'expired') {
            next.state = 'DONE';
            reasonCodes.push('PDH_PDL_RECLAIM_EXPIRED');
            return finalizePhase({ state: next, reasonCodes });
        }
        if (sweep.status === 'none') {
            return finalizePhase({ state: next, reasonCodes });
        }
        next.state = 'CONFIRMING';
        reasonCodes.push(sweep.direction === 'BULLISH' ? 'PDH_PDL_SELL_SIDE_RECLAIM' : 'PDH_PDL_BUY_SIDE_RECLAIM');
    }

    if (next.state === 'CONFIRMING') {
        const rejectionTsMs = Number(next.sweep?.rejectedTsMs);
        const direction = expectedDirectionFromSweep(next);
        if (!(Number.isFinite(rejectionTsMs) && rejectionTsMs > 0 && direction)) {
            reasonCodes.push('PDH_PDL_CONFIRM_REQUIRES_REJECTED_SWEEP');
            next.state = 'DONE';
            return finalizePhase({ state: next, reasonCodes });
        }

        const confirmation = detectConfirmation({
            candles: input.market.confirmCandles,
            nowMs: input.nowMs,
            rejectionTsMs,
            pipSize: pipSizeForScalpSymbol(next.symbol),
            atrPeriod: input.cfg.data.atrPeriod,
            direction,
            cfg: input.cfg.confirm,
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

        if (confirmation.displacementTsMs && confirmation.structureShiftTsMs) {
            const ifvg = detectIfvg({
                candles: input.market.confirmCandles,
                direction,
                displacementTsMs: confirmation.displacementTsMs,
                structureShiftTsMs: confirmation.structureShiftTsMs,
                nowMs: input.nowMs,
                atrPeriod: input.cfg.data.atrPeriod,
                cfg: input.cfg.ifvg,
            });
            reasonCodes.push(...ifvg.reasonCodes);
            if (!ifvg.zone) {
                if (input.nowMs > rejectionTsMs + Math.max(1, input.cfg.confirm.ttlMinutes) * 60_000) {
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
            candles: input.market.confirmCandles,
            ifvg: next.ifvg,
            nowMs: input.nowMs,
        });
        reasonCodes.push(...touch.reasonCodes);
        if (touch.touched) {
            next.ifvg = {
                ...next.ifvg,
                touched: true,
            };
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

export const PDH_PDL_RECLAIM_M15_M3_STRATEGY_ID = 'pdh_pdl_reclaim_m15_m3';

export function buildPdhPdlReclaimM15M3Strategy(
    overrides: Partial<PdhPdlReclaimStrategyOptions> = {},
): ScalpStrategyDefinition {
    const options: PdhPdlReclaimStrategyOptions = {
        id: PDH_PDL_RECLAIM_M15_M3_STRATEGY_ID,
        shortName: 'PDH/PDL Reclaim',
        longName: 'Previous-Day High/Low Sweep Reclaim (M15/M3)',
        ...overrides,
    };
    return {
        id: options.id,
        shortName: options.shortName,
        longName: options.longName,
        applyPhaseDetectors(input: ScalpStrategyPhaseInput): ScalpStrategyPhaseOutput {
            return applyPhaseDetectorsWithOptions(input, options);
        },
    };
}

export const pdhPdlReclaimM15M3Strategy = buildPdhPdlReclaimM15M3Strategy();
