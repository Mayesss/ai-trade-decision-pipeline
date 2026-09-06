// lib/swing/prompt.ts
//
// Builds what the model actually reads: the STATE/MARKET payloads plus the
// system and user turns. computeSwingState() does the cheap derivation up front
// and returns an assemble() closure for the expensive string work, so a
// non-actionable flat tick can be gated out before paying for prompt assembly
// or a news fetch.
//
// Transport lives in lib/gatewayResponses.ts and lib/gatewayMessages.ts; what happens to the
// model's ANSWER lives in decisionRules.ts.

import { CONTEXT_TIMEFRAME, DEFAULT_TAKER_FEE_RATE, MACRO_TIMEFRAME, MICRO_TIMEFRAME, NANO_TIMEFRAME, PRIMARY_TIMEFRAME } from '../constants';
import type { LevelDescriptor, MultiTFIndicators, SRLevels } from '../indicators';
import { wakePlanGraceMinutes, wakeBreakConfirmAtr, WAKE_CONFIRM_MAX_MINUTES, WAKE_CONFIRM_MIN_MINUTES } from './wakeWatch';
import type { EventReactionMeasurement } from './eventReaction';
import type { BtcContext } from './btcContext';
import type { FearGreedContext } from './fearGreed';
import type { ForexSessionLevelsContext } from './sessionLevels';
import type { RecentActionEntry } from './recentActions';
import { computeWaveGeometry } from './waveGeometry';
import type { NanoContext } from './waveGeometry';
import { SWING_STRATEGIES } from './decisionSchema';
import { RISK_EQUITY_PCT, EXPOSURE_CAP_EQUITY_MULT } from './riskSizing';
import {
    resolveDecisionPolicy,
    REENTRY_COOLDOWN_MIN,
    restingEntryKindsFor,
    RESTING_ENTRY_MAX_AGE_MINUTES,
    ENTRY_STOP_MIN_ATR,
    ENTRY_STOP_MAX_ATR,
    SESSION_OFFENSE_ENABLED,
    POSITION_WAKE_ENABLED,
    POSITION_WAKE_MIN_ATR,
    HOLD_COOLDOWN_MIN_MINUTES,
    HOLD_COOLDOWN_MAX_MINUTES,
    EXCHANGE_TP_FALLBACK_ATR_MULT,
    ENTRY_LIMIT_MIN_ATR,
    ENTRY_LIMIT_MAX_ATR,
} from './decisionConfig';
import type {
    PositionContext,
    MomentumSignals,
    SwingMarketBundle,
    SwingAnalytics,
    SwingGatesInput,
    DecisionPolicy,
    ForexEventContextForPrompt,
    CapitalMarketContextForPrompt,
    LastClosedPosition,
} from './decisionConfig';
import {
    computeMomentumSignals,
    computeSignalStrength,
    evaluateActionability,
    readIndicator,
    resolveReentryCooldown,
} from './signals';

// MARKET half of the prompt payload (raw inputs). The optional blocks are
// attached conditionally below; shapes mirror exactly what gets stringified.
type MarketPayload = {
    price: { last: number | null; change_24h_pct: number | null };
    recent_candles: Array<{
        ts: string;
        open: number | null;
        high: number | null;
        low: number | null;
        close: number | null;
        volume: number | null;
    } | null>;
    liquidity: {
        spread_bps: number | null;
        best_bid: number | null;
        best_ask: number | null;
        bid_walls: unknown;
        ask_walls: unknown;
    };
    volume_profile: Array<{ price: number | null; volume: unknown }>;
    news: { sentiment: string | null; headlines: string[] };
    recent_actions: Array<Record<string, unknown>>;
    forex_events?: ForexEventContextForPrompt;
    forex_session?: ForexSessionLevelsContext;
    event_reaction?: EventReactionMeasurement[];
    btc_context?: BtcContext;
    fear_greed?: FearGreedContext;
    cooldown_wake?: {
        crossed: 'above' | 'below';
        level: number;
        set_minutes_ago: number | null;
        expired?: boolean;
        sustained_minutes?: number;
        break_extension_atr?: number;
        note?: string;
    };
    wake_band_sweeps?: Array<{
        side: 'above' | 'below';
        level: number;
        touched_minutes_ago: number;
        held_minutes: number;
        extreme?: number;
    }>;
    reclaim_wake?: {
        side: 'above' | 'below';
        level: number;
        extreme?: number;
        depth_atr?: number;
        held_minutes: number;
        reclaimed_minutes_ago: number;
        note?: string;
    };
    session_reclaim?: {
        kind: 'last_session_high' | 'last_session_low' | 'prior_day_high' | 'prior_day_low';
        side: 'above' | 'below';
        level: number;
        extreme?: number;
        depth_atr?: number;
        held_minutes: number;
        reclaimed_minutes_ago: number;
    };
    failed_break?: {
        side: 'long' | 'short';
        trigger_price: number;
        bar_close: number;
        bar_closed_minutes_ago: number | null;
    };
    position_wake?: {
        crossed: 'above' | 'below';
        level: number;
        set_minutes_ago: number | null;
        expired?: boolean;
        note?: string;
    };
    position_wake_armed?: {
        above: number | null;
        below: number | null;
        set_minutes_ago: number | null;
        note?: string;
    };
    venue_session?: CapitalMarketContextForPrompt['venue_session'];
    venue_events?: CapitalMarketContextForPrompt['venue_events'];
};

// Two-phase by design: computes signal_strength + the decision
// context (cheap), and returns an `assemble(news)` closure that builds the actual
// STATE/MARKET prompt strings (the expensive JSON.stringify + template work).
// Callers can read the actionability gate BEFORE assembling — so non-actionable
// flat ticks never pay for prompt assembly or the news fetch. News is not needed here.
export function computeSwingState(
    symbol: string,
    timeframe: string,
    bundle: SwingMarketBundle,
    analytics: SwingAnalytics,
    position_status: string = 'none',
    forex_event_context: ForexEventContextForPrompt | null = null,
    forex_session_context: ForexSessionLevelsContext | null = null,
    indicators: MultiTFIndicators,
    gates: SwingGatesInput, // <--- Retain the gates object for the base gate checks
    position_context: PositionContext | null = null,
    momentumSignalsOverride?: MomentumSignals,
    recentActions: RecentActionEntry[] = [],
    realizedRoiPct?: number | null,
    dryRun?: boolean,
    spreadBpsOverride?: number,
    decisionPolicy?: DecisionPolicy,
    category?: string | null,
    platform?: string | null,
    lastClosedPosition?: LastClosedPosition | null,
    nowMs?: number,
    capitalMarketContext?: CapitalMarketContextForPrompt | null,
    // Set when this evaluation exists because price crossed the wake band the
    // model attached to its previous flat HOLD cooldown (the caller bypasses
    // the flat quality gates for these ticks). Surfaces as market.cooldown_wake.
    // `note` is the plan the model attached when it set the band — echoed back
    // so the (stateless) wake evaluation knows why it was scheduled.
    // `expired` = the band crossed past its plan horizon (cooldown end + one
    // primary candle of grace): the caller does NOT bypass the quality gates
    // for it, and the prompt flags it as a stale idea to re-derive.
    // `sustainedMinutes` = the band carried a sustained-confirmation window
    // and price held beyond it that long before this wake fired.
    // `breakExtensionAtr` = the wake confirmed by FORCE instead of time: price
    // extended that many primary-ATRs beyond the level.
    cooldownWake?: {
        crossed: 'above' | 'below';
        level: number;
        setAtMs: number | null;
        note?: string | null;
        expired?: boolean;
        sustainedMinutes?: number | null;
        breakExtensionAtr?: number | null;
    } | null,
    // Set when the position's break-entry trigger has been closed back through
    // by a primary bar (the model's own failed-break lesson). Surfaces as
    // market.failed_break so the exit decision is made with the fact in hand.
    failedBreak?: { side: 'long' | 'short'; triggerPrice: number; barClose: number; barClosedAtMs: number | null } | null,
    // In-position wake bands (ENABLE_POSITION_WAKE_BANDS). `fired` = price
    // crossed a band the model set on a previous management look — THIS tick
    // exists (or is let through) because of it; surfaces as
    // market.position_wake. `armed` = the standing bands on an ordinary look;
    // surfaces as market.position_wake_armed so re-stating them is trivial
    // under the replace-on-every-look contract. Fired suppresses armed.
    positionWake?: {
        fired?: { crossed: 'above' | 'below'; level: number; setAtMs: number | null; note?: string | null } | null;
        armed?: { above: number | null; below: number | null; note?: string | null; setAtMs?: number | null } | null;
    } | null,
    // Failed touches of the standing flat wake band (touched, then reclaimed
    // before the sustained-confirmation window elapsed). Surfaces as
    // market.wake_band_sweeps on any flat look that carries the row — fired
    // wakes, expiry consumes and ordinary scans alike: a swept level is
    // liquidity-grab evidence the model should see at its next calm look.
    wakeSweeps?: Array<{
        side: 'above' | 'below';
        level: number;
        touchedAtMs: number;
        reclaimedAtMs: number;
        extreme: number | null;
    }> | null,
    // A fresh sweep of the standing sustained band claimed its one-shot
    // reclaim look — THIS evaluation is the bounce moment. Surfaces as
    // market.reclaim_wake; the band's own plan stays armed regardless.
    reclaimWake?: {
        side: 'above' | 'below';
        level: number;
        extreme: number | null;
        touchedAtMs: number;
        reclaimedAtMs: number;
        atr: number | null;
        note?: string | null;
    } | null,
    // Same event class at a CODE-KNOWN session/prior-day liquidity pool
    // (reclaim-wake phase 2) — surfaces as market.session_reclaim.
    sessionReclaim?: {
        kind: 'last_session_high' | 'last_session_low' | 'prior_day_high' | 'prior_day_low';
        side: 'above' | 'below';
        level: number;
        extreme: number | null;
        touchedAtMs: number;
        reclaimedAtMs: number;
        atr: number | null;
    } | null,
) {
    const t = Array.isArray(bundle.ticker) ? bundle.ticker[0] : bundle.ticker;
    const price = Number(t?.lastPr ?? t?.last ?? t?.close ?? t?.price);
    const change = Number(t?.change24h ?? t?.changeUtc24h ?? t?.chgPct);
    const last = price; // Use price as 'last'
    const microTimeframe = indicators.microTimeFrame ?? MICRO_TIMEFRAME;
    const macroTimeframe = indicators.macroTimeFrame ?? MACRO_TIMEFRAME;
    const primaryTimeframe = indicators.primary?.timeframe ?? PRIMARY_TIMEFRAME;
    const contextTimeframe = indicators.context?.timeframe ?? indicators.contextTimeFrame ?? CONTEXT_TIMEFRAME;
    const momentumSignals =
        momentumSignalsOverride ??
        computeMomentumSignals({
            price: last,
            indicators,
            gates,
            primaryTimeframe,
        });
    // Wave geometry (regression channel, pivot trendlines, last swing points)
    // on the entry-relevant timeframes, from candles already fetched for the
    // indicators — zero extra I/O. Nano (15m) geometry is fetched separately by
    // the caller only once all gates pass, and enters via assemble().
    const microGeometry = computeWaveGeometry(indicators.rawCandles?.[microTimeframe]);
    const primaryGeometry = computeWaveGeometry(indicators.rawCandles?.[primaryTimeframe]);
    const spreadAbsRaw = Number(analytics?.spreadAbs ?? analytics?.spread);
    const spreadBpsFromAnalytics = Number(analytics?.spreadBps);
    const spreadBpsCanonical = Number.isFinite(spreadBpsOverride as number)
        ? Number(spreadBpsOverride)
        : Number.isFinite(spreadBpsFromAnalytics)
          ? spreadBpsFromAnalytics
          : Number.isFinite(spreadAbsRaw) && last > 0
            ? (spreadAbsRaw / last) * 1e4
            : 999;
    const bestBidRaw = Number(analytics?.bestBid);
    const bestAskRaw = Number(analytics?.bestAsk);
    const candles = Array.isArray(bundle.candles) ? bundle.candles : [];
    const priceTrendPoints = candles
        .slice(-5)
        .map((c) => {
            const tsRaw = Number(c?.[0]);
            if (!Number.isFinite(tsRaw)) return null;
            const toNum = (v: unknown) => {
                const n = Number(v);
                return Number.isFinite(n) ? Number(n.toFixed(6)) : null;
            };
            const open = toNum(c?.[1]);
            const high = toNum(c?.[2]);
            const low = toNum(c?.[3]);
            const close = toNum(c?.[4]);
            const volume = toNum(c?.[5] ?? c?.volume);
            const tsMs = tsRaw > 1e12 ? tsRaw : tsRaw * 1000;
            return {
                ts: new Date(tsMs).toISOString(),
                open,
                high,
                low,
                close,
                volume,
            };
        })
        .filter((p) => p !== null);

    const recentActionsExists = Array.isArray(recentActions) && recentActions.length > 0;
    const actionsToShow = recentActionsExists ? Math.min(recentActions.length, 5) : 5;
    const sr = indicators.sr || {};
    const primarySR = sr[primaryTimeframe] ?? sr[indicators.primary?.timeframe || primaryTimeframe];
    const contextSR = sr[contextTimeframe] ?? sr[indicators.context?.timeframe || contextTimeframe];

    const contextSummary = indicators.context?.summary ?? '';
    const contextBias = contextSummary.includes('trend=up')
        ? 'UP'
        : contextSummary.includes('trend=down')
          ? 'DOWN'
          : 'NEUTRAL';

    // ---- Extract indicators for raw metrics (Micro, Macro, Primary) ----
    const micro = indicators.micro || '';
    const macro = indicators.macro || '';
    const primary = indicators.primary?.summary || '';

    // Technical values we want the AI to judge (values, not booleans)
    const ema20_micro = readIndicator('EMA20', micro);
    const ema20_primary = readIndicator('EMA20', primary);
    const slope21_micro = readIndicator('slopeEMA21_10', micro) ?? 0; // % per bar
    const slope21_primary = readIndicator('slopeEMA21_10', primary) ?? 0; // % per bar
    const atr_micro = readIndicator('ATR', micro);
    const atr_macro = readIndicator('ATR', macro);
    const atr_primary = readIndicator('ATR', primary);
    const rsi_micro = readIndicator('RSI', micro);
    const rsi_macro = readIndicator('RSI', macro);
    const rsi_primary = readIndicator('RSI', primary);
    const htfSupportDist = contextSR?.support?.dist_in_atr;
    const htfResistanceDist = contextSR?.resistance?.dist_in_atr;
    let intoContextSupport = false;
    let intoContextResistance = false;
    if (Number.isFinite(htfSupportDist as number)) {
        intoContextSupport = (htfSupportDist as number) < 0.6;
    }
    if (Number.isFinite(htfResistanceDist as number)) {
        intoContextResistance = (htfResistanceDist as number) < 0.6;
    }
    const chopRisk = intoContextSupport && intoContextResistance;
    const htfBreakdownConfirmed = contextSR?.support?.level_state === 'broken';
    const htfBreakoutConfirmed = contextSR?.resistance?.level_state === 'broken';

    // --- KEY METRICS (VALUES, NOT JUDGMENTS) ---
    const atr_pct_macro = last > 0 && atr_macro ? (atr_macro / last) * 100 : 0;
    const atr_pct_primary = last > 0 && atr_primary ? (atr_primary / last) * 100 : 0;

    // Calculate extension (distance from EMA20 in 1m-ATRs)
    const distance_from_ema_atr =
        Number.isFinite(atr_micro as number) && (atr_micro as number) > 0 && Number.isFinite(ema20_micro as number)
            ? (last - (ema20_micro as number)) / (atr_micro as number)
            : 0;
    const distance_from_ema20_primary_atr =
        Number.isFinite(atr_primary as number) &&
        (atr_primary as number) > 0 &&
        Number.isFinite(ema20_primary as number)
            ? (last - (ema20_primary as number)) / (atr_primary as number)
            : 0;

    const metricsByTf = indicators.metrics || {};
    const microMetrics = metricsByTf[microTimeframe] || {};
    const primaryMetrics = metricsByTf[primaryTimeframe] || {};
    const macroMetrics = metricsByTf[macroTimeframe] || {};

    const microStructureState = microMetrics.structure ?? 'range';
    const microBos = Boolean(microMetrics.bos);
    const microBosDir = microBos ? (microMetrics.bosDir ?? null) : null;
    const microChoch = Boolean(microMetrics.choch);
    const microBreakoutRetestOk = Boolean(microMetrics.breakoutRetestOk);
    const microBreakoutRetestDir = microMetrics.breakoutRetestDir ?? null;
    const microStructureBreakState = microMetrics.structureBreakState ?? 'inside';

    const atrPctile4h = typeof primaryMetrics.atrPctile === 'number' ? primaryMetrics.atrPctile : null;
    const atrPctile1d = typeof macroMetrics.atrPctile === 'number' ? macroMetrics.atrPctile : null;
    const rvol4h = typeof primaryMetrics.rvol === 'number' ? primaryMetrics.rvol : null;
    const rvol1d = typeof macroMetrics.rvol === 'number' ? macroMetrics.rvol : null;
    const structure4hState = primaryMetrics.structure ?? 'range';
    const bos4h = Boolean(primaryMetrics.bos);
    const bosDir4h = bos4h ? (primaryMetrics.bosDir ?? null) : null;
    const choch4h = Boolean(primaryMetrics.choch);
    const breakoutRetestOk4h = Boolean(primaryMetrics.breakoutRetestOk);
    const breakoutRetestDir4h = primaryMetrics.breakoutRetestDir ?? null;
    const structureBreakState4h = primaryMetrics.structureBreakState ?? 'inside';
    const valueState1d = macroMetrics.valueState ?? 'n/a';
    const primaryBias =
        Number.isFinite(slope21_primary as number) &&
        Number.isFinite(rsi_primary as number) &&
        Number.isFinite(ema20_primary as number)
            ? (slope21_primary as number) > 0 && (rsi_primary as number) >= 50 && price >= (ema20_primary as number)
                ? 'up'
                : (slope21_primary as number) < 0 && (rsi_primary as number) <= 50 && price <= (ema20_primary as number)
                  ? 'down'
                  : 'neutral'
            : 'neutral';
    let microBiasCalc: 'up' | 'down' | 'neutral' = 'neutral';
    let microBiasSource:
        | 'structure_breakout_retest'
        | 'structure_break_state'
        | 'structure_bos'
        | 'structure_state'
        | 'ema_slope_rsi'
        | 'neutral' = 'neutral';
    // Precedence for micro bias:
    // 1) structure (breakout/retest, break-state, BOS, structure state), 2) EMA+slope+RSI, 3) neutral.
    if (microBreakoutRetestOk && microBreakoutRetestDir === 'up') {
        microBiasCalc = 'up';
        microBiasSource = 'structure_breakout_retest';
    } else if (microBreakoutRetestOk && microBreakoutRetestDir === 'down') {
        microBiasCalc = 'down';
        microBiasSource = 'structure_breakout_retest';
    } else if (microStructureBreakState === 'above') {
        microBiasCalc = 'up';
        microBiasSource = 'structure_break_state';
    } else if (microStructureBreakState === 'below') {
        microBiasCalc = 'down';
        microBiasSource = 'structure_break_state';
    } else if (microBos && microBosDir === 'up') {
        microBiasCalc = 'up';
        microBiasSource = 'structure_bos';
    } else if (microBos && microBosDir === 'down') {
        microBiasCalc = 'down';
        microBiasSource = 'structure_bos';
    } else if (microStructureState === 'bull') {
        microBiasCalc = 'up';
        microBiasSource = 'structure_state';
    } else if (microStructureState === 'bear') {
        microBiasCalc = 'down';
        microBiasSource = 'structure_state';
    } else if (
        Number.isFinite(slope21_micro as number) &&
        Number.isFinite(rsi_micro as number) &&
        Number.isFinite(ema20_micro as number)
    ) {
        microBiasCalc =
            (slope21_micro as number) > 0 && (rsi_micro as number) >= 50 && price >= (ema20_micro as number)
                ? 'up'
                : (slope21_micro as number) < 0 && (rsi_micro as number) <= 50 && price <= (ema20_micro as number)
                  ? 'down'
                  : 'neutral';
        microBiasSource = microBiasCalc === 'neutral' ? 'neutral' : 'ema_slope_rsi';
    }
    const microBiasLabel = microBiasCalc.toUpperCase();
    const primaryTrendUp = structure4hState === 'bull' || (bos4h && bosDir4h === 'up') || primaryBias === 'up';
    const primaryTrendDown = structure4hState === 'bear' || (bos4h && bosDir4h === 'down') || primaryBias === 'down';
    const primaryBreakdownConfirmed =
        structureBreakState4h === 'below' || (breakoutRetestOk4h && breakoutRetestDir4h === 'down');
    const primaryBreakoutConfirmed =
        structureBreakState4h === 'above' || (breakoutRetestOk4h && breakoutRetestDir4h === 'up');


    // --- SIGNAL STRENGTH DRIVERS & CLOSING GUIDANCE ---
    const clampNumber = (value: number | null | undefined, digits = 3) =>
        Number.isFinite(value as number) ? Number((value as number).toFixed(digits)) : null;
    const trendBias = gates.regime_trend_up ? 1 : gates.regime_trend_down ? -1 : 0;

    const supportProximity = typeof htfSupportDist === 'number' ? Math.max(0, 1 - Math.min(htfSupportDist, 2) / 2) : 0;
    const resistanceProximity =
        typeof htfResistanceDist === 'number' ? Math.max(0, 1 - Math.min(htfResistanceDist, 2) / 2) : 0;
    const locationScoreLong = Math.min(1, supportProximity + (htfBreakoutConfirmed ? 0.3 : 0));
    const locationScoreShort = Math.min(1, resistanceProximity + (htfBreakdownConfirmed ? 0.3 : 0));
    const locationConfluenceScore = Math.max(locationScoreLong, locationScoreShort);

    const nearPrimarySupport =
        typeof primarySR?.support?.dist_in_atr === 'number' ? primarySR.support.dist_in_atr <= 0.6 : false;
    const nearPrimaryResistance =
        typeof primarySR?.resistance?.dist_in_atr === 'number' ? primarySR.resistance.dist_in_atr <= 0.6 : false;

    const macroBias = momentumSignals.macroTrendUp ? 'UP' : momentumSignals.macroTrendDown ? 'DOWN' : 'NEUTRAL';
    const regimeAlignmentRaw =
        primaryBias === 'up'
            ? (macroBias === 'UP' ? 1 : macroBias === 'DOWN' ? -1 : 0) +
              (contextBias === 'UP' ? 1 : contextBias === 'DOWN' ? -1 : 0)
            : primaryBias === 'down'
              ? (macroBias === 'DOWN' ? 1 : macroBias === 'UP' ? -1 : 0) +
                (contextBias === 'DOWN' ? 1 : contextBias === 'UP' ? -1 : 0)
              : 0;
    const regimeAlignment = Math.max(-1, Math.min(1, regimeAlignmentRaw / 2));

    const valueOkLong = valueState1d === 'n/a' ? false : valueState1d !== 'below_val';
    const valueOkShort = valueState1d === 'n/a' ? false : valueState1d !== 'above_vah';

    // Directional drivers (macro, context) require ACTUAL alignment with the side,
    // not mere non-opposition. The old `!== opposite` form was near-tautological for
    // the favored side (a NEUTRAL bias counted for both sides), inflating
    // aligned_driver_count so MEDIUM became the floor. `=== direction` makes the
    // count reflect genuine multi-timeframe confluence. The at-level exception
    // (intoContextSupport/Resistance) on the context driver is retained.
    const longDrivers = [
        structure4hState === 'bull' || (bos4h && bosDir4h === 'up'),
        (breakoutRetestOk4h && breakoutRetestDir4h === 'up') || nearPrimarySupport,
        macroBias === 'UP',
        contextBias === 'UP' || intoContextSupport,
        valueOkLong,
    ];

    const shortDrivers = [
        structure4hState === 'bear' || (bos4h && bosDir4h === 'down'),
        (breakoutRetestOk4h && breakoutRetestDir4h === 'down') || nearPrimaryResistance,
        macroBias === 'DOWN',
        contextBias === 'DOWN' || intoContextResistance,
        valueOkShort,
    ];

    const countTrue = (items: boolean[]) => items.reduce((acc, v) => acc + (v ? 1 : 0), 0);
    const longAlignedDriverCount = countTrue(longDrivers);
    const shortAlignedDriverCount = countTrue(shortDrivers);
    const alignedDriverCount = Math.max(longAlignedDriverCount, shortAlignedDriverCount);

    const positionSide = position_context?.side;
    const priceVsBreakevenPctRaw =
        position_context?.breakeven_price && Number.isFinite(position_context.breakeven_price) && price > 0
            ? ((price - position_context.breakeven_price) / position_context.breakeven_price) * 100
            : null;
    const priceVsBreakevenPct =
        positionSide === 'short'
            ? clampNumber(-(priceVsBreakevenPctRaw ?? 0), 3)
            : clampNumber(priceVsBreakevenPctRaw ?? null, 3);
    const macroSupportsPosition =
        positionSide === 'long' ? gates.regime_trend_up : positionSide === 'short' ? gates.regime_trend_down : null;
    const macroOpposesPosition =
        positionSide === 'long' ? gates.regime_trend_down : positionSide === 'short' ? gates.regime_trend_up : null;
    const closingGuidance = {
        macro_bias: trendBias,
        price_vs_breakeven_pct: priceVsBreakevenPct,
        hold_minutes: clampNumber(position_context?.hold_minutes ?? null, 1),
        macro_supports_position: macroSupportsPosition,
        macro_opposes_position: macroOpposesPosition,
    };
    // Costs (educate the model). Two venue models:
    // - Bitget perps: taker fee per side (+ slippage); spread is negligible.
    // - Capital CFDs: NO commission — the real round-trip cost is crossing the
    //   spread once (entry at ask, exit at bid) plus slippage, and holding cost
    //   is the per-night funding adjustment (overnight_fee_pct_per_day below).
    const isCapital = String(platform || '').toLowerCase() === 'capital';
    const taker_fee_rate_side = isCapital
        ? 0
        : Number.isFinite(position_context?.taker_fee_rate as number)
          ? Math.max(0, Number(position_context?.taker_fee_rate))
          : DEFAULT_TAKER_FEE_RATE; // default set via env (e.g., 0.0006 = 6 bps per side)
    const taker_round_trip_bps = Number((taker_fee_rate_side * 2 * 10000).toFixed(2));
    const slippage_bps = 2;
    // Capital: spread cost, only when a real quote was measured this tick — the
    // 999 canonical fallback means "unknown", never a cost.
    const spread_round_trip_bps =
        isCapital && Number.isFinite(spreadBpsCanonical) && spreadBpsCanonical < 999
            ? Number(spreadBpsCanonical.toFixed(1))
            : null;
    const total_cost_bps =
        isCapital && spread_round_trip_bps === null
            ? null
            : Number((taker_round_trip_bps + slippage_bps + (spread_round_trip_bps ?? 0)).toFixed(1));
    const overnight_fee_pct_per_day = isCapital
        ? capitalMarketContext?.overnight_fee_pct_per_day ?? null
        : null;
    // Perp funding (Bitget only — Capital bundles carry none of these fields).
    // bundle.funding = current-fund-rate data ({ fundingRate }, decimal per
    // interval: 0.0001 = 0.01%); bundle.fundingTime = funding-time data
    // ({ nextFundingTime ms, ratePeriod hours }). Positive rate = longs pay
    // shorts. Fails to null field-by-field — all prompt prose referencing
    // funding stays conditional on what was actually measured.
    const fundingRow = Array.isArray(bundle?.funding) ? bundle.funding[0] : bundle?.funding;
    const fundingRateDecimal = Number(fundingRow?.fundingRate);
    const fundingTimeRow = Array.isArray(bundle?.fundingTime) ? bundle.fundingTime[0] : bundle?.fundingTime;
    const nextFundingAtMsRaw = Number(fundingTimeRow?.nextFundingTime);
    const fundingIntervalHoursRaw = Number(fundingTimeRow?.ratePeriod);
    const perpFunding =
        !isCapital && Number.isFinite(fundingRateDecimal)
            ? {
                  rate_pct_per_interval: Number((fundingRateDecimal * 100).toFixed(4)),
                  interval_hours:
                      Number.isFinite(fundingIntervalHoursRaw) && fundingIntervalHoursRaw > 0
                          ? fundingIntervalHoursRaw
                          : null,
                  next_funding_at_ms:
                      Number.isFinite(nextFundingAtMsRaw) && nextFundingAtMsRaw > 0 ? nextFundingAtMsRaw : null,
              }
            : null;

    const resolvedDecisionPolicy = resolveDecisionPolicy(decisionPolicy);
    // The resolved policy drives the base-gate hard-constraint row below, which
    // states exactly what it coerces. The prompt used to ALSO print the raw
    // label ("Decision policy mode: balanced_guardrails") as its closing line —
    // dropped 2026-09-06: it carried no information the constraint row lacked,
    // and "balanced"/"guardrails" are posture words that argue for caution
    // independently of what STATE shows.
    const strictPolicy = resolvedDecisionPolicy === 'strict';

    // Variant key: the system prompt renders a FLAT or an IN-POSITION variant —
    // each byte-stable for its state — so a tick never carries the other
    // state's doctrine. Prompt caching keys on the variant, which flips once
    // per thread (at entry), not per tick.
    const inPosition = Boolean(position_context);

    // Anti-churn: active only when flat (in-position ticks don't re-enter).
    const cooldownNow = position_context ? null : resolveReentryCooldown(lastClosedPosition);
    const reentryCooldown = cooldownNow
        ? { blocked_side: cooldownNow.blockedSide, minutes_left: cooldownNow.minutesLeft }
        : null;

    // No extension threshold reaches the prompt any more (it used to quote
    // resolveExtensionThresholds, shared with the pre-AI gate in /api/analyze).
    // Quoting one meant asserting what that gate had already filtered — but it is
    // bypassed on wake ticks (analyze.ts, `&& !cooldownWakeActive`) and never
    // runs in a position, so the claim was false on exactly the paths where
    // extension is most likely to be extreme. The number is a market
    // measurement the model reads from state.extension_atr; the pipeline's
    // handling of it is not the model's business.

    const modeLabel = dryRun ? 'simulation' : 'live';
    const baseSymbol = symbol.replace(/USDT$/i, '');
    const assetClass = String(category || '').toLowerCase() || 'unknown';
    // On Capital, leverage is fixed by the broker per asset class — the model does
    // not pick it. Only crypto (Bitget) takes a model-chosen 5–10 leverage.
    const leverageGuidance = isCapital
        ? 'Leverage: do NOT set it — on this venue leverage is broker-defined per asset class, not chosen here. Always output leverage=null.'
        : `Leverage 5–10 (crypto): notional is set by your stop distance (see DECISION OWNERSHIP), so leverage does NOT change what a stop-out costs — it only sets how much margin the position locks (higher = less margin tied up, liquidation nearer). Pick it for the margin/liquidation trade-off you want; null on HOLD/CLOSE.`;
    const leverageTask = isCapital
        ? 'do NOT output a leverage field — leverage is broker-defined per asset class on this venue.'
        : inPosition
          ? 'leverage: 5–10 for REVERSE, else null.'
          : 'leverage: 5–10 for BUY/SELL, else null.';
    // Capital: omit the leverage key entirely (no comma). Bitget: include it.
    const leverageJsonField = isCapital ? '' : ',"leverage":null|5|6|7|8|9|10';

    // Profit-lock margin-recycle maneuver (crypto only). The crypto schema always
    // carries these keys (nullable); the maneuver is only *explained* to the model
    // when ENABLE_CRYPTO_MARGIN_RECYCLE is set — otherwise it's told to null them.
    const marginRecycleEnabled = !isCapital && process.env.ENABLE_CRYPTO_MARGIN_RECYCLE === 'true';
    const manageJsonField = isCapital ? '' : ',"raise_leverage_to":null|int,"move_stop_to_be":true|false|null';
    // Doctrine bullet only when the maneuver exists AND the tick manages a
    // position; the disabled/flat "null both" instruction lives in the OUTPUT
    // field rules instead.
    const manageGuidance = isCapital
        ? ''
        : marginRecycleEnabled
          ? 'Margin recycle: winners are AUTO-RECYCLED by the system — once the profit cushion is large enough it moves the stop to breakeven and raises leverage to a liquidation-safe cap, freeing isolated margin for other symbols WITHOUT cutting size (a stop already tighter than breakeven is never loosened back). You MAY additionally request it earlier, on HOLD or a partial CLOSE, with a smaller cushion: move_stop_to_be=true and/or raise_leverage_to (the system clamps your value to [current leverage, the liq-safe cap]). These combine in ONE decision — e.g. a partial exit_size_pct + a tightened stop_loss_price (raise_leverage_to optional — auto covers it) — the system executes breakeven-stop → leverage raise → trim → your stop, and applies your stop only when it is TIGHTER than the breakeven trigger (a looser one is dropped, the breakeven floor stands). A leverage raise always forces breakeven protection first; if that stop cannot rest, the raise is aborted. Not in profit → null both.'
          : '';

    // signal_strength is OWNED BY CODE (computeSignalStrength). It is NOT shown to
    // the model (we don't want it anchoring the model's analysis). Its only
    // remaining consumer is the pre-prompt budget gate (signal_strength_gate):
    // the trend-guard and anti-flip exception thresholds it used to unlock were
    // removed 2026-09-02, so postprocessDecision now just echoes it onto the
    // decision row for later analysis.
    const signalStrength = computeSignalStrength({
        micro_bias_calc: microBiasLabel,
        primary_bias: primaryBias,
        macro_bias: macroBias,
        context_bias: contextBias,
        primary_trend_up: primaryTrendUp,
        primary_trend_down: primaryTrendDown,
        primary_breakdown_confirmed: primaryBreakdownConfirmed,
        primary_breakout_confirmed: primaryBreakoutConfirmed,
        micro_entry_ok: Boolean(momentumSignals.info?.microEntryOk),
        aligned_driver_count: alignedDriverCount,
        regime_alignment: regimeAlignment,
        location_confluence_score: locationConfluenceScore,
        micro_extension_atr: momentumSignals.microExtensionInAtr ?? null,
        primary_extension_atr: distance_from_ema20_primary_atr,
        breakout_retest_ok_primary: breakoutRetestOk4h,
        breakout_retest_dir_primary: breakoutRetestDir4h ?? null,
    });

    // Assembly half: builds the STATE/MARKET JSON + system/user strings. This is the
    // expensive part (two JSON.stringify + a large template), so it's deferred behind
    // this closure and only run once we know the AI will be called. Captures the
    // derivation scope above, so no state needs threading through. News enters here.
    const assemble = (
        news_sentiment: string | null = null,
        news_headlines: string[] = [],
        nano_context: NanoContext | null = null,
        // A standing resting entry that was cancelled by the code-owned age
        // backstop before this evaluation (flat only). Rare — a resting entry
        // now SURVIVES evaluations, so this is not the routine case any more.
        cancelled_pending_entry: { side: 'BUY' | 'SELL' | null; price: number | null; age_min: number | null } | null = null,
        // The entry order resting on the venue RIGHT NOW (flat only). Standing
        // state, exactly like the in-position bracket: it outlives this
        // evaluation unless this decision supersedes or withdraws it.
        resting_entry: {
            kind: 'limit' | 'stop' | null;
            side: 'BUY' | 'SELL' | null;
            price: number | null;
            age_min: number | null;
        } | null = null,
        // Quantified price reaction to just-released high-impact events
        // (market.forex_events.recentEvents). Computed by the caller from the
        // nano 15m candles, so it enters here like nano_context does.
        event_reaction: EventReactionMeasurement[] | null = null,
        // BTC regime coupling for non-BTC crypto ticks (measured correlation/
        // beta + BTC recent state). Fetched by the caller, enters like nano.
        btc_context: BtcContext | null = null,
        // Curated lessons distilled from post-mortems of past LOSING trades on
        // this symbol / its asset class / globally (max 5, confidence-sorted —
        // see lib/swing/lessons.ts). Rendered in the USER turn so the cached
        // system prefix stays byte-stable; [] / null omits the block.
        lessons: Array<{ scope: string; lesson: string; originLabel?: string | null }> | null = null,
        // Fresh search-grounded news+social digest (lib/swing/perplexity.ts),
        // fetched by the caller post-gates. Complements market.news, never
        // replaces it. Rendered in the USER turn (cached system prefix stays
        // byte-stable); null omits the block.
        perplexity_context: { text: string; fetchedAtMs: number } | null = null,
        // Daily crypto Fear & Greed index (lib/swing/fearGreed.ts), fetched by
        // the caller post-gates on crypto ticks only. Market-wide, not
        // per-symbol; null omits the block.
        fear_greed: FearGreedContext | null = null,
    ) => {
    const normalizedNewsSentiment =
        typeof news_sentiment === 'string' && news_sentiment.length > 0 ? news_sentiment : null;
    const normalizedHeadlines = Array.isArray(news_headlines) ? news_headlines.filter((h) => !!h).slice(0, 5) : [];

    const levelBars = (sr: SRLevels | null | undefined) => {
        const depth = Number(sr?.bars_scanned);
        return Number.isFinite(depth) && depth > 0 ? Math.round(depth) : null;
    };
    const srLevel = (lvl: LevelDescriptor | null | undefined) =>
        lvl
            ? {
                  price: lvl.price,
                  dist_atr: lvl.dist_in_atr,
                  bars_ago: lvl.bars_ago,
                  strength: lvl.level_strength,
                  type: lvl.level_type,
                  state: lvl.level_state,
              }
            : null;

    // ---- Single structured payload: one encoding (JSON), no duplicated keys ----
    // STATE = derived signals (what to reason over). MARKET = raw inputs (price/tape/news).
    // Explicit UTC "now" so the model reasons over an unambiguous anchor instead of
    // reconstructing it from scattered ISO timestamps. All fields are UTC; awareness
    // only — no time-based rule is enforced here (those stay in code gates if/when added).
    const nowDate = new Date(Number.isFinite(nowMs) ? (nowMs as number) : Date.now());
    const DOW = ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday'];
    const state = {
        time: {
            iso_utc: nowDate.toISOString(),
            date_utc: nowDate.toISOString().slice(0, 10),
            day_of_week_utc: DOW[nowDate.getUTCDay()],
            hour_utc: nowDate.getUTCHours(),
        },
        biases: {
            micro: microBiasLabel,
            micro_source: microBiasSource,
            primary: primaryBias,
            macro: macroBias,
            context: contextBias,
        },
        trend: {
            primary_up: primaryTrendUp,
            primary_down: primaryTrendDown,
            primary_breakout_confirmed: primaryBreakoutConfirmed,
            primary_breakdown_confirmed: primaryBreakdownConfirmed,
            macro_up: momentumSignals.macroTrendUp,
            macro_down: momentumSignals.macroTrendDown,
        },
        structure: {
            micro: {
                state: microStructureState,
                break_state: microStructureBreakState,
                bos: microBos,
                bos_dir: microBosDir,
                choch: microChoch,
                breakout_retest_ok: microBreakoutRetestOk,
                breakout_retest_dir: microBreakoutRetestDir,
            },
            primary: {
                state: structure4hState,
                break_state: structureBreakState4h,
                bos: bos4h,
                bos_dir: bosDir4h,
                choch: choch4h,
                breakout_retest_ok: breakoutRetestOk4h,
                breakout_retest_dir: breakoutRetestDir4h,
            },
        },
        momentum: {
            rsi: {
                micro: clampNumber(rsi_micro, 1),
                primary: clampNumber(rsi_primary, 1),
                macro: clampNumber(rsi_macro, 1),
            },
            slope_micro_pct_per_bar: clampNumber(slope21_micro, 4),
            slope_primary_pct_per_bar: clampNumber(slope21_primary, 4),
            micro_entry_ok: Boolean(momentumSignals.info?.microEntryOk),
        },
        extension_atr: {
            micro: clampNumber(distance_from_ema_atr, 2),
            primary: clampNumber(distance_from_ema20_primary_atr, 2),
        },
        volatility: {
            atr_pct: { primary: clampNumber(atr_pct_primary, 3), macro: clampNumber(atr_pct_macro, 3) },
            atr_pctile: { primary: clampNumber(atrPctile4h, 0), macro: clampNumber(atrPctile1d, 0) },
            rvol: { primary: clampNumber(rvol4h, 2), macro: clampNumber(rvol1d, 2) },
            value_state_macro: valueState1d,
        },
        location: {
            context_support_dist_atr: clampNumber(htfSupportDist ?? null, 3),
            context_resistance_dist_atr: clampNumber(htfResistanceDist ?? null, 3),
            context_breakout_confirmed: htfBreakoutConfirmed,
            context_breakdown_confirmed: htfBreakdownConfirmed,
            chop_risk: chopRisk,
        },
        // `bars` = the sample the level scan actually had, after the spot
        // backfill in lib/indicators.ts tops up the venue's shallow 1D/1W
        // history. It ships next to the levels it produced rather than being
        // implied by the legend, because it still collapses for a recent
        // listing or a perp with no spot pair.
        levels: {
            primary: {
                bars: levelBars(primarySR),
                support: srLevel(primarySR?.support),
                resistance: srLevel(primarySR?.resistance),
            },
            context: {
                bars: levelBars(contextSR),
                support: srLevel(contextSR?.support),
                resistance: srLevel(contextSR?.resistance),
            },
        },
        // Wave geometry per timeframe: regression channel (slope_atr, channel_pos
        // 0=low..1=high, width), pivot trendlines (live price + slope + touches)
        // and last swing high/low (price, signed ATR distance, bars ago). nano =
        // 15m entry timing, present only when the AI is actually being called.
        geometry: {
            ...(nano_context ? { nano: nano_context } : {}),
            ...(microGeometry ? { micro: microGeometry } : {}),
            ...(primaryGeometry ? { primary: primaryGeometry } : {}),
        },
        gates: {
            spread_ok: gates.spread_ok,
            liquidity_ok: gates.liquidity_ok,
            atr_ok: gates.atr_ok,
            slippage_ok: gates.slippage_ok,
        },
        costs: isCapital
            ? {
                  // Capital CFDs: commission-free — cost = spread + slippage, plus a
                  // per-night funding adjustment while held. total_cost_bps is null
                  // when no live quote was measured this tick (spread unknown).
                  commission_bps: 0,
                  spread_round_trip_bps,
                  slippage_bps,
                  total_cost_bps,
                  overnight_fee_pct_per_day,
                  recent_realized_pnl_pct: clampNumber(realizedRoiPct ?? null, 2),
              }
            : {
                  round_trip_fee_bps: taker_round_trip_bps,
                  slippage_bps,
                  total_cost_bps,
                  // Perp funding while held (positive = longs pay shorts). Key
                  // omitted entirely when the rate wasn't measured this tick.
                  ...(perpFunding
                      ? {
                            funding: {
                                rate_pct_per_interval: perpFunding.rate_pct_per_interval,
                                interval_hours: perpFunding.interval_hours,
                                next_funding_at_utc: perpFunding.next_funding_at_ms
                                    ? new Date(perpFunding.next_funding_at_ms).toISOString()
                                    : null,
                                minutes_to_next_funding: perpFunding.next_funding_at_ms
                                    ? Math.max(
                                          0,
                                          Math.round(
                                              (perpFunding.next_funding_at_ms - nowDate.getTime()) / 60_000,
                                          ),
                                      )
                                    : null,
                            },
                        }
                      : {}),
                  recent_realized_pnl_pct: clampNumber(realizedRoiPct ?? null, 2),
              },
        position: position_context
            ? { open: true, ...position_context }
            : {
                  open: false,
                  status: position_status,
                  reentry_cooldown: reentryCooldown,
                  ...(resting_entry ? { resting_entry } : {}),
                  ...(cancelled_pending_entry ? { cancelled_pending_entry } : {}),
              },
        closing_guardrails: position_context ? closingGuidance : null,
    };

    const market: MarketPayload = {
        price: {
            last: Number.isFinite(price) ? price : null,
            change_24h_pct: Number.isFinite(change) ? change : null,
        },
        recent_candles: priceTrendPoints,
        liquidity: {
            spread_bps: clampNumber(spreadBpsCanonical, 4),
            best_bid: Number.isFinite(bestBidRaw) ? bestBidRaw : null,
            best_ask: Number.isFinite(bestAskRaw) ? bestAskRaw : null,
            bid_walls: analytics.topWalls?.bid ?? [],
            ask_walls: analytics.topWalls?.ask ?? [],
        },
        volume_profile: (analytics.volume_profile || [])
            .slice(0, 10)
            .map((v) => ({ price: clampNumber(v.price, 2), volume: v.volume })),
        news: { sentiment: normalizedNewsSentiment, headlines: normalizedHeadlines },
        recent_actions: recentActionsExists
            ? recentActions.slice(-1 * actionsToShow).map((a) => {
                  // Annotate partial closes (e.g. "CLOSE 30%") so the model can tell a
                  // trim from a full exit; a 100%/absent pct stays a bare "CLOSE".
                  const partial =
                      a.action === 'CLOSE' && a.closePct != null && a.closePct > 0 && a.closePct < 100;
                  const row: Record<string, unknown> = {
                      action: partial ? `CLOSE ${Math.round(a.closePct as number)}%` : a.action,
                      ts: new Date(a.timestamp).toISOString(),
                  };
                  // Measured follow-through (see the recent_actions prose in the
                  // system prompt): what the model asked for vs what happened.
                  if (a.entryLimitPrice != null) {
                      row.rested_at = a.entryLimitPrice;
                      if (a.restingEntryKind) row.rested_as = a.restingEntryKind;
                  }
                  if (a.strategy) row.strategy = a.strategy;
                  if ((a.reissueCount ?? 1) > 1) row.reissued_count = a.reissueCount;
                  if (a.outcome === 'never_filled' || a.outcome === 'still_open') {
                      row.outcome = a.outcome;
                  } else if (a.outcome && typeof a.outcome === 'object') {
                      row.outcome = {
                          closed_pnl_pct_on_margin: a.outcome.closedPnlPctOnMargin,
                          held_min: a.outcome.heldMin,
                      };
                  }
                  return row;
              })
            : [],
    };
    if (forex_event_context && typeof forex_event_context === 'object') {
        market.forex_events = forex_event_context;
    }
    if (forex_session_context && typeof forex_session_context === 'object') {
        market.forex_session = forex_session_context;
    }
    if (Array.isArray(event_reaction) && event_reaction.length > 0) {
        market.event_reaction = event_reaction;
    }
    if (btc_context && typeof btc_context === 'object') {
        market.btc_context = btc_context;
    }
    if (fear_greed && typeof fear_greed === 'object') {
        market.fear_greed = fear_greed;
    }
    if (cooldownWake && Number.isFinite(cooldownWake.level)) {
        const wakeNowMs = Number.isFinite(nowMs as number) ? (nowMs as number) : Date.now();
        market.cooldown_wake = {
            crossed: cooldownWake.crossed,
            level: cooldownWake.level,
            set_minutes_ago:
                cooldownWake.setAtMs && cooldownWake.setAtMs > 0
                    ? Math.max(0, Math.round((wakeNowMs - cooldownWake.setAtMs) / 60_000))
                    : null,
            // Present (true) only when the band crossed past its plan horizon —
            // prose tells the model to treat the note as a stale idea.
            ...(cooldownWake.expired ? { expired: true } : {}),
            // Present only when the band carried a sustained-confirmation
            // window: price has already HELD beyond the level this long.
            ...(Number.isFinite(Number(cooldownWake.sustainedMinutes)) && Number(cooldownWake.sustainedMinutes) > 0
                ? { sustained_minutes: Math.round(Number(cooldownWake.sustainedMinutes)) }
                : {}),
            // Present when the break confirmed by FORCE before the window
            // elapsed: how many primary-ATRs beyond the level price stands.
            ...(Number.isFinite(Number(cooldownWake.breakExtensionAtr)) && Number(cooldownWake.breakExtensionAtr) > 0
                ? { break_extension_atr: Math.round(Number(cooldownWake.breakExtensionAtr) * 100) / 100 }
                : {}),
        };
        if (typeof cooldownWake.note === 'string' && cooldownWake.note.trim()) {
            market.cooldown_wake.note = cooldownWake.note.trim();
        }
    }
    if (Array.isArray(wakeSweeps) && wakeSweeps.length > 0) {
        const swNowMs = Number.isFinite(nowMs as number) ? (nowMs as number) : Date.now();
        market.wake_band_sweeps = wakeSweeps
            .filter((s) => Number.isFinite(s.level) && s.touchedAtMs > 0 && s.reclaimedAtMs > 0)
            .map((s) => ({
                side: s.side,
                level: s.level,
                touched_minutes_ago: Math.max(0, Math.round((swNowMs - s.touchedAtMs) / 60_000)),
                held_minutes: Math.max(0, Math.round((s.reclaimedAtMs - s.touchedAtMs) / 60_000)),
                ...(s.extreme !== null && Number.isFinite(s.extreme) ? { extreme: s.extreme } : {}),
            }));
    }
    if (reclaimWake && Number.isFinite(reclaimWake.level) && reclaimWake.level > 0) {
        const rwNowMs = Number.isFinite(nowMs as number) ? (nowMs as number) : Date.now();
        const depthAtr =
            reclaimWake.extreme !== null &&
            Number.isFinite(reclaimWake.extreme) &&
            Number.isFinite(Number(reclaimWake.atr)) &&
            Number(reclaimWake.atr) > 0
                ? Math.round((Math.abs((reclaimWake.extreme as number) - reclaimWake.level) / Number(reclaimWake.atr)) * 100) / 100
                : null;
        market.reclaim_wake = {
            side: reclaimWake.side,
            level: reclaimWake.level,
            ...(reclaimWake.extreme !== null && Number.isFinite(reclaimWake.extreme)
                ? { extreme: reclaimWake.extreme }
                : {}),
            ...(depthAtr !== null ? { depth_atr: depthAtr } : {}),
            held_minutes: Math.max(0, Math.round((reclaimWake.reclaimedAtMs - reclaimWake.touchedAtMs) / 60_000)),
            reclaimed_minutes_ago: Math.max(0, Math.round((rwNowMs - reclaimWake.reclaimedAtMs) / 60_000)),
            ...(typeof reclaimWake.note === 'string' && reclaimWake.note.trim() ? { note: reclaimWake.note.trim() } : {}),
        };
    }
    if (sessionReclaim && Number.isFinite(sessionReclaim.level) && sessionReclaim.level > 0) {
        const srNowMs = Number.isFinite(nowMs as number) ? (nowMs as number) : Date.now();
        const srDepthAtr =
            sessionReclaim.extreme !== null &&
            Number.isFinite(sessionReclaim.extreme) &&
            Number.isFinite(Number(sessionReclaim.atr)) &&
            Number(sessionReclaim.atr) > 0
                ? Math.round(
                      (Math.abs((sessionReclaim.extreme as number) - sessionReclaim.level) / Number(sessionReclaim.atr)) * 100,
                  ) / 100
                : null;
        market.session_reclaim = {
            kind: sessionReclaim.kind,
            side: sessionReclaim.side,
            level: sessionReclaim.level,
            ...(sessionReclaim.extreme !== null && Number.isFinite(sessionReclaim.extreme)
                ? { extreme: sessionReclaim.extreme }
                : {}),
            ...(srDepthAtr !== null ? { depth_atr: srDepthAtr } : {}),
            held_minutes: Math.max(0, Math.round((sessionReclaim.reclaimedAtMs - sessionReclaim.touchedAtMs) / 60_000)),
            reclaimed_minutes_ago: Math.max(0, Math.round((srNowMs - sessionReclaim.reclaimedAtMs) / 60_000)),
        };
    }
    if (failedBreak && Number.isFinite(failedBreak.triggerPrice) && Number.isFinite(failedBreak.barClose)) {
        const fbNowMs = Number.isFinite(nowMs as number) ? (nowMs as number) : Date.now();
        market.failed_break = {
            side: failedBreak.side,
            trigger_price: failedBreak.triggerPrice,
            bar_close: failedBreak.barClose,
            bar_closed_minutes_ago:
                failedBreak.barClosedAtMs && failedBreak.barClosedAtMs > 0
                    ? Math.max(0, Math.round((fbNowMs - failedBreak.barClosedAtMs) / 60_000))
                    : null,
        };
    }
    if (positionWake?.fired && Number.isFinite(positionWake.fired.level)) {
        const pwNowMs = Number.isFinite(nowMs as number) ? (nowMs as number) : Date.now();
        const pwSetMinutesAgo =
            positionWake.fired.setAtMs && positionWake.fired.setAtMs > 0
                ? Math.max(0, Math.round((pwNowMs - positionWake.fired.setAtMs) / 60_000))
                : null;
        market.position_wake = {
            crossed: positionWake.fired.crossed,
            level: positionWake.fired.level,
            set_minutes_ago: pwSetMinutesAgo,
            // In-position bands are replaced on every look, so an age past one
            // primary candle of grace means no look happened in between (venue
            // closure, AI outage) — the attached plan predates a blind window.
            ...(pwSetMinutesAgo !== null && pwSetMinutesAgo > wakePlanGraceMinutes() ? { expired: true } : {}),
        };
        if (typeof positionWake.fired.note === 'string' && positionWake.fired.note.trim()) {
            market.position_wake.note = positionWake.fired.note.trim();
        }
    } else if (positionWake?.armed && (positionWake.armed.above !== null || positionWake.armed.below !== null)) {
        const pwNowMs = Number.isFinite(nowMs as number) ? (nowMs as number) : Date.now();
        market.position_wake_armed = {
            above: positionWake.armed.above,
            below: positionWake.armed.below,
            set_minutes_ago:
                positionWake.armed.setAtMs && positionWake.armed.setAtMs > 0
                    ? Math.max(0, Math.round((pwNowMs - positionWake.armed.setAtMs) / 60_000))
                    : null,
        };
        if (typeof positionWake.armed.note === 'string' && positionWake.armed.note.trim()) {
            market.position_wake_armed.note = positionWake.armed.note.trim();
        }
    }
    if (capitalMarketContext?.venue_session) {
        market.venue_session = capitalMarketContext.venue_session;
    }
    if (capitalMarketContext?.venue_events) {
        market.venue_events = capitalMarketContext.venue_events;
    }

    // SITUATIONAL DOCTRINE gating. Each block below explains how to READ one
    // optional market.* payload — measured in prod, most are absent on a
    // typical tick (event_reaction 1.2%, failed_break 0.3%, position_wake
    // 0.2%, LESSONS 9.8%), so unconditional prose spent thousands of chars
    // describing blocks that were not there. Reading the flags off the
    // ASSEMBLED market object (not the raw inputs) guarantees the doctrine is
    // present exactly when its payload is. All gated blocks are rendered in
    // one section at the END of the system prompt: prompt caching matches on
    // PREFIX, so per-tick variation must live in the tail or it fragments the
    // stable prefix that every symbol in a cron sweep shares.
    const hasEventReaction = Array.isArray(market.event_reaction) && market.event_reaction.length > 0;
    const hasBtcContext = !!market.btc_context;
    const hasSessionContext = !!market.forex_session || !!market.venue_events;
    const hasVenueSession = !!market.venue_session;
    const hasVenueEvents = !!market.venue_events;
    const hasCooldownWake = !!market.cooldown_wake;
    const hasWakeSweeps = Array.isArray(market.wake_band_sweeps) && market.wake_band_sweeps.length > 0;
    const hasReclaim = !!market.reclaim_wake || !!market.session_reclaim;
    const hasFailedBreak = !!market.failed_break;
    const hasPositionWake = !!market.position_wake;
    const hasLessons = Array.isArray(lessons) && lessons.length > 0;

    // Each note describes ONLY the context this category actually receives (see
    // /api/analyze: forex_session AND forex_events are both built for
    // forex/commodity/index — events resolve to the instrument's macro currency;
    // crypto gets forex_events on the USD calendar but no session levels).
    // Keep prose aligned with the data or it misleads.
    const assetNote =
        assetClass === 'crypto'
            ? `Asset class: crypto. Trades 24/7 — no session boundaries or weekend gaps, and no session-levels block is provided. market.forex_events carries the USD macro calendar (CPI/NFP/FOMC) — crypto reacts to these like a USD risk asset; treat it as event-risk context. News/sentiment can move price fast. market.fear_greed (when present) is the daily market-wide crypto Fear & Greed index (0=extreme fear … 100=extreme greed) — broad mood context. The recent daily history ships with it so that level, direction and persistence are yours to read; this prompt attaches no interpretation to any part of the range. Perp funding, when measured, is in state.costs.funding (borrow is not modeled); judge on structure, regime and location.`
            : assetClass === 'forex'
              ? 'Asset class: forex. Liquidity and volatility are session-dependent and weekend gaps exist. Treat market.forex_session levels and market.forex_events as first-class swing context (location + event risk).'
              : assetClass === 'commodity'
                ? 'Asset class: commodity (e.g. metals). Sensitive to USD, real yields and risk-on/off flows; strongly session-driven (London/NY). market.forex_events carries the relevant macro calendar (USD for metals — CPI/NFP/FOMC). Use market.forex_session levels + events as location and risk context.'
                : assetClass === 'index'
                  ? "Asset class: index. Session-driven and gap-prone around the cash open/close. market.forex_events carries the index's home-economy macro calendar. Use market.forex_session levels + events as location and risk context."
                  : `Asset class: ${assetClass}. No session or event context is provided; judge on structure, regime, location and cost.`;


    // Venue-session note — rendered only on ticks that CARRY market.venue_session
    // (the venue is inside a session), in the situational tail.
    const venueSessionNote =
        isCapital && hasVenueSession
            ? `Venue session (market.venue_session, ISO UTC): the current session ends at closes_at_utc (minutes_to_close min from now) and trading resumes at reopens_at_utc. While the venue is closed your exchange-side TP/SL bracket CANNOT fill, and the reopen can gap past your stop for a worse fill. Near the session end, avoid opening fresh risk unless the setup explicitly justifies holding through the closed-venue gap. minutes_since_open (when present) is the current session's age: small values mean the open just happened — spreads are still wide, the gap candle distorts short-timeframe indicators, and structure read from pre-close candles may no longer hold. Managing a position right after the open, judge the gap itself (did it break the thesis level?) rather than indicator readings that have not digested it yet.`
            : '';

    // Venue liquidity clock: schedule facts (when the tape's character changes),
    // paired with the session-offense guidance bullet.
    const venueEventsNote =
        isCapital && hasVenueEvents
            ? `Venue liquidity clock (market.venue_events, ISO UTC): recent/upcoming venue events (cash open/close, lunch break, exchange maintenance halt, weekly reopen) with minutes_ago/minutes_to, plus liquidity_phase ∈ {pre_open, opening_drive, into_close, venue_break, off_hours, thin_reopen, normal}. These are schedule facts, not signals — the session-liquidity guidance says how to trade around them.`
            : '';

    // Session doctrine, two modes (SESSION_OFFENSE_ENABLED, default OFF):
    // OFF = swing-defensive — session sweeps and venue phases are HAZARD
    // context (don't chase a sweep, don't add risk into thin tape), never an
    // entry playbook. ON = the intraday offense doctrine (sweep-capture resting
    // entries, opening-drive tactics), preserved for a future day-trade model.
    // Keyed on asset class (forex_session is built for forex/commodity/index;
    // crypto never gets it) with "when present" phrasing for tick-level blocks.
    const sessionOffenseGuidance =
        (assetClass === 'forex' || assetClass === 'commodity' || assetClass === 'index') && hasSessionContext
            ? SESSION_OFFENSE_ENABLED
                ? `Session liquidity offense (market.forex_session.signals, when present; phase tactics need market.venue_events): a sweep of a prior-day/session extreme is REVERSAL fuel, not continuation proof. When swept*Low=true, do NOT open or rest fresh shorts below that low unless price has ACCEPTED below it (primary close under the level); bullishLiquidityReclaim=true (a swept low reclaimed) is a long trigger AT the extreme — mirror exactly for swept highs (bearishLiquidityRejection). The offensive resting entry around a liquidity event sits BEYOND the level likely to be swept — BUY below the prior-day/session low when primary drift is up, SELL above the swept high when drift is down — so the stop-run itself fills you at the extreme and the snap-back is the trade; stop past the sweep extension, target back inside the range. Never leave a shallow resting entry IN THE PATH of an imminent venue event: it fills exactly when the level breaks against you. Phase tactics (when market.venue_events is present): opening_drive = displacement window — enter WITH the confirmed drive at market, or fade a COMPLETED sweep at an extreme; pre_open / into_close / venue_break / off_hours = thin, gap-prone tape — no fresh momentum entries, sweep-fade at clear levels only, reduced conviction; thin_reopen = the worst spreads of the week, treat fills as suspect and prefer HOLD.`
                : `Session liquidity (market.forex_session.signals + market.venue_events — DEFENSIVE context for a swing book, not an entry playbook): a sweep of a prior-day/session extreme is REVERSAL fuel, not continuation proof — do NOT open fresh risk in the sweep's direction unless price has ACCEPTED beyond the level (primary close through it), and never chase the sweep itself. During thin phases (pre_open, into_close, venue_break, off_hours, thin_reopen) spreads and gap risk are at their worst: no fresh entries there on session-timing grounds alone — a swing entry must be valid on primary structure regardless of the session clock, and if it is, the phase only argues for waiting, not hurrying.`
            : '';

    // Post-event reaction doctrine: how to read market.event_reaction (built only
    // for the calendar-carrying asset classes, and only when a high-impact event
    // released within the recent lookback). Keyed on asset class with "when
    // present" phrasing — same byte-stability rule as the other notes. The base
    // rates cited are measured (1m study, CPI/NFP/FOMC Jun 2024–Jun 2026): pre-
    // release drift direction ~50/50; net 30m move ≈ 1/3 of the 30m range; the
    // ~45min reaction direction persisted over the following ~4h on gold/EUR and
    // was gone by 24h. Crypto gets its own variant with crypto-measured base
    // rates (BTC/ETH/SOL replication, see eventReaction.ts header): persistence
    // confirmed for CPI/FOMC but NOT for NFP, which tends to give back — the
    // separate string keeps the other classes' prompt bytes unchanged.
    const eventReactionGuidance = !hasEventReaction
        ? ''
        : assetClass === 'forex' || assetClass === 'commodity' || assetClass === 'index'
          ? `Post-event reaction (market.event_reaction): a high-impact release just happened (see market.forex_events.recentEvents); each entry quantifies the reaction since the pre-release close — ret_since_release_bp (signed net move), range_since_release_bp (total excursion incl. whipsaw), retrace_pct (0 = price at the reaction extreme, 1 = push fully given back), minutes_since_release. Measured base rates: the PRE-release drift direction carries no information, and the release burst is mostly whipsaw (net move ≈ one-third of range) — but a decisive reaction direction, once established ~45 min after release, has historically persisted over the following ~2–4 h and decays after. Read it accordingly: large |ret| with low retrace_pct = post-event drift context in that direction; large range with |ret| near zero = undecided, treat as chop; retrace_pct ≈ 1 = the event is spent as a directional input. Weigh WITH structure/location as usual.`
          : assetClass === 'crypto'
            ? `Post-event reaction (market.event_reaction): a high-impact USD release just happened (see market.forex_events.recentEvents); each entry quantifies the reaction since the pre-release close — ret_since_release_bp (signed net move), range_since_release_bp (total excursion incl. whipsaw), retrace_pct (0 = price at the reaction extreme, 1 = push fully given back), minutes_since_release. Measured base rates on crypto (BTC/ETH/SOL, 2024–2026): the PRE-release drift direction carries no information (pre-FOMC drift, if anything, reversed), and the release burst is mostly whipsaw — but a decisive reaction direction, once established ~45 min after release, held through the following ~2–4 h on CPI and FOMC releases. NFP is the exception: its reaction direction was the least reliable and on average partially gave back, so discount NFP drift. Read it accordingly: large |ret| with low retrace_pct = post-event drift context in that direction (except NFP); large range with |ret| near zero = undecided, treat as chop; retrace_pct ≈ 1 = the event is spent as a directional input. Weigh WITH structure/location as usual.`
              : '';

    // BTC regime doctrine: how to read market.btc_context. Gated on the payload,
    // so BTC's OWN ticks (which never carry it — ~47% of crypto ticks) no longer
    // pay for it. Coupling numbers are FED, not asserted — correlation is
    // regime-dependent (SOL 30d slipped 0.84→0.77 while LINK sat at 0.94, Jul
    // 2026), so the model weighs the measured value instead of a hardcoded
    // "crypto = BTC beta" claim.
    const btcContextGuidance = hasBtcContext
        ? `BTC regime (market.btc_context): this asset's measured coupling to BTC — corr_30d/corr_90d (daily-return correlation), beta_90d, btc.ret_*_bp (BTC's own recent moves), and alt_vs_btc_residual_7d_bp (this asset's 7d return minus beta x BTC's; positive = idiosyncratic strength). corr_* is how much of this asset's recent daily movement BTC's direction actually accounted for over that window, beta_90d is how hard it moved per unit of BTC, and the residual is the part that BTC did NOT account for. Those three fix the weight BTC's direction deserves here on this tick; there is no threshold in this prompt at which it starts or stops mattering, and no extra burden on a trade in either direction relative to it. Measurements, not a verdict — combine with structure and location as usual.`
        : '';

    // In-position wake bands (ENABLE_POSITION_WAKE_BANDS): how to SET them (rides
    // the bracket section) and how to read a FIRED one (market.position_wake).
    // Additive early looks only — the flat cooldown machinery is untouched.
    // IN-POSITION variant only; the per-minute watch fact lives in CADENCE and
    // the code-enforced drop rules in the Output-hygiene hard constraint.
    const positionWakeGuidance = POSITION_WAKE_ENABLED && inPosition
        ? `\n  • Wake bands (HOLD or partial CLOSE only): set cooldown_wake_above / cooldown_wake_below to the price levels INSIDE your bracket where you want to look again the MOMENT price touches them instead of at the next ${primaryTimeframe} close — the structural levels that would change your management ("losing 3.42 support = thesis damaged → decide exit", "at 117.8k resistance decide trail vs take"). Bands suppress nothing (your regular ${primaryTimeframe}-close look still happens) and they are never protection — the bracket remains the guard, a band is only an early look. Whenever you set a band, ALSO set cooldown_wake_note — one short line stating the decision you plan to make there; it returns as market.position_wake.note when the band fires. Your bands are REPLACED by every decision you make: re-state them each look if you still want them (market.position_wake_armed shows what is currently armed); null clears them.`
        : '';
    const positionWakeTriggerGuidance = POSITION_WAKE_ENABLED && hasPositionWake
        ? `Position-wake trigger (market.position_wake): THIS look exists because price crossed the wake band you set on a previous management look (crossed = which side, level, set_minutes_ago, note = the plan you attached). Treat the note as a standing order from your past self: execute that plan if current structure confirms it, or explicitly override it in your reason (what changed?) — never ignore it. Being woken within ~a minute of a fast cross is the expected signature of the event you scheduled, not by itself alarming; judge whether the move through the level is real (acceptance, structure break) or a sweep, and manage accordingly. If expired: true (set_minutes_ago is past ~one primary candle — no management look happened in between: venue closure or an outage), the plan predates a blind window: judge the position fresh against current structure (and any gap) first, and treat the note as background, not as the decision.`
        : '';

    // How to read a FIRED wake band. Gated on market.cooldown_wake: ~78% of
    // flat ticks are routine scans that never carry it.
    const wakeTriggerGuidance = hasCooldownWake
        ? `Wake-band trigger (market.cooldown_wake): THIS evaluation exists because price crossed the wake band you set on a previous flat HOLD (crossed = which side, level, set_minutes_ago, note = the plan you attached when you set it). Treat it as the breakout/breakdown check you scheduled, not a routine scan — and treat the note as a standing order from your past self: EXECUTE it if current structure confirms it, or explicitly override it in your reason (what changed?), but never ignore it and rediscover the level from scratch. Extension on this tick: you are woken within ~a minute of the cross, and a level that gets crossed is almost always crossed FAST — elevated |extension_atr.micro| or a crest channel_pos at the instant of the cross is the expected signature of the very event you scheduled, NOT by itself a reason to skip. Judge instead: is the move through the level real (acceptance, structure break) or a sweep/fake-out, and is price still workably near the level (within ~1 primary-ATR) so the entry's risk anchors to it? When sustained_minutes or break_extension_atr is present, this wake is already CONFIRMED by construction — price either held beyond your band for the window you asked for, or broke with ≥0.5 primary-ATR of force — so the sweep-vs-break question is answered; weigh entry timing and location instead. RETEST WAKES: when the note marks this as the planned retest entry of an ALREADY-CONFIRMED break, the acceptance question was settled when the break confirmed — do NOT re-demand fresh BOS/acceptance evidence at the retest and do NOT treat the level as an anonymous cross; the confirmed break plus your presence at the level IS the setup you scheduled. Judge only whether structure has GENUINELY changed since confirmation (a primary bar closed back through the level, a regime flip, invalidating news) — otherwise execute the plan. Declining a planned retest entry requires naming the specific structural change, not restating generic location caution. EXPIRED wakes (expired: true, or a large set_minutes_ago): the band crossed only AFTER the plan's horizon had passed — a venue closure or an outage kept you from watching the market between setting the plan and now. The note is then a stale IDEA from a market state you never saw evolve, NOT a standing order: re-derive the setup from current structure as if scanning fresh, and mention the old plan in your reason only as background. Executing a stale plan because "past me scheduled it" is exactly the failure this flag exists to prevent. ${
              'If the move is real but price has already run multiple ATR beyond the level, you have both a resting entry back at the level and a fresh wake band on it WITH a note naming the intended entry ("retest of X after breakout → long on hold"); either beats chasing, and which one fits is yours to judge.'
          } ACT-OR-FOLD: this wake look ends one of exactly three ways — you ENTER (market or a resting entry), you arm the OPPOSITE side (e.g. the broken level’s retest per the retest protocol), or you FOLD the level (HOLD; the symbol returns to the normal cadence). Refusing a wake costs the watch even when you judge the cross a fake-out (the re-armed fired side is dropped — see the hard constraints), so do not spend this look re-scheduling the same rejection; if the level still matters after a fake-out, the evidence will be in wake_band_sweeps and the next scheduled scan can re-derive it.`
        : '';

    // Swept-band evidence. Independent of a FIRED wake — a band can be swept
    // repeatedly without ever confirming — so it carries its own gate.
    const wakeSweepsGuidance = hasWakeSweeps
        ? `market.wake_band_sweeps: earlier touches of your band that FAILED to hold (side, level, when, how deep, how long they lasted) — repeated sweeps of the same level mark a liquidity grab zone, not acceptance; treat additional pokes there with suspicion and consider fading rather than following.`
        : '';

    // One-shot bounce look after a sweep+reclaim, at your own band
    // (market.reclaim_wake) or at an unarmed session/prior-day pool
    // (market.session_reclaim). Gated on either payload.
    const reclaimGuidance = hasReclaim
        ? `Reclaim trigger (market.reclaim_wake${market.session_reclaim ? ' / market.session_reclaim' : ''}): THIS evaluation exists because a level was POKED and RECLAIMED minutes ago (side, level, extreme, depth_atr, held_minutes${market.reclaim_wake ? ', note = the band\'s plan' : ''}) — a liquidity grab AT the level, not acceptance beyond it. This is NOT a breakout check (any band plan stays fully armed and is untouched by this look): it is a one-shot BOUNCE OPPORTUNITY look at the freshest possible moment. Judge it as a level-bounce entry TOWARD the range per the level-bounce doctrine: the sweep extreme is the natural invalidation (stop just beyond it), the reclaim is the micro turn. Require genuine structural confluence — mechanically fading every sweep LOSES money (measured: 21% win rate); a sweep into a defended HTF level with room toward the other side is the shape that pays, a mid-chop poke is not. If you do not enter, simply HOLD: this look is READ-ONLY — your standing band, cooldown and plan remain exactly as they were, any cooldown_* fields you output are ignored in code, and the sweep stays recorded in wake_band_sweeps. Declining is final for this plan's lifetime (one look per band, and one per pool per session), so decide on the confluence in front of you, not on hope for a deeper sweep.${market.session_reclaim ? ' A session_reclaim is the same event class at a session/prior-day pool you did not have to arm yourself — kind names the pool (last_session_high/low, prior_day_high/low).' : ''}`
        : '';

    // Failed break-entry: the cheapest exit the model will be offered. Gated on
    // the payload (present on ~0.3% of ticks).
    const failedBreakGuidance = hasFailedBreak
        ? `Failed-break trigger (market.failed_break): you entered this position on a break of trigger_price and a ${primaryTimeframe} bar has now CLOSED back through it (bar_close, side, bar_closed_minutes_ago) — the break has FAILED by your own post-mortem lesson standard, and the first close back through the trigger is usually the cheapest exit you will be offered. Default action: CLOSE. Staying (or trimming instead) requires an explicit CURRENT structural reason stated in your reason — e.g. the close-through was a sweep that has already decisively reclaimed the level — not the entry thesis restated and not hope for a reclaim.`
        : '';

    // Lessons doctrine — how to weigh the LESSONS block. Gated on lessons
    // actually being attached: measured in prod, only ~10% of ticks carry any,
    // so the other 90% were paying ~1.4K chars to be told how to read a block
    // that wasn't there.
    const lessonsGuidance = hasLessons
        ? `LESSONS (user turn): 1-2 line lessons distilled from forensic evaluations of your OWN past trading — losses, wins, and MISSED ENTRIES (setups you declined that then worked) — on this symbol, its asset class, or any instrument. Each carries a [scope | learned from ...] tag showing what taught it: a lesson from losses warns you off a mistake; a lesson from missed entries exists because WAITING cost money — it is there to push you toward action, not away from it; a lesson backed from several sides (losses AND wins or missed entries) is a boundary tested from both directions — trust its bound most. These are patterns from your actual record, not generic advice — before deciding, check the setup against them and note in your reason when one applies. They are cautionary evidence like recent_actions outcomes, never hard rules: current structure and measurements win on conflict. When a lesson states a numeric bound (an ATR distance, a time window, a count), apply the bound EXACTLY as written: a setup OUTSIDE the bound is not blocked by that lesson, and citing a lesson as a reason to skip requires quoting the measured value that puts THIS setup inside its bound — "per lesson" without the number is not a valid refusal. Lessons name the mistake to avoid, not a mandate to never trade the pattern: a setup that satisfies a lesson's stated conditions is CLEARED by it, not merely tolerated.`
        : '';


    // ------------------------------
    // Resting entries — tools, not tactics
    // ------------------------------
    // Both order shapes are handed over unconditionally. WHICH to reach for,
    // and whether to reach at all, is the model's call from the data. This text
    // is MECHANICS plus the one selection asymmetry that is a property of the
    // instruments themselves; it deliberately prescribes no setup. The previous
    // prescriptive version ("rest a limit when the wave position is bad") is
    // what every losing post-mortem traced back to — the tool was never the
    // problem, being told where to point it was. It crept back once anyway,
    // phrased as a reading of micro_entry_ok ("poor timing ... is precisely when
    // a resting order at the level you would rather pay is the better tool") and
    // was removed again 2026-09-06. If a bullet anywhere in YOUR JOB ends by
    // naming which entry tool suits a measurement, that is this bug returning.
    const restingKinds = restingEntryKindsFor(platform);
    const canRestLimit = restingKinds.includes('limit');
    const canRestStop = restingKinds.includes('stop');
    const restingToolList = [
        canRestLimit
            ? `entry_limit_price — rests AGAINST your trade (BUY below current price, SELL above), ${ENTRY_LIMIT_MIN_ATR}–${ENTRY_LIMIT_MAX_ATR} primary-ATR away`
            : null,
        canRestStop
            ? `entry_stop_price — rests WITH your trade (BUY above current price, SELL below), ${ENTRY_STOP_MIN_ATR}–${ENTRY_STOP_MAX_ATR} primary-ATR away`
            : null,
    ]
        .filter(Boolean)
        .join('; ');
    // Name only the fields this venue can actually rest. Both keys always exist
    // in the schema, so an unusable one is called out explicitly — sending it
    // would drop the entry rather than degrade to the other tool.
    const restingUsable = [canRestLimit ? 'entry_limit_price' : null, canRestStop ? 'entry_stop_price' : null]
        .filter(Boolean)
        .join(' / ');
    const restingUnusable = [!canRestLimit ? 'entry_limit_price' : null, !canRestStop ? 'entry_stop_price' : null]
        .filter(Boolean)
        .join(' / ');
    const bothKinds = restingKinds.length > 1;
    const restingEntryFieldRule = restingKinds.length
        ? `${restingUsable}: on flat BUY/SELL you MAY rest ${bothKinds ? 'ONE of them' : 'it'} instead of taking the market (see resting-entry guidance); null = market now.${bothKinds ? ' Never set both.' : ''}${restingUnusable ? ` ${restingUnusable}: always null — not supported on this venue.` : ''}`
        : 'entry_limit_price / entry_stop_price: always null (market entries only).';
    const restingEntryGuidance = restingKinds.length
        ? `- Resting entry (flat BUY/SELL only) — you have ${bothKinds ? 'three' : 'two'} ways to get filled and they are TOOLS, not strategies: market now (${bothKinds ? 'both fields' : 'the field'} null), or ${bothKinds ? 'ONE resting order' : 'a resting order'}: ${restingToolList}.${bothKinds ? ' Setting both is contradictory and drops the entry.' : ''} The order rests on the venue and is CANCELLED at your next evaluation if unfilled — short-lived, not a standing commitment. take_profit_price and stop_loss_price anchor at the RESTING price, not current price. An invalid request (wrong side for the field you used, inside the minimum window${bothKinds ? ', or both fields set' : ''}) drops the ENTIRE entry for this evaluation and does NOT fall back to market — send null when you actually want market.
- The one thing worth knowing about the${bothKinds ? ' two' : 'se'} shape${bothKinds ? 's' : ''}, because it is a property of the instruments and not a view about markets: a resting LIMIT is adversely selected — it fills only because someone was willing to trade through your level, so you are filled most reliably exactly when the level is failing. A resting STOP is favourably selected — it triggers only once price has already moved your way — but you pay the spread on a moving market and it is exactly what a liquidity sweep is hunting. Neither fact makes either tool right or wrong here; weigh them against what the current structure, levels and liquidity picture actually show you, and pick the ${bothKinds ? 'one that fits' : 'entry that fits'} the play you are running. Nothing in this prompt tells you which play that should be.
- A resting entry SURVIVES this evaluation. When state.position.resting_entry is present (kind/side/price/age_min), that order is live on the venue right now and stays live unless THIS decision changes it — exactly like the standing TP/SL bracket in a position, where null means "leave the leg alone". Your options: HOLD leaves it resting (this is the default, and doing nothing is a real choice here, not an omission); a fresh BUY/SELL SUPERSEDES it (the old order is cancelled and yours replaces it — you do not need to cancel first); a BUY/SELL with both price fields null cancels it and enters at market; and withdraw_resting_entry=true on a HOLD takes it back without trading. Because silence keeps it, an order you no longer believe in must be withdrawn ON PURPOSE — read its age_min and ask whether the thesis that placed it still holds. The conversation above is where you placed it; re-validate that thesis against the CURRENT measurements rather than re-deriving it from scratch. A standing entry whose level price has trended away from, or whose structure has broken, is not a free option — it is a live order that will fill on the move that invalidates it.
- state.position.cancelled_pending_entry means a standing entry was cancelled by the code-owned age backstop (${Math.round(RESTING_ENTRY_MAX_AGE_MINUTES / 60)}h) before this evaluation — not by you. Treat it as a stale idea from a market state you have not re-checked, not as a plan to re-issue reflexively.
`
        : '';

    // The model names its own play. Never derived in code, never prescribed
    // here — the list is a vocabulary so the choice is measurable across
    // outcomes, not a menu of approved setups.
    const strategyGuidance = `- strategy: on BUY/SELL name the play you are actually running, one of: ${SWING_STRATEGIES.join(', ')}. This is a LABEL for your own decision, not a constraint on it and not a ranking — no play here is preferred, discouraged, or expected of you, and the code neither checks it against your entry nor treats any value differently. It exists so that which plays work on which instruments becomes measurable over time. Use 'other' rather than forcing a fit; null on HOLD.\n`;

    // Cost prose per venue. The live NUMBERS live in state.costs (user turn) —
    // this prose only explains how to read them, so the system prompt stays
    // byte-identical across ticks (prompt-caching prefix stability). Fields that
    // may be absent are described with "when present" so the prose never claims
    // a measurement that wasn't taken this tick.
    const costChurnLine = isCapital
        ? `Cost/churn: no commission on this venue; round-trip cost = state.costs.total_cost_bps (spread crossed once over entry+exit, + slippage; null = no live quote was measured this tick — spread unknown, see market.liquidity.spread_bps). Holding cost: state.costs.overnight_fee_pct_per_day, when present, accrues each night held (per side; negative = you pay) — over a multi-day swing it can rival the round-trip cost, so weigh it on HOLD vs CLOSE and when choosing direction. Cost is one term in the trade, not a veto — weigh it against what you expect the move to pay.`
        : `Cost/churn: round-trip cost = state.costs.total_cost_bps. Perp funding (state.costs.funding, present only when measured this tick): rate_pct_per_interval accrues each funding settlement (every interval_hours hours when given; next charge at next_funding_at_utc when given) while held — positive = longs pay shorts, negative = shorts pay longs. Over a multi-day hold funding can rival the round-trip fee; weigh it on HOLD vs CLOSE and when choosing direction. Cost is one term in the trade, not a veto — weigh it against what you expect the move to pay.`;

    const bracketFillNote = isCapital
        ? 'Exchange-side TP/SL bracket: rests on the venue between these evaluations, but fills ONLY while the venue is open — a reopening gap can jump the stop and fill worse than the stop level.'
        : 'Exchange-side TP/SL bracket: fills 24/7 between these evaluations.';

    // Code-enforced value clamps, collected in ONE hard-constraint row per
    // variant. The doctrine sections no longer restate these — a rule that code
    // enforces gets one line here, prose explains only the judgment.
    const outputHygieneRow = inPosition
        ? `Output hygiene: prices must sit on the correct side of current price and clear it by more than noise, and a stop amendment may only TIGHTEN.${POSITION_WAKE_ENABLED ? ` A wake band at/beyond your standing SL/TP, or within ~${POSITION_WAKE_MIN_ATR} primary-ATR of price, is dropped.` : ''} Entry-only and cooldown fields stay null in a position.`
        : `Output hygiene: prices must sit on the correct side of current price and clear it by more than noise; cooldown_minutes clamps to ${HOLD_COOLDOWN_MIN_MINUTES}–${HOLD_COOLDOWN_MAX_MINUTES} and cooldown_wake_confirm_minutes to ${WAKE_CONFIRM_MIN_MINUTES}–${WAKE_CONFIRM_MAX_MINUTES}; re-arming the wake side that just fired is dropped. A resting entry is the one field that drops the WHOLE entry when invalid (wrong side for the field used, inside its minimum window, both set at once, or unsupported here) — it becomes a HOLD rather than a market order. One more way an entry can vanish: size comes from your stop distance, so a stop wide enough to put the position under this venue's minimum order size drops the entry to HOLD as well (recorded in the reason). Nothing bounds how wide a stop you may set — but past a point the trade stops existing rather than getting smaller.`;

    // Sizing ownership. Stop distance sets the notional only at the moment a
    // position is OPENED, so the "tighter stop = smaller position" half is true
    // for an entry and false for a stop amendment on a live position — the size
    // is already fixed there, and only the loss shrinks. In-position ticks get
    // the amendment-true version, plus the entry version scoped to REVERSE,
    // which is the one in-position action that opens a position.
    const capBindsUnderPct = RISK_EQUITY_PCT / EXPOSURE_CAP_EQUITY_MULT;
    const entrySizingRule =
        `notional = ${RISK_EQUITY_PCT}% of equity ÷ (stop distance as a fraction of entry), capped at ` +
        `${EXPOSURE_CAP_EQUITY_MULT}× equity in exposure. That cap binds for any stop tighter than ` +
        `${capBindsUnderPct}% of entry — i.e. almost always — and while it binds a stop-out costs ` +
        `${EXPOSURE_CAP_EQUITY_MULT} × your stop distance in equity, so a TIGHTER STOP IS A SMALLER ` +
        `POSITION AND A SMALLER LOSS, not the same loss at better R`;
    const sizingOwnershipLine = inPosition
        ? ` Sizing is already settled for this position — it was fixed from the stop distance at entry, and amending a stop does NOT resize it (a tighter stop shrinks the remaining loss, not the exposure; only exit_size_pct changes size). On a REVERSE the new position is sized fresh: ${entrySizingRule}.`
        : ` Position size is computed in code from your stop distance: ${entrySizingRule}. Place the stop where the setup is actually invalidated; do not shrink it to buy nominal R, and do not widen it to buy size. Your conviction is expressed through taking or skipping the trade and through stop/target placement, not through size.`;

    // Hard constraints, variant-scoped: flat ticks never read in-position rows
    // and vice versa. Numbering is per-variant (nothing references the numbers).
    const hardConstraintRows = [
        inPosition
            ? 'Allowed actions (you are IN A POSITION): HOLD/CLOSE/REVERSE only.'
            : 'Allowed actions (you are FLAT): BUY/SELL/HOLD.',
        // Flat entries have no timing constraint any more: micro_entry_ok was
        // demoted to a measurement (see evaluateActionability). Nothing replaces
        // the row — claiming a constraint that no longer coerces would spend the
        // model's caution on a rule that cannot fire.
        // Mirrors postprocessDecision's base-gate block exactly, per variant and
        // per policy: flat is always HOLD; in a position `strict` forces CLOSE
        // while `balanced` only demotes REVERSE→CLOSE and otherwise leaves the
        // action alone. Stating "risk-off forced" under `balanced` (the default
        // since 2026-09-02) would claim a coercion that cannot fire.
        inPosition
            ? `Base gates: if any of state.gates.{spread_ok,liquidity_ok,atr_ok,slippage_ok} is false → ${
                  strictPolicy
                      ? 'the position is force-CLOSED whatever you answer'
                      : 'a REVERSE is demoted to CLOSE; HOLD and CLOSE stand as you set them'
              }.`
            : 'Base gates: if any of state.gates.{spread_ok,liquidity_ok,atr_ok,slippage_ok} is false → entries forced to HOLD.',
        ...(!inPosition && REENTRY_COOLDOWN_MIN > 0
            ? [
                  `Re-entry cooldown: for ${REENTRY_COOLDOWN_MIN} min after a position closes, re-entering the SAME direction is blocked (state.position.reentry_cooldown shows the blocked side when active; the opposite direction stays allowed).${SESSION_OFFENSE_ENABLED ? ' Exception: a sweep-reclaim re-entry passes — when the matching reclaim signal is live (market.forex_session.signals.bullishLiquidityReclaim for a blocked long, bearishLiquidityRejection for a blocked short), the block is lifted, so a stop-out on a swept extreme does NOT forfeit the reclaim trade.' : ''}`,
              ]
            : []),
        outputHygieneRow,
    ];
    const hardConstraintsBlock = hardConstraintRows.map((row, i) => `  ${i + 1}. ${row}`).join('\n');

    // Per-field output rules (the former user-turn TASKS list). Moved into the
    // byte-stable system prompt: chained threads used to replay this ~1.9K
    // block once per STORED turn. Variant-scoped like the hard constraints.
    const fieldRules = inPosition
        ? [
              'action: exactly one of HOLD/CLOSE/REVERSE (see DECISION OWNERSHIP).',
              leverageTask,
              'exit_size_pct for CLOSE/REVERSE (100 = full close, 30–70 = trim; REVERSE is always 100), else null.',
              'take_profit_price / stop_loss_price: on REVERSE set both for the NEW opposite-side position (TP required — see bracket guidance); on HOLD/partial CLOSE a non-null value amends the standing leg (stop tighten-only), null = unchanged.',
              'entry_limit_price, entry_stop_price, withdraw_resting_entry and entry_trigger_price: always null in a position.',
              'strategy: name the play this POSITION was opened on if you know it, else null.',
              POSITION_WAKE_ENABLED
                  ? 'cooldown_wake_above / cooldown_wake_below (+ cooldown_wake_note whenever a band is set) arm in-position wake bands (see wake-band guidance); cooldown_minutes and cooldown_wake_confirm_minutes stay null.'
                  : 'cooldown_minutes and all cooldown_wake_* fields: always null in a position.',
              'summary ≤3 lines; reason = brief rationale.',
              ...(isCapital
                  ? []
                  : [
                        marginRecycleEnabled
                            ? 'raise_leverage_to / move_stop_to_be: the margin-recycle request fields (see guidance); null both unless requesting the maneuver.'
                            : 'raise_leverage_to / move_stop_to_be: always null (feature disabled).',
                    ]),
          ]
        : [
              'action: exactly one of BUY/SELL/HOLD (see DECISION OWNERSHIP).',
              leverageTask,
              'exit_size_pct: always null when flat.',
              'take_profit_price: REQUIRED price target on BUY/SELL (resting exchange TP); stop_loss_price: the structural invalidation stop on BUY/SELL (null = wide catastrophe default); both null on HOLD.',
              restingEntryFieldRule,
              'strategy: on BUY/SELL, name the play you are running (see the strategy list); null on HOLD.',
              'withdraw_resting_entry: true only on a flat HOLD to cancel a standing resting entry you no longer want; else null. Omitting it LEAVES the order resting.',
              'cooldown_minutes (+ optional cooldown_wake_above/cooldown_wake_below, cooldown_wake_note whenever a band is set, and cooldown_wake_confirm_minutes for breakout-intent bands that should only wake on a HELD break): on a flat HOLD you MAY request a quiet period (see flat-cooldown guidance); else null.',
              'entry_trigger_price: on a flat BUY/SELL with a breakout/breakdown thesis, the trigger level whose break justifies the trade (arms the failed-break watch — see guidance); else null.',
              'summary ≤3 lines; reason = brief rationale.',
              ...(isCapital ? [] : ['raise_leverage_to / move_stop_to_be: always null when flat.']),
          ];
    const fieldRulesBlock = fieldRules.map((rule, i) => `  ${i + 1}) ${rule}`).join('\n');

    // SITUATIONAL DOCTRINE: every block whose payload is optional, collected in
    // the TAIL of the system prompt. Order is fixed (trigger-for-this-tick
    // first, then background colour) so the rendered text is deterministic for
    // a given combination of present payloads. Empty on a plain routine scan,
    // which is the common case.
    const situationalBlocks = [
        wakeTriggerGuidance,
        reclaimGuidance,
        failedBreakGuidance,
        positionWakeTriggerGuidance,
        wakeSweepsGuidance,
        venueSessionNote,
        venueEventsNote,
        sessionOffenseGuidance,
        eventReactionGuidance,
        btcContextGuidance,
        lessonsGuidance,
    ].filter((block) => !!block);
    // VENUE & ASSET CLASS: the only per-INSTRUMENT prose in these instructions,
    // collected in the TAIL next to the situational blocks. Prompt caching
    // matches on PREFIX and a cron sweep mixes asset classes within one cache
    // window, so the single asset-class line that used to sit at the TOP gave
    // every asset class its own prefix — it diverged at char 103 of a ~20K
    // system prompt and the measured cache-read rate was 13% overall (0 on a
    // typical tick). Everything ABOVE this section is byte-identical for every
    // symbol of the same variant, whatever the asset class. Keep it that way:
    // anything per-instrument or per-tick belongs here or below, never above.
    const venueSection = `\nVENUE & ASSET CLASS (fixed facts about this instrument — read alongside YOUR JOB)\n${[
        assetNote,
        costChurnLine,
        bracketFillNote,
        leverageGuidance,
        inPosition ? manageGuidance : '',
    ]
        .filter((row) => !!row)
        .map((row) => `- ${row}`)
        .join('\n')}`;

    const situationalSection = situationalBlocks.length
        ? `\n\nSITUATIONAL DOCTRINE (optional blocks that ARE present on this tick — read alongside YOUR JOB)\n${situationalBlocks
              .map((block) => `- ${block}`)
              .join('\n')}`
        : '';

    const sys = `
You are a swing-trading market-structure analyst. Decide one action and size it.

TIMEFRAMES (fixed)
- micro=${microTimeframe} (entry timing/confirmation), primary=${primaryTimeframe} (setup+execution), macro=${macroTimeframe} (regime bias), context=${contextTimeframe} (HTF location + major levels, risk lever)${inPosition ? '' : `, nano=${NANO_TIMEFRAME} (state.geometry.nano, flat entry scans only — fine-timing of an already-valid entry, never a setup by itself and never an exit signal)`}.
Scale: ${primaryTimeframe} is the execution timeframe and you are consulted on its bar close, so a position typically lives days rather than minutes. That is a property of the cadence, not a target — which setups on which timeframes are worth taking is yours to decide.

CADENCE (how often you are actually consulted)
- You are evaluated once per ${primaryTimeframe} bar close — flat scans and in-position management alike. Between looks the exchange-side TP/SL bracket is the ONLY manager, so every bracket you leave behind must stand on its own for at least one full ${primaryTimeframe} bar.
- Earlier looks happen only when: a wake band you set is crossed${POSITION_WAKE_ENABLED ? ' (flat or in a position)' : ' (flat)'} or, in a position, price has moved several primary-ATRs since your last look (emergency check — do not rely on it for routine management). Both conditions are watched roughly once per MINUTE, so a crossed band reaches you almost immediately — place bands exactly at the decision levels, no padding needed. A wake band and an entry placed now are two tools for the same level: the band keeps the decision and costs you a beat, the entry commits and cannot be reconsidered until it fills or you withdraw it. Which one fits the level is yours.
- A resting entry needs no look at all: it stands on the venue between evaluations and fills whenever price reaches it, without consulting you. You find out by arriving to an OPEN POSITION on a later tick. That is the point of the tool — but it also means a standing order is exposure you are carrying while unable to reconsider, so place it only where you would still want the fill on the tape you cannot see.

INPUTS
- You receive two JSON objects: STATE (derived signals — your single source of truth) and MARKET (raw price/tape/news). All keys are pre-computed; do not invent fields.
- state.levels gives the nearest support/resistance per timeframe. Distances are in that timeframe's ATR and level state ∈ {at_level, approaching, rejected, broken, retesting}. Three qualifiers decide how much a level is worth, and they are NOT interchangeable:
  • bars = how many candles the level scan actually had for this timeframe, typically 200. It can be much smaller for a recently listed instrument, and when it is, the level came from a short sample and is provisional rather than structural — anything older than that window is invisible to you, not absent from the market.
  • type = where the level came from. swing_pivot = a confirmed pivot high/low (its bar's extreme beat the two bars either side). range_extreme = no pivot existed in the sample, so this is the nearest bar extreme — a single wick print that was never tested. forming_bar = the extreme of the bar that is STILL OPEN, so it has not closed even once; it is the weakest label here and it can move before your next look.
  • bars_ago / strength = the level's AGE, raw and normalized. bars_ago counts bars of that timeframe (0 = the bar still forming). strength rescales it over the sample actually available — strength = 1 − bars_ago/min(bars,150) — so 1.0 is the newest bar and 0.2 is as old as this timeframe's history goes. Age is ALL either field measures: neither counts touches, holds or rejections, and a level's price alone tells you nothing about whether it was ever defended. So a high strength on a range_extreme or forming_bar reads "very recent and untested", NOT "reliable" — do not treat it as conviction, and do not price a bracket off it as if it were a tested shelf. If you want to know whether a level held, read the bar extremes and structure fields, not these two.
- micro_bias precedence (already applied in state.biases.micro): structure (breakout-retest → break-state → BOS → structure-state) first, momentum (EMA slope+RSI+price vs EMA20) as fallback; structure wins ties.
- market.recent_actions: your last few decisions on this symbol (oldest first) with their MEASURED follow-through where known — rested_at / rested_as = the price that entry rested at and which tool rested there (limit or stop), absent when it took the market; strategy = the play you named at the time; reissued_count = consecutive re-issues of the same resting idea collapsed into one row (one idea, not repeated trades); outcome ∈ never_filled (the resting order was cancelled unfilled — NO position resulted, you did not trade) | still_open | {closed_pnl_pct_on_margin (leverage-multiplied), held_min}. Weigh outcomes as recent evidence about your read of this market — e.g. a just-stopped-out direction needs a materially changed setup, and a never_filled entry means that idea was never tested. Over several rows this is also feedback on your own play selection and entry mechanics on THIS instrument: if one strategy or one resting tool keeps producing never_filled or quick losses here while another works, that is data about this market, not a rule — read it and adjust.
- Optional blocks: STATE/MARKET carry extra keys only when this tick measured them, and the user turn may carry extra sections. Any present on THIS tick are explained in the SITUATIONAL DOCTRINE section at the end of these instructions; when that section is absent this tick carries none beyond the keys described above.

DECISION OWNERSHIP
- You own the conviction read: judge setup quality and selectivity yourself from the structure, location, regime and momentum measurements in STATE — there is no pre-computed verdict to defer to.${sizingOwnershipLine}
- The HARD constraints below are enforced in code AFTER you respond. Do not spend reasoning re-deriving them — if you violate one your action is silently coerced (a wasted call). Just stay inside them:
${hardConstraintsBlock}

YOUR JOB (soft judgment — where your reasoning actually matters)
- Decide the action from STATE and place its bracket. STATE gives you structure (BOS/CHoCH/breakout-retest), momentum, location and regime as separate measurements; how you weigh them against each other is the judgment this call exists to make, and nothing here ranks them for you.
- One ordering IS asserted, once, and it holds only BETWEEN kinds of input, never within them: the price-derived measurements in STATE are the primary evidence, and the surrounding context blocks (BTC coupling, market mood, the macro calendar, session levels, post-event reaction, news and sentiment) are weighed ALONGSIDE that evidence rather than in place of it. Inside each group nothing is ranked, and no context block is separately demoted — they all sit at the same distance from the decision.
- Location vs regime: state.biases.macro / .context are the higher-timeframe lean, and alignment with them is a measurement you weigh, not a requirement — a counter-regime entry at a well-defined level with clean invalidation is a normal trade, not an exception that needs justifying. A near opposite level (levels.*.dist_atr or location.context_*_dist_atr under ~0.6 ATR) cuts the room available to a market entry taken now — and is simultaneously the best-defined price on the chart to rest an order at or beyond. Read it as location information, not a prohibition: what it rules out is paying market into a wall, not trading the wall. Same for location.chop_risk (both nearest levels close): it prices down a directional market entry and prices up working the range edges. Which of those, if either, is worth doing is your call.
- momentum.micro_entry_ok is a coarse timing READ, not a constraint: true when price sits near either EMA20 or micro RSI is at an extreme — i.e. somewhere a MARKET fill is reasonable right now. false does NOT mean "do not enter": it means a MARKET fill taken right here is poorly timed. What follows from that — take it anyway, rest an order somewhere better, or wait — is yours, and this prompt does not pick. Weigh it against extension below, which measures the same thing more finely.
- Extension: state.extension_atr is how far price has travelled from its EMA20, in ATRs — the distance a market fill would be paying up for, and the distance a pullback would give back. What to do with it is yours.
- Wave position (state.geometry): channel_pos maps price inside the timeframe's regression channel (0=low, 1=high) and slope_atr is its drift per bar, so together they say where in the current leg price sits and which way the leg is going. support_trendline / resistance_trendline give the live trendline price and slope; a close through one plus a structure signal is a break, a touch alone is a reaction point.${inPosition ? '' : ' geometry.nano is the same measurement one timeframe down, for fine-timing.'} Whether a given position in the wave is a place to buy, to fade, or to wait is a read, and it is yours.${
        inPosition
            ? `\n- PnL scales — state.position.unrealized_pnl_pct_on_margin (and max_drawdown_pct/max_profit_pct) are leverage-multiplied return on margin; price_move_pct and closing_guardrails.price_vs_breakeven_pct are on PRICE scale. Judge "how far has this actually moved" on price scale, not margin scale.\n- Managing the position: you have four moves and they are not ranked — HOLD (leave the standing bracket to work), amend a leg, trim part of it (exit_size_pct under 100), or close it (100). The bracket you left last time is already an exit plan that works without you between looks, so HOLD is what happens if you do nothing, not a recommendation. What is worth knowing: an exit forfeits whatever the remaining target was worth, and a re-entry later pays the round trip again — both are costs to weigh, neither is a rule. REVERSE is a full close plus an opposite open in one action (exit_size_pct=100, no partials).\n- Entry thesis: earlier turns of this conversation are your own entry decision and management ticks for this position — manage against that thesis: HOLD while it stays intact; trim/CLOSE when it is invalidated or has played out. Weigh it as context, not a command: current structure wins on conflict. If this conversation has no earlier turns (position adopted mid-life), judge purely from current structure. Those earlier user turns are ABBREVIATED records (marked as such): they keep the readings each past decision rested on, but their candles, orderbook, geometry and news were dropped as stale. That is by design, not missing data — never treat a field's absence THERE as a change in the market, and read every current measurement from the STATE/MARKET of THIS turn, which is complete.`
            : ''
    }
- Exchange-side TP/SL bracket:
  • On ${inPosition ? 'REVERSE — for the NEW opposite-side position —' : 'BUY/SELL'} set BOTH legs — take_profit_price at your structural target, stop_loss_price at the invalidation that voids the setup. Their distances are yours: no minimum on either, and no required ratio between them. A tight stop with a near target and a wide stop with a far one are both whole trades; what they have to beat is cost (state.costs.total_cost_bps, round trip), not a threshold. Code only keeps each leg on the correct side of price and off the current print. The bracket rests on the exchange until it fills or a later evaluation amends it. A leg you leave null gets a wide ${EXCHANGE_TP_FALLBACK_ATR_MULT}×ATR default — never the trade you meant, so set both.${
        inPosition
            ? `\n  • On HOLD or partial CLOSE, you MAY amend the standing bracket: output a new take_profit_price and/or stop_loss_price, or null to leave a leg unchanged. state.position.take_profit_price / stop_loss_price show the current resting levels (null = none on that leg). Whether either leg should move as the trade develops, and to where, is the same structural judgment that placed it.`
            : ''
    }${positionWakeGuidance}
${
        inPosition
            ? ''
            : restingEntryGuidance + strategyGuidance
    }${
        inPosition
            ? ''
            : `- entry_trigger_price (flat BUY/SELL only, protective bookkeeping — never an order parameter): when the entry THESIS is a breakout/breakdown (including a breakout-retest continuation), set this to the trigger level whose break justifies the trade — the broken structure level itself (BUY: below current price, SELL: above; wrong-side values are dropped). Code arms a failed-break watch on it: if a later ${primaryTimeframe} bar CLOSES back through this level, you are woken within minutes with market.failed_break to decide the exit instead of discovering the failure bars later. null when the thesis is not a break (level bounce, range fade, reclaim) — a null on a genuine break entry silently disarms this protection.\n`
    }${
        inPosition
            ? ''
            : `- Flat plan (flat HOLD only; ignored on any other action — enforced in code). When you are not committing this tick you may still leave a plan behind, built from four fields that each answer ONE question:
  • cooldown_minutes — how long to stay quiet. Suppresses routine flat re-scans of this symbol only; it never mutes in-position management or resting-entry re-evaluations. Clamps to ${HOLD_COOLDOWN_MIN_MINUTES}–${HOLD_COOLDOWN_MAX_MINUTES}. null = keep the normal cadence.
  • cooldown_wake_above / cooldown_wake_below — WHERE you want to be looking: the price levels that would change your mind. Crossing one ends the cooldown and brings you back. They are watched roughly once per MINUTE, so place them exactly at your decision levels, with no padding.
  • cooldown_wake_note — REQUIRED whenever you set a band. One short line stating the plan the band encodes ("acceptance above 3.42 → breakout check", "retest of broken 118.4k → long on reclaim"). The wake evaluation is a fresh stateless scan; without the note your future self receives an anonymous level cross and has to rediscover the idea.
  • cooldown_wake_confirm_minutes — WHAT COUNTS as an event at that level, and nothing else. Set it and the wake fires only if price is STILL beyond the band that many minutes after first touch; a poke that reclaims sooner never wakes you and comes back instead as market.wake_band_sweeps (evidence of a liquidity grab, not acceptance). About 10 minutes is the measured figure for "the level actually went"; clamps to ${WAKE_CONFIRM_MIN_MINUTES}–${WAKE_CONFIRM_MAX_MINUTES}. null = the touch ITSELF is the event, unfiltered — right when any tag of the level changes your read. Either way a break that extends ≥${wakeBreakConfirmAtr()} primary-ATR beyond the band confirms IMMEDIATELY by force, before the clock, so you are never late on a runner.
- A wake band is a WATCH. It never trades for you — however it fires, it brings you back to decide, and cooldown_wake_confirm_minutes only changes what is worth waking you for. If you want to COMMIT to a break rather than look at it, that is a resting stop order beyond the level: same trigger, placed at the venue, no second decision. Choose deliberately — the band keeps the decision and costs you a beat; the order takes the trade and can be swept. Nothing filters an order the way confirm_minutes filters a band.
- PLACEMENT: a wake spends the watch whichever way you answer it — refusing the cross drops the re-armed fired side (see the hard constraints) — so the band's cost is the same for a YES and for a NO. Price that in when you choose the level: if the first ~1 primary-ATR beyond it runs straight into an unbroken opposing level, the room a fill would have there is what you are buying the watch for.
- When a CONFIRMED wake fires but the location has degraded (price extended well past the level, or the move has already landed on the next opposing level), you are not limited to chasing or re-arming: rest an order back at the broken level's retest, or arm the retest band with confirm null and a note saying what you intend there. Re-arming the side that JUST fired is dropped in code — choose a different level or a different tool.
`
    }${
        inPosition
            ? `- Position truthfulness: never describe a position as winning when unrealized_pnl_pct_on_margin < 0 or price_vs_breakeven_pct is on the losing side.\n`
            : ''
    }${venueSection}${situationalSection}

OUTPUT (every response)
- Strict JSON only, parseable by JSON.parse — no markdown, comments, trailing commas, or extra keys:
{"action":"BUY|SELL|HOLD|CLOSE|REVERSE","summary":"≤2 lines","reason":"brief rationale","exit_size_pct":null|0-100,"take_profit_price":null|price,"stop_loss_price":null|price,"entry_limit_price":null|price,"entry_stop_price":null|price,"withdraw_resting_entry":true|false|null,"entry_trigger_price":null|price,"strategy":null|"one of the strategy list","cooldown_minutes":null|minutes,"cooldown_wake_above":null|price,"cooldown_wake_below":null|price,"cooldown_wake_note":null|"≤1 short line","cooldown_wake_confirm_minutes":null|minutes${leverageJsonField}${manageJsonField}}
- Field rules:
${fieldRulesBlock}
`.trim();

    // The user turn carries only per-tick DATA: chained threads store every
    // user turn verbatim (swing.ai_threads.transcript), so static prose here
    // was replayed once per stored turn — it now lives in the system prompt.
    const user = `
You are analyzing ${baseSymbol} for swing trading (mode=${modeLabel}, asset_class=${assetClass}).

STATE (derived signals — single source of truth):
${JSON.stringify(state)}

MARKET (raw inputs):
${JSON.stringify(market)}
${
    Array.isArray(lessons) && lessons.length
        ? `\nLESSONS (from forensic evaluations of your past trading — see INPUTS):\n${lessons
              .map((l) => `- [${l.scope}${l.originLabel ? ` | learned from ${l.originLabel}` : ''}] ${l.lesson}`)
              .join('\n')}\n`
        : ''
}${
    perplexity_context?.text
        ? `\nFRESH SENTIMENT (search-grounded news + social digest, generated ${new Date(perplexity_context.fetchedAtMs).toISOString()} — a context block, weighed per the ordering in your system instructions). Item times are absolute UTC; each parenthesised age is measured from that generation time, NOT from now, so add the gap when the digest is older than this tick:\n${perplexity_context.text}\n`
        : ''
}
Decide now per the OUTPUT contract in your system instructions — strict JSON only.
`;

    // ---- Abbreviated form of THIS turn, for the stored transcript ----------
    // A chained in-position thread resends its whole transcript every call, and
    // each stored turn used to carry the full STATE/MARKET tape — measured at
    // 14.4K avg / 33K peak input tokens on in-position ticks vs 7.4K flat. The
    // tape is worthless eight turns later (those candles, walls and trendlines
    // describe a market that no longer exists); what the model actually manages
    // against is its own past REASONING plus the readings that reasoning rested
    // on. So the LIVE turn stays complete and only the ARCHIVED copy is slimmed.
    //
    // Kept: the decision-relevant readings (bias/structure/momentum/location/
    // levels/position) and any TRIGGER block, which is why the decision came out
    // the way it did. Dropped: geometry, candles, orderbook, volume profile,
    // recent_actions (the transcript IS that history), and the per-tick colour
    // (news headlines, sentiment digest, lessons, calendars) that is re-fetched
    // fresh every tick anyway. Same key names as the live turn so the shape
    // stays familiar — just fewer of them.
    const compactState: Record<string, unknown> = {
        time: { iso_utc: state.time.iso_utc },
        biases: state.biases,
        trend: state.trend,
        structure: state.structure,
        momentum: { rsi: state.momentum.rsi, micro_entry_ok: state.momentum.micro_entry_ok },
        extension_atr: state.extension_atr,
        volatility: { atr_pct: state.volatility.atr_pct },
        location: state.location,
        levels: state.levels,
        position: state.position,
    };
    if (state.closing_guardrails) compactState.closing_guardrails = state.closing_guardrails;

    const compactMarket: Record<string, unknown> = { price: market.price };
    // Trigger blocks only — the reason THIS look happened.
    for (const key of [
        'cooldown_wake',
        'wake_band_sweeps',
        'reclaim_wake',
        'session_reclaim',
        'failed_break',
        'position_wake',
    ] as const) {
        if (market[key] !== undefined) compactMarket[key] = market[key];
    }

    const userCompact = `[ABBREVIATED EARLIER TURN — the full STATE/MARKET tape from this evaluation has been dropped to keep this conversation small. These are the readings the decision below rested on.]
STATE: ${JSON.stringify(compactState)}
MARKET: ${JSON.stringify(compactMarket)}`;

        return { system: sys, user, userCompact };
    };

    const context = {
        // Code-owned conviction. NOT shown to the model (kept out of the prompt so it
        // doesn't anchor the model's analysis) — used only to gate the AI call before
        // spending it (flat + sub-MEDIUM → no call) and by postprocessDecision.
        signal_strength: signalStrength,
        micro_bias_calc: microBiasLabel,
        primary_bias: primaryBias,
        macro_bias: macroBias,
        context_bias: contextBias,
        primary_trend_up: primaryTrendUp,
        primary_trend_down: primaryTrendDown,
        primary_breakdown_confirmed: primaryBreakdownConfirmed,
        primary_breakout_confirmed: primaryBreakoutConfirmed,
        micro_entry_ok: Boolean(momentumSignals.info?.microEntryOk),
        aligned_driver_count: alignedDriverCount,
        regime_alignment: regimeAlignment,
        location_confluence_score: locationConfluenceScore,
        micro_extension_atr: momentumSignals.microExtensionInAtr ?? null,
        primary_extension_atr: distance_from_ema20_primary_atr,
        breakout_retest_ok_primary: breakoutRetestOk4h,
        breakout_retest_dir_primary: breakoutRetestDir4h ?? null,
        forex_session_context,
    };

    const trendlineDistAtr = (priceNow?: number | null): number | null => {
        const atrP = Number(atr_primary);
        if (!Number.isFinite(priceNow as number) || !Number.isFinite(atrP) || atrP <= 0) return null;
        if (!Number.isFinite(last) || last <= 0) return null;
        return Math.abs(last - (priceNow as number)) / atrP;
    };

    const actionability = evaluateActionability({
        primaryBreakoutConfirmed,
        primaryBreakdownConfirmed,
        primaryBreakoutRetestOk: breakoutRetestOk4h,
        primaryBreakoutRetestDir: breakoutRetestDir4h,
        primaryBos: bos4h,
        primaryBosDir: bosDir4h,
        primaryBreakState: structureBreakState4h,
        primarySupportDistAtr: primarySR?.support?.dist_in_atr ?? null,
        primaryResistanceDistAtr: primarySR?.resistance?.dist_in_atr ?? null,
        microBreakoutRetestOk,
        microBreakoutRetestDir,
        microBos,
        microBosDir,
        microBreakState: microStructureBreakState,
        contextSupportDistAtr: contextSR?.support?.dist_in_atr ?? null,
        contextSupportState: contextSR?.support?.level_state ?? null,
        contextResistanceDistAtr: contextSR?.resistance?.dist_in_atr ?? null,
        contextResistanceState: contextSR?.resistance?.level_state ?? null,
        // Geometry door inputs. Trendlines carry price_now, not a distance, so
        // the ATR normalization happens here where `last` and `atr_primary` live.
        primaryChannelPos: primaryGeometry?.channel_pos ?? null,
        primarySupportTrendlineDistAtr: trendlineDistAtr(primaryGeometry?.support_trendline?.price_now),
        primaryResistanceTrendlineDistAtr: trendlineDistAtr(primaryGeometry?.resistance_trendline?.price_now),
    });

    return { signalStrength, context, assemble, actionability };
}
