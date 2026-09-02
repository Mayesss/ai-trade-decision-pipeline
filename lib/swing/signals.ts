// lib/swing/signals.ts
//
// Pure DERIVATION over the indicator/market inputs: momentum signals, the
// code-owned actionability gate, the re-entry cooldown and the signal-strength
// score. No I/O, no prompt text, no order side effects — every function here is
// a measurement the prompt (prompt.ts) or the post-parse rules
// (decisionRules.ts) reason with.

import type { MultiTFIndicators } from '../indicators';
import {
    ACTIONABILITY_NEAR_ATR,
    ACTIONABILITY_ROOM_ATR,
    ACTIONABILITY_WALL_ATR,
    ACTIONABILITY_CHANNEL_EDGE,
    REENTRY_COOLDOWN_MIN,
} from './decisionConfig';
import type { MomentumSignals, SwingGatesInput, LastClosedPosition, ActionabilityInputs, PromptDecisionContext } from './decisionConfig';

const indicatorRegexCache = new Map<string, RegExp>();

export function readIndicator(name: string, src: string): number | null {
    if (!src) return null;
    let regex = indicatorRegexCache.get(name);
    if (!regex) {
        regex = new RegExp(`${name}=([+-]?[0-9]*\.?[0-9]+)`);
        indicatorRegexCache.set(name, regex);
    }
    const match = src.match(regex);
    if (!match) return null;
    const val = Number(match[1]);
    return Number.isFinite(val) ? val : null;
}

function distanceOk(price: number, target: number | null, atr: number | null) {
    if (!Number.isFinite(price) || !Number.isFinite(target as number)) return false;
    const threshold = Number.isFinite(atr as number) && (atr as number) > 0 ? (atr as number) * 0.8 : price * 0.0015;
    return Math.abs(price - (target as number)) <= threshold;
}

function microDistanceOk(price: number, target: number | null, atr: number | null) {
    if (!Number.isFinite(price) || !Number.isFinite(target as number)) return false;
    const threshold = Number.isFinite(atr as number) && (atr as number) > 0 ? (atr as number) * 1.2 : price * 0.0008;
    return Math.abs(price - (target as number)) <= threshold;
}

export function computeMomentumSignals(params: {
    price: number;
    indicators: MultiTFIndicators;
    gates: SwingGatesInput;
    primaryTimeframe: string;
}): MomentumSignals {
    const { price, indicators, gates, primaryTimeframe } = params;
    const microSummary = indicators.micro || '';
    const primarySummary = indicators.primary?.summary || '';
    const primaryTf = String(primaryTimeframe || '').trim();
    const microTf = String(indicators.microTimeFrame || '').trim();
    const primaryMetrics = primaryTf ? indicators.metrics?.[primaryTf] : undefined;
    const microMetrics = microTf ? indicators.metrics?.[microTf] : undefined;

    const ema50Primary = readIndicator('EMA50', primarySummary);
    const ema20Primary = readIndicator('EMA20', primarySummary);
    const ema20Micro = readIndicator('EMA20', microSummary);
    const atrPrimaryMetric = Number(primaryMetrics?.atr);
    const atrMicroMetric = Number(microMetrics?.atr);
    const atrPrimary = Number.isFinite(atrPrimaryMetric) && atrPrimaryMetric > 0 ? atrPrimaryMetric : readIndicator('ATR', primarySummary);
    const atrMicro = Number.isFinite(atrMicroMetric) && atrMicroMetric > 0 ? atrMicroMetric : readIndicator('ATR', microSummary);
    const rsiPrimary = readIndicator('RSI', primarySummary);
    const rsiMicro = readIndicator('RSI', microSummary);
    const slopePrimary = readIndicator('slopeEMA21_10', primarySummary);

    const macroTrendUp = Boolean(gates.regime_trend_up);
    const macroTrendDown = Boolean(gates.regime_trend_down);

    const priceAbovePrimary50 = Number.isFinite(ema50Primary as number) ? price >= (ema50Primary as number) : true;
    const priceBelowPrimary50 = Number.isFinite(ema50Primary as number) ? price <= (ema50Primary as number) : true;

    const rsiPullbackLong = Number.isFinite(rsiPrimary as number)
        ? (rsiPrimary as number) >= 35 && (rsiPrimary as number) <= 50
        : false;
    const rsiPullbackShort = Number.isFinite(rsiPrimary as number)
        ? (rsiPrimary as number) >= 50 && (rsiPrimary as number) <= 65
        : false;

    const slopeUp = Number.isFinite(slopePrimary as number) ? (slopePrimary as number) > 0 : false;
    const slopeDown = Number.isFinite(slopePrimary as number) ? (slopePrimary as number) < 0 : false;

    const nearPrimary = distanceOk(price, ema20Primary, atrPrimary);
    const nearMicro = microDistanceOk(price, ema20Micro, atrMicro);
    const microOversold = Number.isFinite(rsiMicro as number) ? (rsiMicro as number) <= 40 : false;
    const microOverbought = Number.isFinite(rsiMicro as number) ? (rsiMicro as number) >= 60 : false;
    const microEntryOk = nearPrimary || nearMicro || microOversold || microOverbought;

    const longMomentum = priceAbovePrimary50 && rsiPullbackLong && slopeUp;
    const shortMomentum = priceBelowPrimary50 && rsiPullbackShort && slopeDown;

    const microExtensionInAtr =
        Number.isFinite(atrMicro as number) && (atrMicro as number) > 0 && Number.isFinite(ema20Micro as number)
            ? (price - (ema20Micro as number)) / (atrMicro as number)
            : null;

    return {
        macroTrendUp,
        macroTrendDown,
        longMomentum,
        shortMomentum,
        nearPrimaryEMA20: nearPrimary,
        nearMicroEMA20: nearMicro,
        primaryRSI: rsiPrimary,
        primarySlope: slopePrimary,
        microRSI: rsiMicro,
        primaryAtr: atrPrimary,
        microExtensionInAtr,
        info: {
            primaryTimeframe,
            microEntryOk,
        },
    };
}

// The cooldown that applies to a flat tick right now, or null when inactive.
export function resolveReentryCooldown(
    lastClosed: LastClosedPosition | null | undefined,
    nowMs = Date.now(),
): { blockedSide: 'long' | 'short'; minutesLeft: number } | null {
    if (!lastClosed || REENTRY_COOLDOWN_MIN <= 0) return null;
    const elapsedMin = (nowMs - lastClosed.exitTsMs) / 60_000;
    if (!(elapsedMin >= 0) || elapsedMin >= REENTRY_COOLDOWN_MIN) return null;
    return { blockedSide: lastClosed.side, minutesLeft: Math.ceil(REENTRY_COOLDOWN_MIN - elapsedMin) };
}

export function evaluateActionability(x: ActionabilityInputs): { actionable: boolean; reason: string } {
    // micro_entry_ok is NOT a gate any more (removed 2026-08-30). It asks "is
    // price at a sane place to take the market right now" — the only question
    // worth asking when a market fill was the only way in. With resting entries
    // available, a good setup at bad timing has an answer (rest a limit back at
    // the mean) that this gate used to delete.
    //
    // The measured case for it was thin anyway: 328 skips in 90 days, 0.39% of
    // all ticks and 6.4% of this gate's skips, with micro_entry_ok true on ~94%
    // of ticks that reached it — the union
    // (nearPrimaryEMA20 || nearMicroEMA20 || rsiMicro<=40 || rsiMicro>=60)
    // excludes little. Its stated support, "all opens had it", is evidence it
    // never blocked a good trade, not evidence it blocked a bad one. Extension
    // (state.extension_atr) measures the same thing more directly and keeps its
    // own gate.
    //
    // It survives as a MEASUREMENT: momentum.micro_entry_ok still reaches the
    // prompt for the model to weigh. boxed_or_unconfirmed below is what actually
    // carries this gate (78% of its skips).
    //
    // (a) confirmed primary structure — the universal opener (17/18 opens, all asset classes).
    const confirmed =
        x.primaryBreakoutConfirmed ||
        x.primaryBreakdownConfirmed ||
        x.primaryBreakoutRetestOk ||
        x.primaryBos ||
        (!!x.primaryBreakState && x.primaryBreakState !== 'inside');
    if (confirmed) {
        // A confirmed direction pressing straight into a NEAR, UNBROKEN MAJOR
        // (context/weekly) opposing wall used to be a hard skip: the AI reliably
        // HOLDed "confirmed breakdown but sitting on major weekly support", so
        // dropping those calls cost nothing (measured: 104 HOLD-calls dropped,
        // 0 opens lost).
        //
        // That held only while a market entry was the sole way in. A wall is
        // ALSO the highest-quality bounce location on the chart, and with
        // resting entries available the model can now trade one — fade off it
        // with a limit, or arm a stop beyond it — instead of being limited to
        // "take the market here or nothing". The old measurement said the AI
        // wouldn't ACT on these; it could not have, since it had no tool that
        // fit. So the wall stops being a gate and becomes what it always
        // should have been: a measurement the model sees (it already reaches
        // the prompt as location.context_*_dist_atr) and weighs itself.
        //
        // The branch is still NAMED so the skip/decision trail stays queryable
        // and the ai-bouncer — which runs after this and can still decline the
        // expensive call — gets the context.
        const confDown =
            x.primaryBreakdownConfirmed ||
            (x.primaryBreakoutRetestOk && x.primaryBreakoutRetestDir === 'down') ||
            (x.primaryBos && x.primaryBosDir === 'down') ||
            x.primaryBreakState === 'below';
        const confUp =
            x.primaryBreakoutConfirmed ||
            (x.primaryBreakoutRetestOk && x.primaryBreakoutRetestDir === 'up') ||
            (x.primaryBos && x.primaryBosDir === 'up') ||
            x.primaryBreakState === 'above';
        const dir = confDown && !confUp ? 'down' : confUp && !confDown ? 'up' : null;
        const blocking = (s?: string | null) => !!s && s !== 'broken' && s !== 'retesting';
        const csd = Number.isFinite(x.contextSupportDistAtr as number) ? (x.contextSupportDistAtr as number) : null;
        const crd = Number.isFinite(x.contextResistanceDistAtr as number) ? (x.contextResistanceDistAtr as number) : null;
        const intoWall =
            (dir === 'down' && csd != null && csd <= ACTIONABILITY_WALL_ATR && blocking(x.contextSupportState)) ||
            (dir === 'up' && crd != null && crd <= ACTIONABILITY_WALL_ATR && blocking(x.contextResistanceState));
        if (intoWall) return { actionable: true, reason: 'confirmed_primary_structure_into_context_wall' };
        return { actionable: true, reason: 'confirmed_primary_structure' };
    }
    // (b) tight bounce — at one level, opposite level far (room to run), micro turning that way.
    const sup = Number.isFinite(x.primarySupportDistAtr as number) ? (x.primarySupportDistAtr as number) : null;
    const res = Number.isFinite(x.primaryResistanceDistAtr as number) ? (x.primaryResistanceDistAtr as number) : null;
    const microUp =
        (x.microBreakoutRetestOk && x.microBreakoutRetestDir === 'up') ||
        (x.microBos && x.microBosDir === 'up') ||
        x.microBreakState === 'above';
    const microDown =
        (x.microBreakoutRetestOk && x.microBreakoutRetestDir === 'down') ||
        (x.microBos && x.microBosDir === 'down') ||
        x.microBreakState === 'below';
    const longBounce =
        sup != null && res != null && sup <= ACTIONABILITY_NEAR_ATR && res >= ACTIONABILITY_ROOM_ATR && microUp;
    const shortBounce =
        sup != null && res != null && res <= ACTIONABILITY_NEAR_ATR && sup >= ACTIONABILITY_ROOM_ATR && microDown;
    // Same reasoning as the confirmed branch: a bounce whose room runs into a
    // near, unbroken MAJOR (context/weekly) wall is reported, not rejected. A
    // short-room bounce is a legitimate trade when the entry is placed AT the
    // level rather than taken at market — which is now expressible.
    const blockingBounce = (s?: string | null) => !!s && s !== 'broken' && s !== 'retesting';
    const bounceCsd = Number.isFinite(x.contextSupportDistAtr as number) ? (x.contextSupportDistAtr as number) : null;
    const bounceCrd = Number.isFinite(x.contextResistanceDistAtr as number) ? (x.contextResistanceDistAtr as number) : null;
    const longBounceIntoWall =
        longBounce && bounceCrd != null && bounceCrd <= ACTIONABILITY_WALL_ATR && blockingBounce(x.contextResistanceState);
    const shortBounceIntoWall =
        shortBounce && bounceCsd != null && bounceCsd <= ACTIONABILITY_WALL_ATR && blockingBounce(x.contextSupportState);
    if (longBounceIntoWall) return { actionable: true, reason: 'bounce_long_into_context_wall' };
    if (shortBounceIntoWall) return { actionable: true, reason: 'bounce_short_into_context_wall' };
    if (longBounce) return { actionable: true, reason: 'bounce_long' };
    if (shortBounce) return { actionable: true, reason: 'bounce_short' };

    // (c) ANCHOR door — at a primary level, either side, with no room or micro
    // requirement. Doors (a) and (b) both key on the same thing: a move that has
    // already happened. Every strategy that starts from LOCATION rather than
    // momentum fails them, so relaxing their thresholds only ever admits weaker
    // breakouts and weaker bounces. This door keys on a different measurement —
    // is there a defined risk anchor near price — which is what a range fade, a
    // reversal at a level and a bounce with no room all actually need, and which
    // is strategy-neutral by construction.
    //
    // Concretely this is what admits the SYMMETRIC range: (b) demands
    // ACTIONABILITY_ROOM_ATR on the far side, so a tight box never reached the
    // model even after the entry TP floor was dropped to make range trading
    // expressible (docs/measured-hold-causes.md §4).
    const atSupport = sup != null && sup <= ACTIONABILITY_NEAR_ATR;
    const atResistance = res != null && res <= ACTIONABILITY_NEAR_ATR;
    if (atSupport || atResistance) {
        const both = atSupport && atResistance;
        return {
            actionable: true,
            reason: both ? 'at_primary_level_boxed' : atSupport ? 'at_primary_support' : 'at_primary_resistance',
        };
    }

    // (d) GEOMETRY door — the wave's own structure, for setups with no swing S/R
    // nearby at all. primaryBreakState is "last close beyond the last swing
    // extreme", so it stays true while price holds out there: a SHALLOW pullback
    // still passes (a), but a DEEP one — price back at the channel floor or the
    // trendline, the better-priced entry — flips to 'inside' and then needs a
    // level within NEAR_ATR to survive. Without this door the gate systematically
    // admits price that has LEFT a level and drops price that has come BACK to
    // one, which is a bias toward chasing, not toward breakouts as such.
    const channelPos = Number.isFinite(x.primaryChannelPos as number) ? (x.primaryChannelPos as number) : null;
    const atChannelLow = channelPos != null && channelPos <= ACTIONABILITY_CHANNEL_EDGE;
    const atChannelHigh = channelPos != null && channelPos >= 1 - ACTIONABILITY_CHANNEL_EDGE;
    if (atChannelLow) return { actionable: true, reason: 'channel_low' };
    if (atChannelHigh) return { actionable: true, reason: 'channel_high' };

    const supTl = Number.isFinite(x.primarySupportTrendlineDistAtr as number)
        ? (x.primarySupportTrendlineDistAtr as number)
        : null;
    const resTl = Number.isFinite(x.primaryResistanceTrendlineDistAtr as number)
        ? (x.primaryResistanceTrendlineDistAtr as number)
        : null;
    if (supTl != null && supTl <= ACTIONABILITY_NEAR_ATR) return { actionable: true, reason: 'at_support_trendline' };
    if (resTl != null && resTl <= ACTIONABILITY_NEAR_ATR) return { actionable: true, reason: 'at_resistance_trendline' };

    // Still water: no break, no level within NEAR_ATR on either side, not at a
    // channel edge, no trendline in reach. Nothing to anchor invalidation to, so
    // there is no trade to price — this is the only case code decides alone.
    // Everything admitted above still faces the ai-bouncer, which prices the call
    // and may decline it; a cost filter may skip work, never override a gate.
    return { actionable: false, reason: 'boxed_or_unconfirmed' };
}

export function computeSignalStrength(context: PromptDecisionContext): 'LOW' | 'MEDIUM' | 'HIGH' {
    const aligned = Number.isFinite(context.aligned_driver_count) ? context.aligned_driver_count : 0;
    const regime = Number.isFinite(context.regime_alignment) ? Math.abs(context.regime_alignment) : 0;
    const location = Number.isFinite(context.location_confluence_score) ? context.location_confluence_score : 0;
    const microExt = Number.isFinite(context.micro_extension_atr as number)
        ? Math.abs(context.micro_extension_atr as number)
        : 0;
    const primaryExt = Number.isFinite(context.primary_extension_atr as number)
        ? Math.abs(context.primary_extension_atr as number)
        : 0;

    let score = 0;
    if (aligned >= 5) score += 3;
    else if (aligned >= 4) score += 2;
    else if (aligned >= 3) score += 1;

    if (regime >= 0.5) score += 1;
    if (location >= 0.6) score += 1;

    if (microExt >= 2.5 || primaryExt >= 2.5) score -= 1;
    if (microExt >= 3 || primaryExt >= 3) score -= 1;

    if (score >= 4) return 'HIGH';
    if (score >= 2) return 'MEDIUM';
    return 'LOW';
}
