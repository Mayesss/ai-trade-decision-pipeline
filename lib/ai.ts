// lib/ai.ts

import { AI_BASE_URL, AI_MODEL, TRADE_WINDOW_MINUTES } from './constants';
import type { MultiTFIndicators } from './indicators';

export type PositionContext = {
    side: 'long' | 'short';
    entry_price?: number;
    entry_ts?: string;
    hold_minutes?: number;
    unrealized_pnl_pct?: number;
    breakeven_price?: number;
    taker_fee_rate?: number;
    flow?: {
        cvd?: number;
        ob_imbalance?: number;
        pressure_delta?: number;
        alignment?: 'bullish' | 'bearish' | 'neutral';
        against_position?: boolean;
    };
};

// ------------------------------
// Prompt Builder (with guardrails, regime, momentum & extension gates)
// ------------------------------

export function buildPrompt(
    symbol: string,
    timeframe: string,
    bundle: any,
    analytics: any,
    position_status: string = 'none',
    news_sentiment: string | null = null,
    indicators: MultiTFIndicators,
    gates: any, // <--- Retain the gates object for the base gate checks
    position_context: PositionContext | null = null
) {
    const t = Array.isArray(bundle.ticker) ? bundle.ticker[0] : bundle.ticker;
    const price = Number(t?.lastPr ?? t?.last ?? t?.close ?? t?.price);
    const change = Number(t?.change24h ?? t?.changeUtc24h ?? t?.chgPct);
    const last = price; // Use price as 'last'

    const market_data = `price=${price}, change24h=${Number.isFinite(change) ? change : 'n/a'}`;

    const order_flow = `buys=${analytics.buys.toFixed(3)}, sells=${analytics.sells.toFixed(
        3,
    )}, CVD=${analytics.cvd.toFixed(3)} (obImb=${analytics.obImb.toFixed(2)})`;

    const liquidity_data = `top bid walls: ${JSON.stringify(analytics.topWalls.bid)}, top ask walls: ${JSON.stringify(
        analytics.topWalls.ask,
    )}`;

    const derivatives = `funding=${bundle.funding?.[0]?.fundingRate ?? 'n/a'}, openInterest=${
        bundle.oi?.openInterestList?.[0]?.size ?? bundle.oi?.openInterestList?.[0]?.openInterest ?? 'n/a'
    }`;

    const candles = Array.isArray(bundle.candles) ? bundle.candles : [];
    const priceTrendPoints = candles
        .slice(-5)
        .map((c: any) => {
            const tsRaw = Number(c?.[0]);
            if (!Number.isFinite(tsRaw)) return null;
            const toNum = (v: any) => {
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
        .filter((p: any) => p !== null);
    const priceTrendSeries = JSON.stringify(priceTrendPoints);

    const normalizedNewsSentiment =
        typeof news_sentiment === 'string' && news_sentiment.length > 0 ? news_sentiment : null;
    const newsSentimentBlock = normalizedNewsSentiment
        ? `- News sentiment ONLY: ${normalizedNewsSentiment.toLowerCase()}\n`
        : '';
    const positionContextBlock = position_context ? `- Position context (JSON): ${JSON.stringify(position_context)}\n` : '';
    const primaryIndicatorsBlock = indicators.primary
        ? `- Primary timeframe (${indicators.primary.timeframe}) indicators: ${indicators.primary.summary}\n`
        : '';

    const vol_profile_str = (analytics.volume_profile || [])
        .slice(0, 10)
        .map((v: any) => `(${v.price.toFixed(2)} → ${v.volume})`)
        .join(', ');

    // --- Helpers to read numeric fields from indicator strings ---
    const readNum = (name: string, src: string): number | null => {
        const m = src.match(new RegExp(`${name}=([+-]?[\\d\\.]+)`));
        return m ? Number(m[1]) : null;
    };

    // ---- Extract indicators for raw metrics (Micro = 1m, Macro = 1H) ----
    const micro = indicators.micro || '';
    const macro = indicators.macro || '';
    
    // Technical values we want the AI to judge (values, not booleans)
    const ema20_micro = readNum('EMA20', micro);
    const slope21_micro = readNum('slopeEMA21_10', micro) ?? 0; // % per bar
    const atr_micro = readNum('ATR', micro);
    const atr_macro = readNum('ATR', macro);
    const rsi_micro = readNum('RSI', micro);
    const rsi_macro = readNum('RSI', macro);
    
    // --- KEY METRICS (VALUES, NOT JUDGMENTS) ---
    const spread_bps = last > 0 ? (analytics.spread || 0) / last * 1e4 : 999;
    const atr_pct_macro = last > 0 && atr_macro ? (atr_macro / last) * 100 : 0;
    
    // Calculate extension (distance from EMA20 in 1m-ATRs)
    const distance_from_ema_atr =
        Number.isFinite(atr_micro as number) && (atr_micro as number) > 0 && Number.isFinite(ema20_micro as number)
            ? (last - (ema20_micro as number)) / (atr_micro as number)
            : 0;

    const key_metrics =
        `spread_bps=${spread_bps.toFixed(2)}, book_imbalance=${analytics.obImb.toFixed(2)}, ` +
        `atr_pct_${indicators.macroTimeFrame}=${atr_pct_macro.toFixed(2)}%, rsi_${indicators.microTimeFrame}=${rsi_micro}, ` +
        `rsi_${indicators.macroTimeFrame}=${rsi_macro}, micro_slope_pct_per_bar=${slope21_micro.toFixed(4)}, ` +
        `dist_from_ema20_${indicators.microTimeFrame}_in_atr=${distance_from_ema_atr.toFixed(2)}`;
    
    // --- SIGNAL STRENGTH DRIVERS & CLOSING GUIDANCE ---
    const clampNumber = (value: number | null | undefined, digits = 3) =>
        Number.isFinite(value as number) ? Number((value as number).toFixed(digits)) : null;
    const trendBias = gates.regime_trend_up ? 1 : gates.regime_trend_down ? -1 : 0;
    const cvdStrength = clampNumber(Math.tanh(analytics.cvd / 50));
    const oversoldMicro = typeof rsi_micro === 'number' && rsi_micro < 35;
    const overboughtMicro = typeof rsi_micro === 'number' && rsi_micro > 65;
    const reversalOpportunity = oversoldMicro && (cvdStrength ?? 0) < -0.4
        ? 'oversold_with_sell_pressure'
        : overboughtMicro && (cvdStrength ?? 0) > 0.4
        ? 'overbought_with_buy_pressure'
        : null;

    const driverComponents = [
        Math.abs(trendBias),
        Math.abs(cvdStrength ?? 0),
        Math.abs(analytics.obImb ?? 0),
        Math.abs(slope21_micro),
        Math.abs(distance_from_ema_atr),
        Math.abs(atr_pct_macro),
    ];
    const alignedDriverCount = driverComponents.filter((v) => v >= 0.35).length;

    const signalDrivers = {
        trend_bias: trendBias,
        cvd_strength: cvdStrength,
        orderbook_pressure: clampNumber(analytics.obImb, 3),
        momentum_slope_pct_per_bar: clampNumber(slope21_micro, 4),
        extension_atr: clampNumber(distance_from_ema_atr, 3),
        atr_pct_macro: clampNumber(atr_pct_macro, 3),
        rsi_micro,
        rsi_macro,
        oversold_rsi_micro: oversoldMicro,
        overbought_rsi_micro: overboughtMicro,
        aligned_driver_count: alignedDriverCount,
    };

    const priceVsBreakevenPct =
        position_context?.breakeven_price && Number.isFinite(position_context.breakeven_price) && price > 0
            ? clampNumber(((price - position_context.breakeven_price) / price) * 100, 3)
            : null;
    const positionSide = position_context?.side;
    const macroSupportsPosition =
        positionSide === 'long'
            ? gates.regime_trend_up
            : positionSide === 'short'
            ? gates.regime_trend_down
            : null;
    const macroOpposesPosition =
        positionSide === 'long'
            ? gates.regime_trend_down
            : positionSide === 'short'
            ? gates.regime_trend_up
            : null;
    const flowAgainstPosition =
        positionSide === 'long'
            ? analytics.obImb < -0.15 || (cvdStrength ?? 0) < -0.35
            : positionSide === 'short'
            ? analytics.obImb > 0.15 || (cvdStrength ?? 0) > 0.35
            : null;
    const closingAlert = Boolean(
        flowAgainstPosition &&
            (Math.abs(cvdStrength ?? 0) > 0.4 || Math.abs(analytics.obImb) > 0.2 || (priceVsBreakevenPct ?? 0) < -0.15),
    );

    const flowContradictionScore =
        positionSide === 'long'
            ? Math.max(0, (-(analytics.obImb ?? 0)) + (-(cvdStrength ?? 0)))
            : positionSide === 'short'
            ? Math.max(0, (analytics.obImb ?? 0) + (cvdStrength ?? 0))
            : 0;

    const closingGuidance = {
        macro_bias: trendBias,
        flow_pressure: clampNumber(analytics.obImb, 3),
        cvd_strength: cvdStrength,
        price_vs_breakeven_pct: priceVsBreakevenPct,
        hold_minutes: clampNumber(position_context?.hold_minutes ?? null, 1),
        macro_supports_position: macroSupportsPosition,
        macro_opposes_position: macroOpposesPosition,
        flow_against_position: flowAgainstPosition,
        closing_alert: closingAlert,
        reversal_opportunity: reversalOpportunity,
        flow_contradiction_score: clampNumber(flowContradictionScore, 3),
    };
    // Costs (educate the model)
    const taker_round_trip_bps = 5; // 5 bps
    const slippage_bps = 2;

    const risk_policy =
        `fees=${taker_round_trip_bps}bps round-trip, slippage=${slippage_bps}bps, ` +
        `stop=1.5xATR(${indicators.macroTimeFrame}), take_profit=2.5xATR(${indicators.macroTimeFrame}), time_stop=${parseInt(
            timeframe,
            10,
        ) * 3} minutes`;

    // We only pass the BASE gates now, as the AI will judge the strategy gates using metrics
    const base_gating_flags =
        `spread_ok=${gates.spread_ok}, liquidity_ok=${gates.liquidity_ok}, atr_ok=${gates.atr_ok}, slippage_ok=${gates.slippage_ok}`;
        
    // We still pass the Regime for a strong trend bias signal
    const regime_flags =
        `regime_trend_up=${gates.regime_trend_up}, regime_trend_down=${gates.regime_trend_down}`;


    const sys = `
You are an expert crypto market microstructure analyst and quantitative trading assistant.
Your goal is to find high-probability, short-term trades. You must be risk-averse.
Respond in strict JSON ONLY.

GUIDELINES & HEURISTICS:
- **Base Gates**: Trade ONLY if ALL base gates are TRUE: spread_ok, liquidity_ok, atr_ok, slippage_ok. If any is FALSE, HOLD.
- **Costs**: Always weigh expected edge vs fees + slippage; if edge ≤ costs, HOLD.
- **Signal Strength**: If signal_strength is LOW => HOLD. MEDIUM requires aligned_driver_count ≥ 3 in the Signal strength drivers; otherwise HOLD. aligned_driver_count ≥ 4 typically implies HIGH.
- **Extension/Fading**: If 'dist_from_ema20_${indicators.microTimeFrame}_in_atr' is > 1.5 (over-extended) or < -1.5, consider fading the move or prioritizing "HOLD" unless other signals are overwhelming. Never ignore strong tape/flow cues solely because price looks extended.
- **Signal Drivers**: Use the "Signal strength drivers" JSON to distinguish MEDIUM vs HIGH confidence. Multiple aligned drivers + macro agreement → HIGH; mixed drivers → MEDIUM.
- **Reversal Discipline**: Only reverse (flip long ↔ short) if flow/pressure clearly contradicts the current position with strong drivers.
- **Reverse Action**: Use the "REVERSE" action when you want to flatten the current position and immediately open the opposite side; treat it as a close + restart. Strong flow_contradiction_score or reversal_opportunity should bias toward REVERSE over HOLD.
- **Closing Discipline**: Check "Closing guardrails". If macro_supports_position is true and closing_alert is false, prefer HOLD. Close only when closing_alert is true or macro_opposes_position.
- **Reversal Opportunities**: When "Closing guardrails".reversal_opportunity is set (e.g., oversold_with_sell_pressure) or flow_contradiction_score is high (>0.5), reassess MEDIUM signals aggressively—this often upgrades to HIGH for CLOSE/REVERSE actions.
`.trim();

    const user = `
You are analyzing ${symbol} on a ${timeframe} horizon (simulation).

RISK/COSTS:
- ${risk_policy}

BASE GATES (Filter for tradeability):
- ${base_gating_flags}

STRATEGY BIAS (Macro Trend):
- ${regime_flags}

KEY METRICS (Values for Judgment):
- ${key_metrics}

DATA INPUTS (with explicit windows):
- Current price and % change (now): ${market_data}
- Volume profile (fixed lookback window = ${TRADE_WINDOW_MINUTES}m): ${vol_profile_str}
- Order flow summary (1m tape/CVD, last 5–15m): ${order_flow}
- Order book & liquidity (snapshot): ${liquidity_data}
- Funding rate & open interest (last 30–60m): ${derivatives}
- Recent price trend (last ${priceTrendPoints.length} bars): ${priceTrendSeries}
${newsSentimentBlock}- Current position: ${position_status}
${positionContextBlock}- Technical (short-term, ${indicators.microTimeFrame}, last 30 candles): ${indicators.micro}
- Macro (${indicators.macroTimeFrame}, last 30 candles): ${indicators.macro}
${primaryIndicatorsBlock}
- Signal strength drivers: ${JSON.stringify(signalDrivers)}
- Closing guardrails: ${JSON.stringify(closingGuidance)}

TASKS:
1) Evaluate short-term bias (UP/DOWN/NEUTRAL) from all data.
2) Output one action only: "BUY", "SELL", "HOLD", "CLOSE", or "REVERSE".
   - If no position is open, return BUY/SELL/HOLD.
   - If a position is open, you may HOLD, CLOSE, or REVERSE (REVERSE = close + open opposite side).
3) Assess signal strength: LOW, MEDIUM, or HIGH.
4) Summarize in ≤2 lines.

JSON OUTPUT (strict):
{"action":"BUY|SELL|HOLD|CLOSE|REVERSE","bias":"UP|DOWN|NEUTRAL","signal_strength":"LOW|MEDIUM|HIGH","summary":"≤2 lines","reason":"brief rationale (flow/liquidity/technicals/sentiment/metrics)"}
`.trim();

    return { system: sys, user };
}

// ------------------------------
// OpenAI API Call
// ------------------------------

export async function callAI(system: string, user: string) {
    const apiKey = process.env.OPENAI_API_KEY;
    if (!apiKey) throw new Error('Missing OPENAI_API_KEY');

    const base = AI_BASE_URL;
    const model = AI_MODEL;

    const res = await fetch(`${base}/chat/completions`, {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
            Authorization: `Bearer ${apiKey}`,
        },
        body: JSON.stringify({
            model,
            messages: [
                { role: 'system', content: system },
                { role: 'user', content: user },
            ],
            temperature: 0.2,
            response_format: { type: 'json_object' },
        }),
    });

    if (!res.ok) throw new Error(`AI error: ${res.status} ${res.statusText}`);

    const data = await res.json();
    const text = data.choices?.[0]?.message?.content || '{}';
    return JSON.parse(text);
}
