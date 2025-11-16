// lib/ai.ts

import { AI_BASE_URL, AI_MODEL, TRADE_WINDOW_MINUTES } from './constants';

export type PositionContext = {
    side: 'long' | 'short';
    entry_price?: number;
    entry_ts?: string;
    hold_minutes?: number;
    unrealized_pnl_pct?: number;
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
    indicators: { micro: string; macro: string },
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
    const ema20_1m = readNum('EMA20', micro);
    const slope21_1m = readNum('slopeEMA21_10', micro) ?? 0; // % per bar
    const atr_1m = readNum('ATR', micro);
    const atr_1h = readNum('ATR', macro);
    const rsi_1m = readNum('RSI', micro);
    const rsi_1h = readNum('RSI', macro);
    
    // --- KEY METRICS (VALUES, NOT JUDGMENTS) ---
    const spread_bps = last > 0 ? (analytics.spread || 0) / last * 1e4 : 999;
    const atr_pct_1h = last > 0 && atr_1h ? (atr_1h / last) * 100 : 0;
    
    // Calculate extension (distance from EMA20 in 1m-ATRs)
    const distance_from_ema_atr =
        Number.isFinite(atr_1m as number) && (atr_1m as number) > 0 && Number.isFinite(ema20_1m as number)
            ? (last - (ema20_1m as number)) / (atr_1m as number)
            : 0;

    const key_metrics =
        `spread_bps=${spread_bps.toFixed(2)}, book_imbalance=${analytics.obImb.toFixed(2)}, ` +
        `atr_pct_1h=${atr_pct_1h.toFixed(2)}%, rsi_1m=${rsi_1m}, rsi_1h=${rsi_1h}, ` +
        `micro_slope_pct_per_bar=${slope21_1m.toFixed(4)}, ` +
        `dist_from_ema20_1m_in_atr=${distance_from_ema_atr.toFixed(2)}`;
    
    // Costs (educate the model)
    const taker_round_trip_bps = 5; // 5 bps
    const slippage_bps = 2;

    const risk_policy =
        `fees=${taker_round_trip_bps}bps round-trip, slippage=${slippage_bps}bps, ` +
        `stop=1.5xATR(1H), take_profit=2.5xATR(1H), time_stop=${parseInt(timeframe, 10) * 3} minutes`;

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
- **Costs**: Your primary goal is to overcome costs (fees + slippage). If expected edge <= costs, HOLD.
- **Signal Strength**: If signal_strength is LOW or MEDIUM => "HOLD" (unless closing an open position).
- **Extension/Fading**: If 'dist_from_ema20_1m_in_atr' is > 1.5 (over-extended) or < -1.5, consider fading the move or prioritizing "HOLD" unless other signals are overwhelming.
- **Prediction Horizon**: Do not predict beyond 1 hour.
`.trim();

    const user = `
You are analyzing ${symbol} on a ${parseInt(timeframe, 10)}-minute horizon (simulation).

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
${positionContextBlock}- Technical (short-term, 1m, last 30 candles): ${indicators.micro}
- Macro (1h, last 30 candles): ${indicators.macro}

TASKS:
1) Evaluate short-term bias (UP/DOWN/NEUTRAL) from all data.
2) Output one action only: "BUY", "SELL", "HOLD", or "CLOSE".
   - If no position is open, return BUY/SELL/HOLD.
   - If a position is open, return HOLD or CLOSE only.
3) Assess signal strength: LOW, MEDIUM, or HIGH.
4) Summarize in ≤2 lines.

JSON OUTPUT (strict):
{"action":"BUY|SELL|HOLD|CLOSE","bias":"UP|DOWN|NEUTRAL","signal_strength":"LOW|MEDIUM|HIGH","summary":"≤2 lines","reason":"brief rationale (flow/liquidity/technicals/sentiment/metrics)"}
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
