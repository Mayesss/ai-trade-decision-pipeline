// lib/ai.ts

import { AI_BASE_URL, AI_MODEL, TRADE_WINDOW_MINUTES } from './constants';

// ------------------------------
// Prompt Builder (with guardrails, regime, momentum & extension gates)
// ------------------------------

export function buildPrompt(
    symbol: string,
    timeframe: string,
    bundle: any,
    analytics: any,
    position_status: string = 'none',
    news_sentiment: string = 'neutral',
    indicators: { micro: string; macro: string },
    gates: any,
) {
    const t = Array.isArray(bundle.ticker) ? bundle.ticker[0] : bundle.ticker;
    const price = Number(t?.lastPr ?? t?.last ?? t?.close ?? t?.price);
    const change = Number(t?.change24h ?? t?.changeUtc24h ?? t?.chgPct);

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

    const vol_profile_str = (analytics.volume_profile || [])
        .slice(0, 10)
        .map((v: any) => `(${v.price.toFixed(2)} → ${v.volume})`)
        .join(', ');

    // --- Helpers to read numeric fields from indicator strings ---
    const readNum = (name: string, src: string): number | null => {
        const m = src.match(new RegExp(`${name}=([+-]?[\\d\\.]+)`));
        return m ? Number(m[1]) : null;
    };

    // ---- Extract indicators (micro = 1m, macro = 1H) ----
    const micro = indicators.micro || '';
    const macro = indicators.macro || '';

    const ema9_1m = readNum('EMA9', micro);
    const ema21_1m = readNum('EMA21', micro);
    const ema20_1m = readNum('EMA20', micro);
    const ema20_1h = readNum('EMA20', macro);
    const ema50_1h = readNum('EMA50', macro);
    const sma200_1h = readNum('SMA200', macro);
    const slope21_1m = readNum('slopeEMA21_10', micro) ?? 0; // % per bar
    const atr_1m = readNum('ATR', micro);
    const atr_1h = readNum('ATR', macro);
    const rsi_1m = readNum('RSI', micro);
    const rsi_1h = readNum('RSI', macro); 

    // --- KEY METRICS (VALUES, NOT JUDGMENTS) ---
    const last = analytics.last || price || 0;
    const spread_bps = last > 0 ? ((analytics.spread || 0) / last) * 1e4 : 999;
    const atr_pct_1h = last > 0 && atr_1h ? (atr_1h / last) * 100 : 0;

    // Calculate extension (distance from EMA20 in 1m-ATRs)
    const distance_from_ema_atr =
        Number.isFinite(atr_1m as number) && (atr_1m as number) > 0 && Number.isFinite(ema20_1m as number)
            ? (last - (ema20_1m as number)) / (atr_1m as number)
            : 0;

    // This is the data for the AI
    const key_metrics =
        `spread_bps=${spread_bps.toFixed(2)}, book_imbalance=${analytics.obImb.toFixed(2)}, ` +
        `atr_pct_1h=${atr_pct_1h.toFixed(2)}%, rsi_1m=${rsi_1m}, rsi_1h=${rsi_1h}, ` +
        `micro_slope_pct_per_bar=${slope21_1m.toFixed(4)}, ` +
        `dist_from_ema20_1m_in_atr=${distance_from_ema_atr.toFixed(2)}`;

    // Gating flags (we still pass the booleans for the AI to reference)
    const gating_flags =
        `regime_trend_up=${gates.regime_trend_up}, regime_trend_down=${gates.regime_trend_down}, `
        + `momentum_long=${gates.momentum_long}, momentum_short=${gates.momentum_short}`;
    // Costs (educate the model)
    const taker_round_trip_bps = 5; // 5 bps
    const slippage_bps = 2;

    const risk_policy =
        `fees=${taker_round_trip_bps}bps round-trip, slippage=${slippage_bps}bps, ` +
        `stop=1.5xATR(1H), take_profit=2.5xATR(1H), time_stop=${parseInt(timeframe, 10) * 3} minutes`;


    const sys = `
You are an expert crypto market microstructure analyst and quantitative trading assistant.
Your goal is to find high-probability, short-term trades. You must be risk-averse.
Respond in strict JSON ONLY.

GUIDELINES & HEURISTICS:
- **Costs**: Your primary goal is to overcome costs (fees + slippage). If the signal is weak, HOLD.
- **Signal Strength**: LOW/MEDIUM signal strength should always result in "HOLD" (unless a position is open).
- **Base Conditions**: You should heavily prefer to trade ONLY if conditions are good.
  - Good: 'spread_bps' < 4, 'atr_pct_1h' between 0.1 and 1.5, 'book_imbalance' favors the trade direction.
  - Bad: 'dist_from_ema20_1m_in_atr' > 1.5 (over-extended). If so, prefer "HOLD".
- **BUY Signal**: Requires 'regime_trend_up'=true AND 'momentum_long'=true.
  - Confirmation: Look for positive 'book_imbalance' (e.g., > 0.1), strong tape (CVD > 0), and 'rsi_1m' not overbought (e.g., < 75).
- **SELL Signal**: Requires 'regime_trend_down'=true AND 'momentum_short'=true.
  - Confirmation: Look for negative 'book_imbalance' (e.g., < -0.1), strong tape (CVD < 0), and 'rsi_1m' not oversold (e.g., > 25).
- **Prediction Horizon**: Do not predict beyond 1 hour.
`.trim();

    // --- NEW USER PROMPT ---
    const user = `
You are analyzing ${symbol} on a ${parseInt(timeframe, 10)}-minute horizon (simulation).

RISK/COSTS:
- ${risk_policy}

KEY METRICS (Values):
- ${key_metrics}

GATING (Judgments):
- ${gating_flags}

DATA INPUTS (with explicit windows):
- Current price and % change (now): ${market_data}
- Volume profile (fixed lookback window = ${TRADE_WINDOW_MINUTES}m): ${vol_profile_str}
- Order flow summary (1m tape/CVD, last 5–15m): ${order_flow}
- Order book & liquidity (snapshot): ${liquidity_data}
- Funding rate & open interest (last 30–60m): ${derivatives}
- News sentiment ONLY: ${news_sentiment.toLowerCase()}
- Current position: ${position_status}
- Technical (short-term, 1m, last 30 candles): ${indicators.micro}
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
