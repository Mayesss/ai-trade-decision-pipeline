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

    // --- Gating flags (existing) ---
    const last = analytics.last || price || 0;
    const spread = analytics.spread || 0;
    const spread_ok = last > 0 ? spread / last < 0.0004 : false; // < 4 bps

    const sumTop = (lvls: any[], n: number) =>
        (lvls || []).slice(0, n).reduce((a, l) => a + Number(l[1] ?? l.size ?? 0), 0);
    const topBid = sumTop(bundle.orderbook?.bids || [], 5);
    const topAsk = sumTop(bundle.orderbook?.asks || [], 5);
    const liquidity_ok = topBid + topAsk > 0 ? topBid > topAsk * 0.6 : false;

    // --- Regime gate (macro trend + long-term bias) ---
    const regime_trend_up = (ema20_1h ?? -Infinity) > (ema50_1h ?? Infinity) && price > (sma200_1h ?? -Infinity);
    const regime_trend_down = (ema20_1h ?? Infinity) < (ema50_1h ?? -Infinity) && price < (sma200_1h ?? Infinity);

    // --- ATR gate (vol window sanity) ---
    const atrAbs = Number.isFinite(atr_1h as number) ? (atr_1h as number) : NaN;
    const atr_ok = Number.isFinite(atrAbs) && last > 0 ? atrAbs / last > 0.001 && atrAbs / last < 0.01 : true; // 10–100 bps

    // --- Momentum & extension gates (1m) ---
    const slopeThresh = 0.01; // 0.01% per 1m bar (tune 0.005–0.02)
    const momentum_long = (ema9_1m ?? 0) > (ema21_1m ?? 0) && slope21_1m > slopeThresh;
    const momentum_short = (ema9_1m ?? 0) < (ema21_1m ?? 0) && slope21_1m < -slopeThresh;

    // Extension guard vs EMA20(1m)
    const extension_ok =
        Number.isFinite(atr_1m as number) && Number.isFinite(ema20_1m as number) && last > 0
            ? Math.abs(last - (ema20_1m as number)) / (atr_1m as number) <= 1.5
            : true;

    // Costs (educate the model)
    const taker_round_trip_bps = 5; // 5 bps
    const slippage_bps = 2;

    const risk_policy =
        `fees=${taker_round_trip_bps}bps round-trip, slippage=${slippage_bps}bps, ` +
        `stop=1.5xATR(1H), take_profit=2.5xATR(1H), time_stop=${parseInt(timeframe, 10) * 3} minutes`;

    const gating_flags =
        `regime_trend_up=${regime_trend_up}, regime_trend_down=${regime_trend_down}, ` +
        `spread_ok=${spread_ok}, liquidity_ok=${liquidity_ok}, atr_ok=${atr_ok}, ` +
        `momentum_long=${momentum_long}, momentum_short=${momentum_short}, extension_ok=${extension_ok}`;

    const sys = `
You are an expert crypto market microstructure analyst and quantitative trading assistant.
Respond in strict JSON ONLY.

HARD RULES:
- If signal_strength is LOW or MEDIUM => "HOLD" (unless a position is open, then "CLOSE" is allowed).
- Trade ONLY if ALL base gates are true: spread_ok, liquidity_ok, atr_ok, extension_ok.
- For BUY, ALSO require: regime_trend_up=true AND momentum_long=true.
- For SELL, ALSO require: regime_trend_down=true AND momentum_short=true.
- Consider costs (taker + slippage) as provided; if expected edge <= costs => HOLD.
- Do not predict beyond 1 hour.
`.trim();

    const user = `
You are analyzing ${symbol} on a ${parseInt(timeframe, 10)}-minute horizon (simulation).

RISK/COSTS:
- ${risk_policy}

GATING:
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
1) Evaluate short-term bias (UP/DOWN/NEUTRAL) from order flow, liquidity, derivatives, and indicators.
2) Output one action only: "BUY", "SELL", "HOLD", or "CLOSE".
   - If no position is open, return BUY/SELL/HOLD.
   - If a position is open, return HOLD or CLOSE only.
   - If currentPnL > 1% but signal weakens/CVD flips, consider "CLOSE".
   - If currentPnL < -1% with building pressure against, consider "CLOSE".
3) Assess signal strength: LOW, MEDIUM, or HIGH (volume clarity & book imbalance).
4) Summarize in ≤2 lines (choppy/trending/trapping etc.)

JSON OUTPUT (strict):
{"action":"BUY|SELL|HOLD|CLOSE","bias":"UP|DOWN|NEUTRAL","signal_strength":"LOW|MEDIUM|HIGH","summary":"≤2 lines","reason":"brief rationale (flow/liquidity/derivatives/technicals/sentiment)"}
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
