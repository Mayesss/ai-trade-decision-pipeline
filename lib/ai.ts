// lib/ai.ts

import { AI_BASE_URL, AI_MODEL, TRADE_WINDOW_MINUTES } from './constants';

// ------------------------------
// Prompt Builder (with guardrails & gating flags)
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

    const market_data = `price=${price}, change24h=${isFinite(change) ? change : 'n/a'}`;

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

    // --- Gating flags (simple & robust) ---
    const last = analytics.last || price || 0;
    const spread = analytics.spread || 0;
    const spread_ok = last > 0 ? spread / last < 0.0004 : false; // < 4 bps
    const sumTop = (lvls: any[], n: number) =>
        (lvls || []).slice(0, n).reduce((a, l) => a + Number(l[1] ?? l.size ?? 0), 0);
    const topBid = sumTop(bundle.orderbook?.bids || [], 5);
    const topAsk = sumTop(bundle.orderbook?.asks || [], 5);
    const liquidity_ok = topBid + topAsk > 0 ? topBid > topAsk * 0.6 : false;

    const trendMacroUp = indicators.macro.includes('trend=up');
    const trendMacroDown = indicators.macro.includes('trend=down');
    // Regime gate: only buy in up regime, only sell in down regime (the model will decide side, we enforce gate here)
    // We pass both booleans and instruct the model accordingly.
    const regime_trend_up = trendMacroUp;
    const regime_trend_down = trendMacroDown;

    // Extract ATR(1H) if present in string (best-effort)
    const atrMatch = indicators.macro.match(/ATR=([\d.]+)/);
    const atrAbs = atrMatch ? Number(atrMatch[1]) : NaN;
    const atrPct = isFinite(atrAbs) && last > 0 ? atrAbs / last : NaN;
    // atr_ok ETH between 0.0008–0.02, BTC between 0.0007–0.015 (8–200 bps) — tune per symbol
    const atr_ok = isFinite(atrPct) ? atrPct > 0.0008 && atrPct < 0.02 : true;
    // Costs (educate the model)
    const taker_round_trip_bps = 5; // 5 bps
    const slippage_bps = 2;

    const risk_policy = `fees=${taker_round_trip_bps}bps round-trip, slippage=${slippage_bps}bps, 
stop=1.5xATR(1H), take_profit=2.5xATR(1H), time_stop=${parseInt(timeframe, 10) * 3} minutes`;

    const gating_flags = `regime_trend_up=${regime_trend_up}, regime_trend_down=${regime_trend_down}, spread_ok=${spread_ok}, liquidity_ok=${liquidity_ok}, atr_ok=${atr_ok}`;

    const sys = `
You are an expert crypto market microstructure analyst and quantitative trading assistant. 
Respond in strict JSON ONLY.

HARD RULES:
- If signal_strength is LOW or MEDIUM => "HOLD" (unless a position is open, then "CLOSE" is allowed).
- Trade ONLY if ALL gating flags are true: spread_ok, liquidity_ok, atr_ok, AND (for BUY require regime_trend_up=true; for SELL require regime_trend_down=true). Otherwise action="HOLD".
- Consider costs: taker+slippage as provided. Marginal edges that don't exceed costs => HOLD.
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
    console.log(sys, user);

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
