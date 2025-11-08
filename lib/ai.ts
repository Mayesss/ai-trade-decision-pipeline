// lib/ai.ts

import { AI_BASE_URL, AI_MODEL, TRADE_WINDOW_MINUTES } from './constants';

// ------------------------------
// Prompt Builder
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
    const price = bundle.ticker?.[0]?.lastPr || bundle.ticker?.[0]?.last || bundle.ticker?.[0]?.close;
    const change = bundle.ticker?.[0]?.change24h || bundle.ticker?.[0]?.changeUtc24h || bundle.ticker?.[0]?.chgPct;
    const market_data = `price=${price}, change24h=${change}`;
    const order_flow = `buys=${analytics.buys.toFixed(3)}, sells=${analytics.sells.toFixed(
        3,
    )}, CVD=${analytics.cvd.toFixed(3)}`;
    const liquidity_data = `top bid walls: ${JSON.stringify(analytics.topWalls.bid)}, top ask walls: ${JSON.stringify(
        analytics.topWalls.ask,
    )}`;
    const derivatives = bundle.productType
        ? `funding=${bundle.funding?.[0]?.fundingRate ?? 'n/a'}, openInterest=${
              bundle.oi?.openInterestList?.[0]?.size ?? 'n/a'
          }`
        : 'n/a';

    const vol_profile_str = analytics.volume_profile
        .slice(0, 10)
        .map((v: any) => `(${v.price.toFixed(2)} → ${v.volume})`)
        .join(', ');

    const sys = `You are an expert crypto market microstructure analyst and quantitative trading assistant. You operate in a high-frequency environment and respond in strict JSON only.`;

    const user = `
You are analyzing ${symbol} on a ${parseInt(timeframe, 10)}-minute time horizon. Assume simulation only.

DATA INPUTS (with explicit windows):
- Current price and % change (now): ${market_data}
- Volume profile (fixed lookback window = 30m): ${vol_profile_str}
- Order flow summary (1m tape/CVD, buy-sell imbalance, last 5–15m): ${order_flow}
- Order book & liquidity map (visible walls/spoofs, current snapshot): ${liquidity_data}
- Funding rate, open interest, and liquidations (changes over last 30–60m): ${derivatives}
- News sentiment ONLY (exclude social/market sentiment): ${news_sentiment.toLowerCase()}
- Current position: ${position_status}
- Technical indicators (short-term, 1m timeframe, last 30 candles): ${indicators.micro}
- Macro indicators (context on 1h timeframe, last 30 candles): ${indicators.macro}

TASKS:
1. Evaluate short-term bias (up / down / neutral) based on volume delta, liquidity, and derivative data.
2. Output one action only: "BUY", "SELL", "HOLD", or "CLOSE".
   - If no position is open, return BUY/SELL/HOLD.
   - If position is open, return HOLD or CLOSE only.
        i. If currentPnL is > 0.5% but signal weakens or CVD flips, consider "CLOSE".
        ii. If currentPnL is < -0.5% and pressure builds against, consider "CLOSE".
3. Assess signal strength: LOW, MEDIUM, HIGH (based on volume and order flow clarity).
4. Summarize market in ≤2 lines (mention if choppy, trending, trapping, etc.)

RULES:
- Avoid trades if signal is LOW.
- Favor setups with volume confirmation and clear book imbalance.
- Do not predict beyond 1 hour.
- If volatility is flat, return HOLD.
- Be strict: NO position flip without real pressure.

OUTPUT (strict JSON):
{"action":"BUY|SELL|HOLD|CLOSE","bias":"UP|DOWN|NEUTRAL","signal_strength":"LOW|MEDIUM|HIGH","summary":"≤2 lines","reason":"brief rationale (flow/liquidity/derivatives/technicals/sentiment)"}
`;

    console.log(user);

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
