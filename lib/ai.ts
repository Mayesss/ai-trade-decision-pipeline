// lib/ai.ts

import { AI_BASE_URL, AI_MODEL, TRADE_WINDOW_MINUTES } from './constants';
import type { TradeDecision } from './trading';

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
    lastDecision: TradeDecision | null,
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

    const sys = `You are an expert quantitative crypto market analyst. Output JSON only.`;

    const user = `Assess short-term direction for ${symbol} based on inputs and constraints.
DATA INPUTS:
- Current price and % change: ${market_data}
- Volume profile (fixed lookback window: ${TRADE_WINDOW_MINUTES || 30}m): ${vol_profile_str}
- Order flow summary (buy/sell imbalance, CVD): ${order_flow}
- Order book & liquidity map (visible walls): ${liquidity_data}
- Funding rate, open interest, and liquidation data: ${derivatives}
- News sentiment: ${news_sentiment.toLowerCase()}
- Current position: ${position_status}
- Technical indicators (short-term): ${indicators.micro}
- Macro indicators (1h context): ${indicators.macro}
- Last AI action: ${
        lastDecision ? `${lastDecision.action} (${new Date(lastDecision?.timestamp!).toLocaleString()})` : 'None'
    }

TASK:
1. Analyze whether current conditions favor short-term long, short, or no trade.
2. If in a position, decide whether to stay in, scale out, or close.
3. Explain reasoning briefly (price action, volume delta, liquidity shifts, or sentiment change).
4. Output one action from: BUY | SELL | HOLD | CLOSE.
5. Include 1–2 line market context.

Constraints:
- Time horizon = ${timeframe}
- Do not make predictions beyond 1 hour.
- Assume educational simulation, not live trading.
- Return JSON strictly as: {"action":"BUY|SELL|HOLD|CLOSE","summary":"...","reason":"..."}
- If a position is open, only options are HOLD|CLOSE.
`;

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
