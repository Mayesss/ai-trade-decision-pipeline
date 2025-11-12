// lib/ai.ts

import { AI_BASE_URL, AI_MODEL, TRADE_WINDOW_MINUTES } from './constants';

// Types you might already have elsewhere; keeping lightweight here
export type AllowedAction = 'BUY' | 'SELL' | 'HOLD' | 'CLOSE';

export interface GatesForPrompt {
  spread_ok: boolean;
  liquidity_ok: boolean;
  atr_ok: boolean;
  slippage_ok?: boolean;
  // optional regime flags for transparency (your gates module can set them)
  regime_trend_up?: boolean;
  regime_trend_down?: boolean;
  // optional extras (nice to display / log)
  tier?: string;
}

export interface GatesMetricsForPrompt {
  // optional, purely for debug visibility in the prompt/logs
  spreadBpsNow?: number;
  top5BidUsdNow?: number;
  obImbNow?: number;
  atrPctNow?: number;

  // percentiles / fallbacks used
  spreadBpsP50?: number;
  spreadBpsP75?: number;
  depthUsdP50?: number;
  atrPctP25?: number;
  atrPctP85?: number;

  spreadBpsMax?: number;
  depthMinUSD?: number;
  atrPctFloor?: number;
  atrPctCeil?: number;
  slippageBpsMax?: number;
}

// ------------------------------
// Prompt Builder (consumes allowed_actions + gates)
// ------------------------------
export function buildPrompt(
  symbol: string,
  timeframe: string,
  bundle: any,
  analytics: any,
  position_status: string = 'none',
  news_sentiment: string = 'neutral',
  indicators: { micro: string; macro: string },
  // NEW: pass gates + allowed_actions from lib/gates.ts
  opts: {
    allowed_actions: AllowedAction[];
    gates: GatesForPrompt;
    metrics?: GatesMetricsForPrompt;
  }
) {
  const { allowed_actions, gates, metrics } = opts || ({} as any);

  // Ticker normalization
  const t = Array.isArray(bundle.ticker) ? bundle.ticker[0] : bundle.ticker;
  const price = Number(t?.lastPr ?? t?.last ?? t?.close ?? t?.price);
  const change = Number(t?.change24h ?? t?.changeUtc24h ?? t?.chgPct);
  const market_data = `price=${price}, change24h=${isFinite(change) ? change : 'n/a'}`;

  // Order-flow summary
  const order_flow = `buys=${(analytics.buys ?? 0).toFixed(3)}, sells=${(analytics.sells ?? 0).toFixed(
    3
  )}, CVD=${(analytics.cvd ?? 0).toFixed(3)} (obImb=${(analytics.obImb ?? 0).toFixed(2)})`;

  // Liquidity (top walls)
  const liquidity_data = `top bid walls: ${JSON.stringify(analytics.topWalls?.bid ?? [])}, top ask walls: ${JSON.stringify(
    analytics.topWalls?.ask ?? []
  )}`;

  // Derivatives
  const derivatives = `funding=${bundle.funding?.[0]?.fundingRate ?? 'n/a'}, openInterest=${
    bundle.oi?.openInterestList?.[0]?.size ??
    bundle.oi?.openInterestList?.[0]?.openInterest ??
    'n/a'
  }`;

  // Volume profile (compact)
  const vol_profile_str = (analytics.volume_profile || [])
    .slice(0, 10)
    .map((v: any) => `(${Number(v.price).toFixed(2)} → ${v.volume})`)
    .join(', ');

  // Costs & policy (display only; your execution layer enforces)
  const taker_round_trip_bps = 5;
  const slippage_bps = 2;
  const risk_policy = `fees=${taker_round_trip_bps}bps round-trip, slippage=${slippage_bps}bps, stop=1.5xATR(1H), take_profit=2.5xATR(1H), time_stop=${parseInt(timeframe, 10) * 3} minutes`;

  // Gating flags line
  const gating_flags =
    `regime_trend_up=${!!gates.regime_trend_up}, regime_trend_down=${!!gates.regime_trend_down}, ` +
    `spread_ok=${!!gates.spread_ok}, liquidity_ok=${!!gates.liquidity_ok}, atr_ok=${!!gates.atr_ok}` +
    (gates.slippage_ok === undefined ? '' : `, slippage_ok=${!!gates.slippage_ok}`) +
    (gates.tier ? `, tier=${gates.tier}` : '');

  // Optional metrics string (good for debugging)
  const metrics_line = metrics
    ? `METRICS: ${JSON.stringify(metrics)}`
    : '';

  const sys = `
You are an expert crypto market microstructure analyst and quantitative trading assistant.
Respond in strict JSON ONLY.

HARD RULES:
- You MUST choose the "action" from ALLOWED_ACTIONS only.
- If signal_strength is LOW or MEDIUM => "HOLD" (unless a position is open, then "CLOSE" is allowed).
- Trade ONLY if ALL gating flags are true: spread_ok, liquidity_ok, atr_ok (and slippage_ok if provided), AND
  (for BUY require regime_trend_up=true; for SELL require regime_trend_down=true).
- Consider costs (taker + slippage). Marginal edges that don't exceed costs => HOLD.
- Do not predict beyond 1 hour.
`.trim();

  const user = `
You are analyzing ${symbol} on a ${parseInt(timeframe, 10)}-minute horizon (simulation).

ALLOWED_ACTIONS: ${JSON.stringify(allowed_actions)}
GATING: ${gating_flags}
${metrics_line ? metrics_line + '\n' : ''}

RISK/COSTS:
- ${risk_policy}

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
2) Output exactly one action from ALLOWED_ACTIONS.
   - If no position is open, allowed_actions typically are ["BUY","HOLD"] or ["SELL","HOLD"] or ["HOLD"].
   - If a position is open, allowed_actions are ["HOLD","CLOSE"].
3) Assess signal strength: LOW, MEDIUM, or HIGH (volume clarity & book imbalance).
4) Summarize in ≤2 lines (e.g., choppy/trending/trapping).

STRICT JSON OUTPUT:
{"action":"BUY|SELL|HOLD|CLOSE","bias":"UP|DOWN|NEUTRAL","signal_strength":"LOW|MEDIUM|HIGH","summary":"≤2 lines","reason":"brief rationale (flow/liquidity/derivatives/technicals/sentiment)"}
`.trim();
 console.log(sys, user)
  return { system: sys, user, allowed_actions };
}

// ------------------------------
// OpenAI API Call (with post-parse enforcement)
// ------------------------------
export async function callAI(system: string, user: string, opts?: { allowed_actions?: AllowedAction[] }) {
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

  let out: any;
  try {
    out = JSON.parse(text);
  } catch {
    out = { action: 'HOLD', bias: 'NEUTRAL', signal_strength: 'LOW', summary: 'Invalid JSON from model', reason: 'Parser error' };
  }

  // Enforce allowed_actions if provided
  const allowed = opts?.allowed_actions ?? ['BUY', 'SELL', 'HOLD', 'CLOSE'];
  if (!allowed.includes(out.action)) {
    // If the model violated constraints, coerce to the safest valid action.
    out.action = allowed[0]; // prioritize first allowed action decided by your gating logic
    out.reason = (out.reason ? out.reason + ' | ' : '') + 'Action coerced to allowed set.';
  }

  return out;
}
