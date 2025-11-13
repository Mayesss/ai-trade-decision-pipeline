// lib/ai.ts

import { AI_BASE_URL, AI_MODEL, TRADE_WINDOW_MINUTES } from './constants';

// Types you might already have elsewhere; keeping lightweight here
export type AllowedAction = 'BUY' | 'SELL' | 'HOLD' | 'CLOSE';

export interface CloseConds {
  // set these in analyze.ts
  pnl_gt_pos?: boolean;     // e.g., PnL > +1%
  pnl_lt_neg?: boolean;     // e.g., PnL < -1%
  opposite_regime?: boolean;// macro regime now opposes position side
  cvd_flip?: boolean;       // CVD flipped against position in recent window
  time_stop?: boolean;      // optional: you can compute/bar-count
}

export function buildPrompt(
  symbol: string,
  timeframe: string,
  bundle: any,
  analytics: any,
  position_status: string = 'none',
  news_sentiment: string = 'neutral',
  indicators: { micro: string; macro: string },
  opts: {
    allowed_actions: AllowedAction[];
    gates: {
      spread_ok: boolean;
      liquidity_ok: boolean;
      atr_ok: boolean;
      slippage_ok?: boolean;
      regime_trend_up?: boolean;
      regime_trend_down?: boolean;
      tier?: string;
    };
    // NEW: explicit close permissions
    close_conditions?: CloseConds;
    // Optional: hide metrics
    includeMetrics?: boolean;
    metrics?: any;
  }
) {
  const { allowed_actions, gates, metrics, includeMetrics = false, close_conditions } = opts || ({} as any);
  const t = Array.isArray(bundle.ticker) ? bundle.ticker[0] : bundle.ticker;
  const price = Number(t?.lastPr ?? t?.last ?? t?.close ?? t?.price);
  const change = Number(t?.change24h ?? t?.changeUtc24h ?? t?.chgPct);
  const market_data = `price=${price}, change24h=${isFinite(change) ? change : 'n/a'}`;

  const order_flow = `buys=${(analytics.buys ?? 0).toFixed(3)}, sells=${(analytics.sells ?? 0).toFixed(
    3
  )}, CVD=${(analytics.cvd ?? 0).toFixed(3)} (obImb=${(analytics.obImb ?? 0).toFixed(2)})`;

  const liquidity_data = `top bid walls: ${JSON.stringify(analytics.topWalls?.bid ?? [])}, top ask walls: ${JSON.stringify(
    analytics.topWalls?.ask ?? []
  )}`;

  const derivatives = `funding=${bundle.funding?.[0]?.fundingRate ?? 'n/a'}, openInterest=${
    bundle.oi?.openInterestList?.[0]?.size ??
    bundle.oi?.openInterestList?.[0]?.openInterest ??
    'n/a'
  }`;

  const vol_profile_str = (analytics.volume_profile || [])
    .slice(0, 10)
    .map((v: any) => `(${Number(v.price).toFixed(2)} → ${v.volume})`)
    .join(', ');

  const taker_round_trip_bps = 5;
  const slippage_bps = 2;
  const risk_policy = `fees=${taker_round_trip_bps}bps round-trip, slippage=${slippage_bps}bps, stop=1.5xATR(1H), take_profit=2.5xATR(1H), time_stop=${parseInt(timeframe, 10) * 3} minutes`;

  const gating_flags =
    `regime_trend_up=${!!gates.regime_trend_up}, regime_trend_down=${!!gates.regime_trend_down}, ` +
    `spread_ok=${!!gates.spread_ok}, liquidity_ok=${!!gates.liquidity_ok}, atr_ok=${!!gates.atr_ok}` +
    (gates.slippage_ok === undefined ? '' : `, slippage_ok=${!!gates.slippage_ok}`) +
    (gates.tier ? `, tier=${gates.tier}` : '');

  const sys = `
You are an expert crypto market microstructure analyst and quantitative trading assistant.
Respond in strict JSON ONLY.

HARD RULES:
- You MUST choose the "action" from ALLOWED_ACTIONS only.
- You may choose "CLOSE" ONLY if CLOSE_CONDITIONS is present AND at least one of its booleans is true. Holding is in that case also allowed and is up t your analisys.
- Be strict, dont close without real pressure.
- If signal_strength is LOW or MEDIUM => "HOLD" (unless a position is open AND CLOSE_CONDITIONS true).
- Trade ONLY if ALL gating flags are true: spread_ok, liquidity_ok, atr_ok (and slippage_ok if provided), AND
  (for BUY require regime_trend_up=true; for SELL require regime_trend_down=true).
- Consider costs (taker + slippage). Marginal edges that don't exceed costs => HOLD.
- Do not predict beyond 1 hour.
`.trim();

  const metrics_line = includeMetrics && metrics ? `METRICS: ${JSON.stringify(metrics)}` : '';

  const user = `
You are analyzing ${symbol} on a ${parseInt(timeframe, 10)}-minute horizon (simulation).

ALLOWED_ACTIONS: ${JSON.stringify(allowed_actions)}
GATING: ${gating_flags}
${metrics_line ? metrics_line + '\n' : ''}
${close_conditions ? `CLOSE_CONDITIONS: ${JSON.stringify(close_conditions)}\n` : ''}

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
3) Assess signal strength: LOW, MEDIUM, or HIGH (volume clarity & book imbalance).
4) Summarize in ≤2 lines.

STRICT JSON OUTPUT:
{"action":"BUY|SELL|HOLD|CLOSE","bias":"UP|DOWN|NEUTRAL","signal_strength":"LOW|MEDIUM|HIGH","summary":"≤2 lines","reason":"brief rationale"}
`.trim();
console.log(sys,'\n',user)
  return { system: sys, user, allowed_actions, close_conditions };
}

export async function callAI(system: string, user: string, opts?: { allowed_actions?: AllowedAction[], close_conditions?: CloseConds }) {
  const apiKey = process.env.OPENAI_API_KEY;
  if (!apiKey) throw new Error('Missing OPENAI_API_KEY');

  const res = await fetch(`${AI_BASE_URL}/chat/completions`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${apiKey}` },
    body: JSON.stringify({
      model: AI_MODEL,
      messages: [{ role: 'system', content: system }, { role: 'user', content: user }],
      temperature: 0.2,
      response_format: { type: 'json_object' },
    }),
  });

  if (!res.ok) throw new Error(`AI error: ${res.status} ${res.statusText}`);
  const data = await res.json();
  let out: any;
  try { out = JSON.parse(data.choices?.[0]?.message?.content || '{}'); }
  catch { out = { action: 'HOLD', bias: 'NEUTRAL', signal_strength: 'LOW', summary: 'Invalid JSON', reason: 'parse error' }; }

  // Post-parse enforcement
  const allowed = opts?.allowed_actions ?? ['BUY','SELL','HOLD','CLOSE'];
  if (!allowed.includes(out.action)) out.action = allowed[0];

  // CLOSE only if permitted
  const cc = opts?.close_conditions;
  const ccTrue = !!cc && Object.values(cc).some(Boolean);
  if (out.action === 'CLOSE' && !ccTrue) {
    out.action = allowed.includes('HOLD') ? 'HOLD' : allowed[0];
    out.reason = (out.reason ? out.reason + ' | ' : '') + 'CLOSE not permitted by CLOSE_CONDITIONS.';
  }

  return out;
}