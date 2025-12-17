import type { NextApiRequest, NextApiResponse } from 'next';

import { callAI } from '../../lib/ai';
import { computeAnalytics, fetchMarketBundle, fetchRealizedRoi } from '../../lib/analytics';
import { DEFAULT_TAKER_FEE_RATE, TRADE_WINDOW_MINUTES } from '../../lib/constants';
import { getGates } from '../../lib/gates';
import { fetchNewsWithHeadlines } from '../../lib/news';
import { readPlan, savePlan } from '../../lib/planStore';
import {
    buildIndicatorsFromMetrics,
    computeLocationConfluence,
    computeMetrics,
    deriveRegimeFlags,
    fetchCandles,
    TimeframeKey,
    TimeframeMetrics,
} from '../../src/plan/facts';

const PLAN_SCHEMA = `{
  "version": "plan_v1",
  "symbol": "BTCUSDT",
  "mode": "live",
  "plan_ts": "2025-12-16T15:00:00.000Z",
  "horizon_minutes": 60,
  "context_bias": "UP|DOWN|NEUTRAL",
  "macro_bias": "UP|DOWN|NEUTRAL",
  "primary_bias": "UP|DOWN|NEUTRAL",
  "micro_bias": "UP|DOWN|NEUTRAL",
  "allowed_directions": "LONG_ONLY|SHORT_ONLY|BOTH|NONE",
  "risk_mode": "OFF|CONSERVATIVE|NORMAL|AGGRESSIVE",
  "max_leverage": 1,
  "entry_mode": "PULLBACK|BREAKOUT|EITHER|NONE",
  "key_levels": {
    "1H": {
      "support_price": 0,
      "support_strength": 0,
      "support_state": "at_level",
      "resistance_price": 0,
      "resistance_strength": 0,
      "resistance_state": "at_level"
    },
    "4H": { "support_price": 0, "resistance_price": 0 },
    "1D": { "support_price": 0, "resistance_price": 0 }
  },
  "no_trade_rules": {
    "avoid_long_if_dist_to_resistance_atr_1H_lt": 0.6,
    "avoid_short_if_dist_to_support_atr_1H_lt": 0.6,
    "max_dist_from_ema20_15m_in_atr_for_new_entries": 2.2
  },
  "exit_urgency": {
    "trim_if_near_opposite_level": true,
    "close_if_invalidation": true,
    "invalidation_notes": "string"
  },
  "cooldown": {
    "enabled": false,
    "until_ts": null,
    "reason": ""
  },
  "summary": "<= 2 lines",
  "reason": "brief rationale"
}`;

type Plan = Record<string, any>;

function formatNumber(n: any, digits = 4) {
    const num = Number(n);
    return Number.isFinite(num) ? Number(num.toFixed(digits)) : null;
}

function pickParam(req: NextApiRequest, key: string, fallback?: any) {
    const raw = req.query?.[key] ?? (req.body as any)?.[key];
    if (Array.isArray(raw)) return raw[0] ?? fallback;
    return raw ?? fallback;
}

function levelOrDefault(level?: TimeframeMetrics['sr'], side?: 'support' | 'resistance') {
    if (!level || !side) return { price: null, dist: null, strength: null, state: 'rejected' };
    const descriptor = side === 'support' ? level.support : level.resistance;
    return {
        price: descriptor?.price ?? null,
        dist: descriptor?.dist_in_atr ?? null,
        strength: descriptor?.level_strength ?? null,
        state: descriptor?.level_state ?? 'rejected',
    };
}

function validateKeys(obj: any, expected: string[], path: string, errors: string[]) {
    expected.forEach((k) => {
        if (!(k in obj)) errors.push(`${path}.${k} missing`);
    });
    Object.keys(obj || {}).forEach((k) => {
        if (!expected.includes(k)) errors.push(`${path}.${k} unexpected`);
    });
}

function validatePlanShape(plan: any) {
    const errors: string[] = [];
    const rootKeys = [
        'version',
        'symbol',
        'mode',
        'plan_ts',
        'horizon_minutes',
        'context_bias',
        'macro_bias',
        'primary_bias',
        'micro_bias',
        'allowed_directions',
        'risk_mode',
        'max_leverage',
        'entry_mode',
        'key_levels',
        'no_trade_rules',
        'exit_urgency',
        'cooldown',
        'summary',
        'reason',
    ];
    if (!plan || typeof plan !== 'object') {
        errors.push('plan must be an object');
        return errors;
    }
    validateKeys(plan, rootKeys, 'plan', errors);

    const kl = plan.key_levels || {};
    validateKeys(kl, ['1H', '4H', '1D'], 'key_levels', errors);
    validateKeys(kl['1H'] || {}, ['support_price', 'support_strength', 'support_state', 'resistance_price', 'resistance_strength', 'resistance_state'], 'key_levels.1H', errors);
    validateKeys(kl['4H'] || {}, ['support_price', 'resistance_price'], 'key_levels.4H', errors);
    validateKeys(kl['1D'] || {}, ['support_price', 'resistance_price'], 'key_levels.1D', errors);

    validateKeys(plan.no_trade_rules || {}, ['avoid_long_if_dist_to_resistance_atr_1H_lt', 'avoid_short_if_dist_to_support_atr_1H_lt', 'max_dist_from_ema20_15m_in_atr_for_new_entries'], 'no_trade_rules', errors);
    validateKeys(plan.exit_urgency || {}, ['trim_if_near_opposite_level', 'close_if_invalidation', 'invalidation_notes'], 'exit_urgency', errors);
    if (plan?.exit_urgency && !invalidationNotesValid(plan.exit_urgency.invalidation_notes)) {
        errors.push('exit_urgency.invalidation_notes invalid_format');
    }
    validateKeys(plan.cooldown || {}, ['enabled', 'until_ts', 'reason'], 'cooldown', errors);

    return errors;
}

function invalidationNotesValid(val: any): boolean {
    if (val === 'NONE') return true;
    if (typeof val !== 'string') return false;
    const pattern =
        /^LVL=(-?\d+(\.\d+)?);FAST=(5m|15m|30m|1H|4H)_close_(above|below)_x\d+;MID=(5m|15m|30m|1H|4H)_close_(above|below)_x\d+;HARD=(5m|15m|30m|1H|4H)_close_(above|below)_x\d+;ACTION=(CLOSE|TRIM50|TRIM30|TRIM70)$/;
    return pattern.test(val);
}

function buildSystemPrompt() {
    return `
You are a crypto trading Planner agent. Your job is to produce a stable one-hour plan for the executor to follow. You do not place trades and you do not output BUY/SELL/CLOSE as an action. You output only a strict JSON object matching the provided schema exactly.

Principles:
- Timeframes: 1D = context risk throttle, 4H = macro regime, 1H = primary structure, 15m = micro tilt (mainly entry preference).
- Prefer stability: do not flip allowed_directions unless there is a clear regime/structure shift.
- Avoid micro-noise: do not use 1m tape/orderbook to choose direction.
- Use proximity to strong opposing levels to restrict entries and tighten invalidation rules.
- Cap risk via risk_mode and max_leverage (cap only).

Output requirements:
- Output JSON only, parseable by JSON.parse.
- Do not wrap the output in markdown or code fences.
- No extra keys anywhere; every key in schema must be present.
- invalidation_notes must be "NONE" or follow the exact grammar provided by the user.
- Use the user-provided facts as truth; do not invent indicator values.`
}

function buildUserPrompt(params: {
    symbol: string;
    mode: string;
    asof: string;
    horizon: number;
    baseGates: any;
    spreadBps: number;
    takerFeeRate: number;
    slippageBps: number;
    totalCostBps: number;
    lastClosedPnlPct: number | null;
    prevPlan: any;
    tf: Record<TimeframeKey, TimeframeMetrics>;
    regime: { regime_trend_up: boolean; regime_trend_down: boolean };
    location: {
        location_confluence_score: number;
        into_context_support: boolean;
        into_context_resistance: boolean;
        context_breakdown_confirmed: boolean;
        context_breakout_confirmed: boolean;
    };
    news: { sentiment: string; headlines: string[] };
    coarseLiquidity?: { bidBandUsd?: number; askBandUsd?: number; bandBps?: number };
}) {
    const { symbol, mode, asof, horizon, baseGates, spreadBps, takerFeeRate, slippageBps, totalCostBps } = params;
    const { tf, prevPlan, regime, location, news, lastClosedPnlPct, coarseLiquidity } = params;

    const ctx = tf['1D'];
    const macro = tf['4H'];
    const primary = tf['1H'];
    const micro = tf['15m'];

    const ctxSupport = levelOrDefault(ctx.sr, 'support');
    const ctxResistance = levelOrDefault(ctx.sr, 'resistance');
    const macroSupport = levelOrDefault(macro.sr, 'support');
    const macroResistance = levelOrDefault(macro.sr, 'resistance');
    const primarySupport = levelOrDefault(primary.sr, 'support');
    const primaryResistance = levelOrDefault(primary.sr, 'resistance');

    const coarseLiquidityLine = coarseLiquidity
        ? `coarse_liquidity_bid_band_usd=${formatNumber(coarseLiquidity.bidBandUsd)}, coarse_liquidity_ask_band_usd=${formatNumber(
              coarseLiquidity.askBandUsd,
          )}, band_bps=${formatNumber(coarseLiquidity.bandBps)}`
        : '';

    return `
You are planning for ${symbol} (mode=${mode}), horizon_minutes=${horizon}, asof=${asof}.
Return strict JSON following this schema exactly (no extra keys). Set plan_ts to the asof timestamp.

SCHEMA:
${PLAN_SCHEMA}

VALIDATION RULES (must follow):
1. No extra keys are allowed anywhere.
2. All keys in schema must be present.
3. invalidation_notes must be either "NONE" OR match this grammar exactly:
LVL=<number>;FAST=<tf>_close_<above|below>_x<n>;MID=<tf>_close_<above|below>_x<n>;HARD=<tf>_close_<above|below>_x<n>;ACTION=<CLOSE|TRIM50|TRIM30|TRIM70>
- <tf> must be one of [5m, 15m, 30m, 1H, 4H] (case-sensitive), and <n> is a positive integer.
- Example: LVL=2963.54;FAST=5m_close_above_x2;MID=15m_close_above_x1;HARD=1H_close_above_x1;ACTION=CLOSE
4. LVL selection:
Set LVL to the key level relevant to direction bias: if the plan favors shorts, prefer the nearest 1H resistance; if it favors longs, prefer the nearest 1H support (fallback to the closest strong 4H level if 1H is missing).
- If allowed_directions includes shorts (SHORT_ONLY or BOTH): LVL must be nearest 1H resistance_price; if missing/invalid then use strong 4H resistance_price.
- If allowed_directions includes longs (LONG_ONLY or BOTH): LVL must be nearest 1H support_price; if missing/invalid then use strong 4H support_price.
- If allowed_directions=NONE, set invalidation_notes="NONE".
5. FAST tightness by risk_mode:
- If risk_mode=CONSERVATIVE, FAST must be tight (prefer 5m_close_*_x2).
- If risk_mode=AGGRESSIVE, FAST may be slower (15m allowed) but MUST be present.


FACTS:
Base gates:
- spread_ok=${baseGates.spread_ok}, liquidity_ok=${baseGates.liquidity_ok}, atr_ok=${baseGates.atr_ok}, slippage_ok=${baseGates.slippage_ok}
- spread_bps=${formatNumber(spreadBps, 3)}${coarseLiquidityLine ? `, ${coarseLiquidityLine}` : ''}

Costs:
- taker_fee_rate=${takerFeeRate}, slippage_bps=${formatNumber(slippageBps, 3)}, total_cost_bps=${formatNumber(totalCostBps, 3)}

Recent performance:
- last_closed_realized_pnl_pct=${formatNumber(lastClosedPnlPct, 3)}
- prev_plan=${JSON.stringify(prevPlan ?? null)}

1D (context):
- trend=${ctx.trend}, rsi=${formatNumber(ctx.rsi, 2)}, atr_pct=${formatNumber(ctx.atr_pct, 4)}
- ema20=${formatNumber(ctx.ema20)}, ema50=${formatNumber(ctx.ema50)}, sma200=${formatNumber(ctx.sma200)}, slope_ema21_10=${formatNumber(ctx.slopeEMA21_10, 4)}
- dist_from_ema20_in_atr=${formatNumber(ctx.dist_from_ema20_in_atr, 3)}
- S/R: 
    support_price=${ctxSupport.price}, dist_to_support_in_atr=${formatNumber(ctxSupport.dist, 3)}, strength=${formatNumber(
        ctxSupport.strength,
        3,
    )}, state=${ctxSupport.state}
    resistance_price=${ctxResistance.price}, dist_to_resistance_in_atr=${formatNumber(
        ctxResistance.dist,
        3,
    )}, strength=${formatNumber(ctxResistance.strength, 3)}, state=${ctxResistance.state}

4H (macro):
- regime_trend_up=${regime.regime_trend_up}, regime_trend_down=${regime.regime_trend_down}
- trend=${macro.trend}, rsi=${formatNumber(macro.rsi, 2)}, atr_pct=${formatNumber(macro.atr_pct, 4)}
- ema20=${formatNumber(macro.ema20)}, ema50=${formatNumber(macro.ema50)}, slope_ema21_10=${formatNumber(macro.slopeEMA21_10, 4)}
- dist_from_ema20_in_atr=${formatNumber(macro.dist_from_ema20_in_atr, 3)}
- S/R (if available): 
    support_price=${macroSupport.price}, dist_to_support_in_atr=${formatNumber(
        macroSupport.dist,
        3,
    )}, strength=${formatNumber(macroSupport.strength, 3)}, state=${macroSupport.state}
    resistance_price=${macroResistance.price}, dist_to_resistance_in_atr=${formatNumber(
        macroResistance.dist,
        3,
    )}, strength=${formatNumber(macroResistance.strength, 3)}, state=${macroResistance.state}

1H (primary):
- trend=${primary.trend}, rsi=${formatNumber(primary.rsi, 2)}, atr_pct=${formatNumber(primary.atr_pct, 4)}
- ema20=${formatNumber(primary.ema20)}, ema50=${formatNumber(primary.ema50)}, sma200=${formatNumber(
        primary.sma200,
    )}, slope_ema21_10=${formatNumber(primary.slopeEMA21_10, 4)}
- dist_from_ema20_in_atr=${formatNumber(primary.dist_from_ema20_in_atr, 3)}
S/R: 
    support_price=${primarySupport.price}, dist_to_support_in_atr=${formatNumber(
        primarySupport.dist,
        3,
    )}, strength=${formatNumber(primarySupport.strength, 3)}, state=${primarySupport.state}
    resistance_price=${primaryResistance.price}, dist_to_resistance_in_atr=${formatNumber(
        primaryResistance.dist,
        3,
    )}, strength=${formatNumber(primaryResistance.strength, 3)}, state=${primaryResistance.state}

15m (micro tilt):
- trend=${micro.trend}, rsi=${formatNumber(micro.rsi, 2)}, atr_pct=${formatNumber(micro.atr_pct, 4)}
- ema20=${formatNumber(micro.ema20)}, ema50=${formatNumber(micro.ema50)}, slope_ema21_10=${formatNumber(
        micro.slopeEMA21_10,
        4,
    )}
- dist_from_ema20_in_atr=${formatNumber(micro.dist_from_ema20_in_atr, 3)}

Location:
- location_confluence_score=${formatNumber(location.location_confluence_score, 3)}
- into_context_support=${location.into_context_support}, into_context_resistance=${location.into_context_resistance}
- context_breakdown_confirmed=${location.context_breakdown_confirmed}, context_breakout_confirmed=${location.context_breakout_confirmed}

News:
- News_sentiment=${news.sentiment} 
- headlines=${JSON.stringify(news.headlines ?? [])}

TASK:
- Set biases: context_bias, macro_bias, primary_bias, micro_bias (UP/DOWN/NEUTRAL).
- If ANY base gate is false: allowed_directions=NONE, risk_mode=OFF, entry_mode=NONE, invalidation_notes="NONE".
- Otherwise decide: allowed_directions, risk_mode, max_leverage cap, entry_mode.
- Populate key_levels from provided S/R.
- Keep default no_trade_rules unless there is a clear reason to adjust (ATR-based).
- Set exit_urgency flags and invalidation_notes using the strict grammar above.
- Keep plan stable vs prev_plan unless a clear regime/structure shift occurred.

Return strict JSON only.`;
}

async function fetchTimeframeMetrics(symbol: string) {
    const tfs: TimeframeKey[] = ['1D', '4H', '1H', '15m'];
    const candles = await Promise.all(tfs.map((tf) => fetchCandles(symbol, tf)));
    return tfs.reduce((acc, tf, idx) => {
        acc[tf] = computeMetrics(tf, candles[idx] || []);
        return acc;
    }, {} as Record<TimeframeKey, TimeframeMetrics>);
}

async function handler(req: NextApiRequest, res: NextApiResponse) {
    if (req.method !== 'POST' && req.method !== 'GET') {
        return res.status(405).json({ error: 'Method Not Allowed', message: 'Use POST or GET' });
    }

    const symbol = String(pickParam(req, 'symbol', '') || '').toUpperCase();
    const mode = String(pickParam(req, 'mode', 'live'));
    const horizon = 60;
    const notional = Number(pickParam(req, 'notional', 50));

    if (!symbol) {
        return res.status(400).json({ error: 'symbol_required' });
    }

    try {
        const asof = new Date().toISOString();

        const [tfMetrics, prevPlan, newsBundle] = await Promise.all([
            fetchTimeframeMetrics(symbol),
            readPlan(symbol),
            fetchNewsWithHeadlines(symbol),
        ]);

        const indicators = buildIndicatorsFromMetrics(tfMetrics);
        const regime = deriveRegimeFlags(tfMetrics['4H']);
        const location = computeLocationConfluence(tfMetrics);

        const [bundle, roi] = await Promise.all([
            fetchMarketBundle(symbol, '1H', { includeTrades: false, tradeMinutes: Number(TRADE_WINDOW_MINUTES || 60) }),
            fetchRealizedRoi(symbol, 24),
        ]);
        const analytics = computeAnalytics({ ...bundle, trades: [] });
        const gatesResult = getGates({
            symbol,
            bundle,
            analytics,
            indicators,
            notionalUSDT: Number.isFinite(notional) && notional > 0 ? notional : 50,
            positionOpen: false,
        });

        const baseGates = gatesResult.gates;
        const gateMetrics = gatesResult.metrics || {};

        const spreadBps =
            Number.isFinite(gateMetrics.spreadBpsNow) && gateMetrics.spreadBpsNow !== undefined
                ? gateMetrics.spreadBpsNow
                : 0;
        const slippageBps =
            Number.isFinite(gateMetrics.expectedSlippageBps) && gateMetrics.expectedSlippageBps !== undefined
                ? gateMetrics.expectedSlippageBps
                : 0;
        const envFee = Number(process.env.TAKER_FEE_RATE);
        const takerFeeRate = Number.isFinite(envFee) ? envFee : DEFAULT_TAKER_FEE_RATE;
        const totalCostBps = takerFeeRate * 2 * 10000 + slippageBps;

        const userPrompt = buildUserPrompt({
            symbol,
            mode,
            asof,
            horizon,
            baseGates,
            spreadBps,
            takerFeeRate,
            slippageBps,
            totalCostBps,
            lastClosedPnlPct: roi.lastNetPct ?? null,
            prevPlan: prevPlan?.plan ?? null,
            tf: tfMetrics,
            regime,
            location,
            news: {
                sentiment: newsBundle.sentiment || 'NEUTRAL',
                headlines: Array.isArray(newsBundle.headlines) ? newsBundle.headlines.slice(0, 5) : [],
            },
            coarseLiquidity: {
                bidBandUsd: gateMetrics.bidBandUsdNow,
                askBandUsd: gateMetrics.askBandUsdNow,
                bandBps: gateMetrics.bandBps,
            },
        });

        const systemPrompt = buildSystemPrompt();

        const attempts: { plan?: Plan; raw: any; errors?: string[] }[] = [];
        let plan: Plan | null = null;

        for (let attempt = 0; attempt < 3; attempt += 1) {
            const promptToSend =
                attempt === 0
                    ? userPrompt
                    : `${userPrompt}\n\nThe last response was invalid or missing keys/grammar (especially invalidation_notes). Fix JSON to match schema exactly. Previous response: ${JSON.stringify(
                          attempts[attempt - 1]?.raw,
                      )}`;
            const aiResp = await callAI(systemPrompt, promptToSend);
            const validationErrors = validatePlanShape(aiResp);
            attempts.push({ plan: validationErrors.length ? undefined : aiResp, raw: aiResp, errors: validationErrors });
            if (!validationErrors.length) {
                plan = aiResp;
                break;
            }
        }

        if (!plan) {
            return res
                .status(500)
                .json({ error: 'plan_validation_failed', attempts: attempts.map((a) => ({ errors: a.errors, raw: a.raw })) });
        }

        // Force deterministic fields
        plan.plan_ts = asof;
        plan.symbol = symbol;
        plan.mode = mode;
        plan.horizon_minutes = horizon;

        const persistResult = await savePlan(symbol, plan, { system: systemPrompt, user: userPrompt });

        return res.status(200).json({
            plan,
            persisted: persistResult.persisted,
            persist_error: persistResult.error,
            asof,
            attempts: attempts.map((a) => ({ errors: a.errors, raw: a.raw })),
        });
    } catch (err) {
        console.error('Plan generation failed:', err);
        return res.status(500).json({ error: 'plan_generation_failed', message: err instanceof Error ? err.message : String(err) });
    }
}

export default handler;
export const config = { runtime: 'nodejs' };
