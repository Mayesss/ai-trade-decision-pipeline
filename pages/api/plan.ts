import type { NextApiRequest, NextApiResponse } from 'next';

import { callAI } from '../../lib/ai';
import { computeAnalytics, fetchMarketBundle, fetchRealizedRoi } from '../../lib/analytics';
import { DEFAULT_TAKER_FEE_RATE, TRADE_WINDOW_MINUTES } from '../../lib/constants';
import { getGates } from '../../lib/gates';
import { fetchNewsWithHeadlines } from '../../lib/news';
import { readPlan, savePlan } from '../../lib/planStore';
import { loadExecutionLogs } from '../../lib/execLog';
import {
    buildIndicatorsFromMetrics,
    computeLocationConfluence,
    computeMetrics,
    deriveRegimeFlags,
    fetchCandles,
    TimeframeKey,
    TimeframeMetrics,
} from '../../src/plan/facts';

// This schema is a JSON *shape* template. In your output, replace placeholders with real values of the same JSON type.
// Do NOT output type-marker strings like "number"/"string" or placeholder text like "<SYMBOL>".
const PLAN_SCHEMA = `{
  "version": "plan_v1",
  "symbol": "<SYMBOL>",
  "mode": "live",
  "asof_ts": "<ISO_TIMESTAMP>",
  "plan_ts": "<ISO_TIMESTAMP>",
  "horizon_minutes": 60,
  "context_bias": "<UP|DOWN|NEUTRAL>",
  "macro_bias": "<UP|DOWN|NEUTRAL>",
  "primary_bias": "<UP|DOWN|NEUTRAL>",
  "micro_bias": "<UP|DOWN|NEUTRAL>",
  "allowed_directions": "<LONG_ONLY|SHORT_ONLY|BOTH|NONE>",
  "risk_mode": "<OFF|CONSERVATIVE|NORMAL|AGGRESSIVE>",
  "max_leverage": 1,
  "entry_mode": "<PULLBACK|BREAKOUT|EITHER|NONE>",
  "key_levels": {
    "1H": {
      "support_price": null,
      "support_strength": null,
      "support_state": "<at_level|approaching|rejected|broken|retesting>",
      "resistance_price": null,
      "resistance_strength": null,
      "resistance_state": "<at_level|approaching|rejected|broken|retesting>"
    },
    "4H": { "support_price": null, "resistance_price": null },
    "1D": { "support_price": null, "resistance_price": null }
  },
  "no_trade_rules": {
    "avoid_long_if_dist_to_resistance_atr_1H_lt": 0.6,
    "avoid_short_if_dist_to_support_atr_1H_lt": 0.6,
    "max_dist_from_ema20_15m_in_atr_for_new_entries": 2.2
  },
  "exit_urgency": {
    "trim_if_near_opposite_level": true,
    "close_if_invalidation": true,
    "invalidation_notes": "NONE"
  },
  "cooldown": {
    "enabled": false,
    "until_ts": null,
    "reason": ""
  },
  "summary": "<=2 lines>",
  "reason": "brief rationale"
}`;

type Plan = Record<string, any>;

const BIAS_VALUES = ['UP', 'DOWN', 'NEUTRAL'] as const;
const ALLOWED_DIRECTIONS = ['LONG_ONLY', 'SHORT_ONLY', 'BOTH', 'NONE'] as const;
const RISK_MODES = ['OFF', 'CONSERVATIVE', 'NORMAL', 'AGGRESSIVE'] as const;
const ENTRY_MODES = ['PULLBACK', 'BREAKOUT', 'EITHER', 'NONE'] as const;
const LEVEL_STATES = ['at_level', 'approaching', 'rejected', 'broken', 'retesting'] as const;

function formatNumber(n: any, digits = 4) {
    const num = Number(n);
    return Number.isFinite(num) ? Number(num.toFixed(digits)) : null;
}

function tfMs(granularity: string) {
    const m = String(granularity || '').trim().match(/^(\d+)([smHD])$/);
    if (!m) return 0;
    const v = Number(m[1]);
    const unit = m[2];
    if (!Number.isFinite(v) || v <= 0) return 0;
    switch (unit) {
        case 's':
            return v * 1000;
        case 'm':
            return v * 60 * 1000;
        case 'H':
            return v * 60 * 60 * 1000;
        case 'D':
            return v * 24 * 60 * 60 * 1000;
        default:
            return 0;
    }
}

function confirmedCandles(candles: any[], granularity: string, nowMs: number) {
    if (!Array.isArray(candles) || candles.length === 0) return [];
    const ms = tfMs(granularity);
    if (!ms) return candles;
    const lastTs = Number(candles.at(-1)?.[0]);
    if (!Number.isFinite(lastTs)) return candles;
    if (nowMs < lastTs + ms) return candles.slice(0, -1);
    return candles;
}

async function fetchPlanBucketTs(symbol: string, nowMs: number): Promise<string> {
    try {
        const candles1h = await fetchCandles(symbol, '1H', 10);
        const confirmed = confirmedCandles(candles1h, '1H', nowMs);
        const lastOpen = Number(confirmed.at(-1)?.[0]);
        const closeMs = Number.isFinite(lastOpen) ? lastOpen + tfMs('1H') : Math.floor(nowMs / tfMs('1H')) * tfMs('1H');
        return new Date(closeMs).toISOString();
    } catch {
        const ms = tfMs('1H') || 60 * 60 * 1000;
        const closeMs = Math.floor(nowMs / ms) * ms;
        return new Date(closeMs).toISOString();
    }
}

function safeNum(v: any) {
    const n = Number(v);
    return Number.isFinite(n) ? n : null;
}

function execWindowSummary(logs: any[], nowMs: number) {
    const windowStart = nowMs - 60 * 60 * 1000;
    const window = (logs || [])
        .map((l) => (l?.payload && typeof l.payload === 'object' ? l.payload : l))
        .filter((p: any) => Number(p?.ts ? Date.parse(p.ts) : p?.timestamp) >= windowStart);

    const isEntry = (d: string) => d.startsWith('ENTER_');
    const isExit = (d: string) => d === 'CLOSE' || d === 'TRIM';

    let trades = 0;
    let entries = 0;
    let exits = 0;
    const holdSecs: number[] = [];
    const slippageBps: number[] = [];
    const exitCauses: Record<string, number> = {};
    const invalidationHits = { fast: 0, mid: 0, hard: 0 };
    let blockedEntries = 0;
    let planAlignmentIssues = 0;

    for (const p of window) {
        const decision = String(p?.decision || '').toUpperCase();
        if (isEntry(decision)) entries += 1;
        if (isExit(decision)) exits += 1;
        if (isEntry(decision) || isExit(decision)) trades += 1;
        const hs = safeNum(p?.hold_seconds);
        if (hs !== null) holdSecs.push(hs);
        const slip = safeNum(p?.fill_slippage_bps);
        if (slip !== null) slippageBps.push(slip);
        const ec = String(p?.exit_cause || '').toUpperCase();
        if (ec) exitCauses[ec] = (exitCauses[ec] || 0) + 1;
        const trig = String(p?.trigger || '').toUpperCase();
        if (trig.startsWith('INVALIDATION')) {
            if (trig.includes('FAST')) invalidationHits.fast += 1;
            else if (trig.includes('MID')) invalidationHits.mid += 1;
            else if (trig.includes('HARD')) invalidationHits.hard += 1;
        }
        if (Array.isArray(p?.entry_blockers) && p.entry_blockers.length > 0) blockedEntries += 1;
        if (ec === 'DIRECTION_NOT_ALLOWED' || trig === 'DIR_MISMATCH') planAlignmentIssues += 1;
    }

    const avg = (arr: number[]) => (arr.length ? arr.reduce((a, b) => a + b, 0) / arr.length : null);
    const median = (arr: number[]) => {
        if (!arr.length) return null;
        const sorted = [...arr].sort((a, b) => a - b);
        const mid = Math.floor(sorted.length / 2);
        return sorted.length % 2 ? sorted[mid] : (sorted[mid - 1] + sorted[mid]) / 2;
    };

    const winRate = null; // not tracked here (needs realized PnL); leave null
    const churnFlag = trades >= 4 && median(holdSecs) !== null && median(holdSecs)! < 120;

    return {
        window_minutes: 60,
        sample_size: window.length,
        trades_count: trades,
        entries_count: entries,
        exits_count: exits,
        avg_hold_seconds: avg(holdSecs),
        median_hold_seconds: median(holdSecs),
        realized_pnl_pct_sum: null,
        realized_pnl_pct_avg: null,
        win_rate: winRate,
        slippage_bps_avg: avg(slippageBps),
        fees_bps_est_total: null,
        top_exit_causes: exitCauses,
        invalidation_hits: invalidationHits,
        blocked_entries_count: blockedEntries,
        plan_alignment_issues: planAlignmentIssues,
        churn_flag: churnFlag,
        executor_scalping_detected: churnFlag,
    };
}

function pickParam(req: NextApiRequest, key: string, fallback?: any) {
    const raw = req.query?.[key] ?? (req.body as any)?.[key];
    if (Array.isArray(raw)) return raw[0] ?? fallback;
    return raw ?? fallback;
}

function levelOrDefault(level?: TimeframeMetrics['sr'], side?: 'support' | 'resistance') {
    if (!level || !side) return { price: null, dist: null, strength: null, state: 'approaching' };
    const descriptor = side === 'support' ? level.support : level.resistance;
    return {
        price: descriptor?.price ?? null,
        dist: descriptor?.dist_in_atr ?? null,
        strength: descriptor?.level_strength ?? null,
        state: descriptor?.level_state ?? 'approaching',
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
        'asof_ts',
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

    if (plan.version !== 'plan_v1') errors.push(`plan.version must be "plan_v1"`);
    if (plan.mode !== 'live') errors.push(`plan.mode must be "live"`);
    if (typeof plan.symbol !== 'string' || !plan.symbol.trim()) errors.push('plan.symbol must be a non-empty string');
    if (!BIAS_VALUES.includes(plan.context_bias)) errors.push('plan.context_bias invalid_enum');
    if (!BIAS_VALUES.includes(plan.macro_bias)) errors.push('plan.macro_bias invalid_enum');
    if (!BIAS_VALUES.includes(plan.primary_bias)) errors.push('plan.primary_bias invalid_enum');
    if (!BIAS_VALUES.includes(plan.micro_bias)) errors.push('plan.micro_bias invalid_enum');
    if (!ALLOWED_DIRECTIONS.includes(plan.allowed_directions)) errors.push('plan.allowed_directions invalid_enum');
    if (!RISK_MODES.includes(plan.risk_mode)) errors.push('plan.risk_mode invalid_enum');
    if (!ENTRY_MODES.includes(plan.entry_mode)) errors.push('plan.entry_mode invalid_enum');

    if (typeof plan.asof_ts !== 'string' || !plan.asof_ts || !Number.isFinite(Date.parse(plan.asof_ts))) {
        errors.push('plan.asof_ts must be an ISO timestamp string');
    }
    if (typeof plan.plan_ts !== 'string' || !plan.plan_ts || !Number.isFinite(Date.parse(plan.plan_ts))) {
        errors.push('plan.plan_ts must be an ISO timestamp string');
    }
    if (typeof plan.horizon_minutes !== 'number' || !Number.isFinite(plan.horizon_minutes) || plan.horizon_minutes !== 60) {
        errors.push('plan.horizon_minutes must be the number 60');
    }
    if (typeof plan.max_leverage !== 'number' || !Number.isFinite(plan.max_leverage)) {
        errors.push('plan.max_leverage must be a number');
    } else {
        const lev = plan.max_leverage;
        if (!Number.isInteger(lev) || lev < 1 || lev > 5) errors.push('plan.max_leverage must be an integer in [1..5]');
    }

    const kl = plan.key_levels || {};
    validateKeys(kl, ['1H', '4H', '1D'], 'key_levels', errors);
    validateKeys(kl['1H'] || {}, ['support_price', 'support_strength', 'support_state', 'resistance_price', 'resistance_strength', 'resistance_state'], 'key_levels.1H', errors);
    validateKeys(kl['4H'] || {}, ['support_price', 'resistance_price'], 'key_levels.4H', errors);
    validateKeys(kl['1D'] || {}, ['support_price', 'resistance_price'], 'key_levels.1D', errors);

    const numOrNull = (v: any) => v === null || (typeof v === 'number' && Number.isFinite(v));
    const str = (v: any) => typeof v === 'string';

    if (!numOrNull(kl?.['1H']?.support_price)) errors.push('key_levels.1H.support_price must be number|null');
    if (!numOrNull(kl?.['1H']?.resistance_price)) errors.push('key_levels.1H.resistance_price must be number|null');
    if (!numOrNull(kl?.['1H']?.support_strength)) errors.push('key_levels.1H.support_strength must be number|null');
    if (!numOrNull(kl?.['1H']?.resistance_strength)) errors.push('key_levels.1H.resistance_strength must be number|null');
    if (!LEVEL_STATES.includes(kl?.['1H']?.support_state)) errors.push('key_levels.1H.support_state invalid_enum');
    if (!LEVEL_STATES.includes(kl?.['1H']?.resistance_state)) errors.push('key_levels.1H.resistance_state invalid_enum');
    if (!numOrNull(kl?.['4H']?.support_price)) errors.push('key_levels.4H.support_price must be number|null');
    if (!numOrNull(kl?.['4H']?.resistance_price)) errors.push('key_levels.4H.resistance_price must be number|null');
    if (!numOrNull(kl?.['1D']?.support_price)) errors.push('key_levels.1D.support_price must be number|null');
    if (!numOrNull(kl?.['1D']?.resistance_price)) errors.push('key_levels.1D.resistance_price must be number|null');

    validateKeys(plan.no_trade_rules || {}, ['avoid_long_if_dist_to_resistance_atr_1H_lt', 'avoid_short_if_dist_to_support_atr_1H_lt', 'max_dist_from_ema20_15m_in_atr_for_new_entries'], 'no_trade_rules', errors);
    validateKeys(plan.exit_urgency || {}, ['trim_if_near_opposite_level', 'close_if_invalidation', 'invalidation_notes'], 'exit_urgency', errors);
    if (plan?.exit_urgency && !invalidationNotesValid(plan.exit_urgency.invalidation_notes)) {
        errors.push('exit_urgency.invalidation_notes invalid_format');
    }
    validateKeys(plan.cooldown || {}, ['enabled', 'until_ts', 'reason'], 'cooldown', errors);

    const ntr = plan.no_trade_rules || {};
    if (typeof ntr.avoid_long_if_dist_to_resistance_atr_1H_lt !== 'number' || !Number.isFinite(ntr.avoid_long_if_dist_to_resistance_atr_1H_lt))
        errors.push('no_trade_rules.avoid_long_if_dist_to_resistance_atr_1H_lt must be a number');
    if (typeof ntr.avoid_short_if_dist_to_support_atr_1H_lt !== 'number' || !Number.isFinite(ntr.avoid_short_if_dist_to_support_atr_1H_lt))
        errors.push('no_trade_rules.avoid_short_if_dist_to_support_atr_1H_lt must be a number');
    if (
        typeof ntr.max_dist_from_ema20_15m_in_atr_for_new_entries !== 'number' ||
        !Number.isFinite(ntr.max_dist_from_ema20_15m_in_atr_for_new_entries)
    )
        errors.push('no_trade_rules.max_dist_from_ema20_15m_in_atr_for_new_entries must be a number');

    const ex = plan.exit_urgency || {};
    if (typeof ex.trim_if_near_opposite_level !== 'boolean') errors.push('exit_urgency.trim_if_near_opposite_level must be boolean');
    if (typeof ex.close_if_invalidation !== 'boolean') errors.push('exit_urgency.close_if_invalidation must be boolean');
    if (!(typeof ex.invalidation_notes === 'string')) errors.push('exit_urgency.invalidation_notes must be a string');

    const cd = plan.cooldown || {};
    if (typeof cd.enabled !== 'boolean') errors.push('cooldown.enabled must be boolean');
    if (!(cd.until_ts === null || (str(cd.until_ts) && Number.isFinite(Date.parse(cd.until_ts))))) errors.push('cooldown.until_ts must be null or ISO timestamp string');
    if (!str(cd.reason)) errors.push('cooldown.reason must be string');

    if (!str(plan.summary) || !String(plan.summary).trim()) errors.push('plan.summary must be a non-empty string');
    if (!str(plan.reason) || !String(plan.reason).trim()) errors.push('plan.reason must be a non-empty string');
    if (str(plan.summary) && String(plan.summary).split('\n').length > 2) errors.push('plan.summary must be <= 2 lines');

    return errors;
}

function invalidationNotesValid(val: any): boolean {
    if (val === 'NONE') return true;
    if (typeof val !== 'string') return false;
    const pattern =
        /^LVL=(-?\d+(\.\d+)?);FAST=(5m|15m|1H)_close_(above|below)_x\d+;MID=(5m|15m|1H)_close_(above|below)_x\d+;HARD=(5m|15m|1H)_close_(above|below)_x\d+;ACTION=(CLOSE|TRIM50|TRIM30|TRIM70)$/;
    return pattern.test(val);
}

function expectedInvalidationDirection(allowedDirections: string, primaryBias?: string): 'above' | 'below' | null {
    if (allowedDirections === 'SHORT_ONLY') return 'above';
    if (allowedDirections === 'LONG_ONLY') return 'below';
    if (allowedDirections === 'BOTH') {
        if (primaryBias === 'DOWN') return 'above';
        if (primaryBias === 'UP') return 'below';
    }
    return null;
}

function normalizeInvalidationNotes(plan: Plan) {
    if (!plan || typeof plan !== 'object') return;
    if (plan.allowed_directions === 'NONE' && plan.exit_urgency?.invalidation_notes !== 'NONE') {
        plan.exit_urgency.invalidation_notes = 'NONE';
        return;
    }
    const notes = plan?.exit_urgency?.invalidation_notes;
    if (notes === 'NONE' || typeof notes !== 'string') return;
    if (!invalidationNotesValid(notes)) return;
    const expected = expectedInvalidationDirection(plan.allowed_directions, plan.primary_bias);
    if (!expected) return;
    plan.exit_urgency.invalidation_notes = notes.replace(/_close_(above|below)_x/g, `_close_${expected}_x`);
}

function buildSystemPrompt() {
    return `
You are a crypto trading Planner agent. Your job is to produce a stable one-hour plan for the executor to follow. You do not place trades and you do not output BUY/SELL/CLOSE as an action. You output only a strict JSON object matching the provided schema exactly.

Principles:
- Timeframes: 1D = context risk throttle, 4H = macro regime, 1H = primary structure, 15m = micro tilt (mainly entry preference).
- Prefer stability: do not flip allowed_directions unless there is a clear regime/structure shift.
- Avoid micro-noise: do not use 1m tape/orderbook to choose direction.
- Notional independence: treat notional-dependent gate/cost inputs (slippage/total_cost) as tradeability + sizing signals only. They may force allowed_directions=NONE / risk_mode=OFF, or reduce leverage/risk_mode, but must NOT flip the directional biases (UP/DOWN) or switch LONG_ONLY vs SHORT_ONLY.
- Use proximity to strong opposing levels to restrict entries and tighten invalidation rules.
- Cap risk via risk_mode and max_leverage (cap only).

Output requirements:
- Output JSON only, parseable by JSON.parse.
- Do not wrap the output in markdown or code fences.
- No extra keys anywhere; every key in schema must be present.
- The schema is a JSON shape template: replace placeholder values with real values of the same JSON type.
- Angle-bracket values like <A|B|C> are placeholders: replace them with exactly one valid value (do NOT output the angle brackets or the | list).
- invalidation_notes must be "NONE" or follow the exact grammar provided by the user.
- Use the user-provided facts as truth; do not invent indicator values.`
}

function buildUserPrompt(params: {
    symbol: string;
    mode: string;
    asof: string;
    planBucketTs: string;
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
    execSummary: any;
}) {
    const { symbol, mode, asof, planBucketTs, horizon, baseGates, spreadBps, takerFeeRate, slippageBps, totalCostBps } = params;
    const { tf, prevPlan, regime, location, news, lastClosedPnlPct, coarseLiquidity, execSummary } = params;

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
You are planning for ${symbol} (mode=${mode}), horizon_minutes=${horizon}.
Timestamps:
- asof_ts=${asof} (generation time)
- plan_bucket_ts=${planBucketTs} (last confirmed 1H close; use this for plan_ts)
Return strict JSON following this schema exactly (no extra keys). Set plan_ts=plan_bucket_ts and asof_ts=asof_ts.
The SCHEMA below is a JSON shape template: in your output, replace placeholders with real values of the same JSON type.
Replace any <...> placeholder with exactly one of the allowed values. Do not output the angle brackets or the | list.

SCHEMA:
${PLAN_SCHEMA}

VALIDATION RULES (must follow):
1. No extra keys are allowed anywhere.
2. All keys in schema must be present.
3. Enums + types:
- context_bias/macro_bias/primary_bias/micro_bias must be one of: UP, DOWN, NEUTRAL
- allowed_directions must be one of: LONG_ONLY, SHORT_ONLY, BOTH, NONE
- risk_mode must be one of: OFF, CONSERVATIVE, NORMAL, AGGRESSIVE
- entry_mode must be one of: PULLBACK, BREAKOUT, EITHER, NONE
- max_leverage must be an integer in [1..5] (set based on risk_mode + confidence)
4. Timestamp formats:
- asof_ts and plan_ts must be ISO strings, e.g. "2025-12-16T15:07:12.000Z"
5. invalidation_notes must be either "NONE" OR match this grammar exactly:
LVL=<number>;FAST=<tf>_close_<above|below>_x<n>;MID=<tf>_close_<above|below>_x<n>;HARD=<tf>_close_<above|below>_x<n>;ACTION=<CLOSE|TRIM50|TRIM30|TRIM70>
- <tf> must be one of [5m, 15m, 1H] (case-sensitive), and <n> is a positive integer.
- Note: Bitget expects lowercase 'm' for minutes and uppercase 'H' for hours. Use '1H' (not '1h').
- Example: LVL=2963.54;FAST=5m_close_above_x2;MID=15m_close_above_x1;HARD=1H_close_above_x1;ACTION=CLOSE
5b. Direction rule:
- If allowed_directions=SHORT_ONLY, FAST/MID/HARD must all use close_above (invalidation is resistance break).
- If allowed_directions=LONG_ONLY, FAST/MID/HARD must all use close_below (invalidation is support break).
- If allowed_directions=BOTH, keep direction consistent with primary_bias (DOWN => close_above, UP => close_below); if NEUTRAL, choose one and keep all three consistent.
6. LVL selection:
Set LVL to the key level relevant to direction bias: if the plan favors shorts, prefer the nearest 1H resistance; if it favors longs, prefer the nearest 1H support (fallback to the closest strong 4H level if 1H is missing).
- If allowed_directions includes shorts (SHORT_ONLY or BOTH): LVL must be nearest 1H resistance_price; if missing/invalid then use strong 4H resistance_price.
- If allowed_directions includes longs (LONG_ONLY or BOTH): LVL must be nearest 1H support_price; if missing/invalid then use strong 4H support_price.
- If allowed_directions=NONE, set invalidation_notes="NONE".
7. FAST tightness by risk_mode:
- If risk_mode=CONSERVATIVE, FAST must be tight (prefer 5m_close_*_x2).
- If risk_mode=AGGRESSIVE, FAST may be slower (15m allowed) but MUST be present.


FACTS:
Base gates:
- spread_ok=${baseGates.spread_ok}, liquidity_ok=${baseGates.liquidity_ok}, atr_ok=${baseGates.atr_ok}, slippage_ok=${baseGates.slippage_ok}
- spread_bps=${formatNumber(spreadBps, 3)}${coarseLiquidityLine ? `, ${coarseLiquidityLine}` : ''}
Exec window summary (last 60m, deterministic):
${JSON.stringify(execSummary)}

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
- Do not change directional bias because of notional-dependent cost/gate metrics. Use costs/gates only to decide tradeability (NONE/OFF) and risk sizing (risk_mode/max_leverage), not LONG-vs-SHORT preference.
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
    const mode = 'live';
    const horizon = 60;
    const notional = Number(pickParam(req, 'notional', 50));

    if (!symbol) {
        return res.status(400).json({ error: 'symbol_required' });
    }

    try {
        const now = Date.now();
        const asof = new Date(now).toISOString();
        const planBucketTs = await fetchPlanBucketTs(symbol, now);

        const [tfMetrics, prevPlan, newsBundle, execLogs] = await Promise.all([
            fetchTimeframeMetrics(symbol),
            readPlan(symbol),
            fetchNewsWithHeadlines(symbol),
            loadExecutionLogs(symbol, 120),
        ]);

        const execSummary = execWindowSummary(execLogs || [], now);

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
        planBucketTs,
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
        execSummary,
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
        plan.asof_ts = asof;
        plan.plan_ts = planBucketTs;
        plan.symbol = symbol;
        plan.mode = mode;
        plan.horizon_minutes = horizon;
        normalizeInvalidationNotes(plan);

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
