import type { NextApiRequest, NextApiResponse } from 'next';

import { computeATR, computeEMA, computeRSI_Wilder } from '../../lib/indicators';
import { bitgetFetch, resolveProductType } from '../../lib/bitget';
import { computeAnalytics, fetchPositionInfo, fetchRecentPositionWindows, fetchTradesBudgeted, PositionInfo } from '../../lib/analytics';
import {
    FLOW_SURGE_MULT_1M,
    FLOW_SURGE_WINDOW_1M,
    TRIM_MIN_HOLD_SECONDS_CONSERVATIVE,
    TRIM_MIN_HOLD_SECONDS_DEFAULT,
    TRIM_PROXIMITY_ATR_CONSERVATIVE,
    TRIM_PROXIMITY_ATR_DEFAULT,
} from '../../lib/constants';
import { readPlan } from '../../lib/planStore';
import { readExecState, saveExecState } from '../../lib/execState';
import { executeDecision } from '../../lib/trading';
import { appendExecutionLog } from '../../lib/execLog';

type InvalidationRule = {
    tf: string;
    direction: 'above' | 'below';
    count: number;
};

type ParsedInvalidation = {
    lvl: number | null;
    fast?: InvalidationRule;
    mid?: InvalidationRule;
    hard?: InvalidationRule;
    action?: 'CLOSE' | 'TRIM30' | 'TRIM50' | 'TRIM70';
};

type PositionState = 'FLAT' | 'LONG' | 'SHORT';

export const config = { runtime: 'nodejs' };

const STALE_GRACE_MINUTES = 10;
const ENTRY_COOLDOWN_MS = 10 * 60 * 1000; // 10 minutes
const REENTER_AFTER_INVALIDATION_MS = 3 * 60 * 1000;
const EXIT_ORDER_LOCK_MS = 60 * 1000;

function toNum(x: any) {
    const n = Number(x);
    return Number.isFinite(n) ? n : NaN;
}

function normalizeTf(tf: string) {
    const m = String(tf || '').trim().match(/^(\d+)([a-zA-Z])$/);
    if (!m) return String(tf || '').trim();
    const v = m[1];
    const unit = m[2];
    if (unit === 'm' || unit === 'M') return `${v}m`;
    if (unit === 'h' || unit === 'H') return `${v}H`;
    if (unit === 'd' || unit === 'D') return `${v}D`;
    if (unit === 's' || unit === 'S') return `${v}s`;
    return `${v}${unit}`;
}

function tfMs(tf: string) {
    const m = normalizeTf(tf).match(/^(\d+)([smSMHD])$/);
    if (!m) return 0;
    const v = Number(m[1]);
    const unit = m[2];
    if (!Number.isFinite(v) || v <= 0) return 0;
    switch (unit) {
        case 's':
        case 'S':
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

function confirmedCandles(candles: any[], tf: string, nowMs: number) {
    if (!Array.isArray(candles) || candles.length === 0) return [];
    const ms = tfMs(tf);
    if (!ms) return candles;
    const lastTs = Number(candles.at(-1)?.[0]);
    if (!Number.isFinite(lastTs)) return candles;
    // Bitget often includes the currently-forming candle; drop it so we only use confirmed closes.
    if (nowMs < lastTs + ms) return candles.slice(0, -1);
    return candles;
}

function obImbCentered(obImb: number) {
    if (!Number.isFinite(obImb)) return 0;
    // Our computeAnalytics() uses [-1,1], but normalize [0,1] inputs defensively.
    if (obImb >= 0 && obImb <= 1) return obImb - 0.5;
    return obImb;
}

function compactExecutionResult(x: any) {
    if (!x || typeof x !== 'object') return x ?? null;
    const keep: any = {};
    const keys = [
        'placed',
        'orderId',
        'clientOid',
        'leverage',
        'leverageApplied',
        'leverageError',
        'closed',
        'partial',
        'partialClosePct',
        'size',
        'reversed',
        'targetSide',
        'note',
    ];
    for (const k of keys) {
        if (k in x) keep[k] = (x as any)[k];
    }
    return keep;
}

function formatLevel(level: any) {
    return Number.isFinite(level) ? Number(level).toFixed(2) : null;
}

function buildReasonDetail(payload: any) {
    const reason = String(payload?.reason || '').trim();
    const decision = String(payload?.decision || '').trim();
    const blockers = Array.isArray(payload?.entry_blockers) ? payload.entry_blockers.filter(Boolean) : [];
    const invalidation = payload?.invalidation_eval;

    if (decision === 'WAIT' && blockers.length) {
        return `Entries disabled: ${blockers.join(', ')}.`;
    }
    if (reason === 'pending_exit_order') {
        return 'Exit order recently placed; waiting for lock expiry.';
    }
    if (reason.startsWith('invalidation_')) {
        const lvl = formatLevel(invalidation?.lvl);
        const trigger = String(payload?.trigger || 'invalidation').replace(/_/g, ' ');
        return `Invalidation triggered${lvl ? ` at ${lvl}` : ''} (${trigger}).`;
    }
    if (reason === 'direction_not_allowed') return 'Open position conflicts with plan allowed directions.';
    if (reason === 'flow_reversal') return 'Flow reversal against position while in profit; trimming risk.';
    if (reason === 'trim_near_support') return 'Trim as price nears 1H support.';
    if (reason === 'trim_near_resistance') return 'Trim as price nears 1H resistance.';
    if (reason === 'holding') return 'No exit or trim trigger; holding position.';
    if (reason === 'recent_exit_cooldown') return 'Recent exit cooldown active; waiting before re-entry.';
    if (reason === 'entry_cooldown') return 'Entry cooldown active.';
    if (reason.startsWith('insufficient_market_data')) return 'Insufficient market data for indicators; skipping.';
    if (reason === 'extension_block') return 'Price too extended from 15m EMA20; no entry.';
    if (reason === 'no_dir') return 'No clear preferred direction.';
    if (reason === 'micro_bias_block') return 'Micro trend bias conflicts with entry direction.';
    if (reason === 'micro_filter_block') {
        const failures = Array.isArray(payload?.entry_eval?.micro_filter_failures)
            ? payload.entry_eval.micro_filter_failures.filter(Boolean)
            : [];
        return failures.length ? `Micro filters failed: ${failures.join(', ')}.` : 'Micro filters failed (RSI/EMA/orderbook).';
    }
    if (reason === 'flow_against') {
        const gateReason = payload?.entry_eval?.flowGateReason ? ` (${payload.entry_eval.flowGateReason})` : '';
        return `Orderflow against preferred direction${gateReason}.`;
    }
    if (reason === 'too_close_support') return 'Too close to plan 1H support; avoid shorting into support.';
    if (reason === 'too_close_resistance') return 'Too close to plan 1H resistance; avoid longing into resistance.';
    if (reason === 'entry_mode_none') return 'Plan entry mode is NONE.';
    if (reason === 'pullback_long') return 'Pullback into 1H support zone with confirmation.';
    if (reason === 'pullback_short') return 'Pullback into 1H resistance zone with confirmation.';
    if (reason === 'breakout_long') return 'Breakout above 1H resistance with confirmation.';
    if (reason === 'breakout_short') return 'Breakout below 1H support with confirmation.';
    if (reason === 'no_entry_signal') return 'No entry conditions met for the current mode.';
    if (reason === 'stale_plan') return 'Plan is stale; entries disabled.';
    if (reason === 'no_plan') return 'No plan available.';
    if (reason === 'plan_entries_disabled') return 'Plan disables entries.';
    if (!reason) return null;
    return `Decision ${decision || 'WAIT'} due to ${reason.replace(/_/g, ' ')}.`;
}

function enrichReasonDetail(payload: any) {
    payload.reason_detail = buildReasonDetail(payload);
    return payload;
}

async function lookupRecentRealizedPnl(symbol: string, nowMs: number) {
    const windows = await fetchRecentPositionWindows(symbol, 6);
    const closed = windows.filter((w) => typeof w.exitTimestamp === 'number' && w.exitTimestamp <= nowMs);
    if (!closed.length) return null;
    const closest = closed.reduce((best, curr) => {
        const bestDiff = Math.abs(nowMs - (best.exitTimestamp as number));
        const currDiff = Math.abs(nowMs - (curr.exitTimestamp as number));
        return currDiff < bestDiff ? curr : best;
    }, closed[0]);
    const diffMs = Math.abs(nowMs - Number(closest.exitTimestamp || 0));
    if (diffMs > 2 * 60 * 60 * 1000) return null;
    return {
        id: closest.id,
        side: closest.side ?? null,
        entry_ts: closest.entryTimestamp ?? null,
        exit_ts: closest.exitTimestamp ?? null,
        pnl_net: closest.pnlNet ?? null,
        pnl_gross: closest.pnlGross ?? null,
        pnl_pct: closest.pnlPct ?? null,
        pnl_gross_pct: closest.pnlGrossPct ?? null,
        notional: closest.notional ?? null,
    };
}

async function maybeAttachRealizedPnl(payload: any, symbol: string, nowMs: number) {
    if (payload?.dryRun) return payload;
    const decision = String(payload?.decision || '').toUpperCase();
    if (decision !== 'CLOSE' && decision !== 'TRIM') return payload;
    const exec = payload?.order_details?.execution;
    const executed = exec?.placed || exec?.closed || exec?.partial;
    if (!executed) return payload;
    payload.realized_pnl = await lookupRecentRealizedPnl(symbol, nowMs);
    return payload;
}

function pickParam(req: NextApiRequest, key: string, fallback?: any) {
    const raw = req.query?.[key] ?? (req.body as any)?.[key];
    if (Array.isArray(raw)) return raw[0] ?? fallback;
    return raw ?? fallback;
}

async function fetchCandles(symbol: string, granularity: string, limit: number) {
    const productType = resolveProductType();
    const cs = await bitgetFetch('GET', '/api/v2/mix/market/candles', {
        symbol,
        productType,
        granularity,
        limit,
    });
    return (cs || []).slice().sort((a: any, b: any) => Number(a[0]) - Number(b[0]));
}

function parseInvalidation(notes: any): ParsedInvalidation | null {
    if (typeof notes !== 'string' || notes === 'NONE') return null;
    const regex =
        /^LVL=(-?\d+(?:\.\d+)?);FAST=(5m|15m|1h)_close_(above|below)_x(\d+);MID=(5m|15m|1h)_close_(above|below)_x(\d+);HARD=(5m|15m|1h)_close_(above|below)_x(\d+);ACTION=(CLOSE|TRIM50|TRIM30|TRIM70)$/i;
    const m = notes.match(regex);
    if (!m) {
        if (notes.startsWith('LVL=')) console.warn('Invalid invalidation_notes format:', notes);
        return null;
    }
    const lvl = Number(m[1]);
    const fast: InvalidationRule = {
        tf: normalizeTf(String(m[2])),
        direction: String(m[3]).toLowerCase() as any,
        count: Number(m[4]),
    };
    const mid: InvalidationRule = {
        tf: normalizeTf(String(m[5])),
        direction: String(m[6]).toLowerCase() as any,
        count: Number(m[7]),
    };
    const hard: InvalidationRule = {
        tf: normalizeTf(String(m[8])),
        direction: String(m[9]).toLowerCase() as any,
        count: Number(m[10]),
    };
    const action = String(m[11]).toUpperCase() as ParsedInvalidation['action'];
    return { lvl, fast, mid, hard, action };
}

function invalidationDirectionInfo(parsed: ParsedInvalidation | null): { direction: 'above' | 'below' | null; mixed: boolean } {
    if (!parsed) return { direction: null, mixed: false };
    const dirs = new Set<string>();
    if (parsed.fast?.direction) dirs.add(parsed.fast.direction);
    if (parsed.mid?.direction) dirs.add(parsed.mid.direction);
    if (parsed.hard?.direction) dirs.add(parsed.hard.direction);
    if (dirs.size === 1) return { direction: dirs.values().next().value as 'above' | 'below', mixed: false };
    if (dirs.size > 1) return { direction: null, mixed: true };
    return { direction: null, mixed: false };
}

function countConsecutive(closes: number[], lvl: number, direction: 'above' | 'below') {
    let count = 0;
    for (let i = closes.length - 1; i >= 0; i -= 1) {
        const c = closes[i];
        if (direction === 'above' ? c > lvl : c < lvl) count += 1;
        else break;
    }
    return count;
}

function countConsecutiveWithBuffer(closes: number[], lvl: number, direction: 'above' | 'below', buffer: number) {
    const adj = Number.isFinite(buffer) ? buffer : 0;
    const threshold = direction === 'above' ? lvl + adj : lvl - adj;
    let count = 0;
    for (let i = closes.length - 1; i >= 0; i -= 1) {
        const c = closes[i];
        if (direction === 'above' ? c > threshold : c < threshold) count += 1;
        else break;
    }
    return count;
}

function tfMinutes(tf: string) {
    const m = String(tf || '').trim().match(/^(\d+)([smhdSMHD])$/);
    if (!m) return 1;
    const v = Number(m[1]);
    const unit = m[2];
    switch (unit) {
        case 's':
        case 'S':
            return Math.max(1 / 60, v / 60);
        case 'm':
        case 'M':
            return v;
        case 'h':
        case 'H':
            return v * 60;
        case 'd':
        case 'D':
            return v * 60 * 24;
        default:
            return 1;
    }
}

function gateChecks(spreadBps: number, depthUSD: number, slippageBps = 5) {
    return {
        spread_ok_now: spreadBps > 0 && spreadBps < 10,
        liquidity_ok_now: depthUSD > 0 && depthUSD >= 5_000,
        slippage_ok_now: slippageBps < 12,
    };
}

function positionStateFrom(pos: any): PositionState {
    if (!pos || pos.status === 'none') return 'FLAT';
    return pos.holdSide === 'short' ? 'SHORT' : 'LONG';
}

function allowedForDirection(allowed: string, side: PositionState) {
    if (allowed === 'NONE') return false;
    if (side === 'LONG') return allowed === 'LONG_ONLY' || allowed === 'BOTH';
    if (side === 'SHORT') return allowed === 'SHORT_ONLY' || allowed === 'BOTH';
    return true;
}

function pickDirection(allowed: string, macro: string, primary: string) {
    if (allowed === 'LONG_ONLY') return 'LONG';
    if (allowed === 'SHORT_ONLY') return 'SHORT';
    if (allowed !== 'BOTH') return null;
    if (macro === 'UP' && primary !== 'DOWN') return 'LONG';
    if (macro === 'DOWN' && primary !== 'UP') return 'SHORT';
    if (primary === 'UP') return 'LONG';
    if (primary === 'DOWN') return 'SHORT';
    return null;
}

function leverageFromRisk(risk: string, cap: number) {
    const base = risk === 'AGGRESSIVE' ? 3 : risk === 'NORMAL' ? 2 : 1;
    return Math.min(cap || base, cap ? cap : base);
}

function reasonCode(reason: string) {
    if (!reason) return '';
    return String(reason).trim().replace(/\s+/g, '_').toUpperCase();
}

function sizeFromRisk(notional: number, risk: string) {
    if (!Number.isFinite(notional) || notional <= 0) return 0;
    if (risk === 'CONSERVATIVE') return notional * 0.5;
    return notional;
}

function profitActive(pos: any, last: number) {
    const entry = Number(pos?.entryPrice);
    if (!entry || !last) return false;
    return pos.holdSide === 'long' ? last > entry : last < entry;
}

async function handler(req: NextApiRequest, res: NextApiResponse) {
    if (req.method !== 'POST' && req.method !== 'GET') return res.status(405).json({ error: 'Method Not Allowed', message: 'Use POST or GET' });

    const symbol = String(pickParam(req, 'symbol', '') || '').toUpperCase();
    const notional = Number(pickParam(req, 'notional', 50));
    const rawDryRun = pickParam(req, 'dryRun', pickParam(req, 'dryrun', 'true'));
    const dryRun = String(rawDryRun).toLowerCase() !== 'false';

    if (!symbol) return res.status(400).json({ error: 'symbol_required' });

    const now = Date.now();
    const asofIso = new Date(now).toISOString();

    try {
        const [planRecord, pos, execState] = await Promise.all([
            readPlan(symbol),
            fetchPositionInfo(symbol),
            readExecState(symbol),
        ]);
        const plan = planRecord?.plan;
        const positionState = positionStateFrom(pos);
        let planStale = false;
        if (!plan || plan.version !== 'plan_v1') planStale = true;
        const planTsMs = plan?.plan_ts ? Date.parse(plan.plan_ts) : 0;
        const horizon = Number(plan?.horizon_minutes || 0);
        const graceMs = STALE_GRACE_MINUTES * 60 * 1000;
        if (plan && planTsMs && horizon) {
            if (now > planTsMs + horizon * 60 * 1000 + graceMs) planStale = true;
        }

        const entriesDisabled =
            !plan ||
            planStale ||
            plan.allowed_directions === 'NONE' ||
            plan.risk_mode === 'OFF' ||
            plan.entry_mode === 'NONE';

        if (positionState === 'FLAT' && entriesDisabled) {
            const reason = !plan ? 'no_plan' : planStale ? 'stale_plan' : 'plan_entries_disabled';
            const entry_blockers = [];
            if (!plan) entry_blockers.push('no_plan');
            if (planStale) entry_blockers.push('stale_plan');
            if (plan?.allowed_directions === 'NONE') entry_blockers.push('dir_none');
            if (plan?.risk_mode === 'OFF') entry_blockers.push('risk_off');
            if (plan?.entry_mode === 'NONE') entry_blockers.push('entry_mode_none');
            const payload = {
                ts: asofIso,
                symbol,
                plan_id: plan?.plan_ts ? `${symbol}:${plan.plan_ts}` : null,
                plan_ts: plan?.plan_ts || null,
                plan_stale: planStale,
                plan_allowed_directions: plan?.allowed_directions,
                plan_risk_mode: plan?.risk_mode,
                plan_entry_mode: plan?.entry_mode,
                position_state: positionState,
                entries_disabled: true,
                entry_blockers,
                market: null,
                indicators: null,
                levels: null,
                entry_eval: null,
                invalidation_eval: null,
                decision: 'WAIT',
                reason,
                reason_code: reasonCode(reason),
                trigger: null,
                orders: [],
                dryRun,
                gatesNow: null,
                skipped_market_fetch: true,
                hold_seconds: null,
                mfe: null,
                mae: null,
                fill_slippage_bps: null,
                exit_cause: null,
                position_size_base: null,
                position_notional: null,
                exit_order_reduce_only: null,
                exit_order_size_base: null,
                exit_order_size_notional: null,
                invalidation_notes: plan?.exit_urgency?.invalidation_notes ?? null,
                invalidation_rule_direction: invalidationDirectionInfo(parseInvalidation(plan?.exit_urgency?.invalidation_notes)).direction,
            };
            const finalizedPayload = enrichReasonDetail(payload);
            try {
                await appendExecutionLog({ symbol, timestamp: now, payload: finalizedPayload });
            } catch (err) {
                console.warn('Failed to append execution log:', err);
            }
            return res.status(200).json(finalizedPayload);
        }

        // Market snapshot
        const productType = resolveProductType();
        const [orderbook, candles1m, candles5m, candles15m, candles30m, candles1h, candles4h, trades] = await Promise.all([
            bitgetFetch('GET', '/api/v2/mix/market/orderbook', {
                symbol,
                productType,
                limit: 50,
            }),
            fetchCandles(symbol, '1m', 120),
            fetchCandles(symbol, '5m', 120),
            fetchCandles(symbol, '15m', 120),
            fetchCandles(symbol, '30m', 120),
            fetchCandles(symbol, '1H', 220),
            fetchCandles(symbol, '4H', 220),
            fetchTradesBudgeted(symbol, productType, { minutes: 5, maxTrades: 800, maxPages: 5, maxMs: 1200 }),
        ]);

        const candles1mConfirmed = confirmedCandles(candles1m, '1m', now);
        const candles5mConfirmed = confirmedCandles(candles5m, '5m', now);
        const candles15mConfirmed = confirmedCandles(candles15m, '15m', now);
        const candles30mConfirmed = confirmedCandles(candles30m, '30m', now);
        const candles1hConfirmed = confirmedCandles(candles1h, '1H', now);
        const candles4hConfirmed = confirmedCandles(candles4h, '4H', now);

        const bestBid = toNum(orderbook?.bids?.[0]?.[0] ?? orderbook?.bids?.[0]?.price);
        const bestAsk = toNum(orderbook?.asks?.[0]?.[0] ?? orderbook?.asks?.[0]?.price);
        const entryPx =
            pos && (pos as PositionInfo).status === 'open'
                ? toNum((pos as any).entryPrice ?? (pos as any).openPriceAvg)
                : NaN;
        const last = bestBid > 0 && bestAsk > 0 ? (bestBid + bestAsk) / 2 : entryPx;
        const spread = bestAsk > 0 && bestBid > 0 ? bestAsk - bestBid : 0;
        const spreadBps = last > 0 ? (spread / last) * 1e4 : 999;
        const depthUSD =
            (orderbook?.bids || []).slice(0, 10).reduce((acc: number, [p, s]: any) => acc + Number(p) * Number(s), 0) +
            (orderbook?.asks || []).slice(0, 10).reduce((acc: number, [p, s]: any) => acc + Number(p) * Number(s), 0);

        let positionSizeBaseRaw = NaN;
        if (pos && (pos as PositionInfo).status === 'open') {
            const openPos = pos as Extract<PositionInfo, { status: 'open' }>;
            positionSizeBaseRaw = toNum(openPos.total ?? openPos.available);
        }
        const positionSizeBase = Number.isFinite(positionSizeBaseRaw) && positionSizeBaseRaw !== 0 ? Math.abs(positionSizeBaseRaw) : null;
        const positionNotional =
            positionSizeBase && Number.isFinite(last) && last > 0 ? positionSizeBase * last : null;

        const analytics = computeAnalytics({
            orderbook,
            trades,
            ticker: { lastPr: last, last },
            candles: candles1m,
        });
        const flowTotal = Number(analytics.buys) + Number(analytics.sells);
        const flowBias = flowTotal > 0 ? (Number(analytics.buys) - Number(analytics.sells)) / flowTotal : 0;
        const surgeWindow = Math.max(5, Math.min(120, FLOW_SURGE_WINDOW_1M));
        const surgeMult = FLOW_SURGE_MULT_1M;
        const recentVolumes = candles1mConfirmed.slice(-surgeWindow).map((c: any) => toNum(c[5]));
        const volSum = recentVolumes.reduce((acc, v) => acc + (Number.isFinite(v) ? v : 0), 0);
        const volAvg = recentVolumes.length ? volSum / recentVolumes.length : 0;
        const lastVol = toNum(candles1mConfirmed.at(-1)?.[5]);
        const flowSurge = Number.isFinite(lastVol) && volAvg > 0 ? lastVol >= volAvg * surgeMult : false;

        // Indicators
        const closes5m = candles5mConfirmed.map((c: any) => toNum(c[4]));
        const closes15m = candles15mConfirmed.map((c: any) => toNum(c[4]));
        //const closes1h = candles1h.map((c: any) => toNum(c[4]));
        //const atr5m = computeATR(candles5m, 14);
        const atr15m = computeATR(candles15mConfirmed, 14);
        const atr1h = computeATR(candles1hConfirmed, 14);
        const ema20_15m = computeEMA(closes15m, 20).at(-1) ?? last;
        const rsi5m = computeRSI_Wilder(closes5m, 14);
        const rsi15m = computeRSI_Wilder(closes15m, 14);
        //const rsi15m = computeRSI_Wilder(closes15m, 14);
        const dist_from_ema20_15m_in_atr = atr15m > 0 ? (last - ema20_15m) / atr15m : 0;

        const slippageBpsEstimate = spreadBps;
        const gatesNow = gateChecks(spreadBps, depthUSD, slippageBpsEstimate);

        const result: any = {
            ts: asofIso,
            symbol,
            plan_id: plan?.plan_ts ? `${symbol}:${plan.plan_ts}` : null,
            plan_ts: plan?.plan_ts || null,
            plan_stale: planStale,
            plan_allowed_directions: plan?.allowed_directions,
            plan_risk_mode: plan?.risk_mode,
            plan_entry_mode: plan?.entry_mode,
            position_state: positionState,
            entries_disabled: false,
            entry_blockers: [] as string[],
            market: {
                last,
                spreadBps,
                depthUSD,
                flowBias,
                flowCvd: Number(analytics.cvd ?? 0),
                flowBuys: Number(analytics.buys ?? 0),
                flowSells: Number(analytics.sells ?? 0),
                flowSurge,
            },
            indicators: {
                atr1h,
                atr15m,
                rsi5m,
                ema20_15m,
                dist_from_ema20_15m_in_atr,
            },
            levels: {
                support: plan?.key_levels?.['1H']?.support_price ?? null,
                resistance: plan?.key_levels?.['1H']?.resistance_price ?? null,
                distSupportAtr: null as number | null,
                distResistanceAtr: null as number | null,
            },
            entry_eval: {
                preferredDir: null as any,
                confirmationCount: 0,
                rsiOk: null as boolean | null,
                rsi15mOk: null as boolean | null,
                rsi15mValue: null as number | null,
                rsi15mThreshold: null as number | null,
                emaOk: null as boolean | null,
                ema15mOk: null as boolean | null,
                ema15mLast: null as number | null,
                ema20_15m: null as number | null,
                ema15mDirection: null as string | null,
                obImbCentered: obImbCentered(Number(analytics.obImb ?? 0)),
                obImbOk: null as boolean | null,
                micro_filter_failures: [] as string[],
                flowBias: flowBias as number,
                flowOk: null as boolean | null,
                flowAgainst: null as boolean | null,
                flowGate: 'skipped' as string | null,
                flowGateReason: 'not_evaluated' as string | null,
                flowSurge: flowSurge as boolean,
                inPullbackZone: null as boolean | null,
                breakout2x5m: null as boolean | null,
            },
            invalidation_eval: null as any,
            decision: 'WAIT',
            reason: '',
            reason_code: '',
            trigger: null as string | null,
            orders: [],
            dryRun,
            gatesNow,
            hold_seconds: null as number | null,
            mfe: null as number | null,
            mae: null as number | null,
            fill_slippage_bps: null as number | null,
            exit_cause: null as string | null,
            exit_signal_ts: null as string | null,
            exit_signal_ms: null as number | null,
            exit_order_ts: null as string | null,
            exit_order_ts_ms: null as number | null,
            exit_intended_size_pct: null as number | null,
            exit_intended_size_base: null as number | null,
            exit_intended_notional: null as number | null,
            position_size_base: positionSizeBase,
            position_notional: positionNotional,
            exit_order_reduce_only: null as boolean | null,
            exit_order_size_base: null as number | null,
            exit_order_size_notional: null as number | null,
            invalidation_notes: plan?.exit_urgency?.invalidation_notes ?? null,
            invalidation_rule_direction: invalidationDirectionInfo(null).direction,
            invalidation_triggered: null as boolean | null,
            invalidation_trigger: null as string | null,
            invalidation_requested_action: null as string | null,
            invalidation_executed_action: null as string | null,
            invalidation_executed_size_pct: null as number | null,
        };

        const invalidation = parseInvalidation(plan?.exit_urgency?.invalidation_notes);
        const invalidationDirection = invalidationDirectionInfo(invalidation);
        result.invalidation_rule_direction = invalidationDirection.direction;

        const baseEntryBlockers = [];
        if (planStale) baseEntryBlockers.push('stale_plan');
        if (!plan) baseEntryBlockers.push('no_plan');
        if (plan?.risk_mode === 'OFF') baseEntryBlockers.push('risk_off');
        if (plan?.allowed_directions === 'NONE') baseEntryBlockers.push('dir_none');
        if (plan?.cooldown?.enabled) {
            const untilTs = plan.cooldown?.until_ts ? Date.parse(plan.cooldown.until_ts) : NaN;
            if (!plan.cooldown?.until_ts || !Number.isFinite(untilTs)) baseEntryBlockers.push('cooldown_missing_until');
            if (!Number.isFinite(untilTs) || now < untilTs) baseEntryBlockers.push('cooldown');
        }
        if (!gatesNow.spread_ok_now || !gatesNow.liquidity_ok_now || !gatesNow.slippage_ok_now)
            baseEntryBlockers.push('gates_fail');
        const entryBlocked = baseEntryBlockers.length > 0;
        result.entries_disabled = entryBlocked;
        result.entry_blockers = baseEntryBlockers;

        const exitOrderLockActive =
            positionState !== 'FLAT' &&
            execState.last_exit_order_ts &&
            now - execState.last_exit_order_ts < EXIT_ORDER_LOCK_MS;

        if (exitOrderLockActive) {
            result.decision = 'WAIT';
            result.reason = 'pending_exit_order';
            result.reason_code = reasonCode(result.reason);
            result.hold_seconds =
                execState.last_entry_ts && execState.last_entry_ts > 0 ? Math.floor((now - execState.last_entry_ts) / 1000) : null;
            try {
                await appendExecutionLog({ symbol, timestamp: now, payload: enrichReasonDetail(result) });
            } catch (err) {
                console.warn('Failed to append execution log:', err);
            }
            return res.status(200).json(result);
        }

        // Invalidation checks for open position (explicitly enabled by plan contract)
        if (positionState !== 'FLAT' && invalidation && invalidation.lvl !== null) {
            const expectedDirection = positionState === 'SHORT' ? 'above' : positionState === 'LONG' ? 'below' : null;
            let invalidationDirectionOk = true;
            let invalidationDirectionError: string | null = null;
            if (invalidationDirection.mixed) {
                invalidationDirectionOk = false;
                invalidationDirectionError = 'direction_mixed';
            } else if (invalidationDirection.direction && expectedDirection && invalidationDirection.direction !== expectedDirection) {
                invalidationDirectionOk = false;
                invalidationDirectionError = `direction_mismatch(expected_${expectedDirection})`;
            } else if (!invalidationDirection.direction) {
                invalidationDirectionOk = false;
                invalidationDirectionError = 'direction_missing';
            }

            const invalidationBuffer =
                (Number.isFinite(atr15m) && atr15m > 0 ? atr15m : Number.isFinite(atr1h) && atr1h > 0 ? atr1h : 0) * 0.1;
            const evalRule = (rule?: InvalidationRule) => {
                if (!rule) return false;
                const tf = normalizeTf(rule.tf);
                const minutes = tfMinutes(tf);
                let candles = candles5mConfirmed;
                if (minutes >= 15) candles = candles15mConfirmed;
                if (minutes >= 30) candles = candles30mConfirmed;
                if (minutes >= 60) candles = candles1hConfirmed;
                if (minutes >= 240) candles = candles4hConfirmed;
                const closes = candles.map((c: any) => toNum(c[4]));
                const count = countConsecutiveWithBuffer(closes, invalidation.lvl!, rule.direction, invalidationBuffer);
                return count >= rule.count;
            };
            const fastHit = invalidationDirectionOk ? evalRule(invalidation.fast) : false;
            const midHit = invalidationDirectionOk ? evalRule(invalidation.mid) : false;
            const hardHit = invalidationDirectionOk ? evalRule(invalidation.hard) : false;
            const triggered = fastHit || midHit || hardHit;
            const invalidationTriggeredBy = fastHit ? 'FAST' : midHit ? 'MID' : hardHit ? 'HARD' : null;
            const invalidationAction = invalidation.action || 'CLOSE';
            result.invalidation_eval = {
                lvl: invalidation.lvl,
                action: invalidationAction,
                fastHit,
                midHit,
                hardHit,
                triggered,
                triggered_by: invalidationTriggeredBy,
                enabled: plan?.exit_urgency?.close_if_invalidation === true && invalidationDirectionOk,
                direction_ok: invalidationDirectionOk,
                error: invalidationDirectionError,
                requested_action: invalidationAction,
                executed_action: null as string | null,
                executed_size_pct: null as number | null,
            };
            result.invalidation_triggered = triggered;
            result.invalidation_trigger = invalidationTriggeredBy;
            result.invalidation_requested_action = invalidationAction;
            if (!invalidationDirectionOk && invalidationDirectionError) {
                console.warn('Invalidation direction mismatch:', invalidationDirectionError, plan?.exit_urgency?.invalidation_notes);
            }
            if (triggered && plan?.exit_urgency?.close_if_invalidation === true && invalidationDirectionOk) {
                const act = invalidation.action || 'CLOSE';
                const trimPct = act === 'TRIM30' ? 30 : act === 'TRIM50' ? 50 : act === 'TRIM70' ? 70 : 100;
                result.decision = trimPct === 100 ? 'CLOSE' : 'TRIM';
                result.reason = `invalidation_${act}`;
                result.reason_code = reasonCode(result.reason);
                result.trigger = fastHit ? 'INVALIDATION_FAST' : midHit ? 'INVALIDATION_MID' : hardHit ? 'INVALIDATION_HARD' : 'INVALIDATION';
                result.exit_cause = trimPct === 100 ? result.trigger : 'INVALIDATION_TRIM';
                result.exit_order_reduce_only = trimPct < 100;
                result.exit_signal_ts = asofIso;
                result.exit_signal_ms = now;
                result.exit_intended_size_pct = trimPct;
                result.exit_intended_notional = notional;
                result.exit_intended_size_base =
                    positionSizeBase && Number.isFinite(positionSizeBase) ? (positionSizeBase * trimPct) / 100 : null;
                result.invalidation_executed_action = result.decision;
                result.invalidation_executed_size_pct = trimPct;
                result.invalidation_eval.executed_action = result.decision;
                result.invalidation_eval.executed_size_pct = trimPct;
                result.hold_seconds =
                    execState.last_entry_ts && execState.last_entry_ts > 0 ? Math.floor((now - execState.last_entry_ts) / 1000) : null;
                if (!dryRun) {
                    const exec = await executeDecision(
                        symbol,
                        notional,
                        { action: 'CLOSE', exit_size_pct: trimPct, summary: 'invalidation', reason: act },
                        resolveProductType(),
                        false,
                    );
                    result.order_details = {
                        intended: { action: 'CLOSE', exit_size_pct: trimPct, sizeUSDT: notional },
                        execution: compactExecutionResult(exec),
                    };
                    const execSizeBase = Number(exec?.size);
                    result.exit_order_size_base = Number.isFinite(execSizeBase) && execSizeBase > 0 ? execSizeBase : null;
                    result.exit_order_size_notional =
                        result.exit_order_size_base && last > 0 ? result.exit_order_size_base * last : null;
                    if (exec?.placed || exec?.closed) {
                        result.exit_order_ts = asofIso;
                        result.exit_order_ts_ms = now;
                        execState.last_exit_order_ts = now;
                    }
                } else {
                    result.order_details = { intended: { action: 'CLOSE', exit_size_pct: trimPct, sizeUSDT: notional }, execution: null };
                    result.exit_order_ts = asofIso;
                    result.exit_order_ts_ms = now;
                }
                await saveExecState(symbol, {
                    ...execState,
                    last_exit_ts: now,
                    last_action: result.decision,
                    last_plan_ts: plan?.plan_ts ?? execState.last_plan_ts,
                });
                try {
                    await maybeAttachRealizedPnl(result, symbol, now);
                    await appendExecutionLog({ symbol, timestamp: now, payload: enrichReasonDetail(result) });
                } catch (err) {
                    console.warn('Failed to append execution log:', err);
                }
                return res.status(200).json(result);
            }
        }

        // Forced exit if position conflicts with plan directions
        if (
            positionState !== 'FLAT' &&
            plan &&
            !planStale &&
            !allowedForDirection(plan.allowed_directions, positionState)
        ) {
            result.decision = 'CLOSE';
            result.reason = 'direction_not_allowed';
            result.reason_code = reasonCode(result.reason);
            result.exit_cause = 'DIRECTION_NOT_ALLOWED';
            result.trigger = 'DIR_MISMATCH';
            result.exit_order_reduce_only = false;
            result.exit_signal_ts = asofIso;
            result.exit_signal_ms = now;
            result.exit_intended_size_pct = 100;
            result.exit_intended_notional = notional;
            result.exit_intended_size_base =
                positionSizeBase && Number.isFinite(positionSizeBase) ? positionSizeBase : null;
            result.hold_seconds =
                execState.last_entry_ts && execState.last_entry_ts > 0 ? Math.floor((now - execState.last_entry_ts) / 1000) : null;
            if (!dryRun) {
                const exec = await executeDecision(
                    symbol,
                    notional,
                    { action: 'CLOSE', exit_size_pct: 100, summary: 'dir_mismatch', reason: result.reason },
                    resolveProductType(),
                    false,
                );
                result.order_details = {
                    intended: { action: 'CLOSE', exit_size_pct: 100, sizeUSDT: notional },
                    execution: compactExecutionResult(exec),
                };
                const execSizeBase = Number(exec?.size);
                result.exit_order_size_base = Number.isFinite(execSizeBase) && execSizeBase > 0 ? execSizeBase : null;
                result.exit_order_size_notional =
                    result.exit_order_size_base && last > 0 ? result.exit_order_size_base * last : null;
                if (exec?.placed || exec?.closed) {
                    result.exit_order_ts = asofIso;
                    result.exit_order_ts_ms = now;
                    execState.last_exit_order_ts = now;
                }
            } else {
                result.order_details = { intended: { action: 'CLOSE', exit_size_pct: 100, sizeUSDT: notional }, execution: null };
                result.exit_order_ts = asofIso;
                result.exit_order_ts_ms = now;
            }
            await saveExecState(symbol, {
                ...execState,
                last_exit_ts: now,
                last_action: result.decision,
                last_plan_ts: plan?.plan_ts ?? execState.last_plan_ts,
            });
            try {
                await maybeAttachRealizedPnl(result, symbol, now);
                await appendExecutionLog({ symbol, timestamp: now, payload: enrichReasonDetail(result) });
            } catch (err) {
                console.warn('Failed to append execution log:', err);
            }
            return res.status(200).json(result);
        }

        // Flow reversal trim (tape/flow safety)
        if (positionState !== 'FLAT') {
            const holdSeconds =
                execState.last_entry_ts && execState.last_entry_ts > 0 ? Math.floor((now - execState.last_entry_ts) / 1000) : null;
            const inProfit = profitActive(pos, last);
            const flowExitThreshold = 0.35;
            const flowAgainst =
                positionState === 'LONG' ? flowBias <= -flowExitThreshold : positionState === 'SHORT' ? flowBias >= flowExitThreshold : false;
            const rsiAgainst = positionState === 'LONG' ? rsi5m < 45 : positionState === 'SHORT' ? rsi5m > 55 : false;
            const holdOk = holdSeconds !== null && holdSeconds >= 120;
            if (flowAgainst && flowSurge && rsiAgainst && holdOk && inProfit) {
                const riskMode = plan?.risk_mode ?? 'NORMAL';
                const pct = riskMode === 'CONSERVATIVE' ? 50 : 30;
                result.decision = 'TRIM';
                result.reason = 'flow_reversal';
                result.reason_code = reasonCode(result.reason);
                result.trigger = 'FLOW_REVERSAL';
                result.exit_cause = 'FLOW_REVERSAL';
                result.exit_order_reduce_only = true;
                result.exit_signal_ts = asofIso;
                result.exit_signal_ms = now;
                result.exit_intended_size_pct = pct;
                result.exit_intended_notional = notional;
                result.exit_intended_size_base =
                    positionSizeBase && Number.isFinite(positionSizeBase) ? (positionSizeBase * pct) / 100 : null;
                result.hold_seconds = holdSeconds;
                if (!dryRun) {
                    const exec = await executeDecision(
                        symbol,
                        notional,
                        { action: 'CLOSE', exit_size_pct: pct, summary: 'flow_reversal', reason: result.reason },
                        productType,
                        false,
                    );
                    result.order_details = {
                        intended: { action: 'CLOSE', exit_size_pct: pct, sizeUSDT: notional },
                        execution: compactExecutionResult(exec),
                    };
                    const execSizeBase = Number(exec?.size);
                    result.exit_order_size_base = Number.isFinite(execSizeBase) && execSizeBase > 0 ? execSizeBase : null;
                    result.exit_order_size_notional =
                        result.exit_order_size_base && last > 0 ? result.exit_order_size_base * last : null;
                    if (exec?.placed || exec?.closed) {
                        result.exit_order_ts = asofIso;
                        result.exit_order_ts_ms = now;
                        execState.last_exit_order_ts = now;
                    }
                } else {
                    result.order_details = { intended: { action: 'CLOSE', exit_size_pct: pct, sizeUSDT: notional }, execution: null };
                    result.exit_order_ts = asofIso;
                    result.exit_order_ts_ms = now;
                }
                await saveExecState(symbol, {
                    ...execState,
                    last_exit_ts: now,
                    last_action: result.decision,
                    last_plan_ts: plan?.plan_ts ?? execState.last_plan_ts,
                });
                try {
                    await maybeAttachRealizedPnl(result, symbol, now);
                    await appendExecutionLog({ symbol, timestamp: now, payload: enrichReasonDetail(result) });
                } catch (err) {
                    console.warn('Failed to append execution log:', err);
                }
                return res.status(200).json(result);
            }
        }

        // Trim near opposite level (skip trims on stale plans; keep invalidation safety)
        if (positionState !== 'FLAT' && plan && !planStale && plan.exit_urgency?.trim_if_near_opposite_level) {
            const support = toNum(plan.key_levels?.['1H']?.support_price);
            const resistance = toNum(plan.key_levels?.['1H']?.resistance_price);
            const proximityAtr = atr15m || atr1h;
            const distSupportAtr = support && proximityAtr ? Math.abs(last - support) / proximityAtr : Infinity;
            const distResistanceAtr = resistance && proximityAtr ? Math.abs(last - resistance) / proximityAtr : Infinity;
            const threshold = plan.risk_mode === 'CONSERVATIVE' ? TRIM_PROXIMITY_ATR_CONSERVATIVE : TRIM_PROXIMITY_ATR_DEFAULT;
            const minHoldSeconds =
                plan.risk_mode === 'CONSERVATIVE' ? TRIM_MIN_HOLD_SECONDS_CONSERVATIVE : TRIM_MIN_HOLD_SECONDS_DEFAULT;
            const holdSeconds =
                execState.last_entry_ts && execState.last_entry_ts > 0 ? Math.floor((now - execState.last_entry_ts) / 1000) : null;
            const inProfit = profitActive(pos, last);
            const holdOk = holdSeconds !== null && holdSeconds >= minHoldSeconds;
            if (positionState === 'SHORT' && inProfit && holdOk && distSupportAtr < threshold) {
                result.decision = 'TRIM';
                result.reason = 'trim_near_support';
                result.reason_code = reasonCode(result.reason);
                result.trigger = 'TRIM_NEAR_LEVEL';
                result.exit_cause = 'TRIM_NEAR_LEVEL';
                result.hold_seconds = holdSeconds;
            }
            if (positionState === 'LONG' && inProfit && holdOk && distResistanceAtr < threshold) {
                result.decision = 'TRIM';
                result.reason = 'trim_near_resistance';
                result.reason_code = reasonCode(result.reason);
                result.trigger = 'TRIM_NEAR_LEVEL';
                result.exit_cause = 'TRIM_NEAR_LEVEL';
                result.hold_seconds = holdSeconds;
            }
            if (result.decision === 'TRIM') {
                const pct = plan.risk_mode === 'CONSERVATIVE' ? 50 : 30;
                result.exit_order_reduce_only = true;
                result.exit_signal_ts = asofIso;
                result.exit_signal_ms = now;
                result.exit_intended_size_pct = pct;
                result.exit_intended_notional = notional;
                result.exit_intended_size_base =
                    positionSizeBase && Number.isFinite(positionSizeBase) ? (positionSizeBase * pct) / 100 : null;
                if (!dryRun) {
                    const exec = await executeDecision(
                        symbol,
                        notional,
                        { action: 'CLOSE', exit_size_pct: pct, summary: 'trim', reason: result.reason },
                        resolveProductType(),
                        false,
                    );
                    result.order_details = {
                        intended: { action: 'CLOSE', exit_size_pct: pct, sizeUSDT: notional },
                        execution: compactExecutionResult(exec),
                    };
                    const execSizeBase = Number(exec?.size);
                    result.exit_order_size_base = Number.isFinite(execSizeBase) && execSizeBase > 0 ? execSizeBase : null;
                    result.exit_order_size_notional =
                        result.exit_order_size_base && last > 0 ? result.exit_order_size_base * last : null;
                    if (exec?.placed || exec?.closed) {
                        result.exit_order_ts = asofIso;
                        result.exit_order_ts_ms = now;
                        execState.last_exit_order_ts = now;
                    }
                } else {
                    result.order_details = { intended: { action: 'CLOSE', exit_size_pct: pct, sizeUSDT: notional }, execution: null };
                    result.exit_order_ts = asofIso;
                    result.exit_order_ts_ms = now;
                }
                await saveExecState(symbol, {
                    ...execState,
                    last_exit_ts: now,
                    last_action: result.decision,
                    last_plan_ts: plan?.plan_ts ?? execState.last_plan_ts,
                });
                try {
                    await maybeAttachRealizedPnl(result, symbol, now);
                    await appendExecutionLog({ symbol, timestamp: now, payload: enrichReasonDetail(result) });
                } catch (err) {
                    console.warn('Failed to append execution log:', err);
                }
                return res.status(200).json(result);
            }
        }

        // If still in position, and no action, wait
        if (positionState !== 'FLAT') {
            result.decision = 'WAIT';
            result.reason = 'holding';
            result.reason_code = reasonCode(result.reason);
            result.hold_seconds =
                execState.last_entry_ts && execState.last_entry_ts > 0 ? Math.floor((now - execState.last_entry_ts) / 1000) : null;
            try {
                await appendExecutionLog({ symbol, timestamp: now, payload: enrichReasonDetail(result) });
            } catch (err) {
                console.warn('Failed to append execution log:', err);
            }
            return res.status(200).json(result);
        }

        // No position: evaluate entries
        if (entryBlocked) {
            result.decision = baseEntryBlockers.includes('gates_fail') ? 'GATES_FAIL' : 'WAIT';
            result.reason = baseEntryBlockers.join(',');
            result.reason_code = reasonCode(result.reason);
            result.entry_eval.flowGate = 'skipped';
            result.entry_eval.flowGateReason = 'skipped_entry_blocked';
            try {
                await appendExecutionLog({ symbol, timestamp: now, payload: enrichReasonDetail(result) });
            } catch (err) {
                console.warn('Failed to append execution log:', err);
            }
            return res.status(200).json(result);
        }

        // Re-entry cooldown after invalidation/exit
        if (
            execState.last_exit_ts &&
            now - execState.last_exit_ts < REENTER_AFTER_INVALIDATION_MS &&
            planTsMs === Date.parse(execState.last_plan_ts || '')
        ) {
            result.decision = 'WAIT';
            result.reason = 'recent_exit_cooldown';
            result.reason_code = reasonCode(result.reason);
            result.entry_eval.flowGate = 'skipped';
            result.entry_eval.flowGateReason = 'skipped_recent_exit_cooldown';
            try {
                await appendExecutionLog({ symbol, timestamp: now, payload: enrichReasonDetail(result) });
            } catch (err) {
                console.warn('Failed to append execution log:', err);
            }
            return res.status(200).json(result);
        }
        if (execState.last_entry_ts && now - execState.last_entry_ts < ENTRY_COOLDOWN_MS) {
            result.decision = 'WAIT';
            result.reason = 'entry_cooldown';
            result.reason_code = reasonCode(result.reason);
            result.entry_eval.flowGate = 'skipped';
            result.entry_eval.flowGateReason = 'skipped_entry_cooldown';
            try {
                await appendExecutionLog({ symbol, timestamp: now, payload: enrichReasonDetail(result) });
            } catch (err) {
                console.warn('Failed to append execution log:', err);
            }
            return res.status(200).json(result);
        }

        // Market data guards (Bitget can return short arrays / partial data)
        if (closes5m.length < 25 || !Number.isFinite(atr1h) || atr1h <= 0) {
            result.decision = 'WAIT';
            result.reason = `insufficient_market_data(closes5m=${closes5m.length},atr1h=${Number.isFinite(atr1h) ? atr1h.toFixed(6) : 'NaN'})`;
            result.entry_eval.flowGate = 'skipped';
            result.entry_eval.flowGateReason = 'skipped_insufficient_market_data';
            try {
                await appendExecutionLog({ symbol, timestamp: now, payload: enrichReasonDetail(result) });
            } catch (err) {
                console.warn('Failed to append execution log:', err);
            }
            return res.status(200).json(result);
        }

        const supportPx = toNum(plan?.key_levels?.['1H']?.support_price);
        const resistancePx = toNum(plan?.key_levels?.['1H']?.resistance_price);

        const distSupportAtr =
            Number.isFinite(supportPx) && atr1h
                ? Math.abs(last - supportPx) / atr1h
                : Infinity;
        const distResistanceAtr =
            Number.isFinite(resistancePx) && atr1h
                ? Math.abs(resistancePx - last) / atr1h
                : Infinity;

        result.levels = {
            support: Number.isFinite(supportPx) ? supportPx : null,
            resistance: Number.isFinite(resistancePx) ? resistancePx : null,
            distSupportAtr: Number.isFinite(distSupportAtr) && distSupportAtr !== Infinity ? distSupportAtr : null,
            distResistanceAtr: Number.isFinite(distResistanceAtr) && distResistanceAtr !== Infinity ? distResistanceAtr : null,
        };

        // Extension filter
        if (
            Math.abs(dist_from_ema20_15m_in_atr) >
            (plan?.no_trade_rules?.max_dist_from_ema20_15m_in_atr_for_new_entries ?? 2.2)
        ) {
            result.decision = 'WAIT';
            result.reason = 'extension_block';
            result.reason_code = reasonCode(result.reason);
            result.entry_eval.flowGate = 'skipped';
            result.entry_eval.flowGateReason = 'skipped_extension_block';
            try {
                await appendExecutionLog({ symbol, timestamp: now, payload: enrichReasonDetail(result) });
            } catch (err) {
                console.warn('Failed to append execution log:', err);
            }
            return res.status(200).json(result);
        }

        // Direction choice
        const preferredDir = pickDirection(plan.allowed_directions, plan.macro_bias, plan.primary_bias);
        result.entry_eval.preferredDir = preferredDir;
        if (!preferredDir) {
            result.decision = 'WAIT';
            result.reason = 'no_dir';
            result.reason_code = reasonCode(result.reason);
            result.entry_eval.flowGate = 'skipped';
            result.entry_eval.flowGateReason = 'skipped_no_dir';
            try {
                await appendExecutionLog({ symbol, timestamp: now, payload: enrichReasonDetail(result) });
            } catch (err) {
                console.warn('Failed to append execution log:', err);
            }
            return res.status(200).json(result);
        }

        if (preferredDir === 'SHORT' && plan.micro_bias === 'UP') {
            result.decision = 'WAIT';
            result.reason = 'micro_bias_block';
            result.reason_code = reasonCode(result.reason);
            result.entry_eval.flowGate = 'skipped';
            result.entry_eval.flowGateReason = 'skipped_micro_bias';
            try {
                await appendExecutionLog({ symbol, timestamp: now, payload: enrichReasonDetail(result) });
            } catch (err) {
                console.warn('Failed to append execution log:', err);
            }
            return res.status(200).json(result);
        }
        if (preferredDir === 'LONG' && plan.micro_bias === 'DOWN') {
            result.decision = 'WAIT';
            result.reason = 'micro_bias_block';
            result.reason_code = reasonCode(result.reason);
            result.entry_eval.flowGate = 'skipped';
            result.entry_eval.flowGateReason = 'skipped_micro_bias';
            try {
                await appendExecutionLog({ symbol, timestamp: now, payload: enrichReasonDetail(result) });
            } catch (err) {
                console.warn('Failed to append execution log:', err);
            }
            return res.status(200).json(result);
        }

        const rsi15mOk = Number.isFinite(rsi15m)
            ? preferredDir === 'LONG'
                ? rsi15m >= 50
                : rsi15m <= 50
            : true;
        const ema15mOk = preferredDir === 'LONG' ? last >= ema20_15m : last <= ema20_15m;
        const microFilterFailures: string[] = [];
        const rsi15mThreshold = 50;
        const emaDirection = preferredDir === 'LONG' ? 'above' : 'below';
        const rsi15mValue = Number.isFinite(rsi15m) ? Number(rsi15m.toFixed(2)) : null;
        const ema20_15mValue = Number.isFinite(ema20_15m) ? Number(ema20_15m.toFixed(6)) : null;
        const lastValue = Number.isFinite(last) ? Number(last.toFixed(6)) : null;
        if (!rsi15mOk) {
            microFilterFailures.push(
                rsi15mValue !== null ? `rsi15m(${rsi15mValue}<${rsi15mThreshold})` : 'rsi15m(unavailable)',
            );
        }
        if (!ema15mOk) {
            microFilterFailures.push(
                lastValue !== null && ema20_15mValue !== null
                    ? `ema15m(last=${lastValue},ema20=${ema20_15mValue},need_${emaDirection})`
                    : 'ema15m(unavailable)',
            );
        }
        result.entry_eval.rsi15mOk = rsi15mOk;
        result.entry_eval.rsi15mValue = rsi15mValue;
        result.entry_eval.rsi15mThreshold = rsi15mThreshold;
        result.entry_eval.ema15mOk = ema15mOk;
        result.entry_eval.ema15mLast = lastValue;
        result.entry_eval.ema20_15m = ema20_15mValue;
        result.entry_eval.ema15mDirection = emaDirection;
        result.entry_eval.micro_filter_failures = microFilterFailures;
        if (microFilterFailures.length) {
            result.decision = 'WAIT';
            result.reason = 'micro_filter_block';
            result.reason_code = reasonCode(result.reason);
            result.entry_eval.flowGate = 'skipped';
            result.entry_eval.flowGateReason = 'skipped_micro_filter';
            try {
                await appendExecutionLog({ symbol, timestamp: now, payload: enrichReasonDetail(result) });
            } catch (err) {
                console.warn('Failed to append execution log:', err);
            }
            return res.status(200).json(result);
        }

        const flowAgainst = preferredDir === 'LONG' ? flowBias < -0.1 : flowBias > 0.1;
        result.entry_eval.flowAgainst = flowAgainst;
        result.entry_eval.flowOk = !(flowAgainst && flowSurge);
        result.entry_eval.flowGate = result.entry_eval.flowOk ? 'ok' : 'blocked';
        result.entry_eval.flowGateReason = flowAgainst && flowSurge
            ? 'flow_against_with_surge'
            : flowAgainst
            ? 'flow_against_no_surge'
            : flowSurge
            ? 'flow_with_surge'
            : 'flow_align_no_surge';
        if (flowAgainst && flowSurge) {
            result.decision = 'WAIT';
            result.reason = 'flow_against';
            result.reason_code = reasonCode(result.reason);
            try {
                await appendExecutionLog({ symbol, timestamp: now, payload: enrichReasonDetail(result) });
            } catch (err) {
                console.warn('Failed to append execution log:', err);
            }
            return res.status(200).json(result);
        }

        // No-trade buffers
        if (
            preferredDir === 'SHORT' &&
            distSupportAtr < (plan.no_trade_rules?.avoid_short_if_dist_to_support_atr_1H_lt ?? 0.6)
        ) {
            result.decision = 'WAIT';
            result.reason = 'too_close_support';
            result.reason_code = reasonCode(result.reason);
            try {
                await appendExecutionLog({ symbol, timestamp: now, payload: enrichReasonDetail(result) });
            } catch (err) {
                console.warn('Failed to append execution log:', err);
            }
            return res.status(200).json(result);
        }
        if (
            preferredDir === 'LONG' &&
            distResistanceAtr < (plan.no_trade_rules?.avoid_long_if_dist_to_resistance_atr_1H_lt ?? 0.6)
        ) {
            result.decision = 'WAIT';
            result.reason = 'too_close_resistance';
            result.reason_code = reasonCode(result.reason);
            try {
                await appendExecutionLog({ symbol, timestamp: now, payload: enrichReasonDetail(result) });
            } catch (err) {
                console.warn('Failed to append execution log:', err);
            }
            return res.status(200).json(result);
        }

        // Entry mode logic (simplified deterministic)
        let enter = false;
        let entryReason = '';
        if (plan.entry_mode === 'NONE') {
            enter = false;
            entryReason = 'entry_mode_none';
        } else {
            const rsiOkShort = preferredDir === 'SHORT' ? rsi5m <= 55 : false;
            const rsiOkLong = preferredDir === 'LONG' ? rsi5m >= 45 : false;
            const emaCheckShort =
                preferredDir === 'SHORT'
                    ? closes5m.at(-1)! < (computeEMA(closes5m, 20).at(-1) ?? closes5m.at(-1)!)
                    : false;
            const emaCheckLong =
                preferredDir === 'LONG'
                    ? closes5m.at(-1)! > (computeEMA(closes5m, 20).at(-1) ?? closes5m.at(-1)!)
                    : false;
            const obImb = obImbCentered(Number(analytics.obImb ?? 0));
            const obImbOk =
                preferredDir === 'LONG' ? obImb >= 0.08 : preferredDir === 'SHORT' ? obImb <= -0.08 : false;
            const rsiOk = preferredDir === 'LONG' ? rsiOkLong : rsiOkShort;
            const emaOk = preferredDir === 'LONG' ? emaCheckLong : emaCheckShort;
            const confirmationCount = [
                rsiOkShort || rsiOkLong,
                emaCheckShort || emaCheckLong,
                obImbOk,
            ].filter(Boolean).length;
            result.entry_eval.rsiOk = rsiOk;
            result.entry_eval.emaOk = emaOk;
            result.entry_eval.obImbCentered = obImb;
            result.entry_eval.obImbOk = obImbOk;
            result.entry_eval.confirmationCount = confirmationCount;

            if (plan.entry_mode === 'PULLBACK' || plan.entry_mode === 'EITHER') {
                if (preferredDir === 'SHORT' && Number.isFinite(plan.key_levels?.['1H']?.resistance_price)) {
                    const resPx = Number(plan.key_levels['1H'].resistance_price);
                    const zoneHigh = resPx + 0.05 * atr1h;
                    const zoneLow = resPx - 0.35 * atr1h;
                    const inZone = last <= zoneHigh && last >= zoneLow;
                    result.entry_eval.inPullbackZone = inZone;
                    if (inZone && confirmationCount >= 1) {
                        enter = true;
                        entryReason = 'pullback_short';
                    }
                }
                if (preferredDir === 'LONG' && Number.isFinite(plan.key_levels?.['1H']?.support_price)) {
                    const supPx = Number(plan.key_levels['1H'].support_price);
                    const zoneLow = supPx - 0.05 * atr1h;
                    const zoneHigh = supPx + 0.35 * atr1h;
                    const inZone = last >= zoneLow && last <= zoneHigh;
                    result.entry_eval.inPullbackZone = inZone;
                    if (inZone && confirmationCount >= 1) {
                        enter = true;
                        entryReason = 'pullback_long';
                    }
                }
            }

            if (!enter && (plan.entry_mode === 'BREAKOUT' || plan.entry_mode === 'EITHER')) {
                const breakoutAtr = atr15m || atr1h;
                const breakoutBuffer = breakoutAtr && Number.isFinite(breakoutAtr) ? 0.1 * breakoutAtr : 0;
                if (preferredDir === 'SHORT' && Number.isFinite(plan.key_levels?.['1H']?.support_price)) {
                    const supPx = Number(plan.key_levels['1H'].support_price);
                    const lastClose5m = closes5m.at(-1)!;
                    const below = lastClose5m < supPx - breakoutBuffer && last < supPx - breakoutBuffer;
                    result.entry_eval.breakout2x5m = below;
                    if (below && confirmationCount >= 1) {
                        enter = true;
                        entryReason = 'breakout_short';
                    }
                }
                if (preferredDir === 'LONG' && Number.isFinite(plan.key_levels?.['1H']?.resistance_price)) {
                    const resPx = Number(plan.key_levels['1H'].resistance_price);
                    const lastClose5m = closes5m.at(-1)!;
                    const above = lastClose5m > resPx + breakoutBuffer && last > resPx + breakoutBuffer;
                    result.entry_eval.breakout2x5m = above;
                    if (above && confirmationCount >= 1) {
                        enter = true;
                        entryReason = 'breakout_long';
                    }
                }
            }
        }

        if (!enter) {
            result.decision = 'WAIT';
            result.reason = entryReason || 'no_entry_signal';
            try {
                await appendExecutionLog({ symbol, timestamp: now, payload: enrichReasonDetail(result) });
            } catch (err) {
                console.warn('Failed to append execution log:', err);
            }
            return res.status(200).json(result);
        }

        const leverage = leverageFromRisk(plan.risk_mode, plan.max_leverage || 1);
        const sideSize = sizeFromRisk(notional, plan.risk_mode);
        const tradeAction = preferredDir === 'LONG' ? 'BUY' : 'SELL';

        result.decision = preferredDir === 'LONG' ? 'ENTER_LONG' : 'ENTER_SHORT';
        result.reason = entryReason;
        result.orders = dryRun ? [] : [{ side: tradeAction, leverage, size: sideSize }];

        if (!dryRun) {
            const exec = await executeDecision(
                symbol,
                sideSize,
                { action: tradeAction as any, leverage, summary: entryReason, reason: entryReason },
                resolveProductType(),
                false,
            );
            result.order_details = {
                intended: { action: tradeAction, leverage, sizeUSDT: sideSize, summary: entryReason },
                execution: compactExecutionResult(exec),
            };
        } else {
            result.order_details = { intended: { action: tradeAction, leverage, sizeUSDT: sideSize, summary: entryReason }, execution: null };
        }

        await saveExecState(symbol, {
            ...execState,
            last_entry_ts: now,
            last_action: result.decision,
            last_plan_ts: plan?.plan_ts ?? execState.last_plan_ts,
        });

        try {
            await appendExecutionLog({ symbol, timestamp: now, payload: enrichReasonDetail(result) });
        } catch (err) {
            console.warn('Failed to append execution log:', err);
        }
        return res.status(200).json(result);
    } catch (err) {
        console.error('Executor error:', err);
        return res
            .status(500)
            .json({ error: 'executor_failed', message: err instanceof Error ? err.message : String(err) });
    }
}

export default handler;
