import type { NextApiRequest, NextApiResponse } from 'next';

import { computeATR, computeEMA, computeRSI_Wilder } from '../../lib/indicators';
import { bitgetFetch, resolveProductType } from '../../lib/bitget';
import { computeAnalytics, fetchPositionInfo, PositionInfo } from '../../lib/analytics';
import { readPlan } from '../../lib/planStore';
import { readExecState, saveExecState } from '../../lib/execState';
import { executeDecision } from '../../lib/trading';

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
const ENTRY_COOLDOWN_MS = 10 * 60 * 1000;
const REENTER_AFTER_INVALIDATION_MS = 3 * 60 * 1000;

function toNum(x: any) {
    const n = Number(x);
    return Number.isFinite(n) ? n : NaN;
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
        /^LVL=(-?\d+(\.\d+)?);FAST=(\d+[smhd])_close_(above|below)_x(\d+);MID=(\d+[smhd])_close_(above|below)_x(\d+);HARD=(\d+[smhd])_close_(above|below)_x(\d+);ACTION=(CLOSE|TRIM50|TRIM30|TRIM70)$/;
    const m = notes.match(regex);
    if (!m) return null;
    const lvl = Number(m[1]);
    const fast: InvalidationRule = { tf: m[3], direction: m[4] as any, count: Number(m[5]) };
    const mid: InvalidationRule = { tf: m[6], direction: m[7] as any, count: Number(m[8]) };
    const hard: InvalidationRule = { tf: m[9], direction: m[10] as any, count: Number(m[11]) };
    const action = m[12] as ParsedInvalidation['action'];
    return { lvl, fast, mid, hard, action };
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

function tfMinutes(tf: string) {
    const m = tf.match(/^(\d+)([smhd])$/);
    if (!m) return 1;
    const v = Number(m[1]);
    const unit = m[2];
    switch (unit) {
        case 's':
            return Math.max(1 / 60, v / 60);
        case 'm':
            return v;
        case 'h':
            return v * 60;
        case 'd':
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
    return Math.min(cap || base, base);
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

        // Market snapshot
        const [orderbook, candles1m, candles5m, candles15m, candles1h] = await Promise.all([
            bitgetFetch('GET', '/api/v2/mix/market/orderbook', {
                symbol,
                productType: resolveProductType(),
                limit: 50,
            }),
            fetchCandles(symbol, '1m', 120),
            fetchCandles(symbol, '5m', 120),
            fetchCandles(symbol, '15m', 120),
            fetchCandles(symbol, '1H', 220),
        ]);

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

        const analytics = computeAnalytics({
            orderbook,
            trades: [],
            ticker: { lastPr: last, last },
            candles: candles1m,
        });

        // Indicators
        const closes5m = candles5m.map((c: any) => toNum(c[4]));
        const closes15m = candles15m.map((c: any) => toNum(c[4]));
        //const closes1h = candles1h.map((c: any) => toNum(c[4]));
        //const atr5m = computeATR(candles5m, 14);
        const atr15m = computeATR(candles15m, 14);
        const atr1h = computeATR(candles1h, 14);
        const ema20_15m = computeEMA(closes15m, 20).at(-1) ?? last;
        const rsi5m = computeRSI_Wilder(closes5m, 14);
        //const rsi15m = computeRSI_Wilder(closes15m, 14);
        const dist_from_ema20_15m_in_atr = atr15m > 0 ? (last - ema20_15m) / atr15m : 0;

        const gatesNow = gateChecks(spreadBps, depthUSD, 5);

        const result: any = {
            ts: asofIso,
            symbol,
            plan_ts: plan?.plan_ts || null,
            plan_stale: planStale,
            plan_allowed_directions: plan?.allowed_directions,
            plan_risk_mode: plan?.risk_mode,
            plan_entry_mode: plan?.entry_mode,
            position_state: positionState,
            decision: 'WAIT',
            reason: '',
            orders: [],
            dryRun,
            gatesNow,
        };

        const invalidation = parseInvalidation(plan?.exit_urgency?.invalidation_notes);

        const baseEntryBlockers = [];
        if (planStale) baseEntryBlockers.push('stale_plan');
        if (!plan) baseEntryBlockers.push('no_plan');
        if (plan?.risk_mode === 'OFF') baseEntryBlockers.push('risk_off');
        if (plan?.allowed_directions === 'NONE') baseEntryBlockers.push('dir_none');
        if (plan?.cooldown?.enabled && plan.cooldown?.until_ts && now < Date.parse(plan.cooldown.until_ts))
            baseEntryBlockers.push('cooldown');
        if (!gatesNow.spread_ok_now || !gatesNow.liquidity_ok_now || !gatesNow.slippage_ok_now)
            baseEntryBlockers.push('gates_fail');
        const entryBlocked = baseEntryBlockers.length > 0;

        // Invalidation checks for open position
        if (positionState !== 'FLAT' && invalidation && invalidation.lvl !== null) {
            const evalRule = (rule?: InvalidationRule) => {
                if (!rule) return false;
                const tf = rule.tf.toLowerCase();
                const minutes = tfMinutes(tf);
                let candles = candles5m;
                if (minutes >= 15) candles = candles15m;
                if (minutes >= 60) candles = candles1h;
                const closes = candles.map((c: any) => toNum(c[4]));
                const count = countConsecutive(closes, invalidation.lvl!, rule.direction);
                return count >= rule.count;
            };
            const triggered = evalRule(invalidation.fast) || evalRule(invalidation.mid) || evalRule(invalidation.hard);
            if (triggered) {
                const act = invalidation.action || 'CLOSE';
                const trimPct = act === 'TRIM30' ? 30 : act === 'TRIM50' ? 50 : act === 'TRIM70' ? 70 : 100;
                result.decision = trimPct === 100 ? 'CLOSE' : 'TRIM';
                result.reason = `invalidation_${act}`;
                const tradeAction = positionState === 'LONG' ? 'SELL' : 'BUY';
                if (!dryRun) {
                    await executeDecision(
                        symbol,
                        notional,
                        { action: tradeAction as any, exit_size_pct: trimPct, summary: 'invalidation', reason: act },
                        resolveProductType(),
                        false,
                    );
                }
                await saveExecState(symbol, {
                    ...execState,
                    last_exit_ts: now,
                    last_action: result.decision,
                    last_plan_ts: plan?.plan_ts ?? execState.last_plan_ts,
                });
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
            const tradeAction = positionState === 'LONG' ? 'SELL' : 'BUY';
            result.decision = 'CLOSE';
            result.reason = 'direction_not_allowed';
            if (!dryRun) {
                await executeDecision(
                    symbol,
                    notional,
                    { action: tradeAction as any, summary: 'dir_mismatch', reason: result.reason },
                    resolveProductType(),
                    false,
                );
            }
            await saveExecState(symbol, {
                ...execState,
                last_exit_ts: now,
                last_action: result.decision,
                last_plan_ts: plan?.plan_ts ?? execState.last_plan_ts,
            });
            return res.status(200).json(result);
        }

        // Trim near opposite level
        if (positionState !== 'FLAT' && plan && plan.exit_urgency?.trim_if_near_opposite_level) {
            const support = toNum(plan.key_levels?.['1H']?.support_price);
            const resistance = toNum(plan.key_levels?.['1H']?.resistance_price);
            const distSupportAtr = support && atr1h ? Math.abs(last - support) / atr1h : Infinity;
            const distResistanceAtr = resistance && atr1h ? Math.abs(last - resistance) / atr1h : Infinity;
            const threshold = 0.7;
            const inProfit = profitActive(pos, last);
            if (positionState === 'SHORT' && inProfit && distSupportAtr < threshold) {
                result.decision = 'TRIM';
                result.reason = 'trim_near_support';
            }
            if (positionState === 'LONG' && inProfit && distResistanceAtr < threshold) {
                result.decision = 'TRIM';
                result.reason = 'trim_near_resistance';
            }
            if (result.decision === 'TRIM') {
                const pct = plan.risk_mode === 'CONSERVATIVE' ? 50 : 30;
                const tradeAction = positionState === 'LONG' ? 'SELL' : 'BUY';
                if (!dryRun) {
                    await executeDecision(
                        symbol,
                        notional,
                        { action: tradeAction as any, exit_size_pct: pct, summary: 'trim', reason: result.reason },
                        resolveProductType(),
                        false,
                    );
                }
                await saveExecState(symbol, {
                    ...execState,
                    last_exit_ts: now,
                    last_action: result.decision,
                    last_plan_ts: plan?.plan_ts ?? execState.last_plan_ts,
                });
                return res.status(200).json(result);
            }
        }

        // If still in position, and no action, wait
        if (positionState !== 'FLAT') {
            result.decision = 'WAIT';
            result.reason = 'holding';
            return res.status(200).json(result);
        }

        // No position: evaluate entries
        if (entryBlocked) {
            result.decision = baseEntryBlockers.includes('gates_fail') ? 'GATES_FAIL' : 'WAIT';
            result.reason = baseEntryBlockers.join(',');
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
            return res.status(200).json(result);
        }
        if (execState.last_entry_ts && now - execState.last_entry_ts < ENTRY_COOLDOWN_MS) {
            result.decision = 'WAIT';
            result.reason = 'entry_cooldown';
            return res.status(200).json(result);
        }

        const distSupportAtr =
            plan?.key_levels?.['1H']?.support_price && atr1h
                ? (last - Number(plan.key_levels['1H'].support_price)) / atr1h
                : Infinity;
        const distResistanceAtr =
            plan?.key_levels?.['1H']?.resistance_price && atr1h
                ? (Number(plan.key_levels['1H'].resistance_price) - last) / atr1h
                : Infinity;

        // Extension filter
        if (
            Math.abs(dist_from_ema20_15m_in_atr) >
            (plan?.no_trade_rules?.max_dist_from_ema20_15m_in_atr_for_new_entries ?? 2.2)
        ) {
            result.decision = 'WAIT';
            result.reason = 'extension_block';
            return res.status(200).json(result);
        }

        // Direction choice
        const preferredDir = pickDirection(plan.allowed_directions, plan.macro_bias, plan.primary_bias);
        if (!preferredDir) {
            result.decision = 'WAIT';
            result.reason = 'no_dir';
            return res.status(200).json(result);
        }

        // No-trade buffers
        if (
            preferredDir === 'SHORT' &&
            Math.abs(distSupportAtr) < (plan.no_trade_rules?.avoid_short_if_dist_to_support_atr_1H_lt ?? 0.6)
        ) {
            result.decision = 'WAIT';
            result.reason = 'too_close_support';
            return res.status(200).json(result);
        }
        if (
            preferredDir === 'LONG' &&
            Math.abs(distResistanceAtr) < (plan.no_trade_rules?.avoid_long_if_dist_to_resistance_atr_1H_lt ?? 0.6)
        ) {
            result.decision = 'WAIT';
            result.reason = 'too_close_resistance';
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
            const confirmationCount = [
                rsiOkShort || rsiOkLong,
                emaCheckShort || emaCheckLong,
                analytics.obImb < -0.1 || analytics.obImb > 0.1,
            ].filter(Boolean).length;

            if (plan.entry_mode === 'PULLBACK' || plan.entry_mode === 'EITHER') {
                if (preferredDir === 'SHORT' && Number.isFinite(plan.key_levels?.['1H']?.resistance_price)) {
                    const resPx = Number(plan.key_levels['1H'].resistance_price);
                    const zoneHigh = resPx + 0.05 * atr1h;
                    const zoneLow = resPx - 0.35 * atr1h;
                    if (last <= zoneHigh && last >= zoneLow && confirmationCount >= 2) {
                        enter = true;
                        entryReason = 'pullback_short';
                    }
                }
                if (preferredDir === 'LONG' && Number.isFinite(plan.key_levels?.['1H']?.support_price)) {
                    const supPx = Number(plan.key_levels['1H'].support_price);
                    const zoneLow = supPx - 0.05 * atr1h;
                    const zoneHigh = supPx + 0.35 * atr1h;
                    if (last >= zoneLow && last <= zoneHigh && confirmationCount >= 2) {
                        enter = true;
                        entryReason = 'pullback_long';
                    }
                }
            }

            if (!enter && (plan.entry_mode === 'BREAKOUT' || plan.entry_mode === 'EITHER')) {
                if (preferredDir === 'SHORT' && Number.isFinite(plan.key_levels?.['1H']?.support_price)) {
                    const supPx = Number(plan.key_levels['1H'].support_price);
                    const lastClose5m = closes5m.at(-1)!;
                    const prevClose5m = closes5m.at(-2)!;
                    const below = lastClose5m < supPx && prevClose5m < supPx;
                    if (below && confirmationCount >= 1) {
                        enter = true;
                        entryReason = 'breakout_short';
                    }
                }
                if (preferredDir === 'LONG' && Number.isFinite(plan.key_levels?.['1H']?.resistance_price)) {
                    const resPx = Number(plan.key_levels['1H'].resistance_price);
                    const lastClose5m = closes5m.at(-1)!;
                    const prevClose5m = closes5m.at(-2)!;
                    const above = lastClose5m > resPx && prevClose5m > resPx;
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
            return res.status(200).json(result);
        }

        const leverage = leverageFromRisk(plan.risk_mode, plan.max_leverage || 1);
        const sideSize = sizeFromRisk(notional, plan.risk_mode);
        const tradeAction = preferredDir === 'LONG' ? 'BUY' : 'SELL';

        result.decision = preferredDir === 'LONG' ? 'ENTER_LONG' : 'ENTER_SHORT';
        result.reason = entryReason;
        result.orders = dryRun ? [] : [{ side: tradeAction, leverage, size: sideSize }];

        if (!dryRun) {
            await executeDecision(
                symbol,
                sideSize,
                { action: tradeAction as any, leverage, summary: entryReason, reason: entryReason },
                resolveProductType(),
                false,
            );
        }

        await saveExecState(symbol, {
            ...execState,
            last_entry_ts: now,
            last_action: result.decision,
            last_plan_ts: plan?.plan_ts ?? execState.last_plan_ts,
        });

        return res.status(200).json(result);
    } catch (err) {
        console.error('Executor error:', err);
        return res
            .status(500)
            .json({ error: 'executor_failed', message: err instanceof Error ? err.message : String(err) });
    }
}

export default handler;
