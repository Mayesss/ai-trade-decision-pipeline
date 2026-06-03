import {
  DAY_MODEL_GUIDED_COMPOSER_V1_STRATEGY_ID,
  parseDayComposerTuneId,
  type DayComposerLevelBlockId,
  type DayComposerManagementBlockId,
} from "../../scalp-v2/dayComposer";
import {
  inScalpEntrySessionProfileWindow,
  normalizeScalpEntrySessionProfile,
} from "../sessions";
import type { ScalpCandle } from "../types";

import {
  candleBody,
  close,
  computeAtrSeries,
  high,
  low,
  open,
  ts,
} from "./syntheticSignal";
import type { ScalpStrategyDefinition, ScalpStrategyPhaseInput } from "./types";

export const DAY_COMPOSER_MIN_M15_CANDLES = 160;

type LevelSet = {
  high: number | null;
  low: number | null;
  mid: number | null;
  label: string;
};

function dedupeReasonCodes(values: string[]): string[] {
  return Array.from(new Set(values.filter((value) => Boolean(String(value || "").trim()))));
}

function aggregateCandles(candles: ScalpCandle[], tfMinutes: number): ScalpCandle[] {
  const tfMs = Math.max(1, Math.floor(tfMinutes)) * 60_000;
  const buckets = new Map<number, ScalpCandle[]>();
  for (const candle of candles) {
    const key = Math.floor(ts(candle) / tfMs) * tfMs;
    const rows = buckets.get(key) || [];
    rows.push(candle);
    buckets.set(key, rows);
  }
  return Array.from(buckets.entries())
    .sort((a, b) => a[0] - b[0])
    .map(([key, rows]) => {
      const sorted = rows.slice().sort((a, b) => ts(a) - ts(b));
      const first = sorted[0]!;
      const last = sorted[sorted.length - 1]!;
      return [
        key,
        open(first),
        Math.max(...sorted.map(high)),
        Math.min(...sorted.map(low)),
        close(last),
        sorted.reduce((acc, row) => acc + Number(row[5] || 0), 0),
      ] as ScalpCandle;
    });
}

function startOfUtcDay(ms: number): number {
  const date = new Date(ms);
  return Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), date.getUTCDate());
}

function startOfUtcWeekMonday(ms: number): number {
  const dayStart = startOfUtcDay(ms);
  const day = new Date(dayStart).getUTCDay();
  const offsetDays = day === 0 ? 6 : day - 1;
  return dayStart - offsetDays * 24 * 60 * 60_000;
}

function windowLevel(candles: ScalpCandle[], startMs: number, endMs: number, label: string): LevelSet {
  const rows = candles.filter((candle) => ts(candle) >= startMs && ts(candle) < endMs);
  if (!rows.length) return { high: null, low: null, mid: null, label };
  const h = Math.max(...rows.map(high));
  const l = Math.min(...rows.map(low));
  return {
    high: Number.isFinite(h) ? h : null,
    low: Number.isFinite(l) ? l : null,
    mid: Number.isFinite(h) && Number.isFinite(l) ? (h + l) / 2 : null,
    label,
  };
}

function sessionVwap(candles: ScalpCandle[], startMs: number, endMs: number): number | null {
  const rows = candles.filter((candle) => ts(candle) >= startMs && ts(candle) < endMs);
  let pv = 0;
  let vol = 0;
  for (const candle of rows) {
    const volume = Math.max(0, Number(candle[5] || 0));
    const typical = (high(candle) + low(candle) + close(candle)) / 3;
    if (volume > 0) {
      pv += typical * volume;
      vol += volume;
    }
  }
  if (vol > 0) return pv / vol;
  if (!rows.length) return null;
  return rows.reduce((acc, candle) => acc + close(candle), 0) / rows.length;
}

function percentileRank(values: number[], current: number): number | null {
  const usable = values.filter((value) => Number.isFinite(value) && value > 0);
  if (!usable.length || !(Number.isFinite(current) && current > 0)) return null;
  return (usable.filter((value) => value <= current).length / usable.length) * 100;
}

function computeLevels(params: {
  candles: ScalpCandle[];
  nowMs: number;
  sessionStartMs: number;
}): Record<DayComposerLevelBlockId, LevelSet> {
  const todayStart = startOfUtcDay(params.nowMs);
  const priorDay = windowLevel(
    params.candles,
    todayStart - 24 * 60 * 60_000,
    todayStart,
    "prior_day_hl",
  );
  const asia = windowLevel(
    params.candles,
    todayStart,
    todayStart + 8 * 60 * 60_000,
    "asia_range_hl",
  );
  const sessionLengthMs = 8 * 60 * 60_000;
  const previousSession = windowLevel(
    params.candles,
    params.sessionStartMs - sessionLengthMs,
    params.sessionStartMs,
    "previous_session_hl",
  );
  const openingRange = windowLevel(
    params.candles,
    params.sessionStartMs,
    params.sessionStartMs + 30 * 60_000,
    "opening_range_30m",
  );
  const weeklyStart = startOfUtcWeekMonday(params.nowMs);
  const weeklyOpenCandle = params.candles.find((candle) => ts(candle) >= weeklyStart);
  const weeklyOpen = Number.isFinite(Number(weeklyOpenCandle?.[1]))
    ? Number(weeklyOpenCandle![1])
    : null;
  const vwap = sessionVwap(params.candles, params.sessionStartMs, params.nowMs);
  return {
    prior_day_hl: priorDay,
    asia_range_hl: asia,
    previous_session_hl: previousSession,
    weekly_open: {
      high: weeklyOpen,
      low: weeklyOpen,
      mid: weeklyOpen,
      label: "weekly_open",
    },
    session_vwap: {
      high: vwap,
      low: vwap,
      mid: vwap,
      label: "session_vwap",
    },
    opening_range_30m: openingRange,
  };
}

function directionFromContext(params: {
  input: ScalpStrategyPhaseInput;
  plan: ReturnType<typeof parseDayComposerTuneId>;
  h1: ScalpCandle[];
  d1: ScalpCandle[];
  last: ScalpCandle;
  atrPercentile: number | null;
  weeklyOpen: number | null;
}): "BUY" | "SELL" | "BOTH" | null {
  const lastClose = close(params.last);
  const h1Last = params.h1[params.h1.length - 1];
  const h1Prev = params.h1[params.h1.length - 2];
  const d1Last = params.d1[params.d1.length - 1];
  const d1Prev = params.d1[params.d1.length - 2];
  if (params.plan.contextId === "h1_trend_d1_bias") {
    if (!h1Last || !h1Prev || !d1Last || !d1Prev) return null;
    const h1Up = close(h1Last) > close(h1Prev);
    const d1Up = close(d1Last) >= close(d1Prev);
    if (h1Up && d1Up) return "BUY";
    if (!h1Up && !d1Up) return "SELL";
    return null;
  }
  if (params.plan.contextId === "h1_range_bound") return "BOTH";
  if (params.plan.contextId === "atr_compression_expansion") {
    if (params.atrPercentile === null || params.atrPercentile < 35) return null;
    return close(params.last) >= open(params.last) ? "BUY" : "SELL";
  }
  if (params.plan.contextId === "inside_day_breakout") return "BOTH";
  if (params.plan.contextId === "session_momentum") {
    return close(params.last) >= open(params.last) ? "BUY" : "SELL";
  }
  if (params.plan.contextId === "weekly_open_bias") {
    if (!(Number.isFinite(params.weeklyOpen) && Number(params.weeklyOpen) > 0)) return null;
    return lastClose >= Number(params.weeklyOpen) ? "BUY" : "SELL";
  }
  return "BOTH";
}

function chooseTriggerSide(params: {
  plan: ReturnType<typeof parseDayComposerTuneId>;
  level: LevelSet;
  last: ScalpCandle;
  prev: ScalpCandle;
  atrAbs: number;
  contextSide: "BUY" | "SELL" | "BOTH";
}): "BUY" | "SELL" | null {
  const buffer = Math.max(0, params.atrAbs * 0.08);
  const levelHigh = params.level.high;
  const levelLow = params.level.low;
  const midpoint = params.level.mid;
  const canBuy = params.contextSide === "BUY" || params.contextSide === "BOTH";
  const canSell = params.contextSide === "SELL" || params.contextSide === "BOTH";
  if (params.plan.triggerId === "sweep_reclaim") {
    if (canSell && levelHigh !== null && high(params.last) > levelHigh + buffer && close(params.last) < levelHigh) return "SELL";
    if (canBuy && levelLow !== null && low(params.last) < levelLow - buffer && close(params.last) > levelLow) return "BUY";
  }
  if (params.plan.triggerId === "failed_breakout_return") {
    if (canSell && levelHigh !== null && close(params.prev) > levelHigh && close(params.last) < levelHigh) return "SELL";
    if (canBuy && levelLow !== null && close(params.prev) < levelLow && close(params.last) > levelLow) return "BUY";
  }
  if (params.plan.triggerId === "breakout_retest_hold") {
    if (canBuy && levelHigh !== null && close(params.prev) > levelHigh && low(params.last) <= levelHigh + params.atrAbs * 0.3 && close(params.last) > levelHigh) return "BUY";
    if (canSell && levelLow !== null && close(params.prev) < levelLow && high(params.last) >= levelLow - params.atrAbs * 0.3 && close(params.last) < levelLow) return "SELL";
  }
  if (params.plan.triggerId === "vwap_pullback_continuation" && midpoint !== null) {
    if (canBuy && low(params.last) <= midpoint && close(params.last) > midpoint && close(params.last) > open(params.last)) return "BUY";
    if (canSell && high(params.last) >= midpoint && close(params.last) < midpoint && close(params.last) < open(params.last)) return "SELL";
  }
  return null;
}

function confirmationPasses(params: {
  plan: ReturnType<typeof parseDayComposerTuneId>;
  side: "BUY" | "SELL";
  level: LevelSet;
  last: ScalpCandle;
  h1: ScalpCandle[];
  atrAbs: number;
  recentVolumes: number[];
}): boolean {
  const levelRef =
    params.side === "BUY"
      ? params.level.high ?? params.level.mid
      : params.level.low ?? params.level.mid;
  if (params.plan.confirmationId === "m15_close_acceptance") {
    if (levelRef === null) return false;
    return params.side === "BUY" ? close(params.last) > levelRef : close(params.last) < levelRef;
  }
  if (params.plan.confirmationId === "h1_close_acceptance") {
    const h1Last = params.h1[params.h1.length - 1];
    if (!h1Last || levelRef === null) return false;
    return params.side === "BUY" ? close(h1Last) > levelRef : close(h1Last) < levelRef;
  }
  if (params.plan.confirmationId === "body_atr_expansion") {
    return params.atrAbs > 0 && candleBody(params.last) >= params.atrAbs * 0.65;
  }
  if (params.plan.confirmationId === "volume_expansion_20") {
    const current = Math.max(0, Number(params.last[5] || 0));
    const baseline = params.recentVolumes.slice(-20);
    const avg = baseline.length
      ? baseline.reduce((acc, value) => acc + value, 0) / baseline.length
      : 0;
    return current > 0 && avg > 0 && current >= avg * 1.15;
  }
  return false;
}

function targetForManagement(params: {
  managementId: DayComposerManagementBlockId;
  side: "BUY" | "SELL";
  entryPrice: number;
  stopPrice: number;
  levels: Record<DayComposerLevelBlockId, LevelSet>;
}): number | null {
  const riskAbs =
    params.side === "BUY"
      ? params.entryPrice - params.stopPrice
      : params.stopPrice - params.entryPrice;
  if (!(Number.isFinite(riskAbs) && riskAbs > 0)) return null;
  if (params.managementId === "target_opposite_session_level") {
    const session = params.levels.previous_session_hl;
    const target = params.side === "BUY" ? session.high : session.low;
    if (target !== null) return target;
  }
  if (params.managementId === "target_pdh_pdl") {
    const priorDay = params.levels.prior_day_hl;
    const target = params.side === "BUY" ? priorDay.high : priorDay.low;
    if (target !== null) return target;
  }
  const r = params.managementId === "fixed_2r_time_6h" ? 2 : 2.2;
  return params.side === "BUY"
    ? params.entryPrice + riskAbs * r
    : params.entryPrice - riskAbs * r;
}

export const dayModelGuidedComposerV1Strategy: ScalpStrategyDefinition = {
  id: DAY_MODEL_GUIDED_COMPOSER_V1_STRATEGY_ID,
  shortName: "Day Composer V1",
  longName: "Day-Trade Model Guided Composer V1 (M15/H1 Structure)",
  preferredBaseTf: "M15",
  preferredConfirmTf: "M3",
  applyPhaseDetectors(input) {
    const plan = parseDayComposerTuneId(input.state.tuneId);
    const entrySessionProfile = normalizeScalpEntrySessionProfile(
      input.cfg.sessions.entrySessionProfile,
      "berlin",
    );
    const base = input.market.baseCandles || [];
    const nextState = { ...input.state, updatedAtMs: input.nowMs };
    const baseTs = base[base.length - 1]?.[0] ?? null;
    if (baseTs !== null && input.market.baseTf === "M15") {
      nextState.lastProcessed = {
        ...nextState.lastProcessed,
        m15ClosedTsMs: baseTs,
      };
    }

    const baseReasons = [
      "DAY_MODEL_GUIDED_COMPOSER_ACTIVE",
      `DAY_CTX_${plan.contextId.toUpperCase()}`,
      `DAY_LVL_${plan.levelId.toUpperCase()}`,
      `DAY_TRG_${plan.triggerId.toUpperCase()}`,
      `DAY_CNF_${plan.confirmationId.toUpperCase()}`,
      `DAY_MGMT_${plan.managementId.toUpperCase()}`,
    ];

    if (!inScalpEntrySessionProfileWindow(input.nowMs, entrySessionProfile)) {
      return {
        state: { ...nextState, state: "IDLE" },
        reasonCodes: dedupeReasonCodes([
          ...baseReasons,
          "SESSION_FILTER_OUTSIDE_ENTRY_PROFILE",
          `SESSION_PROFILE_${entrySessionProfile.toUpperCase()}`,
        ]),
        entryIntent: null,
      };
    }
    if (input.state.trade) {
      return {
        state: { ...nextState, state: "IN_TRADE" },
        reasonCodes: dedupeReasonCodes([...baseReasons, "DAY_COMPOSER_MANAGEMENT_ONLY_OPEN_TRADE"]),
        entryIntent: null,
      };
    }
    if (input.state.stats.tradesPlaced >= 1) {
      return {
        state: { ...nextState, state: "DONE" },
        reasonCodes: dedupeReasonCodes([...baseReasons, "DAY_COMPOSER_MAX_ONE_TRADE_PER_SYMBOL_DAY"]),
        entryIntent: null,
      };
    }
    if (base.length < DAY_COMPOSER_MIN_M15_CANDLES) {
      return {
        state: { ...nextState, state: "IDLE" },
        reasonCodes: dedupeReasonCodes([...baseReasons, "DAY_COMPOSER_INSUFFICIENT_M15_HISTORY"]),
        entryIntent: null,
      };
    }

    const last = base[base.length - 1]!;
    const prev = base[base.length - 2]!;
    const h1 = aggregateCandles(base, 60);
    const d1 = aggregateCandles(base, 24 * 60);
    const atrSeries = computeAtrSeries(base, 14);
    const atrAbs = atrSeries[atrSeries.length - 1] || 0;
    const atrPercentile = percentileRank(atrSeries.slice(-96), atrAbs);
    const levels = computeLevels({
      candles: base,
      nowMs: input.nowMs,
      sessionStartMs: input.windows.raidStartMs,
    });
    const selectedLevel = levels[plan.levelId];
    const weeklyOpen = levels.weekly_open.mid;
    const contextSide = directionFromContext({
      input,
      plan,
      h1,
      d1,
      last,
      atrPercentile,
      weeklyOpen,
    });
    if (!contextSide) {
      return {
        state: { ...nextState, state: "IDLE" },
        reasonCodes: dedupeReasonCodes([...baseReasons, "DAY_COMPOSER_CONTEXT_FILTER_BLOCKED"]),
        entryIntent: null,
      };
    }

    const side = chooseTriggerSide({
      plan,
      level: selectedLevel,
      last,
      prev,
      atrAbs,
      contextSide,
    });
    if (!side) {
      return {
        state: { ...nextState, state: "IDLE" },
        reasonCodes: dedupeReasonCodes([...baseReasons, "DAY_COMPOSER_TRIGGER_NOT_READY"]),
        entryIntent: null,
      };
    }
    if (
      !confirmationPasses({
        plan,
        side,
        level: selectedLevel,
        last,
        h1,
        atrAbs,
        recentVolumes: base.slice(-21, -1).map((candle) => Number(candle[5] || 0)),
      })
    ) {
      return {
        state: { ...nextState, state: "CONFIRMING" },
        reasonCodes: dedupeReasonCodes([...baseReasons, "DAY_COMPOSER_CONFIRMATION_BLOCKED"]),
        entryIntent: null,
      };
    }

    const entryPrice = input.market.quote.price > 0 ? input.market.quote.price : close(last);
    const levelStop =
      side === "BUY"
        ? selectedLevel.low ?? low(last)
        : selectedLevel.high ?? high(last);
    const stopBuffer = Math.max(input.market.quote.spreadAbs * 2, atrAbs * 0.18);
    const stopPrice = side === "BUY" ? levelStop - stopBuffer : levelStop + stopBuffer;
    const takeProfitPrice = targetForManagement({
      managementId: plan.managementId,
      side,
      entryPrice,
      stopPrice,
      levels,
    });
    const targetDistance =
      takeProfitPrice !== null
        ? side === "BUY"
          ? takeProfitPrice - entryPrice
          : entryPrice - takeProfitPrice
        : 0;
    if (
      !(targetDistance > 0) ||
      targetDistance < Math.max(input.market.quote.spreadAbs * 3, atrAbs * 1.2)
    ) {
      return {
        state: { ...nextState, state: "CONFIRMING" },
        reasonCodes: dedupeReasonCodes([...baseReasons, "DAY_COMPOSER_EXPECTED_MOVE_TOO_SMALL"]),
        entryIntent: null,
      };
    }

    return {
      state: { ...nextState, state: "CONFIRMING" },
      reasonCodes: dedupeReasonCodes([
        ...baseReasons,
        "DAY_COMPOSER_STRUCTURE_LEVEL_READY",
        `DAY_SIDE_${side}`,
        `DAY_LEVEL_SOURCE_${selectedLevel.label.toUpperCase()}`,
      ]),
      entryIntent: {
        model: "structure_level",
        side,
        entryMode: "market",
        entryReferencePrice: entryPrice,
        stopPrice,
        takeProfitPrice,
        setupKey: [
          DAY_MODEL_GUIDED_COMPOSER_V1_STRATEGY_ID,
          input.state.tuneId,
          input.state.dayKey,
          selectedLevel.label,
          side,
          String(ts(last)),
        ].join(":"),
        reasonCodes: [
          "DAY_COMPOSER_STRUCTURE_LEVEL_READY",
          `DAY_CONTEXT_${plan.contextId.toUpperCase()}`,
          `DAY_TRIGGER_${plan.triggerId.toUpperCase()}`,
          `DAY_CONFIRMATION_${plan.confirmationId.toUpperCase()}`,
        ],
        metadata: {
          plan,
          level: selectedLevel,
          atrAbs,
          atrPercentile,
          targetDistance,
        },
      },
      meta: {
        dayComposer: {
          plan,
          level: selectedLevel,
          atrAbs,
          atrPercentile,
          h1Candles: h1.length,
          d1Candles: d1.length,
        },
      },
    };
  },
};
