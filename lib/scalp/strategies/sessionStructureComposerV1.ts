import {
  SESSION_STRUCTURE_COMPOSER_V1_STRATEGY_ID,
  parseSessionStructureComposerTuneId,
  type SessionStructureLevelBlockId,
  type SessionStructureManagementBlockId,
  type SessionStructureTriggerBlockId,
} from "../../scalp-v2/sessionStructureComposer";
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

export const SESSION_STRUCTURE_MIN_M15_CANDLES = 96;

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

function computeIntradaySwing(candles: ScalpCandle[], nowMs: number): LevelSet {
  const rows = candles
    .filter((candle) => ts(candle) < nowMs)
    .slice(-32);
  if (rows.length < 8) return { high: null, low: null, mid: null, label: "intraday_swing_hl" };
  const h = Math.max(...rows.map(high));
  const l = Math.min(...rows.map(low));
  return {
    high: h,
    low: l,
    mid: (h + l) / 2,
    label: "intraday_swing_hl",
  };
}

function computeLevels(params: {
  candles: ScalpCandle[];
  nowMs: number;
  sessionStartMs: number;
}): Record<SessionStructureLevelBlockId, LevelSet> {
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
  const openingRange15 = windowLevel(
    params.candles,
    params.sessionStartMs,
    params.sessionStartMs + 15 * 60_000,
    "opening_range_15m",
  );
  const openingRange30 = windowLevel(
    params.candles,
    params.sessionStartMs,
    params.sessionStartMs + 30 * 60_000,
    "opening_range_30m",
  );
  const openingRange45 = windowLevel(
    params.candles,
    params.sessionStartMs,
    params.sessionStartMs + 45 * 60_000,
    "opening_range_45m",
  );
  const openingRange60 = windowLevel(
    params.candles,
    params.sessionStartMs,
    params.sessionStartMs + 60 * 60_000,
    "opening_range_60m",
  );
  const vwap = sessionVwap(params.candles, params.sessionStartMs, params.nowMs);
  return {
    session_vwap: {
      high: vwap,
      low: vwap,
      mid: vwap,
      label: "session_vwap",
    },
    opening_range_15m: openingRange15,
    opening_range_30m: openingRange30,
    opening_range_45m: openingRange45,
    opening_range_60m: openingRange60,
    previous_session_hl: previousSession,
    asia_range_hl: asia,
    prior_day_hl: priorDay,
    intraday_swing_hl: computeIntradaySwing(params.candles, params.nowMs),
  };
}

function directionFromContext(params: {
  plan: ReturnType<typeof parseSessionStructureComposerTuneId>;
  m30: ScalpCandle[];
  h1: ScalpCandle[];
  last: ScalpCandle;
  prev: ScalpCandle;
  atrPercentile: number | null;
  vwap: number | null;
  openingRange: LevelSet;
}): "BUY" | "SELL" | "BOTH" | null {
  if (params.plan.contextId === "m30_session_momentum") {
    const m30Last = params.m30[params.m30.length - 1];
    const m30Prev = params.m30[params.m30.length - 2];
    if (!m30Last || !m30Prev) return null;
    if (close(m30Last) > open(m30Last) && close(m30Last) >= close(m30Prev)) return "BUY";
    if (close(m30Last) < open(m30Last) && close(m30Last) <= close(m30Prev)) return "SELL";
    return null;
  }
  if (params.plan.contextId === "h1_directional_bias") {
    const h1Last = params.h1[params.h1.length - 1];
    const h1Prev = params.h1[params.h1.length - 2];
    if (!h1Last || !h1Prev) return null;
    if (close(h1Last) >= close(h1Prev)) return "BUY";
    return "SELL";
  }
  if (params.plan.contextId === "opening_drive" || params.plan.contextId === "london_open_drive") {
    const mid = params.openingRange.mid;
    if (mid === null) return null;
    const body = candleBody(params.last);
    const minBody = params.plan.contextId === "london_open_drive" ? 0.0000001 : 0;
    if (body < minBody) return null;
    if (close(params.last) > mid && close(params.last) >= open(params.last)) return "BUY";
    if (close(params.last) < mid && close(params.last) <= open(params.last)) return "SELL";
    return null;
  }
  if (params.plan.contextId === "atr_expansion") {
    if (params.atrPercentile === null || params.atrPercentile < 45) return null;
    return close(params.last) >= open(params.last) ? "BUY" : "SELL";
  }
  if (params.plan.contextId === "atr_low_chop_avoid") {
    if (params.atrPercentile === null || params.atrPercentile < 35) return null;
    if (close(params.last) > close(params.prev) && close(params.last) >= open(params.last)) return "BUY";
    if (close(params.last) < close(params.prev) && close(params.last) <= open(params.last)) return "SELL";
    return null;
  }
  if (params.plan.contextId === "vwap_balance_shift") {
    if (params.vwap === null) return null;
    if (close(params.prev) <= params.vwap && close(params.last) > params.vwap) return "BUY";
    if (close(params.prev) >= params.vwap && close(params.last) < params.vwap) return "SELL";
    return close(params.last) >= params.vwap ? "BUY" : "SELL";
  }
  if (params.plan.contextId === "ny_continuation") {
    const h1Last = params.h1[params.h1.length - 1];
    const h1Prev = params.h1[params.h1.length - 2];
    if (!h1Last || !h1Prev || params.vwap === null) return null;
    if (close(h1Last) >= close(h1Prev) && close(params.last) >= params.vwap) return "BUY";
    if (close(h1Last) < close(h1Prev) && close(params.last) <= params.vwap) return "SELL";
    return null;
  }
  return "BOTH";
}

function breakoutRetestTolerance(triggerId: SessionStructureTriggerBlockId): number {
  if (triggerId === "breakout_retest_hold_tight") return 0.18;
  if (triggerId === "breakout_retest_hold_loose") return 0.55;
  return 0.35;
}

function chooseTriggerSide(params: {
  plan: ReturnType<typeof parseSessionStructureComposerTuneId>;
  level: LevelSet;
  last: ScalpCandle;
  prev: ScalpCandle;
  atrAbs: number;
  contextSide: "BUY" | "SELL" | "BOTH";
}): "BUY" | "SELL" | null {
  const buffer = Math.max(0, params.atrAbs * 0.06);
  const levelHigh = params.level.high;
  const levelLow = params.level.low;
  const midpoint = params.level.mid;
  const canBuy = params.contextSide === "BUY" || params.contextSide === "BOTH";
  const canSell = params.contextSide === "SELL" || params.contextSide === "BOTH";
  if (
    params.plan.triggerId === "breakout_retest_hold" ||
    params.plan.triggerId === "breakout_retest_hold_tight" ||
    params.plan.triggerId === "breakout_retest_hold_loose"
  ) {
    const tolerance = params.atrAbs * breakoutRetestTolerance(params.plan.triggerId);
    if (canBuy && levelHigh !== null && close(params.prev) > levelHigh && low(params.last) <= levelHigh + tolerance && close(params.last) > levelHigh) return "BUY";
    if (canSell && levelLow !== null && close(params.prev) < levelLow && high(params.last) >= levelLow - tolerance && close(params.last) < levelLow) return "SELL";
  }
  if (params.plan.triggerId === "vwap_pullback_continuation" && midpoint !== null) {
    if (canBuy && low(params.last) <= midpoint && close(params.last) > midpoint && close(params.last) > open(params.last)) return "BUY";
    if (canSell && high(params.last) >= midpoint && close(params.last) < midpoint && close(params.last) < open(params.last)) return "SELL";
  }
  if (params.plan.triggerId === "sweep_reclaim") {
    if (canSell && levelHigh !== null && high(params.last) > levelHigh + buffer && close(params.last) < levelHigh) return "SELL";
    if (canBuy && levelLow !== null && low(params.last) < levelLow - buffer && close(params.last) > levelLow) return "BUY";
  }
  if (params.plan.triggerId === "failed_breakout_return") {
    if (canSell && levelHigh !== null && close(params.prev) > levelHigh && close(params.last) < levelHigh) return "SELL";
    if (canBuy && levelLow !== null && close(params.prev) < levelLow && close(params.last) > levelLow) return "BUY";
  }
  return null;
}

function confirmationPasses(params: {
  plan: ReturnType<typeof parseSessionStructureComposerTuneId>;
  side: "BUY" | "SELL";
  level: LevelSet;
  last: ScalpCandle;
  m30: ScalpCandle[];
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
  if (params.plan.confirmationId === "m30_close_acceptance") {
    const m30Last = params.m30[params.m30.length - 1];
    if (!m30Last || levelRef === null) return false;
    return params.side === "BUY" ? close(m30Last) > levelRef : close(m30Last) < levelRef;
  }
  if (params.plan.confirmationId === "body_atr_expansion") {
    return params.atrAbs > 0 && candleBody(params.last) >= params.atrAbs * 0.5;
  }
  if (params.plan.confirmationId === "volume_expansion_20") {
    const current = Math.max(0, Number(params.last[5] || 0));
    const baseline = params.recentVolumes.slice(-20);
    const avg = baseline.length
      ? baseline.reduce((acc, value) => acc + value, 0) / baseline.length
      : 0;
    return current > 0 && avg > 0 && current >= avg * 1.1;
  }
  if (params.plan.confirmationId === "retest_wick_rejection") {
    if (levelRef === null) return false;
    const body = Math.max(candleBody(params.last), params.atrAbs * 0.05);
    const lowerWick = Math.min(open(params.last), close(params.last)) - low(params.last);
    const upperWick = high(params.last) - Math.max(open(params.last), close(params.last));
    if (params.side === "BUY") {
      return low(params.last) <= levelRef && close(params.last) > levelRef && lowerWick >= body * 0.55;
    }
    return high(params.last) >= levelRef && close(params.last) < levelRef && upperWick >= body * 0.55;
  }
  return false;
}

function targetForManagement(params: {
  managementId: SessionStructureManagementBlockId;
  side: "BUY" | "SELL";
  entryPrice: number;
  stopPrice: number;
  levels: Record<SessionStructureLevelBlockId, LevelSet>;
}): number | null {
  const riskAbs =
    params.side === "BUY"
      ? params.entryPrice - params.stopPrice
      : params.stopPrice - params.entryPrice;
  if (!(Number.isFinite(riskAbs) && riskAbs > 0)) return null;
  if (params.managementId === "target_next_session_level") {
    const levelTargets = [
      params.levels.previous_session_hl,
      params.levels.opening_range_15m,
      params.levels.opening_range_30m,
      params.levels.opening_range_45m,
      params.levels.opening_range_60m,
      params.levels.asia_range_hl,
      params.levels.prior_day_hl,
      params.levels.intraday_swing_hl,
    ]
      .map((level) => (params.side === "BUY" ? level.high : level.low))
      .filter((value): value is number => Number.isFinite(value));
    if (params.side === "BUY") {
      const above = levelTargets.filter((value) => value > params.entryPrice);
      if (above.length) return Math.min(...above);
    } else {
      const below = levelTargets.filter((value) => value < params.entryPrice);
      if (below.length) return Math.max(...below);
    }
  }
  const r =
    params.managementId === "fixed_1_5r_time_2h"
      ? 1.5
      : params.managementId === "trail_after_0_8r_time_3h"
        ? 1.8
        : 2;
  return params.side === "BUY"
    ? params.entryPrice + riskAbs * r
    : params.entryPrice - riskAbs * r;
}

export const sessionStructureComposerV1Strategy: ScalpStrategyDefinition = {
  id: SESSION_STRUCTURE_COMPOSER_V1_STRATEGY_ID,
  shortName: "Session Composer V1",
  longName: "Session Structure Composer V1 (M15/M30 Session Swing)",
  preferredBaseTf: "M15",
  preferredConfirmTf: "M3",
  applyPhaseDetectors(input) {
    const plan = parseSessionStructureComposerTuneId(input.state.tuneId);
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
      "SESSION_STRUCTURE_COMPOSER_ACTIVE",
      `SESSION_CTX_${plan.contextId.toUpperCase()}`,
      `SESSION_LVL_${plan.levelId.toUpperCase()}`,
      `SESSION_TRG_${plan.triggerId.toUpperCase()}`,
      `SESSION_CNF_${plan.confirmationId.toUpperCase()}`,
      `SESSION_MGMT_${plan.managementId.toUpperCase()}`,
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
        reasonCodes: dedupeReasonCodes([...baseReasons, "SESSION_COMPOSER_MANAGEMENT_ONLY_OPEN_TRADE"]),
        entryIntent: null,
      };
    }
    if (input.state.stats.tradesPlaced >= 2) {
      return {
        state: { ...nextState, state: "DONE" },
        reasonCodes: dedupeReasonCodes([...baseReasons, "SESSION_COMPOSER_MAX_TWO_TRADES_PER_SYMBOL_DAY"]),
        entryIntent: null,
      };
    }
    if (
      input.state.stats.consecutiveLosses > 0 &&
      input.state.stats.lastExitAtMs !== null &&
      input.state.stats.lastExitAtMs >= input.windows.raidStartMs
    ) {
      return {
        state: { ...nextState, state: "DONE" },
        reasonCodes: dedupeReasonCodes([...baseReasons, "SESSION_COMPOSER_SAME_SESSION_REENTRY_AFTER_STOP_BLOCKED"]),
        entryIntent: null,
      };
    }
    if (base.length < SESSION_STRUCTURE_MIN_M15_CANDLES) {
      return {
        state: { ...nextState, state: "IDLE" },
        reasonCodes: dedupeReasonCodes([...baseReasons, "SESSION_COMPOSER_INSUFFICIENT_M15_HISTORY"]),
        entryIntent: null,
      };
    }

    const last = base[base.length - 1]!;
    const prev = base[base.length - 2]!;
    const m30 = aggregateCandles(base, 30);
    const h1 = aggregateCandles(base, 60);
    const atrSeries = computeAtrSeries(base, 14);
    const atrAbs = atrSeries[atrSeries.length - 1] || 0;
    const atrPercentile = percentileRank(atrSeries.slice(-96), atrAbs);
    const levels = computeLevels({
      candles: base,
      nowMs: input.nowMs,
      sessionStartMs: input.windows.raidStartMs,
    });
    const selectedLevel = levels[plan.levelId];
    const contextSide = directionFromContext({
      plan,
      m30,
      h1,
      last,
      prev,
      atrPercentile,
      vwap: levels.session_vwap.mid,
      openingRange: levels.opening_range_30m,
    });
    if (!contextSide) {
      return {
        state: { ...nextState, state: "IDLE" },
        reasonCodes: dedupeReasonCodes([...baseReasons, "SESSION_COMPOSER_CONTEXT_FILTER_BLOCKED"]),
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
        reasonCodes: dedupeReasonCodes([...baseReasons, "SESSION_COMPOSER_TRIGGER_NOT_READY"]),
        entryIntent: null,
      };
    }
    if (
      !confirmationPasses({
        plan,
        side,
        level: selectedLevel,
        last,
        m30,
        atrAbs,
        recentVolumes: base.slice(-21, -1).map((candle) => Number(candle[5] || 0)),
      })
    ) {
      return {
        state: { ...nextState, state: "CONFIRMING" },
        reasonCodes: dedupeReasonCodes([...baseReasons, "SESSION_COMPOSER_CONFIRMATION_BLOCKED"]),
        entryIntent: null,
      };
    }

    const entryPrice = input.market.quote.price > 0 ? input.market.quote.price : close(last);
    const levelStop =
      side === "BUY"
        ? selectedLevel.low ?? low(last)
        : selectedLevel.high ?? high(last);
    const stopBuffer = Math.max(input.market.quote.spreadAbs * 2, atrAbs * 0.14);
    const stopPrice = side === "BUY" ? levelStop - stopBuffer : levelStop + stopBuffer;
    const riskAbs = side === "BUY" ? entryPrice - stopPrice : stopPrice - entryPrice;
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
      !(riskAbs > 0) ||
      targetDistance / riskAbs < 1.2 ||
      targetDistance < Math.max(input.market.quote.spreadAbs * 3, atrAbs * 0.8)
    ) {
      return {
        state: { ...nextState, state: "CONFIRMING" },
        reasonCodes: dedupeReasonCodes([...baseReasons, "SESSION_COMPOSER_EXPECTED_MOVE_TOO_SMALL"]),
        entryIntent: null,
      };
    }

    return {
      state: { ...nextState, state: "CONFIRMING" },
      reasonCodes: dedupeReasonCodes([
        ...baseReasons,
        "SESSION_COMPOSER_STRUCTURE_LEVEL_READY",
        `SESSION_SIDE_${side}`,
        `SESSION_LEVEL_SOURCE_${selectedLevel.label.toUpperCase()}`,
      ]),
      entryIntent: {
        model: "structure_level",
        side,
        entryMode: "market",
        entryReferencePrice: entryPrice,
        stopPrice,
        takeProfitPrice,
        setupKey: [
          SESSION_STRUCTURE_COMPOSER_V1_STRATEGY_ID,
          input.state.tuneId,
          input.state.dayKey,
          selectedLevel.label,
          side,
          String(ts(last)),
        ].join(":"),
        reasonCodes: [
          "SESSION_COMPOSER_STRUCTURE_LEVEL_READY",
          `SESSION_CONTEXT_${plan.contextId.toUpperCase()}`,
          `SESSION_TRIGGER_${plan.triggerId.toUpperCase()}`,
          `SESSION_CONFIRMATION_${plan.confirmationId.toUpperCase()}`,
        ],
        metadata: {
          plan,
          level: selectedLevel,
          atrAbs,
          atrPercentile,
          targetDistance,
          riskReward: targetDistance / riskAbs,
        },
      },
      meta: {
        sessionComposer: {
          plan,
          level: selectedLevel,
          atrAbs,
          atrPercentile,
          m30Candles: m30.length,
          h1Candles: h1.length,
        },
      },
    };
  },
};
